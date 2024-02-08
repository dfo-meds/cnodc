import datetime
import json
import typing as t

import flask
from autoinject import injector

from cnodc.api.auth import LoginController
from cnodc.codecs import OCProc2BinCodec
from cnodc.nodb import NODBController, LockType
from cnodc.ocproc2.operations import QCOperator
import cnodc.ocproc2.structures as ocproc2
from cnodc.util import CNODCError, clean_for_json, vlq_encode
import uuid
import cnodc.nodb.structures as structures
import threading
import itsdangerous

from cnodc.workflow.workflow import WorkflowPayload

DB_LOCK_TIME = 3600  # in seconds


@injector.injectable
class NODBWebController:

    nodb: NODBController = None
    login: LoginController = None

    @injector.construct
    def __init__(self):
        self._serializer = None
        self._serializer_lock = threading.Lock()

    def _get_serializer(self) -> itsdangerous.Serializer:
        if not flask.current_app.config.get('SECRET_KEY'):
            self._logger.error("Secret key is not defined properly")
            raise CNODCError('Missing secret key', 'NODBWEB', 1004)
        if self._serializer is None:
            with self._serializer_lock:
                if self._serializer is None:
                    self._serializer = itsdangerous.Serializer(flask.current_app.config['SECRET_KEY'])
        return self._serializer

    def get_next_queue_item(self,
                            queue_name: str,
                            subqueue_name: t.Optional[str] = None):
        access_perms = self.login.current_permissions()
        if f'handle_{queue_name}' not in access_perms:
            raise ValueError('cannot access this queue')
        app_id = f"{self.login.current_user()}.{uuid.uuid4()}"
        with self.nodb as db:
            queue_item = db.fetch_next_queue_item(
                queue_name=queue_name,
                app_id=app_id,
                subqueue_name=subqueue_name
            )
            if queue_item is None:
                return {'item_uuid': None}
            else:
                kwargs = {
                    'queue_item_uuid': queue_item.queue_uuid,
                    '_external': True
                }
                response = {
                    'item_uuid': queue_item.queue_uuid,
                    'lock_expiry': queue_item.locked_since + datetime.timedelta(seconds=DB_LOCK_TIME),
                    'app_id': self._get_serializer().dumps(app_id, 'queue_app_id'),
                    'data': queue_item.data,
                    'actions': {
                        'renew': flask.url_for('cnodc.renew_queue_lock', **kwargs),
                        'release': flask.url_for('cnodc.release_queue_item', **kwargs),
                    }
                }
                if f'fail_{queue_name}' in access_perms:
                    response['actions']['fail'] = flask.url_for('cnodc.complete_queue_item', **kwargs)
                if f'complete_{queue_name}' in access_perms:
                    response['actions']['complete'] = flask.url_for('cnodc.fail_queue_item', **kwargs)
                if 'metadata' in queue_item.data:
                    if f'escalate_{queue_name}' in access_perms and 'escalation-queue' in queue_item.data['metadata']:
                        esc_queue = queue_item.data['metadata']['escalation-queue'] or ''
                        if esc_queue and esc_queue != queue_name:
                            response['actions']['escalate'] = flask.url_for('cnodc.escalate_queue_item', **kwargs)
                    if f'descalate_{queue_name}' in access_perms and 'descalation-queue' in queue_item.data['metadata']:
                        desc_queue = queue_item.data['metadata']['descalation-queue'] or ''
                        if desc_queue and desc_queue != queue_name:
                            response['actions']['descalate'] = flask.url_for('cnodc.descalate_queue_item', **kwargs)
                if 'batch_info' in queue_item.data:
                    response['actions']['download_working'] = flask.url_for('cnodc.download_batch', **kwargs)
                    response['actions']['apply_working'] = flask.url_for('cnodc.apply_changes', **kwargs)
                    if f'clear_actions_{queue_name}' in access_perms:
                        response['actions']['clear_actions'] = flask.url_for('cnodc.reset_actions', **kwargs)
                elif 'source_info' in queue_item.data:
                    if f'retry_download_{queue_name}' in access_perms:
                        response['actions']['retry_decode'] = flask.url_for('cnodc.retry_decode', **kwargs)
                return response

    def retry_decode(self,
                        item_uuid: str,
                        enc_app_id: str):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id, 'retry_decode')
            payload = WorkflowPayload.build(queue_item)
            payload.enqueue_followup(db)
            db.commit()
            return {
                'success': True
            }

    def renew_queue_item_lock(self,
                              item_uuid: str,
                              enc_app_id: str):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id, 'handle')
            queue_item.renew(db)
            db.commit()
            return {
                'lock_expiry': queue_item.locked_since + datetime.timedelta(seconds=DB_LOCK_TIME),
            }

    def release_queue_item_lock(self,
                                item_uuid: str,
                                enc_app_id: str,
                                delay: t.Optional[int] = 0):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id, 'handle')
            queue_item.release(db, delay)
            db.commit()
            return {
                'success': True
            }

    def escalate_queue_item(self,
                            item_uuid: str,
                            enc_app_id: str):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id, 'escalate')
            queue_item.release(db, escalation_level=1)
            db.commit()
            return {
                'success': True
            }

    def descalate_queue_item(self,
                            item_uuid: str,
                            enc_app_id: str):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id, 'descalate')
            queue_item.release(db, escalation_level=0)
            db.commit()
            return {
                'success': True
            }

    def mark_queue_item_failed(self,
                          item_uuid: str,
                          enc_app_id: str):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id, 'fail')
            queue_item.mark_failed(db)
            db.commit()
            return {
                'success': True
            }

    def reset_actions(self,
                               item_uuid: str,
                               enc_app_id: str):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id, 'clear_actions')
            if 'batch_info' in queue_item.data:
                batch: structures.NODBBatch = structures.NODBBatch.find_by_uuid(db, queue_item.data['batch_info']['uuid'])
                for wr in batch.stream_working_records(db, lock_type=LockType.FOR_NO_KEY_UPDATE):
                    if not isinstance(wr.qc_metadata, dict):
                        continue
                    save = False
                    if 'actions' in wr.qc_metadata:
                        del wr.qc_metadata['actions']
                        save = True
                    if 'action_hash' in wr.qc_metadata:
                        del wr.qc_metadata['action_hash']
                        save = True
                    if save:
                        wr.mark_modified('qc_metadata')
                        db.update_object(wr)
            db.commit()
            return {'success': True}

    def stream_batch_working_records(self,
                                     item_uuid: str,
                                     enc_app_id: str) -> t.Iterable[bytes]:
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id, 'handle')
            batch: structures.NODBBatch = structures.NODBBatch.find_by_uuid(db, queue_item.data['batch_info']['uuid'])
            if batch is None:
                raise ValueError('invalid batch')
            batch_size = structures.NODBBatch.count_working_by_uuid(db, queue_item.data['batch_info']['uuid'])
            codec = OCProc2BinCodec()
            yield vlq_encode(batch_size)
            for wr in batch.stream_working_records(db):
                yield vlq_encode(len(wr.working_uuid))
                yield wr.working_uuid.encode('ascii')
                record = wr.record
                hash_code = record.generate_hash()
                yield vlq_encode(len(hash_code))
                yield hash_code.encode('ascii')
                actions = wr.get_metadata('actions', None)
                if actions:
                    self._apply_all_actions(record, actions)
                data = b''.join(codec.encode_records(
                    [record],
                    codec='JSON',
                    compression='LZMA6CRC4'
                ))
                yield vlq_encode(len(data))
                yield data

    def _apply_all_actions(self, record: ocproc2.DataRecord, actions: list[dict]):
        for action_def in actions:
            action = QCOperator.from_map(action_def)
            action.apply(record)

    def create_station(self, station_def: dict):
        if not isinstance(station_def, dict):
            raise ValueError('invalid station definition')
        # TODO: station validation
        # TODO: check for conflicts with existing station identifiers
        with self.nodb as db:
            station = structures.NODBStation(**station_def)
            db.insert_object(station)
            db.commit()
            return {
                'success': True,
                'station_uuid': station.station_uuid
            }

    def list_stations(self):
        with self.nodb as db:
            for station_raw in structures.NODBStation.find_all_raw(db):
                yield json.dumps(clean_for_json(station_raw))

    def save_updates(self,
                     item_uuid: str,
                     enc_app_id: str,
                     update_json: dict[str, dict]):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id, 'handle')
            batch: structures.NODBBatch = structures.NODBBatch.find_by_uuid(db, queue_item.data['batch_info']['uuid'])
            if batch is None:
                raise ValueError('invalid batch')
            results = {}
            for wr_uuid in update_json:
                working_record: structures.NODBWorkingRecord = structures.NODBWorkingRecord.find_by_uuid(
                    db=db,
                    obs_uuid=wr_uuid,
                    lock_type=LockType.FOR_NO_KEY_UPDATE
                )
                if working_record is None:
                    results[wr_uuid] = (False, "no such record")
                    continue
                if working_record.qc_batch_id != batch.batch_uuid:
                    results[wr_uuid] = (False, "not assigned to this batch")
                    continue
                record_hash = working_record.record.generate_hash()
                if update_json[wr_uuid]['hash'] != record_hash:
                    results[wr_uuid] = (False, 'invalid hash')
                metadata = {} if working_record.qc_metadata is None else working_record.qc_metadata
                if 'actions' not in metadata:
                    metadata['actions'] = []
                if 'action_hash' not in metadata:
                    metadata['action_hash'] = record_hash
                metadata['actions'].extend(update_json[wr_uuid]['actions'])
                working_record.qc_metadata = metadata
                db.update_object(working_record)
            db.commit()
            return results

    def mark_queue_item_complete(self,
                                 item_uuid: str,
                                 enc_app_id: str,
                                 recheck: bool = False):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id, 'complete')
            if 'batch_info' in queue_item.data:
                batch: structures.NODBBatch = structures.NODBBatch.find_by_uuid(db, queue_item.data['batch_info']['uuid'])
                for wr in batch.stream_working_records(db, lock_type=LockType.FOR_NO_KEY_UPDATE):
                    actions = wr.get_metadata('actions', [])
                    if actions:
                        if 'action_hash' not in wr.qc_metadata:
                            raise ValueError('missing action hash')
                        record = wr.record
                        if wr.get('action_hash', '') != record.generate_hash():
                            raise ValueError('invalid hash')
                        self._apply_all_actions(record, actions)
                        wr.record = record
                        del wr.qc_metadata['actions']
                        wr.mark_modified('qc_metadata')
                        db.update_object(wr)
            queue_item.mark_complete(db)
            payload = WorkflowPayload.build(queue_item)
            payload.increment_priority()
            if recheck and payload.get_metadata('recheck-queue', None) is not None:
                payload.set_followup_queue(payload.get_metadata('recheck-queue'))
            payload.enqueue_followup(db)
            db.commit()
            return {
                'success': True
            }

    def _load_queue_item(self, db, item_uuid: str, enc_app_id: str, perm_prefix: str) -> structures.NODBQueueItem:
        queue_item = db.load_queue_item(item_uuid)
        if queue_item is None:
            raise CNODCError('Invalid queue item ID', 'NODBWEB', 1001)
        if queue_item.status != structures.QueueStatus.LOCKED:
            raise CNODCError('Invalid queue state', 'NODBWEB', 1002)
        perms = self.login.current_permissions()
        if f'{perm_prefix}_{queue_item.queue_name}' not in perms:
            raise CNODCError('Insufficient permissions', 'NODBWEB', 1005)
        app_id = self._get_serializer().loads(enc_app_id, 'queue_app_id')
        if queue_item.locked_by != app_id:
            raise CNODCError('Invalid user ID', 'NODBWEB', 1003)
        return queue_item
