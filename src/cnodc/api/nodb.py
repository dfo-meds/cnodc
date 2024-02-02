import datetime
import json
import typing as t

import flask
from autoinject import injector

from cnodc.api.auth import LoginController
from cnodc.codecs import OCProc2BinCodec
from cnodc.nodb import NODBController, LockType
from cnodc.ocproc2.operations import QCOperator
from cnodc.util import CNODCError, clean_for_json
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
        app_id = f"{self.login.current_user()}.{uuid.uuid4()}"
        with self.nodb as db:
            queue_item = db.fetch_next_queue_item(
                queue_name=queue_name,
                app_id=app_id,
                subqueue_name=subqueue_name
            )
            if queue_item is None:
                return {'item_uuid': None, 'lock_expiry': None, 'actions': {}}
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
                        'fail': flask.url_for('cnodc.complete_queue_item', **kwargs),
                        'complete': flask.url_for('cnodc.fail_queue_item', **kwargs),
                    }
                }
                if 'batch_info' in queue_item.data:
                    response['actions']['download_working'] = flask.url_for('cnodc.download_batch', **kwargs)
                    response['actions']['apply_working'] = flask.url_for('cnodc.apply_changes', **kwargs)
                elif 'source_info' in queue_item.data:
                    response['actions']['retry_decode'] = flask.url_for('cnodc.retry_decode', **kwargs)
                return response

    def retry_decode(self,
                        item_uuid: str,
                        enc_app_id: str):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id)
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
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id)
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
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id)
            queue_item.release(db, delay)
            db.commit()
            return {
                'success': True
            }

    def mark_queue_item_failed(self,
                          item_uuid: str,
                          enc_app_id: str):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id)
            queue_item.mark_failed(db)
            db.commit()
            return {
                'success': True
            }

    def stream_batch_working_records(self,
                                     item_uuid: str,
                                     enc_app_id: str) -> t.Iterable[bytes]:
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id)
            batch: structures.NODBBatch = structures.NODBBatch.find_by_uuid(db, queue_item.data['batch_info']['uuid'])
            if batch is None:
                raise ValueError('invalid batch')
            codec = OCProc2BinCodec()
            yield from codec.encode_records(
                (wr.record for wr in batch.stream_working_records(db)),
                codec='JSON',
                compression='LZMA6CRC8'
            )

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

    def apply_updates(self,
                      item_uuid: str,
                      enc_app_id: str,
                      update_json: dict[str, dict]):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id)
            batch: structures.NODBBatch = structures.NODBBatch.find_by_uuid(db, queue_item.data['batch_info']['uuid'])
            if batch is None:
                raise ValueError('invalid batch')
            results = {}
            for wr_uuid in update_json:
                try:
                    self._apply_updates_to_working_record(
                        db,
                        wr_uuid,
                        batch.batch_uuid,
                        update_json[wr_uuid]['hash'],
                        update_json[wr_uuid]['actions']
                    )
                    results[wr_uuid] = (True, None)
                except Exception as ex:
                    results[wr_uuid] = (False, repr(ex))
            db.commit()
            return results

    def _apply_updates_to_working_record(self,
                                         db,
                                         record_uuid: str,
                                         batch_uuid: str,
                                         hash_check: str,
                                         update_list: list[dict]):
        working_record: structures.NODBWorkingRecord = structures.NODBWorkingRecord.find_by_uuid(
            db,
            record_uuid,
            lock_type=LockType.FOR_NO_KEY_UPDATE
        )
        if working_record is None:
            raise ValueError('missing record')
        if working_record.qc_batch_id != batch_uuid:
            raise ValueError('item is no longer assigned to the batch')
        data_record = working_record.record
        if data_record.generate_hash() != hash_check:
            raise ValueError('record has changed after export')
        for op_def in update_list:
            op = QCOperator.from_map(op_def)
            op.apply(data_record, working_record)
        working_record.record = data_record

    def mark_queue_item_complete(self,
                          item_uuid: str,
                          enc_app_id: str):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id)
            queue_item.mark_complete(db)
            payload = WorkflowPayload.build(queue_item)
            payload.increment_priority()
            payload.enqueue_followup(db)
            db.commit()
            return {
                'success': True
            }

    def _load_queue_item(self, db, item_uuid: str, enc_app_id: str) -> structures.NODBQueueItem:
        queue_item = db.load_queue_item(item_uuid)
        if queue_item is None:
            raise CNODCError('Invalid queue item ID', 'NODBWEB', 1001)
        if queue_item.status != structures.QueueStatus.LOCKED:
            raise CNODCError('Invalid queue state', 'NODBWEB', 1002)
        app_id = self._get_serializer().loads(enc_app_id, 'queue_app_id')
        if queue_item.locked_by != app_id:
            raise CNODCError('Invalid user ID', 'NODBWEB', 1003)
        return queue_item
