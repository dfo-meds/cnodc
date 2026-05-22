import datetime
import json
import typing as t

import flask
from autoinject import injector

from pipeman_web.auth import LoginController
from medsutil.ocproc2.codecs import OCProc2BinCodec
from nodb.observations import NODBBatch, NODBWorkingRecord, NODBPlatform
from nodb.interface import NODB, LockType, QueueStatus
from nodb.queue import NODBQueueItem
from nodb.workflow import NODBUploadWorkflow
from medsutil.ocproc2.operations import QCOperator
import medsutil.ocproc2 as ocproc2
from pipeman.exceptions import CNODCError
from medsutil.vlq import vlq_encode
import uuid
import threading
import itsdangerous
from medsutil.sanitize import coerce

from pipeman.processing.payloads import WorkflowPayload
import zirconium as zr
DB_LOCK_TIME = 3600  # in seconds


@injector.injectable
class NODBWebController:

    nodb: NODB = None
    login: LoginController = None
    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._serializer = None
        self._serializer_lock = threading.Lock()

    def access_list(self):
        access_perms = self.login.current_permissions()
        access_list = {
            'other': {
                'change_password': flask.url_for('cnodc.change_password', _external=True),
                'renew': flask.url_for('cnodc.renew', _external=True),
                'logout': flask.url_for('cnodc.logout', _external=True),
                'access': flask.url_for('cnodc.list_access', _external=True),
            },
            'service_queues': self._all_queue_services(access_perms),
            'workflows': self._all_workflows(access_perms)
        }
        if '__admin__' in access_perms or 'handle_nodb_station_failure' in access_perms:
            access_list['other'].update({
                'create_station': flask.url_for('cnodc.create_station', _external=True),
                'list_stations': flask.url_for('cnodc.list_stations', _external=True),
            })
        return access_list

    def _all_workflows(self, access_perms: set) -> dict:
        results = {}
        with self.nodb as db:
            for workflow in NODBUploadWorkflow.find_all(db):
                if workflow.check_access(access_perms):
                    results[workflow.workflow_name] = {
                        'url': flask.url_for('cnodc.submit_file', workflow_name=workflow.workflow_name, _external=True),
                        'name': workflow.configuration['label'] if 'label' in workflow.configuration else {}
                    }
        return results

    def _all_queue_services(self, access_perms: set) -> dict:
        results: dict[str, dict[str, t.Any]] = {}
        services = self.config.as_dict(('cnodc', 'queue_services'), {})
        for service_name in services:
            if '__admin__' in access_perms or f'handle_{services[service_name]["queue_name"]}' in access_perms:
                results[service_name]: dict[str, t.Any] = {
                    'url': flask.url_for('cnodc.next_queue_item', queue_service_name=service_name, _external=True),
                    'name': {
                        x: services[service_name][x]
                        for x in services[service_name]
                        if x not in ('permission', 'queue_name', 'subqueue_name')
                    }
                }
        return results

    def _get_serializer(self) -> itsdangerous.Serializer:
        if not flask.current_app.config.get('SECRET_KEY'):
            self._logger.error("Secret key is not defined properly")
            raise CNODCError('Missing secret key', 'NODBWEB', 1004)
        if self._serializer is None:
            with self._serializer_lock:
                if self._serializer is None:
                    self._serializer = itsdangerous.Serializer(flask.current_app.config['SECRET_KEY'])
        return self._serializer

    @staticmethod
    def _build_notes(d: dict[str, t.Any]) -> str:
        report = []
        if 'executions' in d:
            report.append(f'Number of Executions: {d['executions']}')
        if 'next_execution' in d:
            report.append(f'Next Execution Time: {d['next_execution']}')
        if 'items_processed' in d:
            report.append(f'Items Processed: {d["items_processed"]} [{d['items_success']} success; {d['items_error']} error; {d['items_retry']} retries]')
        if 'fetch_errors' in d:
            report.append(f'Fetch errors: {d['fetch_errors']}')
        if 'temp_free' in d:
            report.append(f'Temp Free Space: {d['temp_free'] / 1024 / 1024:.2f} MiB')
        if 'temp_total' in d:
            report.append(f'Temp Total Space: {d['temp_total'] / 1024 / 1024:.2f} MiB')
        return '<br />'.join(report)

    def workflow_report(self):
        s = "<html><head></head><body>"
        status_order = ('UNLOCKED', 'LOCKED', 'COMPLETE', 'DELAYED_RELEASE', 'ERROR')
        with self.nodb as db:
            for workflow in NODBUploadWorkflow.find_all(db):
                queue_info = db.fetch_queue_summary(workflow.workflow_name)
                s += f'<h2>{workflow.workflow_name}</h2>'
                s += '<table><thead><tr><th>Step Name</th><th>Unlocked</th><th>Locked</th><th>Complete</th><th>Pending Release</th><th>Errored</th></thead><tbody>'
                for step_name in workflow.configuration.ordered_steps():
                    step_info = workflow.configuration.steps[step_name]
                    s += f'<tr><th>{step_name}</th>'
                    for stat in status_order:
                        s += '<td>'
                        if step_info.name in queue_info and stat in queue_info[step_info.name]:
                            s += str(queue_info[step_info.name][stat])
                        else:
                            s += "0"
                        s += '</td>'
                    s += '</tr>'
                s += '</tbody></table>'
        s += "</body></html>"
        return s

    def status_report(self):
        map_: list[tuple[str, str | t.Callable[[dict[str, t.Any]], str]]] = [
            ('Process ID', 'process_id'),
            ('Server Name', 'server_name'),
            ('Status', 'status'),
            ('Activity', 'activity'),
            ('Start Time', lambda x: x['db_created_date'].strftime('%Y-%m-%d %H:%M:%S')),
            ('Report Time', lambda x: x['db_modified_date'].strftime('%Y-%m-%d %H:%M:%S')),
            ('CPU', lambda x: f"{x['cpu_percent']}%" if 'cpu_percent' in x else ''),
            ('CPU Time', lambda x: f"{x['cpu_user']} s / {x['cpu_system']} s" if 'cpu_user' in x and 'cpu_system' in x else ''),
            ('IO Wait', lambda x: f"{x['cpu_iowait']} s" if 'cpu_iowait' in x else ''),
            ('Memory', lambda x: f"{x['memory_total'] / 1024 / 1024:.2f} MiB" if 'memory_total' in x else ''),
            ('Notes', self._build_notes)
        ]
        s = """<html><head><style>
        body {
            font-family: Calibri;
            font-size: 11px;
        }
        table {
            width: 100%;
        }
        table td, table th {
            text-align: left;
            vertical-align: top;
            padding-left: 3px;
            padding-right: 3px;
            padding-top: 1px;
            padding-bottom: 1px;
        }
        </style></head><body><h1>Status Report</h1><h2>Registered Processes</h2><table><thead><tr>"""
        for header, _ in map_:
            s += f'<th>{header}</th>'
        s += '</tr></thead><tbody>'
        with self.nodb as db:
            for process in db.fetch_processes():
                if process['exited'] == 'Y':
                    continue
                s += '<tr>'
                for _, value_key in map_:
                    if isinstance(value_key, str):
                        s += f'<td>{process[value_key] if value_key in process else 'N/A'}</td>'
                    else:
                        s += f'<td>{value_key(process)}</td>'
                s += '</tr>'
            s += '</tbody></table><h2>Queue Items</h2><table><thead><tr><th>Queue Name</th>'
            status_order = ('UNLOCKED', 'LOCKED', 'COMPLETE', 'DELAYED_RELEASE', 'ERROR')
            for stat in status_order:
                s += f'<th>{stat.lower().replace('_', ' ').capitalize()}</th>'
            s += '</tr></thead><tbody>'
            queues = db.fetch_queue_summary()
            for queue_name in sorted(queues.keys()):
                s += f'<tr><th>{queue_name}</th>'
                for stat in status_order:
                    if stat in queues[queue_name]:
                        s += f'<td>{queues[queue_name][stat]}</td>'
                    else:
                        s += '<td>0</td>'
                s += '</tr>'
            s += '</tbody></table><h2>Workflows</h2><table><thead><tr><th>Workflow Name</th></tr>'

        s += '</body></html>'
        return s

    def get_next_queue_item(self,
                            service_name: str):
        services = self.config.as_dict(('cnodc', 'queue_services'), {})
        if service_name not in services:
            raise ValueError('invalid service name')
        service_config = services[service_name]
        queue_name = service_config['queue_name']
        access_perms = self.login.current_permissions()
        if f"handle_{queue_name}" not in access_perms:
            raise ValueError('cannot access this queue')
        app_id = f"{self.login.current_user()}.{uuid.uuid4()}"
        with self.nodb as db:
            kwargs = {
                'app_id': app_id,
                'queue_name': service_config['queue_name']
            }
            if 'subqueue_name' in service_config:
                kwargs['subqueue_name'] = service_config['subqueue_name']
            queue_item = db.fetch_next_queue_item(**kwargs)
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
                    if 'current-qc-tests' in queue_item.data['metadata']:
                        response['current_tests'] = queue_item.data['current_tests']
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
                    response['batch_size'] = NODBBatch.count_working_by_uuid(db, queue_item.data['batch_info']['uuid'])
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
            payload = WorkflowPayload.from_queue_item(queue_item)
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
                batch: NODBBatch = NODBBatch.find_by_uuid(db, queue_item.data['batch_info']['uuid'])
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
            batch: NODBBatch = NODBBatch.find_by_uuid(db, queue_item.data['batch_info']['uuid'])
            if batch is None:
                raise ValueError('invalid batch')
            codec = OCProc2BinCodec()
            for wr in batch.stream_working_records(db):
                yield vlq_encode(len(wr.working_uuid))
                yield wr.working_uuid.encode('ascii')
                record = wr.record
                hash_code = record.generate_hash()
                yield vlq_encode(len(hash_code))
                yield hash_code.encode('ascii')
                actions = wr.get_metadata('actions', None)
                if actions is not None:
                    content = json.dumps(actions)
                    yield vlq_encode(len(content))
                    yield actions.encode('utf-8')
                else:
                    yield vlq_encode(0)
                data = b''.join(codec.encode_records(
                    [record],
                    codec='JSON',
                    compression='LZMA6CRC4'
                ))
                yield vlq_encode(len(data))
                yield data

    def _apply_all_actions(self, record: ocproc2.ParentRecord, actions: list[dict]):
        for action_def in actions:
            action = QCOperator.from_map(action_def)
            action.apply(record)

    def create_station(self, station_def: dict):
        if not isinstance(station_def, dict):
            raise ValueError('invalid station definition')
        # TODO: station validation
        # TODO: check for conflicts with existing station identifiers
        with self.nodb as db:
            station = NODBPlatform(**station_def)
            db.insert_object(station)
            db.commit()
            return {
                'success': True,
                'station_uuid': station.station_uuid
            }

    def list_stations(self):
        with self.nodb as db:
            for station_raw in NODBPlatform.find_all_raw(db):
                yield coerce.as_json_safe(station_raw)

    def save_updates(self,
                     item_uuid: str,
                     enc_app_id: str,
                     update_json: dict[str, dict]):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id, 'handle')
            batch: NODBBatch = NODBBatch.find_by_uuid(db, queue_item.data['batch_info']['uuid'])
            if batch is None:
                raise ValueError('invalid batch')
            results = {}
            for wr_uuid in update_json:
                working_record: NODBWorkingRecord = NODBWorkingRecord.find_by_uuid(
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
                                 enc_app_id: str):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id, 'complete')
            if 'batch_info' in queue_item.data:
                batch: NODBBatch = NODBBatch.find_by_uuid(db, queue_item.data['batch_info']['uuid'])
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
            payload = WorkflowPayload.from_queue_item(queue_item)
            payload.enqueue(db)
            db.commit()
            return {
                'success': True
            }

    def _load_queue_item(self, db, item_uuid: str, enc_app_id: str, perm_prefix: str) -> NODBQueueItem:
        queue_item = NODBQueueItem.find_by_uuid(db, item_uuid)
        if queue_item is None:
            raise CNODCError('Invalid queue item ID', 'NODBWEB', 1001)
        if queue_item.status != QueueStatus.LOCKED:
            raise CNODCError('Invalid queue state', 'NODBWEB', 1002)
        perms = self.login.current_permissions()
        if f'{perm_prefix}_{queue_item.queue_name}' not in perms:
            raise CNODCError('Insufficient permissions', 'NODBWEB', 1005)
        app_id = self._get_serializer().loads(enc_app_id, 'queue_app_id')
        if queue_item.locked_by != app_id:
            raise CNODCError('Invalid user ID', 'NODBWEB', 1003)
        return queue_item
