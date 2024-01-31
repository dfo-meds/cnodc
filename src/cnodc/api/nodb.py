import datetime
import typing as t

import flask
from autoinject import injector
from cnodc.nodb import NODBController
from cnodc.util import CNODCError
import uuid
import cnodc.nodb.structures as structures
import threading
import itsdangerous

DB_LOCK_TIME = 3600  # in seconds


@injector.injectable
class NODBWebController:

    nodb: NODBController = None

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
                            user_id: str,
                            subqueue_name: str):
        app_id = f"{user_id}.{uuid.uuid4()}"
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
                }
                return {
                    'item_uuid': queue_item.queue_uuid,
                    'lock_expiry': queue_item.locked_since + datetime.timedelta(seconds=DB_LOCK_TIME),
                    'app_id': self._get_serializer().dumps(app_id, 'queue_app_id'),
                    'actions': {
                        'renew': flask.url_for('cnodc.renew_queue_lock', **kwargs),
                        'release': flask.url_for('cnodc.release_queue_item', **kwargs),
                        'fail': flask.url_for('cnodc.complete_queue_item', **kwargs),
                        'complete': flask.url_for('cnodc.fail_queue_item', **kwargs),
                    }

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

    def mark_queue_item_complete(self,
                          item_uuid: str,
                          enc_app_id: str):
        with self.nodb as db:
            queue_item = self._load_queue_item(db, item_uuid, enc_app_id)
            queue_item.mark_complete(db)
            db.commit()
            return {
                'success': True
            }

    def _load_queue_item(self, db, item_uuid: str, enc_app_id: str) -> structures.NODBQueueItem:
        queue_item = db.load_queue_item(item_uuid)
        if queue_item is None:
            raise CNODCError('Invalid queue item ID', 'NODBWEB', 1001)
        if queue_item.status != structures.QueueStatus.LOCKED:
            raise CNODCError('Invalid queue state', 'NODBWEB', 1003)
        app_id = self._get_serializer().loads(enc_app_id, 'queue_app_id')
        if queue_item.locked_by != app_id:
            raise CNODCError('Invalid user ID', 'NODBWEB', 1002)
        return queue_item

