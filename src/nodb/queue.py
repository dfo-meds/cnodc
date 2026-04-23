import datetime
import typing as t
import enum

import zrlog

import medsutil.json as json
from medsutil.awaretime import AwareDateTime

import nodb.base as s
import nodb.interface as interface

_log = zrlog.get_logger('nodb.queue')

class NODBQueueItem(s.NODBBaseObject):
    """Queue item in the database."""

    TABLE_NAME: str = "nodb_queues"
    PRIMARY_KEYS: tuple[str] = ("queue_uuid",)
    MOCK_INDEX_KEYS = (
        ('queue_name',)
    )

    queue_uuid: str = s.UUIDColumn()
    created_date: AwareDateTime = s.DateTimeColumn(readonly=True, managed_name='db_created_date')
    modified_date: AwareDateTime = s.DateTimeColumn(readonly=True, managed_name='db_modified_date')
    delay_release: AwareDateTime | None = s.DateTimeColumn(readonly=True)
    status: interface.QueueStatus = s.EnumColumn(interface.QueueStatus)
    locked_by: str | None = s.StringColumn(readonly=True)
    locked_since: AwareDateTime | None = s.DateTimeColumn(readonly=True)
    queue_name: str = s.StringColumn(readonly=True)
    escalation_level: int | None = s.IntColumn()
    subqueue_name: str | None = s.StringColumn(readonly=True)
    unique_item_name: str | None = s.StringColumn(readonly=True)
    priority: int = s.IntColumn(readonly=True, default=0)
    correlation_id: str = s.UUIDColumn()
    data: dict = s.JsonDictColumn()

    def get_worker_config(self, process_name: str, process_version: str) -> dict[str, t.Any]:
        worker_config = {}
        if 'worker_config' not in self.data:
            _log.trace(f'Queue item [%s] has no worker_config available', self.queue_uuid)
            return {}
        elif not isinstance(self.data['worker_config'], (dict, str)):
            _log.trace(f'Queue item [%s] has worker_config available, but is not a dict or str', self.queue_uuid)
            return {}
        else:
            all_worker_config: dict[str, t.Any]= self.data['worker_config'] if isinstance(self.data['worker_config'], dict) else json.load_dict(self.data['worker_config'])
            for key in (process_name, f'{process_name}_{process_version}'):
                if key not in all_worker_config:
                    _log.trace(f'Queue item [%s] worker_config[%s] is not present', self.queue_uuid, key)
                elif not isinstance(all_worker_config[key], dict):
                    _log.trace(f'Queue item [%s] worker_config[%s] is not a dict', self.queue_uuid, key)
                else:
                    _log.trace(f'Queue item [%s] updating worker_config from [%s]', self.queue_uuid, key)
                    worker_config.update(all_worker_config[key])
        return worker_config

    def mark_complete(self, db: interface.NODBInstance):
        """Mark the queue item as complete."""
        self._set_queue_status(db=db, new_status=interface.QueueStatus.COMPLETE)

    def mark_failed(self, db: interface.NODBInstance):
        """Mark the queue item as failed."""
        self._set_queue_status(db=db, new_status=interface.QueueStatus.ERROR)

    def release(self,
                db: interface.NODBInstance,
                release_in_seconds: t.Optional[int] = None,
                reduce_priority: bool = False,
                escalation_level: t.Optional[int] = None):
        """Release the queue item, optionally delaying for a number of seconds."""
        if release_in_seconds is None or release_in_seconds <= 0:
            self._set_queue_status(
                db=db,
                new_status=interface.QueueStatus.UNLOCKED,
                reduce_priority=reduce_priority,
                escalation_level=escalation_level
            )
        else:
            self._set_queue_status(
                db=db,
                new_status=interface.QueueStatus.DELAYED_RELEASE,
                release_at=AwareDateTime.now() + datetime.timedelta(seconds=release_in_seconds),
                reduce_priority=reduce_priority,
                escalation_level=escalation_level
            )

    def renew(self, db: interface.NODBInstance):
        """Renew a lock on the queue item"""
        if self.status == interface.QueueStatus.LOCKED:
            with self.readonly_access():
                self.locked_since = db.fast_renew_queue_item(self.queue_uuid)

    def _set_queue_status(self,
                          db: interface.NODBInstance,
                          new_status: interface.QueueStatus,
                          release_at: AwareDateTime | None = None,
                          reduce_priority: bool = False,
                          escalation_level: t.Optional[int] = None):
        """Set the queue status from LOCKED to a new status."""
        if self.status == interface.QueueStatus.LOCKED:
            db.fast_update_queue_status(
                self.queue_uuid,
                new_status,
                release_at,
                reduce_priority,
                escalation_level if escalation_level is not None else (self.escalation_level or 0)
            )
            with self.readonly_access():
                self.status = new_status
                self.priority = self.priority + (1 if reduce_priority else 0)
                self.locked_by = None
                self.locked_since = None
                if escalation_level is not None:
                    self.escalation_level = escalation_level
                self.delay_release = release_at

    @classmethod
    def find_by_uuid(cls, db, uuid: str, **kwargs):
        return db.load_object(cls, {
            'queue_uuid': uuid
        }, **kwargs)
