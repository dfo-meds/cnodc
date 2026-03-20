from __future__ import annotations
import datetime
import typing as t
import enum
import cnodc.nodb.base as s
import cnodc.util.awaretime as awaretime

if t.TYPE_CHECKING:  # pragma: no coverage
    from cnodc.nodb import NODBControllerInstance

class QueueStatus(enum.Enum):
    """Status of a queue item in the database."""

    UNLOCKED = 'UNLOCKED'
    LOCKED = 'LOCKED'
    COMPLETE = 'COMPLETE'
    DELAYED_RELEASE = 'DELAYED_RELEASE'
    ERROR = 'ERROR'


class NODBQueueItem(s.NODBBaseObject):
    """Queue item in the database."""

    TABLE_NAME: str = "nodb_queues"
    PRIMARY_KEYS: tuple[str] = ("queue_uuid",)

    queue_uuid: str = s.UUIDColumn("queue_uuid")
    created_date: datetime.datetime = s.DateTimeColumn("created_date", readonly=True)
    modified_date: datetime.datetime = s.DateTimeColumn("modified_date", readonly=True)
    delay_release: datetime.datetime = s.DateTimeColumn("delay_release", readonly=True)
    status: QueueStatus = s.EnumColumn("status", QueueStatus)
    locked_by: t.Optional[str] = s.StringColumn("locked_by", readonly=True)
    locked_since: t.Optional[datetime.datetime] = s.DateTimeColumn("locked_since", readonly=True)
    queue_name: str = s.StringColumn("queue_name", readonly=True)
    escalation_level: int = s.IntColumn("escalation_level")
    subqueue_name: str = s.StringColumn("subqueue_name", readonly=True)
    unique_item_name: t.Optional[str] = s.StringColumn("unique_item_name", readonly=True)
    priority: t.Optional[int] = s.IntColumn('priority', readonly=True)
    data: dict = s.JsonColumn("data")

    @classmethod
    def get_str_keys(cls):
        return ['queue_name', 'queue_uuid', 'status']

    def mark_complete(self, db: NODBControllerInstance):
        """Mark the queue item as complete."""
        self._set_queue_status(db=db, new_status=QueueStatus.COMPLETE)

    def mark_failed(self, db: NODBControllerInstance):
        """Mark the queue item as failed."""
        self._set_queue_status(db=db, new_status=QueueStatus.ERROR)

    def release(self,
                db: NODBControllerInstance,
                release_in_seconds: t.Optional[int] = None,
                **kwargs):
        """Release the queue item, optionally delaying for a number of seconds."""
        if release_in_seconds is None or release_in_seconds <= 0:
            self._set_queue_status(db=db, new_status=QueueStatus.UNLOCKED, **kwargs)
        else:
            kwargs.pop('release_at', '')
            self._set_queue_status(
                db=db,
                new_status=QueueStatus.DELAYED_RELEASE,
                release_at=awaretime.utc_now() + datetime.timedelta(seconds=release_in_seconds),
                **kwargs
            )

    def renew(self, db: NODBControllerInstance):
        """Renew a lock on the queue item"""
        if self.status == QueueStatus.LOCKED:
            self._allow_set_readonly = True
            self.locked_since = db.fast_renew_queue_item(self.queue_uuid)
            self._allow_set_readonly = False

    def _set_queue_status(self,
                          db: NODBControllerInstance,
                          new_status: QueueStatus,
                          release_at: t.Optional[datetime.datetime] = None,
                          reduce_priority: bool = False,
                          escalation_level: t.Optional[int] = None):
        """Set the queue status from LOCKED to a new status."""
        if self.status == QueueStatus.LOCKED:
            db.fast_update_queue_status(
                self.queue_uuid,
                new_status,
                release_at,
                reduce_priority,
                escalation_level if escalation_level is not None else (self.escalation_level or 0)
            )
            with self._readonly_access():
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
