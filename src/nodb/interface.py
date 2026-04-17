import collections
import datetime
import decimal
import enum
import functools
import typing as t
import uuid
from contextlib import contextmanager

import psycopg2 as pg
import psycopg2.sql as pgs
import medsutil.types as ct
from autoinject import injector

from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError

POSTGRES_ALLOWED_CHARACTERS = 'abcdefghijklmnopqrstuvwxyz0123456789_'

type SupportsPostgres = None | bool | float | int | decimal.Decimal | str | collections.abc.Buffer | datetime.date | datetime.time | datetime.timedelta | uuid.UUID
type DatabaseIdentifier = str
type FilterDict = dict[str, SupportsPostgres | tuple[str, SupportsPostgres] | tuple[str, SupportsPostgres | bool]]
type JoinString = t.Literal["AND", "OR"]

if t.TYPE_CHECKING:
    from nodb import NODBQueueItem

RECOVERABLE_ERRORS: list[str] = [
    '08***',  # Connection errors
    '25***',  # transaction states
    '28***',  # Authorization errors
    '40***',  # Transaction isolation errors
    '53***',  # Insufficient resource errors
    '58***',  # Non-postgresql errors
    '57***',  # Operator intervention
    '55***',  # Invalid prerequisite state
    '42501',  # Insufficient privileges
]

RECOVERABLE_MESSAGE_FRAGMENTS: list[str] = [
    'server closed the connection unexpectedly',
    'could not receive data from server',
    'the database system is starting up',
]


class ScannedFileStatus(enum.Enum):
    """The status of a scanned file"""

    NOT_PRESENT = "0"
    UNPROCESSED = "1"
    PROCESSED = "2"
    ERRORED = "3"


class QueueStatus(enum.Enum):
    """Status of a queue item in the database."""

    UNLOCKED = 'UNLOCKED'
    LOCKED = 'LOCKED'
    COMPLETE = 'COMPLETE'
    DELAYED_RELEASE = 'DELAYED_RELEASE'
    ERROR = 'ERROR'


class LockType(enum.Enum):
    """The type of lock to take on the row when performing a select."""

    NONE = "1"
    FOR_UPDATE = "2"
    FOR_NO_KEY_UPDATE = "3"
    FOR_SHARE = "4"
    FOR_KEY_SHARE = "5"


class SqlState(enum.Enum):
    """Specific postgresql SQL state codes that are used to properly handle errors."""

    UNIQUE_VIOLATION = '23505'
    FOREIGN_KEY_VIOLATION = '23503'
    NOT_NULL_VIOLATION = '23502'

    SERIALIZATION_FAILURE = '40001'
    DEADLOCK_DETECTED = '40P01'


class NODBError(CodedError):
    """Wrapper for NODB-related errors."""
    CODE_SPACE = 'NODB'

    def __init__(self, msg, code, pgcode: str | None, is_transient: bool = False):
        ist = is_transient
        super().__init__(
            f"Database error: {msg} [{pgcode or ''}]",
            code,
            is_transient=(
                    ist
                    or (pgcode is not None and (pgcode in RECOVERABLE_ERRORS or f"{pgcode[0:2]}***" in RECOVERABLE_ERRORS))
                    or (msg is not None and any(x in msg for x in RECOVERABLE_MESSAGE_FRAGMENTS))
            )
        )
        self.pgcode = pgcode

    def is_serialization_error(self):
        return self.pgcode in (
            SqlState.UNIQUE_VIOLATION.value,
            SqlState.SERIALIZATION_FAILURE.value,
            SqlState.DEADLOCK_DETECTED.value
        )

    def sql_state(self) -> t.Optional[SqlState]:
        try:
            return SqlState(self.pgcode)
        except ValueError:
            return None

class NODBValidationError(CodedError): CODE_SPACE = 'NODB-VALIDATION'

def wrap_nodb_exceptions(cb: t.Callable):
    """Wrap postgres errors into NODB errors."""

    @functools.wraps(cb)
    def _inner(*args, **kwargs):
        try:
            return cb(*args, **kwargs)
        except pg.Error as ex:
            raise NODBError(f"{ex.__class__.__name__}: {str(ex)} [{ex.pgcode}]", 1000, ex.pgcode) from ex
    return _inner


class NODBCursor(t.Protocol):

    def execute(self, query: str | pgs.Composable, args: t.Iterable[SupportsPostgres] | t.Mapping[str, SupportsPostgres]): ...
    def fetchone(self) -> dict: ...
    def fetchmany(self, size: int = None) -> t.Iterable[dict]: ...
    def fetch_stream(self, size: int = None) -> t.Iterable[dict]: ...
    def commit(self): ...
    def rollback(self): ...
    def create_savepoint(self, name: str) -> str: ...
    def rollback_to_savepoint(self, name: str): ...
    def release_savepoint(self, name: str): ...


class NODBObject(t.Protocol):

    def __init__(self, *, is_new: bool = False, **kwargs): ...

    @classmethod
    def get_table_name(cls) -> DatabaseIdentifier: ...

    @classmethod
    def get_mock_index_keys(cls) -> list[list[str]]: ...

    @classmethod
    def get_primary_keys(cls) -> t.Sequence[str]: ...

    @classmethod
    def find_all[X](cls: X, db: NODBInstance) -> t.Iterable[X]: ...

    @property
    def is_new(self) -> bool: ...

    @is_new.setter
    def is_new(self, b: bool): ...

    @property
    def modified_values(self) -> set[str]: ...

    def mark_modified(self, item: str): ...
    def clear_modified(self): ...

    def get_for_db(self, item: str) -> t.Any: ...
    def set_from_db(self, item: str, value: t.Any): ...

    @contextmanager
    def readonly_access(self) -> t.Generator[t.Self, t.Any, None]: ...

ConcreteNODBObject = t.TypeVar("ConcreteNODBObject", bound=NODBObject)
NODBObjectType = type[ConcreteNODBObject]

class NODBInstance(t.Protocol):

    @contextmanager
    def cursor(self) -> t.Generator[NODBCursor, t.Any, None]: ...

    def commit(self): ...
    def rollback(self): ...
    def close(self): ...

    def create_savepoint(self, name: DatabaseIdentifier): ...
    def rollback_to_savepoint(self, name: DatabaseIdentifier): ...
    def release_savepoint(self, name: DatabaseIdentifier): ...

    def rows(self, table_name: DatabaseIdentifier) -> int: ...

    def count_objects(self,
                      obj_cls: NODBObjectType,
                      filters: FilterDict = None,
                      join_str: JoinString = None) -> int: ...
    def upsert_object(self, obj: ConcreteNODBObject) -> bool: ...
    def update_object(self, obj: ConcreteNODBObject) -> bool: ...
    def insert_object(self, obj: ConcreteNODBObject) -> bool: ...
    def delete_object(self, obj: ConcreteNODBObject): ...
    def load_object(self,
                    obj_cls: type[ConcreteNODBObject],
                    filters: FilterDict,
                    join_str: JoinString = None,
                    lock_type: LockType = None,
                    limit_fields: list[str] = None,
                    key_only: bool = False) -> ConcreteNODBObject | None: ...
    def stream_objects(self,
                       obj_cls: type[ConcreteNODBObject],
                       filters: FilterDict = None,
                       join_str: JoinString = None,
                       lock_type: LockType = LockType.NONE,
                       limit_fields: list[str] = None,
                       key_only: bool = False,
                       order_by: list[str] = None) -> t.Iterable[ConcreteNODBObject]: ...
    def stream_raw(self,
                       obj_cls: NODBObjectType,
                       filters: FilterDict = None,
                       join_str: JoinString = None,
                       lock_type: LockType = LockType.NONE,
                       limit_fields: list[str] = None,
                       key_only: bool = False,
                       order_by: list[str] = None) -> t.Iterable[dict[str, SupportsPostgres]]: ...
    def bulk_update_objects(self,
                            obj_cls: NODBObjectType,
                            updates: dict[str, SupportsPostgres],
                            key_field: str,
                            key_values: list[SupportsPostgres]): ...

    def fast_renew_queue_item(self, queue_uuid: str, now_: AwareDateTime | None = None) -> AwareDateTime: ...
    def fast_update_queue_status(self,
                                 queue_uuid: str,
                                 new_status: QueueStatus,
                                 release_at: datetime.datetime | None = None,
                                 reduce_priority: bool = False,
                                 escalation_level: int = 0): ...
    def create_queue_item(self,
                          queue_name: str,
                          data: dict[str, ct.SupportsExtendedJson],
                          priority: int | None = None,
                          unique_item_name: str | None = None,
                          subqueue_name: str | None = None,
                          correlation_id: str | None = None) -> str: ...
    def fetch_next_queue_item(self, queue_name: str, app_id: str, subqueue_name: str | None = None, retries: int = 1) -> NODBQueueItem | None: ...
    def load_queue_items(self) -> t.Iterable[tuple[str, str, str]]: ...

    def grant_permission(self, role_name: str, permission_name: str): ...
    def remove_permission(self, role_name: str, permission_name: str): ...
    def load_permissions(self, roles: t.Iterable[str]) -> set[str]: ...

    def delete_session(self, session_id: str): ...

    def record_login(self, username: str, ip_address: str | None, instance_name: str): ...

    def scanned_file_status(self, file_path: str, mod_time: datetime.datetime | None = None) -> ScannedFileStatus: ...
    def note_scanned_file(self, file_path: str, mod_time: datetime.datetime | None = None): ...
    def mark_scanned_item_failed(self, file_path: str, mod_time: datetime.datetime | None = None): ...
    def mark_scanned_item_success(self, file_path: str, mod_time: datetime.datetime | None = None): ...


@injector.injectable_global
class NODB[X: NODBInstance]:

    def __init__(self):
        self._instance: t.Optional[X] = None
        self._inner_count = 0
        self._stable_sort = False

    def __enter__(self) -> X:
        if self._instance is None:
            self._instance = self._build_controller_instance()
            self._instance._stable_sort_columns = self._stable_sort
        self._inner_count += 1
        return t.cast(NODBInstance, self._instance)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._inner_count -= 1
        if self._inner_count == 0 and self._instance is not None:
            if exc_type is not None:
                self._instance.rollback()
            else:
                self._instance.commit()
            self._instance.close()
            self._instance = None

    def _build_controller_instance(self) -> X:
        raise NotImplementedError  # pragma: no coverage
