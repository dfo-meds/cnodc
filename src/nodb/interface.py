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


LOCK_EXPIRY_TIME = 3600
COMPLETED_QUEUE_ITEM_LIFETIME = 2592000
ERRORED_QUEUE_ITEM_LIFETIME = 2592000
PROCESS_EXPIRY_TIME = 86400

# These are non-pgcode errors from the psycopg2 library
RECOVERABLE_MESSAGE_FRAGMENTS: list[str] = [
    'server closed the connection unexpectedly',
    'could not receive data from server',
    'the database system is starting up',
    'connection refused',
    'the database system is not yet accepting connections',
    'connection already closed',
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


class EType(enum.IntFlag):
    NONE = 0
    CONNECTION = 1
    SERIALIZATION = 2
    RECOVERABLE = 4


class SqlState(enum.Enum):
    """Specific postgresql SQL state codes that are used to properly handle errors."""

    def __new__(cls, value: str, error_type: EType = EType.NONE):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.error_type = error_type
        return obj

    CONNECTION_ERROR = '08000', EType.CONNECTION | EType.RECOVERABLE
    CONNECTION_DOES_NOT_EXIST = '08003', EType.CONNECTION | EType.RECOVERABLE
    CONNECTION_FAILURE = '08006', EType.CONNECTION | EType.RECOVERABLE
    SQLCLIENT_NO_CONNECTION = '08001', EType.CONNECTION | EType.RECOVERABLE
    SQLSERVER_REJECTED_CONNECTION = '08004', EType.CONNECTION | EType.RECOVERABLE
    TRANSACTION_RESOLUTION_UNKNOWN = '08007', EType.CONNECTION | EType.RECOVERABLE
    PROTOCOL_VIOLATION = '08P01', EType.CONNECTION | EType.RECOVERABLE

    UNIQUE_VIOLATION = '23505', EType.SERIALIZATION

    INVALID_TRANSACTION_STATE = '25000', EType.RECOVERABLE
    ACTIVE_SQL_TRANSACTION = '25001', EType.RECOVERABLE
    BRANCH_TRANSACTION_ALREADY_ACTIVE = '25002', EType.RECOVERABLE
    HELD_CURSOR_REQUIRES_SAME_ISOLATION = '25008', EType.RECOVERABLE
    INAPPROPRIATE_ACCESS_MODE = '25003', EType.RECOVERABLE
    INAPPROPRIATE_ISOLATION_MODE = '25004', EType.RECOVERABLE
    NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH = '25005', EType.RECOVERABLE
    READ_ONLY_SQL_TRANSACTION = '25006', EType.RECOVERABLE
    SCHEMA_DATA_MIXING_NOT_SUPPORTED = '25007', EType.RECOVERABLE
    NO_ACTIVE_SQL_TRANSACTION = '25P01', EType.RECOVERABLE
    IN_FAILED_SQL_TRANSACTION = '25P02', EType.RECOVERABLE
    IDLE_IN_TRANSACTION_TIMEOUT = '25P03', EType.RECOVERABLE
    TRANSACTION_TIMEOUT = '25P04', EType.RECOVERABLE

    INVALID_AUTHORIZATION_SPEC = '28000', EType.RECOVERABLE
    INVALID_PASSWORD = '28P01', EType.RECOVERABLE

    TRANSACTION_ROLLBACK = '40000', EType.RECOVERABLE
    SERIALIZATION_FAILURE = '40001', EType.SERIALIZATION | EType.RECOVERABLE
    TRANSACTION_INTEGRITY_CONSTRAINT_VIOLATION = '40002', EType.RECOVERABLE
    DEADLOCK_DETECTED = '40P01', EType.SERIALIZATION | EType.RECOVERABLE
    STATEMENT_COMPLETION_UNKNOWN = '40003', EType.RECOVERABLE

    INSUFFICIENT_PRIVILEGES = '42501', EType.RECOVERABLE

    INSUFFICIENT_RESOURCES = '53000', EType.RECOVERABLE
    DISK_FULL = '53100', EType.RECOVERABLE
    OUT_OF_MEMORY = '53200', EType.RECOVERABLE
    TOO_MANY_CONNECTIONS = '53300', EType.RECOVERABLE
    CONFIGURATION_LIMIT_EXCEEDED = '53400', EType.RECOVERABLE

    OBJECT_NOT_IN_PREREQ_STATE = '55000', EType.RECOVERABLE
    OBJECT_IN_USE = '55006', EType.RECOVERABLE
    CANT_CHANGE_RUNTIME_PARAM = '55P02', EType.RECOVERABLE
    LOCK_NOT_AVAILABLE = '55P03', EType.RECOVERABLE
    UNSAFE_NEW_ENUM_USAGE = '55P04', EType.RECOVERABLE

    OP_INTERVENTION = '57000', EType.RECOVERABLE
    QUERY_CANCELLED = '57014', EType.RECOVERABLE
    ADMIN_SHUTDOWN = '57P01', EType.RECOVERABLE | EType.CONNECTION
    CRASH_SHUTDOWN = '57P02', EType.RECOVERABLE | EType.CONNECTION
    CANNOT_CONNECT_NOW = '57P03', EType.RECOVERABLE | EType.CONNECTION
    DATABASE_DROPPED = '57P04', EType.RECOVERABLE
    IDLE_SESSION_TIMEOUT = '57P05', EType.RECOVERABLE | EType.CONNECTION

    SYSTEM_ERROR = '58000', EType.RECOVERABLE
    IO_ERROR = '58030', EType.RECOVERABLE
    UNDEFINED_FILE = '58P01', EType.RECOVERABLE
    DUPLICATE_FILE = '58P02', EType.RECOVERABLE
    FILENAME_TOO_LONG = '58P03', EType.RECOVERABLE




class NODBError(CodedError):
    """Wrapper for NODB-related errors."""
    CODE_SPACE = 'NODB'

    def __init__(self, msg, code, pgcode: str | None, is_transient: bool = False):
        super().__init__(f"Database error: {msg} [{pgcode or ''}]", code)
        self._state = None
        self.pgcode = pgcode
        try:
            if self.pgcode:
                self._state = SqlState(self.pgcode)
        except ValueError:
            ...

        self._is_db_available = True
        self._is_retryable = False

        if msg and any(x in msg.lower() for x in RECOVERABLE_MESSAGE_FRAGMENTS):
            self._is_db_available = False
            self.is_transient = True

        if self._state:
            if EType.CONNECTION in self._state.error_type:
                self._is_db_available = False
            if EType.RECOVERABLE in self._state.error_type:
                self.is_transient = True
            if EType.SERIALIZATION in self._state.error_type:
                self._is_retryable = True

    @property
    def is_db_available(self) -> bool:
        return self._is_db_available

    @property
    def state(self) -> t.Optional[SqlState]:
        return self._state

    @property
    def is_retryable_error(self):
        return self._is_retryable


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

    def execute(self, query: str | pgs.Composable, args: t.Iterable[SupportsPostgres] | t.Mapping[str, SupportsPostgres] | None = None): ...
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


class PreparedStatementProtocol(t.Protocol):
    def execute(self, nodb_object: NODBObject): ...
    def __enter__(self): ...
    def __exit__(self, exc_type, exc_val, exc_tb): ...


class NODBInstance(t.Protocol):

    @contextmanager
    def cursor(self) -> t.Generator[NODBCursor, t.Any, None]: ...

    def commit(self): ...
    def rollback(self): ...
    def close(self): ...

    def create_savepoint(self, name: DatabaseIdentifier): ...
    def rollback_to_savepoint(self, name: DatabaseIdentifier): ...
    def release_savepoint(self, name: DatabaseIdentifier): ...

    def fetch_processes(self) -> t.Iterable[dict[str, t.Any]]: ...
    def clear_process_info(self, server_name: str, process_id: str): ...
    def clear_process_info_for_server(self, server_name: str): ...
    def upsert_process_info(self,
                            server_name: str,
                            process_id: str,
                            process_name: str,
                            process_version: str,
                            info: dict[str, t.Any]): ...

    def prepared_insert(self, object_type: NODBObjectType, name: str, data_map: dict[str, str]) -> PreparedStatementProtocol: ...

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

    def fetch_queue_summary(self) -> dict[str, dict[str, int]]: ...
    def fast_renew_queue_item(self, queue_uuid: str, now_: AwareDateTime | None = None) -> AwareDateTime: ...
    def fast_update_queue_status(self,
                                 queue_uuid: str,
                                 new_status: QueueStatus,
                                 release_at: datetime.datetime | None = None,
                                 reduce_priority: bool = False,
                                 escalation_level: int = 0,
                                 is_closed: bool = False) -> AwareDateTime | None: ...
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

    def run_maintenance(self,
                        lock_expiry_seconds: int = LOCK_EXPIRY_TIME,
                        completed_lifetime_seconds: int = COMPLETED_QUEUE_ITEM_LIFETIME,
                        error_lifetime_seconds: int = ERRORED_QUEUE_ITEM_LIFETIME,
                        process_expiry_seconds: int = PROCESS_EXPIRY_TIME): ...

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
