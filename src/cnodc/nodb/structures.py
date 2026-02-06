
from __future__ import annotations
import datetime
import enum
import functools
import hashlib
import json
import typing as t
import secrets
import zrlog
from autoinject import injector
from cnodc.codecs.ocproc2bin import OCProc2BinCodec
import cnodc.ocproc2 as ocproc2
from cnodc.storage import StorageController, StorageTier
from cnodc.util import CNODCError, dynamic_object, DynamicObjectLoadError

if t.TYPE_CHECKING:
    from cnodc.nodb import NODBControllerInstance, LockType, NODBController
    import cnodc.storage.core


def parse_received_date(rdate: t.Union[str, datetime.date]) -> datetime.date:
    """Convert a date string or date object into a date object."""
    if isinstance(rdate, str):
        try:
            return datetime.date.fromisoformat(rdate)
        except (TypeError, ValueError) as ex:
            raise CNODCError(f"Invalid received date [{rdate}]", "NODB", 1000) from ex
    else:
        return rdate


class NODBValidationError(CNODCError):
    """Base exception for validation issues."""

    def __init__(self, *args, **kwargs):
        if len(args) > 1:
            kwargs['code_number'] = args[1]
        super().__init__(args[0], code_space="NODBV", **kwargs)


class UserStatus(enum.Enum):
    """Status of a user in the database."""

    ACTIVE = 'ACTIVE'
    INACTIVE = 'INACTIVE'


class SourceFileStatus(enum.Enum):
    """Status of a source file in the database."""

    NEW = 'NEW'
    QUEUED = 'QUEUED'
    IN_PROGRESS = 'IN_PROGRESS'
    ERROR = 'ERROR'
    COMPLETE = 'COMPLETE'


class ObservationStatus(enum.Enum):
    """Status of an archived observation in the database."""

    UNVERIFIED = 'UNVERIFIED'
    VERIFIED = 'VERIFIED'
    DISCARDED = 'DISCARDED'
    DUPLICATE = 'DUPLICATE'
    ARCHIVED = 'ARCHIVED'
    DUBIOUS = 'DUBIOUS'


class ObservationType(enum.Enum):
    """Type of observation (i.e. profile vs. surface vs. measurement at depth)."""

    SURFACE = 'SURFACE'
    AT_DEPTH = 'AT_DEPTH'
    PROFILE = 'PROFILE'
    OTHER = 'OTHER'


class BatchStatus(enum.Enum):
    """Status of a batch in the database."""

    NEW = 'NEW'
    QUEUED = 'QUEUED'
    IN_PROGRESS = 'IN_PROGRESS'
    MANUAL_REVIEW = 'MANUAL_REVIEW'
    COMPLETE = 'COMPLETE'
    ERRORED = 'ERRORED'


class PlatformStatus(enum.Enum):
    """Status of a platform in the database."""

    ACTIVE = 'ACTIVE'
    INCOMPLETE = 'INCOMPLETE'
    HISTORICAL = 'HISTORICAL'
    REMOVED = 'REMOVED'
    REPLACED = 'REPLACED'


class QueueStatus(enum.Enum):
    """Status of a queue item in the database."""

    UNLOCKED = 'UNLOCKED'
    LOCKED = 'LOCKED'
    COMPLETE = 'COMPLETE'
    DELAYED_RELEASE = 'DELAYED_RELEASE'
    ERROR = 'ERROR'


class ProcessingLevel(enum.Enum):
    """Processing level of a record in the database."""

    RAW = 'RAW'
    ADJUSTED = 'ADJUSTED'
    REAL_TIME = 'REAL_TIME'
    DELAYED_MODE = 'DELAYED_MODE'


class _NODBBaseObject:
    """Base class for all NODB objects.

        This provides a lot of tools for building database classes.
    """

    def __init__(self, *, is_new: bool = True, **kwargs):
        self._data = {}
        self.modified_values = set()
        self._allow_set_readonly = True
        self.is_new = is_new
        for x in kwargs:
            if hasattr(self, x):
                setattr(self, x, kwargs[x])
            else:
                self._data[x] = kwargs[x]
        self.loaded_values = set(x for x in kwargs)
        if not is_new:
            # Reset modified values if we loaded an original object
            # so we don't update all the values all the time.
            self.modified_values = set()
        self._allow_set_readonly = False
        self._cache = {}

    def __str__(self):
        s = f"{self.__class__.__name__}: "
        s += "; ".join(f"{x}={self._data[x]}" for x in self._data)
        s += " [modified:"
        s += ";".join(self.modified_values)
        s += "]"
        return s

    def __getitem__(self, item):
        return self.get(item)

    def __setitem__(self, item, value):
        self.set(item, value)

    def in_cache(self, key: str) -> bool:
        """Check if the key exists in the cache."""
        return key in self._cache

    def get_cached(self, key: str):
        """Retrieve a value from the cache."""
        return self._cache[key]

    def set_cached(self, key: str, value):
        """Set a value in the cache."""
        self._cache[key] = value

    def get(self, item, default=None):
        """Get an item from the data dictionary."""
        if item in self._data and self._data[item] is not None:
            return self._data[item]
        return default

    def get_for_db(self, item, default=None):
        """Get an item from the data dictionary for insertion into the database."""
        retval = default
        if item in self._data and self._data[item] is not None:
            retval = self._data[item]
        if isinstance(retval, enum.Enum):
            retval = retval.value
        elif isinstance(retval, (list, tuple, set, dict)):
            return json.dumps(retval)
        return retval

    def set(self, value, item, coerce=None, readonly: bool = False):
        """Set a value on the data dictionary."""
        if readonly and not self._allow_set_readonly:
            raise AttributeError(f"{item} is read-only")
        if coerce is not None and value is not None:
            value = coerce(value)
        if not self._value_equal(item, value):
            self._data[item] = value
            self.mark_modified(item)

    def _value_equal(self, item, value) -> bool:
        """Check if the value of an item is equal to the given value."""
        # No item means can't be equal
        if item not in self._data:
            return False
        # Handle the none case for the current value
        if self._data[item] is None:
            return value is None
        # avoid checking None == self._data[item] by handling this case
        elif value is None:
            return False  # self._data[item] is not None, so not equal
        # Two non-none values
        else:
            return self._data[item] == value

    def mark_modified(self, item):
        """Mark an item as modified"""
        self.modified_values.add(item)

    def clear_modified(self):
        """Clear the set of modified values."""
        self.modified_values.clear()

    @classmethod
    def get_table_name(cls):
        """Get the name of the table."""
        if hasattr(cls, 'TABLE_NAME'):
            return cls.TABLE_NAME
        return cls.__name__

    @classmethod
    def get_primary_keys(cls) -> t.Sequence:
        """Get the list of primary keys."""
        if hasattr(cls, 'PRIMARY_KEYS'):
            return cls.PRIMARY_KEYS
        return tuple()

    @classmethod
    def make_property(cls, item: str, coerce=None, readonly: bool = False):
        """Create a property."""
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=coerce, readonly=readonly)
        )

    @classmethod
    def make_datetime_property(cls, item: str, readonly: bool = False):
        """Create a datetime property"""
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=_NODBBaseObject.to_datetime, readonly=readonly)
        )

    @classmethod
    def make_date_property(cls, item: str, readonly: bool = False):
        """Create a date property."""
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=_NODBBaseObject.to_date, readonly=readonly)
        )

    @classmethod
    def make_enum_property(cls, item: str, enum_cls: type, readonly: bool = False):
        """Create an enum property"""
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=_NODBBaseObject.to_enum(enum_cls), readonly=readonly)
        )

    @classmethod
    def make_wkt_property(cls, item: str, readonly: bool = False):
        """Create a text property that will contain a WKT element."""
        # TODO: currently a string but could add better validation
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=str, readonly=readonly)
        )

    @classmethod
    def make_json_property(cls, item: str):
        """Create a property that will contain a JSON list or object."""
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=_NODBBaseObject.to_json)
        )

    @staticmethod
    def to_json(x):
        """Convert a string to JSON."""
        if isinstance(x, str) and x[0] in ('[', '{'):
            return json.loads(x)
        return x

    @staticmethod
    def to_enum(enum_cls):
        """Convert a value to an enum."""
        def _coerce(x):
            if isinstance(x, str):
                return enum_cls(x)
            return x
        return _coerce

    @staticmethod
    def to_datetime(dt):
        """Convert a value to a datetime object."""
        if isinstance(dt, str):
            return datetime.datetime.fromisoformat(dt)
        else:
            return dt

    @staticmethod
    def to_date(dt):
        """Convert a value to a date object."""
        if isinstance(dt, str):
            return datetime.date.fromisoformat(dt)
        else:
            return dt

    def validate_in_list(self, value: t.Any, valid_options: t.Sequence[t.Any], allow_none: bool = False, message: str = None) -> bool:
        """Validate that the given value is in the list of options."""
        if value is None:
            if not allow_none:
                raise NODBValidationError(message or f"Expected not-None found None for [{self.identifier()}]", 1000)
        elif value not in valid_options:
            opts = ','.join(str(x) for x in valid_options)
            raise NODBValidationError(message or f"Expected one of [{opts}] found [{value}] for [{self.identifier()}]", 1001)
        return True

    def identifier(self) -> str:
        """Return the identifier for this object."""
        return f"{self.__class__.__name__}"


IntColumn = functools.partial(_NODBBaseObject.make_property, coerce=int)
BooleanColumn = functools.partial(_NODBBaseObject.make_property, coerce=bool)
StringColumn = functools.partial(_NODBBaseObject.make_property, coerce=str)
FloatColumn = functools.partial(_NODBBaseObject.make_property, coerce=float)
UUIDColumn = StringColumn
ByteColumn = _NODBBaseObject.make_property
DateTimeColumn = _NODBBaseObject.make_datetime_property
DateColumn = _NODBBaseObject.make_date_property
EnumColumn = _NODBBaseObject.make_enum_property
JsonColumn = _NODBBaseObject.make_json_property
WKTColumn = _NODBBaseObject.make_wkt_property


class NODBQueueItem(_NODBBaseObject):
    """Queue item in the database."""

    TABLE_NAME: str = "nodb_queues"
    PRIMARY_KEYS: tuple[str] = ("queue_uuid",)

    queue_uuid: str = UUIDColumn("queue_uuid")
    created_date: datetime.datetime = DateTimeColumn("created_date", readonly=True)
    modified_date: datetime.datetime = DateTimeColumn("modified_date", readonly=True)
    status: QueueStatus = EnumColumn("status", QueueStatus)
    locked_by: t.Optional[str] = StringColumn("locked_by", readonly=True)
    locked_since: t.Optional[datetime.datetime] = DateTimeColumn("locked_since", readonly=True)
    queue_name: str = StringColumn("queue_name", readonly=True)
    escalation_level: int = IntColumn("escalation_level")
    subqueue_name: str = StringColumn("subqueue_name", readonly=True)
    unique_item_name: t.Optional[str] = StringColumn("unique_item_name", readonly=True)
    priority: t.Optional[int] = IntColumn('priority', readonly=True)
    data: dict = JsonColumn("data")

    def mark_complete(self, db: NODBControllerInstance):
        """Mark the queue item as complete."""
        self.set_queue_status(db=db, new_status=QueueStatus.COMPLETE)

    def mark_failed(self, db: NODBControllerInstance):
        """Mark the queue item as failed."""
        self.set_queue_status(db=db, new_status=QueueStatus.ERROR)

    def release(self,
                db: NODBControllerInstance,
                release_in_seconds: t.Optional[int] = None,
                **kwargs):
        """Release the queue item, optionally delaying for a number of seconds."""
        if release_in_seconds is None or release_in_seconds <= 0:
            self.set_queue_status(db=db, new_status=QueueStatus.UNLOCKED, **kwargs)
        else:
            self.set_queue_status(
                db=db,
                new_status=QueueStatus.DELAYED_RELEASE,
                release_at=datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=release_in_seconds),
                **kwargs
            )

    def renew(self, db: NODBControllerInstance):
        """Renew a lock on the queue item"""
        if self.status == QueueStatus.LOCKED:
            with db.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.TABLE_NAME}
                    SET
                        locked_since = %s
                    WHERE
                        queue_uuid = %s
                        AND status = 'LOCKED'
                """, [
                    datetime.datetime.now(datetime.timezone.utc),
                    self.queue_uuid
                ])

    def set_queue_status(self,
                         db: NODBControllerInstance,
                         new_status: QueueStatus,
                         release_at: t.Optional[datetime.datetime] = None,
                         reduce_priority: bool = False,
                         escalation_level: t.Optional[int] = None):
        """Set the queue status from LOCKED to a new status."""
        if self.status == QueueStatus.LOCKED:
            with db.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.TABLE_NAME}
                    SET
                        status = %s,
                        locked_by = NULL,
                        locked_since = NULL,
                        delay_release = %s,
                        priority = priority - %s,
                        escalation_level = %s
                    WHERE 
                        queue_uuid = %s
                        AND status = 'LOCKED'
                """, [
                    new_status.value,
                    release_at,
                    1 if reduce_priority else 0,
                    escalation_level if escalation_level is not None else (self.escalation_level or 0),
                    self.queue_uuid
                ])
                # TODO: check if the row was actually updated?
                self.status = new_status


class NODBSourceFile(_NODBBaseObject):

    TABLE_NAME: str = "nodb_source_files"
    PRIMARY_KEYS: tuple[str] = ("source_uuid", "received_date",)

    source_uuid: str = UUIDColumn("source_uuid")
    received_date: datetime.date = DateColumn("received_date")

    source_path: str = StringColumn("source_path")
    file_name: str = StringColumn("file_name")

    original_uuid: str = StringColumn("original_uuid")
    original_idx: int = IntColumn("original_idx", coerce=int)

    status: SourceFileStatus = EnumColumn("status", SourceFileStatus)

    history: list = JsonColumn("history")

    metadata: t.Optional[dict] = JsonColumn("metadata")

    def set_metadata(self, key, value):
        """Set a metadata property."""
        if self.metadata is None:
            self.metadata = {key: value}
            self.modified_values.add("metadata")
        else:
            self.metadata[key] = value
            self.modified_values.add("metadata")

    def clear_metadata(self, key):
        """Clear a metadata property."""
        if self.metadata is None:
            return
        if key not in self.metadata:
            return
        del self.metadata[key]
        self.modified_values.add("metadata")
        if not self.metadata:
            self.metadata = None

    def get_metadata(self, key, default=None):
        """Get a metadata property."""
        if self.metadata is None or key not in self.metadata:
            return default
        return self.metadata[key]

    def add_to_metadata(self, key, value):
        """Add a value to a metadata set if not in that set already."""
        if self.metadata is None:
            self.metadata = {key: [value]}
            self.modified_values.add("metadata")
        elif key not in self.metadata:
            self.metadata[key] = [value]
            self.modified_values.add("metadata")
        elif value not in self.metadata[key]:
            self.metadata[key].append(value)
            self.modified_values.add("metadata")

    def report_error(self, message, name, version, instance):
        """Add an error to the file history."""
        self.add_history(message, name, version, instance, 'ERROR')

    def report_warning(self, message, name, version, instance):
        """Add a warning to the file history."""
        self.add_history(message, name, version, instance, 'WARNING')

    def add_history(self, message, name, version, instance, level='INFO'):
        """Add a history entry to this file."""
        if self.history is None:
            self.history = []
        self.history.append({
            'msg': message,
            'src': name,
            'ver': version,
            'ins': instance,
            'lvl': level,
            'asc': datetime.datetime.utcnow().isoformat()
        })
        self.modified_values.add('history')

    def stream_observation_data(self, db: NODBControllerInstance, lock_type: LockType = None) -> t.Iterable[NODBObservationData]:
        """Find all observations associated with this source file."""
        with db.cursor() as cur:
            query = f"""
                SELECT * 
                FROM {NODBObservationData.TABLE_NAME} 
                WHERE 
                    received_date = %s
                    AND source_file_uuid = %s
            """
            query += db.build_lock_type_clause(lock_type)
            cur.execute(query, [self.received_date, self.source_uuid])
            for row in cur.fetch_stream():
                yield NODBObservationData(
                    is_new=False,
                    **{x: row[x] for x in row.keys()}
                )

    def stream_working_records(self, db: NODBControllerInstance, lock_type: LockType = None, order_by: t.Optional[str] = None) -> t.Iterable[NODBWorkingRecord]:
        """Find a working record associated with this source file."""
        with db.cursor() as cur:
            query = f"""
                   SELECT * 
                   FROM {NODBWorkingRecord.TABLE_NAME} 
                   WHERE 
                       received_date = %s
                       AND source_file_uuid = %s
               """ + NODBWorkingRecord.build_query_extras(order_by)
            query += db.build_lock_type_clause(lock_type)
            cur.execute(query, [self.received_date, self.source_uuid])
            for row in cur.fetch_stream():
                yield NODBWorkingRecord(
                    is_new=False,
                    **{x: row[x] for x in row.keys()}
                )

    @classmethod
    def find_by_source_path(cls, db: NODBControllerInstance, source_path: str, **kwargs):
        """Locate a source file by the source path."""
        return db.load_object(cls, {
            'source_path': source_path
        }, **kwargs)

    @classmethod
    def find_by_original_info(cls, db: NODBControllerInstance, original_uuid: str, received_date: t.Union[datetime.date, str], message_idx: int, **kwargs):
        """Locate a source file that was a part of another source file by the original file info."""
        return db.load_object(cls, {
            'original_idx': message_idx,
            'received_date': parse_received_date(received_date),
            'original_uuid': original_uuid
        }, **kwargs)

    @classmethod
    def find_by_uuid(cls, db: NODBControllerInstance, source_uuid: str, received: t.Union[datetime.date, str], **kwargs):
        """Locate a source file by UUID."""
        return db.load_object(cls, {
            'source_uuid': source_uuid,
            'received_date': parse_received_date(received)
        }, **kwargs)


class NODBUser(_NODBBaseObject):

    TABLE_NAME = "nodb_users"
    PRIMARY_KEYS: tuple[str] = ("username",)

    username: str = StringColumn("username")
    phash: bytes = ByteColumn("phash")
    salt: bytes = ByteColumn("salt")
    old_phash: t.Optional[bytes] = ByteColumn("old_phash")
    old_salt: t.Optional[bytes] = ByteColumn("old_salt")
    old_expiry: t.Optional[datetime] = DateTimeColumn("old_expiry")
    status: UserStatus = EnumColumn("status", UserStatus)
    roles: list = JsonColumn("roles")

    def assign_role(self, role_name):
        """Assign a role to the user."""
        if self.roles is None:
            self.roles = [role_name]
            self.modified_values.add('roles')
        elif role_name not in self.roles:
            self.roles.append(role_name)
            self.modified_values.add('roles')

    def unassign_role(self, role_name):
        """Unassign a role from the user."""
        if self.roles is not None and role_name in self.roles:
            self.roles.remove(role_name)
            self.modified_values.add('roles')

    def set_password(self, new_password, salt_length: int = 16, old_expiry_seconds: int = 0):
        """Set the users password."""
        if not isinstance(new_password, str):
            raise CNODCError("Invalid type for new password", "USERCHECK", 1002)
        if len(new_password) > 1024:
            raise CNODCError("Password is too long", "USERCHECK", 1001)
        if new_password == '':
            raise CNODCError('No password provided', 'USERCHECK', 1003)
        if old_expiry_seconds > 0:
            self.old_salt = self.salt
            self.old_phash = self.phash
            self.old_expiry = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=old_expiry_seconds)
        self.salt = secrets.token_bytes(salt_length)
        self.phash = NODBUser.hash_password(new_password, self.salt)

    def check_password(self, password):
        """Check a password to see if it is the correct one."""
        check_hash = NODBUser.hash_password(password, self.salt)
        if secrets.compare_digest(check_hash, self.phash):
            return True
        if self.old_phash and self.old_salt and self.old_expiry:
            if self.old_expiry > datetime.datetime.now(datetime.timezone.utc):
                old_check_hash = NODBUser.hash_password(password, self.old_salt)
                if secrets.compare_digest(old_check_hash, self.old_phash):
                    zrlog.get_logger("cnodc").notice(f"Old password used for login by {self.username}")
                    return True
        return False

    def cleanup(self):
        """Cleanup a user's password entry."""
        if self.old_expiry is not None and self.old_expiry <= datetime.datetime.now(datetime.timezone.utc):
            self.old_phash = None
            self.old_salt = None
            self.old_expiry = None

    def permissions(self, db: NODBControllerInstance) -> set:
        """Retrieve a user's permissions."""
        if not self.in_cache('permissions'):
            permissions = set()
            if self.roles:
                with db.cursor() as cur:
                    role_placeholders = ', '.join('%s' for _ in self.roles)
                    cur.execute(f"""
                        SELECT permission FROM 
                        nodb_permissions WHERE role_name IN ({role_placeholders})
                    """, [self.roles])
                    permissions.update(row[0] for row in cur.fetch_stream())
            self.set_cached('permissions', permissions)
        return self.get_cached('permissions')

    @staticmethod
    def hash_password(password: str, salt: bytes, iterations=752123) -> bytes:
        """Hash a password."""
        if not isinstance(password, str):
            raise CNODCError("Invalid password", "USERCHECK", 1000)
        return hashlib.pbkdf2_hmac('sha512', password.encode('utf-8', errors="replace"), salt, iterations)

    @classmethod
    def find_by_username(cls, db, username: str, **kwargs):
        """Locate a user by their username."""
        return db.load_object(cls, {"username": username}, **kwargs)


class NODBSession(_NODBBaseObject):

    TABLE_NAME: str = "nodb_sessions"
    PRIMARY_KEYS: tuple[str] = ("session_id",)

    session_id: str = StringColumn("session_id")
    start_time: datetime = DateTimeColumn("start_time")
    expiry_time: datetime = DateTimeColumn("expiry_time")
    username: str = StringColumn("username")
    session_data: dict = JsonColumn("session_data")

    def set_session_value(self, key, value):
        """Set a session value"""
        if self.session_data is None:
            self.session_data = {}
        self.session_data[key] = value

    def get_session_value(self, key, default=None):
        """Get a session value"""
        return self.session_data[key] if self.session_data and key in self.session_data else default

    def is_expired(self) -> bool:
        """Check if the session is expired."""
        return self.expiry_time < datetime.datetime.now(datetime.timezone.utc)

    @classmethod
    def find_by_session_id(cls, db, session_id: str, **kwargs):
        """Locate a session by its ID."""
        return db.load_object(cls, {"session_id": session_id}, **kwargs)


class NODBUploadWorkflow(_NODBBaseObject):

    TABLE_NAME = "nodb_upload_workflows"
    PRIMARY_KEYS = ("workflow_name",)

    workflow_name: str = StringColumn("workflow_name")
    configuration: dict[str, t.Any] = JsonColumn("configuration")
    is_active: bool = BooleanColumn('is_active')

    def permissions(self):
        """Retrieve the permissions associated with this workflow."""
        return self.get_config('permission', default=None)

    def get_config(self, config_key: str, default=None):
        """Get a configuration value for this workflow."""
        if self.configuration and config_key in self.configuration:
            return self.configuration[config_key]
        return default

    def ordered_processing_steps(self) -> list[str]:
        if self.configuration is None or 'processing_steps' not in self.configuration:
            return []
        return NODBUploadWorkflow.build_ordered_processing_steps(self.configuration['processing_steps'])

    @staticmethod
    def build_ordered_processing_steps(steps: dict[str, dict[str, t.Any]]) -> list[str]:
        sort_me = []
        for step_name in steps:
            step = steps[step_name]
            if 'name' not in step or not step['name']:
                raise CNODCError(f'Step {step_name} is missing a [name] value', 'NODB_WORKFLOW', 1000)
            if 'order' not in step or step['order'] is None:
                raise CNODCError(f'Step {step_name} is missing a proper [order] value', 'NODB_WORKFLOW', 1001)
            try:
                sort_me.append((step['name'], int(step['order'])))
            except (ValueError, TypeError):
                raise CNODCError(f"Step {step_name} is missing an integer [order] value", 'NODB_WORKFLOW', 1002)
        sort_me.sort(key=lambda x: x[1])
        return [x[0] for x in sort_me]

    def update_config(self, config: dict[str, t.Any]):
        if self.configuration is not None and self.configuration['processing_steps'] is not None and 'processing_steps' in config and config['processing_steps']:
            current_steps = self.ordered_processing_steps()
            new_steps = NODBUploadWorkflow.build_ordered_processing_steps(config['processing_steps'])
            NODBUploadWorkflow._validate_step_order(current_steps, new_steps)
        self.configuration = config
        self.check_config()

    @staticmethod
    def _validate_step_order(current: list[str], new: list[str]):
        # removing steps leads to issues, dont do it!
        for old_step in current:
            if old_step not in new:
                raise NODBValidationError(f"Step {old_step} cannot be removed", 2020)

        # Reordering steps leads to problems (e.g. a step may be re-executed for an existing workflow item
        for step_name in new:
            # adding steps is fine
            if step_name not in current:
                continue
            current_pos = current.index(step_name)
            new_pos = current.index(step_name)
            for other_step in current:
                if other_step == step_name:
                    continue
                is_before_in_current = current.index(other_step) < current_pos
                is_before_in_new = new.index(other_step) < new_pos
                if is_before_in_new and not is_before_in_current:
                    raise NODBValidationError(f"Step {step_name} cannot be moved before {other_step}", 2016)
                if is_before_in_current and not is_before_in_new:
                    raise NODBValidationError(f"Step {step_name} cannot be moved after {other_step}", 2017)

    @injector.inject
    def check_config(self, files: cnodc.storage.core.StorageController):
        """Validate the configuration for this workflow."""
        if 'label' not in self.configuration:
            raise NODBValidationError(f"A label is required for workflows", 2020)
        lbl = self.configuration['label']
        if not isinstance(lbl, dict):
            raise NODBValidationError(f"The workflow label must be a dict", 2021)
        if 'en' not in lbl and 'und' not in lbl:
            raise NODBValidationError("An English or language-neutral name must be provided for the workflow", 2022)
        if 'fr' not in lbl and 'und' not in lbl:
            raise NODBValidationError("An French or language-neutral name must be provided for the workflow", 2023)
        if 'validation' in self.configuration and self.configuration['validation'] is not None:
            try:
                x = dynamic_object(self.configuration['validation'])
                if not callable(x):
                    raise NODBValidationError(f'Invalid value for [validation]: {self.configuration["validation"]}, must be a Python callable', 2000)
            except DynamicObjectLoadError:
                raise NODBValidationError(f'Invalid value for [validation]: {self.configuration["validation"]}, must be a Python object', 2001)
        has_upload = False
        if 'working_target' in self.configuration and self.configuration['working_target']:
            has_upload = True
            self._check_upload_target_config(self.configuration['working_target'], files, 'working')
        if 'additional_targets' in self.configuration and self.configuration['additional_targets']:
            has_upload = True
            for idx, target in enumerate(self.configuration['additional_targets']):
                self._check_upload_target_config(target, files, f'additional{idx}')
        if not has_upload:
            raise NODBValidationError(f"Workflow missing either upload or archive URL", 2002)
        if 'processing_steps' in self.configuration and self.configuration['processing_steps'] is not None:
            psteps = self.configuration['processing_steps']
            if not isinstance(psteps, dict):
                raise NODBValidationError("Processing steps must be a dictionary", 2010)
            for key in psteps:
                entry = psteps[key]
                if not isinstance(entry, dict):
                    raise NODBValidationError(f"Processing step {key} must be a dictionary", 2011)
                if 'order' not in entry or entry['order'] is None:
                    raise NODBValidationError(f"Processing step {key} must have an order value", 2012)
                if not isinstance(entry['order'], int):
                    raise NODBValidationError(f"Processing step {key} must have an integer order value", 2013)
                if 'name' not in entry or not entry['name']:
                    raise NODBValidationError(f"Processing step {key} must have an name value", 2014)
                if 'priority' in entry:
                    try:
                        _ = int(entry['priority'])
                    except (ValueError, TypeError):
                        raise NODBValidationError(f"Processing step {key} must have an integer priority value", 2015)
        if 'filename_pattern' in self.configuration and self.configuration['filename_pattern'] is not None:
            if not isinstance(self.configuration['filename_pattern'], str):
                raise NODBValidationError("The filename_pattern must be a string", 2018)
        if 'default_metadata' in self.configuration and self.configuration['default_metadata'] is not None:
            dm = self.configuration['default_metadata']
            if not isinstance(dm, dict):
                raise NODBValidationError("The default_metadata must be a dictionary", 2019)



    def _check_upload_target_config(self, config: dict, files: StorageController, tn: str):
        """Validate an upload target."""
        if 'directory' not in config:
            raise NODBValidationError(f'Target directory missing in [{tn}]', 2007)
        try:
            _ = files.get_handle(config['directory'])
        except Exception as ex:
            raise NODBValidationError(f'Target directory is not supported by storage subsystem in [{tn}]', 2008) from ex
        if 'allow_overwrite' in config and config['allow_overwrite'] not in ('user', 'always', 'never'):
            raise NODBValidationError(f'Overwrite setting must be one of [user|always|never] in [{tn}]', 2009)
        if 'tier' in config:
            try:
                _ = StorageTier(config['tier'])
            except Exception as ex:
                raise NODBValidationError(f'Tier value [{config["tier"]} is not supported in [{tn}]', 2006) from ex
        if 'metadata' in config and config['metadata']:
            if not isinstance(config['metadata'], dict):
                raise NODBValidationError(f"Invalid value for [metadata] in [{tn}]: must be a dictionary", 2005)
            for x in self.configuration['metadata'].keys():
                if not isinstance(x, str):
                    raise NODBValidationError(f"Invalid key for [metadata] in [{tn}]: {x}, must be a string", 2004)
                if not isinstance(self.configuration['metadata'][x], str):
                    raise NODBValidationError(f'Invalid value for [metadata.{x}] in [{tn}]: {self.configuration["metadata"][x]}, must be a string', 2003)

    def check_access(self, user_permissions: t.Union[list, set, tuple]) -> bool:
        """Check if a user has access to this workflow based on their permissions."""
        if '__admin__' in user_permissions:
            return True
        needed_permissions = self.permissions()
        if '__any__' in needed_permissions:
            return True
        return any(x in user_permissions for x in needed_permissions)

    @classmethod
    def find_by_name(cls, db, workflow_name: str, **kwargs):
        """Find a workflow by name."""
        return db.load_object(cls, {"workflow_name": workflow_name},  **kwargs)

    @classmethod
    def find_all(cls, db, **kwargs):
        """Find all workflows."""
        with db.cursor() as cur:
            cur.execute(f'SELECT * FROM {NODBUploadWorkflow.TABLE_NAME}')
            for row in cur.fetch_stream():
                yield NODBUploadWorkflow(**row, is_new=False)


class NODBMission(_NODBBaseObject):

    TABLE_NAME = 'nodb_missions'
    PRIMARY_KEYS = ("mission_uuid",),

    mission_uuid: str = UUIDColumn("mission_uuid")
    mission_name: str = StringColumn("mission_name")
    metadata: dict = JsonColumn("metadata")
    start_date: datetime.datetime = DateTimeColumn("start_date")
    end_date: t.Optional[datetime.datetime] = DateTimeColumn("end_date")


class NODBObservation(_NODBBaseObject):
    """Represents an archived observation in the database.

        In particular, this table/class represents the characteristics of data records
        that are usually searchable. The actual record is stored as an NODBObservationData.
    """

    TABLE_NAME = "nodb_obs"
    PRIMARY_KEYS = ("obs_uuid", "received_date")

    obs_uuid: str = UUIDColumn("obs_uuid")
    received_date: datetime.date = DateColumn("received_date")

    platform_uuid: t.Optional[str] = UUIDColumn("platform_uuid")
    mission_uuid: t.Optional[str] = UUIDColumn("mission_uuid")
    source_name: str = StringColumn("source_name")
    instrument_type: str = StringColumn("instrument_type")
    program_name: str = StringColumn("program_name")
    obs_time: datetime.datetime = DateTimeColumn("obs_time")
    min_depth: float = FloatColumn("min_depth")
    max_depth: float = FloatColumn("max_depth")
    location: str = WKTColumn("location")
    observation_type: ObservationType = EnumColumn("observation_type", ObservationType)
    surface_parameters: list = JsonColumn("surface_parameters")
    profile_parameters: list = JsonColumn("profile_parameters")
    processing_level: ProcessingLevel = EnumColumn("processing_level", ProcessingLevel)
    embargo_date: datetime.datetime = DateTimeColumn("embargo_date")

    @classmethod
    def find_by_uuid(cls, db, obs_uuid: str, received_date: t.Union[str, datetime.date], **kwargs):
        """Find an observation by UUID and received date."""
        return db.load_object(cls, {
            "obs_uuid": obs_uuid,
            "received_date": parse_received_date(received_date)
        }, **kwargs)


class NODBObservationData(_NODBBaseObject):
    """Represents the 'meat' of an archived observation; the full record and associated metadata."""

    TABLE_NAME = "nodb_obs_data"
    PRIMARY_KEYS = ("obs_uuid", "received_date")

    obs_uuid: str = UUIDColumn("obs_uuid")
    received_date: datetime.date = DateColumn("received_date")
    source_file_uuid: str = StringColumn("source_file_uuid")
    message_idx: int = IntColumn("message_idx")
    record_idx: int = IntColumn("record_idx")
    data_record: t.Optional[bytes] = ByteColumn("data_record")
    process_metadata: dict = JsonColumn("process_metadata")
    qc_tests: dict = JsonColumn("qc_tests")
    duplicate_uuid: str = UUIDColumn("duplicate_uuid")
    duplicate_received_date: datetime.date = DateColumn("duplicate_received_date")
    status: ObservationStatus = EnumColumn("status", ObservationStatus)

    def get_process_metadata(self, key, default=None):
        """Retrieve metadata information about the record."""
        if self.process_metadata and key in self.process_metadata:
            return self.process_metadata[key]
        return default

    def set_process_metadata(self, key, value):
        """Set metadata about the record."""
        if self.process_metadata is None:
            self.process_metadata = {}
        self.process_metadata[key] = value
        self.modified_values.add('process_metadata')

    @classmethod
    def find_by_uuid(cls, db, obs_uuid: str, received_date: t.Union[str, datetime.date], *args, **kwargs):
        """Locate a record by UUID."""
        return db.load_object(cls, {
            "obs_uuid": obs_uuid,
            "received_date": parse_received_date(received_date)
        }, *args, **kwargs)

    @classmethod
    def find_by_source_info(cls,
                            db,
                            source_file_uuid: str,
                            source_received_date: t.Union[str, datetime.date],
                            message_idx: int,
                            record_idx: int,
                            *args, **kwargs):
        """Locate a record by information about it in the source file."""
        return db.load_object(cls, {
                "received_date": parse_received_date(source_received_date),
                "source_file_uuid": source_file_uuid,
                "message_idx": message_idx,
                "record_idx": record_idx
            }, *args, **kwargs)

    @property
    def record(self) -> t.Optional[ocproc2.ParentRecord]:
        """Extract the data record."""
        if self.data_record is None:
            return None
        if not self.in_cache('loaded_record') is None:
            decoder = OCProc2BinCodec()
            records = [x for x in decoder.load_all(self.data_record)]
            if records:
                self.set_cached('loaded_record', records[0])
            else:
                self.set_cached('loaded_record', None)
        return self.get_cached('loaded_record')

    @record.setter
    def record(self, data_record: ocproc2.ParentRecord):
        """Set the data record."""
        self.set_cached('loaded_record', data_record)
        if data_record is None:
            self.data_record = None
            self.mark_modified('data_record')
        else:
            decoder = OCProc2BinCodec()
            ba = bytearray()
            for byte_ in decoder.encode_records(
                    [data_record],
                    codec='JSON',
                    compression='LZMA2CRC4',
                    correction=None):
                ba.extend(byte_)
            self.data_record = ba
            self.mark_modified('data_record')


class NODBStation(_NODBBaseObject):

class NODBPlatform(_NODBBaseObject):

    TABLE_NAME = 'nodb_platforms'
    PRIMARY_KEYS = ('platform_uuid', )

    platform_uuid: str = UUIDColumn("platform_uuid")
    wmo_id: str = StringColumn("wmo_id")
    wigos_id: str = StringColumn("wigos_id")
    platform_name: str = StringColumn("platform_name")
    platform_id: str = StringColumn("platform_id")
    platform_type: str = StringColumn("platform_type")
    service_start_date: datetime.datetime = DateTimeColumn("service_start_date")
    service_end_date: datetime.datetime = DateTimeColumn("service_end_date")
    instrumentation: dict = JsonColumn('instrumentation')
    metadata: dict = JsonColumn('metadata')
    map_to_uuid: str = UUIDColumn("map_to_uuid")
    status: PlatformStatus = EnumColumn("status", PlatformStatus)
    embargo_data_days: int = IntColumn('embargo_data_days')

    def get_metadata(self, metadata_key, default=None):
        """Retrieve metadata about a platform."""
        if self.metadata is not None and metadata_key in self.metadata:
            return self.metadata[metadata_key] or default
        return None

    @classmethod
    def search(cls,
               db: NODBControllerInstance,
               in_service_time: t.Optional[datetime.datetime] = None,
               wmo_id: t.Optional[str] = None,
               wigos_id: t.Optional[str] = None,
               platform_id: t.Optional[str] = None,
               platform_name: t.Optional[str] = None) -> list[NODBPlatform]:
        """Search for a platform by various identifiers."""
        with db.cursor() as cur:
            args = []
            clauses = []
            if wmo_id is not None and wmo_id != '':
                args.append(wmo_id)
                clauses.append('wmo_id = %s')
            if wigos_id is not None and wigos_id != '':
                args.append(wigos_id)
                clauses.append('wigos_id = %s')
            if platform_id is not None and platform_id != '':
                args.append(platform_id)
                clauses.append('platform_id = %s')
            if platform_name is not None and platform_name != '':
                args.append(platform_name)
                clauses.append('platform_name = %s')
            if not args:
                return []
            if in_service_time is not None:
                clauses.append('((service_start_date IS NULL OR service_start_date <= %s) AND (service_end_date IS NULL or service_end_date >= %s)')
                args.extend([in_service_time, in_service_time])
            query = f"SELECT * FROM {NODBPlatform.TABLE_NAME} WHERE " + ' OR '.join(clauses)
            cur.execute(query, args)
            return [NODBPlatform(is_new=False, **x) for x in cur.fetch_all()]

    @classmethod
    def find_by_uuid(cls, db: NODBControllerInstance, platform_uuid: str, **kwargs):
        """Locate a platform by its unique identifier."""
        return db.load_object(cls, {
            'platform_uuid': platform_uuid
        }, **kwargs)

    @classmethod
    def find_all_raw(cls, db: NODBControllerInstance) -> t.Iterable[dict]:
        """Retrieve all platforms in a raw (i.e. database dictionary) format."""
        with db.cursor() as cur:
            cur.execute(f"SELECT * FROM {NODBPlatform.TABLE_NAME}")
            for row in cur.fetch_stream():
                yield row


class NODBWorkingRecord(_NODBBaseObject):
    """Represents a record currently being processed in the database."""

    TABLE_NAME = "nodb_working"
    PRIMARY_KEYS = ("working_uuid",)

    working_uuid: str = UUIDColumn("working_uuid")
    record_uuid: t.Optional[str] = UUIDColumn("record_uuid")
    received_date: datetime.date = DateColumn("received_date")
    source_file_uuid: str = UUIDColumn("source_file_uuid")
    message_idx: int = IntColumn("message_idx")
    record_idx: int = IntColumn("record_idx")
    data_record: t.Optional[bytes] = ByteColumn("data_record")
    qc_metadata: dict = JsonColumn("qc_metadata")
    qc_batch_id: str = UUIDColumn("qc_batch_id")
    platform_uuid: str = UUIDColumn("platform_uuid")
    obs_time: datetime.datetime = DateTimeColumn("obs_time")
    location: str = WKTColumn("location")

    def set_metadata(self, key, value):
        """Set metadata on the working record"""
        if self.qc_metadata is None:
            self.qc_metadata = {}
        self.qc_metadata[key] = value
        self.mark_modified('qc_metadata')

    def get_metadata(self, key, default=None):
        """Retrieve metadata on the working record."""
        if self.qc_metadata is None or key not in self.qc_metadata:
            return default
        return self.qc_metadata[key]

    @classmethod
    def find_by_uuid(cls, db, obs_uuid: str, *args, **kwargs):
        """Find a working record by its identifier"""
        return db.load_object(cls, {
            "obs_uuid": obs_uuid,
        }, *args, **kwargs)

    @classmethod
    def find_by_source_info(cls,
                            db,
                            source_file_uuid: str,
                            source_received_date: t.Union[str, datetime.date],
                            message_idx: int,
                            record_idx: int,
                            *args, **kwargs):
        """Find a working record by its source information"""
        return db.load_object(cls, {
                "received_date": parse_received_date(source_received_date),
                "source_file_uuid": source_file_uuid,
                "message_idx": message_idx,
                "record_idx": record_idx
            }, *args, **kwargs)

    @property
    def record(self) -> t.Optional[ocproc2.ParentRecord]:
        """Extract the OCProc2 record."""
        if self.data_record is None:
            return None
        if not self.in_cache('loaded_record') is None:
            decoder = OCProc2BinCodec()
            records = [x for x in decoder.load_all(self.data_record)]
            if records:
                self.set_cached('loaded_record', records[0])
            else:
                self.set_cached('loaded_record', None)
        return self.get_cached('loaded_record')

    @record.setter
    def record(self, data_record: ocproc2.ParentRecord):
        """Set the OCProc2 record."""
        self.set_cached('loaded_record', data_record)
        self._update_from_data_record(data_record)
        if data_record is None:
            self.data_record = None
            self.mark_modified('data_record')
        else:
            decoder = OCProc2BinCodec()
            ba = bytearray()
            for byte_ in decoder.encode_records(
                    [data_record],
                    codec='PICKLE',
                    compression='LZMA2',
                    correction=None):
                ba.extend(byte_)
            self.data_record = ba
            self.mark_modified('data_record')

    def _update_from_data_record(self, data_record: ocproc2.ParentRecord):

        if data_record.coordinates.has_value('Time') and data_record.coordinates['Time'].is_iso_datetime():
            self.obs_time = data_record.coordinates['Time'].to_datetime()
        if data_record.coordinates.has_value('Latitude') and data_record.coordinates['Latitude'].is_numeric() and data_record.coordinates.has_value('Longitude') and data_record.coordinates['Longitude'].is_numeric():
            lat = data_record.coordinates['Latitude'].to_float()
            lon = data_record.coordinates['Longitude'].to_float()
            self.location = f"POINT ({round(lon, 5)} {round(lat, 5)})"
        self.platform_uuid = data_record.metadata.best_value('CNODCPlatform', None)

    @staticmethod
    def bulk_set_batch_uuid(
            db: NODBControllerInstance,
            working_uuids: list[str],
            batch_uuid: str
    ):
        with db.cursor() as cur:
            for uuid_subset in db.chunk_for_in(working_uuids):
                cur.execute(f"""
                    UPDATE {NODBWorkingRecord.TABLE_NAME} 
                    SET qc_batch_id = %s
                    WHERE working_uuid IN {','.join('%s' for _ in range(0, len(uuid_subset)))}""", [
                    batch_uuid,
                    *uuid_subset
                ])

    @staticmethod
    def build_query_extras(order_by: t.Optional[str]):
        extras = ""
        if order_by is None:
            pass
        elif order_by == 'obs_time_asc':
            extras += " ORDER BY obs_time ASC"
        else:
            zrlog.get_logger('cnodc.nodb.wr').error(f'Invalid order by statement {order_by}')
            pass
        return extras


class NODBBatch(_NODBBaseObject):

    batch_uuid: str = _NODBBaseObject.make_property("batch_uuid", coerce=str)
    qc_metadata: dict = _NODBBaseObject.make_property("qc_metadata")
    status: BatchStatus = EnumColumn("status", BatchStatus)

    @classmethod
    def find_by_uuid(cls, db: NODBControllerInstance, batch_uuid: str, **kwargs):
        return db.load_object(cls, {
            'batch_uuid': batch_uuid
        }, **kwargs)

    @classmethod
    def count_working_by_uuid(cls, db: NODBControllerInstance, batch_uuid: str) -> int:
        with db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {NODBWorkingRecord.TABLE_NAME} WHERE qc_batch_id = %s", [batch_uuid])
            row = cur.fetch_one()
            return row[0]

    def stream_working_records(self, db: NODBControllerInstance, lock_type: LockType = None, order_by: t.Optional[str] = None):
        with db.cursor() as cur:
            query = f"""
                   SELECT * 
                   FROM {NODBWorkingRecord.TABLE_NAME} 
                   WHERE 
                       qc_batch_id = %s
               """ + NODBWorkingRecord.build_query_extras(order_by)
            query += db.build_lock_type_clause(lock_type)
            cur.execute(query, [self.batch_uuid])
            for row in cur.fetch_stream():
                yield NODBWorkingRecord(
                    is_new=False,
                    **{x: row[x] for x in row.keys()}
                )
