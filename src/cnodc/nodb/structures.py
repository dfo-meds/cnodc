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
from cnodc.ocproc2 import DataRecord
from cnodc.storage import StorageController
from cnodc.storage.base import StorageTier
from cnodc.util import CNODCError, dynamic_object, DynamicObjectLoadError

if t.TYPE_CHECKING:
    from cnodc.nodb import NODBControllerInstance, LockType
    import cnodc.storage.core


def parse_received_date(rdate: t.Union[str, datetime.date]) -> datetime.date:
    if isinstance(rdate, str):
        try:
            return datetime.date.fromisoformat(rdate)
        except ValueError as ex:
            raise CNODCError(f"Invalid received date [{rdate}]", "NODB", 1000)
    else:
        return rdate


class NODBValidationError(CNODCError):
    pass


class UserStatus(enum.Enum):

    ACTIVE = 'ACTIVE'
    INACTIVE = 'INACTIVE'


class SourceFileStatus(enum.Enum):

    NEW = 'NEW'
    QUEUED = 'QUEUED'
    IN_PROGRESS = 'IN_PROGRESS'
    ERROR = 'ERROR'
    COMPLETE = 'COMPLETE'


class ObservationStatus(enum.Enum):

    UNVERIFIED = 'UNVERIFIED'
    VERIFIED = 'VERIFIED'
    DISCARDED = 'DISCARDED'
    DUPLICATE = 'DUPLICATE'
    ARCHIVED = 'ARCHIVED'
    DUBIOUS = 'DUBIOUS'


class ObservationType(enum.Enum):

    SURFACE = 'SURFACE'
    AT_DEPTH = 'AT_DEPTH'
    PROFILE = 'PROFILE'
    OTHER = 'OTHER'


class BatchStatus(enum.Enum):

    NEW = 'NEW'
    QUEUED = 'QUEUED'
    IN_PROGRESS = 'IN_PROGRESS'
    MANUAL_REVIEW = 'MANUAL_REVIEW'
    COMPLETE = 'COMPLETE'
    ERRORED = 'ERRORED'


class ObservationWorkingStatus(enum.Enum):

    NEW = 'NEW'
    AUTO_QUEUED = 'AUTO_QUEUED'
    AUTO_IN_PROGRESS = 'AUTO_IN_PROGRESS'
    USER_QUEUED = 'USER_QUEUED'
    USER_IN_PROGRESS = 'USER_IN_PROGRESS'
    USER_CHECKED = 'USER_CHECKED'
    QUEUE_ERROR = 'QUEUE_ERROR'
    ERROR = 'ERROR'


class StationStatus(enum.Enum):

    ACTIVE = 'ACTIVE'
    INCOMPLETE = 'INCOMPLETE'
    INACTIVE = 'INACTIVE'


class QueueStatus(enum.Enum):

    UNLOCKED = 'UNLOCKED'
    LOCKED = 'LOCKED'
    COMPLETE = 'COMPLETE'
    DELAYED_RELEASE = 'DELAYED_RELEASE'
    ERROR = 'ERROR'


class ProcessingLevel(enum.Enum):

    RAW = 'RAW'
    ADJUSTED = 'ADJUSTED'
    REAL_TIME = 'REAL_TIME'
    DELAYED_MODE = 'DELAYED_MODE'


class _NODBBaseObject:

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

    def in_cache(self, key):
        return key in self._cache

    def get_cached(self, key):
        return self._cache[key]

    def set_cached(self, key, value):
        self._cache[key] = value

    def __str__(self):
        s = f"{self.__class__.__name__}: "
        s += "; ".join(f"{x}={self._data[x]}" for x in self._data)
        s += " [modified:"
        s += ";".join(self.modified_values)
        s += "]"
        return s

    def __getitem__(self, item):
        return self.get(item)

    def get(self, item, default=None):
        if item in self._data and self._data[item] is not None:
            return self._data[item]
        return default

    def get_for_db(self, item, default=None):
        retval = default
        if item in self._data and self._data[item] is not None:
            retval = self._data[item]
        if isinstance(retval, enum.Enum):
            retval = retval.value
        elif isinstance(retval, (list, tuple, set, dict)):
            return json.dumps(retval)
        return retval

    def __setitem__(self, item, value):
        self.set(item, value)

    def set(self, value, item, coerce=None, readonly: bool = False):
        if readonly and not self._allow_set_readonly:
            raise AttributeError(f"{item} is read-only")
        if coerce is not None and value is not None:
            value = coerce(value)
        if not self._value_equal(item, value):
            self._data[item] = value
            self.mark_modified(item)

    def _value_equal(self, item, value) -> bool:
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
        self.modified_values.add(item)

    def clear_modified(self):
        self.modified_values.clear()

    @classmethod
    def get_table_name(cls):
        if hasattr(cls, 'TABLE_NAME'):
            return cls.TABLE_NAME
        return cls.__name__

    @classmethod
    def get_primary_keys(cls) -> t.Sequence:
        if hasattr(cls, 'PRIMARY_KEYS'):
            return cls.PRIMARY_KEYS
        return tuple()

    @classmethod
    def make_property(cls, item: str, coerce=None, readonly: bool = False, primary_key: bool = False):
        if primary_key:
            cls.register_primary_key(item)
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=coerce, readonly=readonly)
        )

    @classmethod
    def make_datetime_property(cls, item: str, readonly: bool = False, primary_key: bool = False):
        if primary_key:
            cls.register_primary_key(item)
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=_NODBBaseObject.to_datetime, readonly=readonly)
        )

    @classmethod
    def make_date_property(cls, item: str, readonly: bool = False, primary_key: bool = False):
        if primary_key:
            cls.register_primary_key(item)
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=_NODBBaseObject.to_date, readonly=readonly)
        )

    @classmethod
    def make_enum_property(cls, item: str, enum_cls: type, readonly: bool = False, primary_key: bool = False):
        if primary_key:
            cls.register_primary_key(item)
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=_NODBBaseObject.to_enum(enum_cls), readonly=readonly)
        )

    @classmethod
    def make_wkt_property(cls, item: str, readonly: bool = False):
        # TODO: currently a string but could add better validation
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=str, readonly=readonly)
        )

    @classmethod
    def make_json_property(cls, item: str):
        return property(
            functools.partial(_NODBBaseObject.get, item=item),
            functools.partial(_NODBBaseObject.set, item=item, coerce=_NODBBaseObject.to_json)
        )

    @staticmethod
    def to_json(x):
        if isinstance(x, str) and x[0] in ('[', '{'):
            return json.loads(x)
        return x

    @classmethod
    def register_primary_key(cls, item_name: str):
        if not hasattr(cls, '_primary_keys'):
            setattr(cls, '_primary_keys', set())
        getattr(cls, '_primary_keys').add(item_name)

    @staticmethod
    def to_enum(enum_cls):
        def _coerce(x):
            if isinstance(x, str):
                return enum_cls(x)
            return x
        return _coerce

    @staticmethod
    def to_datetime(dt):
        if isinstance(dt, str):
            return datetime.datetime.fromisoformat(dt)
        else:
            return dt

    @staticmethod
    def to_date(dt):
        if isinstance(dt, str):
            return datetime.date.fromisoformat(dt)
        else:
            return dt

    def validate_in_list(self, value: t.Any, valid_options: t.Sequence[t.Any], allow_none: bool = False, message: str = None) -> bool:
        if value is None:
            if not allow_none:
                raise NODBValidationError(message or f"Expected not-None found None for [{self.identifier()}]")
        elif value not in valid_options:
            opts = ','.join(str(x) for x in valid_options)
            raise NODBValidationError(message or f"Expected one of [{opts}] found [{value}] for [{self.identifier()}]")
        return True

    def identifier(self) -> str:
        return f"{self.__class__.__name__}"


class _NODBWithMetadata:

    metadata: t.Optional[dict] = _NODBBaseObject.make_property("metadata")

    def set_metadata(self, key, value):
        if self.metadata is None:
            self.metadata = {key: value}
            self.modified_values.add("metadata")
        else:
            self.metadata[key] = value
            self.modified_values.add("metadata")

    def clear_metadata(self, key):
        if self.metadata is None:
            return
        if key not in self.metadata:
            return
        del self.metadata[key]
        self.modified_values.add("metadata")
        if not self.metadata:
            self.metadata = None

    def get_metadata(self, key, default=None):
        if self.metadata is None or key not in self.metadata:
            return default
        return self.metadata[key]

    def add_to_metadata(self, key, value):
        if self.metadata is None:
            self.metadata = {key: [value]}
            self.modified_values.add("metadata")
        elif key not in self.metadata:
            self.metadata[key] = [value]
            self.modified_values.add("metadata")
        elif value not in self.metadata[key]:
            self.metadata[key].append(value)
            self.modified_values.add("metadata")


class NODBQueueItem(_NODBBaseObject):

    TABLE_NAME: str = "nodb_queues"
    PRIMARY_KEYS: tuple[str] = ("queue_uuid",)

    queue_uuid: str = _NODBBaseObject.make_property("queue_uuid", coerce=str)
    created_date: datetime.datetime = _NODBBaseObject.make_datetime_property("created_date", readonly=True)
    modified_date: datetime.datetime = _NODBBaseObject.make_datetime_property("modified_date", readonly=True)
    status: QueueStatus = _NODBBaseObject.make_enum_property("status", QueueStatus)
    locked_by: t.Optional[str] = _NODBBaseObject.make_property("locked_by", coerce=str, readonly=True)
    locked_since: t.Optional[datetime.datetime] = _NODBBaseObject.make_datetime_property("locked_since", readonly=True)
    queue_name: str = _NODBBaseObject.make_property("queue_name", readonly=True, coerce=str)
    subqueue_name: str = _NODBBaseObject.make_property("subqueue_name", readonly=True, coerce=str)
    unique_item_name: t.Optional[str] = _NODBBaseObject.make_property("unique_item_name", readonly=True, coerce=str)
    priority: t.Optional[int] = _NODBBaseObject.make_property("priority", readonly=True, coerce=int)
    data: dict = _NODBBaseObject.make_json_property("data")

    def mark_complete(self, db: NODBControllerInstance):
        self.set_queue_status(db, QueueStatus.COMPLETE)

    def mark_failed(self, db: NODBControllerInstance):
        self.set_queue_status(db, QueueStatus.ERROR)

    def release(self, db: NODBControllerInstance, release_in_seconds: t.Optional[int] = None, reduce_priority: bool = False):
        if release_in_seconds is None or release_in_seconds <= 0:
            self.set_queue_status(db, QueueStatus.UNLOCKED, reduce_priority=reduce_priority)
        else:
            self.set_queue_status(
                db,
                QueueStatus.DELAYED_RELEASE,
                datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=release_in_seconds),
                reduce_priority=reduce_priority
            )

    def renew(self, db: NODBControllerInstance):
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
                         reduce_priority: bool = False):
        if self.status == QueueStatus.LOCKED:
            with db.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.TABLE_NAME}
                    SET
                        status = %s,
                        locked_by = NULL,
                        locked_since = NULL,
                        delay_release = %s
                        priority -= %s
                    WHERE 
                        queue_uuid = %s
                        AND status = 'LOCKED'
                """, [
                    new_status.value,
                    release_at,
                    1 if reduce_priority else 0,
                    self.queue_uuid
                ])
                # TODO: check if the row was actually updated?
                self.status = new_status


class NODBSourceFile(_NODBWithMetadata, _NODBBaseObject):

    TABLE_NAME: str = "nodb_source_files"
    PRIMARY_KEYS: tuple[str] = ("source_uuid", "received_date",)

    source_uuid: str = _NODBBaseObject.make_property("source_uuid", coerce=str)
    received_date: datetime.date = _NODBBaseObject.make_date_property("received_date")

    source_path: str = _NODBBaseObject.make_property("source_path", coerce=str)
    file_name: str = _NODBBaseObject.make_property("file_name", coerce=str)

    original_uuid: str = _NODBBaseObject.make_property("original_uuid", coerce=str)
    original_idx: int = _NODBBaseObject.make_property("original_idx", coerce=int)

    status: SourceFileStatus = _NODBBaseObject.make_enum_property("status", SourceFileStatus)

    history: list = _NODBBaseObject.make_property("history")

    def report_error(self, message, name, version, instance):
        self.add_history(message, name, version, instance, 'ERROR')

    def report_warning(self, message, name, version, instance):
        self.add_history(message, name, version, instance, 'WARNING')

    def add_history(self, message, name, version, instance, level='INFO'):
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
        return db.load_object(cls, {
            'source_path': source_path
        }, **kwargs)

    @classmethod
    def find_by_original_info(cls, db: NODBControllerInstance, original_uuid: str, received_date: t.Union[datetime.date, str], message_idx: int, **kwargs):
        return db.load_object(cls, {
            'original_idx': message_idx,
            'received_date': parse_received_date(received_date),
            'original_uuid': original_uuid
        }, **kwargs)

    @classmethod
    def find_by_uuid(cls, db: NODBControllerInstance, source_uuid: str, received: t.Union[datetime.date, str], **kwargs):
        return db.load_object(cls, {
            'source_uuid': source_uuid,
            'received_date': parse_received_date(received)
        }, **kwargs)


class NODBUser(_NODBBaseObject):

    TABLE_NAME = "nodb_users"
    PRIMARY_KEYS: tuple[str] = ("username",)

    username: str = _NODBBaseObject.make_property("username", coerce=str)
    phash: bytes = _NODBBaseObject.make_property("phash")
    salt: bytes = _NODBBaseObject.make_property("salt")
    old_phash: bytes = _NODBBaseObject.make_property("old_phash")
    old_salt: bytes = _NODBBaseObject.make_property("old_salt")
    old_expiry: datetime = _NODBBaseObject.make_datetime_property("old_expiry")
    status: UserStatus = _NODBBaseObject.make_enum_property("status", UserStatus)
    roles: list = _NODBBaseObject.make_property("roles")

    def assign_role(self, role_name):
        if self.roles is None:
            self.roles = [role_name]
            self.modified_values.add('roles')
        elif role_name not in self.roles:
            self.roles.append(role_name)
            self.modified_values.add('roles')

    def unassign_role(self, role_name):
        if self.roles is not None and role_name in self.roles:
            self.roles.remove(role_name)
            self.modified_values.add('roles')

    def set_password(self, new_password, salt_length: int = 16, old_expiry_seconds: int = 0):
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
        if self.old_expiry is not None and self.old_expiry <= datetime.datetime.now(datetime.timezone.utc):
            self.old_phash = None
            self.old_salt = None
            self.old_expiry = None

    def permissions(self, db: NODBControllerInstance) -> set:
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
        if not isinstance(password, str):
            raise CNODCError("Invalid password", "USERCHECK", 1000)
        return hashlib.pbkdf2_hmac('sha512', password.encode('utf-8', errors="replace"), salt, iterations)

    @classmethod
    def find_by_username(cls, db, username: str, **kwargs):
        return db.load_object(cls, {"username": username}, **kwargs)


class NODBSession(_NODBBaseObject):

    TABLE_NAME: str = "nodb_sessions"
    PRIMARY_KEYS: tuple[str] = ("session_id",)

    session_id: str = _NODBBaseObject.make_property("session_id", coerce=str)
    start_time: datetime = _NODBBaseObject.make_datetime_property("start_time")
    expiry_time: datetime = _NODBBaseObject.make_datetime_property("expiry_time")
    username: str = _NODBBaseObject.make_property("username", coerce=str)
    session_data: dict = _NODBBaseObject.make_property("session_data")

    def set_session_value(self, key, value):
        if self.session_data is None:
            self.session_data = {}
        self.session_data[key] = value

    def get_session_value(self, key, default=None):
        return self.session_data[key] if self.session_data and key in self.session_data else default

    def is_expired(self) -> bool:
        return self.expiry_time < datetime.datetime.now(datetime.timezone.utc)

    @classmethod
    def find_by_session_id(cls, db, session_id: str, **kwargs):
        return db.load_object(cls, {"session_id": session_id}, **kwargs)


class NODBUploadWorkflow(_NODBBaseObject):

    TABLE_NAME = "nodb_upload_workflows"
    PRIMARY_KEYS = ("workflow_name",)

    workflow_name: str = _NODBBaseObject.make_property("workflow_name", coerce=str)
    configuration: dict[str, t.Any] = _NODBBaseObject.make_property("configuration")
    is_active: bool = _NODBBaseObject.make_property('is_active', coerce=bool)

    def permissions(self):
        return self.get_config('permission', default=None)

    def get_config(self, config_key: str, default=None):
        if self.configuration and config_key in self.configuration:
            return self.configuration[config_key]
        return default

    @injector.inject
    def check_config(self, files: cnodc.storage.core.StorageController):
        if 'validation' in self.configuration and self.configuration['validation'] is not None:
            try:
                x = dynamic_object(self.configuration['validation'])
                if not callable(x):
                    raise CNODCError(
                        f'Invalid value for [validation]: {self.configuration["validation"]}, must be a Python callable', 'WFCHECK', 1001)
            except DynamicObjectLoadError:
                raise CNODCError(f'Invalid value for [validation]: {self.configuration["validation"]}, must be a Python object', 'WFCHECK', 1002)
        has_upload = False
        if 'working_target' in self.configuration and self.configuration['working_target']:
            has_upload = True
            self._check_upload_target_config(self.configuration['working_target'], files, 'working')
        if 'additional_targets' in self.configuration and self.configuration['additional_targets']:
            has_upload = True
            for idx, target in enumerate(self.configuration['additional_targets']):
                self._check_upload_target_config(target, files, f'additional{idx}')
        if not has_upload:
            raise CNODCError(f"Workflow missing either upload or archive URL", "WFCHECK", 1008)
        if 'upload_tier' in self.configuration and self.configuration['upload_tier']:
            if not ('upload' in self.configuration and self.configuration['upload']):
                raise CNODCError('Workflow specifies an [upload_tier] without an [upload]', 'WFCHECK', 1009)
            if self.configuration['upload_tier'] not in ('frequent', 'infrequent', 'archival'):
                raise CNODCError(f'Invalid value for [upload_tier]: {self.configuration["upload_tier"]}, expecting (frequent|infrequent|archival)', "WFCHECK", 1010)
        if 'archive_tier' in self.configuration and self.configuration['archive_tier']:
            if not ('archive' in self.configuration and self.configuration['archive']):
                raise CNODCError('Workflow specifies an [archive_tier] without an [archive]', 'WFCHECK', 1011)
            if self.configuration['archive_tier'] not in ('frequent', 'infrequent', 'archival'):
                raise CNODCError(f'Invalid value for [archive_tier]: {self.configuration["archive_tier"]}, expecting (frequent|infrequent|archival)', "WFCHECK", 1012)
        # TODO: check default_headers is a dict[str, str]
        # TODO: check filename_pattern is a string or missing
        # TODO: check processing steps

    def _check_upload_target_config(self, config: dict, files: StorageController, tn: str):
        if 'directory' not in config:
            raise CNODCError(f'Target directory missing in [{tn}]', 'WFCHECK', 1006)
        try:
            _ = files.get_handle(config['directory'])
        except Exception as ex:
            raise CNODCError(f'Target directory is not supported by storage subsystem in [{tn}]', 'WFCHECK', 1007) from ex
        if 'allow_overwrite' in config and config['allow_overwrite'] not in ('user', 'always', 'never'):
            raise CNODCError(f'Overwrite setting must be one of [user|always|never] in [{tn}]', 'WFCHECK', 1000)
        if 'tier' in config:
            try:
                _ = StorageTier(config['tier'])
            except Exception as ex:
                raise CNODCError(f'Tier value [{config["tier"]} is not supported in [{tn}]', 'WFCHECK', 1015) from ex
        if 'metadata' in config and config['metadata']:
            if not isinstance(config['metadata'], dict):
                raise CNODCError(f"Invalid value for [metadata] in [{tn}]: must be a dictionary", "WFCHECK", 1003)
            for x in self.configuration['metadata'].keys():
                if not isinstance(x, str):
                    raise CNODCError(f"Invalid key for [metadata] in [{tn}]: {x}, must be a string", "WFCHECK", 1004)
                if not isinstance(self.configuration['metadata'][x], str):
                    raise CNODCError(f'Invalid value for [metadata.{x}] in [{tn}]: {self.configuration["metadata"][x]}, must be a string', 'WFCHECK', 1005)

    def check_access(self, user_permissions: t.Union[list, set, tuple]):
        if '_admin' in user_permissions:
            return True
        needed_permissions = self.permissions()
        if '_any' in needed_permissions:
            return True
        return any(x in user_permissions for x in needed_permissions)

    @classmethod
    def find_by_name(cls, db, workflow_name: str, **kwargs):
        return db.load_object(cls, {"workflow_name": workflow_name},  **kwargs)

    @classmethod
    def find_all(cls, db, **kwargs):
        with db.cursor() as cur:
            cur.execute(f'SELECT * FROM {NODBUploadWorkflow.TABLE_NAME}')
            for row in cur.fetch_stream():
                yield NODBUploadWorkflow(**row, is_new=False)



class NODBObservation(_NODBBaseObject):

    TABLE_NAME = "nodb_obs"
    PRIMARY_KEYS = ("obs_uuid", "received_date")

    obs_uuid: str = _NODBBaseObject.make_property("obs_uuid", coerce=str)
    received_date: datetime.date = _NODBBaseObject.make_date_property("received_date")

    station_uuid: str = _NODBBaseObject.make_property("station_uuid", coerce=str)
    mission_name: str = _NODBBaseObject.make_property("mission_name", coerce=str)
    source_name: str = _NODBBaseObject.make_property("source_name", coerce=str)
    instrument_type: str = _NODBBaseObject.make_property("instrument_type", coerce=str)
    program_name: str = _NODBBaseObject.make_property("program_name", coerce=str)
    obs_time: datetime.datetime = _NODBBaseObject.make_datetime_property("obs_time")
    min_depth: float = _NODBBaseObject.make_property("min_depth", coerce=float)
    max_depth: float = _NODBBaseObject.make_property("max_depth", coerce=float)
    location: str = _NODBBaseObject.make_wkt_property("location")
    observation_type: ObservationType = _NODBBaseObject.make_enum_property("observation_type", ObservationType)
    surface_parameters: list = _NODBBaseObject.make_property("surface_parameters")
    profile_parameters: list = _NODBBaseObject.make_property("profile_parameters")
    processing_level: ProcessingLevel = _NODBBaseObject.make_enum_property("processing_level", ProcessingLevel)
    embargo_date: datetime.datetime = _NODBBaseObject.make_datetime_property("embargo_date")

    @classmethod
    def find_by_uuid(cls, db, obs_uuid: str, received_date: t.Union[str, datetime.date], **kwargs):
        return db.load_object(cls, {
            "obs_uuid": obs_uuid,
            "received_date": parse_received_date(received_date)
        }, **kwargs)


class NODBObservationData(_NODBBaseObject):

    TABLE_NAME = "nodb_obs_data"
    PRIMARY_KEYS = ("obs_uuid", "received_date")

    obs_uuid: str = _NODBBaseObject.make_property("obs_uuid", coerce=str)
    received_date: datetime.date = _NODBBaseObject.make_date_property("received_date")
    source_file_uuid: str = _NODBBaseObject.make_property("source_file_uuid", coerce=str)
    message_idx: int = _NODBBaseObject.make_property("message_idx", coerce=int)
    record_idx: int = _NODBBaseObject.make_property("record_idx", coerce=int)
    data_record: t.Optional[bytes] = _NODBBaseObject.make_property("data_record")
    process_metadata: dict = _NODBBaseObject.make_property("process_metadata")
    qc_tests: dict = _NODBBaseObject.make_property("qc_tests")
    duplicate_uuid: str = _NODBBaseObject.make_property("duplicate_uuid", coerce=str)
    duplicate_received_date: datetime.date = _NODBBaseObject.make_date_property("duplicate_received_date")
    status: ObservationStatus = _NODBBaseObject.make_enum_property("status", ObservationStatus)

    def get_process_metadata(self, key, default=None):
        if self.process_metadata and key in self.process_metadata:
            return self.process_metadata[key]
        return default

    def set_process_metadata(self, key, value):
        if self.process_metadata is None:
            self.process_metadata = {}
        self.process_metadata[key] = value
        self.modified_values.add('process_metadata')

    @classmethod
    def find_by_uuid(cls, db, obs_uuid: str, received_date: t.Union[str, datetime.date], *args, **kwargs):
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

        return db.load_object(cls, {
                "received_date": parse_received_date(source_received_date),
                "source_file_uuid": source_file_uuid,
                "message_idx": message_idx,
                "record_idx": record_idx
            }, *args, **kwargs)

    @property
    def record(self) -> t.Optional[DataRecord]:
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
    def record(self, data_record: DataRecord):
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
                    compression='LZMA6CRC4',
                    correction=None):
                ba.extend(byte_)
            self.data_record = ba
            self.mark_modified('data_record')


class NODBStation(_NODBBaseObject):

    TABLE_NAME = 'nodb_stations'
    PRIMARY_KEYS = ('station_uuid', )

    station_uuid: str = _NODBBaseObject.make_property("station_uuid", coerce=str)
    wmo_id: str = _NODBBaseObject.make_property("wmo_id", coerce=str)
    wigos_id: str = _NODBBaseObject.make_property("wigos_id", coerce=str)
    station_name: str = _NODBBaseObject.make_property("station_name", coerce=str)
    station_id: str = _NODBBaseObject.make_property("station_id", coerce=str)
    station_type: str = _NODBBaseObject.make_property("station_type", coerce=str)
    service_start_date: datetime.datetime = _NODBBaseObject.make_datetime_property("service_start_date")
    service_end_date: datetime.datetime = _NODBBaseObject.make_datetime_property("service_end_date")
    instrumentation: dict = _NODBBaseObject.make_property('instrumentation')
    metadata: dict = _NODBBaseObject.make_property('metadata')
    map_to_uuid: str = _NODBBaseObject.make_property("map_to_uuid", coerce=str)
    status: StationStatus = _NODBBaseObject.make_enum_property("status", StationStatus)
    embargo_data_days: int = _NODBBaseObject.make_property('embargo_data_days', coerce=int)

    def get_metadata(self, metadata_key, default=None):
        if self.metadata is not None and metadata_key in self.metadata:
            return self.metadata[metadata_key] or default
        return None

    @classmethod
    def search(cls,
               db: NODBControllerInstance,
               in_service_time: t.Optional[datetime.datetime] = None,
               wmo_id: t.Optional[str] = None,
               wigos_id: t.Optional[str] = None,
               station_id: t.Optional[str] = None,
               station_name: t.Optional[str] = None) -> list[NODBStation]:
        with db.cursor() as cur:
            args = []
            clauses = []
            if wmo_id is not None and wmo_id != '':
                args.append(wmo_id)
                clauses.append('wmo_id = %s')
            if wigos_id is not None and wigos_id != '':
                args.append(wigos_id)
                clauses.append('wigos_id = %s')
            if station_id is not None and station_id != '':
                args.append(station_id)
                clauses.append('station_id = %s')
            if station_name is not None and station_name != '':
                args.append(station_name)
                clauses.append('station_name = %s')
            if not args:
                return []
            if in_service_time is not None:
                clauses.append('((service_start_date IS NULL OR service_start_date <= %s) AND (service_end_date IS NULL or service_end_date >= %s)')
                args.extend([in_service_time, in_service_time])
            query = f"SELECT * FROM {NODBStation.TABLE_NAME} WHERE " + ' OR '.join(clauses)
            cur.execute(query, args)
            return [NODBStation(is_new=False, **x) for x in cur.fetch_all()]

    @classmethod
    def find_by_uuid(cls, db: NODBControllerInstance, station_uuid: str, **kwargs):
        return db.load_object(cls, {
            'station_uuid': station_uuid
        }, **kwargs)

    @classmethod
    def find_all_raw(cls, db: NODBControllerInstance):
        with db.cursor() as cur:
            cur.execute(f"SELECT * FROM {NODBStation.TABLE_NAME}")
            for row in cur.fetch_stream():
                yield row


class NODBWorkingRecord(_NODBBaseObject):

    TABLE_NAME = "nodb_working"
    PRIMARY_KEYS = ("working_uuid",)

    working_uuid: str = _NODBBaseObject.make_property("working_uuid", coerce=str)
    received_date: datetime.date = _NODBBaseObject.make_date_property("received_date")
    source_file_uuid: str = _NODBBaseObject.make_property("source_file_uuid", coerce=str)
    message_idx: int = _NODBBaseObject.make_property("message_idx", coerce=int)
    record_idx: int = _NODBBaseObject.make_property("record_idx", coerce=int)
    data_record: t.Optional[bytes] = _NODBBaseObject.make_property("data_record")
    qc_metadata: dict = _NODBBaseObject.make_property("qc_metadata")
    qc_batch_id: str = _NODBBaseObject.make_property("qc_batch_id", coerce=str)
    station_uuid: str = _NODBBaseObject.make_property("station_uuid", coerce=str)
    obs_time: datetime.datetime = _NODBBaseObject.make_datetime_property("obs_time")
    location: str = _NODBBaseObject.make_wkt_property("location")
    record_uuid: str = _NODBBaseObject.make_property("record_uuid", coerce=str)

    def set_metadata(self, key, value):
        if self.qc_metadata is None:
            self.qc_metadata = {}
        self.qc_metadata[key] = value
        self.mark_modified('qc_metadata')

    def get_metadata(self, key, default=None):
        if self.qc_metadata is None or key not in self.qc_metadata:
            return default
        return self.qc_metadata[key]

    @classmethod
    def find_by_uuid(cls, db, obs_uuid: str,*args, **kwargs):
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

        return db.load_object(cls, {
                "received_date": parse_received_date(source_received_date),
                "source_file_uuid": source_file_uuid,
                "message_idx": message_idx,
                "record_idx": record_idx
            }, *args, **kwargs)

    @property
    def record(self) -> t.Optional[DataRecord]:
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
    def record(self, data_record: DataRecord):
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
                    codec='JSON',
                    compression='LZMA6CRC4',
                    correction=None):
                ba.extend(byte_)
            self.data_record = ba
            self.mark_modified('data_record')

    def _update_from_data_record(self, data_record: DataRecord):
        if data_record.coordinates.has_value('Time') and data_record.coordinates['Time'].is_iso_datetime():
            self.obs_time = data_record.coordinates['Time'].to_datetime()
        if data_record.coordinates.has_value('Latitude') and data_record.coordinates['Latitude'].is_numeric() and data_record.coordinates.has_value('Longitude') and data_record.coordinates['Longitude'].is_numeric():
            lat = data_record.coordinates['Latitude'].to_float()
            lon = data_record.coordinates['Longitude'].to_float()
            self.location = f"POINT ({round(lon, 5)} {round(lat, 5)})"
        if data_record.metadata.has_value('CNODCStation'):
            self.station_uuid = data_record.metadata['CNODCStation'].best_value()

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
    status: BatchStatus = _NODBBaseObject.make_enum_property("status", BatchStatus)

    @classmethod
    def find_by_uuid(cls, db: NODBControllerInstance, batch_uuid: str, **kwargs):
        return db.load_object(cls, {
            'batch_uuid': batch_uuid
        }, **kwargs)

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






"""



class _NODBWithQCMetdata(_NODBWithMetadata):

    qc_metadata: t.Optional[dict] = _NODBBaseObject.make_property("qc_metadata")

    def set_qc_metadata(self, key, value):
        if self.qc_metadata is None:
            self.qc_metadata = {key: value}
            self.modified_values.add("qc_metadata")
        else:
            self.qc_metadata[key] = value
            self.modified_values.add("qc_metadata")

    def clear_qc_metadata(self, key):
        if self.qc_metadata is None:
            return
        if key not in self.qc_metadata:
            return
        del self.qc_metadata[key]
        self.modified_values.add("qc_metadata")
        if not self.qc_metadata:
            self.qc_metadata = None

    def get_qc_metadata(self, key, default=None):
        if self.qc_metadata is None or key not in self.qc_metadata:
            return default
        return self.qc_metadata[key]

    def add_to_qc_metadata(self, key, value):
        if self.qc_metadata is None:
            self.qc_metadata = {key: [value]}
            self.modified_values.add("qc_metadata")
        elif key not in self.qc_metadata:
            self.qc_metadata[key] = [value]
            self.modified_values.add("qc_metadata")
        elif value not in self.qc_metadata[key]:
            self.qc_metadata[key].append(value)
            self.modified_values.add("qc_metadata")

    def apply_qc_code(self, qc_message_code):
        self.add_to_qc_metadata("qc_codes", qc_message_code)

    def has_qc_code(self, qc_message_code):
        return self.qc_metadata and 'qc_codes' in self.qc_metadata and qc_message_code in self.qc_metadata['qc_codes']

    def has_any_qc_code(self):
        return self.qc_metadata and 'qc_codes' in self.qc_metadata and self.qc_metadata['qc_codes']

    def clear_qc_codes(self):
        if self.qc_metadata and 'qc_codes' in self.qc_metadata:
            del self.qc_metadata['qc_codes']


class _NODBWithQCProperties(_NODBWithQCMetdata):

    qc_test_status: QualityControlStatus = _NODBBaseObject.make_enum_property("qc_test_status", QualityControlStatus)
    qc_process_name: str = _NODBBaseObject.make_property("qc_process_name", coerce=str)
    qc_current_step: int = _NODBBaseObject.make_property("qc_current_step", coerce=int)
    working_status: ObservationWorkingStatus = _NODBBaseObject.make_enum_property("working_status", ObservationWorkingStatus)


class _NODBWithDataRecord:

    data_record: t.Union[bytes, bytearray] = _NODBBaseObject.make_property("data_record")
    _data_record_cache: t.Optional[DataRecord] = None

    def extract_data_record(self) -> DataRecord:
        if self._data_record_cache is None:
            if self.data_record is not None:
                codec = OCProc2BinaryCodec()
                self._data_record_cache = codec.decode([self.data_record])
        return self._data_record_cache

    def store_data_record(self, dr: DataRecord, **kwargs):
        self._data_record_cache = dr
        codec = OCProc2BinaryCodec()
        new_data = bytearray()
        for bytes_ in codec.encode(dr, **kwargs):
            new_data.extend(bytes_)
        self.data_record = new_data

    def clear_data_record_cache(self):
        self._data_record_cache = None



class NODBQCBatch(_NODBWithQCProperties, _NODBBaseObject):
    pass


class NODBQCProcess(_NODBBaseObject):

    version_no: int = _NODBBaseObject.make_property("version_no", coerce=int)
    rt_qc_steps: list = _NODBBaseObject.make_property("rt_qc_steps")
    dm_qc_steps: list = _NODBBaseObject.make_property("dm_qc_steps")
    dm_qc_freq_days: int = _NODBBaseObject.make_property("dm_qc_freq_days", coerce=int)
    dm_qc_delay_days: int = _NODBBaseObject.make_property("dm_qc_delay_days", coerce=int)

    def has_rt_qc(self) -> bool:
        return bool(self.rt_qc_steps)


class NODBWorkingObservation(_NODBWithDataRecord, _NODBWithQCProperties, _NODBBaseObject):

    qc_batch_id: str = _NODBBaseObject.make_property("qc_batch_id", coerce=str)

    station_uuid: str = _NODBBaseObject.make_property("station_uuid", coerce=str)

    def mark_qc_test_complete(self, qc_test_name):
        self.add_to_metadata("qc_tests", qc_test_name)

    def qc_test_completed(self, qc_test_name):
        return self.metadata and "qc_tests" in self.metadata and qc_test_name in self.metadata["qc_tests"]

    @staticmethod
    def create_from_primary(primary_obs):
        working_obs = NODBWorkingObservation(primary_obs.pkey)
        working_obs.data_record = primary_obs.data_record
        working_obs._data_record_cache = primary_obs._data_record_cache
        working_obs.station_uuid = primary_obs.station_uuid
        working_obs.metadata = primary_obs.metadata
        return working_obs


class NODBObservation(_NODBWithDataRecord, _NODBWithMetadata, _NODBBaseObject):

    source_file_uuid: str = _NODBBaseObject.make_property("source_file_uuid", coerce=str)
    message_idx: int = _NODBBaseObject.make_property("message_idx", coerce=int)
    record_idx: int = _NODBBaseObject.make_property("record_idx", coerce=int)

    station_uuid: str = _NODBBaseObject.make_property("station_uuid", coerce=str)
    mission_name: str = _NODBBaseObject.make_property("mission_name", coerce=str)

    obs_time: t.Optional[datetime.datetime] = _NODBBaseObject.make_datetime_property("obs_time")
    latitude: t.Optional[float] = _NODBBaseObject.make_property("latitude", coerce=float)
    longitude: t.Optional[float] = _NODBBaseObject.make_property("longitude", coerce=float)

    status: ObservationStatus = _NODBBaseObject.make_enum_property("status", ObservationStatus)
    duplicate_uuid: str = _NODBBaseObject.make_property("duplicate_uuid", coerce=str)

    search_data: dict = _NODBBaseObject.make_property("search_data")

    def update_from_working(self, working_record: NODBWorkingObservation):
        self.station_uuid = working_record.station_uuid
        self.metadata = working_record.metadata
        self.data_record = working_record.data_record
        self.data_record_cache = working_record._data_record_cache
        self.update_from_data_record(working_record.extract_data_record())

    def update_from_data_record(self, data_record: DataRecord):
        search_data_agg = _SearchDataAggregator(data_record)
        self.latitude = search_data_agg.statistics['_rpr_lat']
        self.longitude = search_data_agg.statistics['_rpr_lon']
        self.obs_time = search_data_agg.statistics['_rpr_time']
        # Underscore attributes are for other purposes
        self.search_data = {
            x: search_data_agg.statistics[x]
            for x in search_data_agg.statistics
            if x[0] != '_'
        }


def searchable_time(t: str):
    if t is None:
        return None
    # TODO: ensure UTC
    dt = datetime.datetime.fromisoformat(t)
    return int(dt.strftime("%Y%m%d%H%M%S"))


class _SearchDataAggregator:

    def __init__(self, data_record: DataRecord):
        self.statistics: dict[str, t.Union[str, set, list, float, int, None]] = {
            'record_type': self._detect_geometry(data_record),
            '_rpr_lat': None,
            '_rpr_lon': None,
            '_rpr_time': None
        }
        self._internals = {
            'coordinates': [],
            'time': []
        }
        self._consider_record(data_record)
        if (data_record.coordinates.has_value('LAT')
                and data_record.coordinates.has_value('LON')
                and data_record.coordinates['LAT'].nodb_flag != NODBQCFlag.BAD
                and data_record.coordinates['LON'].value != NODBQCFlag.BAD):
            self.statistics['_rpr_lat'] = data_record.coordinates.get_value('LAT')
            self.statistics['_rpr_lon'] = data_record.coordinates.get_value('LON')
        if data_record.coordinates.has_value('TIME') and data_record.coordinates['TIME'].nodb_flag != NODBQCFlag.BAD:
            self.statistics['_rpr_time'] = data_record.coordinates['TIME'].as_datetime()
        if self.statistics['_rpr_lat'] is None or self.statistics['_rpr_lon'] is None or self.statistics['_rpr_time'] is None:
            rlat, rlon, rtime = self._estimate_best_position()
            if self.statistics['_rpr_lat'] is None:
                self.statistics['_rpr_lat'] = rlat
            if self.statistics['_rpr_lon'] is None:
                self.statistics['_rpr_lon'] = rlon
            if self.statistics['_rpr_time'] is None:
                self.statistics['_rpr_time'] = rtime
        for x in self.statistics:
            if isinstance(self.statistics[x], set):
                self.statistics[x] = list(self.statistics[x])

    def _estimate_best_position(self) -> tuple[t.Optional[float], t.Optional[float], t.Optional[float]]:
        # No entries, no position
        if not self._internals['coordinates']:
            return None, None, self._estimate_best_time()
        # One entry, one position
        if len(self._internals['coordinates']) == 1:
            return self._internals['coordinates'][0][0], self._internals['coordinates'][0][1], self._estimate_best_time()
        # Use a geodetic mean of the coordinates. This is slow but coordinates is usually going to be small in size
        mean = ocean_math.mean_vector(self._internals['coordinates'])
        return mean[0], mean[1], self._estimate_best_time()

    def _estimate_best_time(self) -> t.Optional[float]:
        # No entries, no time
        if not self._internals['time']:
            return None
        # One entry, one time
        if len(self._internals['time']) == 1:
            return self._internals['time'][0]
        # Find the first time
        min_time = min(x for x in self._internals['time'])
        # Calculate the average number of seconds since the earliest time for all records
        total_diff = 0
        entries = 0
        for x in self._internals['time']:
            total_diff += (x - min_time).total_seconds()
            entries += 1
        # Calculate an average
        return min_time + datetime.timedelta(seconds=(total_diff / entries))

    def _consider_record(self, record: DataRecord):
        lat = self._add_coordinate_statistics(record, 'LAT')
        lon = self._add_coordinate_statistics(record, 'LON')
        if lat is not None and lon is not None:
            self._internals['coordinates'].append((lon, lat))
        self._add_coordinate_statistics(record, 'DEPTH')
        self._add_coordinate_statistics(record, 'PRESSURE')
        time = self._add_coordinate_statistics(record, 'TIME', coerce=searchable_time)
        if time is not None:
            self._internals['time'].append(datetime.datetime.strptime(str(time), "%Y%m%d%H%M%S"))
        if 'vars' not in self.statistics:
            self.statistics['vars'] = set()
        self.statistics['vars'].update(vname for vname in record.variables)
        self.statistics['vars'].update(vname for vname in record.coordinates)
        if record.subrecords:
            if 'child_types' not in self.statistics:
                self.statistics['child_types'] = set()
            for srs_name in record.subrecords:
                # Omit last component since it is the index
                self.statistics['child_types'].add(srs_name[:srs_name.rfind("_")])
                for sr in record.subrecords[srs_name]:
                    self._consider_record(sr)

    def _detect_geometry(self, parent_record: DataRecord):

        # Level 1
        parent_coords = set(x for x in parent_record.coordinates if parent_record.coordinates[x].value() is not None)

        # Level 2
        subrecord_types = parent_record.subrecords.record_types()

        # Level 3
        subsubrecord_types = set()
        for srt in parent_record.subrecords:
            for record in parent_record.subrecords[srt]:
                subsubrecord_types.update(record.subrecords.record_types())

        # X, Y, T specified (point or profile)
        if 'LAT' in parent_coords and 'LON' in parent_coords and 'TIME' in parent_coords:

            # Profiles
            if 'PROFILE' in subrecord_types:
                return 'profile'

            # At-depth observation
            elif 'DEPTH' in parent_coords or 'PRESSURE' in parent_coords:
                return 'at_depth'

            # Surface observation
            elif 'TSERIES' not in subrecord_types and 'TRAJ' not in subrecord_types:
                return 'surface'

        # No TIME specified, but has TIMESERIES means it is a time series at a fixed station
        elif 'LAT' in parent_coords and 'LON' in parent_coords and 'TSERIES' in subrecord_types:
            return 'time_series_profile' if 'PROFILE' in subsubrecord_types else 'time_series'

        # No LAT, LON, or TIME means a trajectory
        elif 'LAT' not in parent_coords and 'LON' not in parent_coords and 'TIME' not in parent_coords and 'TRAJ' in subrecord_types:
            return 'trajectory_profile' if 'PROFILE' in subsubrecord_types else 'trajectory'

        return 'other'

    def _add_coordinate_statistics(self, record: DataRecord, coordinate_name: str, coerce: callable = None):
        if record.coordinates.has_value(coordinate_name):
            coord = record.coordinates[coordinate_name]
            if coord.nodb_flag == NODBQCFlag.BAD:
                return None
            key = coordinate_name.lower()
            val = coerce(coord.value()) if coerce is not None else coord.value()
            if f"min_{key}" not in self.statistics or self.statistics[f"min_{key}"] > val:
                self.statistics[f"min_{key}"] = val
            if f"max_{key}" not in self.statistics or self.statistics[f"max_{key}"] < val:
                self.statistics[f"max_{key}"] = val
            return val
        return None



"""