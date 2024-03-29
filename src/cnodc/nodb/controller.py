"""Wrapper around psycopg2 that provides additional functionality
    to support the NODB."""
import contextlib
import datetime
import functools
import json
import tempfile
import enum
import psycopg2
import zrlog
import psycopg2 as pg
import psycopg2.extras as pge
import zirconium as zr
from autoinject import injector
import typing as t
from psycopg2._psycopg import cursor as PGCursor
import cnodc.nodb.structures as structures
from cnodc.util import CNODCError


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


class ScannedFileStatus(enum.Enum):
    """The status of a scanned file"""

    NOT_PRESENT = "0"
    UNPROCESSED = "1"
    PROCESSED = "2"


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


class NODBError(CNODCError):
    """Wrapper for NODB-related errors."""

    def __init__(self, msg, code, pgcode: str):
        super().__init__(
            f"Database error: {msg}",
            "NODB",
            code,
            pgcode in RECOVERABLE_ERRORS or f"{pgcode[0:2]}***" in RECOVERABLE_ERRORS
        )
        self.pgcode = pgcode

    def is_serialization_error(self):
        return self.pgcode in (
            SqlState.SERIALIZATION_FAILURE.value,
            SqlState.DEADLOCK_DETECTED.value
        )

    def sql_state(self) -> t.Optional[SqlState]:
        try:
            return SqlState(self.pgcode)
        except ValueError:
            return None


def wrap_nodb_exceptions(cb: callable):
    """Wrap postgres errors into NODB errors."""

    @functools.wraps(cb)
    def _inner(*args, **kwargs):
        try:
            return cb(*args, **kwargs)
        except psycopg2.Error as ex:
            raise NODBError(f"{ex.__class__.__name__}: {str(ex)} [{ex.pgcode}]", 1001, ex.pgcode) from ex
    return _inner


class _PGCursor:
    """Cursor class for postgresql."""

    def __init__(self, cursor, pg_conn):
        self._cursor = cursor
        self._conn = pg_conn
        self._log = zrlog.get_logger("cnodc.nodb")

    @wrap_nodb_exceptions
    def execute(self, query: str, args: t.Union[list, dict, tuple, None] = None):
        """Execute a query against the database."""
        self._log.debug(f"SQL Query: {query}")
        return self._cursor.execute(query, args)

    @wrap_nodb_exceptions
    def executemany(self, query: str, args_list: t.Iterable[t.Union[list, dict, tuple, None]] = None):
        """Execute a query multiple times."""
        return self._cursor.executemany(query, args_list)

    @wrap_nodb_exceptions
    def callproc(self, procname: str, parameters: t.Union[list, tuple, None] = None):
        """Call a stored procedure."""
        return self._cursor.callproc(procname, parameters)

    @wrap_nodb_exceptions
    def copy_expert(self, *args, **kwargs):
        """Copy records."""
        self._cursor.copy_expert(*args, **kwargs)

    @wrap_nodb_exceptions
    def fetchone(self):
        """Fetch a single record"""
        return self._cursor.fetchone()

    @wrap_nodb_exceptions
    def fetchall(self):
        """Fetch all records"""
        return self._cursor.fetchall()

    @wrap_nodb_exceptions
    def fetchmany(self, size=PGCursor.arraysize):
        """Fetch many records."""
        return self._cursor.fetchmany(size)

    @wrap_nodb_exceptions
    def fetch_stream(self, size=PGCursor.arraysize):
        """Fetch records in a stream using fetchmany() repeatedly."""
        res = self.fetchmany(size)
        while res:
            yield from res
            res = self.fetchmany(size)

    @wrap_nodb_exceptions
    def commit(self):
        """Commit the current transaction."""
        self._conn.commit()

    @wrap_nodb_exceptions
    def rollback(self):
        """Rollback the current transaction."""
        self._conn.rollback()

    def create_savepoint(self, name):
        """Create a save point."""
        # TODO: save point names should be validated?
        self._cursor.execute(f"SAVEPOINT {name}")

    def rollback_to_savepoint(self, name):
        """Rollback to a save point."""
        self._cursor.execute(f"ROLLBACK TO SAVEPOINT {name}")

    def release_savepoint(self, name):
        """Release a save point."""
        self._cursor.execute(f"RELEASE SAVEPOINT {name}")


class NODBControllerInstance:
    """Wrapper around a postgresql connection with NODB support"""

    def __init__(self, conn):
        self._conn = conn
        self._is_closed = False
        self._log = zrlog.get_logger("cnodc.db")
        self._max_in_size = 32767

    @contextlib.contextmanager
    def cursor(self) -> _PGCursor:
        """Get a cursor and close it when done."""
        try:
            with self._conn.cursor() as cur:
                yield _PGCursor(cur, self._conn)
        finally:
            pass

    @wrap_nodb_exceptions
    def commit(self):
        """Commit the transaction."""
        self._conn.commit()

    @wrap_nodb_exceptions
    def rollback(self):
        """Rollback the transaction."""
        self._conn.rollback()

    def close(self):
        """Close the connection"""
        pass

    def create_savepoint(self, name):
        """Create a savepoint"""
        with self.cursor() as cur:
            cur.create_savepoint(name)

    def rollback_to_savepoint(self, name):
        """Rollback to a savepoint"""
        with self.cursor() as cur:
            cur.rollback_to_savepoint(name)

    def release_savepoint(self, name):
        """Release a savepoint."""
        with self.cursor() as cur:
            cur.release_savepoint(name)

    def spooled_copy(self,
                     copy_query: str,
                     values: t.Iterable[t.Sequence],
                     mem_size: int = 80000,
                     column_sep: str = "\t",
                     row_sep: str = "\n"):
        """Build and execute a copy query from an iterable sequence of values."""
        mem_file = tempfile.SpooledTemporaryFile(mem_size, mode="w+", encoding="utf-8")
        for value_list in values:
            s = column_sep.join(NODBControllerInstance.escape_copy_value(v) for v in value_list) + row_sep
            mem_file.write(s)
        mem_file.seek(0, 0)
        with self.cursor() as cur:
            cur.copy_expert(copy_query, mem_file)
        mem_file.close()

    @staticmethod
    def escape_copy_value(v):
        """Escape a value to use in a COPY statement."""
        if v is None:
            return "\\N"
        elif isinstance(v, str):
            return (
                v
                .replace("\\", "\\\\")
                .replace("\r", "\\r")
                .replace("\t", "\\t")
                .replace("\n", "\\n")
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\v", "\\v")
            )
        elif isinstance(v, (bytes, bytearray)):
            return "\\\\x" + (''.join(hex(x)[2:].zfill(2) for x in v))
        elif isinstance(v, bool):
            return 't' if v else 'f'
        elif isinstance(v, datetime.date):
            return v.strftime("%Y-%m-%d")
        elif isinstance(v, datetime.datetime):
            utc_v = v.astimezone(datetime.timezone(datetime.timedelta(seconds=0), "UTC"))
            return utc_v.strftime('%Y-%m-%d %H:%M:%SZ')
        elif isinstance(v, (list, tuple, dict)):
            return NODBControllerInstance.escape_copy_value(json.dumps(v))
        else:
            return str(v)

    def scanned_file_status(self, file_path: str) -> ScannedFileStatus:
        """Get the status of a scanned file."""
        with self.cursor() as cur:
            cur.execute("SELECT was_processed FROM nodb_scanned_files WHERE file_path = %s", [file_path])
            row = cur.fetchone()
            if row is None:
                return ScannedFileStatus.NOT_PRESENT
            elif bool(row[0]):
                return ScannedFileStatus.PROCESSED
            else:
                return ScannedFileStatus.UNPROCESSED

    def note_scanned_file(self, file_path):
        """Mark a scanned file as visited."""
        with self.cursor() as cur:
            cur.execute("INSERT INTO nodb_scanned_files (file_path) VALUES (%s)", [file_path])

    def mark_scanned_item_success(self, file_path):
        """Mark a scanned file as a success."""
        with self.cursor() as cur:
            cur.execute("UPDATE nodb_scanned_files SET was_processed = TRUE where file_path = %s", [file_path])

    def mark_scanned_item_failed(self, file_path):
        """Mark a scanned file as failing."""
        with self.cursor() as cur:
            cur.execute("DELETE FROM nodb_scanned_files WHERE file_path = %s", [file_path])

    def create_queue_item(self,
                          queue_name: str,
                          data: dict,
                          priority: t.Optional[int] = None,
                          unique_item_key: t.Optional[str] = None,
                          subqueue_name: t.Optional[str] = None):
        """Create a new queue item."""
        with self.cursor() as cur:
            cur.execute("""
                INSERT INTO nodb_queues (queue_name, subqueue_name, priority, unique_item_name, data) 
                    VALUES (%s, %s, %s, %s, %s)""", [
                queue_name,
                subqueue_name or None,
                priority if priority is not None else 0,
                unique_item_key,
                json.dumps(data)
            ])

    def batch_create_queue_item(self, values: t.Iterable[t.Sequence]):
        """Batch create queue items."""
        with self.cursor() as cur:
            cur.spooled_copy(
                copy_query="COPY nodb_queues (queue_name, subqueue_name, priority, unique_item_name, data) FROM STDIN",
                values=(
                    (x[0],
                     (x[4] or None) if len(x) > 4 else None,
                     (x[2] or 0) if len(x) > 2 else 0,
                     (x[3] or None) if len(x) > 3 else None,
                     x[1])
                    for x in values
                )
            )

    def chunk_for_in(self, values: list) -> t.Iterable[list]:
        """Separate a list of values into manageable lists for an IN clause."""
        x = 0
        l_values = len(values)
        if l_values == 0:
            yield []
        else:
            while x < l_values:
                yield values[x:x+self._max_in_size]
                x += self._max_in_size

    def load_queue_item(self, queue_uuid) -> t.Optional[structures.NODBQueueItem]:
        """Find a queue item."""
        return self.load_object(
            obj_cls=structures.NODBQueueItem,
            filters={"queue_uuid": queue_uuid}
        )

    def fetch_next_queue_item(self,
                              queue_name: str,
                              app_id: str,
                              subqueue_name: t.Optional[str] = None,
                              retries: int = 1) -> t.Optional[structures.NODBQueueItem]:
        """Get the next queue item."""
        with self.cursor() as cur:
            while retries > 0:
                item_uuid = self._attempt_fetch_queue_item(queue_name, subqueue_name, app_id, cur)
                if item_uuid is not None:
                    return self.load_queue_item(item_uuid)
                retries -= 1
            return None

    def _attempt_fetch_queue_item(self, queue_name: str, subqueue_name: t.Optional[str], app_id: str, cur: _PGCursor) -> t.Optional[str]:
        """Make a single attempt to get a queue item."""
        try:
            cur.create_savepoint("fetch_queue_item")
            if subqueue_name:
                cur.execute("SELECT * FROM next_queue_item(%s, %s, %s)", (queue_name, app_id, subqueue_name))
            else:
                cur.execute("SELECT * FROM next_queue_item(%s, %s)", (queue_name, app_id))
            item = cur.fetchone()
            if item is not None and item[0] is not None:
                cur.release_savepoint("fetch_queue_item")
                return item[0]
            else:
                cur.rollback_to_savepoint("fetch_queue_item")
                return None
        except NODBError as ex:
            cur.rollback_to_savepoint("fetch_queue_item")
            # Retry deadlock and serialization errors
            if ex.is_serialization_error():
                return None
            else:
                raise ex
        except Exception as ex:
            cur.rollback_to_savepoint("fetch_queue_item")
            raise ex

    def delete_object(self, obj: structures._NODBBaseObject):
        """Delete an object."""
        query = f'DELETE FROM {obj.get_table_name()} WHERE '
        key_names = obj.get_primary_keys()
        query += ' AND '.join(f'{x} = %s' for x in key_names)
        args = [obj.get_for_db(x) for x in key_names]
        if not args:
            raise CNODCError('Probably an error')
        with self.cursor() as cur:
            cur.execute(query, args)

    def load_object(self,
                    obj_cls: type,
                    filters: dict[str, str],
                    limit_fields: t.Optional[list[str]] = None,
                    lock_type: LockType = LockType.NONE,
                    key_only: bool = False) -> t.Optional[object]:
        """Load an object."""
        key_names = list(x for x in filters.keys())
        if limit_fields:
            limit_fields = set(limit_fields)
            limit_fields.update(key_names)
            limit_fields.update(obj_cls.get_primary_keys())
        elif key_only:
            limit_fields = set(key_names)
            limit_fields.update(obj_cls.get_primary_keys())
        else:
            limit_fields = None
        field_list = '*' if limit_fields is None else ', '.join(limit_fields)
        query = f"SELECT {field_list} FROM {obj_cls.get_table_name()} WHERE "
        query += " AND ".join(f"{x} = %s" for x in key_names)
        query += self.build_lock_type_clause(lock_type)
        with self.cursor() as cur:
            cur.execute(query, [filters[x] for x in key_names])
            first_row = cur.fetchone()
            if first_row:
                return obj_cls(
                    is_new=False,
                    **{x: first_row[x] for x in first_row.keys()}
                )
        return None

    def build_lock_type_clause(self, lock_type: t.Optional[LockType] = None) -> str:
        """Build a clause to add on to a SELECT statement to get a row lock."""
        if lock_type == LockType.FOR_SHARE:
            return " FOR SHARE"
        elif lock_type == LockType.FOR_UPDATE:
            return " FOR UPDATE"
        elif lock_type == LockType.FOR_NO_KEY_UPDATE:
            return " FOR NO KEY UPDATE"
        elif lock_type == LockType.FOR_KEY_SHARE:
            return " FOR KEY SHARE"
        return ""

    def upsert_object(self, obj: structures._NODBBaseObject) -> bool:
        """Upsert an object, if necessary."""
        if obj.is_new:
            return self.insert_object(obj)
        elif obj.modified_values:
            return self.update_object(obj)
        else:
            return True

    def update_object(self, obj: structures._NODBBaseObject):
        """Update an object, if necessary."""
        if not obj.modified_values:
            return True
        primary_keys = obj.get_primary_keys()
        update_values = list(obj.modified_values)
        for pk in primary_keys:
            if pk in update_values:
                update_values.remove(pk)
        args = []
        query = f"UPDATE {obj.get_table_name()} SET "
        query += ", ".join(f"{x} = %s" for x in update_values)
        args.extend([obj.get_for_db(x) for x in update_values])
        query += " WHERE "
        query += " AND ".join(f"{x} = %s" for x in primary_keys)
        args.extend([obj.get_for_db(x) for x in primary_keys])
        with self.cursor() as cur:
            cur.execute(query, args)
        obj.clear_modified()
        return True

    def insert_object(self, obj: structures._NODBBaseObject):
        """Insert an object into its table."""
        args = []
        primary_keys = obj.get_primary_keys()
        insert_values = list(obj.modified_values)
        for pk in primary_keys:
            if pk not in insert_values and obj.get(pk, None) is not None:
                insert_values.append(pk)
        query = f"INSERT INTO {obj.get_table_name()}"
        if insert_values:
            query += " ("
            query += ", ".join(f"{x}" for x in insert_values)
            query += ")"
            query += " VALUES ("
            query += ", ".join("%s" for _ in insert_values)
            query += ")"
            args.extend([obj.get_for_db(x) for x in insert_values])
        else:
            query += " DEFAULT VALUES"
        if primary_keys:
            query += " RETURNING " + ",".join(primary_keys)
        with self.cursor() as cur:
            cur.execute(query, args)
            row = cur.fetchone()
            if row is not None and row[0] is not None:
                for x in primary_keys:
                    obj.set(row[x], x)
        obj.clear_modified()
        return True

    def grant_permission(self, role_name, permission_name):
        """Grant a permission to a role."""
        with self.cursor() as cur:
            cur.execute("SELECT 1 FROM nodb_permissions WHERE role_name = %s and permission = %s", [
                role_name,
                permission_name
            ])
            row = cur.fetchone()
            if row is None:
                cur.execute("INSERT INTO nodb_permissions (role_name, permission) VALUES (%s, %s)", [
                    role_name,
                    permission_name
                ])

    def remove_permission(self, role_name, permission_name):
        """Remove a permission from a role."""
        with self.cursor() as cur:
            cur.execute("DELETE FROM nodb_permissions WHERE role_name = %s and permission = %s", [
                role_name,
                permission_name
            ])

    def delete_session(self, session_id: str):
        """Delete a user session."""
        with self.cursor() as cur:
            cur.execute("DELETE FROM nodb_sessions WHERE session_id = %s", [session_id])

    def record_login(self,
                     username: str,
                     ip_address: str,
                     instance_name: str):
        """Record a user logging in."""
        with self.cursor() as cur:
            cur.execute("INSERT INTO nodb_logins (username, login_time, login_addr, instance_name) VALUES (%s, CURRENT_TIMESTAMP, %s, %s)", [
                username,
                ip_address,
                instance_name
            ])


class NODBControllerBase:
    """Base class for controller objects."""

    def __init__(self):
        self._instance: t.Optional[NODBControllerInstance] = None
        self._inner_count = 0

    def __enter__(self) -> NODBControllerInstance:
        if self._instance is None:
            self._instance = self._build_controller_instance()
        self._inner_count += 1
        return self._instance

    def _build_controller_instance(self):
        raise NotImplementedError

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._inner_count -= 1
        if self._inner_count == 0:
            if exc_type is not None:
                self._instance.rollback()
            else:
                self._instance.commit()
            self._instance.close()
            self._instance = None


@injector.injectable_global
class NODBController(NODBControllerBase):
    """Postgresql-linked instance of the controller object."""

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        super().__init__()
        self._conn = None
        self._connect_args = self.config.as_dict(("cnodc", "nodb_connection"), default={})
        self._connect_args['cursor_factory'] = pge.DictCursor

    def __cleanup__(self):
        if self._conn is not None:
            self._conn.close()

    def _build_controller_instance(self):
        if self._conn is None:
            try:
                self._conn = pg.connect(**self._connect_args)
            except psycopg2.OperationalError as ex:
                if 'connection refused' in str(ex).lower():
                    raise CNODCError(f'Database connection refused', 'NODB', 1002, is_recoverable=True) from ex
                else:
                    raise CNODCError(f'Database connection error: {str(ex)}', 'NODB', 1003) from ex
        return NODBControllerInstance(self._conn)
