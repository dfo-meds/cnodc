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

from cnodc.nodb import NODBQueueItem
from cnodc.nodb.base import NODBBaseObject
from cnodc.util import CNODCError
import cnodc.util.awaretime as awaretime

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
            pgcode is not None and (pgcode in RECOVERABLE_ERRORS or f"{pgcode[0:2]}***" in RECOVERABLE_ERRORS)
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
        return self._cursor.execute(query.strip(), args)

    @wrap_nodb_exceptions
    def fetchone(self):
        """Fetch a single record"""
        return self._cursor.fetchone()

    @wrap_nodb_exceptions
    def fetchmany(self, size=None):
        """Fetch many records."""
        if size:
            return self._cursor.fetchmany(size)
        else:
            return self._cursor.fetchmany()

    @wrap_nodb_exceptions
    def fetch_stream(self, size=None):
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
        self._stable_sort_columns = False

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

    def delete_object(self, obj: NODBBaseObject):
        """Delete an object."""
        query = f'DELETE FROM {obj.get_table_name()}'
        where, args = NODBControllerInstance.build_where_clause({
            x: obj.get_for_db(x)
            for x in obj.get_primary_keys()
        })

        if not args:  # pragma: no coverage (not going to test this, all the objects have good PKs)
            raise NODBError("no args to delete, not good!", 5001, '')
        with self.cursor() as cur:
            cur.execute(query + where, args)

    def load_object(self, obj_cls: type, filters: dict[str, str], **kwargs) -> t.Optional[NODBBaseObject]:
        """Load an object."""
        limit_fields = self.extend_selected_fields(
            kwargs.pop('limit_fields', None),
            filters,
            kwargs.pop('key_only', False),
            obj_cls
        )
        query = self.build_select_clause(obj_cls.get_table_name(), limit_fields, self._stable_sort_columns)
        where, variables = self.build_where_clause(filters=filters, join_str=kwargs.pop('join_str', None))
        query += where
        query += self.build_lock_type_clause(kwargs.pop('lock_type', None))
        with self.cursor() as cur:
            cur.execute(query, variables)
            first_row = cur.fetchone()
            if first_row:
                return obj_cls(
                    is_new=False,
                    **{x: first_row[x] for x in first_row.keys() if isinstance(x, str)}
                )
        return None


    def count_objects(self, obj_cls: type, **kwargs) -> int:
        """Load an object."""
        query = self.build_select_clause(obj_cls.get_table_name(), ['COUNT(*)'], False)
        where, variables = self.build_where_clause(**kwargs)
        query += where
        with self.cursor() as cur:
            cur.execute(query, variables)
            row = cur.fetchone()
            return row[0]

    def stream_objects(self,
                    obj_cls: type,
                    filters: t.Optional[dict[str, str]] = None,
                    filter_type: t.Optional[str] = None,
                    limit_fields: t.Optional[list[str]] = None,
                    lock_type: LockType = LockType.NONE,
                    key_only: bool = False,
                    order_by: t.Optional[list[str]] = None,
                    raw: bool = False) -> t.Iterable[t.Union[NODBBaseObject, dict]]:
        """Load an object."""
        limit_fields = self.extend_selected_fields(limit_fields, filters, key_only, obj_cls)
        query = self.build_select_clause(obj_cls.get_table_name(), limit_fields, self._stable_sort_columns)
        where, variables = self.build_where_clause(filters, join_str=filter_type or ' AND ')
        query += where
        query += self.build_order_by_clause(order_by)
        query += self.build_lock_type_clause(lock_type)
        with self.cursor() as cur:
            cur.execute(query, variables)
            for row in cur.fetch_stream():
                if raw:
                    yield row
                else:
                    yield obj_cls(is_new=False, **{x: row[x] for x in row.keys() if isinstance(x, str)})

    def upsert_object(self, obj: NODBBaseObject) -> bool:
        """Upsert an object, if necessary."""
        if obj.is_new:
            return self.insert_object(obj)
        elif obj.modified_values:
            return self.update_object(obj)
        else:
            return True

    def update_object(self, obj: NODBBaseObject):
        """Update an object, if necessary."""
        if not obj.modified_values:
            return True
        primary_keys = obj.get_primary_keys()
        update_values = list(obj.modified_values)
        for pk in primary_keys:
            if pk in update_values:
                update_values.remove(pk)
        query, args = NODBControllerInstance.build_update_clause(
            obj.get_table_name(),
            { x: obj.get_for_db(x) for x in update_values },
            self._stable_sort_columns
        )
        wq, wargs = NODBControllerInstance.build_where_clause({
            x: obj.get_for_db(x)
            for x in primary_keys
        })
        query += wq
        args.extend(wargs)
        with self.cursor() as cur:
            cur.execute(query, args)
        obj.clear_modified()
        return True

    def insert_object(self, obj: NODBBaseObject):
        """Insert an object into its table."""
        args = []
        primary_keys = list(obj.get_primary_keys())
        insert_values = list(obj.modified_values)
        for pk in primary_keys:
            if pk not in insert_values and obj.get(pk, None) is not None:
                insert_values.append(pk)
        if self._stable_sort_columns:
            insert_values.sort()
            primary_keys.sort()
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
                    obj.set_from_db(x, row[x])
        obj.is_new = False
        obj.clear_modified()
        return True

    def fast_renew_queue_item(self, queue_uuid, now_: t.Optional[datetime.datetime] = None):
        with self.cursor() as cur:
            now_ = now_ or awaretime.utc_now()
            cur.execute(f"UPDATE nodb_queues SET locked_since = %s WHERE queue_uuid = %s AND status = 'LOCKED'", [now_.isoformat(), queue_uuid])
            return now_

    def fast_update_queue_status(self,
                                 queue_uuid: str,
                                 new_status: "cnodc.nodb.QueueStatus",
                                 release_at: t.Optional[datetime.datetime] = None,
                                 reduce_priority: bool = False,
                                 escalation_level: int = 0):
        with self.cursor() as cur:
            cur.execute(f"""
                UPDATE nodb_queues
                SET
                    status = %s,
                    locked_by = NULL,
                    locked_since = NULL,
                    delay_release = %s,
                    priority = priority + %s,
                    escalation_level = %s
                WHERE 
                    queue_uuid = %s
                    AND status = 'LOCKED'
            """, [
                new_status.value,
                release_at.isoformat() if release_at else None,
                1 if reduce_priority else 0,
                escalation_level,
                queue_uuid
            ])

    def scanned_file_status(self, file_path: str, mod_time: t.Optional[datetime.datetime] = None) -> ScannedFileStatus:
        """Get the status of a scanned file."""
        with self.cursor() as cur:
            if mod_time is None:
                cur.execute("SELECT was_processed FROM nodb_scanned_files WHERE file_path = %s AND modified_date IS NULL", [file_path])
            else:
                cur.execute("SELECT was_processed FROM nodb_scanned_files WHERE file_path = %s AND modified_date = %s", [file_path, mod_time.isoformat()])
            row = cur.fetchone()
            if row is None:
                return ScannedFileStatus.NOT_PRESENT
            elif bool(row[0]):
                return ScannedFileStatus.PROCESSED
            else:
                return ScannedFileStatus.UNPROCESSED

    def note_scanned_file(self, file_path, mod_time: t.Optional[datetime.datetime] = None):
        """Mark a scanned file as visited."""
        with self.cursor() as cur:
            cur.execute("INSERT INTO nodb_scanned_files (file_path, modified_date) VALUES (%s, %s)", [file_path, mod_time.isoformat() if mod_time is not None else None])

    def mark_scanned_item_success(self, file_path, mod_date: t.Optional[datetime.datetime] = None):
        """Mark a scanned file as a success."""
        with self.cursor() as cur:
            if mod_date is None:
                cur.execute("UPDATE nodb_scanned_files SET was_processed = TRUE where file_path = %s AND modified_date IS NULL AND was_processed = FALSE", [file_path])
            else:
                cur.execute("SELECT was_processed FROM nodb_scanned_files WHERE file_path = %s AND modified_date = %s", [file_path, mod_date.isoformat()])
                row = cur.fetchone()
                if row is None:
                    cur.execute("INSERT INTO nodb_scanned_files (file_path, modified_date) VALUES (%s, %s)", [file_path, mod_date.isoformat()])
                cur.execute("UPDATE nodb_scanned_files SET was_processed = TRUE where file_path = %s AND (modified_date <= %s or modified_date IS NULL) AND was_processed = FALSE", [file_path, mod_date.isoformat()])

    def mark_scanned_item_failed(self, file_path, mod_date: t.Optional[datetime.datetime] = None):
        """Mark a scanned file as failing."""
        with self.cursor() as cur:
            if mod_date is None:
                cur.execute("DELETE FROM nodb_scanned_files WHERE file_path = %s AND modified_date IS NULL", [file_path])
            else:
                cur.execute("DELETE FROM nodb_scanned_files WHERE file_path = %s AND modified_date = %s", [file_path, mod_date.isoformat()])

    def create_queue_item(self,
                          queue_name: str,
                          data: dict,
                          priority: t.Optional[int] = None,
                          unique_item_name: t.Optional[str] = None,
                          subqueue_name: t.Optional[str] = None):
        """Create a new queue item."""
        with self.cursor() as cur:
            cur.execute("""
                INSERT INTO nodb_queues (queue_name, subqueue_name, priority, unique_item_name, data) 
                    VALUES (%s, %s, %s, %s, %s)""", [
                queue_name,
                subqueue_name or None,
                priority if priority is not None else 0,
                unique_item_name,
                json.dumps(data)
            ])

    def bulk_update(self, cls, updates: dict, key_field: str, key_values: list):
        query, variables = NODBControllerInstance.build_update_clause(
            cls.get_table_name(),
            updates,
            self._stable_sort_columns
        )
        query += f' WHERE {key_field} IN '
        with self.cursor() as cur:
            for subset in self.chunk_for_in(key_values):
                specific_query = query + "(" + ",".join('%s' for _ in range(0, len(subset))) + ")"
                cur.execute(specific_query, [*variables, *subset])

    def chunk_for_in(self, values: list) -> t.Iterable[list]:
        """Separate a list of values into manageable lists for an IN clause."""
        x = 0
        l_values = len(values)
        while x < l_values:
            yield values[x:x+self._max_in_size]
            x += self._max_in_size

    def load_queue_item(self, queue_uuid) -> t.Optional[NODBQueueItem]:
        """Find a queue item."""
        return self.load_object(
            obj_cls=NODBQueueItem,
            filters={"queue_uuid": queue_uuid}
        )

    def fetch_next_queue_item(self,
                              queue_name: str,
                              app_id: str,
                              subqueue_name: t.Optional[str] = None,
                              retries: int = 1) -> t.Optional[NODBQueueItem]:
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
                cur.execute("SELECT * FROM next_queue_item(%s::varchar(126), %s::varchar(126), %s::varchar(126))", (queue_name, app_id, subqueue_name))
            else:
                cur.execute("SELECT * FROM next_queue_item(%s::varchar(126), %s::varchar(126))", (queue_name, app_id))
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

    def load_permissions(self, roles: list[str]) -> set[str]:
        permissions = set()
        if roles:
            with self.cursor() as cur:
                role_placeholders = ', '.join('%s' for _ in roles)
                cur.execute(f"""
                                SELECT permission FROM 
                                nodb_permissions WHERE role_name IN ({role_placeholders})
                            """, [*roles])
                permissions.update(row[0] for row in cur.fetch_stream(20))
        return permissions

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

    @staticmethod
    def extend_selected_fields(limit_fields, filters, key_only, obj_cls):
        if limit_fields:
            limit_fields = set(limit_fields)
            if filters:
                limit_fields.update(filters.keys())
            limit_fields.update(obj_cls.get_primary_keys())
        elif key_only:
            limit_fields = set(filters.keys()) if filters else set()
            limit_fields.update(obj_cls.get_primary_keys())
        else:
            limit_fields = None
        return limit_fields

    @staticmethod
    def build_update_clause(table_name: str, set_values: dict[str, t.Any], stable_sort: bool = False):
        q = f'UPDATE {table_name} SET '
        args = []
        keys = [x for x in set_values.keys()]
        if stable_sort:
            keys.sort()
        q += ','.join(f"{x}=%s" for x in keys)
        return q, [set_values[x] for x in keys]

    @staticmethod
    def build_select_clause(table_name, fields, stable_sort):
        if not fields:
            return f'SELECT * FROM {table_name}'
        # stable field sorting is occasionally important
        if stable_sort:
            fields = list(fields)
            fields.sort()
        return f'SELECT {','.join(fields)} FROM {table_name}'

    @staticmethod
    def build_where_clause(filters=None, join_str = None):
        if not filters:
            return '', []
        clauses = []
        parameters = []
        join_str = join_str or 'AND'
        for key in filters:
            if filters[key] is None:
                clauses.append(f'{key} IS NULL')
            elif isinstance(filters[key], tuple):
                # (value, operation[, allow_null])
                prefix = ''
                suffix = ''
                if len(filters[key]) > 2 and filters[key][2]:
                    prefix = f'({key} IS NULL OR '
                    suffix = ')'
                op = filters[key][1].strip().upper()
                val = filters[key][0]
                if op == 'IN':
                    clauses.append(f"{prefix}{key} {op} ({','.join('%s' for _ in val)}){suffix}")
                    parameters.extend(val)
                else:
                    clauses.append(f"{prefix}{key} {op} %s{suffix}")
                    parameters.append(val)
            else:
                clauses.append(f'{key} = %s')
                parameters.append(filters[key])
        if clauses:
            return ' WHERE ' + f" {join_str.strip()} ".join(clauses), parameters
        return '', []  #pragma: no coverage (handled above but just in case)

    @staticmethod
    def build_order_by_clause(order_by: t.Optional[t.Sequence[t.Union[str,tuple[str, bool]]]] = None):
        if not order_by:
            return ''
        actual_order_by = []
        for order_info in order_by:
            if isinstance(order_info, tuple):
                order_field, is_desc = order_info
            else:
                order_field, is_desc = order_info, False
            actual_order_by.append(f"{order_field} {'ASC' if not is_desc else 'DESC'}")
        return f' ORDER BY {','.join(actual_order_by)}'

    @staticmethod
    def build_lock_type_clause(lock_type: t.Optional[LockType] = None) -> str:
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
        elif isinstance(v, datetime.datetime):
            utc_v = v.astimezone(datetime.timezone.utc)
            return utc_v.strftime('%Y-%m-%d %H:%M:%SZ')
        elif isinstance(v, datetime.date):
            return v.strftime("%Y-%m-%d")
        elif isinstance(v, (list, tuple, dict)):
            return NODBControllerInstance.escape_copy_value(json.dumps(v))
        else:
            return str(v)

class NODBControllerBase:
    """Base class for controller objects."""

    def __init__(self):
        self._instance: t.Optional[NODBControllerInstance] = None
        self._inner_count = 0
        self._stable_sort = False

    def __enter__(self) -> NODBControllerInstance:
        if self._instance is None:
            self._instance = NODBControllerInstance(self._build_controller_instance())
            self._instance._stable_sort_columns = self._stable_sort
        self._inner_count += 1
        return self._instance

    def _build_controller_instance(self):
        raise NotImplementedError  # pragma: no coverage

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
                    raise CNODCError(f'Database connection refused', 'NODB', 1002, is_transient=True) from ex
                else:
                    raise CNODCError(f'Database connection error: {str(ex)}', 'NODB', 1003) from ex
        return self._conn
