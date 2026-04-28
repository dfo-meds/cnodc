"""Wrapper around psycopg2 that provides additional functionality
    to support the NODB."""
import contextlib
import datetime
import uuid
import typing as t

import psycopg2 as pg
import psycopg2.extras as pge
import psycopg2.sql as pgs
import zirconium as zr
import zrlog
from autoinject import injector

from medsutil.exceptions import CodedError
from nodb import QueueStatus
import nodb.interface as interface
from nodb.interface import NODBError, wrap_nodb_exceptions, POSTGRES_ALLOWED_CHARACTERS, ScannedFileStatus, LockType
from nodb.queue import NODBQueueItem
from pipeman.exceptions import CNODCError
import medsutil.json as json
import medsutil.types as ct
from medsutil.awaretime import AwareDateTime


class _SqlQueryStringifier:
    def __init__(self, cursor, q: str | pgs.Composable, args):
        self._cursor: pge.DictCursor = cursor
        self._q = q
        self._args = args

    def __str__(self):
        if isinstance(self._q, pgs.Composable):
            query = self._q.as_string(self._cursor)
        else:
            query = self._q
        return self._cursor.mogrify(query, self._args).decode('utf-8')


class _PGCursor(interface.NODBCursor):
    """Cursor class for postgresql."""

    def __init__(self, cursor, pg_conn):
        self._cursor = cursor
        self._conn = pg_conn
        self._log = zrlog.get_logger("cnodc.nodb")

    @wrap_nodb_exceptions
    def execute(self, query: str | pgs.Composable, args=None):
        """Execute a query against the database."""
        self._log.trace(f"SQL Query: [%s]", _SqlQueryStringifier(self._cursor, query, args))
        self._cursor.execute(query, args)

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
        name = self._escape_savepoint_name(name)
        self._cursor.execute(pgs.SQL('SAVEPOINT ') + pgs.SQL(name))
        return name

    def rollback_to_savepoint(self, name):
        """Rollback to a save point."""
        self._cursor.execute(pgs.SQL('ROLLBACK TO SAVEPOINT ') + pgs.SQL(self._escape_savepoint_name(name)))

    def release_savepoint(self, name):
        """Release a save point."""
        self._cursor.execute(pgs.SQL('RELEASE SAVEPOINT ') + pgs.SQL(self._escape_savepoint_name(name)))

    @staticmethod
    def _escape_savepoint_name(name: str):
        return ''.join(n for n in name.lower() if n in POSTGRES_ALLOWED_CHARACTERS)


class PostgresController(interface.NODBInstance):
    """Wrapper around a postgresql connection with NODB support"""

    def __init__(self, conn, cur_cls=_PGCursor):
        self._cur_cls = cur_cls
        self._conn = conn
        self._is_closed = False
        self._log = zrlog.get_logger("cnodc.db")
        self._max_in_size = 32767
        self._stable_sort_columns = False

    @contextlib.contextmanager
    def cursor(self) -> t.Generator[_PGCursor, t.Any, None]:
        """Get a cursor and close it when done."""
        try:
            with self._conn.cursor() as cur:
                yield self._cur_cls(cur, self._conn)
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

    def delete_object(self, obj: interface.NODBObject):
        """Delete an object."""
        args = {
            x: obj.get_for_db(x)
            for x in obj.get_primary_keys()
        }
        if not args:
            raise CNODCError('Cannot delete if no args', 'NODB', 8000)
        query = self.assemble_query(
            self.build_delete_clause(obj.get_table_name()),
            self.build_where_clause(args)
        )
        with self.cursor() as cur:
            cur.execute(query)

    def load_object[T](self,
                       obj_cls: type[T],
                       filters: interface.FilterDict,
                       join_str: interface.JoinString = None,
                       lock_type: interface.LockType = None,
                       limit_fields: list[str] = None,
                       key_only: bool = False, ) -> T | None:
        """Load an object."""
        query = self.assemble_query(
            self.build_select_clause(
                obj_cls.get_table_name(),
                self.extend_selected_fields(limit_fields, filters, key_only, obj_cls),
                self._stable_sort_columns
            ),
            self.build_where_clause(filters=filters, join_str=join_str),
            self.build_lock_type_clause(lock_type)
        )
        with self.cursor() as cur:
            cur.execute(query)
            first_row = cur.fetchone()
            if first_row:
                try:
                    fields = {x: first_row[x] for x in first_row.keys() if isinstance(x, str)}
                    return obj_cls(
                        is_new=False,
                        **fields
                    )
                except TypeError as ex:
                    raise CodedError(f"Data class [{obj_cls.__name__}]cannot handle fields from database, found [{','.join(fields.keys())}]") from ex
        return None

    def rows(self, table_name: str) -> int:
        query = self.assemble_query(
            self.build_select_clause(table_name, [pgs.SQL('COUNT(*)')], False)
        )
        with self.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            return row[0]

    def count_objects(self, obj_cls: interface.NODBObjectType, filters: interface.FilterDict = None, join_str: interface.JoinString = None) -> int:
        """Load an object."""
        query = self.assemble_query(
            self.build_select_clause(obj_cls.get_table_name(), ['COUNT(*)'], False),
            self.build_where_clause(filters, join_str)
        )
        with self.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            return row[0]

    def stream_raw(self,
                   obj_cls: interface.NODBObjectType,
                   filters: interface.FilterDict = None,
                   join_str: interface.JoinString = None,
                   lock_type: interface.LockType = LockType.NONE,
                   limit_fields: t.Optional[list[str]] = None,
                   key_only: bool = False,
                   order_by: t.Optional[list[str]] = None) -> t.Iterable[dict[str, interface.SupportsPostgres]]:
        """Load an object."""
        query = self.assemble_query(
            self.build_select_clause(
                obj_cls.get_table_name(),
                self.extend_selected_fields(limit_fields, filters, key_only, obj_cls),
                self._stable_sort_columns
            ),
            self.build_where_clause(filters, join_str),
            self.build_order_by_clause(order_by),
            self.build_lock_type_clause(lock_type)
        )
        with self.cursor() as cur:
            cur.execute(query)
            for row in cur.fetch_stream():
                yield row

    def stream_objects(self,
                       obj_cls: interface.NODBObjectType,
                       filters: interface.FilterDict = None,
                       join_str: interface.JoinString = None,
                       lock_type: interface.LockType = LockType.NONE,
                       limit_fields: t.Optional[list[str]] = None,
                       key_only: bool = False,
                       order_by: t.Optional[list[str]] = None) -> t.Iterable[interface.NODBObject]:
        for row in self.stream_raw(
            obj_cls=obj_cls,
            filters=filters,
            join_str=join_str,
            lock_type=lock_type,
            limit_fields=limit_fields,
            key_only=key_only,
            order_by=order_by
        ):
            yield obj_cls(is_new=False, **{x: row[x] for x in row.keys() if isinstance(x, str)})

    def upsert_object(self, obj: interface.NODBObject) -> bool:
        """Upsert an object, if necessary."""
        if obj.is_new:
            return self.insert_object(obj)
        elif obj.modified_values:
            return self.update_object(obj)
        else:
            return True

    def update_object(self, obj: interface.NODBObject) -> bool:
        """Update an object, if necessary."""
        if not obj.modified_values:
            return True
        primary_keys = obj.get_primary_keys()
        update_values = list(obj.modified_values)
        for pk in primary_keys:
            if pk in update_values:
                update_values.remove(pk)
        query = self.assemble_query(
            self.build_update_clause(
                obj.get_table_name(),
                { x: obj.get_for_db(x) for x in update_values },
                self._stable_sort_columns
            ),
            self.build_where_clause({
                x: obj.get_for_db(x)
                for x in primary_keys
            })
        )
        with self.cursor() as cur:
            cur.execute(query)
        obj.clear_modified()
        return True

    def insert_object(self, obj: interface.NODBObject) -> bool:
        """Insert an object into its table."""
        primary_keys = list(obj.get_primary_keys())
        insert_statement = self.assemble_query(
            self.build_insert_statement(
                obj.get_table_name(),
                {
                    x: obj.get_for_db(x)
                    for x in obj.modified_values
                },
                primary_keys,
                self._stable_sort_columns
            )
        )
        with self.cursor() as cur:
            cur.execute(insert_statement)
            row = cur.fetchone()
            if row is not None and row[0] is not None:
                with obj.readonly_access():
                    for x in primary_keys:
                        if isinstance(x, str):
                            obj.set_from_db(x, row[x])
        obj.is_new = False
        obj.clear_modified()
        return True

    def fast_renew_queue_item(self, queue_uuid: str, now_: AwareDateTime | None = None) -> AwareDateTime:
        with self.cursor() as cur:
            dt = now_ or AwareDateTime.now()
            cur.execute(f"UPDATE nodb_queues SET locked_since = %s WHERE queue_uuid = %s AND status = 'LOCKED'", [dt.isoformat(), queue_uuid])  # nosec: B608 # not hard coded string
            return dt

    def fast_update_queue_status(self,
                                 queue_uuid: str,
                                 new_status: QueueStatus,
                                 release_at: datetime.datetime | None = None,
                                 reduce_priority: bool = False,
                                 escalation_level: int = 0):
        with self.cursor() as cur:  # nosec B608 # not a hard coded query
            cur.execute("UPDATE nodb_queues SET status = %s, locked_by = NULL, locked_since = NULL, delay_release = %s, priority = priority + %s, escalation_level = %s WHERE queue_uuid = %s AND status = 'LOCKED'", [
                new_status.value,
                release_at.isoformat() if release_at else None,
                1 if reduce_priority else 0,
                escalation_level,
                queue_uuid
            ])

    def scanned_file_status(self, file_path: str, mod_time: datetime.datetime | None = None) -> ScannedFileStatus:
        """Get the status of a scanned file."""
        with self.cursor() as cur:
            if mod_time is None:
                cur.execute("SELECT was_processed, was_errored FROM nodb_scanned_files WHERE file_path = %s AND modified_date IS NULL", [file_path])
            else:
                cur.execute("SELECT was_processed, was_errored FROM nodb_scanned_files WHERE file_path = %s AND modified_date = %s", [file_path, mod_time.isoformat()])
            row = cur.fetchone()
            if row is None:
                return ScannedFileStatus.NOT_PRESENT
            elif bool(row[0]):
                return ScannedFileStatus.PROCESSED
            elif bool(row[1]):
                return ScannedFileStatus.ERRORED
            else:
                return ScannedFileStatus.UNPROCESSED

    def note_scanned_file(self, file_path: str, mod_time: datetime.datetime | None = None):
        """Mark a scanned file as visited."""
        with self.cursor() as cur:
            cur.execute("INSERT INTO nodb_scanned_files (file_path, modified_date) VALUES (%s, %s)", [file_path, mod_time.isoformat() if mod_time is not None else None])

    def mark_scanned_item_success(self, file_path: str, mod_time: datetime.datetime | None = None) -> None:
        """Mark a scanned file as a success."""
        with self.cursor() as cur:
            if mod_time is None:
                cur.execute("UPDATE nodb_scanned_files SET was_processed = TRUE where file_path = %s AND modified_date IS NULL AND was_processed = FALSE AND was_errored = FALSE", [file_path])
            else:
                cur.execute("SELECT was_processed FROM nodb_scanned_files WHERE file_path = %s AND modified_date = %s", [file_path, mod_time.isoformat()])
                row = cur.fetchone()
                if row is None:
                    cur.execute("INSERT INTO nodb_scanned_files (file_path, modified_date) VALUES (%s, %s)", [file_path, mod_time.isoformat()])
                cur.execute("UPDATE nodb_scanned_files SET was_processed = TRUE where file_path = %s AND (modified_date <= %s or modified_date IS NULL) AND was_processed = FALSE AND was_errored = FALSE", [file_path, mod_time.isoformat()])

    def mark_scanned_item_failed(self, file_path: str, mod_time: datetime.datetime | None = None):
        """Mark a scanned file as failing."""
        with self.cursor() as cur:
            if mod_time is None:
                cur.execute("UPDATE nodb_scanned_files SET was_errored = TRUE WHERE file_path = %s AND modified_date IS NULL AND was_processed = FALSE AND was_errored = FALSE", [file_path])
            else:
                cur.execute("UPDATE nodb_scanned_files SET was_errored = TRUE WHERE file_path = %s AND modified_date = %s AND was_processed = FALSE AND was_errored = FALSE", [file_path, mod_time.isoformat()])

    def create_queue_item(self,
                          queue_name: str,
                          data: dict[str, ct.SupportsExtendedJson],
                          priority: t.Optional[int] = None,
                          unique_item_name: t.Optional[str] = None,
                          subqueue_name: t.Optional[str] = None,
                          correlation_id: t.Optional[str] = None) -> str:
        correlation_id = correlation_id or str(uuid.uuid4())
        """Create a new queue item."""
        with self.cursor() as cur:
            cur.execute("""
                INSERT INTO nodb_queues (queue_name, subqueue_name, priority, unique_item_name, data, correlation_id) 
                    VALUES (%s, %s, %s, %s, %s, %s)""", [
                queue_name,
                subqueue_name or None,
                priority if priority is not None else 0,
                unique_item_name or None,
                json.dumps(data),
                correlation_id
            ])
        return t.cast(str, correlation_id)

    def bulk_update_objects(self, obj_cls: interface.NODBObjectType, updates: dict[str, interface.SupportsPostgres], key_field: str, key_values: list[
        interface.SupportsPostgres]):
        base_query = self.assemble_query(
            self.build_update_clause(obj_cls.get_table_name(), updates, self._stable_sort_columns),
            (
                pgs.SQL('WHERE'),
                pgs.Identifier(key_field),
                pgs.SQL('IN'),

            )
        )
        with self.cursor() as cur:
            for subset in self.batched(key_values):
                specific_query = base_query + pgs.SQL("(" + ",".join('%s' for _ in range(0, len(subset))) + ")")
                cur.execute(specific_query, [*subset])

    def batched(self, values: list) -> t.Iterable[list]:
        """Separate a list of values into manageable lists for an IN clause."""
        x = 0
        l_values = len(values)
        while x < l_values:
            yield values[x:x+self._max_in_size]
            x += self._max_in_size

    def fetch_next_queue_item(self,
                              queue_name: str,
                              app_id: str,
                              subqueue_name: str | None = None,
                              retries: int = 1) -> t.Optional[NODBQueueItem]:
        """Get the next queue item."""
        with self.cursor() as cur:
            while retries > 0:
                item_uuid = self._attempt_fetch_queue_item(queue_name, subqueue_name, app_id, cur)
                if item_uuid is not None:
                    return NODBQueueItem.find_by_uuid(self, item_uuid)
                retries -= 1
            return None

    @staticmethod
    def _attempt_fetch_queue_item(queue_name: str, subqueue_name: t.Optional[str], app_id: str, cur: _PGCursor) -> t.Optional[str]:
        """Make a single attempt to get a queue item."""
        sp = False
        try:
            cur.create_savepoint("fetch_queue_item")
            sp = True
            if subqueue_name:
                cur.execute("SELECT * FROM next_queue_item(%s::varchar(126), %s::varchar(126), %s::varchar(126))", (queue_name, app_id, subqueue_name))
            else:
                cur.execute("SELECT * FROM next_queue_item(%s::varchar(126), %s::varchar(126))", (queue_name, app_id))
            item = cur.fetchone()
            if item is not None and item[0] is not None:
                cur.release_savepoint("fetch_queue_item")
                sp = False
                return item[0]
            else:
                cur.rollback_to_savepoint("fetch_queue_item")
                sp = False
                return None
        except NODBError as ex:
            cur.rollback_to_savepoint("fetch_queue_item")
            # Retry deadlock and serialization errors
            if ex.is_serialization_error():
                return None
            else:
                raise ex
        except Exception as ex:
            if sp:
                cur.rollback_to_savepoint("fetch_queue_item")
            raise ex

    def grant_permission(self, role_name: str, permission_name: str):
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

    def remove_permission(self, role_name: str, permission_name: str):
        """Remove a permission from a role."""
        with self.cursor() as cur:
            cur.execute("DELETE FROM nodb_permissions WHERE role_name = %s and permission = %s", [
                role_name,
                permission_name
            ])

    def load_queue_items(self) -> t.Iterable[tuple[str, str, str]]:
        with self.cursor() as cur:
            cur.execute('SELECT queue_name, queue_uuid, status FROM nodb_queues')
            for x in cur.fetch_stream(100):
                yield x[0], x[1], x[2]

    def load_permissions(self, roles: t.Iterable[str]) -> set[str]:
        permissions = set()
        roles = list(roles)
        if roles:
            with self.cursor() as cur:
                query = pgs.SQL("SELECT permission FROM nodb_permissions WHERE role_name IN") + pgs.SQL(',').join(pgs.Placeholder() for _ in roles)
                cur.execute(query, [*roles])
                permissions.update(row[0] for row in cur.fetch_stream(20))
        return permissions

    def delete_session(self, session_id: str):
        """Delete a user session."""
        with self.cursor() as cur:
            cur.execute("DELETE FROM nodb_sessions WHERE session_id = %s", [session_id])

    def record_login(self,
                     username: str,
                     ip_address: str | None,
                     instance_name: str):
        """Record a user logging in."""
        with self.cursor() as cur:
            cur.execute("INSERT INTO nodb_logins (username, login_time, login_addr, instance_name) VALUES (%s, CURRENT_TIMESTAMP, %s, %s)", [
                username,
                ip_address or '??',
                instance_name
            ])

    @staticmethod
    def assemble_query(*args):
        def _chain(a) -> t.Iterable[pgs.Composable]:
            # args is a tuple[pgs.Composable | t.Iterable[pgs.Composable]], this sorts it all out
            if isinstance(a, pgs.Composable):
                yield a
            else:
                for x in a:
                    yield from _chain(x)
        return pgs.SQL(" ").join(_chain(args))

    @staticmethod
    def extend_selected_fields(
            limit_fields: t.Iterable[str] | None,
            filters: interface.FilterDict | None,
            key_only: bool,
            obj_cls: interface.NODBObjectType
    ) -> set[str] | None:
        fields: set[str] = set()
        if limit_fields is not None:
            fields.update(limit_fields)
            fields.update(obj_cls.get_primary_keys())
        if key_only:
            fields.update(obj_cls.get_primary_keys())
        if fields and filters is not None:
            fields.update(filters.keys())
        return fields if fields else None

    @staticmethod
    def build_delete_clause(table_name: str) -> t.Iterable[pgs.Composable]:
        yield pgs.SQL('DELETE FROM')
        yield pgs.Identifier(table_name)

    @staticmethod
    def build_update_clause(table_name: str, set_values: dict[str, t.Any], stable_sort: bool = False) -> t.Iterable[pgs.Composable]:
        keys = list(set(x for x in set_values.keys()))
        if stable_sort:
            keys.sort()
        yield pgs.SQL('UPDATE')
        yield pgs.Identifier(table_name)
        yield pgs.SQL('SET')
        yield pgs.SQL(',').join(
            pgs.Composed((
                pgs.Identifier(key),
                pgs.SQL('='),
                pgs.Literal(set_values[key])
            ))
            for key in keys
        )

    @staticmethod
    def build_insert_statement(table: str, insert_values: dict[str, t.Any] = None, primary_keys: list[str] = None, stable_sort: bool = False) -> t.Iterable[pgs.Composable]:
        yield pgs.SQL('INSERT INTO')
        yield pgs.Identifier(table)
        if insert_values:
            keys = list(insert_values.keys())
            if stable_sort:
                keys.sort()
            yield pgs.SQL('(') + pgs.SQL(',').join(pgs.Identifier(x) for x in keys) + pgs.SQL(")")
            yield pgs.SQL('VALUES (') + pgs.SQL(',').join(pgs.Literal(insert_values[x]) for x in keys) + pgs.SQL(')')
        else:
            yield pgs.SQL('DEFAULT VALUES')
        if primary_keys:
            if stable_sort:
                primary_keys.sort()
            yield pgs.SQL('RETURNING ') + pgs.SQL(',').join(pgs.Identifier(x) for x in primary_keys)

    @staticmethod
    def build_select_clause(table_name, fields, stable_sort) -> t.Iterable[pgs.Composable]:
        yield pgs.SQL('SELECT')
        if not fields:
            yield pgs.SQL('*')
        else:
            fields = list(fields)
            # stable field sorting is occasionally important
            if stable_sort:
                fields.sort()
            yield pgs.SQL(',').join(pgs.Identifier(field) if isinstance(field, str) else field for field in fields)
        yield pgs.SQL('FROM')
        yield pgs.Identifier(table_name)

    @staticmethod
    def build_where_clause(filters=None, join_str = None) -> t.Iterable[pgs.Composable]:
        if filters:
            yield pgs.SQL('WHERE')
            first = True
            join_str = pgs.SQL('AND' if join_str is None else (join_str.strip() or 'AND'))
            for key in filters:
                if first:
                    first = False
                else:
                    yield join_str
                if filters[key] is None:
                    yield pgs.Identifier(key)
                    yield pgs.SQL('IS NULL')
                elif isinstance(filters[key], tuple):
                    # (value, operation[, allow_null])
                    suffix = pgs.SQL("")
                    if len(filters[key]) > 2 and filters[key][2]:
                        yield pgs.SQL('(')
                        yield pgs.Composed((pgs.Identifier(key), pgs.SQL('IS NULL OR')))
                        suffix = pgs.SQL(')')
                    op = filters[key][1].strip().upper()
                    val = filters[key][0]
                    if op == 'IN':
                        yield pgs.Identifier(key)
                        yield pgs.Identifier('IN')
                        yield pgs.Composed((
                            pgs.Identifier('('),
                            pgs.SQL(',').join(pgs.Literal(v) for v in val),
                            pgs.Identifier(')'),
                            suffix
                        ))
                    else:
                        yield pgs.Composed((
                            pgs.Identifier(key),
                            pgs.SQL(op),
                            pgs.Literal(val),
                            suffix
                        ))
                else:
                    yield pgs.Identifier(key) + pgs.SQL('=') + pgs.Literal(filters[key])

    @staticmethod
    def build_order_by_clause(order_by: t.Optional[t.Sequence[t.Union[str,tuple[str, bool]]]] = None) -> t.Iterable[pgs.Composable]:
        if order_by:
            yield pgs.SQL('ORDER BY')
            for order_info in order_by:
                if isinstance(order_info, tuple):
                    order_field, is_desc = order_info
                else:
                    order_field, is_desc = order_info, False
                yield pgs.Identifier(order_field)
                yield pgs.SQL('ASC') if not is_desc else pgs.SQL('DESC')

    @staticmethod
    def build_lock_type_clause(lock_type: t.Optional[LockType] = None) -> t.Iterable[pgs.Composable]:
        """Build a clause to add on to a SELECT statement to get a row lock."""
        if lock_type == LockType.FOR_SHARE:
            yield pgs.SQL("FOR SHARE")
        elif lock_type == LockType.FOR_UPDATE:
            yield pgs.SQL("FOR UPDATE")
        elif lock_type == LockType.FOR_NO_KEY_UPDATE:
            yield pgs.SQL("FOR NO KEY UPDATE")
        elif lock_type == LockType.FOR_KEY_SHARE:
            yield pgs.SQL("FOR KEY SHARE")
        return None

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
            return PostgresController.escape_copy_value(json.dumps(v))
        else:
            return str(v)


@injector.injectable_global
class NODBPostgresController(interface.NODB):
    """Postgresql-linked instance of the controller object."""

    config: zr.ApplicationConfig = None

    TRANSIENT_ERRORS = (
        'connection refused',
        'server closed the connection unexpectedly',
        'could not receive data from server',
        'the database system is starting up',
        'the database system is not yet accepting connections',
    )

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__()
        self._conn = None
        self._connect_args: dict[str, t.Any] = kwargs if kwargs else t.cast(dict, self.config.as_dict(("nodb",), default={}))
        if 'options' not in self._connect_args:
            self._connect_args['options'] = "-c search_path=public"
        self._connect_args['cursor_factory'] = pge.DictCursor

    def __cleanup__(self):
        if self._conn is not None:
            self._conn.close()

    @interface.wrap_nodb_exceptions
    def _build_controller_instance(self):
        if self._conn is None:
            self._conn = pg.connect(**self._connect_args)
        return PostgresController(self._conn)
