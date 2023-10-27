import datetime
import itertools
import json
import tempfile
import enum

import psycopg2 as pg
import psycopg2.extras as pge
import zirconium as zr
from autoinject import injector
import typing as t
import cnodc.nodb.structures as structures


class LockType(enum.Enum):

    NONE = "1"
    FOR_UPDATE = "2"
    FOR_NO_KEY_UPDATE = "3"
    FOR_SHARE = "4"
    FOR_KEY_SHARE = "5"


class _NODBControllerInstance:

    def __init__(self, pg_connection):
        self._conn = pg_connection
        self._cursor = self._conn.cursor()
        self._is_closed = False

    def execute(self, query: str, args: t.Union[list, dict, tuple, None] = None):
        return self._cursor.execute(query, args)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def executemany(self, query: str, args_list: t.Iterable[t.Union[list, dict, tuple, None]] = None):
        self._cursor.executemany(query, args_list)

    def callproc(self, procname: str, parameters: t.Union[list, tuple, None] = None):
        return self._cursor.callproc(procname, parameters)

    def mogrify(self, query: str, args: t.Union[list, dict, tuple, None] = None) -> str:
        return self._cursor.mogrify(query, args)

    def fetchone(self) -> pge.DictRow:
        return self._cursor.fetchone()

    def fetchmany(self, size=None) -> t.Iterable[pge.DictRow]:
        if size is None:
            return self._cursor.fetchmany()
        else:
            return self._cursor.fetchmany(size)

    def fetchall(self) -> t.Iterable[pge.DictRow]:
        return self._cursor.fetchall()

    def batch_fetchall(self, batch_size=None) -> t.Iterable[pge.DictRow]:
        results = self.fetchmany(batch_size)
        while results:
            yield from results
            results = self.fetchmany(batch_size)

    def close(self):
        if not self._is_closed:
            self._cursor.close()
            self._conn.close()
            self._is_closed = True
            del self._cursor
            del self._conn

    def scroll(self, value, mode='relative'):
        return self._cursor.scroll(value, mode)

    @property
    def rowcount(self) -> t.Optional[int]:
        return self._cursor.rowcount

    @property
    def query(self) -> str:
        return self._cursor.query

    @property
    def statusmessage(self) -> str:
        return self._cursor.statusmessage

    def create_savepoint(self, name):
        self.execute("SAVEPOINT %s", [name])

    def rollback_to_savepoint(self, name):
        self.execute("ROLLBACK TO SAVEPOINT %s", [name])

    def release_savepoint(self, name):
        self.execute("RELEASE SAVEPOINT %s", [name])

    def copy_expert(self, *args, **kwargs):
        return self._cursor.copy_expert(*args, **kwargs)

    def spooled_copy(self,
                     copy_query: str,
                     values: t.Iterable[t.Sequence],
                     mem_size: int = 80000,
                     column_sep: str = "\t",
                     row_sep: str = "\n"):
        mem_file = tempfile.SpooledTemporaryFile(mem_size, mode="w+", encoding="utf-8")
        for value_list in values:
            s = column_sep.join(_NODBControllerInstance.escape_copy_value(v) for v in value_list) + row_sep
            mem_file.write(s)
        mem_file.seek(0, 0)
        self.copy_expert(copy_query, mem_file)

    @staticmethod
    def escape_copy_value(v):
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
            return _NODBControllerInstance.escape_copy_value(json.dumps(v))
        else:
            return str(v)

    def create_queue_item(self,
                          queue_name: str,
                          data: dict,
                          priority: t.Optional[int] = None,
                          unique_item_key: t.Optional[str] = None):
        self.execute("INSERT INTO nodb_queues (queue_name, priority, unique_item_name, data) VALUES (%s, %s, %s, %s)", [
            queue_name,
            unique_item_key,
            priority if priority is not None else 0,
            json.dumps(data)
        ])

    def batch_create_queue_item(self, values: t.Iterable[t.Sequence]):
        self.spooled_copy(
            copy_query="COPY nodb_queues (queue_name, priority, unique_item_name, data) FROM STDIN",
            values=(
                (x[0], x[2] if len(x) > 2 else 0, x[3] if len(x) > 3 else None, x[1])
                for x in values
            )
        )

    def _load_nodb_object_by_primary_key(self,
                                         obj_cls: type,
                                         table_name: str,
                                         filters: dict[str, str],
                                         limit_fields: t.Optional[list[str]] = None,
                                         lock_type: LockType = LockType.NONE):
        key_names = list(x for x in filters.keys())
        if limit_fields:
            limit_fields = set(limit_fields)
            limit_fields.update(key_names)
        else:
            limit_fields = None
        field_list = '*' if limit_fields is None else ', '.join(limit_fields)
        query = f"SELECT {field_list} FROM {table_name} WHERE "
        query += " AND ".join(f"{x} = %s" for x in key_names)
        if lock_type == LockType.FOR_SHARE:
            query += " FOR SHARE"
        elif lock_type == LockType.FOR_UPDATE:
            query += "FOR UPDATE"
        elif lock_type == LockType.FOR_NO_KEY_UPDATE:
            query += "FOR NO KEY UPDATE"
        elif lock_type == LockType.FOR_KEY_SHARE:
            query += " FOR KEY SHARE"
        self.execute(query, [filters[x] for x in key_names])
        first_row = self.fetchone()
        if first_row:
            return obj_cls(
                is_new=False,
                **{x: first_row[x] for x in first_row.keys()}
            )
        return None

    def _upsert_nodb_object_by_primary_key(self,
                                           obj: structures._NODBBaseObject,
                                           table_name: str,
                                           primary_keys: list[str]):
        query = f"INSERT INTO {table_name}"
        args = []
        mod_values = list(obj.modified_values)
        insert_values = list(obj.modified_values)
        insert_values.extend(pk for pk in primary_keys if obj.get(pk, None) is not None and pk not in insert_values)
        if mod_values:
            query += " ("
            query += ", ".join(f"{x}" for x in insert_values)
            query += ")"
            query += " VALUES ("
            query += ", ".join("%s" for _ in insert_values)
            query += ")"
            args.extend([obj.get_for_db(x) for x in insert_values])
        else:
            query += " DEFAULT VALUES"
        query += " ON CONFLICT (" + ",".join(primary_keys) + ") DO"
        if mod_values:
            query += " UPDATE SET "
            query += ", ".join(f"{x} = %s" for x in mod_values if x not in primary_keys)
            args.extend([obj.get_for_db(x) for x in mod_values if x not in primary_keys])
        else:
            query += " NOTHING"
        query += " RETURNING " + ",".join(primary_keys)
        self.execute(query, args)
        row = self.fetchone()
        if row[0] is not None:
            for x in primary_keys:
                obj.set(row[x], x)
            obj.clear_modified()
            return True
        return False

    def load_source_file(self,
                         source_file_uuid: str,
                         partition_key: datetime.date,
                         limit_fields: t.Optional[list[str]] = None,
                         lock_type: LockType = LockType.NONE) -> t.Optional[structures.NODBSourceFile]:
        return self._load_nodb_object_by_primary_key(
            structures.NODBSourceFile,
            "nodb_source_files",
            {"source_uuid": source_file_uuid, "partition_key": partition_key},
            limit_fields, lock_type
        )

    def get_workflow_config(self, workflow_name: str):
        self.execute("SELECT configuration FROM nodb_upload_workflows WHERE workflow_name = %s", [workflow_name])
        first_row = self.fetchone()
        return first_row[0] if first_row else None

    def load_user(self,
                  username: str,
                  lock_type: LockType = LockType.NONE) -> t.Optional[structures.NODBUser]:
        return self._load_nodb_object_by_primary_key(
            obj_cls=structures.NODBUser,
            table_name="nodb_users",
            filters={"username": username},
            lock_type=lock_type
        )

    def load_permissions(self,
                         roles: t.Optional[list[str]]) -> set[str]:
        if not roles:
            return set()
        permissions = set()
        q = "SELECT permission FROM nodb_permissions WHERE role_name IN ("
        q += ", ".join('%s' for _ in roles)
        q += ")"
        self.execute(q, roles)
        for row in self.fetchall():
            permissions.add(row[0])
        return permissions

    def save_user(self,
                  user: structures.NODBUser):
        self._upsert_nodb_object_by_primary_key(
            user,
            "nodb_users",
            ["username"]
        )

    def grant_permission(self, role_name, permission_name):
        self.execute("SELECT 1 FROM nodb_permissions WHERE role_name = %s and permission = %s", [
            role_name,
            permission_name
        ])
        row = self.fetchone()
        if row is not None:
            self.execute("INSERT INTO nodb_permissions (role_name, permission) VALUES (%s, %s)", [
                role_name,
                permission_name
            ])

    def remove_permission(self, role_name, permission_name):
        self.execute("DELETE FROM nodb_permissions WHERE role_name = %s and permission = %s", [
            role_name,
            permission_name
        ])

    def load_session(self,
                     session_id: str,
                     lock_type: LockType = LockType.NONE) -> t.Optional[structures.NODBSession]:
        return self._load_nodb_object_by_primary_key(
            obj_cls=structures.NODBSession,
            table_name="nodb_sessions",
            filters={"session_id": session_id},
            lock_type=lock_type
        )

    def delete_session(self, session_id: str):
        self.execute("DELETE FROM nodb_sessions WHERE session_id = %s", [session_id])

    def save_session(self,
                     session: structures.NODBSession):
        self._upsert_nodb_object_by_primary_key(
            session,
            "nodb_sessions",
            ["session_id"]
        )

    def record_login(self,
                     username: str,
                     ip_address: str,
                     instance_name: str):
        self.execute("INSERT INTO nodb_logins (username, login_time, login_addr, instance_name) VALUES (%s, CURRENT_TIMESTAMP(), %s, %s)", [
            username,
            ip_address,
            instance_name
        ])

    def save_source_file(self, source_file: structures.NODBSourceFile):
        self._upsert_nodb_object_by_primary_key(source_file, "nodb_source_files", ["source_uuid", "partition_key"])

    def load_queue_item(self, queue_uuid) -> t.Optional[structures.NODBQueueItem]:
        return self._load_nodb_object_by_primary_key(
            structures.NODBQueueItem,
            "nodb_queues",
            {"queue_uuid": queue_uuid}
        )

    def mark_queue_item_complete(self, queue_item: structures.NODBQueueItem):
        self._update_queue_item(queue_item, "COMPLETE")

    def mark_queue_item_failed(self, queue_item: structures.NODBQueueItem):
        self._update_queue_item(queue_item, "ERROR")

    def release_queue_item(self, queue_item: structures.NODBQueueItem):
        self._update_queue_item(queue_item, "UNLOCKED")

    def renew_queue_item_lock(self, queue_item: structures.NODBQueueItem):
        self.execute("UPDATE nodb_queues SET locked_since = %s WHERE queue_uuid = %s", [
            datetime.datetime.utcnow(),
            queue_item.queue_uuid
        ])

    def _update_queue_item(self, queue_item: structures.NODBQueueItem, new_status_code: str):
        self.execute("UPDATE nodb_queues SET status = %s, locked_by = NULL, locked_since = NULL WHERE queue_uuid = %s", [
            new_status_code,
            queue_item.queue_uuid
        ])

    def fetch_next_queue_item(self,
                              queue_name: str,
                              app_id: str,
                              retries: int = 5) -> t.Optional[str]:
        while retries > 0:
            item_uuid = self._attempt_fetch_queue_item(queue_name, app_id)
            if item_uuid is not None:
                return self.load_queue_item(item_uuid)
            retries -= 1
        return None

    def _attempt_fetch_queue_item(self, queue_name: str, app_id: str) -> t.Optional[str]:
        try:
            self.create_savepoint("fetch_queue_item")
            self.execute("SELECT next_queue_item(%s, %s)", (queue_name, app_id))
            item = self.fetchone()
            if item[0] is not None:
                self.release_savepoint("fetch_queue_item")
                return item[0]
            else:
                self.rollback_to_savepoint("fetch_queue_item")
                return None
        except (KeyboardInterrupt, SystemExit) as ex:
            self.rollback_to_savepoint("fetch_queue_item")
            raise ex
        except Exception as ex:
            self.rollback_to_savepoint("fetch_queue_item")
            return None


@injector.injectable_global
class NODBController:

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._instance = None
        self._connect_args = self.config.as_dict(("cnodc", "nodb_connection"), default={})
        self._connect_args['cursor_factory'] = pge.DictCursor

    def __enter__(self) -> _NODBControllerInstance:
        self._instance = _NODBControllerInstance(pg.connect(**self._connect_args))
        return self._instance

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._instance.rollback()
        else:
            self._instance.commit()
        self._instance.close()
        self._instance = None
