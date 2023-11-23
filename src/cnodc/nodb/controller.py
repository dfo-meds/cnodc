import datetime
import json
import tempfile
import enum
import zrlog

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


class NODBControllerInstance:

    def __init__(self, pg_connection):
        self._conn = pg_connection
        self._cursor = self._conn.cursor()
        self._is_closed = False
        self._log = zrlog.get_logger("cnodc.db")

    def execute(self, query: str, args: t.Union[list, dict, tuple, None] = None):
        self._log.debug(f"SQL Query: {query}")
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
            s = column_sep.join(NODBControllerInstance.escape_copy_value(v) for v in value_list) + row_sep
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
            return NODBControllerInstance.escape_copy_value(json.dumps(v))
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

    def load_queue_item(self, queue_uuid) -> t.Optional[structures.NODBQueueItem]:
        return self.load_object(
            obj_cls=structures.NODBQueueItem,
            filters={"queue_uuid": queue_uuid}
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

    def load_object(self,
                    obj_cls: type,
                    filters: dict[str, str],
                    limit_fields: t.Optional[list[str]] = None,
                    lock_type: LockType = LockType.NONE):
        key_names = list(x for x in filters.keys())
        if limit_fields:
            limit_fields = set(limit_fields)
            limit_fields.update(key_names)
            limit_fields.update(obj_cls.get_primary_keys())
        else:
            limit_fields = None
        field_list = '*' if limit_fields is None else ', '.join(limit_fields)
        query = f"SELECT {field_list} FROM {obj_cls.get_table_name()} WHERE "
        query += " AND ".join(f"{x} = %s" for x in key_names)
        if lock_type == LockType.FOR_SHARE:
            query += " FOR SHARE"
        elif lock_type == LockType.FOR_UPDATE:
            query += " FOR UPDATE"
        elif lock_type == LockType.FOR_NO_KEY_UPDATE:
            query += " FOR NO KEY UPDATE"
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

    def upsert_object(self, obj: structures._NODBBaseObject, force_update: bool = False):
        if not(force_update or obj.is_new or obj.modified_values):
            return True
        return self.insert_object(obj) if obj.is_new else self.update_object(obj)

    def update_object(self, obj: structures._NODBBaseObject):
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
        self.execute(query, args)
        obj.clear_modified()
        return True

    def insert_object(self, obj: structures._NODBBaseObject):
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
        self.execute(query, args)
        row = self.fetchone()
        if row is not None and row[0] is not None:
            for x in primary_keys:
                obj.set(row[x], x)
        obj.clear_modified()
        return True

    def load_source_file(self,
                         source_file_uuid: str,
                         partition_key: datetime.date,
                         limit_fields: t.Optional[list[str]] = None,
                         lock_type: LockType = LockType.NONE) -> t.Optional[structures.NODBSourceFile]:
        return self.load_object(
            obj_cls=structures.NODBSourceFile,
            filters={"source_uuid": source_file_uuid, "partition_key": partition_key},
            limit_fields=limit_fields,
            lock_type=lock_type
        )

    def save_source_file(self, source_file: structures.NODBSourceFile):
        self.upsert_object(obj=source_file)

    def load_upload_workflow_config(self, workflow_name: str, lock_type: LockType = LockType.NONE) -> t.Optional[
        structures.NODBUploadWorkflow]:
        return self.load_object(
            obj_cls=structures.NODBUploadWorkflow,
            filters={"workflow_name": workflow_name},
            lock_type=lock_type
        )

    def save_upload_workflow_config(self, config: structures.NODBUploadWorkflow):
        self.upsert_object(obj=config)

    def load_user(self,
                  username: str,
                  lock_type: LockType = LockType.NONE) -> t.Optional[structures.NODBUser]:
        return self.load_object(
            obj_cls=structures.NODBUser,
            filters={"username": username},
            lock_type=lock_type
        )

    def save_user(self,
                  user: structures.NODBUser):
        self.upsert_object(
            obj=user,
        )

    def load_permissions(self, roles: t.Optional[list[str]]) -> set[str]:
        if not roles:
            return set()
        permissions = set()
        q = "SELECT permission FROM nodb_permissions WHERE role_name IN ("
        q += ", ".join('%s' for _ in roles)
        q += ")"
        self.execute(q, roles)
        permissions.update(row[0] for row in self.fetchall())
        return permissions

    def grant_permission(self, role_name, permission_name):
        self.execute("SELECT 1 FROM nodb_permissions WHERE role_name = %s and permission = %s", [
            role_name,
            permission_name
        ])
        row = self.fetchone()
        if row is None:
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
        return self.load_object(
            obj_cls=structures.NODBSession,
            filters={"session_id": session_id},
            lock_type=lock_type
        )

    def delete_session(self, session_id: str):
        self.execute("DELETE FROM nodb_sessions WHERE session_id = %s", [session_id])

    def save_session(self,
                     session: structures.NODBSession):
        self.upsert_object(obj=session)

    def record_login(self,
                     username: str,
                     ip_address: str,
                     instance_name: str):
        self.execute("INSERT INTO nodb_logins (username, login_time, login_addr, instance_name) VALUES (%s, CURRENT_TIMESTAMP, %s, %s)", [
            username,
            ip_address,
            instance_name
        ])


@injector.injectable_global
class NODBController:

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._instance = None
        self._connect_args = self.config.as_dict(("cnodc", "nodb_connection"), default={})
        self._connect_args['cursor_factory'] = pge.DictCursor

    def __enter__(self) -> NODBControllerInstance:
        self._instance = NODBControllerInstance(pg.connect(**self._connect_args))
        return self._instance

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._instance.rollback()
        else:
            self._instance.commit()
        self._instance.close()
        self._instance = None
