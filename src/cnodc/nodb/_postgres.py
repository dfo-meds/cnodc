import enum
import json
import tempfile
from contextlib import contextmanager
import typing as t
from collections.abc import Sequence
import psycopg2
import psycopg2.extras
import zirconium as zr
import datetime
from autoinject import injector


from cnodc.nodb import NODBWorkingObservation, NODBStation, NODBSourceFile
from cnodc.nodb.structures import _NODBBaseObject, NODBQCBatch
from .proto import NODBDatabaseProtocol, NODBTransaction


class WrappedCursor(NODBTransaction):

    def __init__(self, cur):
        self._cur = cur

    def __getattr__(self, item):
        return getattr(self._cur, item)

    def __setattr__(self, item, value):
        setattr(self._cur, item, value)

    def execute(self, query: str, args: t.Union[list, dict, tuple, None] = None):
        self._cur.execute(query, args)

    def executemany(self, query: str, args_list: t.Iterable[t.Union[list, dict, tuple, None]] = None):
        self._cur.executemany(query, args_list)

    def callproc(self, procname: str, parameters: t.Union[list, tuple, None] = None):
        return self._cur.callproc(procname, parameters)

    def mogrify(self, query: str, args: t.Union[list, dict, tuple, None] = None) -> str:
        return self._cur.mogrify(query, args)

    def fetchone(self) -> psycopg2.extras.DictRow:
        return self._cur.fetchone()

    def fetchmany(self, size=None) -> t.Iterable[psycopg2.extras.DictRow]:
        if size is None:
            return self._cur.fetchmany()
        else:
            return self._cur.fetchmany(size)

    def fetchall(self) -> t.Iterable[psycopg2.extras.DictRow]:
        return self._cur.fetchall()

    def batch_fetchall(self, batch_size=None) -> t.Iterable[psycopg2.extras.DictRow]:
        results = self.fetchmany(batch_size)
        while results:
            yield from results
            results = self.fetchmany(batch_size)

    def scroll(self, value, mode='relative'):
        return self._cur.scroll(value, mode)

    @property
    def rowcount(self) -> t.Optional[int]:
        return self._cur.rowcount

    @property
    def query(self) -> str:
        return self._cur.query

    @property
    def statusmessage(self) -> str:
        return self._cur.statusmessage

    def commit(self):
        self._cur.connection.commit()

    def rollback(self):
        self._cur.connection.rollback()

    def close(self):
        self._cur.close()

    def create_savepoint(self, name):
        pass

    def rollback_to_savepoint(self, name):
        pass

    def release_savepoint(self, name):
        pass

    def copy_expert(self, *args, **kwargs):
        return self._cur.copy_expert(*args, **kwargs)


class NODBPostgresController(NODBDatabaseProtocol):

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._connection = None

    def _connect(self):
        if self._connection is None:
            conn_args = self.config.as_dict(("cnodc", "database"), default={})
            self._connection = psycopg2.connect(**conn_args, cursor_factory=psycopg2.extras.DictCursor)

    @contextmanager
    def start_transaction(self) -> WrappedCursor:
        self._connect()
        try:
            with self._connection.cursor() as cur:
                yield WrappedCursor(cur)
        finally:
            pass

    def start_bare_transaction(self) -> WrappedCursor:
        self._connect()
        return self._connection.cursor()

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
            return NODBPostgresController.escape_copy_value(json.dumps(v))
        else:
            return str(v)

    def spooled_copy(self,
                     copy_query: str,
                     values: t.Iterable[Sequence],
                     mem_size: int = 80000,
                     column_sep: str = "\t",
                     row_sep: str = "\n",
                     tx: t.Optional[WrappedCursor] = None):
        if tx is None:
            with self.start_transaction() as tx:
                self.spooled_copy(copy_query, values, mem_size, column_sep, row_sep, tx)
                tx.commit()
        mem_file = tempfile.SpooledTemporaryFile(mem_size, mode="w+", encoding="utf-8")
        for value_list in values:
            s = column_sep.join(NODBPostgresController.escape_copy_value(v) for v in value_list) + row_sep
            mem_file.write(s)
        mem_file.seek(0, 0)
        tx.copy_expert(copy_query, mem_file)

    def create_queue_item(self,
                          queue_name: str,
                          data: dict,
                          priority: int = 0,
                          unique_key: str = None,
                          tx: t.Optional[WrappedCursor] = None):
        if tx is None:
            with self.start_transaction() as tx:
                self.create_queue_item(queue_name, data, priority, unique_key, tx)
                tx.commit()
        tx.execute("INSERT INTO nodb_queue (queue_name, priority, unique_key, data) VALUES (%s, %s, %s, %s)", (
            queue_name,
            priority,
            unique_key,
            json.dumps(data)
        ))

    def batch_create_queue_item(self,
                                values: t.Iterable[Sequence[str, dict, t.Optional[int], t.Optional[str]]],
                                tx: t.Optional[WrappedCursor] = None):
        self.spooled_copy(
            copy_query="COPY nodb_queue (queue_name, priority, unique_key, data) FROM STDIN",
            values=(
                (x[0], x[2] if len(x) > 2 else 0, x[3] if len(x) > 3 else None, x[1])
                for x in values
            ),
            tx=tx
        )

    def next_queue_item(self,
                        queue_name: str,
                        app_id: str,
                        retries: int = 5) -> t.Optional[str]:
        while retries > 0:
            item = self._attempt_fetch_queue_item(queue_name, app_id)
            if item is not None:
                return item
            retries -= 1
        return None

    def _attempt_fetch_queue_item(self, queue_name: str, app_id: str) -> t.Optional[str]:
        with self.start_transaction() as tx:
            try:
                tx.execute("SELECT next_queue_item(%s, %s)", (queue_name, app_id))
                item = tx.fetchone()
                tx.commit()
                return item[0] if item else None
            except (KeyboardInterrupt, SystemExit) as ex:
                tx.rollback()
                raise ex
            except Exception as ex:
                tx.rollback()
                return None

    def queue_source_file_download(self, source_file: NODBSourceFile, priority: int = 0, tx: t.Optional[WrappedCursor] = None):
        self.create_queue_item(
            queue_name="source_file_download",
            data={"source_file_uuid": source_file.pkey},
            priority=priority,
            tx=tx
        )

    def queue_source_file_decode_error(self, source_file: NODBSourceFile, priority: int = 0, tx: t.Optional[WrappedCursor] = None):
        self.create_queue_item(
            queue_name="source_file_errors",
            data={"source_file_uuid": source_file.pkey},
            priority=priority,
            tx=tx
        )

    def queue_basic_qc_review(self, batch: NODBQCBatch, priority: int = 0, tx: t.Optional[NODBTransaction] = None):
        self.create_queue_item(
            queue_name="basic_qc_manual",
            data={"batch_uuid": batch.pkey},
            priority=priority,
            tx=tx
        )

    def queue_basic_qc_process(self, batch: NODBQCBatch, priority: int = 0, tx: t.Optional[NODBTransaction] = None):
        self.create_queue_item(
            queue_name="basic_qc_second",
            data={"batch_uuid": batch.pkey},
            priority=priority,
            tx=tx
        )

    def queue_next_qc(self, batch: NODBQCBatch, priority: int = 0, tx: t.Optional[NODBTransaction] = None):
        self.create_queue_item(
            queue_name="qc_orchestrator",
            data={"batch_uuid": batch.pkey},
            priority=priority,
            tx=tx
        )


"""

    def find_nodb_object(self, nodb_cls: type, table_name: str, pkey: str, limit_fields: t.Union[set[str], list[str], None] = None, cur: WrappedCursor = None):
        if cur is None:
            with self.cursor() as cur:
                return self.find_nodb_object(nodb_cls, table_name, pkey, limit_fields, cur)
        if limit_fields:
            limit_fields = set(limit_fields)
            if 'pkey' not in limit_fields:
                limit_fields.add('pkey')
        else:
            limit_fields = None
        fields = '*' if limit_fields is not None else ', '.join(limit_fields)
        cur.execute(f'SELECT {fields} FROM {table_name} WHERE pkey = %s', (pkey,))
        first = cur.fetchone()
        if first:
            return nodb_cls(**{
                x: first[x]
                for x in first.keys()
            })
        return None

    def _cast(self, obj):
        if isinstance(obj, enum.Enum):
            return obj.value()
        elif isinstance(obj, (dict, list, set, tuple)):
            return psycopg2.extras.Json(obj)
        return obj

    def create_nodb_object(self, nodb_obj: _NODBBaseObject, table_name: str, limit_fields: t.Union[set[str], list[str], None] = None, cur: WrappedCursor = None):
        if cur is None:
            with self.cursor() as cur:
                return self.create_nodb_object(nodb_obj, table_name, limit_fields, cur)
        if nodb_obj.pkey:
            raise ValueError("Cannot create object from an existing object, create a new one first")
        fields = set(x for x in nodb_obj.modified_values if limit_fields is None or x in limit_fields)
        if not fields:
            raise ValueError("No fields to insert")
        cur.execute(f"INSERT INTO {table_name} ({','.join(fields)}) VALUES ({','.join('%s' for _ in fields)}) RETURNING pkey", [
            self._cast(nodb_obj.get(fn)) for fn in fields
        ])
        cur.commit()
        first = cur.fetchone()
        if first:
            nodb_obj.pkey = first[0]
            return True
        return False

    def update_nodb_object(self, nodb_obj: _NODBBaseObject, table_name: str, limit_fields: t.Union[set[str], list[str], None] = None, cur: WrappedCursor = None):
        if cur is None:
            with self.cursor() as cur:
                return self.update_nodb_object(nodb_obj, table_name, limit_fields, cur)
        if nodb_obj.pkey is None:
            raise ValueError("Cannot update an object that doesn't already exist")
        fields = set(x for x in nodb_obj.modified_values if limit_fields is None or x in limit_fields)
        if not fields:
            return True
        set_clause = ', '.join(f'{fn} = %s' for fn in fields)
        cur.execute(f"UPDATE {table_name} SET {set_clause} WHERE pkey = %s", [
            *[self._cast(nodb_obj.get(fn)) for fn in fields],
            nodb_obj.pkey
        ])
        cur.commit()
        return True

    def save_nodb_object(self, obj: _NODBBaseObject, table_name: str, limit_fields: t.Union[set[str], list[str], None] = None, cur: WrappedCursor = None):
        if obj.pkey is None:
            return self.create_nodb_object(obj, table_name, limit_fields, cur)
        else:
            return self.update_nodb_object(obj, table_name, limit_fields, cur)

"""