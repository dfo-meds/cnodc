import enum
from contextlib import contextmanager
import typing as t
import psycopg2
import psycopg2.extras
import zirconium as zr
from autoinject import injector

from cnodc.nodb import NODBObservation, NODBStation, NODBSourceFile
from cnodc.nodb.structures import _NODBBaseObject


class WrappedCursor:

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


class NODBController:

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._connection = None

    def _connect(self):
        if self._connection is None:
            conn_args = self.config.as_dict(("cnodc", "database"), default={})
            self._connection = psycopg2.connect(**conn_args, cursor_factory=psycopg2.extras.DictCursor)

    @contextmanager
    def cursor(self) -> WrappedCursor:
        self._connect()
        try:
            with self._connection.cursor() as cur:
                yield WrappedCursor(cur)
        finally:
            pass

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

    def find_observation(self, pkey: str, limit_fields: t.Union[set[str], list[str], None] = None, cur: WrappedCursor = None) -> t.Optional[NODBObservation]:
        return self.find_nodb_object(NODBObservation, "nodb_observations", pkey, limit_fields, cur)

    def find_station(self, pkey: str, limit_fields: t.Union[set[str], list[str], None] = None, cur: WrappedCursor = None) -> t.Optional[NODBStation]:
        return self.find_nodb_object(NODBStation, "nodb_stations", pkey, limit_fields, cur)

    def find_source_file(self, pkey: str, limit_fields: t.Union[set[str], list[str], None] = None, cur: WrappedCursor = None) -> t.Optional[NODBSourceFile]:
        return self.find_nodb_object(NODBSourceFile, "nodb_source_files", pkey, limit_fields, cur)

    def save_observation(self, obs: NODBObservation, limit_fields: t.Union[set[str], list[str], None] = None, cur: WrappedCursor = None) -> bool:
        return self.save_nodb_object(obs, 'nodb_observations', limit_fields, cur)

    def save_source_file(self, src_f: NODBSourceFile, limit_fields: t.Union[set[str], list[str], None] = None, cur: WrappedCursor = None) -> bool:
        return self.save_nodb_object(src_f, 'nodb_source_files', limit_fields, cur)

    def save_station(self, station: NODBStation, limit_fields: t.Union[set[str], list[str], None] = None, cur: WrappedCursor = None) -> bool:
        return self.save_nodb_object(station, 'nodb_stations', limit_fields, cur)

    def search_observations(self, search_criteria: list[tuple[str, str, t.Optional[t.Any]]], limit=None) -> t.Iterable[NODBObservation]:
        pass
