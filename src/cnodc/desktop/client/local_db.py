import datetime
import itertools
import json
import sqlite3
import pathlib

import zrlog
from autoinject import injector
import typing as t


class CursorWrapper:

    def __init__(self, cursor: sqlite3.Cursor):
        self._cursor = cursor
        self._in_tx = False
        self._log = zrlog.get_logger('cnodc.desktop.db')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.commit()
        else:
            self.rollback()
        self._cursor.close()

    def execute(self, sql: str, parameters: t.Optional[t.Union[tuple, list]] = None):
        self._log.debug(f"{sql}\n{parameters}")
        if parameters:
            return self._cursor.execute(sql, parameters)
        else:
            return self._cursor.execute(sql)

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchone(self):
        return self._cursor.fetchone()

    def execute_script(self, sql_script: str):
        return self._cursor.executescript(sql_script)

    def commit(self):
        if self._in_tx:
            self.execute('COMMIT')
            self._in_tx = False

    def rollback(self):
        if self._in_tx:
            self.execute('ROLLBACK')
            self._in_tx = False

    def begin_transaction(self):
        if not self._in_tx:
            self.execute('BEGIN TRANSACTION')
            self._in_tx = True

    def truncate_table(self, table_name: str):
        self.execute(f'DELETE FROM {table_name}')

    def insert(self, table_name: str, values: dict) -> int:
        keys = list(values.keys())
        values = [self._clean_for_insert(values[k]) for k in keys]
        value_placeholders = ','.join('?' for _ in keys)
        q = f'INSERT INTO {table_name}({",".join(keys)}) VALUES ({value_placeholders})'
        self.execute(q, values)
        return self._cursor.lastrowid

    def update(self, table_name: str, values: dict, where: dict):
        v_keys = list(values.keys())
        w_keys = list(where.keys())
        set_clause = ', '.join(f'{key} = ?' for key in v_keys)
        where_clause = ' AND '.join(f'{key} = ?' for key in w_keys)
        q = f'UPDATE {table_name} SET {set_clause} WHERE {where_clause}'
        values = [itertools.chain((values[v] for v in v_keys), (where[w] for w in w_keys))]
        self.execute(q, values)

    def delete(self, table_name: str, values: dict):
        keys = list(values.keys())
        clause = ' AND '.join(f'{key} = ?' for key in keys)
        q = f'DELETE FROM {table_name} WHERE {clause}'
        self.execute(q, [values[x] for x in keys])

    def _clean_for_insert(self, value):
        if isinstance(value, (dict, tuple, list, set)):
            return json.dumps(value)
        elif isinstance(value, (datetime.datetime, datetime.date)):
            return value.isoformat()
        else:
            return value


@injector.injectable
class LocalDatabase:

    def __init__(self):
        self._database_file = pathlib.Path('~/cnodcqc.local.db').expanduser().absolute().resolve()
        self._connection = None
        self.get_connection()

    def get_connection(self):
        if self._connection is None:
            self._connection = sqlite3.connect(self._database_file, isolation_level=None)
            self._create_db()
        return self._connection

    def cursor(self) -> CursorWrapper:
        return CursorWrapper(self.get_connection().cursor())

    def _create_db(self):
        sql_file = pathlib.Path(__file__).absolute().resolve().parent / 'local_db.sql'
        if not sql_file.exists():
            raise ValueError('schema file not defined')
        with open(sql_file, 'r', encoding='utf-8') as h:
            with self.cursor() as cur:
                cur.execute_script(h.read())
