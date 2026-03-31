import datetime
import decimal
import unittest as ut
from contextlib import contextmanager
import typing as t

import psycopg2.errors
from psycopg2.extras import DictCursor

from cnodc.nodb import NODBControllerInstance, NODBBatch, NODBQueueItem, ScannedFileStatus, QueueStatus, NODBObservation
from cnodc.nodb.controller import NODBControllerBase, LockType, NODBError, SqlState

from psycopg2.extensions import connection


class BlankConn(connection):

    def __init__(self):
        pass

class FastPsycopgError(psycopg2.Error):

    def __init__(self, pgcode):
        super().__init__('oh no')
        self._pgcode = pgcode

    @property
    def pgcode(self):
        return self._pgcode


class MockPostgresConnectionCursor:

    def __init__(self):
        self._commands = []
        self._dummy_cursor = DictCursor(BlankConn())
        self._responses: dict[str, t.Union[Exception, list]] = {}
        self._next_response: list = []
        self.arraysize = self._dummy_cursor.arraysize

    @contextmanager
    def cursor(self):
        yield self

    def execute(self, query: str, args: list = None):
        if args:
            q = self._dummy_cursor.mogrify(query, args).decode('utf-8')
        else:
            q = query
        q = q.replace("\r", " ").replace("\n", " ").replace("\t", " ").strip()
        while "  " in q:
            q = q.replace("  ", " ")
        self._commands.append(q)
        if q in self._responses:
            if isinstance(self._responses[q], Exception):
                self._next_response = []
                raise self._responses[q]
            self._next_response = self._responses[q]
        else:
            self._next_response = []

    def executemany(self, query: str, arg_list: list):
        for arg_set in arg_list:
            self.execute(query, arg_set)

    def callproc(self, procname, parameters):
        self.execute(f'CALL {procname}({','.join('%s' for _ in parameters)})', parameters)

    def copy_expert(self, *args, **kwargs):
        pass

    def fetchone(self):
        if not self._next_response:
            return None
        return self._next_response.pop()

    def fetchall(self):
        while self._next_response:
            yield self._next_response.pop()

    def fetchmany(self, size: t.Optional[int] = None):
        size = size or self.arraysize
        if size >= len(self._next_response):
            ret = self._next_response
            self._next_response = []
        else:
            ret = self._next_response[:size]
            self._next_response = self._next_response[size:]
        return ret

    def commit(self):
        self.execute('COMMIT')

    def rollback(self):
        self.execute('ROLLBACK')

    def reset_all(self):
        self._commands.clear()
        self._responses.clear()
        self._next_response = []


class NODBMockController(NODBControllerBase):

    def __init__(self):
        super().__init__()
        self._conn = MockPostgresConnectionCursor()
        self._stable_sort = True

    def _build_controller_instance(self):
        return self._conn


class TestControllerInstance(ut.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.nodb = NODBMockController()

    def setUp(self):
        self.nodb._conn.reset_all()

    def _add_response(self, query: str, response: t.Union[type, Exception, list[dict]]):
        if isinstance(response, type):
            self.nodb._conn._responses[query] = response()
        elif isinstance(response, Exception):
            self.nodb._conn._responses[query] = response
        else:
            self.nodb._conn._responses[query] = [
                TestControllerInstance.row(x)
                for x in response
            ]

    @staticmethod
    def row(values: dict[str, t.Any]):
        x = {}
        for idx, key in enumerate(values):
            x[idx] = values[key]
            x[key] = values[key]
        return x

    @contextmanager
    def assertQueries(self, queries: list[t.Union[str, tuple[str, list]]]):
        try:
            self.nodb._conn.reset_all()
            actual_queries = []
            for query in queries:
                if isinstance(query, tuple):
                    actual_queries.append(query[0])
                    self._add_response(query[0], query[1])
                else:
                    actual_queries.append(query)
            queries = actual_queries
            yield self.nodb._conn._commands
        finally:
            commands = self.nodb._conn._commands
            comm_len = len(commands)
            query_len = len(queries)
            if not queries == commands:
                msg = 'Query Lists Differ:'
                for i in range(0, max(comm_len, query_len)):
                    if i >= query_len:
                        msg += f"\nQuery {i+1}\n"
                        msg += f"ADDT: {self.nodb._conn._commands[i]}"
                    elif i >= comm_len:
                        msg += f"\nQuery {i+1}\n"
                        msg += f"MISS: {queries[i]}"
                    elif queries[i] != self.nodb._conn._commands[i]:
                        msg += f"\nQuery {i+1}\n"
                        msg += f"EXPC: {queries[i]}\nACTL: {self.nodb._conn._commands[i]}"
                raise AssertionError(msg)
            self.assertEqual(queries, self.nodb._conn._commands)
            self.nodb._conn.reset_all()

    def test_commit(self):
        with self.nodb as db:
            with self.assertQueries(["COMMIT"]):
                db.commit()

    def test_commit_cursor(self):
        with self.nodb as db:
            with self.assertQueries(["COMMIT"]):
                with db.cursor() as cur:
                    cur.commit()

    def test_commit_end(self):
        with self.assertQueries(["COMMIT"]):
            with self.nodb as db:
                pass

    def test_rollback(self):
        with self.nodb as db:
            with self.assertQueries(["ROLLBACK"]):
                db.rollback()

    def test_rollback_cursor(self):
        with self.nodb as db:
            with self.assertQueries(["ROLLBACK"]):
                with db.cursor() as cur:
                    cur.rollback()
    def test_rollback_end(self):
        with self.assertQueries(["ROLLBACK"]):
            with self.assertRaises(ValueError):
                with self.nodb as db:
                    raise ValueError("oh no")

    def test_create_savepoint(self):
        with self.nodb as db:
            with self.assertQueries(["SAVEPOINT foobar"]):
                db.create_savepoint('foobar')

    def test_rollback_to_savepoint(self):
        with self.nodb as db:
            with self.assertQueries(["ROLLBACK TO SAVEPOINT foobar"]):
                db.rollback_to_savepoint('foobar')

    def test_release_savepoint(self):
        with self.nodb as db:
            with self.assertQueries(["RELEASE SAVEPOINT foobar"]):
                db.release_savepoint('foobar')

    def test_record_login(self):
        with self.assertQueries([
            "INSERT INTO nodb_logins (username, login_time, login_addr, instance_name) VALUES ('foobar', CURRENT_TIMESTAMP, '127.0.0.1', 'hello_world')",
            'COMMIT'
        ]):
            with self.nodb as db:
                db.record_login('foobar', '127.0.0.1', 'hello_world')

    def test_delete_session(self):
        with self.assertQueries([
            "DELETE FROM nodb_sessions WHERE session_id = '12345'",
            "COMMIT"
        ]):
            with self.nodb as db:
                db.delete_session('12345')

    def test_remove_permission(self):
        with self.assertQueries([
            "DELETE FROM nodb_permissions WHERE role_name = 'foo' and permission = 'bar'",
            "COMMIT"
        ]):
            with self.nodb as db:
                db.remove_permission('foo', 'bar')

    def test_load_permissions(self):
        with self.assertQueries([
            ("SELECT permission FROM nodb_permissions WHERE role_name IN ('a', 'b')", [{'permission': 'p1'}, {'permission': 'p2'}, {'permission': 'p3'}, {'permission': 'p1'}]),
            "COMMIT"
        ]):
            with self.nodb as db:
                self.assertEqual({'p1', 'p2', 'p3'}, db.load_permissions(['a', 'b']))

    def test_grant_permission_not_exists(self):
        with self.assertQueries([
            "SELECT 1 FROM nodb_permissions WHERE role_name = 'foo' and permission = 'bar'",
            "INSERT INTO nodb_permissions (role_name, permission) VALUES ('foo', 'bar')",
            "COMMIT"
        ]):
            with self.nodb as db:
                db.grant_permission('foo', 'bar')

    def test_grant_permission_exists(self):
        with self.assertQueries([
            ("SELECT 1 FROM nodb_permissions WHERE role_name = 'foo' and permission = 'bar'", [{0: 1}]),
            "COMMIT"
        ]):
            with self.nodb as db:
                db.grant_permission('foo', 'bar')

    def test_attempt_fetch_queue_item_success(self):
        with self.nodb as db:
            with db.cursor() as cur:
                with self.assertQueries([
                    'SAVEPOINT fetch_queue_item',
                    ("SELECT * FROM next_queue_item('foobar'::varchar(126), 'hello'::varchar(126))", [{'item_uuid': '12345'}]),
                    'RELEASE SAVEPOINT fetch_queue_item'
                ]):
                    x = db._attempt_fetch_queue_item('foobar', None, 'hello', cur)
                self.assertIsNotNone(x)
                self.assertEqual('12345', x)

    def test_attempt_fetch_queue_item_with_subqueue_success(self):
        with self.nodb as db:
            with db.cursor() as cur:
                with self.assertQueries([
                    'SAVEPOINT fetch_queue_item',
                    ("SELECT * FROM next_queue_item('foobar'::varchar(126), 'hello'::varchar(126), 'sub'::varchar(126))", [{'item_uuid': '12345'}]),
                    'RELEASE SAVEPOINT fetch_queue_item'
                ]):
                    x = db._attempt_fetch_queue_item('foobar', 'sub', 'hello', cur)
                self.assertIsNotNone(x)
                self.assertEqual('12345', x)

    def test_attempt_fetch_queue_item_no_item(self):
        with self.nodb as db:
            with db.cursor() as cur:
                with self.assertQueries([
                    'SAVEPOINT fetch_queue_item',
                    ("SELECT * FROM next_queue_item('foobar'::varchar(126), 'hello'::varchar(126))", []),
                    'ROLLBACK TO SAVEPOINT fetch_queue_item'
                ]):
                    x = db._attempt_fetch_queue_item('foobar', None, 'hello', cur)
                self.assertIsNone(x)

    def test_attempt_fetch_queue_item_deadlock_error(self):
        with self.nodb as db:
            with db.cursor() as cur:
                with self.assertQueries([
                    'SAVEPOINT fetch_queue_item',
                    ("SELECT * FROM next_queue_item('foobar'::varchar(126), 'hello'::varchar(126))", FastPsycopgError(pgcode='40P01')),
                    'ROLLBACK TO SAVEPOINT fetch_queue_item'
                ]):
                    x = db._attempt_fetch_queue_item('foobar', None, 'hello', cur)
                self.assertIsNone(x)

    def test_attempt_fetch_queue_item_serialization_error(self):
        with self.nodb as db:
            with db.cursor() as cur:
                with self.assertQueries([
                    'SAVEPOINT fetch_queue_item',
                    ("SELECT * FROM next_queue_item('foobar'::varchar(126), 'hello'::varchar(126))", FastPsycopgError(pgcode='40001')),
                    'ROLLBACK TO SAVEPOINT fetch_queue_item'
                ]):
                    x = db._attempt_fetch_queue_item('foobar', None, 'hello', cur)
                self.assertIsNone(x)

    def test_attempt_fetch_queue_item_other_error(self):
        with self.nodb as db:
            with db.cursor() as cur:
                with self.assertQueries([
                    'SAVEPOINT fetch_queue_item',
                    ("SELECT * FROM next_queue_item('foobar'::varchar(126), 'hello'::varchar(126))", psycopg2.errors.ConnectionFailure),
                    'ROLLBACK TO SAVEPOINT fetch_queue_item'
                ]):
                    with self.assertRaises(NODBError):
                        _ = db._attempt_fetch_queue_item('foobar', None, 'hello', cur)

        with self.nodb as db:
            with db.cursor() as cur:
                with self.assertQueries([
                    'SAVEPOINT fetch_queue_item',
                    ("SELECT * FROM next_queue_item('foobar'::varchar(126), 'hello'::varchar(126))", ValueError),
                    'ROLLBACK TO SAVEPOINT fetch_queue_item'
                ]):
                    with self.assertRaises(ValueError):
                        _ = db._attempt_fetch_queue_item('foobar', None, 'hello', cur)

    def test_load_queue_item(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT * FROM nodb_queues WHERE queue_uuid = '12345'", [{'queue_uuid': '12345'}])
            ]):
                x = db.load_queue_item('12345')
                self.assertIsInstance(x, NODBQueueItem)
                self.assertEqual(x.queue_uuid, '12345')

    def test_fetch_next_item(self):
        with self.nodb as db:
            with self.assertQueries([
                'SAVEPOINT fetch_queue_item',
                ("SELECT * FROM next_queue_item('foobar'::varchar(126), 'hello'::varchar(126))", [{'item_uuid': '12345'}]),
                'RELEASE SAVEPOINT fetch_queue_item',
                ("SELECT * FROM nodb_queues WHERE queue_uuid = '12345'", [{'queue_uuid': '12345'}]),
            ]):
                x = db.fetch_next_queue_item('foobar', 'hello')
                self.assertIsInstance(x, NODBQueueItem)
                self.assertEqual(x.queue_uuid, '12345')

    def test_retries(self):
        with self.nodb as db:
            with self.assertQueries([
                'SAVEPOINT fetch_queue_item',
                "SELECT * FROM next_queue_item('foobar'::varchar(126), 'hello'::varchar(126))",
                'ROLLBACK TO SAVEPOINT fetch_queue_item',
                'SAVEPOINT fetch_queue_item',
                "SELECT * FROM next_queue_item('foobar'::varchar(126), 'hello'::varchar(126))",
                'ROLLBACK TO SAVEPOINT fetch_queue_item',
                'SAVEPOINT fetch_queue_item',
                "SELECT * FROM next_queue_item('foobar'::varchar(126), 'hello'::varchar(126))",
                'ROLLBACK TO SAVEPOINT fetch_queue_item',
            ]):
                x = db.fetch_next_queue_item('foobar', 'hello', retries=3)
                self.assertIsNone(x)

    def test_create_queue_item(self):
        with self.nodb as db:
            with self.assertQueries([
                "INSERT INTO nodb_queues (queue_name, subqueue_name, priority, unique_item_name, data, correlation_id) VALUES ('foobar', NULL, 0, NULL, '{\"hello\": \"world\"}', NULL)",
            ]):
                db.create_queue_item('foobar', {'hello': 'world'})

    def test_create_queue_item_with_subqueue(self):
        with self.nodb as db:
            with self.assertQueries([
                "INSERT INTO nodb_queues (queue_name, subqueue_name, priority, unique_item_name, data, correlation_id) VALUES ('foobar', 'bar2', 0, NULL, '{\"hello\": \"world\"}', NULL)",
            ]):
                db.create_queue_item('foobar', {'hello': 'world'}, subqueue_name='bar2')

    def test_create_queue_item_with_unique(self):
        with self.nodb as db:
            with self.assertQueries([
                "INSERT INTO nodb_queues (queue_name, subqueue_name, priority, unique_item_name, data, correlation_id) VALUES ('foobar', NULL, 0, 'foo', '{\"hello\": \"world\"}', NULL)",
            ]):
                db.create_queue_item('foobar', {'hello': 'world'}, unique_item_name='foo')

    def test_create_queue_item_with_priority(self):
        with self.nodb as db:
            with self.assertQueries([
                "INSERT INTO nodb_queues (queue_name, subqueue_name, priority, unique_item_name, data, correlation_id) VALUES ('foobar', NULL, 7, NULL, '{\"hello\": \"world\"}', NULL)",
            ]):
                db.create_queue_item('foobar', {'hello': 'world'}, priority=7)

    def test_fast_update_queue_item(self):
        with self.nodb as db:
            with self.assertQueries([
                "UPDATE nodb_queues SET status = 'UNLOCKED', locked_by = NULL, locked_since = NULL, delay_release = NULL, priority = priority + 0, escalation_level = 0 WHERE queue_uuid = '12345' AND status = 'LOCKED'"
            ]):
                db.fast_update_queue_status(
                    '12345',
                    QueueStatus.UNLOCKED
                )

    def test_fast_update_queue_item_nudge_priority(self):
        with self.nodb as db:
            with self.assertQueries([
                "UPDATE nodb_queues SET status = 'UNLOCKED', locked_by = NULL, locked_since = NULL, delay_release = NULL, priority = priority + 1, escalation_level = 0 WHERE queue_uuid = '12345' AND status = 'LOCKED'"
            ]):
                db.fast_update_queue_status(
                    '12345',
                    QueueStatus.UNLOCKED,
                    reduce_priority=True
                )

    def test_fast_update_queue_item_change_escalation_level(self):
        with self.nodb as db:
            with self.assertQueries([
                "UPDATE nodb_queues SET status = 'UNLOCKED', locked_by = NULL, locked_since = NULL, delay_release = NULL, priority = priority + 0, escalation_level = 5 WHERE queue_uuid = '12345' AND status = 'LOCKED'"
            ]):
                db.fast_update_queue_status(
                    '12345',
                    QueueStatus.UNLOCKED,
                    escalation_level=5
                )

    def test_fast_renew_queue_item(self):
        with self.nodb as db:
            with self.assertQueries([
                "UPDATE nodb_queues SET locked_since = '2015-01-02T03:04:05+00:00' WHERE queue_uuid = '12345' AND status = 'LOCKED'"
            ]):
                self.assertIsInstance(
                    db.fast_renew_queue_item('12345', datetime.datetime(2015, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)),
                    datetime.datetime
                )

    def test_fast_update_queue_item_delayed_release(self):
        with self.nodb as db:
            with self.assertQueries([
                "UPDATE nodb_queues SET status = 'UNLOCKED', locked_by = NULL, locked_since = NULL, delay_release = '2015-01-02T03:04:05+00:00', priority = priority + 0, escalation_level = 0 WHERE queue_uuid = '12345' AND status = 'LOCKED'"
            ]):
                db.fast_update_queue_status(
                    '12345',
                    QueueStatus.UNLOCKED,
                    release_at=datetime.datetime(2015, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
                )

    def test_scanned_file_status_no_modtime_missing(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT was_processed, was_errored FROM nodb_scanned_files WHERE file_path = '/path/file.txt' AND modified_date IS NULL", [])
            ]):
                self.assertIs(
                    db.scanned_file_status('/path/file.txt'),
                    ScannedFileStatus.NOT_PRESENT
                )

    def test_scanned_file_status_modtime_missing(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT was_processed, was_errored FROM nodb_scanned_files WHERE file_path = '/path/file.txt' AND modified_date = '2015-01-02T03:04:05+00:00'", [])
            ]):
                self.assertIs(
                    db.scanned_file_status('/path/file.txt', datetime.datetime(2015, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)),
                    ScannedFileStatus.NOT_PRESENT
                )

    def test_scanned_file_status_no_modtime_processed(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT was_processed, was_errored FROM nodb_scanned_files WHERE file_path = '/path/file.txt' AND modified_date IS NULL", [{'was_processed': 1}])
            ]):
                self.assertIs(
                    db.scanned_file_status('/path/file.txt'),
                    ScannedFileStatus.PROCESSED
                )

    def test_scanned_file_status_modtime_processed(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT was_processed, was_errored FROM nodb_scanned_files WHERE file_path = '/path/file.txt' AND modified_date = '2015-01-02T03:04:05+00:00'", [{'was_processed': 1}])
            ]):
                self.assertIs(
                    db.scanned_file_status('/path/file.txt', datetime.datetime(2015, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)),
                    ScannedFileStatus.PROCESSED
                )

    def test_scanned_file_status_no_modtime_unprocessed(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT was_processed, was_errored FROM nodb_scanned_files WHERE file_path = '/path/file.txt' AND modified_date IS NULL", [{'was_processed': 0, 'was_errored': 0}])
            ]):
                self.assertIs(
                    db.scanned_file_status('/path/file.txt'),
                    ScannedFileStatus.UNPROCESSED
                )

    def test_scanned_file_status_modtime_unprocessed(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT was_processed, was_errored FROM nodb_scanned_files WHERE file_path = '/path/file.txt' AND modified_date = '2015-01-02T03:04:05+00:00'", [{'was_processed': 0, 'was_errored': 0}])
            ]):
                self.assertIs(
                    db.scanned_file_status('/path/file.txt', datetime.datetime(2015, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)),
                    ScannedFileStatus.UNPROCESSED
                )

    def test_mark_scanned_item_failed_no_date(self):
        with self.nodb as db:
            with self.assertQueries([
                "UPDATE nodb_scanned_files SET was_errored = TRUE WHERE file_path = '/path/file.txt' AND modified_date IS NULL AND was_processed = FALSE AND was_errored = FALSE"
            ]):
                db.mark_scanned_item_failed('/path/file.txt')

    def test_mark_scanned_item_failed_with_date(self):
        with self.nodb as db:
            with self.assertQueries([
                "UPDATE nodb_scanned_files SET was_errored = TRUE WHERE file_path = '/path/file.txt' AND modified_date = '2015-01-02T03:04:05+00:00' AND was_processed = FALSE AND was_errored = FALSE"
            ]):
                db.mark_scanned_item_failed('/path/file.txt', datetime.datetime(2015, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc))

    def test_note_scanned_file_no_date(self):
        with self.nodb as db:
            with self.assertQueries([
                "INSERT INTO nodb_scanned_files (file_path, modified_date) VALUES ('/path/file.txt', NULL)"
            ]):
                db.note_scanned_file('/path/file.txt')

    def test_note_scanned_file_with_date(self):
        with self.nodb as db:
            with self.assertQueries([
                "INSERT INTO nodb_scanned_files (file_path, modified_date) VALUES ('/path/file.txt', '2015-01-02T03:04:05+00:00')"
            ]):
                db.note_scanned_file('/path/file.txt', datetime.datetime(2015, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc))

    def test_mark_scanned_success_no_date(self):
        with self.nodb as db:
            with self.assertQueries([
                "UPDATE nodb_scanned_files SET was_processed = TRUE where file_path = '/path/file.txt' AND modified_date IS NULL AND was_processed = FALSE AND was_errored = FALSE"
            ]):
                db.mark_scanned_item_success('/path/file.txt')

    def test_mark_scanned_success_date_exists(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT was_processed FROM nodb_scanned_files WHERE file_path = '/path/file.txt' AND modified_date = '2015-01-02T03:04:05+00:00'", [{'was_processed': False}]),
                "UPDATE nodb_scanned_files SET was_processed = TRUE where file_path = '/path/file.txt' AND (modified_date <= '2015-01-02T03:04:05+00:00' or modified_date IS NULL) AND was_processed = FALSE AND was_errored = FALSE",
            ]):
                db.mark_scanned_item_success('/path/file.txt', datetime.datetime(2015, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc))

    def test_mark_scanned_success_date_no_exists(self):
        with self.nodb as db:
            with self.assertQueries([
                "SELECT was_processed FROM nodb_scanned_files WHERE file_path = '/path/file.txt' AND modified_date = '2015-01-02T03:04:05+00:00'",
                "INSERT INTO nodb_scanned_files (file_path, modified_date) VALUES ('/path/file.txt', '2015-01-02T03:04:05+00:00')",
                "UPDATE nodb_scanned_files SET was_processed = TRUE where file_path = '/path/file.txt' AND (modified_date <= '2015-01-02T03:04:05+00:00' or modified_date IS NULL) AND was_processed = FALSE AND was_errored = FALSE",
            ]):
                db.mark_scanned_item_success('/path/file.txt', datetime.datetime(2015, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc))


    def test_bulk_update(self):
        with self.nodb as db:
            reset = db._max_in_size
            db._max_in_size = 3
            with self.assertQueries([
                "UPDATE nodb_qc_batches SET bar=NULL,foo=2 WHERE qc_batch_id IN (1,2,3)",
                "UPDATE nodb_qc_batches SET bar=NULL,foo=2 WHERE qc_batch_id IN (4,5,6)",
                "UPDATE nodb_qc_batches SET bar=NULL,foo=2 WHERE qc_batch_id IN (7)",
            ]):
                db.bulk_update(NODBBatch, {'foo': 2, 'bar': None}, 'qc_batch_id', [1, 2, 3, 4, 5, 6, 7])

            db._max_in_size = reset

    def test_bulk_update_no_vals(self):
        with self.nodb as db:
            reset = db._max_in_size
            db._max_in_size = 3
            with self.assertQueries([]):
                db.bulk_update(NODBBatch, {'foo': 2, 'bar': None}, 'qc_batch_id', [])

            db._max_in_size = reset

    def test_chunk_for_in(self):
        with self.nodb as db:
            reset = db._max_in_size
            db._max_in_size = 3
            self.assertEqual([x for x in db.chunk_for_in([1, 2, 3, 4, 5, 6, 7])], [[1, 2, 3], [4, 5, 6], [7]])
            db._max_in_size = reset

    def test_chunk_for_in_none(self):
        with self.nodb as db:
            reset = db._max_in_size
            db._max_in_size = 3
            self.assertEqual([x for x in db.chunk_for_in([])], [])
            db._max_in_size = reset

    def test_build_lock_type(self):
        tests = [
            (LockType.FOR_SHARE, " FOR SHARE"),
            (LockType.FOR_UPDATE, " FOR UPDATE"),
            (LockType.FOR_NO_KEY_UPDATE, " FOR NO KEY UPDATE"),
            (LockType.FOR_KEY_SHARE, " FOR KEY SHARE"),
            (LockType.NONE, ""),
            (None, ""),
        ]
        for lock_type, result in tests:
            with self.subTest(lock_type=lock_type):
                self.assertEqual(
                    NODBControllerInstance.build_lock_type_clause(lock_type),
                    result
                )

    def test_extend_selected_fields(self):
        tests = [
            (['a', 'b'], {'c': 'd'}, False, {'a', 'b', 'c', 'batch_uuid'}),
            (['a', 'b'], {'c': 'd'}, True, {'a', 'b', 'c', 'batch_uuid'}),
            (None, None, False, None),
            (['a', 'b'], None, False, {'a', 'b', 'batch_uuid'}),
            (None, {'c': 'd'}, False, None),
            (None, {'c': 'd'}, True, {'batch_uuid', 'c'}),
            (None, None, True, {'batch_uuid'}),
            (['a', 'b', 'batch_uuid'], {'a': 'd', 'batch_uuid': 'foo', 'c': 'd'}, False, {'a', 'b', 'c', 'batch_uuid'}),
        ]
        for limit_select, filters, key_only, results in tests:
            with self.subTest(limit=limit_select, filters=filters, key_only=key_only):
                self.assertEqual(
                    results,
                    NODBControllerInstance.extend_selected_fields(limit_select, filters, key_only, NODBBatch)
                )

    def test_select_clause(self):
        tests = [
            ('foo', [], 'SELECT * FROM foo'),
            ('foo', ['*'], 'SELECT * FROM foo'),
            ('foo', {'c1', 'c2'}, 'SELECT c1,c2 FROM foo'),
        ]
        for tname, cols, result in tests:
            with self.subTest(tname=tname, cols=cols):
                self.assertEqual(result, NODBControllerInstance.build_select_clause(tname, cols, True))

    def test_where_clause(self):
        tests = [
            ({}, None, '', []),
            ({'foo': 'bar'}, None, ' WHERE foo = %s', ['bar']),
            ({'foo': None}, None, ' WHERE foo IS NULL', []),
            ({'foo': (2, '<=')}, None, ' WHERE foo <= %s', [2]),
            ({'foo': (2, '>=')}, None, ' WHERE foo >= %s', [2]),
            ({'foo': (2, '<')}, None, ' WHERE foo < %s', [2]),
            ({'foo': (2, '>')}, None, ' WHERE foo > %s', [2]),
            ({'foo': ((2, 3, 4), 'IN')}, None, ' WHERE foo IN (%s,%s,%s)', [2, 3, 4]),
            ({'foo': (2, '<=', True)}, None, ' WHERE (foo IS NULL OR foo <= %s)', [2]),
            ({'foo': (2, '<=', False)}, None, ' WHERE foo <= %s', [2]),
            ({'foo': 2, 'bar': 3}, None, ' WHERE foo = %s AND bar = %s', [2, 3]),
            ({'foo': 2, 'bar': 3}, 'OR', ' WHERE foo = %s OR bar = %s', [2, 3]),
        ]
        for in_val1, in_val2, out_val1, out_val2 in tests:
            with self.subTest(filters=in_val1, join_str=in_val2):
                clause, params = NODBControllerInstance.build_where_clause(in_val1, in_val2)
                self.assertEqual(clause, out_val1)
                self.assertEqual(params, out_val2)

    def test_order_by_clause(self):
        tests = [
            (None, ""),
            ([], ""),
            ((), ""),
            (["test"], " ORDER BY test ASC"),
            ([("test", True)], " ORDER BY test DESC"),
            ([("test", False)], " ORDER BY test ASC"),
            ([("test", False), "test2", ("test3", True)], " ORDER BY test ASC,test2 ASC,test3 DESC"),
        ]
        for in_val, out_val in tests:
            with self.subTest(input=in_val):
                self.assertEqual(out_val, NODBControllerInstance.build_order_by_clause(in_val))

    def test_escape_copy_value(self):
        tests = [
            (None, "\\N"),
            ("hello\\world", "hello\\\\world"),
            ("hello\rworld", "hello\\rworld"),
            ("hello\tworld", "hello\\tworld"),
            ("hello\nworld", "hello\\nworld"),
            ("hello\bworld", "hello\\bworld"),
            ("hello\fworld", "hello\\fworld"),
            ("hello\vworld", "hello\\vworld"),
            (b"12345", "\\\\x3132333435"),
            (True, 't'),
            (False, 'f'),
            (datetime.date(2015, 1, 2), '2015-01-02'),
            (datetime.datetime(2015, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc), '2015-01-02 03:04:05Z'),
            # TODO: check timezone?
            (['hello', 'world'], '["hello", "world"]'),
            (('hello', 'world'), '["hello", "world"]'),
            ({'hello': 'world'}, '{"hello": "world"}'),
            ("foo", "foo"),
            (1, "1"),
            (2.345, "2.345"),
            (decimal.Decimal("1234.56"), "1234.56")
        ]
        for input_val, output_val in tests:
            with self.subTest(input=input_val):
                self.assertEqual(
                    NODBControllerInstance.escape_copy_value(input_val),
                    output_val
                )

    def test_delete_object(self):
        batch = NODBBatch(batch_uuid='12345')
        with self.nodb as db:
            with self.assertQueries([
                "DELETE FROM nodb_qc_batches WHERE batch_uuid = '12345'"
            ]):
                db.delete_object(batch)

    def test_composite_object(self):
        obs = NODBObservation(obs_uuid='12345', received_date='2015-01-02')
        with self.nodb as db:
            with self.assertQueries([
                "DELETE FROM nodb_obs WHERE obs_uuid = '12345' AND received_date = '2015-01-02'::date"
            ]):
                db.delete_object(obs)

    def test_count_objects(self):
        with self.nodb as db:
            with self.assertQueries([
                ('SELECT COUNT(*) FROM nodb_obs', [{0: 5}])
            ]):
                self.assertEqual(5, db.count_objects(NODBObservation))

    def test_count_objects_filters(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT COUNT(*) FROM nodb_obs WHERE foo = 'bar'", [{0: 5}])
            ]):
                self.assertEqual(5, db.count_objects(NODBObservation, filters={'foo': 'bar'}))

    def test_load_object_none(self):
        with self.nodb as db:
            with self.assertQueries([
                "SELECT * FROM nodb_obs WHERE obs_uuid = '12345'"
            ]):
                self.assertIsNone(db.load_object(NODBObservation, filters={'obs_uuid': '12345'}))

    def test_load_object_one(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT * FROM nodb_obs WHERE obs_uuid = '12345'", [{'obs_uuid': '12345'}])
            ]):
                obj = db.load_object(NODBObservation, filters={'obs_uuid': '12345'})
                self.assertIsInstance(obj, NODBObservation)
                self.assertEqual(obj.obs_uuid, '12345')

    def test_load_object_two(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT * FROM nodb_obs WHERE obs_uuid = '12345'", [{'obs_uuid': '12345'}, {'obs_uuid': '12345'}])
            ]):
                obj = db.load_object(NODBObservation, filters={'obs_uuid': '12345'})
                self.assertIsInstance(obj, NODBObservation)
                self.assertEqual(obj.obs_uuid, '12345')

    def test_load_object_lock(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT * FROM nodb_obs WHERE obs_uuid = '12345' FOR SHARE", [{'obs_uuid': '12345'}])
            ]):
                obj = db.load_object(NODBObservation, filters={'obs_uuid': '12345'}, lock_type=LockType.FOR_SHARE)
                self.assertIsInstance(obj, NODBObservation)
                self.assertEqual(obj.obs_uuid, '12345')

    def test_load_object_limit_fields(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT bar,foo,obs_uuid,received_date FROM nodb_obs WHERE obs_uuid = '12345'", [{'obs_uuid': '12345'}])
            ]):
                obj = db.load_object(NODBObservation, filters={'obs_uuid': '12345'}, limit_fields=['foo', 'bar'])
                self.assertIsInstance(obj, NODBObservation)
                self.assertEqual(obj.obs_uuid, '12345')

    def test_load_object_key_only(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT obs_uuid,received_date FROM nodb_obs WHERE obs_uuid = '12345'", [{'obs_uuid': '12345'}])
            ]):
                obj = db.load_object(NODBObservation, filters={'obs_uuid': '12345'}, key_only=True)
                self.assertIsInstance(obj, NODBObservation)
                self.assertEqual(obj.obs_uuid, '12345')

    def test_update_object_no_changes(self):
        with self.nodb as db:
            with self.assertQueries([]):
                obj = NODBBatch(batch_uuid='12345', is_new=False)
                self.assertTrue(db.update_object(obj))


    def test_update_object_changes(self):
        with self.nodb as db:
            with self.assertQueries([
                "UPDATE nodb_obs SET max_depth=25.0,min_depth=5.0 WHERE obs_uuid = '12345' AND received_date = '2015-01-02'::date"
            ]):
                obj = NODBObservation(obs_uuid='12345', received_date='2015-01-02', location='POINT(5 6)', is_new=False)
                obj.min_depth = 5
                obj.max_depth = 25
                self.assertTrue(db.update_object(obj))

    def test_update_object_changes_no_pk_change(self):
        with self.nodb as db:
            with self.assertQueries([
                "UPDATE nodb_obs SET max_depth=25.0,min_depth=5.0 WHERE obs_uuid = '23456' AND received_date = '2015-01-02'::date"
            ]):
                obj = NODBObservation(obs_uuid='12345', received_date='2015-01-02', location='POINT(5 6)', is_new=False)
                obj.min_depth = 5
                obj.max_depth = 25
                obj.obs_uuid = '23456'
                self.assertTrue(db.update_object(obj))

    def test_insert_object(self):
        with self.nodb as db:
            with self.assertQueries([
                ("INSERT INTO nodb_obs (max_depth, min_depth, received_date) VALUES (25.0, 5.0, '2015-10-11'::date) RETURNING obs_uuid,received_date", [{"obs_uuid": "12345", "received_date": "2015-10-11"}])
            ]):
                obj = NODBObservation(received_date='2015-10-11', is_new=False)
                obj.is_new = True
                obj.min_depth = 5
                obj.max_depth = 25
                self.assertEqual(2, len(obj.modified_values))
                self.assertTrue(obj.is_new)
                self.assertTrue(db.insert_object(obj))
                self.assertEqual(0, len(obj.modified_values))
                self.assertEqual(obj.obs_uuid, '12345')
                self.assertEqual(obj.received_date, datetime.date(2015, 10, 11))
                self.assertFalse(obj.is_new)

    def test_insert_object_defaults(self):
        with self.nodb as db:
            with self.assertQueries([
                ("INSERT INTO nodb_qc_batches DEFAULT VALUES RETURNING batch_uuid", [{"batch_uuid": "12345"}])
            ]):
                obj = NODBBatch()
                self.assertTrue(obj.is_new)
                self.assertTrue(db.insert_object(obj))
                self.assertEqual(obj.batch_uuid, '12345')
                self.assertEqual(0, len(obj.modified_values))
                self.assertFalse(obj.is_new)

    def test_upsert_to_insert(self):
        with self.nodb as db:
            with self.assertQueries([
                ("INSERT INTO nodb_qc_batches DEFAULT VALUES RETURNING batch_uuid", [{"batch_uuid": "12345"}])
            ]):
                obj = NODBBatch()
                self.assertTrue(obj.is_new)
                self.assertTrue(db.upsert_object(obj))
                self.assertEqual(obj.batch_uuid, '12345')
                self.assertEqual(0, len(obj.modified_values))
                self.assertFalse(obj.is_new)

    def test_upsert_to_update(self):
        with self.nodb as db:
            with self.assertQueries([
                "UPDATE nodb_obs SET max_depth=25.0,min_depth=5.0 WHERE obs_uuid = '12345' AND received_date = '2015-01-02'::date"
            ]):
                obj = NODBObservation(obs_uuid='12345', received_date='2015-01-02', location='POINT(5 6)', is_new=False)
                obj.min_depth = 5
                obj.max_depth = 25
                self.assertTrue(db.upsert_object(obj))

    def test_upsert_to_update_no_changes(self):
        with self.nodb as db:
            with self.assertQueries([]):
                obj = NODBObservation(obs_uuid='12345', received_date='2015-01-02', location='POINT(5 6)', is_new=False)
                self.assertTrue(db.upsert_object(obj))

    def test_stream_objects(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT * FROM nodb_qc_batches", [{"batch_uuid": "1"}, {"batch_uuid": "2"}])
            ]):
                x = [
                    x.batch_uuid
                    for x in db.stream_objects(NODBBatch)
                ]
                self.assertIn("1", x)
                self.assertIn("2", x)

    def test_stream_objects_order_by(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT * FROM nodb_qc_batches ORDER BY foo ASC,bar DESC", [{"batch_uuid": "1"}, {"batch_uuid": "2"}])
            ]):
                x = [
                    x.batch_uuid
                    for x in db.stream_objects(NODBBatch, order_by=['foo', ('bar', True)])
                ]
                self.assertIn("1", x)
                self.assertIn("2", x)

    def test_stream_objects_filter(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT * FROM nodb_qc_batches WHERE foo = 'bar'", [{"batch_uuid": "1"}, {"batch_uuid": "2"}])
            ]):
                x = [
                    x.batch_uuid
                    for x in db.stream_objects(NODBBatch, filters={'foo': 'bar'})
                ]
                self.assertIn("1", x)
                self.assertIn("2", x)

    def test_stream_objects_lock(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT * FROM nodb_qc_batches FOR UPDATE", [{"batch_uuid": "1"}, {"batch_uuid": "2"}])
            ]):
                x = [
                    x.batch_uuid
                    for x in db.stream_objects(NODBBatch, lock_type=LockType.FOR_UPDATE)
                ]
                self.assertIn("1", x)
                self.assertIn("2", x)

    def test_stream_objects_key_only(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT batch_uuid FROM nodb_qc_batches", [{"batch_uuid": "1"}, {"batch_uuid": "2"}])
            ]):
                x = [
                    x.batch_uuid
                    for x in db.stream_objects(NODBBatch, key_only=True)
                ]
                self.assertIn("1", x)
                self.assertIn("2", x)

    def test_stream_objects_limit_fields(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT batch_uuid,foo FROM nodb_qc_batches", [{"batch_uuid": "1"}, {"batch_uuid": "2"}])
            ]):
                x = [
                    x.batch_uuid
                    for x in db.stream_objects(NODBBatch, limit_fields=['foo'])
                ]
                self.assertIn("1", x)
                self.assertIn("2", x)

    def test_stream_objects_raw(self):
        with self.nodb as db:
            with self.assertQueries([
                ("SELECT * FROM nodb_qc_batches", [{"batch_uuid": "1"}, {"batch_uuid": "2"}])
            ]):
                x = [
                    x
                    for x in db.stream_objects(NODBBatch, raw=True)
                ]
                self.assertIsInstance(x[0], dict)
                self.assertEqual(x[0][0], '1')
                self.assertEqual(x[0]['batch_uuid'], '1')

    def test_sql_state(self):
        ex = NODBError('foo', 1, '23505')
        self.assertEqual(ex.sql_state(), SqlState.UNIQUE_VIOLATION)

    def test_sql_state_bad(self):
        ex = NODBError('foo', 1, 'FFFFF')
        self.assertIsNone(ex.sql_state())