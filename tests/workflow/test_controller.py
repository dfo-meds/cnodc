import functools
import gzip
import hashlib
from autoinject import injector
import datetime

from cnodc.storage.local import LocalHandle
from cnodc.workflow import WorkflowController
from cnodc.workflow.payloads import WorkflowPayload, FilePayload, BatchPayload
from cnodc.nodb.structures import NODBQueueItem
from cnodc.util import CNODCError
from core import BaseTestCase, InjectableDict


class TestWorkflowController(BaseTestCase):

    def test_handle_file_upload(self):
        workflow = WorkflowController("test", {})
        test_file = self.temp_dir / "hello.txt"
        with open(test_file, "w") as h:
            h.write("hello world")
        handle, storage_tier = workflow._handle_file_upload(
            test_file,
            'hello2.txt',
            {},
            {
                'directory': str(self.temp_dir),
                'tier': 'frequent',
            }
        )
        self.assertEqual(str(handle), str(self.temp_dir / "hello2.txt"))
        self.assertTrue((self.temp_dir / 'hello2.txt').exists())
        self.assertTrue(handle.exists())
        # doesn't support storage tiers - hard to test
        self.assertIsNone(storage_tier)

    def test_handle_file_upload_missing_directory(self):
        workflow = WorkflowController("test", {})
        test_file = self.temp_dir / "hello.txt"
        with open(test_file, "w") as h:
            h.write("hello world")
        with self.assertRaises(CNODCError):
            workflow._handle_file_upload(
                test_file,
                'hello2.txt',
                {},
                {
                    'tier': 'frequent',
                }
            )

    def test_handle_file_upload_none_directory(self):
        workflow = WorkflowController("test", {})
        test_file = self.temp_dir / "hello.txt"
        with open(test_file, "w") as h:
            h.write("hello world")
        with self.assertRaises(CNODCError):
            workflow._handle_file_upload(
                test_file,
                'hello2.txt',
                {},
                {
                    'directory': None,
                    'tier': 'frequent',
                }
            )

    def test_handle_file_upload_empty_directory(self):
        workflow = WorkflowController("test", {})
        test_file = self.temp_dir / "hello.txt"
        with open(test_file, "w") as h:
            h.write("hello world")
        with self.assertRaises(CNODCError):
            workflow._handle_file_upload(
                test_file,
                'hello2.txt',
                {},
                {
                    'directory': '',
                    'tier': 'frequent',
                }
            )

    def test_handle_file_upload_bad_directory(self):
        workflow = WorkflowController("test", {})
        test_file = self.temp_dir / "hello.txt"
        with open(test_file, "w") as h:
            h.write("hello world")
        with self.assertRaises(CNODCError):
            workflow._handle_file_upload(
                test_file,
                'hello2.txt',
                {},
                {
                    'directory': 'protocol://test_hello/hello/',
                    'tier': 'frequent',
                }
            )

    def test_handle_file_upload_already_exists_global(self):
        workflow = WorkflowController("test", {})
        test_file = self.temp_dir / "hello.txt"
        with open(test_file, "w") as h:
            h.write("hello world")
        with self.assertRaises(CNODCError):
            workflow._handle_file_upload(
                test_file,
                'hello.txt',
                {},
                {
                    'directory': self.temp_dir,
                    'tier': 'frequent',
                    'allow_overwrite': 'never'
                }
            )

    def test_handle_file_upload_already_exists_global_allowed(self):
        workflow = WorkflowController("test", {})
        test_file = self.temp_dir / "hello.txt"
        with open(test_file, "w") as h:
            h.write("hello world")
        handle, tier = workflow._handle_file_upload(
            test_file,
            'hello.txt',
            {},
            {
                'directory': self.temp_dir,
                'tier': 'frequent',
                'allow_overwrite': 'always'
            }
        )
        self.assertEqual(handle.name(), 'hello.txt')
        self.assertTrue(handle.exists())

    def test_handle_file_upload_already_exists_local(self):
        workflow = WorkflowController("test", {})
        test_file = self.temp_dir / "hello.txt"
        with open(test_file, "w") as h:
            h.write("hello world")
        with self.assertRaises(CNODCError):
            workflow._handle_file_upload(
                test_file,
                'hello.txt',
                {
                    'allow-overwrite': '0',
                },
                {
                    'directory': self.temp_dir,
                    'tier': 'frequent',
                }
            )

    def test_handle_file_upload_already_exists_local_allowed(self):
        workflow = WorkflowController("test", {})
        test_file = self.temp_dir / "hello.txt"
        with open(test_file, "w") as h:
            h.write("hello world")
        handle, tier = workflow._handle_file_upload(
            test_file,
            'hello.txt',
            {
                'allow-overwrite': '1',
            },
            {
                'directory': self.temp_dir,
                'tier': 'frequent',
            }
        )
        self.assertEqual(handle.name(), 'hello.txt')
        self.assertTrue(handle.exists())

    def test_substitute_headers(self):
        headers = {
            'test_str': 'foo',
            'test_int': 5,
        }
        t = WorkflowController._substitute_headers
        self.assertEqual('test_str', t("test_str", headers))
        self.assertEqual('test_foo_str', t('test_%{test_str}_str', headers))
        self.assertEqual('test_5_str', t('test_%{test_int}_str', headers))
        now_ = datetime.datetime(2015, 2, 10, 19, 10, 15, 539432, datetime.timezone.utc)
        self.assertEqual('2015-02-10T19:10:15.539432+00:00', t('%{now}', headers, _now=now_))

    def test_bad_file_names(self):
        for invalid_file_name in (
            'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9', 'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9',
            'CON.txt', 'PRN.txt', 'AUX.txt', 'NUL.txt', 'COM1.txt', 'COM2.txt', 'COM3.txt', 'COM4.txt', 'COM5.txt', 'COM6.txt', 'COM7.txt', 'COM8.txt', 'COM9.txt', 'LPT1.txt', 'LPT2.txt', 'LPT3.txt', 'LPT4.txt', 'LPT5.txt', 'LPT6.txt', 'LPT7.txt', 'LPT8.txt', 'LPT9.txt',
            '',
            '01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789',
        ):
            with self.subTest(filename=invalid_file_name):
                self.assertIsNone(WorkflowController._sanitize_filename(invalid_file_name))

    def test_invalid_file_names(self):
        tests = {
            'abc_-é\r\n\t⩐⻰ⳓ⶷⃬❨⹗◝✉⨘ⶣ⠷⹱⪭⾴⍧ⓒⶭ⧨ⶎ⁳ⶇ⑦Ⲷ⼣⡰♱⃶⪙⳴⯥⪧⯦⍷ⲙ⿪⑆⊲⃭⌠⪮❇␄∂ⴊ◩⟢⹫ⶅ⡳▫⼃⼟⤃‶⊷⟂⇨ℐ⥔⾪⦩╋⎙⛾⇌ⴉ⏬✖▕ⰭⰌ⋯⏎⭉⪝⽐⳨⋂⻭⻱␹⠕⾌↷⃢∠Ⓢ⫒⥍⁣✾⩐⛅⒄⮖⮺⼛⿴✂.txt': 'abc_-.txt',
            '.test.txt.': '.test.txt'
        }
        for test_file_name in tests:
            with self.subTest(filename=test_file_name, expected_result=tests[test_file_name]):
                self.assertEqual(tests[test_file_name], WorkflowController._sanitize_filename(test_file_name))

    def test_good_file_names(self):
        for good_file_name in (
            'test.txt', 'test1234.txt', 'test_test.txt', 'test_test', 'TEST.txt', 'test-test.txt',
            'test-_testTEST3412.txt'
        ):
            with self.subTest(filename=good_file_name):
                self.assertEqual(good_file_name, WorkflowController._sanitize_filename(good_file_name))

    def test_sanitize_for_storage(self):
        tests = {
            'test': 'test', 5: '5', 'A5': 'A5', 'A_5': 'A_5', 'A:5': 'A:5', 'A;5': 'A;5','A.5': 'A.5', 'A,5': 'A,5',
            'A\\5': 'A\\5', 'A/5': 'A/5', 'A"5': 'A"5', "A'5": "A'5", 'A?5': 'A?5', 'A!5': 'A!5', 'A(5': 'A(5',
            'A)5': 'A)5', 'A{5': 'A{5', 'A}5': 'A}5', 'A[5': 'A[5', 'A]5': 'A]5', 'A@5': 'A@5', 'A>5': 'A>5',
            'A<5': 'A<5', 'A=5': 'A=5', 'A-5': 'A-5', 'A+5': 'A+5', 'A*5': 'A*5', 'A#5': 'A#5', 'A$5': 'A$5',
            'A&5': 'A&5', 'A`5': 'A`5', 'A|5': 'A|5', 'A~5': 'A~5', 'A^5': 'A^5', 'A%5': 'A%255', 'Aé5': 'A%C3%A95'
        }
        for test_value in tests:
            with self.subTest(input_value=test_value, expected_value=tests[test_value]):
                self.assertEqual(tests[test_value], WorkflowController._sanitize_storage_metadata(test_value))

    def test_determine_random_file_name(self):
        workflow = WorkflowController("test", {})
        filename = workflow._determine_filename({})
        self.assertIsNotNone(filename)
        self.assertRegex(filename, '[0-9a-f-]{36}')

    def test_determine_file_name_from_pattern(self):
        workflow = WorkflowController("test", {
            "filename_pattern": "file_%{lmd}.txt"
        })
        filename = workflow._determine_filename({'lmd': '2015-01-02T01:02:03'})
        self.assertEqual(filename, 'file_2015-01-02T010203.txt')

    def test_determine_file_name_from_metadata(self):
        workflow = WorkflowController("test", {
            "accept_user_filename": True
        })
        filename = workflow._determine_filename({'filename': 'hello_world.txt'})
        self.assertEqual(filename, 'hello_world.txt')

    def test_determine_file_name_from_metadata_not_allowed(self):
        workflow = WorkflowController("test", {})
        filename = workflow._determine_filename({'filename': 'hello_world.txt'})
        self.assertRegex(filename, '[0-9a-f-]{36}')

    def test_determine_file_name_from_metadata_not_provided(self):
        workflow = WorkflowController("test", {'accepts_user_filename': True})
        filename = workflow._determine_filename({})
        self.assertRegex(filename, '[0-9a-f-]{36}')

    def test_determine_file_name_from_static_filename(self):
        workflow = WorkflowController("test", {})
        filename = workflow._determine_filename({'default-filename': 'foobar.txt'})
        self.assertEqual(filename, 'foobar.txt')

    def test_precedence_file_name_pattern_first(self):
        workflow = WorkflowController("test", {
            'accept_user_filename': True,
            'filename_pattern': 'file_%{lmd}.txt',
        })
        filename = workflow._determine_filename({
            'default-filename': 'foobar.txt',
            'lmd': '2015-01-02T01:02:03',
            'filename': 'hello.txt'
        })
        self.assertEqual(filename, 'file_2015-01-02T010203.txt')

    def test_precedence_user_file_name_second(self):
        workflow = WorkflowController("test", {
            'accept_user_filename': True,
        })
        filename = workflow._determine_filename({
            'default-filename': 'foobar.txt',
            'lmd': '2015-01-02T01:02:03',
            'filename': 'hello.txt'
        })
        self.assertEqual(filename, 'hello.txt')

    def test_workflow_steps_no_steps(self):
        workflow = WorkflowController("test", {})
        self.assertFalse(workflow.step_list())
        self.assertEqual((-1, False), workflow._validate_step(None))
        self.assertRaises(CNODCError, workflow._validate_step, 'step1')
        self.assertRaises(CNODCError, workflow._get_next_step, None)
        self.assertRaises(CNODCError, workflow._get_next_step, 'step1')
        self.assertRaises(CNODCError, workflow._get_step_info, 'step1')
        self.assertFalse(workflow.has_more_steps(None))
        self.assertRaises(CNODCError, workflow.has_more_steps, 'step1')

    def test_workflow_steps_good_steps(self):
        workflow = WorkflowController("test", {
            "processing_steps": {
                'step2': {
                    'name': 'step2',
                    'order': 2
                },
                'step1': {
                    'name': 'step1',
                    'order': 1,
                },
                'step4': {
                    'name': 'step4',
                    'order': 10
                }
            }
        })
        self.assertEqual(workflow.step_list(), ['step1', 'step2', 'step4'])
        self.assertEqual(workflow._validate_step(None), (-1, True))
        self.assertEqual(workflow._validate_step('step1'), (0, True))
        self.assertEqual(workflow._validate_step('step2'), (1, True))
        self.assertEqual(workflow._validate_step('step4'), (2, False))
        self.assertRaises(CNODCError, workflow._validate_step, 'step3')
        self.assertTrue(workflow.has_more_steps(None))
        self.assertTrue(workflow.has_more_steps('step1'))
        self.assertTrue(workflow.has_more_steps('step2'))
        self.assertRaises(CNODCError, workflow.has_more_steps, 'step3')
        self.assertFalse(workflow.has_more_steps('step4'))
        self.assertEqual(workflow._get_next_step(None), 'step1')
        self.assertEqual(workflow._get_next_step('step1'), 'step2')
        self.assertEqual(workflow._get_next_step('step2'), 'step4')
        self.assertRaises(CNODCError, workflow._get_next_step, 'step4')
        self.assertRaises(CNODCError, workflow._get_next_step, 'step3')
        self.assertEqual(workflow._get_step_info('step1'), {'name': 'step1', 'order': 1})
        self.assertEqual(workflow._get_step_info('step2'), {'name': 'step2', 'order': 2})
        self.assertEqual(workflow._get_step_info('step4'), {'name': 'step4', 'order': 10})
        self.assertRaises(CNODCError, workflow._get_step_info, 'step3')

    def test_queue_step(self):
        workflow = WorkflowController("test", {
            "processing_steps": {
                'step2': {
                    'name': 'step2',
                    'order': 2,
                    'priority': 15,
                },
                'step1': {
                    'name': 'step1',
                    'order': 1,
                },
                'step4': {
                    'name': 'step4',
                    'order': 10,
                    'priority': 'invalid',
                    'worker_metadata': {
                        'hello': 'world',
                    }
                }
            }
        })
        payload = BatchPayload(batch_uuid='12345', workflow_name='test', current_step=None, current_step_done=True)
        workflow._queue_step(payload, self.db)
        self.assertEqual(payload.current_step, 'step1')
        self.assertFalse(payload.current_step_done)
        queue_item = self.db.fetch_next_queue_item('step1')
        self.assertIsNotNone(queue_item)
        new_payload = WorkflowPayload.from_queue_item(queue_item)
        self.assertEqual(new_payload.batch_uuid, payload.batch_uuid)
        self.assertEqual(new_payload.workflow_name, payload.workflow_name)
        self.assertEqual(new_payload.current_step, payload.current_step)
        self.assertEqual(new_payload.current_step_done, payload.current_step_done)
        self.assertEqual(queue_item.priority, 0)


        payload = BatchPayload(batch_uuid='12345', workflow_name='test', current_step='step1', current_step_done=False)
        workflow._queue_step(payload, self.db)
        self.assertEqual(payload.current_step, 'step1')
        self.assertFalse(payload.current_step_done)
        queue_item = self.db.fetch_next_queue_item('step1')
        self.assertIsNotNone(queue_item)
        new_payload = WorkflowPayload.from_queue_item(queue_item)
        self.assertEqual(new_payload.batch_uuid, payload.batch_uuid)
        self.assertEqual(new_payload.workflow_name, payload.workflow_name)
        self.assertEqual(new_payload.current_step, payload.current_step)
        self.assertEqual(new_payload.current_step_done, payload.current_step_done)
        self.assertEqual(queue_item.priority, 0)

        payload = BatchPayload(batch_uuid='12345', workflow_name='test', current_step='step1', current_step_done=True)
        workflow._queue_step(payload, self.db)
        self.assertEqual(payload.current_step, 'step2')
        self.assertFalse(payload.current_step_done)
        queue_item = self.db.fetch_next_queue_item('step2')
        self.assertIsNotNone(queue_item)
        new_payload = WorkflowPayload.from_queue_item(queue_item)
        self.assertEqual(new_payload.batch_uuid, payload.batch_uuid)
        self.assertEqual(new_payload.workflow_name, payload.workflow_name)
        self.assertEqual(new_payload.current_step, payload.current_step)
        self.assertEqual(new_payload.current_step_done, payload.current_step_done)
        self.assertEqual(queue_item.priority, 15)

        payload = BatchPayload(batch_uuid='12345', workflow_name='test', current_step='step2', current_step_done=True)
        workflow._queue_step(payload, self.db)
        self.assertEqual(payload.current_step, 'step4')
        self.assertFalse(payload.current_step_done)
        queue_item = self.db.fetch_next_queue_item('step4')
        self.assertIsNotNone(queue_item)
        new_payload = WorkflowPayload.from_queue_item(queue_item)
        self.assertEqual(new_payload.batch_uuid, payload.batch_uuid)
        self.assertEqual(new_payload.workflow_name, payload.workflow_name)
        self.assertEqual(new_payload.current_step, payload.current_step)
        self.assertEqual(new_payload.current_step_done, payload.current_step_done)
        self.assertIn('hello', new_payload.metadata)
        self.assertEqual(new_payload.metadata['hello'], 'world')
        self.assertEqual(queue_item.priority, 0)

        payload = BatchPayload(batch_uuid='12345', workflow_name='test', current_step='step4', current_step_done=True)
        self.assertRaises(CNODCError, workflow._queue_step, payload, self.db)

    def test_queue_working_file(self):
        workflow = WorkflowController("test", {
            "processing_steps": {
                'step2': {
                    'name': 'step2',
                    'order': 2,
                    'priority': 15,
                },
                'step1': {
                    'name': 'step1',
                    'order': 1,
                },
                'step4': {
                    'name': 'step4',
                    'order': 10,
                    'priority': 'invalid',
                    'worker_metadata': {
                        'hello': 'world',
                    }
                }
            }
        })
        file = LocalHandle(self.temp_dir / "hello.txt")
        with open(file.path(), 'w') as h:
            h.write("hello")
        workflow._queue_working_file(
            working_file=file,
            metadata={
                'last-modified-time': '2015-01-01T00:00:02'
            },
            filename='hello.txt',
            with_gzip=False,
            unique_file_key=None,
            db=self.db
        )
        item = self.db.fetch_next_queue_item('step1')
        self.assertIsNotNone(item)
        self.assertIsInstance(item, NODBQueueItem)

        payload = WorkflowPayload.from_queue_item(item)
        self.assertIsInstance(payload, FilePayload)
        self.assertEqual(payload.file_info.file_path, str(self.temp_dir / 'hello.txt'))
        self.assertEqual(payload.file_info.filename, 'hello.txt')
        self.assertFalse(payload.file_info.is_gzipped)
        self.assertEqual(payload.file_info.last_modified_date, datetime.datetime(2015, 1, 1, 0, 0, 2))
        self.assertEqual(payload.workflow_name, 'test')
        self.assertEqual(payload.current_step, 'step1')
        self.assertFalse(payload.current_step_done)
        self.assertIsNotNone(item.unique_item_name)
        self.assertEqual(item.unique_item_name, hashlib.md5(str(self.temp_dir / 'hello.txt').encode('utf-8')).hexdigest())

    def test_queue_working_file_with_ufk(self):
        workflow = WorkflowController("test", {
            "processing_steps": {
                'step2': {
                    'name': 'step2',
                    'order': 2,
                    'priority': 15,
                },
                'step1': {
                    'name': 'step1',
                    'order': 1,
                },
                'step4': {
                    'name': 'step4',
                    'order': 10,
                    'priority': 'invalid',
                    'worker_metadata': {
                        'hello': 'world',
                    }
                }
            }
        })
        file = LocalHandle(self.temp_dir / "hello.txt")
        with open(file.path(), 'w') as h:
            h.write("hello")
        workflow._queue_working_file(
            working_file=file,
            metadata={
            },
            filename='hello.txt',
            with_gzip=False,
            unique_file_key='foobar',
            db=self.db
        )
        item = self.db.fetch_next_queue_item('step1')
        self.assertIsNotNone(item)
        self.assertIsInstance(item, NODBQueueItem)
        self.assertIsNotNone(item.unique_item_name)
        self.assertEqual(item.unique_item_name, hashlib.md5('foobar'.encode('utf-8')).hexdigest())
        payload = WorkflowPayload.from_queue_item(item)
        self.assertIsNotNone(payload.file_info.last_modified_date)

    def test_extend_default_metadata(self):
        workflow = WorkflowController("test", {
            'default_metadata': {
                'foo': 'bar',
                'over': 'write'
            }
        })
        metadata = {
            'over': 'not',
            'other': 'base',
        }
        workflow._extend_metadata(metadata)
        self.assertIn('foo', metadata)
        self.assertEqual(metadata['foo'], 'bar')
        self.assertEqual(metadata['over'], 'not')

    @injector.inject
    def test_validate_file(self, d: InjectableDict = None):
        workflow = WorkflowController("test", {
            'validation': 'tests.workflow.test_controller._fake_validation_called',
        })
        workflow._validate_file_upload(self.temp_dir / 'hello.txt', {'foo': 'bar'})
        self.assertEqual(d.data['local_path'], self.temp_dir / 'hello.txt')
        self.assertEqual(d.data['metadata'], {'foo': 'bar'})

    def test_upload_and_queue(self):
        (self.temp_dir / 'hello').mkdir()
        (self.temp_dir / 'hello2').mkdir()
        workflow = WorkflowController("test", {
            'working_target': {
                'directory': self.temp_dir / 'hello',
            },
            'accept_user_filename': True,
            'additional_targets': [
                {
                    'directory': self.temp_dir / 'hello2',
                    'gzip': True,
                },
            ],
            "processing_steps": {
                'step2': {
                    'name': 'step2',
                    'order': 2,
                    'priority': 15,
                },
                'step1': {
                    'name': 'step1',
                    'order': 1,
                },
                'step4': {
                    'name': 'step4',
                    'order': 10,
                    'priority': 'invalid',
                    'worker_metadata': {
                        'hello': 'world',
                    }
                }
            }
        })
        file = self.temp_dir / 'file.txt'
        with open(file, 'w') as h:
            h.write('foobar')
        post_hook_dict = {}
        def post_hook_me(*args, d: dict, **kwargs):
            d['yes'] = True
        workflow._upload_and_queue_file(
            local_path=file,
            metadata={
                'filename': 'world.txt',
                'last-modified-date': '2015-01-02T00:00:01',
            },
            post_hook=functools.partial(post_hook_me, d=post_hook_dict),
            db=self.db,
            unique_queue_id=None
        )
        expected1 = self.temp_dir / 'hello' / 'world.txt'
        self.assertTrue(expected1.exists())
        with open(expected1, 'r') as h:
            self.assertEqual(h.read(), 'foobar')
        expected2 = self.temp_dir / 'hello2' / 'world.txt.gz'
        self.assertTrue(expected2.exists())
        with gzip.open(expected2, 'r') as h:
            self.assertEqual(h.read(), b'foobar')
        self.assertTrue(post_hook_dict['yes'])
        item = self.db.fetch_next_queue_item('step1')
        self.assertIsNotNone(item)
        payload = WorkflowPayload.from_queue_item(item)
        self.assertIsInstance(payload, FilePayload)
        self.assertEqual(payload.file_info.file_path, str(expected1))

    def test_upload_and_queue_with_unique(self):
        (self.temp_dir / 'hello').mkdir()
        (self.temp_dir / 'hello2').mkdir()
        workflow = WorkflowController("test", {
            'working_target': {
                'directory': self.temp_dir / 'hello',
                'gzip': True,
            },
            'accept_user_filename': True,
            'additional_targets': [
                {
                    'directory': self.temp_dir / 'hello2',
                },
            ],
            "processing_steps": {
                'step2': {
                    'name': 'step2',
                    'order': 2,
                    'priority': 15,
                },
                'step1': {
                    'name': 'step1',
                    'order': 1,
                },
                'step4': {
                    'name': 'step4',
                    'order': 10,
                    'priority': 'invalid',
                    'worker_metadata': {
                        'hello': 'world',
                    }
                }
            }
        })
        file = self.temp_dir / 'file.txt'
        with open(file, 'w') as h:
            h.write('foobar')
        workflow._upload_and_queue_file(
            local_path=file,
            metadata={
                'filename': 'world.txt',
                'last-modified-date': '2015-01-02T00:00:01',
            },
            post_hook=None,
            db=self.db,
            unique_queue_id='12345'
        )
        expected1 = self.temp_dir / 'hello2' / 'world.txt'
        self.assertTrue(expected1.exists())
        with open(expected1, 'r') as h:
            self.assertEqual(h.read(), 'foobar')
        expected2 = self.temp_dir / 'hello' / 'world.txt.gz'
        self.assertTrue(expected2.exists())
        with gzip.open(expected2, 'r') as h:
            self.assertEqual(h.read(), b'foobar')
        item = self.db.fetch_next_queue_item('step1')
        self.assertIsNotNone(item)
        self.assertEqual(item.unique_item_name, hashlib.md5(b'12345').hexdigest())
        payload = WorkflowPayload.from_queue_item(item)
        self.assertIsInstance(payload, FilePayload)
        self.assertEqual(payload.file_info.file_path, str(expected2))

    @injector.inject
    def test_handle_incoming(self, d: InjectableDict = None):
        (self.temp_dir / 'hello').mkdir()
        (self.temp_dir / 'hello2').mkdir()
        workflow = WorkflowController("test", {
            'working_target': {
                'directory': self.temp_dir / 'hello',
                'gzip': True,
            },
            'accept_user_filename': True,
            'additional_targets': [
                {
                    'directory': self.temp_dir / 'hello2',
                },
            ],
            "processing_steps": {
                'step2': {
                    'name': 'step2',
                    'order': 2,
                    'priority': 15,
                },
                'step1': {
                    'name': 'step1',
                    'order': 1,
                },
                'step4': {
                    'name': 'step4',
                    'order': 10,
                    'priority': 'invalid',
                    'worker_metadata': {
                        'hello': 'world',
                    }
                }
            },
            'default_metadata': {
              'foo2': 'bar2',
            },
            'validation': 'tests.workflow.test_controller._fake_validation_called',
        })
        file = self.temp_dir / 'file.txt'
        with open(file, 'w') as h:
            h.write('foobar')
        workflow._handle_incoming_file(
            local_path=file,
            metadata={
                'filename': 'world.txt',
                'last-modified-date': '2015-01-02T00:00:01',
            },
            post_hook=None,
            db=self.db,
            unique_queue_id='12345'
        )
        expected1 = self.temp_dir / 'hello2' / 'world.txt'
        self.assertTrue(expected1.exists())
        with open(expected1, 'r') as h:
            self.assertEqual(h.read(), 'foobar')
        expected2 = self.temp_dir / 'hello' / 'world.txt.gz'
        self.assertTrue(expected2.exists())
        with gzip.open(expected2, 'r') as h:
            self.assertEqual(h.read(), b'foobar')
        item = self.db.fetch_next_queue_item('step1')
        self.assertIsNotNone(item)
        self.assertEqual(item.unique_item_name, hashlib.md5(b'12345').hexdigest())
        payload = WorkflowPayload.from_queue_item(item)
        self.assertIsInstance(payload, FilePayload)
        self.assertEqual(payload.file_info.file_path, str(expected2))
        self.assertEqual(d.data['local_path'], file)
        self.assertIn('foo2', d.data['metadata'])
        self.assertEqual(d.data['metadata']['foo2'], 'bar2')
        self.assertIn('filename', d.data['metadata'])
        self.assertEqual(d.data['metadata']['filename'], 'world.txt')

@injector.inject
def _fake_validation_called(local_path, metadata, d: InjectableDict=None):
    d.data['local_path'] = local_path
    d.data['metadata'] = metadata
