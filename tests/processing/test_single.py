import threading

import yaml

from cnodc.processing.control.single import SingleProcessController
from cnodc.processing.control.base import BaseWorker
from cnodc.util import CNODCError
from helpers.base_test_case import BaseTestCase


class TestBaseProcessController(BaseTestCase):

    def test_signal_catching(self):
        file = self.temp_dir / "test.yaml"
        with open(file, "w") as h:
            yaml.safe_dump({
                'process1': {
                    'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                    'config': {
                        'five': 5,
                    }
                },
                'process2': {
                    'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                    'count': 4,
                    'config': {
                        'hello': 'world',
                    }
                }
            }, h)
        nc = SingleProcessController(
            process_name="foo",
            config_file=file
        )
        nc.reload_check()
        self.assertFalse(nc._halt_flag.is_set())
        self.assertEqual(nc._break_count, 0)
        with self.assertLogs('cnodc.single_process', 'INFO'):
            nc._handle_halt(1, "")
        self.assertTrue(nc._halt_flag.is_set())
        self.assertEqual(nc._break_count, 1)
        with self.assertLogs('cnodc.single_process', 'INFO'):
            nc._handle_halt(1, "")
        self.assertTrue(nc._halt_flag.is_set())
        self.assertEqual(nc._break_count, 2)
        with self.assertLogs('cnodc.single_process', 'CRITICAL'):
            with self.assertRaises(KeyboardInterrupt):
                nc._handle_halt(1, "")
        self.assertTrue(nc._halt_flag.is_set())
        self.assertEqual(nc._break_count, 3)

    def test_bad_process_file_error(self):
        file = self.temp_dir / "test.yaml"
        file.touch()
        nc = SingleProcessController(
            process_name="foo",
            config_file=file
        )
        with self.assertLogs("cnodc.single_process", "ERROR"):
            with self.assertRaises(CNODCError):
                nc.reload_check()

    def test_missing_process_directory(self):
        subdir = self.temp_dir / "subdir"
        nc = SingleProcessController(
            process_name="foo",
            config_file_dir=subdir
        )
        with self.assertLogs("cnodc.single_process", "ERROR"):
            with self.assertRaises(CNODCError):
                nc.reload_check()

    def test_bad_process_directory(self):
        file = self.temp_dir / "test.yaml"
        file.touch()
        nc = SingleProcessController(
            process_name="foo",
            config_file_dir=file
        )
        with self.assertLogs("cnodc.single_process", "ERROR"):
            with self.assertRaises(CNODCError):
                nc.reload_check()

    def test_bad_process_missing_file(self):
        file = self.temp_dir / "test.yaml"
        nc = SingleProcessController(
            process_name="foo",
            config_file=file
        )
        with self.assertRaises(CNODCError):
            with self.assertLogs("cnodc.single_process", "ERROR"):
                nc.reload_check()

    def test_bad_process_file(self):
        file = self.temp_dir / "test.yaml"
        nc = SingleProcessController(
            process_name="foo",
            config_file_dir=file
        )
        with self.assertRaises(CNODCError):
            with self.assertLogs("cnodc.single_process", "ERROR"):
                nc.reload_check()

    def test_bad_process_no_class(self):
        file = self.temp_dir / "test.yaml"
        with open(file, "w") as h:
            yaml.safe_dump({
                'process1': {
                    'config': {}
                },
                'good': {
                    'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                    'config': {}
                }
            }, h)
        nc = SingleProcessController(
            process_name="foo",
            config_file=file
        )
        with self.assertLogs('cnodc.single_process', 'ERROR'):
            nc.reload_check()
        self.assertEqual(len(nc._process_info), 1)
        self.assertIn('good', nc._process_info)
        self.assertNotIn('process1', nc._process_info)

    def test_bad_process_invalid_object(self):
        file = self.temp_dir / "test.yaml"
        with open(file, "w") as h:
            yaml.safe_dump({
                'process1': {
                    'class_name': 'cnodc.util.exceptions.CNODCError2',
                    'config': {}
                },
                'good': {
                    'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                    'config': {}
                }
            }, h)
        nc = SingleProcessController(
            process_name="foo",
            config_file=file
        )
        with self.assertLogs('cnodc.single_process', 'ERROR'):
            nc.reload_check()
        self.assertEqual(len(nc._process_info), 1)
        self.assertIn('good', nc._process_info)
        self.assertNotIn('process1', nc._process_info)

    def test_bad_process_no_run_method(self):
        file = self.temp_dir / "test.yaml"
        with open(file, "w") as h:
            yaml.safe_dump({
                'process1': {
                    'class_name': 'cnodc.util.exceptions.CNODCError',
                    'config': {}
                },
                'good': {
                    'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                    'config': {}
                }
            }, h)
        nc = SingleProcessController(
            process_name="foo",
            config_file=file
        )
        with self.assertLogs('cnodc.single_process', 'ERROR'):
            nc.reload_check()
        self.assertEqual(len(nc._process_info), 1)
        self.assertIn('good', nc._process_info)
        self.assertNotIn('process1', nc._process_info)

    def test_bad_process_config_warning(self):
        file = self.temp_dir / "test.yaml"
        with open(file, "w") as h:
            yaml.safe_dump({
                'process1': {
                    'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                    'config': 'foobar'
                },
                'good': {
                    'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                    'config': {}
                }
            }, h)
        nc = SingleProcessController(
            process_name="foo",
            config_file=file
        )
        with self.assertLogs('cnodc.single_process', 'WARNING'):
            nc.reload_check()
        self.assertEqual(len(nc._process_info), 2)
        self.assertIn('good', nc._process_info)
        self.assertIn('process1', nc._process_info)
        self.assertIsInstance(nc._process_info['process1']._config, dict)

    def test_bad_process_count_warning(self):
        file = self.temp_dir / "test.yaml"
        with open(file, "w") as h:
            yaml.safe_dump({
                'process1': {
                    'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                    'count': 'foobar'
                },
                'good': {
                    'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                    'config': {}
                }
            }, h)
        nc = SingleProcessController(
            process_name="foo",
            config_file=file
        )
        with self.assertLogs('cnodc.single_process', 'WARNING'):
            nc.reload_check()
        self.assertEqual(len(nc._process_info), 2)
        self.assertIn('good', nc._process_info)
        self.assertIn('process1', nc._process_info)
        self.assertEqual(nc._process_info['process1']._quota, 1)

    def test_process_config_file(self):
        file = self.temp_dir / "test.yaml"
        with open(file, "w") as h:
            yaml.safe_dump({
                'process1': {
                    'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                    'config': {
                        'five': 5,
                    }
                },
                'process2': {
                    'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                    'count': 4,
                    'config': {
                        'hello': 'world',
                    }
                }
            }, h)
        nc = SingleProcessController(
            process_name="foo",
            config_file=file
        )
        nc.reload_check()
        self.assertEqual(len(nc._process_info), 2)
        self.assertIn('process1', nc._process_info)
        self.assertIn('process2', nc._process_info)
        self.assertEqual(nc._process_info['process1']._worker_cls, 'cnodc.processing.workers.scheduled_task.ScheduledTask')
        self.assertEqual(nc._process_info['process1']._quota, 1)
        self.assertEqual(nc._process_info['process1']._config, {"five": 5})
        self.assertEqual(nc._process_info['process2']._worker_cls, 'cnodc.processing.workers.scheduled_task.ScheduledTask')
        self.assertEqual(nc._process_info['process2']._quota, 4)
        self.assertEqual(nc._process_info['process2']._config, {"hello": "world"})

    def test_process_config_dir(self):
        with open(self.temp_dir / "process1.yaml", "w") as h:
            yaml.safe_dump({
                'process1': {
                    'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                    'config': {
                        'five': 5,
                    }
                },
            }, h)
        subdir = self.temp_dir / 'subdir'
        subdir.mkdir()
        bad_ignore_file = self.temp_dir / 'file.txt'
        bad_ignore_file.touch()
        with open(subdir / 'process2.yaml', 'w') as h:
            yaml.safe_dump({
                'process2': {
                    'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                    'count': 4,
                    'config': {
                        'hello': 'world',
                    }
                }
            }, h)
        nc = SingleProcessController(
            process_name="foo",
            config_file_dir=self.temp_dir,
        )
        nc.reload_check()
        self.assertEqual(len(nc._process_info), 2)
        self.assertIn('process1', nc._process_info)
        self.assertIn('process2', nc._process_info)
        self.assertEqual(nc._process_info['process1']._worker_cls, 'cnodc.processing.workers.scheduled_task.ScheduledTask')
        self.assertEqual(nc._process_info['process1']._quota, 1)
        self.assertEqual(nc._process_info['process1']._config, {"five": 5})
        self.assertEqual(nc._process_info['process2']._worker_cls, 'cnodc.processing.workers.scheduled_task.ScheduledTask')
        self.assertEqual(nc._process_info['process2']._quota, 4)
        self.assertEqual(nc._process_info['process2']._config, {"hello": "world"})

    def test_process_config_reload(self):
        file = self.temp_dir / "test.yaml"
        procs = {
            'process1': {
                'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                'config': {
                    'five': 5,
                }
            },
            'process2': {
                'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                'count': 4,
                'config': {
                    'hello': 'world',
                }
            }
        }
        with open(file, "w") as h:
            yaml.safe_dump(procs, h)
        flag_file = self.temp_dir / 'flag'
        nc = SingleProcessController(
            process_name="foo",
            config_file=file,
            flag_file=flag_file
        )
        nc.reload_check()
        self.assertFalse(flag_file.exists())
        self.assertEqual(len(nc._process_info), 2)
        self.assertFalse(nc._check_reload())
        del procs['process1']
        procs['process2']['config']['hello'] = 'others'
        procs['process3'] = {
            'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
            'config': {},
        }
        with open(file, "w") as h:
            yaml.safe_dump(procs, h)
        flag_file.touch()
        self.assertTrue(flag_file.exists())
        self.assertTrue(nc._check_reload())
        flag_file.touch()
        nc.reload_check()
        self.assertEqual(len(nc._process_info), 3)
        self.assertIn('process2', nc._process_info)
        self.assertIn('process3', nc._process_info)
        self.assertIn('process1', nc._process_info)
        self.assertTrue(nc._process_info['process2'].is_activated())
        self.assertTrue(nc._process_info['process3'].is_activated())
        self.assertFalse(nc._process_info['process1'].is_activated())

    def test_process_config_reload_bad_config(self):
        file = self.temp_dir / "test.yaml"
        procs = {
            'process1': {
                'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                'config': {
                    'five': 5,
                }
            },
            'process2': {
                'class_name': 'cnodc.processing.workers.scheduled_task.ScheduledTask',
                'count': 4,
                'config': {
                    'hello': 'world',
                }
            }
        }
        with open(file, "w") as h:
            yaml.safe_dump(procs, h)
        flag_file = self.temp_dir / 'flag'
        nc = SingleProcessController(
            process_name="foo",
            config_file=file,
            flag_file=flag_file
        )
        nc.reload_check()
        self.assertFalse(flag_file.exists())
        self.assertEqual(len(nc._process_info), 2)
        self.assertFalse(nc._check_reload())
        with self.assertRaises(ValueError):
            nc._register_process('process2', 'cnodc.processing.workers.scheduled_task.ScheduledTask', 4, 'monkeybar')

    def test_run(self):
        file = self.temp_dir / "test.yaml"
        procs = {
            'process1': {
                'class_name': 'tests.processing.test_single.GoodTest',
            },
        }
        with open(file, "w") as h:
            yaml.safe_dump(procs, h)
        nc = SingleProcessController(
            process_name="process1",
            config_file=file
        )
        nc.reload_check()
        with self.assertLogs("cnodc.single_process", "DEBUG"):
            nc.start()
        self.assertTrue(nc._process._worker._did_run)
        self.assertTrue(nc._process._worker._on_start)
        self.assertTrue(nc._process._worker._on_exit)
        self.assertIsNone(nc._process._worker._exception)

    def test_exception_run(self):
        file = self.temp_dir / "test.yaml"
        procs = {
            'process1': {
                'class_name': 'tests.processing.test_single.BadTest',
            },
        }
        with open(file, "w") as h:
            yaml.safe_dump(procs, h)
        nc = SingleProcessController(
            process_name="process1",
            config_file=file
        )
        nc.reload_check()
        with self.assertLogs('cnodc.worker.test', 'ERROR'):
            nc.start()
        self.assertFalse(nc._process._worker._did_run)
        self.assertTrue(nc._process._worker._on_start)
        self.assertTrue(nc._process._worker._on_exit)
        self.assertIsInstance(nc._process._worker._exception, ValueError)

    def test_bad_process_name(self):
        file = self.temp_dir / "test.yaml"
        procs = {
            'process1': {
                'class_name': 'tests.processing.test_single.BadTest',
            },
        }
        with open(file, "w") as h:
            yaml.safe_dump(procs, h)
        nc = SingleProcessController(
            process_name="process2",
            config_file=file
        )
        nc.reload_check()
        with self.assertRaises(CNODCError):
            nc.start()
        self.assertIsNone(nc._process)

    def test_deactivate(self):
        file = self.temp_dir / "test.yaml"
        procs = {
            'process1': {
                'class_name': 'tests.processing.test_single.GoodTest',
                'count': 2,
            },
        }
        with open(file, "w") as h:
            yaml.safe_dump(procs, h)
        nc = SingleProcessController(
            process_name="process1",
            config_file=file
        )
        nc.reload_check()
        self.assertIn('process1', nc._process_info)
        self.assertTrue(nc._process_info['process1'].is_activated())
        nc.reap_and_sow()
        self.assertEqual(len(nc._process_info['process1']._active_processes), 2)
        nc.deactivate_all()
        self.assertFalse(nc._process_info['process1'].is_activated())
        for ap_key in nc._process_info['process1']._active_processes:
            ap = nc._process_info['process1']._active_processes[ap_key]
            self.assertTrue(ap._end_flag.is_set())
            self.assertFalse(ap._halt_flag.is_set())
        self.assertTrue(nc._process_info['process1'].is_active(True))
        self.assertFalse(nc._process_info['process1'].is_active())
        self.assertTrue(nc.wait_for_all(0.5))






class GoodTest(BaseWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="test",
            process_version="1_0",
            **kwargs)
        self._on_start = False
        self._did_run = False
        self._on_exit = False
        self._exception = None

    def on_start(self):
        self._on_start = True
        super().on_start()

    def _run(self):
        self._did_run = True

    def on_exit(self, ex: Exception = None):
        self._on_exit = True
        self._exception = ex
        super().on_exit()


class BadTest(GoodTest):

    def _run(self):
        raise ValueError('oh no')