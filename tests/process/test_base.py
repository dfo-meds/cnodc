import threading
import unittest as ut

import yaml

from cnodc.process.base import _ThreadingHaltFlag, _NoHaltFlag, BaseController, SaveData
from cnodc.util import HaltInterrupt
from core import BaseTestCase


class TestHaltFlags(ut.TestCase):

    def test_threaded_flag(self):
        e = threading.Event()
        hf = _ThreadingHaltFlag(e)
        self.assertTrue(hf._should_continue())
        hf.breakpoint()
        e.set()
        self.assertFalse(hf._should_continue())
        self.assertRaises(HaltInterrupt, hf.breakpoint)

    def test_noop_flag(self):
        hf = _NoHaltFlag()
        self.assertTrue(hf._should_continue())
        hf.breakpoint()


class NoopController(BaseController):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processes = {}

    def _register_process(self,
                          process_name: str,
                          process_cls: str,
                          quota: int,
                          config: dict):
        self.processes[process_name] = (process_cls, quota, config)

    def _deregister_process(self, process_name: str):
        if process_name in self.processes:
            del self.processes[process_name]

    def _registered_process_names(self) -> list[str]:
        return list(self.processes.keys())


class TestBaseProcessController(BaseTestCase):

    def test_process_config_file(self):
        file = self.temp_dir / "test.yaml"
        with open(file, "w") as h:
            yaml.safe_dump({
                'process1': {
                    'class_name': 'cnodc.process.queue_worker.QueueWorker',
                    'config': {
                        'five': 5,
                    }
                },
                'process2': {
                    'class_name': 'cnodc.process.scheduled_task.ScheduledTask',
                    'count': 4,
                    'config': {
                        'hello': 'world',
                    }
                }
            }, h)
        nc = NoopController(
            log_name="test",
            halt_flag=_NoHaltFlag(),
            config_file=file
        )
        nc.reload_check()
        self.assertEqual(len(nc.processes), 2)
        self.assertIn('process1', nc.processes)
        self.assertIn('process2', nc.processes)
        self.assertEqual(nc.processes['process1'][0], 'cnodc.process.queue_worker.QueueWorker')
        self.assertEqual(nc.processes['process1'][1], 1)
        self.assertEqual(nc.processes['process1'][2], {"five": 5})
        self.assertEqual(nc.processes['process2'][0], 'cnodc.process.scheduled_task.ScheduledTask')
        self.assertEqual(nc.processes['process2'][1], 4)
        self.assertEqual(nc.processes['process2'][2], {"hello": "world"})

    def test_process_config_dir(self):
        with open(self.temp_dir / "process1.yaml", "w") as h:
            yaml.safe_dump({
                'process1': {
                    'class_name': 'cnodc.process.queue_worker.QueueWorker',
                    'config': {
                        'five': 5,
                    }
                },
            }, h)

        with open(self.temp_dir / 'process2.yaml', 'w') as h:
            yaml.safe_dump({
                'process2': {
                    'class_name': 'cnodc.process.scheduled_task.ScheduledTask',
                    'count': 4,
                    'config': {
                        'hello': 'world',
                    }
                }
            }, h)
        nc = NoopController(
            log_name="test",
            halt_flag=_NoHaltFlag(),
            config_file_dir=self.temp_dir,
        )
        nc.reload_check()
        self.assertEqual(len(nc.processes), 2)
        self.assertIn('process1', nc.processes)
        self.assertIn('process2', nc.processes)
        self.assertEqual(nc.processes['process1'][0], 'cnodc.process.queue_worker.QueueWorker')
        self.assertEqual(nc.processes['process1'][1], 1)
        self.assertEqual(nc.processes['process1'][2], {"five": 5})
        self.assertEqual(nc.processes['process2'][0], 'cnodc.process.scheduled_task.ScheduledTask')
        self.assertEqual(nc.processes['process2'][1], 4)
        self.assertEqual(nc.processes['process2'][2], {"hello": "world"})

    def test_process_config_reload(self):
        file = self.temp_dir / "test.yaml"
        procs = {
            'process1': {
                'class_name': 'cnodc.process.queue_worker.QueueWorker',
                'config': {
                    'five': 5,
                }
            },
            'process2': {
                'class_name': 'cnodc.process.scheduled_task.ScheduledTask',
                'count': 4,
                'config': {
                    'hello': 'world',
                }
            }
        }
        with open(file, "w") as h:
            yaml.safe_dump(procs, h)
        flag_file = self.temp_dir / 'flag'
        nc = NoopController(
            log_name="test",
            halt_flag=_NoHaltFlag(),
            config_file=file,
            flag_file=flag_file
        )
        nc.reload_check()
        self.assertFalse(flag_file.exists())
        self.assertEqual(len(nc.processes), 2)
        self.assertFalse(nc._check_reload())
        procs['process3'] = {
            'class_name': 'cnodc.process.scheduled_task.ScheduledTask',
            'config': {},
        }
        with open(file, "w") as h:
            yaml.safe_dump(procs, h)
        flag_file.touch()
        self.assertTrue(flag_file.exists())
        self.assertTrue(nc._check_reload())
        flag_file.touch()
        nc.reload_check()
        self.assertEqual(len(nc.processes), 3)


class TestSaveData(BaseTestCase):

    def test_save_data(self):
        file = self.temp_dir / 'file'
        sf = SaveData(file)
        self.assertNotIn('hello', sf)
        sf['hello'] = 'world'
        self.assertEqual(sf['hello'], 'world')
        self.assertFalse(file.exists())
        sf.save_file()
        self.assertTrue(file.exists())
        del sf
        sf2 = SaveData(file)
        self.assertIn('hello', sf2)
        self.assertEqual(sf2.get('hello'), 'world')
        self.assertNotIn('foo', sf2)
        self.assertEqual(sf2.get('foo', 'bar'), 'bar')
        sf2['foo'] = 'world'
        self.assertEqual(sf2.get('foo', 'bar'), 'world')



