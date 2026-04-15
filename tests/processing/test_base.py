import threading

from pipeman_service.controller import BaseProcess
from pipeman.processing.base_worker import BaseWorker, SaveData
from medsutil.exceptions import HaltInterrupt
from medsutil.halts import HaltFlag
from tests.helpers.base_test_case import BaseTestCase


class TestBaseProcess(BaseTestCase):

    def test_shutdown_flag(self):
        hf = threading.Event()
        ef = threading.Event()
        p = BaseProcess(
            'test',
            2,
            '',
            '',
            hf,
            ef,
            '',
            ''
        )
        self.assertEqual(p._process_name, 'test')
        self.assertEqual(p._process_idx, 2)
        self.assertFalse(ef.is_set())
        self.assertFalse(hf.is_set())
        p.shutdown()
        self.assertFalse(hf.is_set())
        self.assertTrue(ef.is_set())

class TestSaveData(BaseTestCase):

    def test_bad_save_file(self):
        file = self.temp_dir / 'file'
        file.mkdir()
        sf = SaveData(file)
        with self.assertLogs('cnodc.save_file', 'ERROR') as h:
            sf.load_file()
        self.assertNotIn('hello', sf)
        sf['hello'] = 'world'
        self.assertEqual(sf['hello'], 'world')
        with self.assertLogs('cnodc.save_file', 'ERROR'):
            sf.save_file()

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


class TestBaseWorker(BaseTestCase):

    def test_save_file(self):
        worker = BaseWorker('foo', 'bar', 'foobar', self._halt_flag, self._halt_flag, {
            'save_file': str(self.temp_dir / 'test.txt')
        })
        worker.save_data['foo'] = 'bar'
        self.assertEqual(worker.save_data['foo'], 'bar')
        worker.on_exit()
        del worker
        worker2 = BaseWorker('foo', 'bar', 'foobar', self._halt_flag, self._halt_flag, {
            'save_file': str(self.temp_dir / 'test.txt')
        })
        self.assertEqual(worker2.save_data['foo'], 'bar')

    def test_empty_save_file(self):
        worker = BaseWorker('foo', 'bar', 'foobar', self._halt_flag, self._halt_flag, {})
        worker.on_start()
        worker.save_data['foo'] = 'bar'
        self.assertEqual(worker.save_data['foo'], 'bar')
        worker.on_exit()
        del worker
        worker2 = BaseWorker('foo', 'bar', 'foobar', self._halt_flag, self._halt_flag, {
            'save_file': str(self.temp_dir / 'test.txt')
        })
        self.assertIsNone(worker2.save_data.get('foo', None))

    def test_temp_dir_cleanup(self):
        worker = BaseWorker('foo', 'bar', 'foobar', self._halt_flag, self._halt_flag, {})
        td = worker.temp_dir()
        self.assertTrue(td.exists())
        file = td / 'file.txt'
        file.touch()
        self.assertTrue(file.exists())
        worker.after_cycle()
        self.assertFalse(file.exists())
        self.assertFalse(td.exists())
        td2 = worker.temp_dir()
        self.assertNotEqual(td2, td)

    def test_worker_config(self):
        worker = BaseWorker('foo', 'bar', 'foobar', self._halt_flag, self._halt_flag, {
            'foo': 'bar',
            'six': 'seven'
        })
        worker.set_defaults({
            'foo': 'hello',
            'world': 'what',
        })
        self.assertEqual(worker.get_config('foo'), 'bar')
        self.assertEqual(worker.get_config('six'), 'seven')
        self.assertEqual(worker.get_config('world'), 'what')
        self.assertIsNone(worker.get_config('no'))

    def test_flags(self):
        flag1 = threading.Event()
        flag2 = threading.Event()
        worker = BaseWorker(
            'foo',
            'bar',
            'foobar',
            HaltFlag(flag1),
            HaltFlag(flag2))
        with self.subTest("no flags set"):
            self.assertTrue(worker.continue_loop())
            try:
                worker.breakpoint()
            except HaltInterrupt:
                self.assertFalse(True, msg='Halt interrupt raised when it should not be')
        with self.subTest("halt flag set"):
            flag1.set()
            self.assertFalse(worker.continue_loop())
            with self.assertRaises(HaltInterrupt):
                worker.breakpoint()
        with self.subTest("both flags set"):
            flag2.set()
            self.assertFalse(worker.continue_loop())
            with self.assertRaises(HaltInterrupt):
                worker.breakpoint()
        with self.subTest("end flag set"):
            flag1.clear()
            self.assertFalse(worker.continue_loop())
            try:
                worker.breakpoint()
            except HaltInterrupt:
                self.assertFalse(True, msg='Halt interrupt raised when it should not be')
