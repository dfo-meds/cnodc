import datetime
import logging
import threading

from autoinject import injector
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
import pyftpdlib.log as ftplogger
import pathlib

from zirconium import test_with_config

from medsutil.storage.interface import StorageError, PathType
from medsutil.storage.ftp import FTPHandle, ftplib_error_wrap
import unittest as ut

from medsutil.exceptions import HaltInterrupt
from tests.helpers.base_test_case import BaseTestCase, skip_long_test
from medsutil.halts import DummyHaltFlag

FTP_HOME = pathlib.Path(__file__).absolute().resolve().parent.parent.parent / 'test_data' / 'ftp_root'


@ftplib_error_wrap
def wrap_and_raise(ex):
    raise ex

class FTPServerThread(threading.Thread):

    def __init__(self):
        super().__init__(daemon=True)
        self._server = None
        self._since = None

    def run(self):
        ftplogger.config_logging(logging.WARNING)
        authorizer = DummyAuthorizer()
        authorizer.add_user('test', 'test', FTP_HOME, perm='elradfmwMT')
        authorizer.add_anonymous(FTP_HOME, perm='elr')
        handler = FTPHandler
        handler.authorizer = authorizer
        self._server = FTPServer(('localhost', 21), handler)
        self._server.max_cons = 10
        self._server.max_cons_per_ip = 10
        self._since = datetime.datetime.now()
        self._server.serve_forever()

    def is_booting(self):
        if not self.is_alive():
            return False
        if self._server is None:
            return True
        return (datetime.datetime.now() - self._since).total_seconds() < 0.05

    def halt(self):
        if self.is_alive():
            if self._server is not None:
                self._server.close_all()
                self._server = None
            self.join()


@skip_long_test
class TestFTPHandle(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._ftp_server = FTPServerThread()
        cls._ftp_server.start()
        while cls._ftp_server.is_booting():
            continue

    @classmethod
    def tearDownClass(cls):
        cls._ftp_server.halt()
        cls._ftp_server = None
        super().tearDownClass()

    @injector.test_case
    @test_with_config(("storage", "ftp", "localhost"), {"username": "test", "password": "test"})
    def test_mkdir(self):
        with FTPHandle("ftp://localhost/hello_world/") as handle:
            with self.temp_data_dir('ftp_root/hello_world') as real_dir:
                self.assertFalse(real_dir.exists())
                self.assertFalse(handle.exists())
                handle.mkdir()
                self.assertTrue(real_dir.exists())
                self.assertTrue(handle.exists())

    def test_remove_no_perms(self):
        with self.temp_data_file('ftp_root/test_remove.txt') as fp:
            fp.touch()
            self.assertTrue(fp.exists())
            with FTPHandle('ftp://localhost/test_remove.txt') as handle:
                self.assertTrue(handle.exists())
                with self.assertRaises(StorageError):
                    handle.remove()
                self.assertTrue(handle.exists())
                self.assertTrue(fp.exists())

    @injector.test_case
    @test_with_config(("storage", "ftp", "localhost"), {"username": "test", "password": "test"})
    def test_remove(self):
        with self.temp_data_file('ftp_root/test_remove.txt') as fp:
            fp.touch()
            self.assertTrue(fp.exists())
            with FTPHandle('ftp://localhost/test_remove.txt') as handle:
                self.assertTrue(handle.exists())
                self.assertFalse(handle.is_dir())
                handle.remove()
                self.assertFalse(handle.exists())
                self.assertFalse(fp.exists())

    def test_read(self):
        with FTPHandle('ftp://localhost/test.txt') as handle:
            self.assertTrue(handle.exists())
            handle.download(self.temp_dir / "hello.txt")
            with open(self.temp_dir / "hello.txt") as h:
                self.assertEqual(h.read(), "foobar")

    def test_size(self):
        with FTPHandle('ftp://localhost/test.txt') as handle:
            self.assertTrue(handle.exists())
            self.assertEqual(6, handle.size())

    def test_read_halt(self):
        with FTPHandle('ftp://localhost/test.txt', halt_flag=DummyHaltFlag()) as handle:
            handle._halt_flag.event.set()
            self.assertTrue(handle.exists())
            file = self.temp_dir / "hello2.txt"
            self.assertFalse(file.exists())
            with self.assertRaises(HaltInterrupt):
                handle.download(file, buffer_size=2)
            self.assertFalse(file.exists())

    def test_read_halt2(self):
        with FTPHandle('ftp://localhost/test.txt', halt_flag=DummyHaltFlag()) as handle:
            handle._halt_flag.event.set()
            self.assertTrue(handle.exists())
            with self.assertRaises(HaltInterrupt):
                _ = [x for x in handle._streaming_read(2)]

    def test_remove_dir(self):
        with FTPHandle('ftp://localhost/subdir/') as handle:
            self.assertTrue(handle.exists())
            with self.assertRaises(StorageError):
                handle.remove()

    def test_write_no_access(self):
        f = self.temp_dir / "foobar.txt"
        with open(f, "w") as h:
            h.write("hello world foobar")
        with self.temp_data_file('ftp_root/test_upload.txt') as fp:
            self.assertFalse(fp.exists())
            with FTPHandle('ftp://localhost/test_upload.txt') as handle:
                self.assertFalse(handle.exists())
                with self.assertRaises(StorageError):
                    handle.upload(f)

    @injector.test_case
    @test_with_config(("storage", "ftp", "localhost"), {"username": "test", "password": "test"})
    def test_write(self):
        f = self.temp_dir / "foobar.txt"
        with open(f, "wb") as h:
            h.write(b"hello\nworld\r\nfoobar")
        with self.temp_data_file('ftp_root/test_upload.txt') as fp:
            self.assertFalse(fp.exists())
            with FTPHandle('ftp://localhost/test_upload.txt') as handle:
                self.assertFalse(handle.exists())
                handle.upload(f)
                self.assertTrue(fp.exists())
                self.assertTrue(handle.exists())
                handle.download(self.temp_dir / 'download.txt')
                with open(fp, 'rb') as h:
                    self.assertEqual(h.read(), b'hello\nworld\r\nfoobar')
                with open(self.temp_dir / 'download.txt', 'rb') as h:
                    self.assertEqual(h.read(), b'hello\nworld\r\nfoobar')

    def test_dir(self):
        with FTPHandle("ftp://localhost/") as handle:
            self.assertTrue(handle.is_dir())
            self.assertTrue(handle.exists())
            self.assertIsNone(handle.size())
            self.assertIsNone(handle.modified_datetime())

    def test_good_file(self):
        with FTPHandle('ftp://localhost/test.txt') as good_file:
            self.assertTrue(good_file.exists())
            self.assertFalse(good_file.is_dir())
            self.assertEqual(6, good_file.size())
            self.assertIsNotNone(good_file.modified_datetime())

    @injector.test_case
    @test_with_config(("storage", "ftp", "localhost"), {"server_timezone": "Etc/UTC"})
    def test_good_file2(self):
        with FTPHandle('ftp://localhost/test.txt') as good_file:
            lmt = good_file.modified_datetime()
            self.assertIsNotNone(lmt)
            self.assertEqual(lmt.strftime('%z'), '+0000')

    def test_bad_file(self):
        with FTPHandle('ftp://localhost/test3.txt') as bad_file:
            self.assertFalse(bad_file.exists())
            self.assertFalse(bad_file.is_dir())
            self.assertIsNone(bad_file.size())
            self.assertIsNone(bad_file.modified_datetime())

    def test_walk(self):
        with FTPHandle("ftp://localhost/") as handle:
            files = [x.name for x in handle.iterdir(False, path_types=PathType.FILE)]
            self.assertIn('test.txt', files)
            self.assertIn('test4.txt', files)
            self.assertNotIn('test2.txt', files)
            self.assertNotIn('test5.txt', files)
            self.assertNotIn('test3.txt', files)
            self.assertNotIn('subdir', files)
            self.assertNotIn('subdir2', files)
            self.assertEqual(len(files), 2)

    def test_walk_recursive(self):
        with FTPHandle("ftp://localhost/") as handle:
            files = [x.path() for x in handle.iterdir(True, path_types=PathType.FILE)]
            self.assertNotIn('ftp://localhost/subdir/', files)
            self.assertNotIn('ftp://localhost/subdir/subdir2/', files)
            self.assertNotIn('ftp://localhost/test3.txt', files)
            self.assertIn('ftp://localhost/test.txt', files)
            self.assertIn('ftp://localhost/test4.txt', files)
            self.assertIn('ftp://localhost/subdir/test2.txt', files)
            self.assertIn('ftp://localhost/subdir/subdir2/test5.txt', files)
            self.assertEqual(len(files), 4)

    def test_walk_not_recursive_with_dirs(self):
        with FTPHandle("ftp://localhost/") as handle:
            files = [x.path() for x in handle.iterdir(False)]
            self.assertIn('ftp://localhost/subdir/', files)
            self.assertNotIn('ftp://localhost/subdir/subdir2/', files)

    def test_walk_not_dir(self):
        with FTPHandle("ftp://localhost/test.txt") as handle:
            files = [x.path() for x in handle.iterdir(False)]
            self.assertEqual(0, len(files))

    @injector.test_case
    def test_walk_and_do_stuff(self):
        with FTPHandle("ftp://localhost/") as handle:
            items_seen = 0
            for file in handle.iterdir(True, path_types=PathType.FILE):
                self.assertTrue(file.exists())
                self.assertIsNotNone(file.size())
                items_seen += 1
            self.assertEqual(items_seen, 4)

@skip_long_test
class TestOlderFTPHandle(ut.TestCase):
    """ These tests test the backup behaviour when MLST and related functions are not available. Note that this makes
        the last modified date and filesize unavailable.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._ftp_server = FTPServerThread()
        cls._ftp_server.start()
        while cls._ftp_server.is_booting():
            continue

    @classmethod
    def tearDownClass(cls):
        cls._ftp_server.halt()
        cls._ftp_server = None
        super().tearDownClass()

    @injector.test_case
    @test_with_config(("storage", "ftp", "localhost"), {"rfc3659_support": False})
    def test_dir(self):
        with FTPHandle("ftp://localhost/") as handle:
            self.assertTrue(handle.is_dir())
            self.assertTrue(handle.exists())
            self.assertIsNone(handle.size())
            self.assertIsNone(handle.modified_datetime())

    @injector.test_case
    @test_with_config(("storage", "ftp", "localhost"), {"rfc3659_support": False})
    def test_good_file(self):
        with FTPHandle('ftp://localhost/test.txt') as good_file:
            self.assertTrue(good_file.exists())
            self.assertFalse(good_file.is_dir())
            self.assertIsNone(good_file.size())
            self.assertIsNone(good_file.modified_datetime())

    @injector.test_case
    @test_with_config(("storage", "ftp", "localhost"), {"rfc3659_support": False})
    def test_bad_file(self):
        with FTPHandle('ftp://localhost/test3.txt') as bad_file:
            self.assertFalse(bad_file.exists())
            self.assertFalse(bad_file.is_dir())
            self.assertIsNone(bad_file.size())
            self.assertIsNone(bad_file.modified_datetime())

    @injector.test_case
    @test_with_config(("storage", "ftp", "localhost"), {"rfc3659_support": False})
    def test_walk(self):
        with FTPHandle("ftp://localhost/") as handle:
            files = [x.name for x in handle.iterdir(False, path_types=PathType.FILE)]
            self.assertIn('test.txt', files)
            self.assertIn('test4.txt', files)
            self.assertNotIn('test2.txt', files)
            self.assertNotIn('test5.txt', files)
            self.assertNotIn('test3.txt', files)
            self.assertNotIn('subdir', files)
            self.assertNotIn('subdir2', files)
            self.assertEqual(len(files), 2)

    @injector.test_case
    @test_with_config(("storage", "ftp", "localhost"), {"rfc3659_support": False})
    def test_walk_recursive(self):
        with FTPHandle("ftp://localhost/") as handle:
            files = [x.path() for x in handle.iterdir(True, path_types=PathType.FILE)]
            self.assertNotIn('ftp://localhost/subdir/', files)
            self.assertNotIn('ftp://localhost/subdir/subdir2/', files)
            self.assertNotIn('ftp://localhost/test3.txt', files)
            self.assertIn('ftp://localhost/test.txt', files)
            self.assertIn('ftp://localhost/test4.txt', files)
            self.assertIn('ftp://localhost/subdir/test2.txt', files)
            self.assertIn('ftp://localhost/subdir/subdir2/test5.txt', files)
            self.assertEqual(len(files), 4)

    @injector.test_case
    @test_with_config(("storage", "ftp", "localhost"), {"rfc3659_support": False})
    def test_walk_and_do_stuff(self):
        with FTPHandle("ftp://localhost/") as handle:
            items_seen = 0
            for file in handle.iterdir(True, path_types=PathType.FILE):
                self.assertTrue(file.exists())
                self.assertTrue(file.is_file())
                items_seen += 1
            self.assertEqual(items_seen, 4)


class TestMEDSServer(ut.TestCase):

    def test_connectivity(self):
        with FTPHandle("ftp://ftp.isdm.gc.ca/pub/requests/") as handle:
            self.assertTrue(handle.exists())

