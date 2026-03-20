import datetime
import ftplib
import logging
import threading

from autoinject import injector
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
import pyftpdlib.log as ftplogger
import pathlib

from zirconium import test_with_config

from cnodc.storage import StorageController
from cnodc.storage.base import StorageError
from cnodc.storage.ftp import FTPHandle, ftplib_error_wrap
import unittest as ut

from cnodc.util import HaltInterrupt
from helpers.base_test_case import BaseTestCase
from cnodc.util.halts import DummyHaltFlag

FTP_HOME = pathlib.Path(__file__).absolute().parent / 'test'


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


class TestFTPHandleNoServer(BaseTestCase):

    def test_not_running(self):
        handle = FTPHandle('ftp://localhost/test_remove.txt')
        with self.assertRaises(StorageError):
            handle.exists(True)

    def test_isdm_anon_ftp_connection(self):
        handle = FTPHandle('ftp://ftp.isdm.gc.ca/pub')
        self.assertTrue(handle.exists())

    def test_error_perm(self):
        with self.assertRaises(StorageError):
            wrap_and_raise(ftplib.error_perm("hello"))

    def test_error_temp(self):
        with self.assertRaises(StorageError):
            wrap_and_raise(ftplib.error_temp("hello"))

    def test_error_proto(self):
        with self.assertRaises(StorageError):
            wrap_and_raise(ftplib.error_proto("hello"))

    def test_error_reply(self):
        with self.assertRaises(StorageError):
            wrap_and_raise(ftplib.error_reply("hello"))

    def test_error_conn(self):
        with self.assertRaises(StorageError):
            wrap_and_raise(ConnectionError("oh no"))



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

    def test_properties(self):
        handle = FTPHandle("ftp://localhost/")
        self.assertFalse(handle.supports_tiering())
        self.assertFalse(handle.supports_metadata())

    def test_name_path(self):
        file = FTPHandle('ftp://localhost/hello/file.txt')
        self.assertEqual(file.name(), 'file.txt')
        self.assertEqual(file.path(), 'ftp://localhost/hello/file.txt')
        self.assertEqual(file.current_dir(), '/hello')

    def test_child_dir(self):
        handle = FTPHandle("ftp://localhost/")
        d = handle.child('hello', True)
        self.assertTrue(d.is_dir())
        self.assertEqual(d.name(), 'hello')
        self.assertEqual(d.path(), 'ftp://localhost/hello/')

    @injector.test_case
    @test_with_config(("storage", "servers", "ftp", "localhost"), {"username": "test", "password": "test"})
    def test_mkdir(self):
        handle = FTPHandle("ftp://localhost/hello_world/")
        real_dir = pathlib.Path(__file__).parent.absolute() / 'test' / 'hello_world'
        try:
            self.assertFalse(real_dir.exists())
            self.assertFalse(handle.exists())
            handle.mkdir()
            self.assertTrue(real_dir.exists())
            self.assertTrue(handle.exists())
        finally:
            if real_dir.exists():
                real_dir.rmdir()

    def test_file_dir(self):
        handle = FTPHandle('ftp://localhost/')
        d = handle.child('hello', False)
        self.assertFalse(d.is_dir())
        self.assertEqual(d.name(), 'hello')
        self.assertEqual(d.path(), 'ftp://localhost/hello')

    def test_get_handle(self):
        sc = StorageController()
        handle = sc.get_handle('ftp://localhost/')
        self.assertIsInstance(handle, FTPHandle)

    def test_remove_no_perms(self):
        fp = None
        try:
            fp = pathlib.Path(__file__).absolute().parent / 'test' / 'test_remove.txt'
            fp.touch()
            self.assertTrue(fp.exists())
            handle = FTPHandle('ftp://localhost/test_remove.txt')
            self.assertTrue(handle.exists())
            with self.assertRaises(StorageError):
                handle.remove()
            self.assertTrue(handle.exists())
            self.assertTrue(fp.exists())
        finally:
            fp.unlink(True)

    @injector.test_case
    @test_with_config(("storage", "servers", "ftp", "localhost"), {"username": "test", "password": "test"})
    def test_remove(self):
        fp = None
        try:
            fp = pathlib.Path(__file__).absolute().parent / 'test' / 'test_remove.txt'
            fp.touch()
            self.assertTrue(fp.exists())
            handle = FTPHandle('ftp://localhost/test_remove.txt')
            self.assertTrue(handle.exists())
            handle.remove()
            self.assertFalse(handle.exists())
            self.assertFalse(fp.exists())
        finally:
            fp.unlink(True)

    def test_read(self):
        handle = FTPHandle('ftp://localhost/test.txt')
        self.assertTrue(handle.exists())
        handle.download(self.temp_dir / "hello.txt")
        with open(self.temp_dir / "hello.txt") as h:
            self.assertEqual(h.read(), "foobar")

    def test_size(self):
        handle = FTPHandle('ftp://localhost/test.txt')
        self.assertTrue(handle.exists())
        self.assertEqual(6, handle.size())

    def test_read_halt(self):
        handle = FTPHandle('ftp://localhost/test.txt', halt_flag=DummyHaltFlag())
        handle._halt_flag.event.set()
        self.assertTrue(handle.exists())
        file = self.temp_dir / "hello2.txt"
        self.assertFalse(file.exists())
        with self.assertRaises(HaltInterrupt):
            handle.download(file, buffer_size=2)
        self.assertFalse(file.exists())

    def test_read_halt2(self):
        handle = FTPHandle('ftp://localhost/test.txt', halt_flag=DummyHaltFlag())
        handle._halt_flag.event.set()
        self.assertTrue(handle.exists())
        with self.assertRaises(HaltInterrupt):
            _ = [x for x in handle._read_chunks(2)]

    def test_remove_dir(self):
        handle = FTPHandle('ftp://localhost/subdir/')
        self.assertTrue(handle.exists())
        with self.assertRaises(StorageError):
            handle.remove()

    def test_write_no_access(self):
        f = self.temp_dir / "foobar.txt"
        fp = pathlib.Path(__file__).absolute().parent / 'test' / 'test_upload.txt'
        self.assertFalse(fp.exists())
        with open(f, "w") as h:
            h.write("hello world foobar")
        try:
            handle = FTPHandle('ftp://localhost/test_upload.txt')
            self.assertFalse(handle.exists())
            with self.assertRaises(StorageError):
                handle.upload(f)
        finally:
            fp.unlink(True)

    @injector.test_case
    @test_with_config(("storage", "servers", "ftp", "localhost"), {"username": "test", "password": "test"})
    def test_write(self):
        f = self.temp_dir / "foobar.txt"
        fp = pathlib.Path(__file__).absolute().parent / 'test' / 'test_upload.txt'
        self.assertFalse(fp.exists())
        with open(f, "wb") as h:
            h.write(b"hello\nworld\r\nfoobar")
        try:
            handle = FTPHandle('ftp://localhost/test_upload.txt')
            self.assertFalse(handle.exists())
            handle.upload(f)
            self.assertTrue(handle.exists())
            self.assertTrue(fp.exists())
            handle.download(self.temp_dir / 'download.txt')
            with open(fp, 'rb') as h:
                self.assertEqual(h.read(), b'hello\nworld\r\nfoobar')
            with open(self.temp_dir / 'download.txt', 'rb') as h:
                self.assertEqual(h.read(), b'hello\nworld\r\nfoobar')
        finally:
            fp.unlink(True)

    def test_dir(self):
        handle = FTPHandle("ftp://localhost/")
        self.assertTrue(handle.is_dir())
        self.assertTrue(handle.exists())
        self.assertIsNone(handle.size())
        self.assertIsNone(handle.modified_datetime())

    def test_good_file(self):
        good_file = FTPHandle('ftp://localhost/test.txt')
        self.assertTrue(good_file.exists())
        self.assertFalse(good_file.is_dir())
        self.assertEqual(6, good_file.size())
        self.assertIsNotNone(good_file.modified_datetime())

    @injector.test_case
    @test_with_config(("storage", "servers", "ftp", "localhost"), {"server_timezone": "America/Toronto"})
    def test_good_file2(self):
        good_file = FTPHandle('ftp://localhost/test.txt')
        lmt = good_file.modified_datetime()
        self.assertIsNotNone(lmt)
        self.assertIn(lmt.strftime("%z"), ("-0400", "-0500"))

    def test_bad_file(self):
        bad_file = FTPHandle('ftp://localhost/test3.txt')
        self.assertFalse(bad_file.exists())
        self.assertFalse(bad_file.is_dir())
        self.assertIsNone(bad_file.size())
        self.assertIsNone(bad_file.modified_datetime())

    def test_walk(self):
        handle = FTPHandle("ftp://localhost/")
        files = [x.name() for x in handle.walk(False)]
        self.assertIn('test.txt', files)
        self.assertIn('test4.txt', files)
        self.assertNotIn('test2.txt', files)
        self.assertNotIn('test5.txt', files)
        self.assertNotIn('test3.txt', files)
        self.assertNotIn('subdir', files)
        self.assertNotIn('subdir2', files)
        self.assertEqual(len(files), 2)

    def test_walk_recursive(self):
        handle = FTPHandle("ftp://localhost/")
        files = [x.path() for x in handle.walk(True)]
        self.assertNotIn('ftp://localhost/subdir', files)
        self.assertNotIn('ftp://localhost/subdir/subdir2', files)
        self.assertNotIn('ftp://localhost/test3.txt', files)
        self.assertIn('ftp://localhost/test.txt', files)
        self.assertIn('ftp://localhost/test4.txt', files)
        self.assertIn('ftp://localhost/subdir/test2.txt', files)
        self.assertIn('ftp://localhost/subdir/subdir2/test5.txt', files)
        self.assertEqual(len(files), 4)

    def test_walk_recursive_with_dirs(self):
        handle = FTPHandle("ftp://localhost/")
        with handle._connection() as ftp:
            files = [x for x, _ in handle._walk('/', False, True, ftp)]
            self.assertIn('/subdir', files)
            self.assertNotIn('/subdir/subdir2', files)

    def test_walk_not_dirs(self):
        handle = FTPHandle("ftp://localhost/test.txt")
        files = [x for x in handle.walk()]
        self.assertEqual(0, len(files))

    def test_support(self):
        self.assertTrue(FTPHandle.supports, 'ftp://localhost')
        self.assertTrue(FTPHandle.supports, 'ftp://localhost/')
        self.assertTrue(FTPHandle.supports, 'ftp://localhost/hello.txt')
        # note this is still explicit FTP/TLS
        self.assertTrue(FTPHandle.supports, 'ftps://localhost')
        self.assertTrue(FTPHandle.supports, 'ftpse://localhost')

    def test_build(self):
        handle = FTPHandle.build('ftp://localhost/subdir/hello.txt')
        self.assertIsInstance(handle, FTPHandle)
        self.assertEqual(handle.name(), 'hello.txt')
        self.assertEqual(handle.current_dir(), '/subdir')


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
    @test_with_config(("storage", "servers", "ftp", "localhost"), {"rfc3659_support": False})
    def test_dir(self):
        handle = FTPHandle("ftp://localhost/")
        self.assertTrue(handle.is_dir())
        self.assertTrue(handle.exists())
        self.assertIsNone(handle.size())
        self.assertIsNone(handle.modified_datetime())

    @injector.test_case
    @test_with_config(("storage", "servers", "ftp", "localhost"), {"rfc3659_support": False})
    def test_good_file(self):
        good_file = FTPHandle('ftp://localhost/test.txt')
        self.assertTrue(good_file.exists())
        self.assertFalse(good_file.is_dir())
        self.assertIsNone(good_file.size())
        self.assertIsNone(good_file.modified_datetime())

    @injector.test_case
    @test_with_config(("storage", "servers", "ftp", "localhost"), {"rfc3659_support": False})
    def test_bad_file(self):
        bad_file = FTPHandle('ftp://localhost/test3.txt')
        self.assertFalse(bad_file.exists())
        self.assertFalse(bad_file.is_dir())
        self.assertIsNone(bad_file.size())
        self.assertIsNone(bad_file.modified_datetime())

    @injector.test_case
    @test_with_config(("storage", "servers", "ftp", "localhost"), {"rfc3659_support": False})
    def test_walk(self):
        handle = FTPHandle("ftp://localhost/")
        files = [x.name() for x in handle.walk(False)]
        self.assertIn('test.txt', files)
        self.assertIn('test4.txt', files)
        self.assertNotIn('test2.txt', files)
        self.assertNotIn('test5.txt', files)
        self.assertNotIn('test3.txt', files)
        self.assertNotIn('subdir', files)
        self.assertNotIn('subdir2', files)
        self.assertEqual(len(files), 2)

    @injector.test_case
    @test_with_config(("storage", "servers", "ftp", "localhost"), {"rfc3659_support": False})
    def test_walk_recursive(self):
        handle = FTPHandle("ftp://localhost/")
        files = [x.path() for x in handle.walk(True)]
        self.assertNotIn('ftp://localhost/subdir', files)
        self.assertNotIn('ftp://localhost/subdir/subdir2', files)
        self.assertNotIn('ftp://localhost/test3.txt', files)
        self.assertIn('ftp://localhost/test.txt', files)
        self.assertIn('ftp://localhost/test4.txt', files)
        self.assertIn('ftp://localhost/subdir/test2.txt', files)
        self.assertIn('ftp://localhost/subdir/subdir2/test5.txt', files)
        self.assertEqual(len(files), 4)
