import ftplib

from medsutil.storage import StorageController
from medsutil.storage.interface import StorageError
from medsutil.storage.ftp import FTPHandle, ftplib_error_wrap

from tests.helpers.base_test_case import BaseTestCase, skip_long_test

@ftplib_error_wrap
def wrap_and_raise(ex):
    raise ex

class TestFTPHandleNoServer(BaseTestCase):

    @skip_long_test
    def test_not_running(self):
        handle = FTPHandle('ftp://localhost/test_remove.txt')
        with self.assertRaises(StorageError):
            handle.exists()

    @skip_long_test
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

    def test_properties(self):
        handle = FTPHandle("ftp://localhost/")
        self.assertFalse(handle.supports_tiering())
        self.assertFalse(handle.supports_metadata())

    def test_name_path(self):
        file = FTPHandle('ftp://localhost/hello/file.txt')
        self.assertEqual(file.name, 'file.txt')
        self.assertEqual(file.path(), 'ftp://localhost/hello/file.txt')
        self.assertEqual(file.current_dir(), '/hello')

    def test_child_dir(self):
        handle = FTPHandle("ftp://localhost/")
        d = handle.child('hello', True)
        self.assertTrue(d.is_dir())
        self.assertEqual(d.name, 'hello')
        self.assertEqual(d.path(), 'ftp://localhost/hello/')

    def test_file_dir(self):
        handle = FTPHandle('ftp://localhost/')
        d = handle.child('hello', False)
        self.assertFalse(d.is_dir())
        self.assertEqual(d.name, 'hello')
        self.assertEqual(d.path(), 'ftp://localhost/hello')

    def test_get_handle(self):
        sc = StorageController()
        handle = sc.get_filepath('ftp://localhost/')
        self.assertIsInstance(handle, FTPHandle)

    def test_support(self):
        for file in [
            'ftp://localhost',
            'ftp://localhost/',
            'ftp://localhost/hello.txt',
            'ftps://localhost',
            'ftpse://localhost',
        ]:
            with self.subTest(supported_file=file):
                self.assertTrue(FTPHandle.supports(file))

    def test_build(self):
        handle = FTPHandle.build('ftp://localhost/subdir/hello.txt')
        self.assertIsInstance(handle, FTPHandle)
        self.assertEqual(handle.name, 'hello.txt')
        self.assertEqual(handle.current_dir(), '/subdir')
