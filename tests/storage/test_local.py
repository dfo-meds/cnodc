import os
import pathlib

from cnodc.storage import StorageController
from cnodc.storage.base import StorageError, local_file_error_wrap
from cnodc.util import HaltInterrupt
from helpers.base_test_case import BaseTestCase
from cnodc.util.halts import DummyHaltFlag
from cnodc.storage.local import LocalHandle


@local_file_error_wrap
def wrap_and_raise(ex):
    raise ex


class TestLocalHandle(BaseTestCase):

    def test_not_a_dir(self):
        with self.assertRaises(StorageError):
            wrap_and_raise(NotADirectoryError("oh no"))

    def test_file_not_found(self):
        with self.assertRaises(StorageError):
            wrap_and_raise(FileNotFoundError("oh no"))

    def test_is_a_dir(self):
        with self.assertRaises(StorageError):
            wrap_and_raise(IsADirectoryError("oh no"))

    def test_perm_error(self):
        with self.assertRaises(StorageError):
            wrap_and_raise(PermissionError("oh no"))

    def test_other_os(self):
        with self.assertRaises(StorageError):
            wrap_and_raise(OSError("oh no"))

    def test_properties(self):
        handle = LocalHandle(self.temp_dir)
        self.assertFalse(handle.supports_tiering())
        self.assertFalse(handle.supports_metadata())

    def test_get_handle(self):
        sc = StorageController()
        self.assertIsInstance(sc.get_handle(pathlib.Path(self.temp_dir)), LocalHandle)
        self.assertIsInstance(sc.get_handle("C:/my/cnodc"), LocalHandle)
        self.assertIsInstance(sc.get_handle("file://C:/my/cnodc"), LocalHandle)
        self.assertIsInstance(sc.get_handle("/foo/bar"), LocalHandle)

    def test_exists(self):
        handle = LocalHandle(self.temp_dir)
        self.assertTrue(handle.exists())
        local_file = self.temp_dir / 'no.txt'
        handle2 = LocalHandle(local_file)
        self.assertFalse(handle2.exists())
        with open(local_file, "w") as h:
            h.write(' ')
        handle2.clear_cache()
        self.assertTrue(handle2.exists())

    def test_is_dir(self):
        handle = LocalHandle(self.temp_dir)
        self.assertTrue(handle.is_dir())
        file = LocalHandle(self.temp_dir / 'file.txt')
        with open(self.temp_dir / 'file.txt', 'w') as h:
            h.write(' ')
        self.assertFalse(file.is_dir())

    def test_name_path(self):
        file = LocalHandle(self.temp_dir / 'file.txt')
        with open(self.temp_dir / 'file.txt', 'w') as h:
            h.write(' ')
        self.assertEqual(file.name(), 'file.txt')
        self.assertEqual(file.path(), str(self.temp_dir / 'file.txt'))

    def test_can_read_file_data(self):
        handle = LocalHandle(self.temp_dir)
        self.assertIsNotNone(handle.modified_datetime())
        self.assertIsNotNone(handle.size())

    def test_child(self):
        handle = LocalHandle(self.temp_dir)
        file = handle.child('file.txt')
        self.assertFalse(file.is_dir())
        self.assertEqual(file.name(), 'file.txt')
        self.assertEqual(file.path(), str(self.temp_dir / 'file.txt'))

    def test_child_dir(self):
        handle = LocalHandle(self.temp_dir)
        file = handle.subdir('foo')
        self.assertTrue(file.is_dir())

    def test_default_entries(self):
        handle = LocalHandle(self.temp_dir)
        self.assertEqual({}, handle.get_metadata())
        self.assertIsNone(handle.get_tier())

    def test_file_size(self):
        handle = LocalHandle(self.temp_dir / 'file.txt')
        with open(self.temp_dir / 'file.txt', 'w') as h:
            h.write('12345')
        self.assertEqual(handle.size(), 5)

    def test_walk(self):
        test_dirs = ['a', 'b', 'e', 'f', 'z']
        test_subdirs = ['t', 'y', 'g']
        subdirs = []
        dirs = []
        for a in test_dirs:
            file = self.temp_dir / f'{a}.txt'
            file.touch()
            dirs.append(str(file))
        for d in test_dirs:
            dir_ = self.temp_dir / d
            dir_.mkdir()
            file = dir_ / 'file.txt'
            file.touch()
            subdirs.append(str(file))
            for sd in test_subdirs:
                sd_path = dir_ / sd
                sd_path.mkdir()
                file = dir_ / 'file.txt'
                file.touch()
                subdirs.append(str(file))
        handle = LocalHandle(self.temp_dir)
        results = [x.path() for x in handle.walk(True)]
        for sd in subdirs:
            with self.subTest(path=sd):
                self.assertIn(sd, results)
        results = [x.path() for x in handle.walk(False)]
        for sd in subdirs:
            with self.subTest(path=sd):
                if sd in dirs:
                    self.assertIn(sd, results)
                else:
                    self.assertNotIn(sd, results)

    def test_remove(self):
        fp = self.temp_dir / "file.txt"
        with open(fp, "w") as h:
            h.write("b")
        self.assertTrue(fp.exists())
        handle = LocalHandle(fp)
        self.assertTrue(handle.exists())
        handle.remove()
        self.assertFalse(handle.exists())
        self.assertFalse(fp.exists())

    def test_upload(self):
        fp = self.temp_dir / "file.txt"
        with open(fp, "w") as h:
            h.write("hello")
        upload = self.temp_dir / 'upload.txt'
        handle = LocalHandle(upload)
        self.assertFalse(handle.exists())
        handle.upload(fp)
        self.assertTrue(handle.exists())
        with open(upload, "r") as h:
            self.assertEqual(h.read(), "hello")

    def test_no_overwrite(self):
        fp = self.temp_dir / "file.txt"
        with open(fp, "w") as h:
            h.write("hello")
        upload = self.temp_dir / 'file.txt'
        handle = LocalHandle(upload)
        self.assertTrue(handle.exists())
        with self.assertRaises(StorageError):
            handle.upload(fp)

    def test_overwrite(self):
        with open(self.temp_dir / "file.txt", "w") as h:
            h.write("hello")
        with open(self.temp_dir / "file2.txt", "w") as h:
            h.write("world")
        handle = LocalHandle(self.temp_dir / "file.txt")
        self.assertTrue(handle.exists())
        handle.upload(self.temp_dir / "file2.txt", True)
        with open(self.temp_dir / "file.txt", "r") as h:
            self.assertEqual(h.read(), "world")

    def test_download(self):
        fp = self.temp_dir / "file.txt"
        with open(fp, "w") as h:
            h.write("hello")
        handle = LocalHandle(fp)
        download = self.temp_dir / 'download.txt'
        self.assertFalse(download.exists())
        handle.download(download)
        self.assertTrue(download.exists())
        with open(download, "r") as h:
            self.assertEqual(h.read(), "hello")

    def test_download_overwrite(self):
        fp = self.temp_dir / "file.txt"
        with open(fp, "w") as h:
            h.write("hello")
        handle = LocalHandle(fp)
        download = self.temp_dir / 'download.txt'
        with open(download, "w") as h:
            h.write("bar")
        self.assertTrue(download.exists())
        handle.download(download, True)
        self.assertTrue(download.exists())
        with open(download, "r") as h:
            self.assertEqual(h.read(), "hello")

    def test_download_no_overwrite(self):
        fp = self.temp_dir / "file.txt"
        with open(fp, "w") as h:
            h.write("hello")
        handle = LocalHandle(fp)
        download = self.temp_dir / 'download.txt'
        with open(download, "w") as h:
            h.write("bar")
        self.assertTrue(download.exists())
        with self.assertRaises(StorageError):
            handle.download(download)

    def test_search(self):
        files = []
        subfiles = []
        with open(self.temp_dir / "file.txt", "w") as h:
            h.write("hello")
            files.append(str(self.temp_dir / "file.txt"))
        with open(self.temp_dir / "file2.nc", "w") as h:
            h.write("hello")
            files.append(str(self.temp_dir / "file2.nc"))
        subdir = self.temp_dir / "foo"
        subdir.mkdir()
        with open(subdir / "file3.nc", "w") as h:
            h.write("hbar")
            subfiles.append(str(subdir / "file3.nc"))
        subdir2 = subdir / 'bar'
        subdir2.mkdir()
        with open(subdir2 / "file3.nc", "w") as h:
            h.write("hbar")
            subfiles.append(str(subdir2 / "file3.nc"))
        subfiles.extend(files)
        handle = LocalHandle(self.temp_dir)
        results = [x.path() for x in handle.search('*')]
        for file in subfiles:
            with self.subTest(path=file):
                self.assertIn(file, results)
        self.assertEqual(len(results), 4)
        results = [x.path() for x in handle.search('*', False)]
        self.assertEqual(len(results), 2)
        for file in subfiles:
            with self.subTest(path=file):
                if file in files:
                    self.assertIn(file, results)
                else:
                    self.assertNotIn(file, results)

    def test_build(self):
        if os.name == 'nt':
            with self.subTest(platform="windows"):
                handle = LocalHandle.build("C:\\hello\\world.txt")
                self.assertEqual(handle.path(), "C:\\hello\\world.txt")
        if os.name == 'posix':
            with self.subTest(platform="posix"):
                handle = LocalHandle.build("/srv/opt/hello")
                self.assertEqual(handle.path(), "/srv/opt/hello")

    def test_build_from_protocol(self):
        if os.name == 'nt':
            with self.subTest(platform="windows"):
                handle = LocalHandle.build("file://C:/hello/world.txt")
                self.assertEqual(handle.path(), "C:\\hello\\world.txt")
        if os.name == 'posix':
            with self.subTest(platform="posix"):
                handle = LocalHandle.build("file:///srv/opt/test")
                self.assertEqual(handle.path(), "/srv/opt/test")

    def test_supports(self):
        self.assertTrue(LocalHandle.supports('C:/12345.txt'))
        self.assertTrue(LocalHandle.supports('C:\\12345.txt'))
        self.assertTrue(LocalHandle.supports('/foo/bar/two'))
        self.assertTrue(LocalHandle.supports('file://C:/12345.txt'))
        self.assertTrue(LocalHandle.supports('file:///foo/bar/two'))
        self.assertFalse(LocalHandle.supports('http://test.com/file.html'))
        self.assertFalse(LocalHandle.supports('ftp://test.com/file.html'))
        self.assertFalse(LocalHandle.supports('ftps://test.com/file.html'))

    def test_read_dir(self):
        h = LocalHandle(self.temp_dir)
        with self.assertRaises(StorageError):
            h.download(self.temp_dir / "file.txt")

    def test_read_bad_dir(self):
        h = LocalHandle(self.temp_dir)
        with self.assertRaises(StorageError):
            h.download(self.temp_dir / "file.txt")

    def test_list_file(self):
        with open(self.temp_dir / "file.txt", "w") as h:
            h.write("a")
        h = LocalHandle(self.temp_dir / "file.txt")
        with self.assertRaises(StorageError):
            _ = [x for x in h.walk()]

    def test_read_chunks_bytes(self):
        handle = LocalHandle(self.temp_dir / "test.txt")
        t = [x for x in handle._local_read_chunks(b'12345')]
        self.assertEqual([b'12345'], t)

    def test_read_array(self):
        handle = LocalHandle(self.temp_dir)
        t = [x for x in handle._local_read_chunks([b'fo', b'ob', b'ar'], 2)]
        self.assertEqual([b'fo', b'ob', b'ar'], t)

    def test_read_chunks_open_file(self):
        p = self.temp_dir / "test.txt"
        with open(p, "w") as h:
            h.write("foobar")
        handle = LocalHandle(p)
        with open(p, "rb") as h:
            t = [x for x in handle._local_read_chunks(h, 2)]
        self.assertEqual([b'fo', b'ob', b'ar'], t)

    def test_halt_read(self):
        hf = DummyHaltFlag()
        class Test:

            def __init__(self):
                self.c = 0

            def read(self, *args, **kwargs):
                self.c += 1
                if self.c >= 2:
                    hf.event.set()
                return str(self.c).encode('utf-8')

        handle = LocalHandle(self.temp_dir, halt_flag=hf)
        data_read = []
        with self.assertRaises(HaltInterrupt):
            for b in handle._local_read_chunks(Test()):
                data_read.append(b)
        self.assertEqual([b'1', b'2'], data_read)
