import gzip
import threading

from cnodc.util import HaltFlag, HaltInterrupt, gzip_with_halt
from cnodc.util.io import copy_with_halt, ungzip_with_halt
from core import BaseTestCase
from cnodc.util.halts import DummyHaltFlag


class TestHaltFlag(BaseTestCase):

    def test_halt_flag(self):
        e = threading.Event()
        hf = HaltFlag(e)
        self.assertTrue(hf._should_continue())
        self.assertTrue(hf.check_continue(False))
        self.assertTrue(hf.check_continue(True))
        try:
            self.assertIsNone(hf.breakpoint())
        except HaltInterrupt:
            self.assertFalse(True, msg='Halt interrupt raised when it should not be')
        e.set()
        self.assertFalse(hf._should_continue())
        self.assertFalse(hf.check_continue(False))
        with self.assertRaises(HaltInterrupt):
            hf.check_continue(True)
        with self.assertRaises(HaltInterrupt):
            hf.breakpoint()

    def test_protocol(self):
        chf = DummyHaltFlag()
        self.assertTrue(chf._should_continue())
        self.assertTrue(chf.check_continue())
        self.assertTrue(chf.check_continue(raise_ex=False))
        self.assertIsNone(chf.breakpoint())
        chf.event.set()
        self.assertFalse(chf._should_continue())
        self.assertFalse(chf.check_continue(raise_ex=False))
        self.assertFalse(chf.check_continue(False))
        self.assertRaises(HaltInterrupt, chf.check_continue)
        self.assertRaises(HaltInterrupt, chf.breakpoint)

    def test_iterate(self):
        chf = DummyHaltFlag()
        items = [1, 2, 3, 4, 5]
        new_items = []
        for item in HaltFlag._iterate(items, chf, False):
            new_items.append(item)
            if item == 3:
                chf.event.set()
        self.assertEqual(new_items, [1, 2, 3])

    def test_iterate_interrupt(self):
        def add_items(items, new_items, halt_flag):
            for item in HaltFlag._iterate(items, chf):
                new_items.append(item)
                if item == 3:
                    halt_flag.event.set()
        chf = DummyHaltFlag()
        items = [1, 2, 3, 4, 5]
        new_items = []
        with self.assertRaises(HaltInterrupt):
            add_items(items, new_items, chf)
        self.assertEqual(new_items, [1, 2, 3])


class TestHaltableIO(BaseTestCase):

    def test_halt_flag(self):
        f1 = self.temp_dir / "test1"
        with open(f1, "w") as h:
            h.write("foobar")
        f2 = self.temp_dir / "test2"
        hf = DummyHaltFlag()
        with open(f1, "r") as in_:
            with open(f2, "w") as out_:
                copy_with_halt(in_, out_, halt_flag=hf)
        self.assertTrue(f2.exists())

    def test_halt_flag_binary(self):
        f1 = self.temp_dir / "test1"
        with open(f1, "w") as h:
            h.write("foobar")
        f2 = self.temp_dir / "test2"
        hf = DummyHaltFlag()
        with open(f1, "rb") as in_:
            with open(f2, "wb") as out_:
                copy_with_halt(in_, out_, halt_flag=hf)
        self.assertTrue(f2.exists())

    def test_no_halt_flag(self):
        f1 = self.temp_dir / "test1"
        with open(f1, "w") as h:
            h.write("foobar")
        f2 = self.temp_dir / "test2"
        with open(f1, "r") as in_:
            with open(f2, "w") as out_:
                copy_with_halt(in_, out_)
        self.assertTrue(f2.exists())

    def test_no_halt_flag_binary(self):
        f1 = self.temp_dir / "test1"
        with open(f1, "w") as h:
            h.write("foobar")
        f2 = self.temp_dir / "test2"
        with open(f1, "rb") as in_:
            with open(f2, "wb") as out_:
                copy_with_halt(in_, out_)
        self.assertTrue(f2.exists())

    def test_gzip_handle_interrupt(self):
        f1 = self.temp_dir / "test1"
        with open(f1, "w") as h:
            h.write("foobar")
        f2 = self.temp_dir / "test1.gz"
        hf = DummyHaltFlag()
        hf.event.set()
        with self.assertRaises(HaltInterrupt):
            gzip_with_halt(f1, f2, 2, halt_flag=hf)
        self.assertFalse(f2.exists())

    def test_ungzip_handle_interrupt(self):
        f1 = self.temp_dir / "test1.gz"
        with gzip.open(f1, "wb") as h:
            h.write(b"foobar")
        f2 = self.temp_dir / "test1"
        hf = DummyHaltFlag()
        hf.event.set()
        with self.assertRaises(HaltInterrupt):
            ungzip_with_halt(f1, f2, 2, halt_flag=hf)
        self.assertFalse(f2.exists())







