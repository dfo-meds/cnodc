from cnodc.util import HaltFlag, HaltInterrupt
from core import BaseTestCase


class ConstantHaltFlag(HaltFlag):

    def __init__(self, sc: bool):
        self.should_continue = sc

    def _should_continue(self) -> bool:
        return self.should_continue


class TestHaltFlag(BaseTestCase):

    def test_protocol(self):
        chf = ConstantHaltFlag(True)
        self.assertTrue(chf._should_continue())
        self.assertTrue(chf.check_continue())
        self.assertTrue(chf.check_continue(raise_ex=False))
        self.assertIsNone(chf.breakpoint())
        chf.should_continue = False
        self.assertFalse(chf._should_continue())
        self.assertFalse(chf.check_continue(raise_ex=False))
        self.assertFalse(chf.check_continue(False))
        self.assertRaises(HaltInterrupt, chf.check_continue)
        self.assertRaises(HaltInterrupt, chf.breakpoint)

    def test_iterate(self):
        chf = ConstantHaltFlag(True)
        items = [1, 2, 3, 4, 5]
        new_items = []
        for item in HaltFlag.iterate(items, chf, False):
            new_items.append(item)
            if item == 3:
                chf.should_continue = False
        self.assertEqual(new_items, [1, 2, 3])

    def test_iterate_interrupt(self):
        def add_items(items, new_items, halt_flag):
            for item in HaltFlag.iterate(items, chf, True):
                new_items.append(item)
                if item == 3:
                    halt_flag.should_continue = False
        chf = ConstantHaltFlag(True)
        items = [1, 2, 3, 4, 5]
        new_items = []
        self.assertRaises(HaltInterrupt, add_items, items, new_items, chf)
        self.assertEqual(new_items, [1, 2, 3])



