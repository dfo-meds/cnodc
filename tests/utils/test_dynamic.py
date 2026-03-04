from core import BaseTestCase
from cnodc.util import DynamicObjectLoadError, dynamic_object


class TestDynamicLoad(BaseTestCase):

    def test_bad(self):
        with self.assertRaises(DynamicObjectLoadError):
            dynamic_object('hello_world')

    def test_function(self):
        obj = dynamic_object('utils.test_dynamic.test_function')
        self.assertEqual('test_function', obj.__name__)

    def test_class(self):
        obj = dynamic_object('utils.test_dynamic.TestClass')
        self.assertEqual('TestClass', obj.__name__)

    def test_bad_module(self):
        with self.assertRaises(DynamicObjectLoadError):
            dynamic_object("utils.test_not_a_module.TestClass")

    def test_bad_target(self):
        with self.assertRaises(DynamicObjectLoadError):
            dynamic_object("utils.test_dynamic.NotAClass")


def test_function():
    pass

class TestClass:
    pass
