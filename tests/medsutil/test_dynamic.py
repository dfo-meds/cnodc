from medsutil.awaretime import AwareDateTime
from medsutil.dynamic import DynamicObjectLoadError, dynamic_name, dynamic_object
from tests.helpers.base_test_case import BaseTestCase


class TestDynamicLoad(BaseTestCase):
    def test_builtin_name(self):
        self.assertEqual(dynamic_name(str), "str")

    def test_object_name(self):
        self.assertEqual(
            dynamic_name(AwareDateTime), "medsutil.awaretime.AwareDateTime"
        )

    def test_bad(self):
        with self.assertRaises(DynamicObjectLoadError):
            dynamic_object("hello_world")

    def test_function(self):
        obj = dynamic_object(dynamic_name(test_function))
        self.assertEqual("test_function", obj.__name__)

    def test_class(self):
        obj = dynamic_object(dynamic_name(TestClass))
        self.assertEqual("TestClass", obj.__name__)

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
