from medsutil.frozendict import FrozenDict
from tests.helpers.base_test_case import BaseTestCase, sub_tests


class TestFrozenDict(BaseTestCase):

    def test_initial_dict_and_contains(self):
        x = FrozenDict({"foo": "bar"})
        self.assertTrue("foo" in x)
        self.assertIn("foo", x)
        self.assertEqual(x["foo"], "bar")

    def test_equals(self):
        x = FrozenDict({"foo": "bar"})
        y = FrozenDict({"foo": "bar"})
        self.assertTrue(x == y)

    def test_not_equals(self):
        x =  FrozenDict({"foo": "bar"})
        y = FrozenDict({"foo": "bar2"})
        self.assertTrue(x != y)

    def test_copy(self):
        x = FrozenDict({"foo": "bar"})
        y = x.__copy__()
        self.assertEqual(x, y)
        self.assertIsNot(x, y)

    def test_deepcopy(self):
        x = FrozenDict({"foo": "bar"})
        y = x.__deepcopy__({})
        self.assertEqual(x, y)
        self.assertIsNot(x, y)

    def test_values(self):
        x = FrozenDict({"foo": "bar"})
        values = x.values()
        self.assertIsInstance(values, list)

    def test_get(self):
        x = FrozenDict({"foo": "bar"})
        self.assertEqual(x.get("foo"), "bar")
        self.assertEqual(x.get("foo2", ""), "")
        with self.assertRaises(KeyError):
            x.get("foo3")

    @sub_tests([
        ("foo", ["bar"]),
        (["bar"], "foo"),
    ])
    def test_cannot_add_non_hashable(self, key, value):
        with self.assertRaises(TypeError):
            x = {}
            FrozenDict._add_hashable(key, value, x)