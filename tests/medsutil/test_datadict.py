import enum
import math

import medsutil.datadict as dd
import medsutil.delayed
from medsutil import json
from medsutil.awaretime import AwareDateTime
from tests.helpers.base_test_case import BaseTestCase
import datetime
import typing as t
from medsutil.types import *


class TestEnum(enum.Enum):

    ONE = 1
    TWO = 2


class BoringObject(dd.DataDictObject):

    int_prop: int = dd.p_int()
    float_prop: float = dd.p_float()
    bool_prop: bool = dd.p_bool()
    str_prop: str = dd.p_str()
    datetime_prop: datetime.datetime = dd.p_datetime()
    date_prop: datetime.date = dd.p_date()
    aware_prop: AwareDateTime = dd.p_awaretime()
    enum_prop: TestEnum = dd.p_enum(TestEnum)

    readonly_prop: str = dd.p_str(readonly=True)
    required_prop: str = dd.p_str(required=True)
    default_prop: str = dd.p_str(default='foobar')
    diff_name_prop: str = dd.p_str(managed_name='diff_name')

    json_dict_prop: dict = dd.p_json_dict()
    json_list_prop: list = dd.p_json_list()
    json_set_prop: set = dd.p_json_set()
    ddo_prop: t.Self = dd.p_json_object()
    ddo_list_prop: list[t.Self] = dd.p_json_object_list()
    ddo_dict_prop: dict[str, t.Self] = dd.p_json_object_dict()
    i18n_prop: LanguageDict = dd.p_i18n_text()



class TestDataDictObject(BaseTestCase):

    def test_default_dict(self):
        d1 = medsutil.delayed.newdict()
        self.assertIsInstance(d1, dict)
        d2 = medsutil.delayed.newdict()
        self.assertIsInstance(d2, dict)
        d1['hello'] = 'foo'
        self.assertIn('hello', d1)
        self.assertNotIn('hello', d2)

    def test_default_list(self):
        d1 = medsutil.delayed.newlist()
        self.assertIsInstance(d1, list)
        d2 = medsutil.delayed.newlist()
        self.assertIsInstance(d2, list)
        d1.append('hello')
        self.assertIn('hello', d1)
        self.assertNotIn('hello', d2)

    def test_default_set(self):
        d1 = medsutil.delayed.newset()
        self.assertIsInstance(d1, set)
        d2 = medsutil.delayed.newset()
        self.assertIsInstance(d2, set)
        d1.add('hello')
        self.assertIn('hello', d1)
        self.assertNotIn('hello', d2)

    def test_default_uuid(self):
        d1 = medsutil.delayed.newuuid()
        self.assertIsInstance(d1, str)
        d2 = medsutil.delayed.newuuid()
        self.assertIsInstance(d2, str)
        self.assertNotEqual(d1, d2)

    def test_new_dict_property(self):
        obj = BoringObject(required_prop='')
        self.assertIsInstance(obj.json_dict_prop, dict)
        obj.json_dict_prop['hello'] = 'world'
        obj2 = BoringObject(required_prop='')
        self.assertIsInstance(obj2.json_dict_prop, dict)
        self.assertNotIn('hello', obj2.json_dict_prop)

    def test_set_not_a_prop(self):
        with self.assertRaises(TypeError):
            obj = BoringObject(required_prop='', not_a_prop='surprise!')

    def test_readonly_prop_set_and_readonly_access(self):
        obj = BoringObject(required_prop='', readonly_prop='foobar')
        self.assertEqual(obj.readonly_prop, 'foobar')
        with self.assertRaises(AttributeError):
            obj.readonly_prop = 'hello world'
        self.assertEqual(obj.readonly_prop, 'foobar')
        with obj.readonly_access():
            obj.readonly_prop = 'hello world 2'
        self.assertEqual(obj.readonly_prop, 'hello world 2')

    def test_required_prop(self):
        with self.assertRaises(ValueError):
            obj = BoringObject()

    def test_default_prop(self):
        obj = BoringObject(required_prop='')
        self.assertEqual(obj.default_prop, 'foobar')

    def test_override_default_prop(self):
        obj = BoringObject(required_prop='', default_prop='hello')
        self.assertEqual(obj.default_prop, 'hello')

    def test_to_map(self):
        obj = BoringObject(
            bool_prop=False,
            int_prop=1,
            enum_prop=TestEnum.TWO,
            float_prop=3.14159,
            str_prop='four',
            datetime_prop='2000-05-06T00:01:02+00:00',
            aware_prop='2000-07-08T01:02:03-05:00',
            json_dict_prop={'one': 1, 'two': 2},
            json_list_prop=[1, 2],
            json_set_prop=[1, 2, 2],
            diff_name_prop='w',
            readonly_prop='x',
            default_prop='y',
            required_prop='z',
        )
        d = obj.export()
        self.assertIsInstance(d, dict)
        self.assertIsNot(d, obj._data)
        json_str = json.dumps(d)
        self.assertIsInstance(json_str, str)

    def test_del_data(self):
        obj = BoringObject(readonly_prop='foobar', required_prop='abc')
        with self.assertRaises(AttributeError):
            del obj.readonly_prop
        with obj.readonly_access():
            del obj.readonly_prop
        self.assertNotIn('readonly_prop', obj._data)

    def _cleanup_value(self, value, type_name):
        if type_name == 'int' and not isinstance(value, int):
            return int(value)
        if type_name == 'str' and not isinstance(value, str):
            return str(value)
        if type_name == 'float' and not isinstance(value, float):
            return float(value)
        if type_name == 'datetime':
            if isinstance(value, str):
                return datetime.datetime.fromisoformat(value)
            elif isinstance(value, AwareDateTime):
                return datetime.datetime.fromisoformat(value.isoformat())
            elif isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
                return datetime.datetime(value.year, value.month, value.day)
        if type_name == 'date':
            if isinstance(value, str):
                return datetime.date.fromisoformat(value)
            elif isinstance(value, (datetime.datetime, AwareDateTime)):
                return value.date()
        if type_name == 'aware':
            if isinstance(value, str):
                return AwareDateTime.fromisoformat(value)
            elif isinstance(value, datetime.datetime):
                return AwareDateTime.from_datetime(value)
            elif isinstance(value, datetime.date):
                return AwareDateTime(value.year, value.month, value.day)
        if type_name == "i18n":
            if isinstance(value, str):
                return {"und": value}
        return value

    def test_properties(self):
        tests = [
            ('int_prop', [5, 0, -5, '5', 5.1]),
            ('float_prop', [1.2, 0.0, -5.1, math.nan, '5.1', 4]),
            ('bool_prop', [True, False]),
            ('str_prop', ['', 'hello_world', 'eh\r\n\t']),
            ('datetime_prop', [datetime.datetime(2015, 1, 2, 3,4, 5), AwareDateTime(2016, 2, 3, 4, 5, 6), '2015-02-03T04:05:06', '2015-01-02', datetime.date(2016, 3, 4)]),
            ('date_prop', [datetime.date(2015, 1, 2), '2015-02-03']),
            ('aware_prop', [datetime.datetime(2015, 1, 2, 3,4, 5), AwareDateTime(2016, 2, 3, 4, 5, 6), '2015-02-03T04:05:06', datetime.date(2017, 4, 5)]),
            ('enum_prop', [TestEnum.ONE, TestEnum.TWO]),
            ("i18n_prop", ["hello", {"und": "hello"}, {"en": "hello"}, {"fr": "hello"}])
        ]
        for prop_name, test_values in tests:
            for test_value in test_values:
                expected = self._cleanup_value(test_value, prop_name[:-5])
                with self.subTest(prop_name=prop_name, test_value=test_value, type='constructor'):
                    obj = BoringObject(required_prop='f', **{prop_name: test_value})
                    if isinstance(expected, float) and math.isnan(expected):
                        self.assertTrue(math.isnan(getattr(obj, prop_name)))
                    else:
                        self.assertEqual(getattr(obj, prop_name), expected)
                    setattr(obj, prop_name, None)
                    self.assertIsNone(getattr(obj, prop_name))
                with self.subTest(prop_name=prop_name, test_value=test_value, type='set'):
                    obj = BoringObject(required_prop='f')
                    self.assertIsNone(getattr(obj, prop_name))
                    setattr(obj, prop_name, test_value)
                    if isinstance(expected, float) and math.isnan(expected):
                        self.assertTrue(math.isnan(getattr(obj, prop_name)))
                    else:
                        self.assertEqual(getattr(obj, prop_name), expected)

