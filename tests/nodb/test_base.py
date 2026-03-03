import datetime
import enum
import json
import unittest as ut
import cnodc.nodb.base as s
from cnodc.nodb.base import parse_received_date
from cnodc.util import CNODCError


class TestEnum(enum.Enum):
    ONE = "1"
    TWO = "2"
    THREE = "3"


class TestStuff(s.NODBBaseObject):

    TABLE_NAME = 'stuff_table'
    PRIMARY_KEYS = ('str',)

    ro = s.StringColumn("ro", readonly=True)
    str = s.StringColumn("str")
    integer = s.IntColumn("integer")
    boolean = s.BooleanColumn("boolean")
    real = s.FloatColumn("float")
    uuid = s.UUIDColumn("uuid")
    byte = s.ByteColumn("byte")
    date_time = s.DateTimeColumn("datetime")
    date_ = s.DateColumn("date")
    enum = s.EnumColumn("enum", TestEnum)
    json_ = s.JsonColumn("json")
    wkt = s.WKTColumn("wkt")

    def test_upper(self):
        return self._with_cache('upper', self._upper)

    def _upper(self):
        return 'foobar'.upper()


class TestDefaults(s.NODBBaseObject):
    pass


class TestBaseObject(ut.TestCase):

    def test_default_table_name(self):
        self.assertEqual('TestDefaults', TestDefaults.get_table_name())

    def test_default_primary_keys(self):
        pk = TestDefaults.get_primary_keys()
        self.assertIsInstance(pk, tuple)
        self.assertEqual(0, len(pk))

    def test_default_kwargs(self):
        td = TestDefaults(xyz="foobar")
        self.assertIn('xyz', td._data)
        self.assertEqual('foobar', td._data['xyz'])
        self.assertEqual('foobar', td.get_for_db('xyz'))
        self.assertEqual('foobar', td.get('xyz'))
        self.assertEqual('blank', td.get('xyz1', default='blank'))

    def test_received_date(self):
        self.assertEqual(datetime.date(2015, 10, 5), parse_received_date("2015-10-05"))
        self.assertEqual(datetime.date(2015, 10, 5), parse_received_date(datetime.date(2015, 10, 5)))
        with self.assertRaises(CNODCError):
            parse_received_date("foobar")

    def test_table_name(self):
        x = TestStuff()
        self.assertEqual(x.get_table_name(), 'stuff_table')
        self.assertEqual(TestStuff.get_table_name(), 'stuff_table')

    def test_primary_keys(self):
        x = TestStuff()
        obj_pks = x.get_primary_keys()
        cls_pks = TestStuff.get_primary_keys()
        self.assertEqual(obj_pks, cls_pks)
        self.assertEqual(1, len(obj_pks))
        self.assertIn('str', obj_pks)

    def test_cache(self):
        x = TestStuff()
        self.assertNotIn('upper', x._cache)
        self.assertEqual('FOOBAR', x.test_upper())
        self.assertIn('upper', x._cache)
        self.assertEqual('FOOBAR', x._cache['upper'])
        x.clear_cache('not_upper')
        self.assertIn('upper', x._cache)
        self.assertEqual('FOOBAR', x._cache['upper'])
        x.clear_cache()
        self.assertNotIn('upper', x._cache)
        self.assertEqual('FOOBAR', x.test_upper())
        self.assertIn('upper', x._cache)
        x.clear_cache('upper')
        self.assertNotIn('upper', x._cache)

    def test_readonly(self):
        x = TestStuff()
        with self.assertRaises(AttributeError):
            x.ro = "bar"

    def test_readonly_override(self):
        x = TestStuff()
        x._allow_set_readonly = True
        x.ro = "bar"
        self.assertEqual(x.ro, "bar")
        self.assertIn('ro', x.modified_values)

    def test_modified(self):
        x = TestStuff()
        x.mark_modified('ro')
        self.assertIn('ro', x.modified_values)
        x.clear_modified()
        self.assertNotIn('ro', x.modified_values)

    def test_string_field(self):
        x = TestStuff()
        x.str = 54321
        self.assertEqual(x.str, "54321")
        self.assertNotEqual(x.str, 54321)
        self.assertIn('str', x.modified_values)
        self.assertEqual(x.get_for_db('str'), '54321')

    def test_str_field_none(self):
        x = TestStuff()
        x.str = None
        self.assertIsNone(x.str)
        self.assertIn('str', x.modified_values)
        self.assertIsNone(x.get_for_db('str'))

    def test_integer_field(self):
        x = TestStuff()
        x.integer = "54321"
        self.assertEqual(x.integer, 54321)
        self.assertNotEqual(x.integer, "54321")
        self.assertIn("integer", x.modified_values)
        self.assertEqual(x.get_for_db("integer"), 54321)

    def test_integer_field_none(self):
        x = TestStuff()
        x.integer = None
        self.assertIsNone(x.integer)
        self.assertIn('integer', x.modified_values)
        self.assertIsNone(x.get_for_db('integer'))

    def test_float_field(self):
        x = TestStuff()
        x.real = "54.321"
        self.assertEqual(x.real, 54.321)
        self.assertNotEqual(x.real, "54.321")
        self.assertIn("float", x.modified_values)
        self.assertEqual(x.get_for_db("float"), 54.321)

    def test_float_field_none(self):
        x = TestStuff()
        x.real = None
        self.assertIsNone(x.real)
        self.assertIn('float', x.modified_values)
        self.assertIsNone(x.get_for_db('float'))

    def test_bool_field(self):
        x = TestStuff()
        x.boolean = 54
        self.assertIs(x.boolean, True)
        self.assertNotEqual(x.boolean, 54)
        self.assertIn("boolean", x.modified_values)
        self.assertIs(x.get_for_db("boolean"), True)

    def test_bool_field_none(self):
        x = TestStuff()
        x.boolean = None
        self.assertIsNone(x.boolean)
        self.assertIn('boolean', x.modified_values)
        self.assertIsNone(x.get_for_db('boolean'))

    def test_uuid_field(self):
        x = TestStuff()
        x.uuid = 54321
        self.assertEqual(x.uuid, "54321")
        self.assertNotEqual(x.uuid, 54321)
        self.assertIn('uuid', x.modified_values)
        self.assertEqual(x.get_for_db('uuid'), '54321')

    def test_uuid_field_none(self):
        x = TestStuff()
        x.uuid = None
        self.assertIsNone(x.uuid)
        self.assertIn('uuid', x.modified_values)
        self.assertIsNone(x.get_for_db('uuid'))

    def test_byte_field(self):
        x = TestStuff()
        x.byte = b'12345'
        self.assertEqual(x.byte, b'12345')
        self.assertIn('byte', x.modified_values)
        self.assertEqual(x.get_for_db('byte'), b'12345')

    def test_byte_field_none(self):
        x = TestStuff()
        x.byte = None
        self.assertIsNone(x.byte)
        self.assertIn('byte', x.modified_values)
        self.assertIsNone(x.get_for_db('byte'))

    def test_datetime_field_date_only(self):
        x = TestStuff()
        test = datetime.datetime(2015, 10, 1, 0, 0, 0)
        x.date_time = '2015-10-01'
        self.assertEqual(x.date_time, test)
        self.assertIn('datetime', x.modified_values)
        self.assertEqual(x.get_for_db('datetime'), test)

    def test_datetime_field(self):
        x = TestStuff()
        test = datetime.datetime(2015, 10, 1, 1, 2, 3)
        x.date_time = '2015-10-01T01:02:03'
        self.assertEqual(x.date_time, test)
        self.assertIn('datetime', x.modified_values)
        self.assertEqual(x.get_for_db('datetime'), test)

    def test_datetime_field_with_zone(self):
        x = TestStuff()
        test = datetime.datetime(2015, 10, 1, 1, 2, 3, tzinfo=datetime.timezone.utc)
        x.date_time = '2015-10-01T01:02:03+00:00'
        self.assertEqual(x.date_time, test)
        self.assertIn('datetime', x.modified_values)
        self.assertEqual(x.get_for_db('datetime'), test)

    def test_datetime_field_no_t(self):
        x = TestStuff()
        test = datetime.datetime(2015, 10, 1, 1, 2, 3)
        x.date_time = '2015-10-01 01:02:03'
        self.assertEqual(x.date_time, test)
        self.assertIn('datetime', x.modified_values)
        self.assertEqual(x.get_for_db('datetime'), test)

    def test_datetime_field_actual(self):
        x = TestStuff()
        test = datetime.datetime(2015, 10, 1, 1, 2, 3)
        x.date_time = test
        self.assertEqual(x.date_time, test)
        self.assertIn('datetime', x.modified_values)
        self.assertEqual(x.get_for_db('datetime'), test)

    def test_datetime_field_none(self):
        x = TestStuff()
        x.date_time = None
        self.assertIsNone(x.date_time)
        self.assertIn('datetime', x.modified_values)
        self.assertIsNone(x.get_for_db('datetime'))

    def test_date_field(self):
        x = TestStuff()
        x.date_ = '2015-01-02'
        self.assertEqual(x.date_, datetime.date(2015, 1, 2))
        self.assertIn('date', x.modified_values)
        self.assertEqual(x.get_for_db('date'), datetime.date(2015, 1, 2))

    def test_date_field_obj(self):
        x = TestStuff()
        x.date_ = datetime.date(2015, 1, 2)
        self.assertEqual(x.date_, datetime.date(2015, 1, 2))
        self.assertIn('date', x.modified_values)
        self.assertEqual(x.get_for_db('date'), datetime.date(2015, 1, 2))

    def test_date_field_none(self):
        x = TestStuff()
        x.date_ = None
        self.assertIsNone(x.date_)
        self.assertIn('date', x.modified_values)
        self.assertIsNone(x.get_for_db('date'))

    def test_enum_field(self):
        x = TestStuff()
        x.enum = TestEnum.ONE
        self.assertIs(x.enum, TestEnum.ONE)
        self.assertIn('enum', x.modified_values)
        self.assertEqual(x.get_for_db('enum'), TestEnum.ONE.value)

    def test_enum_field_str(self):
        x = TestStuff()
        x.enum = TestEnum.ONE.value
        self.assertIs(x.enum, TestEnum.ONE)
        self.assertIn('enum', x.modified_values)
        self.assertEqual(x.get_for_db('enum'), TestEnum.ONE.value)

    def test_enum_field_none(self):
        x = TestStuff()
        x.enum = None
        self.assertIsNone(x.enum)
        self.assertIn('enum', x.modified_values)
        self.assertIsNone(x.get_for_db('enum'))

    def test_json_field_list(self):
        x = TestStuff()
        x.json_ = ["hello", "world"]
        self.assertEqual(x.json_, ["hello", "world"])
        self.assertIn("json", x.modified_values)
        self.assertEqual(x.get_for_db("json"), json.dumps(["hello", "world"]))

    def test_json_field_list_str(self):
        x = TestStuff()
        x.json_ = json.dumps(["hello", "world"])
        self.assertEqual(x.json_, ["hello", "world"])
        self.assertIn("json", x.modified_values)
        self.assertEqual(x.get_for_db("json"), json.dumps(["hello", "world"]))

    def test_json_field_none(self):
        x = TestStuff()
        x.json_ = None
        self.assertIsNone(x.json_)
        self.assertIn("json", x.modified_values)
        self.assertIsNone(x.get_for_db("json"))

    def test_json_field_dict(self):
        x = TestStuff()
        x.json_ = {"hello": "world"}
        self.assertEqual(x.json_, {"hello": "world"})
        self.assertIn("json", x.modified_values)
        self.assertEqual(x.get_for_db("json"), json.dumps({"hello": "world"}))

    def test_json_field_dict_str(self):
        x = TestStuff()
        x.json_ = json.dumps({"hello": "world"})
        self.assertEqual(x.json_, {"hello": "world"})
        self.assertIn("json", x.modified_values)
        self.assertEqual(x.get_for_db("json"), json.dumps({"hello": "world"}))
