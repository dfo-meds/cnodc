import datetime
import decimal
import hashlib
import unittest as ut

from uncertainties import UFloat

import medsutil.ocproc2 as ocproc2

from medsutil.ocproc2 import SingleElement, AbstractElement, MultiElement
from medsutil.ocproc2.elements import normalize_data_value, UNIFORM_CONVERSION_FACTOR


class TestLowLevelThings(ut.TestCase):

    def test_normalize_data_value(self):
        pass_through = [
            'test',
            5,
            5.5,
            True,
            None
        ]
        for pt in pass_through:
            with self.subTest(input=pt):
                self.assertIs(pt, normalize_data_value(pt))
        date_ = datetime.date(2015, 10, 12)
        date_time_ = datetime.datetime(2015, 11, 13, 1, 2, 3)
        self.assertEqual('2015-10-12', normalize_data_value(date_))
        self.assertEqual('2015-11-13T01:02:03', normalize_data_value(date_time_))
        self.assertEqual(['test', 5, 5.5, True, None, '2015-10-12', '2015-11-13T01:02:03'], normalize_data_value([*pass_through, date_, date_time_]))
        self.assertEqual({'test': 5, 'test2': '2015-10-12'}, normalize_data_value({'test': 5, 'test2': date_}))
        with self.assertRaises(ValueError):
            normalize_data_value(self)


class TestSingleElement(ut.TestCase):

    def test_working_quality(self):
        x = SingleElement("five")
        x.metadata['WorkingQuality'] = 2
        self.assertEqual(x.working_quality(), 2)

    def test_units(self):
        x = SingleElement(5)
        x.metadata['Units'] = 'm'
        self.assertEqual(x.units(), 'm')

    def test_best_value(self):
        x = SingleElement('five')
        self.assertEqual(x.best(), 'five')

    def test_is_numeric(self):
        values = [
            ('five', False),
            ('2.3', True),
            ('2', True),
            (2.3, True),
            (2, True),
            (None, False),
            ([12,23], False),
            ({'test': 123}, False),
        ]
        for val, result in values:
            with self.subTest(input=val):
                element = SingleElement(val)
                self.assertEqual(element.is_numeric(), result)

    def test_is_integer(self):
        values = [
            ('five', False),
            ('2.3', False),
            ('2', True),
            (2.3, False),
            (2, True),
            (None, False),
            ([12,23], False),
            ({'test': 123}, False),
        ]
        for val, result in values:
            with self.subTest(input=val):
                element = SingleElement(val)
                self.assertEqual(element.is_integer(), result)

    def test_is_iso_datetime(self):
        values = [
            ('five', False),
            ('2.3', False),
            ('2', False),
            (2.3, False),
            (2, False),
            (None, False),
            ([12,23], False),
            ({'test': 123}, False),
            ('2015-10-05', True),
            ('2015-10-06T12:00:12', True),
            ('2015-10-02T01:02', True),
            ('2015-10-03T01:03:12.313432', True),
            ('2015-10-03T00:12:12-0500', True),
        ]
        for val, result in values:
            with self.subTest(input=val):
                element = SingleElement(val)
                self.assertEqual(element.is_iso_datetime(), result)

    def test_to_decimal(self):
        element = SingleElement("1234.31")
        self.assertEqual(element.to_decimal(), decimal.Decimal("1234.31"))
        with self.assertRaises(ValueError):
            element = SingleElement("str")
            element.to_decimal()

    def test_to_decimal_units(self):
        element = SingleElement("1234")
        element.metadata['Units'] = 'm'
        self.assertEqual(element.to_decimal("km"), decimal.Decimal('1.234'))

    def test_to_ufloat(self):
        element = SingleElement("1234.1")
        element.metadata['Uncertainty'] = "0.05"
        uf = element.to_ufloat()
        self.assertIsInstance(uf, UFloat)
        self.assertEqual(uf.nominal_value, 1234.1)
        self.assertEqual(uf.std_dev, 0.05)

    def test_to_ufloat_units(self):
        element = SingleElement("1234.1")
        element.metadata['Uncertainty'] = "0.05"
        element.metadata['Units'] = 'm'
        uf = element.to_ufloat('km')
        self.assertIsInstance(uf, UFloat)
        self.assertEqual(uf.nominal_value, 1.2341)
        self.assertEqual(uf.std_dev, 0.00005)

    def test_to_ufloat_negative(self):
        element = SingleElement("1234.1")
        element.metadata['Uncertainty'] = "-0.05"
        uf = element.to_ufloat()
        self.assertIsInstance(uf, UFloat)
        self.assertEqual(uf.nominal_value, 1234.1)
        self.assertEqual(uf.std_dev, 0.05)

    def test_to_ufloat_missing(self):
        element = SingleElement("1234.1")
        element.metadata['Uncertainty'] = None
        uf = element.to_ufloat()
        self.assertIsInstance(uf, float)
        self.assertEqual(uf, 1234.1)

    def test_to_ufloat_zero(self):
        element = SingleElement("1234.1")
        element.metadata['Uncertainty'] = 0
        uf = element.to_ufloat()
        self.assertIsInstance(uf, float)
        self.assertEqual(uf, 1234.1)

    def test_to_uniform_ufloat(self):
        element = SingleElement("1234.1")
        element.metadata['Uncertainty'] = "0.05"
        element.metadata['UncertaintyType'] = 'uniform'
        uf = element.to_ufloat()
        self.assertIsInstance(uf, UFloat)
        self.assertEqual(uf.nominal_value, 1234.1)
        self.assertEqual(uf.std_dev, float(decimal.Decimal("0.05") * UNIFORM_CONVERSION_FACTOR))

    def test_to_ufloat_no_unc(self):
        element = SingleElement("1234.1")
        uf = element.to_ufloat()
        self.assertIsInstance(uf, float)
        self.assertEqual(uf, 1234.1)

    def test_is_good(self):
        with self.subTest("blank quality"):
            element = SingleElement('12345')
            self.assertTrue(element.is_good())
        with self.subTest("quality=1"):
            element = SingleElement('12345')
            element.metadata['WorkingQuality'] = 1
            self.assertTrue(element.is_good())
        with self.subTest("quality=5"):
            element = SingleElement('12345')
            element.metadata['WorkingQuality'] = 5
            self.assertTrue(element.is_good())
        with self.subTest("quality=2"):
            element = SingleElement('12345')
            element.metadata['WorkingQuality'] = 2
            self.assertTrue(element.is_good())
        with self.subTest("quality=3"):
            element = SingleElement('12345')
            element.metadata['WorkingQuality'] = 3
            self.assertTrue(element.is_good(True))
            self.assertFalse(element.is_good(False))
        with self.subTest("quality=9"):
            element = SingleElement('12345')
            element.metadata['WorkingQuality'] = 9
            self.assertTrue(element.is_good(allow_empty=True))
            self.assertFalse(element.is_good(allow_empty=False))
        with self.subTest("empty value"):
            element = SingleElement('')
            self.assertTrue(element.is_good(allow_empty=True))
            self.assertFalse(element.is_good(allow_empty=False))
        with self.subTest("quality=4"):
            element = SingleElement('12345')
            element.metadata['WorkingQuality'] = 4
            self.assertFalse(element.is_good(True, True))
            self.assertFalse(element.is_good(False, True))
            self.assertFalse(element.is_good(False, False))
            self.assertFalse(element.is_good(True, False))

    def test_to_float(self):
        element = SingleElement("123.4")
        self.assertEqual(element.to_float(), float("123.4"))
        element = SingleElement("1")
        self.assertEqual(element.to_float(), float("1"))
        with self.assertRaises(ValueError):
            element = SingleElement("str")
            element.to_float()

    def test_to_int(self):
        element = SingleElement("5")
        self.assertEqual(element.to_int(), 5)
        with self.assertRaises(ValueError):
            element = SingleElement("str")
            element.to_int()
        with self.assertRaises(ValueError):
            element = SingleElement("5.2")
            element.to_int()

    def test_to_datetime(self):
        element = SingleElement("2015-01-02T09:08:07")
        self.assertEqual(element.to_datetime(), datetime.datetime(2015, 1, 2, 9, 8, 7, tzinfo=datetime.timezone.utc))
        element = SingleElement("2015-01-02T09:08:07-01:00")
        self.assertEqual(element.to_datetime(), datetime.datetime(2015, 1, 2, 9, 8, 7, tzinfo=datetime.timezone(datetime.timedelta(seconds=-3600))))
        element = SingleElement("2015-01-03")
        self.assertEqual(element.to_datetime(), datetime.datetime(2015, 1, 3, 0, 0, 0, tzinfo=datetime.timezone.utc))
        with self.assertRaises(ValueError):
            element = SingleElement("foobar")
            element.to_datetime()

    def test_to_str(self):
        element = SingleElement(5)
        self.assertEqual(element.to_string(), "5")

    def test_load(self):
        x = AbstractElement.build_from_mapping({'_value': 'hello'})
        self.assertIsInstance(x, SingleElement)
        self.assertEqual(x.value, 'hello')

    def test_value_hash(self):
        h = hashlib.new('sha256')
        x = SingleElement(5)
        x.update_hash(h)
        h2 = hashlib.new('sha256')
        x2 = SingleElement(5)
        x2.update_hash(h2)
        self.assertEqual(h.digest(), h2.digest())

    def test_value_hash_none(self):
        h = hashlib.new('sha256')
        x = SingleElement(None)
        x.update_hash(h)
        h2 = hashlib.new('sha256')
        x2 = SingleElement(None)
        x2.update_hash(h2)
        self.assertEqual(h.digest(), h2.digest())

    def test_value_hash_metadata(self):
        h = hashlib.new('sha256')
        x = SingleElement(5)
        x.metadata['Uncertainty'] = '0.05'
        x.metadata['Units'] = 'km'
        x.update_hash(h)
        h2 = hashlib.new('sha256')
        x2 = SingleElement(5)
        x2.metadata['Units'] = 'km'
        x2.metadata['Uncertainty'] = '0.05'
        x2.update_hash(h2)
        self.assertEqual(h.digest(), h2.digest())

    def test_value_hash_different_metadata(self):
        h = hashlib.new('sha256')
        x = SingleElement(5)
        x.metadata['Uncertainty'] = '0.05'
        x.metadata['Units'] = 'km'
        x.update_hash(h)
        h2 = hashlib.new('sha256')
        x2 = SingleElement(5)
        x2.metadata['SensorType'] = 'ctd'
        x2.metadata['Units'] = 'km'
        x2.metadata['Uncertainty'] = '0.05'
        x2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_value_hash_different_value(self):
        h = hashlib.new('sha256')
        x = SingleElement(5)
        x.metadata['Uncertainty'] = '0.05'
        x.metadata['Units'] = 'km'
        x.update_hash(h)
        h2 = hashlib.new('sha256')
        x2 = SingleElement(6)
        x2.metadata['Units'] = 'km'
        x2.metadata['Uncertainty'] = '0.05'
        x2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_passed_qc(self):
        for qc_flag in (1, 2, 5):
            with self.subTest(qc_flag=qc_flag):
                e = SingleElement(5)
                e.metadata['WorkingQuality'] = qc_flag
                self.assertTrue(e.passed_qc())

    def test_failed_qc(self):
        for qc_flag in (0, 3, 4, 9):
            e = SingleElement(5)
            e.metadata['WorkingQuality'] = qc_flag
            self.assertFalse(e.passed_qc())

    def test_quality(self):
        e = SingleElement(5)
        e.metadata['Quality'] = 2
        e.metadata['WorkingQuality'] = 12
        self.assertEqual(2, e.quality)

    def test_quality_working(self):
        e = SingleElement(5)
        e.metadata['WorkingQuality'] = 3
        self.assertEqual(3, e.quality)

    def test_quality_not_set(self):
        e = SingleElement(5)
        self.assertEqual(0, e.quality)

    def test_quality_missing(self):
        e = SingleElement(None)
        self.assertEqual(9, e.quality)

    def test_contains(self):
        e = SingleElement(5)
        self.assertIn(5, e)
        self.assertNotIn("5", e)

    def test_equal(self):
        e = SingleElement(5)
        e2 = SingleElement(5)
        self.assertEqual(e, e2)

    def test_not_equal_other_obj(self):
        e = SingleElement(5)
        self.assertNotEqual(e, self)

    def test_not_equal_multi(self):
        e = SingleElement(5)
        e2 = MultiElement([SingleElement(5), SingleElement(6)])
        self.assertNotEqual(e, e2)

    def test_equal_with_metadata(self):
        e = SingleElement(5)
        e.metadata['Units'] = 'km'
        e2 = SingleElement(5)
        e2.metadata['Units'] = 'km'
        self.assertEqual(e, e2)

    def test_not_equal(self):
        e = SingleElement(5)
        e2 = SingleElement(6)
        self.assertNotEqual(e, e2)

    def test_not_equal_with_metadata(self):
        e = SingleElement(5)
        e.metadata['Units'] = 'km'
        e2 = SingleElement(5)
        e2.metadata['Units'] = 'm'
        self.assertNotEqual(e, e2)

    def test_find_child(self):
        e = SingleElement(5)
        e.metadata['Units'] = 'km'
        self.assertIs(e.find_child([]), e)
        self.assertIs(e.find_child(["0"]), e)
        self.assertIs(e.find_child(['metadata', 'Units']), e.metadata['Units'])
        self.assertIsNone(e.find_child(["1"]))
        self.assertIsNone(e.find_child(["Uncertainty"]))

    def test_value_setter(self):
        e = SingleElement(None)
        e.value = datetime.datetime(2015, 10, 5, 1, 2, 3)
        self.assertEqual(e.value, '2015-10-05T01:02:03')

    def test_list_value(self):
        e = SingleElement(['a', 'b'])
        self.assertEqual(e.to_mapping(), {'_value': ['a', 'b']})

    def test_dict_value(self):
        e = SingleElement({'a': 'b'})
        self.assertEqual(e.to_mapping(), {'_value': {'a': 'b'}})

    def test_build_no_meta(self):
        e = SingleElement.build('1234')
        self.assertEqual(e.value, '1234')

    def test_build_meta(self):
        e = SingleElement.build('1234', {'Units': 'km'})
        self.assertEqual(e.value, '1234')
        self.assertEqual(e.metadata.best('Units'), 'km')


class TestMultiElement(ut.TestCase):

    def test_load_from_list(self):
        x = AbstractElement.build_from_mapping(['1', '2', '3'])
        self.assertIsInstance(x, MultiElement)
        self.assertEqual(3, len(x.value))
        self.assertEqual('1', x.value[0].value)
        self.assertEqual('2', x.value[1].value)
        self.assertEqual('3', x.value[2].value)

    def test_load(self):
        x = AbstractElement.build_from_mapping({'_values': ['hello', 'world']})
        self.assertIsInstance(x, MultiElement)
        self.assertEqual(len(x.value), 2)
        self.assertEqual(x.value[0].value, 'hello')
        self.assertEqual(x.value[1].value, 'world')

    def test_value_hash_stable_order(self):
        h = hashlib.new('sha256')
        x = MultiElement([
            SingleElement(6),
            SingleElement(5),
            SingleElement(None),
        ])
        x.update_hash(h)
        h2 = hashlib.new('sha256')
        x2 = MultiElement([
            SingleElement(5),
            SingleElement(None),
            SingleElement(6)
        ])
        x2.update_hash(h2)
        self.assertEqual(h.digest(), h2.digest())

class TestOCProc2ValueMap(ut.TestCase):

    def test_metadata_setting(self):
        dr = ocproc2.ParentRecord()
        self.assertFalse('TestValue' in dr.metadata)
        self.assertFalse(dr.metadata.has_value('TestValue'))
        dr.metadata['TestValue'] = 'one'
        self.assertTrue('TestValue' in dr.metadata)
        self.assertTrue(dr.metadata.has_value('TestValue'))
        by_getitem = dr.metadata['TestValue']
        by_get = dr.metadata.get('TestValue')
        self.assertIsInstance(by_getitem, ocproc2.SingleElement)
        self.assertIsInstance(by_get, ocproc2.SingleElement)
        self.assertEqual(by_get, by_getitem)
        self.assertEqual(by_get.value, 'one')
        self.assertEqual(by_get.best(), 'one')

    def test_parameter_setting(self):
        dr = ocproc2.ParentRecord()
        self.assertFalse('TestValue' in dr.parameters)
        self.assertFalse(dr.parameters.has_value('TestValue'))
        dr.parameters['TestValue'] = 'one'
        self.assertTrue('TestValue' in dr.parameters)
        self.assertTrue(dr.parameters.has_value('TestValue'))
        by_getitem = dr.parameters['TestValue']
        by_get = dr.parameters.get('TestValue')
        self.assertIsInstance(by_getitem, ocproc2.SingleElement)
        self.assertIsInstance(by_get, ocproc2.SingleElement)
        self.assertEqual(by_get, by_getitem)
        self.assertEqual(by_get.value, 'one')
        self.assertEqual(by_get.best(), 'one')

    def test_coordinate_setting(self):
        dr = ocproc2.ParentRecord()
        self.assertFalse('TestValue' in dr.coordinates)
        self.assertFalse(dr.coordinates.has_value('TestValue'))
        dr.coordinates['TestValue'] = 'one'
        self.assertTrue('TestValue' in dr.coordinates)
        self.assertTrue(dr.coordinates.has_value('TestValue'))
        by_getitem = dr.coordinates['TestValue']
        by_get = dr.coordinates.get('TestValue')
        self.assertIsInstance(by_getitem, ocproc2.SingleElement)
        self.assertIsInstance(by_get, ocproc2.SingleElement)
        self.assertEqual(by_get, by_getitem)
        self.assertEqual(by_get.value, 'one')
        self.assertEqual(by_get.best(), 'one')

    def test_set_string(self):
        dr = ocproc2.ParentRecord()
        self.assertFalse('TestValue' in dr.metadata)
        dr.metadata['TestValue'] = ocproc2.SingleElement('test')
        self.assertTrue('TestValue' in dr.metadata)
        self.assertFalse(dr.metadata['TestValue'].is_empty())
        self.assertFalse(dr.metadata['TestValue'].is_iso_datetime())
        self.assertFalse(dr.metadata['TestValue'].is_numeric())
        self.assertEqual(dr.metadata['TestValue'].value, 'test')

    def test_set_datetime(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        self.assertTrue('TestValue' in dr.metadata)
        self.assertFalse(dr.metadata['TestValue'].is_empty())
        self.assertTrue(dr.metadata['TestValue'].is_iso_datetime())
        self.assertFalse(dr.metadata['TestValue'].is_numeric())
        self.assertEqual(dr.metadata['TestValue'].value, '2024-01-01T00:00:00+00:00')

    def test_set_date(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = datetime.date(2024, 1, 1)
        self.assertTrue('TestValue' in dr.metadata)
        self.assertFalse(dr.metadata['TestValue'].is_empty())
        self.assertTrue(dr.metadata['TestValue'].is_iso_datetime())
        self.assertFalse(dr.metadata['TestValue'].is_numeric())
        self.assertEqual(dr.metadata['TestValue'].value, '2024-01-01')

    def test_set_float(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = 12.3
        self.assertTrue('TestValue' in dr.metadata)
        self.assertFalse(dr.metadata['TestValue'].is_empty())
        self.assertFalse(dr.metadata['TestValue'].is_iso_datetime())
        self.assertTrue(dr.metadata['TestValue'].is_numeric())
        self.assertEqual(dr.metadata['TestValue'].value, 12.3)

    def test_set_integer(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = 123
        self.assertTrue('TestValue' in dr.metadata)
        self.assertFalse(dr.metadata['TestValue'].is_empty())
        self.assertFalse(dr.metadata['TestValue'].is_iso_datetime())
        self.assertTrue(dr.metadata['TestValue'].is_numeric())
        self.assertEqual(dr.metadata['TestValue'].value, 123)

    def test_set_string_int(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = '123'
        self.assertTrue('TestValue' in dr.metadata)
        self.assertFalse(dr.metadata['TestValue'].is_empty())
        self.assertFalse(dr.metadata['TestValue'].is_iso_datetime())
        self.assertTrue(dr.metadata['TestValue'].is_numeric())
        self.assertEqual(dr.metadata['TestValue'].value, '123')

    def test_set_bool(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = True
        self.assertTrue('TestValue' in dr.metadata)
        self.assertFalse(dr.metadata['TestValue'].is_empty())
        self.assertFalse(dr.metadata['TestValue'].is_iso_datetime())
        self.assertTrue(dr.metadata['TestValue'].is_numeric())
        self.assertEqual(dr.metadata['TestValue'].value, True)

    def test_set_null(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = None
        self.assertTrue('TestValue' in dr.metadata)
        self.assertTrue(dr.metadata['TestValue'].is_empty())
        self.assertFalse(dr.metadata['TestValue'].is_iso_datetime())
        self.assertFalse(dr.metadata['TestValue'].is_numeric())
        self.assertIsNone(dr.metadata['TestValue'].value)

    def test_set_empty_str(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = ''
        self.assertTrue('TestValue' in dr.metadata)
        self.assertTrue(dr.metadata['TestValue'].is_empty())
        self.assertFalse(dr.metadata['TestValue'].is_iso_datetime())
        self.assertFalse(dr.metadata['TestValue'].is_numeric())
        self.assertEqual(dr.metadata['TestValue'].value, '')

    def test_set_multi_value(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = ocproc2.MultiElement([5, 6, 7])
        self.assertTrue('TestValue' in dr.metadata)
        self.assertFalse(dr.metadata['TestValue'].is_empty())
        self.assertTrue(dr.metadata['TestValue'].is_numeric())
        self.assertFalse(dr.metadata['TestValue'].is_iso_datetime())
        self.assertEqual([x.value for x in dr.metadata['TestValue'].values()], [5, 6, 7])
        self.assertEqual(dr.metadata['TestValue'].best(), 5)

    def test_set_multi_value_combo(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = ocproc2.MultiElement([None, 5, '6', 7, ''])
        self.assertTrue('TestValue' in dr.metadata)
        self.assertFalse(dr.metadata['TestValue'].is_empty())
        self.assertEqual(dr.metadata['TestValue'].best(), 5)
        self.assertTrue(dr.metadata['TestValue'].is_numeric())
        self.assertFalse(dr.metadata['TestValue'].is_iso_datetime())
        self.assertIsInstance(dr.metadata['TestValue'], ocproc2.MultiElement)
        self.assertEqual([x.value for x in dr.metadata['TestValue'].values()], [None, 5, '6', 7, ''])
        self.assertEqual(dr.metadata['TestValue'].best(), 5)

    def test_set_multi_numeric(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = ocproc2.MultiElement(['', 5, 6, 7, 8])
        self.assertTrue('TestValue' in dr.metadata)
        self.assertFalse(dr.metadata['TestValue'].is_empty())
        self.assertTrue(dr.metadata['TestValue'].is_numeric())
        self.assertFalse(dr.metadata['TestValue'].is_iso_datetime())
        self.assertEqual(dr.metadata['TestValue'].best(), 5)

    def test_set_multi_date(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = ocproc2.MultiElement([
            '',
            datetime.datetime(2023, 1, 1, 0, 0, 0),
            datetime.datetime(2024, 1, 1, 0, 0, 0),
            datetime.datetime(2023, 5, 3, 0, 0, 0)

        ])
        self.assertTrue('TestValue' in dr.metadata)
        self.assertFalse(dr.metadata['TestValue'].is_empty())
        self.assertFalse(dr.metadata['TestValue'].is_numeric())
        self.assertTrue(dr.metadata['TestValue'].is_iso_datetime())
        self.assertEqual(dr.metadata['TestValue'].best(), '2023-01-01T00:00:00')

    def test_set_multi_empty(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = ocproc2.MultiElement(['', '', None, '', None])
        self.assertTrue('TestValue' in dr.metadata)
        self.assertTrue(dr.metadata['TestValue'].is_empty())
        self.assertFalse(dr.metadata['TestValue'].is_numeric())
        self.assertFalse(dr.metadata['TestValue'].is_iso_datetime())
        self.assertIsNone(dr.metadata['TestValue'].best())

    def test_value_metadata(self):
        dr = ocproc2.ParentRecord()
        dr.metadata.set('TestValue', 5, {'Units': 'm s-1'})
        self.assertTrue('TestValue' in dr.metadata)
        self.assertTrue('Units' in dr.metadata['TestValue'].metadata)
        self.assertEqual(dr.metadata['TestValue'].metadata['Units'].value, 'm s-1')

    def test_update_value_map(self):
        dr = ocproc2.ParentRecord()
        dr.metadata.update({
            'TestValue': 5,
            'TestValue2': 10
        })
        self.assertTrue('TestValue' in dr.metadata)
        self.assertTrue('TestValue2' in dr.metadata)
        self.assertEqual(dr.metadata['TestValue'].value, 5)
        self.assertEqual(dr.metadata['TestValue2'].value, 10)

    def test_set_multiple(self):
        dr = ocproc2.ParentRecord()
        notes = ['abc', 'def', 'ghi']
        dr.metadata.set_many(
            'TestValue',
            values=['', 5, 6],
            common_metadata={'Units': 'm s-1'},
            specific_metadata=[{'Note': notes[0]}, {'Note': notes[1]}, {'Note': notes[2]}],
            metadata={'Note2': 'jkl'}
        )
        self.assertIn('TestValue', dr.metadata)
        values = ['', 5, 6]
        self.assertIsInstance(dr.metadata['TestValue'], ocproc2.MultiElement)
        for i in range(0, len(values)):
            with self.subTest(index=i, value=values[i]):
                obj = dr.metadata['TestValue'].value[i]
                self.assertIsInstance(obj, AbstractElement)
                self.assertIn('Units', obj.metadata)
                self.assertIn('Note', obj.metadata)
                self.assertNotIn('Note2', obj.metadata)
                self.assertEqual(obj.metadata['Note'].value, notes[i])
                self.assertEqual(obj.metadata['Units'].value, 'm s-1')
        self.assertEqual(dr.metadata['TestValue'].best(), 5)
        self.assertIn('Note2', dr.metadata['TestValue'].metadata)
        self.assertNotIn('Units', dr.metadata['TestValue'].metadata)
        self.assertEqual(dr.metadata['TestValue'].metadata['Note2'].value, 'jkl')

    def test_len(self):
        me = MultiElement()
        self.assertEqual(0, len(me))

    def test_len_many(self):
        me = MultiElement([2, 3, 4])
        self.assertEqual(3, len(me))

    def test_equal(self):
        me1 = MultiElement([SingleElement(2), SingleElement(3)])
        me2 = MultiElement([SingleElement(2), SingleElement(3)])
        self.assertEqual(me1, me2)

    def test_equal_different_order(self):
        me1 = MultiElement([SingleElement(2), SingleElement(3)])
        me2 = MultiElement([SingleElement(3), SingleElement(2)])
        self.assertEqual(me1, me2)

    def test_not_equal_too_many(self):
        me1 = MultiElement([SingleElement(2), SingleElement(3)])
        me2 = MultiElement([SingleElement(3), SingleElement(2), SingleElement(2)])
        self.assertNotEqual(me1, me2)

    def test_not_equal_different_meta(self):
        me1 = MultiElement([SingleElement(2), SingleElement(3)])
        me1.metadata['Foo'] = 'Bar'
        me2 = MultiElement([SingleElement(2), SingleElement(3)])
        me1.metadata['Foo'] = 'Bar2'
        self.assertNotEqual(me1, me2)

    def test_not_equal_one_different(self):
        me1 = MultiElement([SingleElement(2), SingleElement(3), SingleElement(2)])
        me2 = MultiElement([SingleElement(2), SingleElement(3), SingleElement(1)])
        self.assertNotEqual(me1, me2)

    def test_not_equal_other_different(self):
        me1 = MultiElement([SingleElement(2), SingleElement(3), SingleElement(1)])
        me2 = MultiElement([SingleElement(2), SingleElement(3), SingleElement(2)])
        self.assertNotEqual(me1, me2)

    def test_not_equal_other(self):
        me1 = MultiElement([SingleElement(2), SingleElement(3)])
        self.assertNotEqual(me1, self)

    def test_equal_single(self):
        me1 = MultiElement([SingleElement(2), SingleElement(3)])
        me2 = SingleElement(3)
        self.assertEqual(me1, me2)

    def test_ideal_single_value(self):
        me = MultiElement([
            SingleElement.build(1, {'Quality': 2}),
            SingleElement.build(2, {'Quality': 5}),
        ])
        self.assertIs(me.ideal(), me._value[1])

    def test_ideal_single_value_same(self):
        me = MultiElement([
            SingleElement.build(1, {'Quality': 0}),
            SingleElement.build(2, {'Quality': 0}),
        ])
        self.assertIs(me.ideal(), me._value[0])

    def test_find_child(self):
        me = MultiElement([
            SingleElement.build(1, {'Quality': 1}),
            SingleElement.build(2, {'Quality': 2}),
        ])
        me.metadata['Units'] = 'km'
        self.assertIs(me.find_child([]), me)
        self.assertIs(me.find_child(["0"]), me._value[0])
        self.assertIs(me.find_child(["1"]), me._value[1])
        self.assertIsNone(me.find_child(["2"]))
        self.assertIs(me.find_child(["metadata", "Units"]), me.metadata['Units'])
        self.assertIsNone(me.find_child(["metadata", "Uncertainty"]))
        self.assertIsNone(me.find_child(["coordinates"]))

    def test_to_mapping(self):
        me = MultiElement([
            SingleElement.build(1, {'Quality': 1}),
            SingleElement.build(2, {'Quality': 2}),
        ])
        me.metadata['Units'] = 'km'
        self.assertEqual(me.to_mapping(), {
            '_values': [
                {'_value': 1, '_metadata': {'Quality': 1}},
                {'_value': 2, '_metadata': {'Quality': 2}}
            ],
            '_metadata': {
                'Units': 'km'
            }
        })

    def test_to_mapping_short(self):
        me = MultiElement([
            SingleElement.build(1, {'Quality': 1}),
            SingleElement.build(2, {'Quality': 2}),
        ])
        self.assertEqual(me.to_mapping(), [
            {'_value': 1, '_metadata': {'Quality': 1}},
            {'_value': 2, '_metadata': {'Quality': 2}}
        ])


class TestElementMap(ut.TestCase):

    def test_find_child(self):
        em = ocproc2.ElementMap()
        em.set('Hello', 'World')
        self.assertIs(em.find_child([]), em)
        self.assertIs(em.find_child(['Hello']), em['Hello'])
        self.assertIsNone(em.find_child(['None']))

    def test_update_kwargs(self):
        em = ocproc2.ElementMap()
        em.update(Hello="World", Units="km")
        self.assertEqual(em.get('Hello').value, 'World')
        self.assertEqual(em.get('Units').value, 'km')

    def test_ensure_element(self):
        em = ocproc2.ElementMap.ensure_element('a', {'b': 'c'}, d='e')
        self.assertEqual(em.value, 'a')
        self.assertIn('b', em.metadata)
        self.assertIn('d', em.metadata)
        self.assertEqual('c', em.metadata['b'].value)
        self.assertEqual('e', em.metadata['d'].value)

