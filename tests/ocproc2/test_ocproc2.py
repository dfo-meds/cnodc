import datetime
import decimal
import unittest as ut
import cnodc.ocproc2 as ocproc2
import typing as t

from cnodc.ocproc2 import SingleElement, AbstractElement, MultiElement
from cnodc.ocproc2.elements import normalize_data_value


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
        self.assertEqual(x.best_value(), 'five')

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
        with self.assertRaises(decimal.InvalidOperation):
            element = SingleElement("str")
            element.to_decimal()

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
        self.assertEqual(element.to_datetime(), datetime.datetime(2015, 1, 2, 9, 8, 7))
        element = SingleElement("2015-01-03")
        self.assertEqual(element.to_datetime(), datetime.datetime(2015, 1, 3, 0, 0, 0))
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


class TestMultiElement(ut.TestCase):

    def test_load(self):
        x = AbstractElement.build_from_mapping({'_values': ['hello', 'world']})
        self.assertIsInstance(x, MultiElement)
        self.assertEqual(len(x.value), 2)
        self.assertEqual(x.value[0].value, 'hello')
        self.assertEqual(x.value[1].value, 'world')


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
        self.assertEqual(by_get.best_value(), 'one')

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
        self.assertEqual(by_get.best_value(), 'one')

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
        self.assertEqual(by_get.best_value(), 'one')

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
        self.assertEqual(dr.metadata['TestValue'].best_value(), 5)

    def test_set_multi_value_combo(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = ocproc2.MultiElement([None, 5, '6', 7, ''])
        self.assertTrue('TestValue' in dr.metadata)

        self.assertFalse(dr.metadata['TestValue'].is_empty())
        self.assertEqual(dr.metadata['TestValue'].best_value(), 5)
        self.assertTrue(dr.metadata['TestValue'].is_numeric())
        self.assertFalse(dr.metadata['TestValue'].is_iso_datetime())
        self.assertEqual([x.value for x in dr.metadata['TestValue'].values()], [None, 5, '6', 7, ''])
        self.assertEqual(dr.metadata['TestValue'].best_value(), 5)

    def test_set_multi_numeric(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = ocproc2.MultiElement(['', 5, 6, 7, 8])
        self.assertTrue('TestValue' in dr.metadata)
        self.assertFalse(dr.metadata['TestValue'].is_empty())
        self.assertTrue(dr.metadata['TestValue'].is_numeric())
        self.assertFalse(dr.metadata['TestValue'].is_iso_datetime())
        self.assertEqual(dr.metadata['TestValue'].best_value(), 5)

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
        self.assertEqual(dr.metadata['TestValue'].best_value(), '2023-01-01T00:00:00')

    def test_set_multi_empty(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['TestValue'] = ocproc2.MultiElement(['', '', None, '', None])
        self.assertTrue('TestValue' in dr.metadata)
        self.assertTrue(dr.metadata['TestValue'].is_empty())
        self.assertFalse(dr.metadata['TestValue'].is_numeric())
        self.assertFalse(dr.metadata['TestValue'].is_iso_datetime())
        self.assertIsNone(dr.metadata['TestValue'].best_value())

    def test_value_metadata(self):
        dr = ocproc2.ParentRecord()
        dr.metadata.set_element('TestValue', 5, {'Units': 'm s-1'})
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
        dr.metadata.set_multiple(
            'TestValue',
            values=['', 5, 6],
            common_metadata={'Units': 'm s-1'},
            specific_metadata=[{'Note': notes[0]}, {'Note': notes[1]}, {'Note': notes[2]}],
            metadata={'Note2': 'jkl'}
        )
        self.assertTrue('TestValue' in dr.metadata)
        values = ['', 5, 6]
        self.assertIsInstance(dr.metadata['TestValue'], ocproc2.MultiElement)
        for i in range(0, len(values)):
            with self.subTest(index=i, value=values[i]):
                obj = dr.metadata['TestValue'][i]
                self.assertIn('Units', obj.metadata)
                self.assertIn('Note', obj.metadata)
                self.assertNotIn('Note2', obj.metadata)
                self.assertEqual(obj.metadata['Note'].value, notes[i])
                self.assertEqual(obj.metadata['Units'].value, 'm s-1')
        self.assertEqual(dr.metadata['TestValue'].best_value(), 5)
        self.assertTrue('Note2' in dr.metadata['TestValue'].metadata)
        self.assertFalse('Units' in dr.metadata['TestValue'].metadata)
        self.assertEqual(dr.metadata['TestValue'].metadata['Note2'].value, 'jkl')


class TestOCProc2ImportExport(ut.TestCase):

    def _check_mapping_dict(self, actual: dict, ref: dict, parent_path: t.Optional[list] = None):
        all_keys = set(ref.keys())
        all_keys.update(actual.keys())
        parent_path = parent_path or []
        for key in all_keys:
            path = [*parent_path, key]
            with self.subTest(item_path='/'.join(str(x) for x in path)):
                self.assertIn(key, actual.keys(), msg="Reference key is not in the actual map")
                self.assertIn(key, ref.keys(), msg="Actual key is not in the reference map")
                self._delegate_check(actual[key], ref[key], path)

    def _delegate_check(self, actual, ref, path):
        self.assertEqual(type(actual), type(ref), msg=f"Bad type match at {path}")
        if isinstance(actual, dict):
            self._check_mapping_dict(actual, ref, path)
        elif isinstance(actual, list):
            self._check_mapping_list(actual, ref, path)
        else:
            self._check_mapping_value(actual, ref)

    def _check_mapping_list(self, actual: list, ref: list, parent_path: t.Optional[list] = None):
        self.assertEqual(len(actual), len(ref))
        for i in range(0, len(ref)):
            self._delegate_check(actual[i], ref[i], [*parent_path, str(i)])

    def _check_mapping_value(self, actual, ref):
        if ref is None:
            self.assertIsNone(actual)
        else:
            self.assertIsNotNone(actual)
            self.assertEqual(actual, ref)

    def test_full_to_mapping(self):
        dr = ocproc2.ParentRecord()
        dr.metadata['M1'] = 'abc'
        dr.metadata['M2'] = datetime.datetime(2023, 1, 1, 1, 2, 3)
        dr.metadata['M3'] = datetime.date(2023, 12, 31)
        dr.metadata['M4'] = 123
        dr.metadata['M5'] = ''
        dr.metadata['M6'] = None
        dr.metadata['M7'] = 123.34
        dr.metadata['M8'] = True
        dr.metadata['M9'] = False
        dr.coordinates['C1'] = ocproc2.SingleElement(123.45, Units="degree")
        dr.coordinates['C2'] = ocproc2.SingleElement(12.34, Units="degree")
        dr.parameters['P1'] = ocproc2.SingleElement(12.34, Units='0.001', Uncertainty=0.01)
        dr.parameters['P2'] = ocproc2.SingleElement(5, Units='m s-1', Uncertainty=0.1, SensorHeight=ocproc2.SingleElement(1, Units='m'))
        dr.history.append(ocproc2.HistoryEntry(
            'hello world',
            '2023-01-01T00:00:00+00:00',
            'test1',
            'version1',
            'instance1',
            ocproc2.MessageType.INFO
        ))
        dr.history.append(ocproc2.HistoryEntry(
            'hello world2',
            '2023-01-02T00:00:00+00:00',
            'test1',
            'version1',
            'instance1',
            ocproc2.MessageType.INFO
        ))
        dr.qc_tests.append(ocproc2.QCTestRunInfo(
            'test1',
            'version1',
            '2023-01-03T00:00:00+00:00',
            ocproc2.QCResult.FAIL,
            [
                ocproc2.QCMessage('lat_fail', ['a', 'b', 'c'], 90),
                ocproc2.QCMessage('lon_fail', ['a', 'b', 'd'], -180)
            ],
            'hello world3',
            test_tags=["foobar"]
        ))
        profile1 = dr.subrecords.new_recordset('PROFILE')
        for i in range(0, 5):
            sr = ocproc2.ParentRecord()
            sr.coordinates['C3'] = ocproc2.SingleElement(10 + (i * 10), Units="m", Uncertainty=5)
            profile1.records.append(sr)
        map_ = dr.to_mapping()
        self.assertIsInstance(map_, dict)
        self._check_mapping_dict(map_, {
            '_parameters': {
                'P1': {
                    '_value': 12.34,
                    '_metadata': {
                        'Units': '0.001',
                        'Uncertainty': 0.01
                    }
                },
                'P2': {
                    '_value': 5,
                    '_metadata': {
                        'Units': 'm s-1',
                        'Uncertainty': 0.1,
                        'SensorHeight': {
                            '_value': 1,
                            '_metadata': {
                                'Units': 'm'
                            }
                        }
                    }
                }
            },
            '_coordinates': {
                'C1': {
                    '_value': 123.45,
                    '_metadata': {
                        'Units': 'degree'
                    }
                },
                'C2': {
                    '_value': 12.34,
                    '_metadata': {
                        'Units': 'degree'
                    }
                }
            },
            '_metadata': {
                'M1': 'abc',
                'M2': '2023-01-01T01:02:03',
                'M3': '2023-12-31',
                'M4': 123,
                'M5': '',
                'M6': None,
                'M7': 123.34,
                'M8': True,
                'M9': False
            },
            '_subrecords': {
                'PROFILE': {
                    '0': {
                        '_records': [
                        {
                            '_coordinates': {
                                'C3': {
                                    '_value': 10,
                                    '_metadata': {
                                        'Units': 'm',
                                        'Uncertainty': 5
                                    }
                                }
                            }
                        },
                        {
                            '_coordinates': {
                                'C3': {
                                    '_value': 20,
                                    '_metadata': {
                                        'Units': 'm',
                                        'Uncertainty': 5
                                    }
                                }
                            }
                        },
                        {
                            '_coordinates': {
                                'C3': {
                                    '_value': 30,
                                    '_metadata': {
                                        'Units': 'm',
                                        'Uncertainty': 5
                                    }
                                }
                            }
                        },
                        {
                            '_coordinates': {
                                'C3': {
                                    '_value': 40,
                                    '_metadata': {
                                        'Units': 'm',
                                        'Uncertainty': 5
                                    }
                                }
                            }
                        },
                        {
                            '_coordinates': {
                                'C3': {
                                    '_value': 50,
                                    '_metadata': {
                                        'Units': 'm',
                                        'Uncertainty': 5
                                    }
                                }
                            }
                        },
                    ], }
                }
            },
            '_history': [
                {
                    '_message': 'hello world',
                    '_timestamp': '2023-01-01T00:00:00+00:00',
                    '_source': ('test1', 'version1', 'instance1'),
                    '_message_type': 'I'
                },
                {
                    '_message': 'hello world2',
                    '_timestamp': '2023-01-02T00:00:00+00:00',
                    '_source': ('test1', 'version1', 'instance1'),
                    '_message_type': 'I'
                }
            ],
            '_qc_tests': [
                {
                    '_name': 'test1',
                    '_version': 'version1',
                    '_date': '2023-01-03T00:00:00+00:00',
                    '_messages': [
                        {
                            '_code': 'lat_fail',
                            '_path': 'a/b/c',
                            '_ref': 90
                        },
                        {
                            '_code': 'lon_fail',
                            '_path': 'a/b/d',
                            '_ref': -180
                        }
                    ],
                    '_result': 'F',
                    '_stale': False,
                    '_notes': 'hello world3',
                    '_tags': ['foobar'],
                }
            ]
        })

    def test_load_from_map(self):
        dr = ocproc2.ParentRecord()
        dr.from_mapping({
            '_parameters': {
                'P1': {
                    '_value': 12.34,
                    '_metadata': {
                        'Units': '0.001',
                        'Uncertainty': 0.01
                    }
                },
                'P2': {
                    '_value': 5,
                    '_metadata': {
                        'Units': 'm s-1',
                        'Uncertainty': 0.1,
                        'SensorHeight': {
                            '_value': 1,
                            '_metadata': {
                                'Units': 'm'
                            }
                        }
                    }
                }
            },
            '_coordinates': {
                'C1': {
                    '_value': 123.45,
                    '_metadata': {
                        'Units': 'degree'
                    }
                },
                'C2': {
                    '_value': 12.34,
                    '_metadata': {
                        'Units': 'degree'
                    }
                }
            },
            '_metadata': {
                'M1': 'abc',
                'M2': '2023-01-01T01:02:03',
                'M3': '2023-12-31',
                'M4': 123,
                'M5': '',
                'M6': None,
                'M7': 123.34,
                'M8': True,
                'M9': False
            },
            '_subrecords': {
                'PROFILE': {
                    '0': [
                        {
                            '_coordinates': {
                                'C3': {
                                    '_value': 10,
                                    '_metadata': {
                                        'Units': 'm',
                                        'Uncertainty': 5
                                    }
                                }
                            }
                        },
                        {
                            '_coordinates': {
                                'C3': {
                                    '_value': 20,
                                    '_metadata': {
                                        'Units': 'm',
                                        'Uncertainty': 5
                                    }
                                }
                            }
                        },
                        {
                            '_coordinates': {
                                'C3': {
                                    '_value': 30,
                                    '_metadata': {
                                        'Units': 'm',
                                        'Uncertainty': 5
                                    }
                                }
                            }
                        },
                        {
                            '_coordinates': {
                                'C3': {
                                    '_value': 40,
                                    '_metadata': {
                                        'Units': 'm',
                                        'Uncertainty': 5
                                    }
                                }
                            }
                        },
                        {
                            '_coordinates': {
                                'C3': {
                                    '_value': 50,
                                    '_metadata': {
                                        'Units': 'm',
                                        'Uncertainty': 5
                                    }
                                }
                            }
                        },
                    ]
                }
            },
            '_history': [
                {
                    '_message': 'hello world',
                    '_timestamp': '2023-01-01T00:00:00+00:00',
                    '_source': ('test1', 'version1', 'instance1'),
                    '_message_type': 'I'
                },
                {
                    '_message': 'hello world2',
                    '_timestamp': '2023-01-02T00:00:00+00:00',
                    '_source': ('test1', 'version1', 'instance1'),
                    '_message_type': 'I'
                }
            ],
            '_qc_tests': [
                {
                    '_name': 'test1',
                    '_version': 'version1',
                    '_date': '2023-01-03T00:00:00+00:00',
                    '_messages': [
                        {
                            '_code': 'lat_fail',
                            '_path': 'a/b/c',
                            '_ref': 90
                        },
                        {
                            '_code': 'lon_fail',
                            '_path': 'a/b/d',
                            '_ref': -180
                        }
                    ],
                    '_result': 'F',
                    '_stale': False,
                    '_notes': 'hello world3'
                }
            ]
        })
        self.assertIn('M1', dr.metadata)
        self.assertEqual(dr.metadata['M1'], ocproc2.SingleElement('abc'))
        self.assertIn('M2', dr.metadata)
        self.assertEqual(dr.metadata['M2'], ocproc2.SingleElement(datetime.datetime(2023, 1, 1, 1, 2, 3)))
        self.assertIn('M3', dr.metadata)
        self.assertEqual(dr.metadata['M3'], ocproc2.SingleElement(datetime.date(2023, 12, 31)))
        self.assertIn('M4', dr.metadata)
        self.assertEqual(dr.metadata['M4'], ocproc2.SingleElement(123))
        self.assertIn('M5', dr.metadata)
        self.assertEqual(dr.metadata['M5'], ocproc2.SingleElement(''))
        self.assertIn('M6', dr.metadata)
        self.assertEqual(dr.metadata['M6'], ocproc2.SingleElement(None))
        self.assertIn('M7', dr.metadata)
        self.assertEqual(dr.metadata['M7'], ocproc2.SingleElement(123.34))
        self.assertIn('M8', dr.metadata)
        self.assertEqual(dr.metadata['M8'], ocproc2.SingleElement(True))
        self.assertIn('M9', dr.metadata)
        self.assertEqual(dr.metadata['M9'], ocproc2.SingleElement(False))
        self.assertIn('C1', dr.coordinates)
        self.assertEqual(dr.coordinates['C1'], ocproc2.SingleElement(123.45, Units="degree"))
        self.assertNotEqual(dr.coordinates['C1'], ocproc2.SingleElement(123.45))
        self.assertIn('C2', dr.coordinates)
        self.assertEqual(dr.coordinates['C2'], ocproc2.SingleElement(12.34, Units="degree"))
        self.assertIn('P1', dr.parameters)
        self.assertEqual(dr.parameters['P1'], ocproc2.SingleElement(12.34, Units="0.001", Uncertainty=0.01))
        self.assertIn('P2', dr.parameters)
        self.assertEqual(dr.parameters['P2'], ocproc2.SingleElement(5, Units='m s-1', Uncertainty=0.1, SensorHeight=ocproc2.SingleElement(1, Units='m')))
        self.assertNotEqual(dr.parameters['P2'], ocproc2.SingleElement(5, Units='m s-1', Uncertainty=0.1, SensorHeight=1))
        self.assertIn('PROFILE', dr.subrecords)
        self.assertIn(0, dr.subrecords['PROFILE'])
        self.assertEqual(5, len(dr.subrecords['PROFILE'][0].records))
        for idx, record in enumerate(dr.subrecords['PROFILE'][0].records):
            with self.subTest(subrecord_index=idx):
                self.assertIn('C3', record.coordinates)
                self.assertEqual(record.coordinates['C3'], ocproc2.SingleElement(10 + (10 * idx), Units="m", Uncertainty=5))
        self.assertEqual(2, len(dr.history))
        # TODO: check history in more depth
        self.assertEqual(1, len(dr.qc_tests))
        # TODO: check qc_tests in more depth




