import datetime
import unittest as ut
import medsutil.ocproc2 as ocproc2
import typing as t


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
            sr = ocproc2.ChildRecord()
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
                    '_source': ['test1', 'version1', 'instance1'],
                    '_message_type': 'I'
                },
                {
                    '_message': 'hello world2',
                    '_timestamp': '2023-01-02T00:00:00+00:00',
                    '_source': ['test1', 'version1', 'instance1'],
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



