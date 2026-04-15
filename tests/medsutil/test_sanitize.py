import datetime
import decimal
import math

import numpy as np

from medsutil.sanitize import utf_normalize_string, unnumpy, coerce
from tests.helpers.base_test_case import BaseTestCase
import medsutil.sanitize as sanitize


class TestDataSanitization(BaseTestCase):

    def test_json_cleaning(self):
        tests = [
            (datetime.date(2015, 1, 2), '2015-01-02'),
            (datetime.datetime(2015, 1, 2, 3, 4, 5), '2015-01-02T03:04:05'),
            ('abc', 'abc'),
            (5, 5),
            (4.5, 4.5),
            ([datetime.date(2015, 1, 2), 'foo', 5], ['2015-01-02', 'foo', 5]),
            ({
                'date': datetime.date(2015, 1, 2)
            }, {
                'date': '2015-01-02'
            }),
            ({
                'dates': [datetime.date(2015, 1, 2)]
            }, {
                'dates': ['2015-01-02']
            }),
            ([
                {
                    'dates': [datetime.date(2015, 1, 2)]
                }
            ], [
                {
                    'dates': ['2015-01-02'],
                }
            ])
        ]
        for in_value, expected in tests:
            with self.subTest(input=in_value, expected=expected):
                self.assertEqual(expected, coerce.as_json_safe(in_value))

    def test_nfc_normalization(self):
        tests = [
            ('\u212B', '\u00C5'),
            ('\u0041\u030A', '\u00C5'),
            ('a\x01b', 'ab'),
            ('abc', 'abc'),
            ("a\nb", "a\nb"),
            ("a\r\nb", "a\nb"),
            ("a\x00b", "ab"),
            ("a  b", "a b"),
            ("a b ", "a b"),
            (" a b", "a b"),
            (" a  b    ", "a b"),
        ]
        for c in sanitize.UNICODE_SPACES:
            tests.append((f"a{c}b", "a b"))
        for c in sanitize.UNICODE_DASHES:
            tests.append((f"a{c}b", "a-b"))
        for in_value, expected in tests:
            with self.subTest(input=in_value, expected=expected):
                self.assertEqual(expected, utf_normalize_string(in_value))

    def test_unnumpy(self):
        tests = [
            (None, None, None),
            (np.float64(5.4), 5.4, float),
            (np.int64(5), 5, int),
            (np.array([1, 2, 3, 4, 5]), [1, 2, 3, 4, 5], list),
            (np.array([1.1, 2.2, 3.3, 4.4, 5.5]), [1.1, 2.2, 3.3, 4.4, 5.5], list),
            (np.array(5), 5, int),
            (np.array(5.5), 5.5, float),
            (np.array(5).item(), 5, int),
            (np.array(5.5).item(), 5.5, float),
            (5, 5, int),
            (5.5, 5.5, float),
            ('hello', 'hello', str),
            (decimal.Decimal("5.5"), decimal.Decimal("5.5"), decimal.Decimal),
            (np.array(math.nan).item(), None, None),
            ([1, 2], [1, 2], list),
        ]
        for in_value, expected, exact_cls in tests:
            with self.subTest(input=in_value, expected=expected):
                result = unnumpy(in_value)
                self.assertEqual(expected, result)
                if exact_cls is not None:
                    self.assertIsInstance(result, exact_cls)
