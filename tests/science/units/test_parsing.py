import itertools
import unittest as ut

import cnodc.science.units.structures as uns
import cnodc.science.units.parsing as unp
import cnodc.science.units.units as un

from cnodc.util import CNODCError


class TestUnitParsing(ut.TestCase):

    SIMPLE_TESTS = [
        ("10e-9", uns.Real("10e-9")),
        ("m", uns.SimpleUnit("m")),
        ("m2", uns.Exponent(uns.SimpleUnit("m"), uns.Integer("2"))),
        ("m^2", uns.Exponent(uns.SimpleUnit("m"), uns.Integer("2"))),
        ("m**2", uns.Exponent(uns.SimpleUnit("m"), uns.Integer("2"))),
        ("m-2", uns.Exponent(uns.SimpleUnit("m"), uns.Integer("-2"))),
        ("m^-2", uns.Exponent(uns.SimpleUnit("m"), uns.Integer("-2"))),
        ("m**-2", uns.Exponent(uns.SimpleUnit("m"), uns.Integer("-2"))),
        ("kg m", uns.Product(uns.SimpleUnit("kg"), uns.SimpleUnit("m"))),
        ("kg.m", uns.Product(uns.SimpleUnit("kg"), uns.SimpleUnit("m"))),
        ("kg*m", uns.Product(uns.SimpleUnit("kg"), uns.SimpleUnit("m"))),
        ("kg/m", uns.Product(uns.SimpleUnit("kg"), uns.Exponent(
            uns.SimpleUnit("m"), uns.Integer("-1")))),
        ("kg per m", uns.Product(uns.SimpleUnit("kg"), uns.Exponent(
            uns.SimpleUnit("m"), uns.Integer("-1")))),
        ("kg PER m", uns.Product(uns.SimpleUnit("kg"), uns.Exponent(
            uns.SimpleUnit("m"), uns.Integer("-1")))),
        ("kgperm", uns.Product(uns.SimpleUnit("kg"), uns.Exponent(
            uns.SimpleUnit("m"), uns.Integer("-1")))),
        ("m / s2", uns.Product(uns.SimpleUnit("m"), uns.Exponent(
            uns.SimpleUnit("s"), uns.Integer("-2")))),
        ("kg s2 m", uns.Product(
            uns.Product(uns.SimpleUnit("kg"), uns.Exponent(
                uns.SimpleUnit("s"), uns.Integer("2"))), uns.SimpleUnit("m"))),
        ("kg / m / s2", uns.Product(
            uns.Product(uns.SimpleUnit("kg"), uns.Exponent(
                uns.SimpleUnit("m"), uns.Integer("-1"))), uns.Exponent(
                uns.SimpleUnit("s"), uns.Integer("-2")))),
    ]

    COMPLEX_TESTS = [
        ("K @ 273.15", uns.Offset(uns.SimpleUnit("K"), uns.Real("273.15"))),
        ("K ref 273.15", uns.Offset(uns.SimpleUnit("K"), uns.Real("273.15"))),
        ("60 sec", uns.Product(uns.Integer("60"), uns.SimpleUnit("sec"))),
    ]

    def test_simple_units(self):
        for test, result in TestUnitParsing.SIMPLE_TESTS:
            with self.subTest(input=test):
                self.assertEqual(un.parse_unit_string(test), result)

    def test_complex_units(self):
        for test, result in TestUnitParsing.COMPLEX_TESTS:
            with self.subTest(input=test):
                self.assertEqual(un.parse_unit_string(test), result)


class TestLowLevel(ut.TestCase):
    INTEGERS = ["+0", "-1", "0", "1234567"]
    DECIMALS = ["3.0", "-3.0", "3.7", "+3.7"]
    LITERALS = ['+3E7', '-4.21E9', '+3.3E-12', '3.4e9', '-2.31e-12']
    NOT_NUMBERS = ["foo", "hello", "bar", "3.4.1", "+-8", "", "2.3E2.1", "-2.3e2.7"]

    def test_is_integer(self):
        for case in itertools.chain(TestLowLevel.NOT_NUMBERS, TestLowLevel.DECIMALS):
            with self.subTest(case=case):
                self.assertFalse(unp._is_integer_number(case))
        for case in TestLowLevel.INTEGERS:
            with self.subTest(case=case):
                self.assertTrue(unp._is_integer_number(case))

    def test_is_decimal(self):
        for case in itertools.chain(TestLowLevel.INTEGERS, TestLowLevel.DECIMALS):
            with self.subTest(case=case):
                self.assertTrue(unp._is_decimal_number(case))
        for case in TestLowLevel.NOT_NUMBERS:
            with self.subTest(case=case):
                self.assertFalse(unp._is_decimal_number(case))

    def test_is_literal(self):
        for case in itertools.chain(TestLowLevel.INTEGERS, TestLowLevel.DECIMALS, TestLowLevel.LITERALS):
            with self.subTest(case=case):
                self.assertTrue(unp._is_literal(case))
        for case in TestLowLevel.NOT_NUMBERS:
            with self.subTest(case=case):
                self.assertFalse(unp._is_decimal_number(case))

    def test_parse_literal(self):
        for case in TestLowLevel.INTEGERS:
            with self.subTest(case=case):
                self.assertIsInstance(unp._parse_literal(case), uns.Integer)
        for case in itertools.chain(TestLowLevel.DECIMALS, TestLowLevel.LITERALS):
            with self.subTest(case=case):
                self.assertIsInstance(unp._parse_literal(case), uns.Real)

    def test_extract_leading_int(self):
        self.assertEqual(unp._extract_leading_int('1234 kg'), ('1234', ' kg'))
        self.assertEqual(unp._extract_leading_int('+1234 kg'), ('+1234', ' kg'))
        self.assertEqual(unp._extract_leading_int('-1234 kg'), ('-1234', ' kg'))
        self.assertEqual(unp._extract_leading_int('1234+567 kg'), ('1234', '+567 kg'))
        self.assertEqual(unp._extract_leading_int('1234-567 k'), ('1234', '-567 k'))
        self.assertEqual(unp._extract_leading_int('1234-567k'), ('1234', '-567k'))
        self.assertEqual(unp._extract_leading_int('test'), ('', 'test'))
        self.assertEqual(unp._extract_leading_int('⁰0 kg'), ('00', ' kg'))
        self.assertEqual(unp._extract_leading_int('⁺⁹ kg'), ('+9', ' kg'))

    def test_bad_unit_components(self):
        tests = [
            '', None, 'kg ** 2.3', 'kg **', 'kg**', 'kg**+', 'kg ** -', 'kg ^ 2.3', 'kg ^', 'kg ^ +', 'kg^-', 'kg+',
            'kg-'
        ]
        for test in tests:
            with self.subTest(input=test):
                with self.assertRaises(CNODCError):
                    unp._parse_unit_for_exponents_and_leading(test)

    def test_simple_unit_component(self):
        u = unp._parse_unit_for_exponents_and_leading('kg')
        self.assertIsInstance(u, uns.SimpleUnit)
        self.assertEqual(u.name, 'kg')

    def test_literal_components(self):
        for case in TestLowLevel.INTEGERS:
            with self.subTest(case=case):
                self.assertEqual(unp._parse_unit_for_exponents_and_leading(case), uns.Integer(case))
        for case in itertools.chain(TestLowLevel.LITERALS, TestLowLevel.DECIMALS):
            with self.subTest(case=case):
                self.assertEqual(unp._parse_unit_for_exponents_and_leading(case), uns.Real(case))

    def test_exponent_component(self):
        tests = [
            ('kg ** 2', 'kg', '2'),
            ('kg^2', 'kg', '2'),
            ('kg2', 'kg', '2'),
            ('kg²', 'kg', '2'),
        ]
        for in_value, base_unit, exponent in tests:
            with self.subTest(input=in_value):
                x = unp._parse_unit_for_exponents_and_leading(in_value)
                self.assertIsInstance(x, uns.Exponent)
                self.assertEqual(x.exponent, uns.Integer(exponent))
                self.assertIsInstance(x.base, uns.SimpleUnit)
                self.assertEqual(x.base.name, base_unit)
        tests = [
            ('kg', 'kg'),
            ('kg1', 'kg'),
            ('kg^1', 'kg'),
            ('kg ** 1', 'kg')
        ]
        for in_value, base_unit in tests:
            with self.subTest(input=in_value):
                x = unp._parse_unit_for_exponents_and_leading(in_value)
                self.assertIsInstance(x, uns.SimpleUnit)
                self.assertEqual(x.name, base_unit)

    def test_decompose_unit_string(self):
        tests = [
            ('kg', ['kg']),
            ('kg ', ['kg']),
            ('kg  ', ['kg']),
            ('kg2', ['kg2']),
            ('kg**2', ['kg^2']),
            ('kg**⁹', ['kg^⁹']),
            ('kg^2', ['kg^2']),
            ('kg m', ['kg', 'm']),
            ('kg2 m', ['kg2', 'm']),
            ('kg m s-1', ['kg', 'm', 's-1']),
            ('kg m-2 s', ['kg', 'm-2', 's']),
            ('kg * m', ['kg', '*', 'm']),
            ('kg*m', ['kg', '*', 'm']),
            ('kg. m', ['kg', '.', 'm']),
            ('kg ·m', ['kg', '·', 'm']),
            ('kg / m', ['kg', '/', 'm']),
            ('kg per m', ['kg', 'per', 'm']),
            ('kg PER m', ['kg', 'PER', 'm']),

        ]
        for in_value, output in tests:
            with self.subTest(input=in_value):
                self.assertEqual(unp._decompose_simple_unit_string(in_value), output)

    def test_parse_simple_unit_string(self):
        for test, result in TestUnitParsing.SIMPLE_TESTS:
            with self.subTest(input=test):
                self.assertEqual(unp._parse_simple_unit_string(test), result)

    def test_decompose_bracket_pairs(self):
        tests = [
            ("(kg)", ["kg"]),
            ("kg m s-1", ["kg m s-1"]),
            ("kg (m s)^-1", ["kg", "m s", "^-1"]),
            ("(kg (m s)^-1) J", ["kg (m s)^-1", "J"])
        ]
        for test, result in tests:
            with self.subTest(input=test):
                self.assertEqual(unp._decompose_bracket_pairs(test), result)
        with self.assertRaises(CNODCError):
            unp._decompose_bracket_pairs("(kg")
        with self.assertRaises(CNODCError):
            unp._decompose_bracket_pairs("kg)")

    def test_parse_logs(self):
        tests = [
            ("log(re 1 V)", [uns.Log(uns.Product(uns.Integer("1"), uns.SimpleUnit("V")), uns.Integer("10"))]),
            ("lg(re 1 V)", [uns.Log(uns.Product(uns.Integer("1"), uns.SimpleUnit("V")), uns.Integer("10"))]),
            ("ln(re 1 V)", [uns.Log(uns.Product(uns.Integer("1"), uns.SimpleUnit("V")), uns.Real("e"))]),
            ("lb(re 1 V)", [uns.Log(uns.Product(uns.Integer("1"), uns.SimpleUnit("V")), uns.Integer("2"))]),
            ("log (re 1 V)", [uns.Log(uns.Product(uns.Integer("1"), uns.SimpleUnit("V")), uns.Integer("10"))]),
            ("log (re: 1 V)", [uns.Log(uns.Product(uns.Integer("1"), uns.SimpleUnit("V")), uns.Integer("10"))]),
            ("log(re1 V)", [uns.Log(uns.Product(uns.Integer("1"), uns.SimpleUnit("V")), uns.Integer("10"))]),
            ("log(re:1 V)", [uns.Log(uns.Product(uns.Integer("1"), uns.SimpleUnit("V")), uns.Integer("10"))]),
            ("log(1 V)", [uns.Log(uns.Product(uns.Integer("1"), uns.SimpleUnit("V")), uns.Integer("10"))]),
        ]
        for test, result in tests:
            with self.subTest(input=test):
                self.assertEqual(unp._parse_logs(unp._decompose_bracket_pairs(test)), result)

    def test_parse_leading_exponents(self):
        tests = [
            (["kg", "^2"], ["kg", "^2"]),
            (["kg", "^ 2"], ["kg", "^2"]),
            (["kg", "^ 2 kg"], ["kg", "^2", "kg"]),
            ([uns.SimpleUnit("kg"), "** 2 kg"], [uns.SimpleUnit("kg"), "^2", "kg"]),
            (["kg", "² m"], ["kg", "^2", "m"]),
            (["kg", "m", "B"], ["kg", "m", "B"]),
        ]
        for test, result in tests:
            with self.subTest(input=test):
                self.assertEqual(unp._parse_leading_exponents(test), result)

    def test_parse_with_groups(self):
        tests = [
            ("log(1 V) kg m", uns.Product(uns.Log(uns.Product(uns.Integer("1"), uns.SimpleUnit("V")), uns.Integer("10")), uns.Product(uns.SimpleUnit("kg"), uns.SimpleUnit("m")))),
            ("(kg m) / (s N)", uns.Quotient(uns.Product(uns.SimpleUnit("kg"), uns.SimpleUnit("m")), uns.Product(uns.SimpleUnit("s"), uns.SimpleUnit("N"))))
        ]
        for test, result in tests:
            with self.subTest(input=test):
                self.assertEqual(unp._parse_unit_for_groups(test), result)

    def test_parse_unit_string_with_shift(self):
        tests = [
            ("K @ 273.15", uns.Offset(uns.SimpleUnit("K"), uns.Real("273.15"))),
            ("s after 30", uns.Offset(uns.SimpleUnit("s"), uns.Integer("30"))),
            ("s from 30", uns.Offset(uns.SimpleUnit("s"), uns.Integer("30"))),
            ("s since 30", uns.Offset(uns.SimpleUnit("s"), uns.Integer("30"))),
            ("s ref 99", uns.Offset(uns.SimpleUnit("s"), uns.Integer("99"))),
            ("kg refrigeration_ton m", uns.Product(uns.Product(uns.SimpleUnit("kg"), uns.SimpleUnit("refrigeration_ton")), uns.SimpleUnit("m")))
        ]
        for test, result in tests:
            with self.subTest(input=test):
                self.assertEqual(unp._parse_unit_string_with_shift(test), result)