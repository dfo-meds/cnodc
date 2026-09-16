from medsutil.first import first
from tests.helpers.base_test_case import BaseTestCase


class TestFirst(BaseTestCase):

    def test_with_skip(self):
        self.assertEqual(first('', None, 'foo'), 'foo')

    def test_with_no_skip(self):
        self.assertEqual(first(None, '', 'foo', skip=None), '')
