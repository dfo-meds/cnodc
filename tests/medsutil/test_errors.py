from medsutil.exceptions import CodedError, TransientCodedError
from tests.helpers.base_test_case import BaseTestCase


class TestCNODCError(BaseTestCase):

    def test_methods(self):
        error = CodedError("hello", 999, code_space='SPACE')
        self.assertEqual(error.internal_code, 'SPACE-999')
        self.assertFalse(error.is_transient)
        self.assertEqual(str(error), 'SPACE-999: hello')
        self.assertEqual(error.obfuscated_code(), 'SPACE-999')

    def test_transient_error(self):
        error = TransientCodedError("hello", 998, code_space='SPACE')
        self.assertEqual(error.internal_code, 'SPACE-998')
        self.assertTrue(error.is_transient)
