from cnodc.util import CNODCError, ConfigError
from cnodc.util.exceptions import TransientError
from helpers.base_test_case import BaseTestCase


class TestCNODCError(BaseTestCase):

    def test_methods(self):
        error = CNODCError("hello", "SPACE", 999)
        self.assertEqual(error.internal_code, 'SPACE-999')
        self.assertFalse(error.is_transient)
        self.assertEqual(str(error), 'SPACE-999: hello')
        self.assertEqual(error.obfuscated_code(), 'SPACE-999')

    def test_transient_error(self):
        error = TransientError("hello", "SPACE", 998)
        self.assertEqual(error.internal_code, 'SPACE-998')
        self.assertTrue(error.is_transient)

    def test_config_error(self):
        x = ConfigError("stuff", code_number=5)
        self.assertFalse(x.is_transient)
        self.assertEqual(str(x), "CONFIG-5: Missing configuration key [stuff]")
