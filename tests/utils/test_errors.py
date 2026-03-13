from cnodc.util import CNODCError, ConfigError
from helpers.base_test_case import BaseTestCase


class TestCNODCError(BaseTestCase):

    def test_methods(self):
        error = CNODCError("hello", "SPACE", 999, True)
        self.assertEqual(error.internal_code, 'SPACE-999')
        self.assertTrue(error.is_recoverable)
        self.assertEqual(str(error), 'SPACE-999: hello')
        self.assertEqual(error.obfuscated_code(), 'SPACE-999')

    def test_config_error(self):
        x = ConfigError("stuff", code_number=5)
        self.assertFalse(x.is_recoverable)
        self.assertEqual(x._msg, "Missing configuration key [stuff]")
