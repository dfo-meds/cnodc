from cnodc.util import CNODCError
from core import BaseTestCase

class TestCNODCError(BaseTestCase):

    def test_methods(self):
        error = CNODCError("hello", "SPACE", 999, True)
        self.assertEqual(error.internal_code, 'SPACE-999')
        self.assertTrue(error.is_recoverable)
        self.assertEqual(str(error), 'SPACE-999: hello')
        self.assertEqual(error.obfuscated_code(), 'SPACE-999')
