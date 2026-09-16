from yaml import emit

from medsutil.exceptions import CodedError, TransientCodedError, exception_kwargs_for_email, with_exception_note
from tests.helpers.base_test_case import BaseTestCase


class TestCodedError(BaseTestCase):

    def test_methods(self):
        error = CodedError("hello", 999, code_space='SPACE')
        self.assertEqual(error.internal_code, 'SPACE-999')
        self.assertFalse(error.is_transient)
        self.assertEqual(str(error), 'SPACE-999: hello')
        self.assertEqual(error.obfuscated_code(), 'SPACE-999')
        self.assertEqual(error.pretty(), "CodedError: SPACE-999: hello")

    def test_transient_error(self):
        error = TransientCodedError("hello", 998, code_space='SPACE')
        self.assertEqual(error.internal_code, 'SPACE-998')
        self.assertTrue(error.is_transient)

    def test_no_code_space_error(self):
        error = CodedError("hello", 998)
        self.assertEqual(error.internal_code, 'UNKNOWN-998')

    def test_kwargs_for_email(self):
        ex = CodedError("hello", 989)
        try:
            raise ex
        except Exception as e:
            email_kwargs = exception_kwargs_for_email(e)
            self.assertIsInstance(email_kwargs, dict)
            self.assertIn("error_description", email_kwargs)
            self.assertIn("error_details", email_kwargs)
            self.assertIsInstance(email_kwargs["error_details"], str)
            self.assertIsInstance(email_kwargs["error_description"], str)

    def test_with_note(self):

        @with_exception_note("foobar")
        def _raise():
            raise CodedError("hello", 999)

        try:
            _raise()
        except Exception as e:
            self.assertIsInstance(e, CodedError)
            self.assertEqual(len(e.__notes__), 1)
            self.assertEqual(e.__notes__[0], "foobar")
