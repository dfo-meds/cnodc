from contextlib import contextmanager

from pipeman.programs.qc.base import QCAssertionError, QCSkipReview, QCSkipTest
from tests.helpers.base_test_case import BaseTestCase


class QCCheckerTestCase(BaseTestCase):

    @contextmanager
    def assertPassesQC(self):
        try:
            yield {}
        except QCAssertionError as ex:
            raise self.failureException(f"Test unexpectedly failed: {ex.error_code}") from ex

    @contextmanager
    def assertFailsQC(self, error_code: str | None = None):
        with self.assertRaises(QCAssertionError) as h:
            yield h
        if error_code is not None:
            self.assertEqual(error_code, h.exception.error_code)

    @contextmanager
    def assertSkipsReview(self):
        with self.assertRaises(QCSkipReview) as h:
            yield h
        pass

    @contextmanager
    def assertSkipsTest(self):
        with self.assertRaises(QCSkipTest) as h:
            yield h
        pass
