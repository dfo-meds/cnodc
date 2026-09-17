import decimal

from tests.helpers.base_test_case import BaseTestCase
from medsutil.math import _constants as constants

class TestConstants(BaseTestCase):

    def test_tau(self):
        self.assertIsInstance(constants.tau(), float)
        self.assertIsInstance(constants.tau(float), float)
        self.assertIsInstance(constants.tau(decimal.Decimal), decimal.Decimal)

    def test_inf(self):
        self.assertIsInstance(constants.inf(), float)
        self.assertIsInstance(constants.inf(float), float)
        self.assertIsInstance(constants.inf(decimal.Decimal), decimal.Decimal)

    def test_e(self):
        self.assertIsInstance(constants.e(), float)
        self.assertIsInstance(constants.e(float), float)
        self.assertIsInstance(constants.e(decimal.Decimal), decimal.Decimal)

    def test_pi(self):
        self.assertIsInstance(constants.pi(), float)
        self.assertIsInstance(constants.pi(float), float)
        self.assertIsInstance(constants.pi(decimal.Decimal), decimal.Decimal)

    def test_epsilon(self):
        self.assertIsInstance(constants.epsilon(), float)
        self.assertIsInstance(constants.epsilon(float), float)
        self.assertIsInstance(constants.epsilon(decimal.Decimal), decimal.Decimal)
