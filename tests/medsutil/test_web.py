import functools

import requests

from medsutil.web import web_request
from tests.helpers.base_test_case import BaseTestCase
from tests.helpers.mock_requests import MockResponse


def raise_exception(*args, ex, **kwargs):
    raise ex


class TestWebRequest(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.web('http://test_success', 'GET')(lambda *args, **kwargs: MockResponse(b'good', 200))
        cls.web('http://test_server_failure', 'GET')(lambda *args, **kwargs: MockResponse(b'bad', 500))
        cls.web('http://test_connection_failure', 'GET')(functools.partial(raise_exception, ex=requests.ConnectTimeout()))
        cls.web('http://test_other_failure', 'GET')(functools.partial(raise_exception, ex=requests.URLRequired))

    def test_success(self):
        with self.mock_web_test():
            self.assertIsNotNone(web_request('GET', 'http://test_success'))

    def test_server_failure(self):
        with self.mock_web_test():
            with self.assertRaisesCNODCError('WEB-1001'):
                web_request('GET', 'http://test_server_failure')

    def test_other_failure(self):
        with self.mock_web_test():
            with self.assertRaisesCNODCError('WEB-1001'):
                web_request('GET', 'http://test_other_failure')

    def test_connection_failure(self):
        with self.mock_web_test():
            with self.assertRaisesCNODCError('WEB-1000'):
                web_request('GET', 'http://test_connection_failure')
