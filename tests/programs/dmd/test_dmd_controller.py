import functools
import json

from autoinject import injector

from pipeman.programs.dmd.dmd import DataManagerController
from pipeman.programs.dmd.metadata import DatasetMetadata
from tests.helpers.base_test_case import BaseTestCase
import zirconium as zr

from tests.helpers.mock_requests import MockResponse


def with_security(cb):
    @functools.wraps(cb)
    def _inner(method, url, **kwargs):
        h = kwargs.pop('headers', {})
        if 'Authorization' not in h:
            return MockResponse(b"Forbidden", 403)
        if h['Authorization'] != '12345':
            return MockResponse(b"Forbidden", 403)
        return cb(method, url, **kwargs)
    return _inner

@with_security
def upsert_dataset(method, url, **kwargs):
    return json.dumps({'guid': '23456'})

@with_security
def create_dataset(method, url, **kwargs):
    return json.dumps({'guid': '34567'})


class TestDataManagerConnection(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.web('http://test/api/upsert-dataset', 'POST')(upsert_dataset)
        cls.web('http://test/api/create-dataset', 'POST')(create_dataset)

    @injector.test_case
    @zr.test_with_config(('dmd', 'base_url'), 'http://test/')
    def test_dmd_bad_auth_post(self):
        x = DataManagerController()
        with self.mock_web_test():
            with self.assertRaisesCNODCError('WEB-1001'):
                x.upsert_dataset(DatasetMetadata())

    @injector.test_case
    @zr.test_with_config(('dmd', 'auth_token'), '12345')
    def test_dmd_good_auth_post_bad_url(self):
        x = DataManagerController()
        with self.mock_web_test():
            with self.assertRaisesCNODCError('WEB-1001'):
                x.upsert_dataset(DatasetMetadata())

    @injector.test_case
    @zr.test_with_config(('dmd', 'auth_token'), '12345')
    @zr.test_with_config(('dmd', 'base_url'), 'http://test/')
    def test_dmd_good_auth_post_url(self):
        x = DataManagerController()
        with self.mock_web_test():
            self.assertEqual(x.upsert_dataset(DatasetMetadata()), '23456')

    @injector.test_case
    @zr.test_with_config(('dmd', 'base_url'), 'http://test/')
    def test_dmd_bad_auth_post_create(self):
        x = DataManagerController()
        with self.mock_web_test():
            with self.assertRaisesCNODCError('WEB-1001'):
                x.create_dataset(DatasetMetadata())

    @injector.test_case
    @zr.test_with_config(('dmd', 'auth_token'), '12345')
    def test_dmd_good_auth_post_bad_url_create(self):
        x = DataManagerController()
        with self.mock_web_test():
            with self.assertRaisesCNODCError('WEB-1001'):
                x.create_dataset(DatasetMetadata())

    @injector.test_case
    @zr.test_with_config(('dmd', 'auth_token'), '12345')
    @zr.test_with_config(('dmd', 'base_url'), 'http://test/')
    def test_dmd_good_auth_post_url_create(self):
        x = DataManagerController()
        with self.mock_web_test():
            self.assertEqual(x.create_dataset(DatasetMetadata()), '34567')
