import base64
import functools
import json

from cnodc.programs.erddap import ErddapController
from core import MockResponse
from processing.helpers import WorkerTestCase
from autoinject import injector
import zirconium as zr

RELOADED = []

def reload_dataset(method, url, reload_list, **kwargs):
    headers = kwargs.pop('headers', {})
    json_data: dict = kwargs.pop('json', None)
    auth_header = headers.pop('Authorization', '')
    if not auth_header.startswith('Basic '):
        return MockResponse(b'Unauthorized: no valid auth header', 403)
    try:
        un, pw = base64.b64decode(auth_header[6:]).decode('utf-8').split(':')
        if un != 'hello' or pw != 'world':
            raise ValueError('wrong un/pw')
    except Exception as ex:
        return MockResponse(f'Unauthorized: {str(ex)}'.encode('utf-8'), 403)
    if json_data is None:
        return MockResponse(b'malformed request', 400)
    if '_broadcast' in json_data and json_data['_broadcast'] not in (0, 1, 2, "0", "1", "2"):
        return MockResponse(b'malformed request', 400)
    if 'flag' in json_data and json_data['flag'] not in (0, 1, 2, "0", "1", "2"):
        return MockResponse(b'malformed request', 400)
    if 'dataset_id' in json_data:
        reload_list.append(json_data['dataset_id'])
    else:
        reload_list.append('__all__')
    return MockResponse(json.dumps({'success': True}).encode('utf-8'), 200)


class TestERDDAPWorkerAndConnection(WorkerTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.reloaded = []
        cls.web('http://test/api/datasets/reload', 'POST')(functools.partial(reload_dataset, reload_list=cls.reloaded))

    def setUp(self):
        super().setUp()
        self.reloaded.clear()

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'word')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    def test_good_config(self):
        with self.mock_web_test():
            control = ErddapController()
            self.assertTrue(control.reload_dataset('foo'))
            self.assertIn('foo', self.reloaded)



