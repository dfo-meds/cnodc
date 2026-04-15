import base64
import functools
import json

from nodb import NODBQueueItem
from pipeman.programs.erddap import ErddapController, ReloadFlag
from pipeman.programs.erddap.reloader import ERDDAPReloadWorker
from tests.helpers.mock_requests import MockResponse
from tests.helpers.base_test_case import BaseTestCase
from autoinject import injector
import zirconium as zr

RELOADED = []

def reload_dataset(method, url, reload_list, username, password, **kwargs):
    headers = kwargs.pop('headers', {})
    json_data: dict = kwargs.pop('json', None)
    auth_header = headers.pop('Authorization', '')
    if not auth_header.startswith('Basic '):
        return MockResponse(b'Unauthorized: no valid auth header', 403)
    try:
        un, pw = base64.b64decode(auth_header[6:]).decode('utf-8').split(':')
        if un != username or pw != password:
            raise ValueError('wrong un/pw')
    except Exception as ex:
        return MockResponse(f'Unauthorized: {str(ex)}'.encode('utf-8'), 403)
    if json_data is None:
        return MockResponse(b'malformed request', 400)
    if '_broadcast' in json_data and json_data['_broadcast'] not in (0, 1, 2, "0", "1", "2"):
        return MockResponse(b'malformed request', 400)
    if 'flag' in json_data and json_data['flag'] not in (0, 1, 2, "0", "1", "2"):
        return MockResponse(b'malformed request', 400)
    if 'dataset_id' in json_data and json_data['dataset_id'] == 'force_false':
        return MockResponse(json.dumps({'success': False}).encode('utf-8'), 200)
    reload_list.append((
        str(json_data['dataset_id']) if 'dataset_id' in json_data else '__all__',
        int(json_data['_broadcast']) if '_broadcast' in json_data else 0,
        int(json_data['flag']) if 'flag' in json_data else 0)
    )
    return MockResponse(json.dumps({'success': True}).encode('utf-8'), 200)


class TestERDDAPWorkerAndConnection(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.reloaded = []
        cls.reloaded2 = []
        cls.web('http://test/api/datasets/reload', 'POST')(functools.partial(reload_dataset, reload_list=cls.reloaded, username='hello', password='world'))
        cls.web('http://test2/api/datasets/reload', 'POST')(functools.partial(reload_dataset, reload_list=cls.reloaded2, username='foo', password='bar'))

    def setUp(self):
        super().setUp()
        self.reloaded.clear()
        self.reloaded2.clear()

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    def test_good_config(self):
        with self.mock_web_test():
            control = ErddapController()
            self.assertTrue(control.reload_dataset('foo'))
            self.assertIn(('foo', 0, 0), self.reloaded)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    @zr.test_with_config(('erddaputil', 'broadcast_mode'), 'cluster')
    def test_good_config_broadcast_cluster(self):
        with self.mock_web_test():
            control = ErddapController()
            self.assertTrue(control.reload_dataset('foo'))
            self.assertIn(('foo', 1, 0), self.reloaded)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    @zr.test_with_config(('erddaputil', 'broadcast_mode'), 'global')
    def test_good_config_broadcast_global(self):
        with self.mock_web_test():
            control = ErddapController()
            self.assertTrue(control.reload_dataset('foo'))
            self.assertIn(('foo', 2, 0), self.reloaded)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    def test_good_config_hard_flag(self):
        with self.mock_web_test():
            control = ErddapController()
            self.assertTrue(control.reload_dataset('foo', flag=ReloadFlag.HARD))
            self.assertIn(('foo', 0, 2), self.reloaded)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    def test_good_config_bad_files_flag(self):
        with self.mock_web_test():
            control = ErddapController()
            self.assertTrue(control.reload_dataset('foo', flag=ReloadFlag.BAD_FILES))
            self.assertIn(('foo', 0, 1), self.reloaded)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'foobar')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    def test_bad_password(self):
        with self.mock_web_test():
            control = ErddapController()
            with self.assertRaisesCNODCError('WEB-1001'):
                control.reload_dataset('foo')
            self.assertNotIn(('foo', 0, 0), self.reloaded)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    def test_bad_password(self):
        with self.mock_web_test():
            control = ErddapController()
            with self.assertRaisesCNODCError('ERDDAPUTIL-1001'):
                control.reload_dataset('force_false')
            self.assertNotIn(('foo', 0, 0), self.reloaded)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    def test_bad_config_no_username(self):
        with self.mock_web_test():
            control = ErddapController()
            with self.assertLogs('cnodc.erddap', 'ERROR'):
                with self.assertRaisesCNODCError('ERDDAPUTIL-1000'):
                    control.reload_dataset('foo')
            self.assertEqual(0, len(self.reloaded))

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    def test_bad_config_no_password(self):
        with self.mock_web_test():
            control = ErddapController()
            with self.assertLogs('cnodc.erddap', 'ERROR'):
                with self.assertRaisesCNODCError('ERDDAPUTIL-1000'):
                    control.reload_dataset('foo')
            self.assertEqual(0, len(self.reloaded))

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    def test_bad_config_no_base_url(self):
        with self.mock_web_test():
            control = ErddapController()
            with self.assertLogs('cnodc.erddap', 'ERROR'):
                with self.assertRaisesCNODCError('ERDDAPUTIL-1000'):
                    control.reload_dataset('foo')
            self.assertEqual(0, len(self.reloaded))

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 12345)
    def test_bad_base_url_not_str(self):
        with self.mock_web_test():
            control = ErddapController()
            with self.assertLogs('cnodc.erddap', 'ERROR'):
                with self.assertRaisesCNODCError('ERDDAPUTIL-1000'):
                    control.reload_dataset('foo')
            self.assertEqual(0, len(self.reloaded))

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    @zr.test_with_config(('erddaputil', 'broadcast_mode'), 'foo')
    def test_bad_broadcast_mode(self):
        with self.mock_web_test():
            control = ErddapController()
            with self.assertLogs('cnodc.erddap', 'WARNING'):
                control.reload_dataset('foo')
            self.assertIn(('foo', 0, 0), self.reloaded)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'foobar')
    @zr.test_with_config(('erddaputil', 'base_url'), 'ftp://erddap/')
    def test_bad_cluster(self):
        with self.mock_web_test():
            control = ErddapController()
            with self.assertLogs('cnodc.erddap', 'ERROR'):
                with self.assertRaisesCNODCError('ERDDAPUTIL-1000'):
                    control.reload_dataset('foo')
            self.assertEqual(0, len(self.reloaded))

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/afoo')
    def test_bad_api_path(self):
        with self.mock_web_test():
            control = ErddapController()
            with self.assertRaisesCNODCError('WEB-1001'):
                control.reload_dataset('foo')
            self.assertEqual(0, len(self.reloaded))

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    def test_bad_config_no_cluster_defined(self):
        with self.mock_web_test():
            control = ErddapController()
            with self.assertLogs('cnodc.erddap', 'ERROR'):
                with self.assertRaisesCNODCError('ERDDAPUTIL-1000'):
                    control.reload_dataset('foo', cluster_name='hello')
            self.assertEqual(0, len(self.reloaded))

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    @zr.test_with_config(('erddaputil', 'cluster1', 'username'), 'foo')
    @zr.test_with_config(('erddaputil', 'cluster1', 'password'), 'bar')
    @zr.test_with_config(('erddaputil', 'cluster1', 'base_url'), 'http://test2/api/')
    def test_multiple_clusters(self):
        with self.mock_web_test():
            control = ErddapController()
            control.reload_dataset('foo')
            self.assertIn(('foo', 0, 0), self.reloaded)
            self.assertNotIn(('foo', 0, 0), self.reloaded2)
            control.reload_dataset('foo2', cluster_name='cluster1')
            self.assertIn(('foo2', 0, 0), self.reloaded2)
            self.assertNotIn(('foo2', 0, 0), self.reloaded)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    def test_erddap_reload_worker(self):
        with self.mock_web_test():
            qi = NODBQueueItem()
            qi.data = {
                'dataset_id': 'foo'
            }
            self.worker_controller.test_queue_worker(
                ERDDAPReloadWorker,
                {},
                qi
            )
            self.assertIn(('foo', 0, 0), self.reloaded)
            self.assertNotIn(('foo', 0, 0), self.reloaded2)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    @zr.test_with_config(('erddaputil', 'cluster1', 'username'), 'foo')
    @zr.test_with_config(('erddaputil', 'cluster1', 'password'), 'bar')
    @zr.test_with_config(('erddaputil', 'cluster1', 'base_url'), 'http://test2/api/')
    def test_erddap_reload_worker_with_cluster(self):
        with self.mock_web_test():
            qi = NODBQueueItem()
            qi.data = {
                'dataset_id': 'foo',
                'cluster_name': 'cluster1'
            }
            self.worker_controller.test_queue_worker(
                ERDDAPReloadWorker,
                {},
                qi
            )
            self.assertIn(('foo', 0, 0), self.reloaded2)
            self.assertNotIn(('foo', 0, 0), self.reloaded)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    @zr.test_with_config(('erddaputil', 'cluster1', 'username'), 'foo')
    @zr.test_with_config(('erddaputil', 'cluster1', 'password'), 'bar')
    @zr.test_with_config(('erddaputil', 'cluster1', 'base_url'), 'http://test2/api/')
    def test_erddap_reload_worker_with_default_cluster(self):
        with self.mock_web_test():
            qi = NODBQueueItem()
            qi.data = {
                'dataset_id': 'foo',
            }
            self.worker_controller.test_queue_worker(
                ERDDAPReloadWorker,
                {
                    'default_cluster': 'cluster1'
                },
                qi
            )
            self.assertIn(('foo', 0, 0), self.reloaded2)
            self.assertNotIn(('foo', 0, 0), self.reloaded)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    @zr.test_with_config(('erddaputil', 'cluster1', 'username'), 'foo')
    @zr.test_with_config(('erddaputil', 'cluster1', 'password'), 'bar')
    @zr.test_with_config(('erddaputil', 'cluster1', 'base_url'), 'http://test2/api/')
    def test_erddap_reload_worker_with_default_cluster_override(self):
        with self.mock_web_test():
            qi = NODBQueueItem()
            qi.data = {
                'dataset_id': 'foo',
                'cluster_name': '__default',
            }
            self.worker_controller.test_queue_worker(
                ERDDAPReloadWorker,
                {
                    'default_cluster': 'cluster1'
                },
                qi
            )
            self.assertIn(('foo', 0, 0), self.reloaded)
            self.assertNotIn(('foo', 0, 0), self.reloaded2)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    def test_erddap_reload_worker_with_hard_flag(self):
        with self.mock_web_test():
            qi = NODBQueueItem()
            qi.data = {
                'dataset_id': 'foo',
                'flag': 2,
            }
            self.worker_controller.test_queue_worker(
                ERDDAPReloadWorker,
                {},
                qi
            )
            self.assertIn(('foo', 0, 2), self.reloaded)
            self.assertNotIn(('foo', 0, 2), self.reloaded2)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    def test_erddap_reload_worker_with_bad_files_flag(self):
        with self.mock_web_test():
            qi = NODBQueueItem()
            qi.data = {
                'dataset_id': 'foo',
                'flag': 1,
            }
            self.worker_controller.test_queue_worker(
                ERDDAPReloadWorker,
                {},
                qi
            )
            self.assertIn(('foo', 0, 1), self.reloaded)
            self.assertNotIn(('foo', 0, 1), self.reloaded2)

    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    def test_erddap_reload_worker_with_incorrect_flag(self):
        with self.mock_web_test():
            qi = NODBQueueItem()
            qi.data = {
                'dataset_id': 'foo',
                'flag': 4,
            }
            with self.assertLogs('cnodc.worker.erddap_reload', 'WARNING'):
                self.worker_controller.test_queue_worker(
                    ERDDAPReloadWorker,
                    {},
                    qi
                )
            self.assertIn(('foo', 0, 0), self.reloaded)
            self.assertNotIn(('foo', 0, 0), self.reloaded2)


    @injector.test_case
    @zr.test_with_config(('erddaputil', 'username'), 'hello')
    @zr.test_with_config(('erddaputil', 'password'), 'world')
    @zr.test_with_config(('erddaputil', 'base_url'), 'http://test/api/')
    def test_erddap_reload_worker_with_bad_dataset_id(self):
        with self.mock_web_test():
            qi = NODBQueueItem()
            qi.data = {}
            with self.assertLogs('cnodc.worker.erddap_reload', 'ERROR'):
                self.worker_controller.test_queue_worker(
                    ERDDAPReloadWorker,
                    {},
                    qi
                )
            self.assertEqual(0, len(self.reloaded))
            self.assertEqual(0, len(self.reloaded2))