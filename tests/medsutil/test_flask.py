import itertools
import logging
import shutil
import subprocess

import zirconium
from autoinject import injector

from pipeman_web.boot import build_cnodc_webapp
from medsutil.flask.requestinfo import RequestInfo
from medsutil.flask.trustedproxy import TrustedProxyFix
from tests.helpers.base_test_case import BaseTestCase, skip_long_test


class BoringApp:

    def __call__(self, environ, start_response):
        return 'foobar'


class ProxyFixDummy:

    def __init__(self, x):
        self._x = x
        self._proxy_called = 0

    def __call__(self, *args, **kwargs):
        self._proxy_called += 1
        return self._x(*args, **kwargs)



@skip_long_test
class TestProxyFix(BaseTestCase):

    VALID_IPS = [
        "0.0.0.0",
        "8.8.8.8",
        "9.1.2.255",
        "10.0.2.255",
        "10.1.2.3",
        "10.1.2.2",
        "10.1.1.3",
        "10.1.1.255",
        "10.1.2.99",
        "10.1.2.255",
        "10.1.2.0",
        "10.2.2.3",
        "11.1.2.3",
        "127.0.0.1",
        "192.168.0.0",
        "196.168.0.1",
        "255.255.255.255",
    ]
    INVALID_IPS = [
        "256.256.256",
        "",
        None,
        'foobar',
        "5",
        "256.256.256.256",
    ]

    TEST_SUBNETS = {
        "": [],
        "*": VALID_IPS,
        "127.0.0.1": ["127.0.0.1"],
        "10.1.2.3": ["10.1.2.3"],
        "10.1.2.0/24": ["10.1.2.3", "10.1.2.99", "10.1.2.255", "10.1.2.0", "10.1.2.2"]
    }

    TEST_BAD_SUBNETS = [
        "foobared",
        "256.256.256.256",
        "127.0.0.0/99"
    ]

    def setUp(self):
        super().setUp()
        logging.disable(logging.ERROR)

    def tearDown(self):
        super().tearDown()
        logging.disable(logging.NOTSET)

    def test_call_trusted(self):
        tpf = TrustedProxyFix(BoringApp(), "*")
        tpf._proxy = ProxyFixDummy(BoringApp())
        k = 0
        self.assertEqual(k, tpf._proxy._proxy_called)
        for ip in TestProxyFix.VALID_IPS:
            with self.subTest(ip=ip):
                self.assertEqual("foobar", tpf({"REMOTE_ADDR": ip}, "b"))
                k += 1
                self.assertEqual(k, tpf._proxy._proxy_called)
        for ip in TestProxyFix.INVALID_IPS:
            with self.subTest(ip=ip):
                self.assertEqual("foobar", tpf({"REMOTE_ADDR": ip}, "b"))
                self.assertEqual(k, tpf._proxy._proxy_called)

    def test_call_mix(self):
        tpf = TrustedProxyFix(BoringApp(), "10.1.2.0/24")
        tpf._proxy = ProxyFixDummy(BoringApp())
        k = 0
        self.assertEqual(k, tpf._proxy._proxy_called)
        for ip in TestProxyFix.VALID_IPS:
            if ip in TestProxyFix.TEST_SUBNETS['10.1.2.0/24']:
                k += 1
            with self.subTest(ip=ip):
                self.assertEqual("foobar", tpf({"REMOTE_ADDR": ip}, "b"))
                self.assertEqual(k, tpf._proxy._proxy_called)

    def test_call_untrusted(self):
        tpf = TrustedProxyFix(BoringApp(), "")
        tpf._proxy = ProxyFixDummy(BoringApp())
        self.assertEqual(0, tpf._proxy._proxy_called)
        for ip in TestProxyFix.VALID_IPS:
            with self.subTest(ip=ip):
                self.assertEqual("foobar", tpf({"REMOTE_ADDR": ip}, "b"))
                self.assertEqual(0, tpf._proxy._proxy_called)
        for ip in TestProxyFix.INVALID_IPS:
            with self.subTest(ip=ip):
                self.assertEqual("foobar", tpf({"REMOTE_ADDR": ip}, "b"))
                self.assertEqual(0, tpf._proxy._proxy_called)


    def test_good_subnets(self):
        for subnet in TestProxyFix.TEST_SUBNETS:
            tpf = TrustedProxyFix(None, subnet)
            for ip in TestProxyFix.VALID_IPS:
                with self.subTest(subnet=subnet, ip=ip):
                    self.assertIs(ip in TestProxyFix.TEST_SUBNETS[subnet], tpf._is_upstream_trustworthy({"REMOTE_ADDR": ip}))
            for ip in TestProxyFix.INVALID_IPS:
                with self.subTest(subnet=subnet, ip=ip):
                    self.assertFalse(tpf._is_upstream_trustworthy({"REMOTE_ADDR": ip}))

    def test_two_subnet_upstream(self):
        tpf = TrustedProxyFix(None, ["10.1.2.0/24", "127.0.0.1"])
        for ip in TestProxyFix.VALID_IPS:
            with self.subTest(ip=ip):
                self.assertIs(
                    ip in TestProxyFix.TEST_SUBNETS["10.1.2.0/24"] or ip in TestProxyFix.TEST_SUBNETS["127.0.0.1"],
                        tpf._is_upstream_trustworthy({"REMOTE_ADDR": ip})
                )
        for ip in TestProxyFix.INVALID_IPS:
            with self.subTest(ip=ip):
                self.assertFalse(tpf._is_upstream_trustworthy({"REMOTE_ADDR": ip}))

    def test_bad_subnet_upstream(self):
        for subnet in TestProxyFix.TEST_BAD_SUBNETS:
            tpf = TrustedProxyFix(None, subnet)
            for ip in itertools.chain(TestProxyFix.INVALID_IPS, TestProxyFix.VALID_IPS):
                with self.subTest(subnet=subnet, ip=ip):
                    self.assertFalse(tpf._is_upstream_trustworthy({"REMOTE_ADDR": ip}))


class TestRequestInfo(BaseTestCase):

    def test_parse_proc_info(self):
        # fake full information coming from linux
        info = RequestInfo()
        info._parse_process_info("testuser x 2015-10-01 01:03:02 (10.1.2.3)")
        info._parse_emulated_user("root")
        info._proc_info_loaded = True
        self.assertEqual(info.sys_username(), "testuser")
        self.assertEqual(info.sys_emulated_username(), "root")
        self.assertEqual(info.sys_logon_time(), "2015-10-01 01:03:02")
        self.assertEqual(info.sys_remote_addr(), "10.1.2.3")

    def test_parse_partial_info(self):
        # fake full information coming from linux, but no remote IP
        info = RequestInfo()
        info._parse_process_info("testuser  x  \t2015-10-01 01:03:02")
        info._parse_emulated_user("")
        info._proc_info_loaded = True
        self.assertEqual(info.sys_username(), "testuser")
        self.assertEqual(info.sys_emulated_username(), "testuser")
        self.assertEqual(info.sys_logon_time(), "2015-10-01 01:03:02")
        self.assertIsNone(info.sys_remote_addr())

    def test_parse_no_date(self):
        # fake full information coming from linux, but no date
        info = RequestInfo()
        info._parse_process_info("testuser")
        info._proc_info_loaded = True
        self.assertEqual(info.sys_username(), "testuser")
        self.assertEqual(info.sys_emulated_username(), "testuser")
        self.assertIsNone(info.sys_logon_time())
        self.assertIsNone(info.sys_remote_addr())

    def test_no_request(self):
        info = RequestInfo()
        self.assertIsNone(info.request_method())
        self.assertIsNone(info.remote_ip())
        self.assertIsNone(info.proxy_ip())
        self.assertIsNone(info.correlation_id())
        self.assertIsNone(info.client_id())
        self.assertIsNone(info.request_url())
        self.assertIsNone(info.user_agent())
        self.assertIsNone(info.username())
        self.assertIsNone(info.referrer())
        res = subprocess.run([str(shutil.which('whoami'))], capture_output=True)
        txt = res.stdout.decode('utf-8').replace("\t", " ").strip("\r\n\t")
        while "  " in txt:
            txt = txt.replace("  ", " ")
        pieces = txt.split(" ")
        self.assertEqual(info.sys_username(), pieces[0])
        self.assertIsNotNone(info.sys_emulated_username())

    @injector.test_case
    @zirconium.test_with_config(('flask', 'SECRET_KEY'), 'hello_world')
    def test_with_request(self):
        info = RequestInfo()
        client = build_cnodc_webapp("app")
        with client.test_client():
            with client.test_request_context('/foobar', method='GET', headers={
                'X-Correlation-ID': '12345',
                'X-Client-ID': '67890',
                'User-Agent': 'test user agent',
                'Referer': 'http://localhost/other'
            }, environ_base={
                'REMOTE_ADDR': '127.0.0.1'
            }):
                self.assertEqual(info.request_method(), 'GET')
                self.assertEqual(info.remote_ip(), '127.0.0.1')
                self.assertEqual(info.correlation_id(), '12345')
                self.assertEqual(info.client_id(), '67890')
                self.assertEqual(info.user_agent(), 'test user agent')
                self.assertEqual(info.request_url(), 'http://localhost/foobar')
                self.assertEqual(info.referrer(), 'http://localhost/other')
                self.assertIsNone(info.proxy_ip())
                self.assertIsNone(info.username())

    @injector.test_case
    @zirconium.test_with_config(('flask', 'SECRET_KEY'), 'hello_world')
    def test_less_helpful_request(self):
        info = RequestInfo()
        client = build_cnodc_webapp("app")
        with client.test_client():
            with client.test_request_context('/foobar2', method='POST', headers={
                'X-Forwarded-For': '17.1.2.3 127.0.0.1'
            }, environ_base={
                'REMOTE_ADDR': '10.1.2.3'
            }):
                self.assertEqual(info.remote_ip(), '127.0.0.1')
                self.assertEqual(info.proxy_ip(), '10.1.2.3')
                self.assertEqual(info.correlation_id(), '')
                self.assertEqual(info.client_id(), '')
                self.assertEqual(info.user_agent(), '')
                self.assertEqual(info.request_url(), 'http://localhost/foobar2')
                self.assertIsNone(info.username())
                self.assertIsNone(info.referrer())
                self.assertEqual(info.request_method(), 'POST')





