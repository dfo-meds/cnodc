import functools
import shutil
import subprocess

import flask

from cnodc.boot.boot import build_cnodc_webapp
from cnodc.util.flask import TrustedProxyFix, RequestInfo
from core import BaseTestCase


class TestProxyFix(BaseTestCase):

    def test_no_upstream(self):
        tpf = TrustedProxyFix(None, "")
        def can_trust(ip):
            return tpf._is_upstream_trustworthy({'REMOTE_ADDR': ip}, None)
        self.assertFalse(can_trust('127.0.0.1'))
        self.assertFalse(can_trust('0.0.0.0'))
        self.assertFalse(can_trust('192.168.0.0'))
        self.assertFalse(can_trust('192.168.0.1'))
        self.assertFalse(can_trust('8.8.8.8'))
        self.assertFalse(can_trust('255.255.255.255'))
        self.assertFalse(can_trust('256.256.256'))
        self.assertFalse(can_trust(''))
        self.assertFalse(can_trust(None))
        self.assertFalse(can_trust('foobar'))
        self.assertFalse(can_trust("5"))
        self.assertFalse(can_trust('256.256.256.256'))

    def test_one_ip_upstream(self):
        tpf = TrustedProxyFix(None, "10.1.2.3")
        def can_trust(ip):
            return tpf._is_upstream_trustworthy({'REMOTE_ADDR': ip}, None)
        self.assertFalse(can_trust('127.0.0.1'))
        self.assertTrue(can_trust('10.1.2.3'))
        self.assertFalse(can_trust('10.1.2.2'))
        self.assertFalse(can_trust('10.1.1.3'))
        self.assertFalse(can_trust('10.2.2.3'))
        self.assertFalse(can_trust('11.1.2.3'))
        self.assertFalse(can_trust('0.0.0.0'))
        self.assertFalse(can_trust('192.168.0.0'))
        self.assertFalse(can_trust('192.168.0.1'))
        self.assertFalse(can_trust('8.8.8.8'))
        self.assertFalse(can_trust('255.255.255.255'))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust('256.256.256'))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust(''))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust(None))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust('foobar'))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust("5"))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust('256.256.256.256'))

    def test_subnet_upstream(self):
        tpf = TrustedProxyFix(None, "10.1.2.0/24")
        def can_trust(ip):
            return tpf._is_upstream_trustworthy({'REMOTE_ADDR': ip}, None)
        self.assertFalse(can_trust('127.0.0.1'))
        self.assertTrue(can_trust('10.1.2.3'))
        self.assertTrue(can_trust('10.1.2.99'))
        self.assertTrue(can_trust('10.1.2.255'))
        self.assertTrue(can_trust('10.1.2.0'))
        self.assertFalse(can_trust('10.1.1.255'))
        self.assertFalse(can_trust('10.0.2.255'))
        self.assertFalse(can_trust('9.1.2.255'))
        self.assertFalse(can_trust('0.0.0.0'))
        self.assertFalse(can_trust('192.168.0.0'))
        self.assertFalse(can_trust('192.168.0.1'))

    def test_two_subnet_upstream(self):
        tpf = TrustedProxyFix(None, ["10.1.2.0/24", "127.0.0.1"])
        def can_trust(ip):
            return tpf._is_upstream_trustworthy({'REMOTE_ADDR': ip}, None)
        self.assertTrue(can_trust('127.0.0.1'))
        self.assertTrue(can_trust('10.1.2.3'))
        self.assertTrue(can_trust('10.1.2.99'))
        self.assertTrue(can_trust('10.1.2.255'))
        self.assertTrue(can_trust('10.1.2.0'))
        self.assertFalse(can_trust('10.1.1.255'))
        self.assertFalse(can_trust('10.0.2.255'))
        self.assertFalse(can_trust('9.1.2.255'))
        self.assertFalse(can_trust('0.0.0.0'))
        self.assertFalse(can_trust('192.168.0.0'))
        self.assertFalse(can_trust('192.168.0.1'))

    def test_bad_subnet_upstream(self):
        tpf = TrustedProxyFix(None, "foobared")
        def can_trust(ip):
            return tpf._is_upstream_trustworthy({'REMOTE_ADDR': ip}, None)
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust('127.0.0.1'))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust('10.1.2.3'))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust('10.1.2.99'))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust('10.1.2.255'))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust('10.1.2.0'))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust('10.1.1.255'))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust('10.0.2.255'))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust('9.1.2.255'))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust('0.0.0.0'))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust('192.168.0.0'))
        with self.assertLogs("cnodc.trusted_proxy", "WARNING"):
            self.assertFalse(can_trust('192.168.0.1'))

    def test_all_ip_upstream(self):
        tpf = TrustedProxyFix(None, "*")
        def can_trust(ip):
            return tpf._is_upstream_trustworthy({'REMOTE_ADDR': ip}, None)
        self.assertTrue(can_trust('127.0.0.1'))
        self.assertTrue(can_trust('10.1.2.3'))
        self.assertTrue(can_trust('10.1.2.2'))
        self.assertTrue(can_trust('10.1.1.3'))
        self.assertTrue(can_trust('10.2.2.3'))
        self.assertTrue(can_trust('11.1.2.3'))
        self.assertTrue(can_trust('0.0.0.0'))
        self.assertTrue(can_trust('192.168.0.0'))
        self.assertTrue(can_trust('192.168.0.1'))
        self.assertTrue(can_trust('8.8.8.8'))
        self.assertTrue(can_trust('255.255.255.255'))
        self.assertTrue(can_trust('256.256.256'))
        self.assertTrue(can_trust(''))
        self.assertTrue(can_trust(None))
        self.assertTrue(can_trust('foobar'))
        self.assertTrue(can_trust("5"))
        self.assertTrue(can_trust('256.256.256.256'))

class TestRequestInfo(BaseTestCase):

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
        res = subprocess.run([shutil.which('whoami')], capture_output=True)
        txt = res.stdout.decode("utf-8").replace("\t", " ").strip("\r\n\t")
        while "  " in txt:
            txt = txt.replace("  ", " ")
        pieces = txt.split(" ")
        self.assertEqual(info.sys_username(), pieces[0])
        self.assertIsNotNone(info.sys_emulated_username())

    def test_with_request(self):
        info = RequestInfo()
        client = build_cnodc_webapp("app", _for_test=True)
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

    def test_less_helpful_request(self):
        info = RequestInfo()
        client = build_cnodc_webapp("app", _for_test=True)
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





