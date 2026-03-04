import flask
import zrlog
from werkzeug.middleware.proxy_fix import ProxyFix
import ipaddress
import os
import shutil
import subprocess


class TrustedProxyFix:

    def __init__(self, app, trust_from_ips="*", **kwargs):
        self._app = app
        self._proxy = ProxyFix(app, **kwargs)
        self._trusted = trust_from_ips
        self._log = zrlog.get_logger("cnodc.trusted_proxy")
        self._cache = {}

    def _is_upstream_trustworthy(self, environ):
        _ip = environ.get("REMOTE_ADDR")
        try:
            upstream_ip = ipaddress.ip_address(_ip)
        except (ipaddress.AddressValueError, ValueError):
            self._log.warning(f"Upstream address could not be parsed: {_ip}")
            return False
        if self._trusted == "*" or self._trusted is True:
            return True
        elif self._trusted == "" or self._trusted is False or self._trusted is None:
            return False
        elif isinstance(self._trusted, str):
            return self._match_ip_address(upstream_ip, self._trusted)
        else:
            return any(self._match_ip_address(upstream_ip, x) for x in self._trusted)

    def _match_ip_address(self, actual: ipaddress, network_def):
        if network_def not in self._cache:
            try:
                self._cache[network_def] = ipaddress.ip_network(network_def, strict=True)
            except (ipaddress.AddressValueError, ipaddress.NetmaskValueError, ValueError) as ex:
                self._log.warning(f"Trusted IP or subnet could not be parsed: {network_def}")
                self._cache[network_def] = None
        if self._cache[network_def] is not None:
            return actual in self._cache[network_def]
        return False

    def __call__(self, environ, *args, **kwargs):
        """Applies proxy configuration only if the upstream IP is allowed."""
        if self._is_upstream_trustworthy(environ):
            self._log.debug("trusting upstream...")
            return self._proxy(environ, *args, **kwargs)
        else:
            self._log.debug("not trusting upstream...")
            return self._app(environ, *args, **kwargs)


class RequestInfo:

    def __init__(self):
        self._remote_ip = None
        self._proxy_ip = None
        self._correl_id = None
        self._client_id = None
        self._request_url = None
        self._request_method = None
        self._user_agent = None
        self._username = None
        self._referrer = None
        self._proc_info_loaded = False
        self._system_username = None
        self._emulated_user = None
        self._logon_time = None
        self._system_remote_addr = None

    def request_method(self):
        if self._request_method is None and flask.has_request_context():
            self._request_method = flask.request.method
        return self._request_method

    def remote_ip(self):
        if self._remote_ip is None and flask.has_request_context():
            if "X-Forwarded-For" in flask.request.headers:
                self._remote_ip = flask.request.headers.getlist("X-Forwarded-For")[0].rpartition(' ')[-1]
            else:
                self._remote_ip = flask.request.remote_addr or 'untrackable'
        return self._remote_ip

    def proxy_ip(self):
        if self._proxy_ip is None and flask.has_request_context():
            if "X-Forwarded-For" in flask.request.headers:
                self._proxy_ip = flask.request.remote_addr or 'untrackable'
        return self._proxy_ip

    def correlation_id(self):
        if self._correl_id is None and flask.has_request_context():
            self._correl_id = flask.request.headers.get("X-Correlation-ID", "")
        return self._correl_id

    def client_id(self):
        if self._client_id is None and flask.has_request_context():
            self._client_id = flask.request.headers.get("X-Client-ID", "")
        return self._client_id

    def request_url(self):
        if self._request_url is None and flask.has_request_context():
            self._request_url = flask.request.url
        return self._request_url

    def user_agent(self):
        if self._user_agent is None and flask.has_request_context():
            self._user_agent = flask.request.user_agent.string
        return self._user_agent

    def username(self):
        if self._username is None and flask.has_request_context():
            # TODO
            pass
        return self._username

    def referrer(self):
        if self._referrer is None and flask.has_request_context():
            self._referrer = flask.request.referrer
        return self._referrer

    def _load_process_info(self):
        if self._proc_info_loaded is False:
            res = subprocess.run([shutil.which("whoami")], capture_output=True)  # noqa: S603
            self._parse_process_info(res.stdout.decode("utf-8"))
            if os.name == "posix":  # pragma: no coverage (windows testing only)
                res = subprocess.run([shutil.which("who")], capture_output=True)  # noqa: S603
                if res.returncode == 0 and res.stdout:
                    self._parse_emulated_user(res.stdout.decode("utf-8"))
            self._proc_info_loaded = True

    def _parse_emulated_user(self, txt: str):
        txt = txt.strip()
        if txt and txt != self._emulated_user:
            self._emulated_user = txt

    def _parse_process_info(self, txt: str):
        txt = txt.replace("\t", " ").strip("\r\n\t ")
        while "  " in txt:
            txt = txt.replace("  ", " ")
        pieces = txt.split(" ")
        self._system_username = pieces[0]
        self._emulated_user = pieces[0]
        if len(pieces) > 2:
            self._logon_time = pieces[2] + " " + pieces[3]
        if len(pieces) > 4:
            self._system_remote_addr = pieces[4].strip("()")

    def sys_username(self):
        self._load_process_info()
        return self._system_username

    def sys_emulated_username(self):
        self._load_process_info()
        return self._emulated_user

    def sys_logon_time(self):
        self._load_process_info()
        return self._logon_time

    def sys_remote_addr(self):
        self._load_process_info()
        return self._system_remote_addr