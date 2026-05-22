import os
import shutil
import subprocess
import sys

import zrlog
from autoinject import injector


@injector.injectable
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
        self._flask_info_loaded = False
        self._proc_info_loaded = False
        self._system_username = None
        self._emulated_user = None
        self._logon_time = None
        self._system_remote_addr = None

    def set_logging_defaults(self):
        zrlog.set_default_extra("sys_username", "")
        zrlog.set_default_extra("sys_emulated", "")
        zrlog.set_default_extra("sys_logon", "")
        zrlog.set_default_extra("sys_remote", "")
        zrlog.set_default_extra("username", "")
        zrlog.set_default_extra("remote_ip", "")
        zrlog.set_default_extra("proxy_ip", "")
        zrlog.set_default_extra("correlation_id", "")
        zrlog.set_default_extra("client_id", "")
        zrlog.set_default_extra("request_url", "")
        zrlog.set_default_extra("user_agent", "")
        zrlog.set_default_extra("referrer", "")
        zrlog.set_default_extra("request_method", "")

    def set_logging_extras_system(self):
        zrlog.set_extras({
            "sys_username": self.sys_username(),
            "sys_emulated": self.sys_emulated_username(),
            "sys_logon": self.sys_logon_time(),
            "sys_remote": self.sys_remote_addr()
        })

    def set_logging_extras_web(self):
        zrlog.set_extras({
            'username': self.username(),
            'remote_ip': self.remote_ip(),
            'proxy_ip': self.proxy_ip(),
            'correlation_id': self.correlation_id(),
            'client_id': self.client_id(),
            'request_url': self.request_url(),
            'user_agent': self.user_agent(),
            'referrer': self.referrer(),
            'request_method': self.request_method(),
        })

    def _load_flask_info(self):
        if not self._flask_info_loaded:
            # we only have flask info if something else imported it already, and its heavy to import
            if "flask" in sys.modules:
                import flask
                if flask.has_request_context():
                    self._request_method = flask.request.method
                    self._remote_ip = flask.request.remote_addr or 'untrackable'
                    self._correl_id = flask.request.headers.get("X-Correlation-ID", "")
                    self._client_id = flask.request.headers.get("X-Client-ID", "")
                    self._request_url = flask.request.url
                    self._user_agent = flask.request.user_agent.string
                    self._referrer = flask.request.referrer
                    self._flask_info_loaded = True

    def request_method(self) -> str | None:
        self._load_flask_info()
        return self._request_method

    def remote_ip(self) -> str | None:
        self._load_flask_info()
        return self._remote_ip

    def proxy_ip(self) -> str | None:
        self._load_flask_info()
        return self._proxy_ip

    def correlation_id(self) -> str | None:
        self._load_flask_info()
        return self._correl_id

    def client_id(self) -> str | None:
        self._load_flask_info()
        return self._client_id

    def request_url(self) -> str | None:
        self._load_flask_info()
        return self._request_url

    def user_agent(self) -> str | None:
        self._load_flask_info()
        return self._user_agent

    def username(self) -> str | None:
        self._load_flask_info()
        return self._username

    def referrer(self) -> str | None:
        self._load_flask_info()
        return self._referrer

    def _load_process_info(self):
        if not self._proc_info_loaded:
            res = subprocess.run([str(shutil.which("whoami"))], capture_output=True)  # noqa: B603 # hard coded
            self._parse_process_info(res.stdout.decode("utf-8"))
            if os.name == "posix":  # pragma: no coverage (windows testing only)
                res = subprocess.run([str(shutil.which("who"))], capture_output=True)  # noqa: B603 # hard coded
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

    def sys_username(self) -> str | None:
        self._load_process_info()
        return self._system_username

    def sys_emulated_username(self) -> str | None:
        self._load_process_info()
        return self._emulated_user

    def sys_logon_time(self) -> str | None:
        self._load_process_info()
        return self._logon_time

    def sys_remote_addr(self) -> str | None:
        self._load_process_info()
        return self._system_remote_addr
