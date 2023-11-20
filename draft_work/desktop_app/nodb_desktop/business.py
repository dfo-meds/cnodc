import datetime
import threading
import queue
import requests
import typing as t
import tkinter as tk
import uuid
from nodb_desktop.i18n import Translator


class NODBRequestController(threading.Thread):

    def __init__(self):
        super().__init__(daemon=True)
        self._job_queue = queue.Queue()
        self._complete_queue = queue.Queue()
        self.halt = threading.Event()

    def request(self, url: str, method: str, data: dict, cb: callable):
        if not self.halt.is_set():
            self._job_queue.put({
                'url': url,
                'method': method,
                'data': data,
                'cb': cb
            })

    def check_results(self, max_check: t.Optional[int] = None):
        while (max_check is None or max_check > 0) and not self._complete_queue.empty():
            result = self._complete_queue.get()
            result['cb'](result['results'], result['status_code'])
            if max_check is not None and max_check > 0:
                max_check -= 1

    def run(self):
        while not self.halt.is_set():
            self._process_jobs()
            self.halt.wait(0.1)

    def _process_jobs(self):
        while not self._job_queue.empty():
            if self.halt.is_set():
                break
            job = self._job_queue.get()
            try:
                resp = requests.request(
                    method=job['method'],
                    url=job['url'],
                    json=job['data']
                )
                self._complete_queue.put({
                    'results': resp.json(),
                    'status_code': resp.status_code,
                    'cb': job['cb']
                })
            except Exception as ex:
                self._complete_queue.put({
                    'results': {
                        'exception': ex,
                        'original': job
                    },
                    'status_code': -1,
                    'cb': job['cb']
                })


class _DesktopApp(t.Protocol):

    def refresh_interface(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def start(self):
        raise NotImplementedError()

    @property
    def top_frame(self):
        raise NotImplementedError()


class DesktopAppController:

    def __init__(self, service_path: str):
        self._app: t.Optional[_DesktopApp] = None
        self._app_uuid = str(uuid.uuid4())
        self.current_language: str = 'en'
        self._translator = Translator()
        self._requests = NODBRequestController()
        self._user_token = None
        self._token_expiry = None
        self._service_path = service_path
        self.username: t.Optional[str] = None
        self._token_renewal_gate = 60
        self._renewal_in_progress = False

    def timed_check(self, max_results: int = 5):
        self._requests.check_results(max_results)
        self._check_time_to_renew_token()

    def _check_time_to_renew_token(self):
        if self._token_expiry is not None:
            expires_in_sec = (self._token_expiry - datetime.datetime.utcnow()).total_seconds()
            if expires_in_sec < 0:
                # TODO: need to login again
                pass
            elif expires_in_sec < self._token_renewal_gate and not self._renewal_in_progress:
                self._renewal_in_progress = True
                self.set_status_message(self.get_text("renewing_session"))
                self.make_request("POST", "renew", None, self._renewal_callback)

    def _renewal_callback(self, response, status):
        if status == 200:
            if 'token' in response and 'expiry' in response:
                self.set_user_token(response['token'], response['expiry'])
                self.set_status_message(self.get_text("session_renewed"))
            else:
                self.set_status_message(self.get_text("session_renewal_missing_token"))
        elif status == -1:
            # TODO: error condition on our side
            pass
        else:
            # TODO: error condition on their side
            pass

    def make_request(self, method, endpoint, data, cb):
        if data is None:
            data = {}
        data['api_uuid'] = self._app_uuid
        if self._user_token is not None:
            data['token'] = self._user_token
        self._requests.request(
            self._service_path + endpoint,
            method,
            data,
            cb
        )

    def get_text(self, resource_name: str, lang: str = None):
        return self._translator.translate(lang or self.current_language, resource_name)

    def set_language(self, new_lang: str):
        self.current_language = new_lang
        self._app.refresh_interface()

    def set_status_message(self, msg: str):
        self._app.top_frame.status_message = msg

    def set_user_token(self, token: str, expiry: t.Union[datetime.datetime, str], username: t.Optional[str] = None):
        self._user_token = token
        self._token_expiry = datetime.datetime.fromisoformat(expiry) if isinstance(expiry, str) else expiry
        if username is not None:
            self.username = username
            self._app.top_frame.username = username

    def clear_user_token(self):
        self._user_token = None
        self._token_expiry = None
        self.username = None
        self._app.top_frame.username = ""

    def close(self):
        if self._requests:
            self._requests.halt.set()
            self._requests.join()
        if self._app:
            self._app.close()

    def launch(self):
        from nodb_desktop.ui.top import DesktopApp
        self._app = DesktopApp(self)
        try:
            self._requests.start()
            self._app.start()
        finally:
            self.close()


