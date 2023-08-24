import threading
import queue
import requests
import typing as t


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





