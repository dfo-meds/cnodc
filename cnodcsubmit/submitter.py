import hashlib
import logging
import pathlib
import typing as t

import requests
import datetime

from requests import JSONDecodeError



class CNODCSubmissionError(Exception):

    def __init__(self, *args, is_transient: bool = False):
        super().__init__(*args)
        self.is_transient = is_transient


class BandwidthLimiter:

    def __init__(self,
                 bandwidth_cap: float | None,
                 initial_request_time: float,
                 initial_overhead: int,
                 sliding_window: float):
        self._window = sliding_window
        self._initial_overhead = initial_overhead
        self._initial_request_time = initial_request_time
        self._bandwidth_cap = bandwidth_cap if bandwidth_cap is not None and bandwidth_cap > 0 else None
        self._reports: list[tuple[int, int, float, datetime.datetime]] = []

    def report_request(self, request_time_ms: float, data_size: int, overhead_size: int):
        self._reports.append((data_size, overhead_size, request_time_ms, datetime.datetime.now()))

    def prune_requests(self):
        now_ = datetime.datetime.now()
        self._reports = [
            x
            for x in self._reports
            if ((x[3] - now_).total_seconds() * 1000) <= self._window
        ]

    def calculate_allowed_bandwidth(self) -> int | None:
        if self._bandwidth_cap is None:
            return None
        t_overhead = 0
        t_time = 0
        t_reports = 0
        t_volume = 0
        for vol, overhead, r_time, _ in self._reports:
            t_overhead += overhead
            t_time += r_time
            t_volume += vol
            t_reports += 1
        if t_time == 0 or t_reports == 0:
            a_time = self._initial_request_time
            a_overhead = self._initial_overhead
        else:
            a_time = (t_time / t_reports)
            a_overhead = t_overhead / t_reports
        return int(max(0, ((a_time + t_time) * self._bandwidth_cap) - t_volume - a_overhead))


class CNODCSubmitter:

    def __init__(self,
                 api_root: str,
                 max_bandwidth_kbps: float = 1024,
                 request_time_guess_ms: float = 1000,
                 overhead_guess_bytes: int = 250,
                 window_ms: float = 10000):
        self._limiter = BandwidthLimiter(
            sliding_window=window_ms,
            initial_overhead=overhead_guess_bytes,
            initial_request_time=request_time_guess_ms,
            bandwidth_cap=(max_bandwidth_kbps / (8 * 1024 * 0.001))
        )
        self._api_root = api_root
        self._workflows: dict[str, dict[str, t.Any]] | None = None
        self._session: requests.Session | None = None
        self._log = logging.getLogger("cnodc.submit")
        self._token: str | None = None
        self._expiry: datetime.datetime | None = None
        self._services: dict | None = None

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
        return t.cast(requests.Session, self._session)

    def _make_raw_request(self, endpoint: str, method: str, headers: dict[str, str] | None = None, data: bytes | None = None, _is_limited: bool = False, **kwargs: str) -> requests.Response:
        full_url = f"{self._api_root.rstrip('/')}/{endpoint.lstrip('/')}" if not endpoint.startswith('http') else endpoint
        self._log.debug(f"Web request: {method} {full_url}")
        headers_actual = {}
        if headers:
            headers_actual.update(headers)
        if self._token is not None:
            headers_actual['Authorization'] = f'Bearer {self._token}'
        try:
            if data is not None:
                response = self.session.request(method, full_url, data=data, headers=headers_actual)
            else:
                response = self.session.request(method, full_url, json=kwargs, headers=headers_actual)
            if _is_limited:
                self._track_limited_request(response)
        except (ConnectionError, TimeoutError) as ex:
            raise CNODCSubmissionError("Connection error", is_transient=True) from ex
        except Exception as ex:
            raise CNODCSubmissionError("HTTP error") from ex

    def _make_json_request(self, *args, **kwargs) -> dict:
        response = self._make_raw_request(*args, **kwargs)
        return self._handle_json_response(response)

    def _handle_json_response(self, response: requests.Response) -> dict:
        content_type = response.headers.get("Content-Type", "")
        if not content_type.startswith('application/json'):
            raise CNODCSubmissionError(f"Invalid content type: {content_type}")
        elif response.status_code >= 400:
            raise CNODCSubmissionError(f"HTTP error {response.status_code}")
        try:
            json_body = response.json()
        except JSONDecodeError as ex:
            raise CNODCSubmissionError(f"Invalid JSON body: {ex}") from ex
        if "error" in json_body and json_body["error"]:
            raise CNODCSubmissionError(f"Remote error: {json_body["error"]}")
        return json_body

    def _track_limited_request(self, response: requests.Response) -> dict:
        overhead = sum(self._get_http_overhead_content(response))
        data = len(response.request.body) if response.request.body else 0
        self._limiter.report_request(response.elapsed.total_seconds() * 1000, data, overhead)
        return self._handle_json_response(response)

    def _get_http_overhead_content(self, response: requests.Response) -> t.Iterable[int]:
        yield len(response.request.method or '')
        yield len(response.request.url or '')
        yield 14
        for k, v in response.request.headers.items():
            yield len(k) + len(v) + 3
        yield len(str(response.status_code))
        yield len(response.reason)
        for k, v in response.headers.items():
            yield len(k) + len(v) + 3
        yield len(response.content)

    def _service_info(self, service_name: str, _service_list: dict[str, dict[str, t.Any]] | None = None) -> dict[str, t.Any]:
        if _service_list is None:
            _service_list = self._services
        if _service_list is None or service_name not in _service_list:
            raise CNODCSubmissionError("No such service")
        else:
            return _service_list[service_name]

    def _make_service_request(self,
                              service_name: str,
                              method: str,
                              headers: dict[str, str] | None = None,
                              _service_list: dict[str, dict] | None = None,
                              **kwargs) -> dict:
        info = self._service_info(service_name, _service_list)
        if "kwargs" in info and info["kwargs"]:
            kwargs.update(**info["kwargs"])
        _headers = {}
        if headers:
            _headers.update(headers)
        _headers.update(info.get("headers", {}))
        return self._make_json_request(
            endpoint=info["endpoint"],
            method=method,
            headers=_headers,
            **kwargs
        )

    def _load_workflows(self) -> dict[str, dict[str, t.Any]]:
        if self._workflows is None:
            response = self._make_service_request(
                "intake.list_available_workflows",
                "GET"
            )
            self._workflows = response.get("data", {})
        return t.cast(dict[str, dict[str, t.Any]], self._workflows)

    def login(self, username, password):
        response = self._make_json_request(
            endpoint="api/create-access-token",
            method="POST",
            username=username,
            password=password
        )
        self._token = response["token"]
        self._expiry = datetime.datetime.fromisoformat(response["expiry"])
        self._services = response["access"]

    def list_workflows(self, lang_code: str = "en", fallback: str = "und") -> dict[str, str]:
        return {
            x: y.get("labels", {}).get(lang_code) or y.get("labels", {}).get(fallback) or x
            for x, y in self._load_workflows()
        }

    def cancel_upload(self, last_actions: dict[str, dict[str, t.Any]]) -> bool:
        response = self._make_service_request(
            service_name="cancel",
            method="POST",
            _service_list=last_actions,
        )
        return response["success"]

    def submit_file(self,
                    workflow_name: str,
                    file: pathlib.Path,
                    metadata: dict[str, str] | None = None):
        start_at = 0
        chunk_number = 0
        last_actions = None
        submit_more = True
        while submit_more:
            offset, at_eof, last_actions = self.submit_file_chunk(
                workflow_name=workflow_name,
                file=file,
                start_at=start_at,
                chunk_number=chunk_number,
                last_actions=last_actions,
                metadata=metadata
            )
            start_at += offset
            submit_more = not at_eof
            chunk_number += 1

    def submit_file_chunk(self,
                          workflow_name: str,
                          file: pathlib.Path,
                          start_at: int = 0,
                          chunk_number: int = 0,
                          metadata: dict[str, str] | None = None,
                          last_actions: dict[str, dict[str, t.Any]] | None = None) -> tuple[int, bool, dict[str, dict[str, t.Any]] | None]:
        workflows = self._load_workflows()
        if workflow_name not in workflows:
            raise CNODCSubmissionError(f"No such workflow: {workflow_name}")
        data_cap = int(workflows[workflow_name].get("max_chunk_size", 1024 * 1024))
        throttled = self._limiter.calculate_allowed_bandwidth()
        if throttled is not None and throttled < data_cap:
            data_cap = throttled
        if data_cap == 0:
            return 0, False, last_actions
        with open(file, "rb") as h:
            h.seek(start_at)
            data_to_send = h.read(data_cap)
            at_eof = h.read(1) != b''
        headers = {
            'x-cnodc-more-data': str(1 if not at_eof else 0),
            'x-cnodc-checksum': hashlib.md5(data_to_send, usedforsecurity=False).hexdigest(),
            'x-cnodc-chunk-number': str(chunk_number),
        }
        if metadata:
            for k, v in metadata.items():
                headers[f"x-cnodc-{k.lower()}"] = str(v)
        response = self._make_service_request(
            service_name="submit",
            method="POST",
            _service_list=last_actions if last_actions is not None else workflows[workflow_name].get("actions", {}),
            headers=headers,
            data=data_to_send,
            _is_limited=True,
        )
        return len(data_to_send), at_eof, (response['data']['actions'] if 'data' in response else None)




