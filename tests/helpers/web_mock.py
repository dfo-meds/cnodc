import cnodc.util.json as json

import requests
from requests import RequestException


class MockResponse:

    def __init__(self, content: bytes, status_code: int, encoding='utf-8', headers=None):
        self.url = None
        self.headers = headers
        self.content = content
        self.encoding = encoding
        self.status_code = status_code
        self.ok = self.status_code < 400

    @property
    def text(self):
        return self.content.decode(self.encoding)

    def json(self):
        return json.loads(self.text)

    def raise_for_status(self):
        if self.status_code < 400:
            pass
        else:
            raise requests.exceptions.HTTPError(f"{self.status_code}: {self.text}")


class QuickWebMock:

    def __init__(self):
        self._refs = {}

    def __call__(self, url, method="GET"):
        def _inner(cb):
            self._refs[f'{method.upper()}::{url}'] = cb
            return cb
        return _inner

    def mock_request(self, method, url, **kwargs):
        key = f'{method.upper()}::{url}'
        if key in self._refs:
            try:
                #if 'json' in kwargs:
                    #json_data = json.dumps(kwargs.pop('json'))
                    #kwargs['json'] = json.loads(json_data)

                res = self._refs[key](method, url, **kwargs)
                if not isinstance(res, MockResponse):
                    res = MockResponse(str(res).encode('utf-8'), 200)
            except RequestException as ex:
                raise ex
            except Exception as ex:
                res = MockResponse(str(ex).encode('utf-8'), 500)
        else:
            res = MockResponse(b"not found", 404)
        res.url = url
        return res

    def mock_get(self, url, **kwargs):
        return self.mock_request('GET', url, **kwargs)

    def mock_post(self, url, **kwargs):
        return self.mock_request('POST', url, **kwargs)
