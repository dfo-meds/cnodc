import typing as t
import requests

from medsutil import json
from medsutil.exceptions import CodedError


if t.TYPE_CHECKING:
    from _typeshed import SupportsRead


class RequestError(CodedError): CODE_SPACE = 'WEB'

UrlParamValueType = str | int | float | bytes | None | list[str | int | float | bytes | None]


def get(url: str, session: requests.Session | None = None, **kwargs) -> requests.Response:
    return request('GET', url, session, **kwargs)

def post(url: str, session: requests.Session | None = None, **kwargs) -> requests.Response:
    return request('POST', url, session, **kwargs)

def request(method: str,
            url: str,
            session: requests.Session | None = None,
            headers: t.MutableMapping[str, str] | None = None,
            data: str | bytes | dict[str, UrlParamValueType] | list[tuple[str, UrlParamValueType]] | SupportsRead | None = None,
            check_for_response_error: bool = True,
            **kwargs) -> requests.Response:
    """ Wrapper around requests.request() that converts errors to appropriate CNODC errors. """
    try:

        # Custom handling for JSON to have it done by orjson and our custom handling of objects instead
        if (not data) and 'json' in kwargs:
            data = json.dumpb(kwargs.pop('json'))
            if headers is None:
                headers = {'Content-Type': 'application/json'}
            else:
                headers['Content-Type'] = 'application/json'

        if session is not None:
            result = session.request(method, url, data=data, headers=headers, **kwargs)
        else:
            result = requests.request(method, url, data=data, headers=headers, **kwargs)
        if check_for_response_error:
            result.raise_for_status()
        return result
    except Exception as ex:
        if isinstance(ex, (requests.ConnectionError, requests.Timeout, requests.TooManyRedirects)):
            raise RequestError(f'An error occurred while trying to connect to the server for [{url}]: {type(ex)}: {str(ex)}', 1000, is_transient=True) from ex
        else:
            raise RequestError(f'An error occurred while trying to execute a request to [{url}]: {type(ex)}: {str(ex)}', 1001) from ex
