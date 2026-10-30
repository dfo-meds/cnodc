""" Support functions for making HTTP requests.

    Mostly this is a wrapper around the requests library, but we include
    built-in CodedErrors being raised (including a transient one if it's
    a connection or timeout error) and improved JSON encoding.

    Also allows a session to be passed as an argument and used to make
    the request.
"""
import typing as t

import requests

from medsutil import json
from medsutil.exceptions import CodedError


if t.TYPE_CHECKING:
    from _typeshed import SupportsRead


class RequestError(CodedError): CODE_SPACE = 'WEB'


UrlParamValueType = str | int | float | bytes | None | list[str | int | float | bytes | None]


def get(url: str, session: requests.Session | None = None, **kwargs) -> requests.Response:
    """ GETs an HTTP request."""
    return request('GET', url, session, **kwargs)

def post(url: str, session: requests.Session | None = None, **kwargs) -> requests.Response:
    """ POSTs an HTTP request. """
    return request('POST', url, session, **kwargs)

def request(method: str,
            url: str,
            session: requests.Session | None = None,
            headers: t.MutableMapping[str, str] | None = None,
            data: str | bytes | dict[str, UrlParamValueType] | list[tuple[str, UrlParamValueType]] | SupportsRead | None = None,
            check_for_response_error: bool = True,
            **kwargs) -> requests.Response:
    """ Makes an HTTP request with improved error handling and better JSON encoding.

        :raises RequestError: If a temporary error occurred while making the request (1000)
        :raises RequestError: If another type of error occurred while making the request (1001)
    """
    try:

        # Custom handling for JSON to have it done by orjson and our custom handling of objects instead
        if (not data) and 'json' in kwargs:
            data = json.dumpb(kwargs.pop('json'))
            if headers is None:
                headers = {'Content-Type': 'application/json'}
            else:
                headers['Content-Type'] = 'application/json'

        # Handle a session being passed
        if session is not None:
            result = session.request(method, url, data=data, headers=headers, **kwargs)
        else:
            result = requests.request(method, url, data=data, headers=headers, **kwargs)

        # Raise for status, if requested
        if check_for_response_error:
            result.raise_for_status()

        return result
    except Exception as ex:
        if isinstance(ex, (requests.ConnectionError, requests.Timeout, requests.TooManyRedirects)):
            raise RequestError(f'An error occurred while trying to connect to the server for [{url}]: {type(ex)}: {str(ex)}', 1000, is_transient=True) from ex
        else:
            raise RequestError(f'An error occurred while trying to execute a request to [{url}]: {type(ex)}: {str(ex)}', 1001) from ex
