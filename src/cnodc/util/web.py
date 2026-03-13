import requests
from requests import HTTPError, ConnectionError, Timeout, TooManyRedirects
from cnodc.util.exceptions import CNODCError


def web_request(method, url, **kwargs):
    try:
        result = requests.request(method, url, **kwargs)
        result.raise_for_status()
        return result
    except Exception as ex:
        if isinstance(ex, (ConnectionError, Timeout, TooManyRedirects)):
            raise CNODCError(f'An error occurred while trying to connect to the server for [{url}]: {type(ex)}: {str(ex)}', 'WEB', 1000, True) from ex
        else:
            raise CNODCError(f'An error occurred while trying to execute a request to [{url}]: {type(ex)}: {str(ex)}', 'WEB', 1001, False) from ex
