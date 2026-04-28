import requests
from medsutil.exceptions import CodedError


class RequestError(CodedError): CODE_SPACE = 'WEB'


def web_request(method: str, url: str, session: requests.Session = None, **kwargs) -> requests.Response:
    """ Wrapper around requests.request() that converts errors to appropriate CNODC errors. """
    try:
        if session is not None:
            result = session.request(method, url, **kwargs)
        else:
            result = requests.request(method, url, **kwargs)
        result.raise_for_status()
        return result
    except Exception as ex:
        if isinstance(ex, (requests.ConnectionError, requests.Timeout, requests.TooManyRedirects)):
            raise RequestError(f'An error occurred while trying to connect to the server for [{url}]: {type(ex)}: {str(ex)}', 1000, is_transient=True) from ex
        else:
            raise RequestError(f'An error occurred while trying to execute a request to [{url}]: {type(ex)}: {str(ex)}', 1001) from ex
