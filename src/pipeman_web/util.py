import functools
import zrlog
import typing as t
import flask

from autoinject import injector
from medsutil.exceptions import CodedError
from pipeman_web.auth import LoginController


def require_permission(permission_names: t.Union[str, t.Iterable[str]]):
    if isinstance(permission_names, str):
        permission_names = [permission_names]

    def _outer_wrapper(cb):
        return require_login(cb, permission_names)

    return _outer_wrapper


def require_inputs(input_names: t.Union[str, t.Iterable[str]]):
    if isinstance(input_names, str):
        input_names = [input_names]

    def _outer_wrapper(cb):
        return json_api(cb, input_names)

    return _outer_wrapper


def json_api(cb: t.Callable, input_names: t.Optional[t.Iterable[str]] = None):

    @functools.wraps(cb)
    def _inner_wrapper(*args, **kwargs):
        return _make_json_api(cb, args, kwargs, input_names)

    return _inner_wrapper


def _make_json_api(cb: t.Callable, args, kwargs, input_names: t.Optional[t.Iterable[str]] = None):
    try:
        if input_names:
            if not flask.request.is_json:
                raise CodedError('Requests must be JSON', 1000, code_space="JSONAPI")
            for x in input_names:
                if x not in flask.request.json:
                    raise CodedError(f"Request is missing required parameter [{x}]", 1001, code_space="JSONAPI")
        res = cb(*args, **kwargs)
        return {'success': True} if res is None else res
    except CodedError as ex:
        zrlog.get_logger("cnodc").exception(ex)
        return {"error": str(ex), "code": ex.obfuscated_code()}
    except Exception as ex:
        zrlog.get_logger("cnodc").exception(ex)
        return {"error": f"{ex.__class__.__name__}: {str(ex)}"}


def require_login(cb: t.Callable, permission_names: t.Optional[t.Iterable[str]] = None):

    @functools.wraps(cb)
    def _inner_wrapper(*args, **kwargs):
        if has_access(permission_names):
            return cb(*args, **kwargs)

    return _inner_wrapper


@injector.inject
def has_access(permissions: t.Optional[t.Iterable[str]], login: LoginController = None, raise_ex: bool = True) -> bool:
    if not login.verify_token():
        if raise_ex:
            raise CodedError("Invalid token", 1000, code_space='ACCESS_TOKEN')
        return False
    current_perms = login.current_permissions()
    if permissions and '__admin__' not in current_perms and any(p not in current_perms for p in permissions):
        zrlog.get_logger("cnodc.auth").debug(f"Access denied because none of [{';'.join(permissions)}] found in [{';'.join(current_perms)}]")
        if raise_ex:
            raise CodedError("Unauthorized", 1001, code_space='ACCESS_TOKEN')
        return False
    return True
