import functools
import zrlog
import typing as t
import flask

from autoinject import injector
from cnodc.util import CNODCError
from cnodc.api.auth import LoginController


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


def json_api(cb: callable, input_names: t.Optional[t.Iterable[str]] = None):

    @functools.wraps(cb)
    def _inner_wrapper(*args, **kwargs):
        return _make_json_api(cb, args, kwargs, input_names)

    return _inner_wrapper


def _make_json_api(cb: callable, args, kwargs, input_names: t.Optional[t.Iterable[str]] = None):
    try:
        if input_names:
            if not flask.request.is_json:
                raise CNODCError('Requests must be JSON', "JSONAPI", 1000)
            for x in input_names:
                if x not in flask.request.json:
                    raise CNODCError(f"Request is missing required parameter [{x}]", "JSONAPI", 1001)
        res = cb(*args, **kwargs)
        return {'success': True} if res is None else res
    except CNODCError as ex:
        zrlog.get_logger("cnodc").exception(ex)
        return {"error": str(ex), "code": ex.obfuscated_code()}
    except Exception as ex:
        zrlog.get_logger("cnodc").exception(ex)
        return {"error": f"{ex.__class__.__name__}: {str(ex)}"}


def require_login(cb: callable, permission_names: t.Optional[t.Iterable[str]] = None):

    @functools.wraps(cb)
    def _inner_wrapper(*args, **kwargs):
        _check_login_access(permission_names)
        return cb(*args, **kwargs)

    return _inner_wrapper


@injector.inject
def _check_login_access(permissions: t.Optional[t.Iterable[str]], login: LoginController = None):
    if not login.verify_token():
        raise CNODCError("Invalid token", "AUTH", 1000)
    current_perms = login.current_permissions()
    if permissions and '__admin__' not in current_perms and any(p not in current_perms for p in permissions):
        zrlog.get_logger("cnodc.auth").debug(f"Access denied because none of [{';'.join(permissions)}] found in [{';'.join(current_perms)}]")
        raise CNODCError("Unauthorized request", "AUTH", 1001)
