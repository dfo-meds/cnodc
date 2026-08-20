import typing as t
from types import EllipsisType

import flask

from gcapp.i18n.base import BaseDString, TString
from medsutil.exceptions import CodedError


def flasht(st: str | BaseDString, msg_type: str):
    if flask.has_request_context():
        flask.flash(TString(st) if isinstance(st, str) else st, msg_type)


def flash(st: str, msg_type: str):
    if flask.has_request_context():
        flask.flash(st, msg_type)


def caps_to_snake(txt: str, separator: str = "_") -> str:
    new_s = txt[0].lower()
    for x in txt[1:]:
        if x.isupper():
            new_s += separator
        new_s += x.lower()
    return new_s


class APIError(CodedError): CODE_SPACE = "API-ERROR"


class FlaskRequestJsonData:

    def __init__(self):
        self._data = flask.request.json

    def get(self, key: str, default=...):
        try:
            return self._data[key]
        except KeyError as ex:
            if default is ...:
                raise APIError(f"Missing key [{key}]", 1000) from ex
            else:
                return default


def json_param[T](param_name: str, coerce: t.Callable[[t.Any], T] | None = None, default: T | EllipsisType = ...) -> T:
    if not flask.request.is_json:
        return flask.abort(400, "Request must be JSON formatted")
    if not isinstance(flask.request.json, dict):
        flask.abort(400, "Request must contain a JSON mapping payload")
    if param_name not in flask.request.json and default is ...:
        flask.abort(400, "Missing mandatory parameter")
    try:
        x = flask.request.json.get(param_name, default)
        if x is not None and coerce is not None:
            x = coerce(x)
        return x
    except (ValueError, TypeError, IndexError) as e:
        flask.abort(400, f"Invalid parameter for [{param_name}]: {e}")
