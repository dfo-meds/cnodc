import flask

from gcflask.i18n import BaseDString, TString
from medsutil.exceptions import CodedError


def flasht(st: str | BaseDString, msg_type: str):
    if flask.has_request_context():
        flask.flash(TString(st) if isinstance(st, str) else st, msg_type)


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