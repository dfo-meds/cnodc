import flask
import typing as t

from gcflask.i18n import BaseDString, TString


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
