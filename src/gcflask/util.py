import flask
import typing as t

from gcflask.i18n import BaseDString


def flasht(st: str | BaseDString):
    if isinstance(st, str):
        flask.flash(t.cast())


def caps_to_snake(txt: str, separator: str = "_") -> str:
    new_s = txt[0].lower()
    for x in txt[1:]:
        if x.isupper():
            new_s += separator
        new_s += x.lower()
    return new_s
