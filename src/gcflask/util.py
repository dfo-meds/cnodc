import flask
import typing as t

from gcflask.i18n import BaseDString


def flasht(st: str | BaseDString):
    if isinstance(st, str):
        flask.flash(t.cast())
