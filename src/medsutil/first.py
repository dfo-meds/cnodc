import itertools
import medsutil.types as ct


def first(*args, default=None, skip=...):
    """ Return the first non-None non-empty string item passed. """
    if skip is ...:
        skip = ('',)
    elif skip is None:
        skip = set()
    for item in args:
        if item is not None and item not in skip:
            return item
    return default


def first_i18n(d: ct.AcceptAsLanguageDict | None, default=None) -> str | None:
    """ Return a single representative item from a language dict """
    if not isinstance(d, dict):
        return d or default
    for key in itertools.chain(('und', 'en', 'fr'), d.keys()):
        if key in d and d[key]: return d[key]
    return default


