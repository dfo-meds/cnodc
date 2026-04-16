"""
    The Python JSON library is inherently slow. There are several faster options. If they are available, we will
    use them. This module provides a wrapper around either orjson or the native JSON module.
"""
import datetime
import enum
import uuid

import medsutil.types as ct
import typing as t

from medsutil.frozendict import FrozenDict

try:
    import orjson

    def _clean_for_orjson(x):
        try:
            return clean_for_json(x)
        except TypeError as ex:
            raise orjson.JSONEncodeError(f'Cannot encode object of type [{x.__class__.__name__}]') from ex

    def dumpb(__obj: ct.SupportsExtendedJson) -> bytes:
        """ Returns a valid JSON string in utf-8 encoding from the given JSON-compatible object. """
        return orjson.dumps(__obj, default=_clean_for_orjson)

    def dumps(__obj: ct.SupportsExtendedJson) -> str:
        return orjson.dumps(__obj, default=_clean_for_orjson).decode('utf-8')

    def loads(__obj: ct.JsonString) -> ct.SupportsNativeJson:
        return orjson.loads(__obj)

except ModuleNotFoundError:
    import json

    def _clean_for_json(x):
        try:
            return clean_for_json(x)
        except TypeError:
            return None

    def dumpb(__obj: ct.SupportsExtendedJson) -> bytes:
        """ Returns a valid JSON string in utf-8 encoding from the given JSON-compatible object. """
        return json.dumps(__obj, default=_clean_for_json).encode('utf-8')

    def dumps(__obj: ct.SupportsExtendedJson) -> str:
        return json.dumps(__obj, default=_clean_for_json)

    def loads(__obj: ct.JsonString) -> ct.SupportsNativeJson:
        return json.loads(__obj)


def clean_for_json(__obj):
    if __obj is None or isinstance(__obj, (str, int, float, bool)):
        return __obj
    elif isinstance(__obj, (t.Mapping, FrozenDict)):
        return {str(x): clean_for_json(__obj[x]) for x in __obj}
    elif isinstance(__obj, t.Iterable):
        return [clean_for_json(x) for x in __obj]
    elif isinstance(__obj, enum.Enum):
        return clean_for_json(__obj.value)
    elif isinstance(__obj, (datetime.date, datetime.time)):
        return __obj.isoformat()
    elif isinstance(__obj, uuid.UUID):
        return str(__obj)
    raise TypeError(f'Invalid data type [{__obj.__class__.__name__}] for a JSON object')


def load_dict(__obj: ct.JsonDictString) -> dict[str, ct.SupportsNativeJson]:
    """ Loads an object from a JSON string and ensures it is a dictionary. """
    d = loads(__obj)
    if not isinstance(d, dict):
        raise TypeError(f'JSON string was not a dictionary, is {d.__class__.__name__}')
    return d

def load_list(__obj: ct.JsonListString) -> list[ct.SupportsNativeJson]:
    """ Loads an object from a JSON string and ensures it is a list"""
    d = loads(__obj)
    if not isinstance(d, list):
        raise TypeError(f'JSON string was not a list, is {d.__class__.__name__}')
    return d

def load_set(__obj: ct.JsonListString) -> set[ct.SupportsNativeJson]:
    """ Loads an object from a JSON string and ensures it is a list"""
    d = loads(__obj)
    if not isinstance(d, list):
        raise TypeError(f'JSON string was not a set, is {d.__class__.__name__}')
    return set(d)