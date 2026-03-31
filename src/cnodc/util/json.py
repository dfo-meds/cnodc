"""
    The Python JSON library is inherently slow. There are several faster options. If they are available, we will
    use them. This module provides a wrapper around either orjson or the native json module.
"""
from __future__ import annotations

import enum
import typing as t
import os

from orjson.orjson import JSONEncodeError

from cnodc.util.awaretime import AwareDateTime
from cnodc.util.types import *
import datetime
import uuid


try:  # pragma: no coverage

    # orjson and json have very slight differences in how they encode objects
    # eg: orjson encodes a list as [a,b,c] and json as [a, b, c]
    # therefore, when testing, we force the use of the native json library for consistency
    # tests that want to use the orjson library should ensure it exists and clear this flag
    # while the test is running, then reset it after.
    if 'CNODC_FORCE_NATIVE_JSON' in os.environ and os.environ['CNODC_FORCE_NATIVE_JSON'] == 'Y':
        raise ModuleNotFoundError('pretending we do not exist')

    import orjson
    json_name = 'orjson'

    def _clean_for_orjson(x):
        res = _clean_for_native_json(x)
        if res is None:
            raise orjson.JSONEncodeError(f'Cannot encode object of type [{x.__class__.__name__}')
        return res

    def dump_string(__obj: SupportsExtendedJson, pretty=False) -> str:
        """ Returns a valid JSON string from the given JSON-compatible object. """
        return orjson.dumps(__obj, default=_clean_for_orjson).decode('utf-8')

    def dump_bytes(__obj: SupportsExtendedJson) -> bytes:
        """ Returns a valid JSON string in utf-8 encoding from the given JSON-compatible object. """
        return orjson.dumps(__obj, default=_clean_for_orjson)

    def load_string(__obj: JsonString) -> SupportsNativeJson:
        """ Returns a JSON object from the given string, bytes, bytearray, or memoryview. """
        return orjson.loads(__obj)

except ModuleNotFoundError:  # pragma: no coverage (fallback for when orjson might not be available)
    import json
    json_name = 'json'

    def dump_string(__obj: SupportsExtendedJson, pretty=False) -> str:
        """ Returns a valid JSON string from the given JSON-compatible object. """
        return json.dumps(__obj, indent=2 if pretty else None, separators=(',', ':'), default=_clean_for_native_json)

    def dump_bytes(__obj: SupportsExtendedJson) -> bytes:
        """ Returns a valid JSON string in utf-8 encoding from the given JSON-compatible object. """
        return dump_string(__obj).encode('utf-8')

    def load_string(__obj: JsonString) -> SupportsNativeJson:
        """ Returns a JSON object from the given string, bytes, bytearray, or memoryview. """
        try:
            return json.loads(__obj)
        except TypeError as ex:
            if isinstance(__obj, memoryview):
                return json.loads(bytes(__obj))
            else:
                raise

def _clean_for_native_json(item):
    if isinstance(item, (datetime.datetime, datetime.date, datetime.time)):
        return item.isoformat()
    elif isinstance(item, uuid.UUID):
        return str(item)
    elif isinstance(item, enum.Enum):
        return item.value
    elif isinstance(item, t.Mapping):
        return {x: y for x, y in item.items()}
    elif isinstance(item, t.Iterable):
        return [x for x in item]

def dumps(__obj: SupportsExtendedJson) -> str:
    return dump_string(__obj)

def dump_pretty(__obj) -> str:
    return dump_string(__obj, pretty=True)

def loads(__obj: JsonString) -> SupportsNativeJson:
    return load_string(__obj)

def clean_for_json(data: SupportsExtendedJson, _max_depth=50) -> SupportsNativeJson:
    """ Cleans a string to ensure it can be read by the native Python JSON library. """
    if data is None or isinstance(data, (bool, int, float, str)):
        return data
    elif isinstance(data, dict):
        return {
            str(x): clean_for_json(data[x], _max_depth) for x in data
        }
    elif isinstance(data, list):
        return [clean_for_json(x, _max_depth) for idx, x in enumerate(data)]
    else:
        result = _clean_for_native_json(data)
        if result is None:
            raise TypeError(f"Invalid item for json encode [{data.__class__.__name__}]")
        elif _max_depth <= 0:
            raise RecursionError(f"Recursion limit exceeded for JSON encoding")
        else:
            result = clean_for_json(result, _max_depth - 1)
        return result

def load_dict(__obj: JsonDictString) -> dict:
    """ Loads an object from a JSON string and ensures it is a dictionary. """
    d = load_string(__obj)
    if not isinstance(d, dict):
        raise ValueError(f'JSON string was not a dictionary')
    return d

def load_list(__obj: JsonListString) -> list:
    """ Loads an object from a JSON string and ensures it is a list"""
    d = load_string(__obj)
    if not isinstance(d, list):
        raise ValueError(f'JSON string was not a list')
    return d

def load_set(__obj: JsonListString) -> set:
    """ Loads an object from a JSON string and ensures it is a list"""
    d = load_string(__obj)
    if not isinstance(d, list):
        raise ValueError(f'JSON string was not a set')
    return set(d)