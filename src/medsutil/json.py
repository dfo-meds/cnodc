"""
Wrapper around JSON functions for speedups and consistency.

The Python JSON library is inherently slow. There are several faster options. If they are available, we will
use them. This module provides a wrapper around either orjson or the native JSON module.
"""

import datetime
import enum
import typing as t
import uuid

import medsutil.types as ct
from medsutil.exceptions import CodedError
from medsutil.frozendict import FrozenDict


class JSONError(CodedError):
    CODE_SPACE = "JSON"


class JSONEncodeError(JSONError): ...
class JSONDecodeError(JSONError): ...


try:
    # orjson is very fast
    import orjson

    def _clean_for_orjson(x):
        """Cleans a value for JSON.
        :raises TypeError: If the object isn't supported.
        """
        try:
            return clean_for_json(x)
        except TypeError as ex:
            raise orjson.JSONEncodeError(
                f"Cannot encode object of type [{x.__class__.__name__}]"
            ) from ex

    def dumpb(__obj: ct.SupportsExtendedJson, /) -> bytes:
        """Returns a JSON string in utf-8 encoding from the given JSON-compatible object."""
        try:
            return orjson.dumps(__obj, default=_clean_for_orjson)
        except orjson.JSONEncodeError as ex:
            raise JSONEncodeError(str(ex), 1000) from ex

    def dumps(__obj: ct.SupportsExtendedJson, /) -> str:
        """Returns a JSON string for the given object."""
        try:
            return orjson.dumps(__obj, default=_clean_for_orjson).decode("utf-8")
        except orjson.JSONEncodeError as ex:
            raise JSONEncodeError(str(ex), 1000) from ex

    def dumpp(__obj: ct.SupportsExtendedJson, /) -> str:
        """Returns a prettified JSON string for the given object."""
        try:
            return orjson.dumps(
                __obj, default=_clean_for_orjson, option=orjson.OPT_INDENT_2
            ).decode("utf-8")
        except orjson.JSONEncodeError as ex:
            raise JSONEncodeError(str(ex), 1000) from ex

    def loads(__obj: ct.JsonString, /) -> ct.SupportsNativeJson:
        """Converts a JSON string into the correct object."""
        try:
            return orjson.loads(__obj)
        except orjson.JSONEncodeError as ex:
            raise JSONDecodeError(str(ex), 1001) from ex


except ModuleNotFoundError:
    # Basic json wrapper
    import json

    def _clean_for_json(x):
        """Cleans the value for JSON."""
        try:
            return clean_for_json(x)
        except TypeError:
            return None

    def dumpb(__obj: ct.SupportsExtendedJson, /) -> bytes:
        """Returns a JSON string in utf-8 encoding from the given JSON-compatible object."""
        try:
            return json.dumps(__obj, default=_clean_for_json).encode("utf-8")
        except TypeError as ex:
            raise JSONEncodeError(str(ex), 1002) from ex

    def dumps(__obj: ct.SupportsExtendedJson, /) -> str:
        """Returns a JSON string for the given object."""
        try:
            return json.dumps(__obj, default=_clean_for_json)
        except TypeError as ex:
            raise JSONEncodeError(str(ex), 1002) from ex

    def dumpp(__obj: ct.SupportsExtendedJson, /) -> str:
        """Returns a prettified JSON string for the given object."""
        try:
            return json.dumps(__obj, default=_clean_for_json, indent=2)
        except TypeError as ex:
            raise JSONEncodeError(str(ex), 1002) from ex

    def loads(__obj: ct.JsonString, /) -> ct.SupportsNativeJson:
        """Converts a JSON string into the correct object."""
        try:
            return json.loads(__obj)
        except TypeError as ex:
            raise JSONDecodeError(str(ex), 1003) from ex


def clean_for_json(__obj, /):
    """General clean-up function to convert to JSON.
    :raises TypeError: If the object isn't supported.
    """
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
    raise JSONEncodeError(f"Invalid data type [{__obj.__class__.__name__}] for a JSON object", 2000)


def load_dict(__obj: ct.JsonDictString, /) -> dict[str, ct.SupportsNativeJson]:
    """Loads an object from a JSON string and ensures it is a dictionary."""
    d = loads(__obj)
    if not isinstance(d, dict):
        raise JSONDecodeError(f"JSON string was not a dictionary, is {d.__class__.__name__}", 2001)
    return d


def load_list(__obj: ct.JsonListString, /) -> list[ct.SupportsNativeJson]:
    """Loads an object from a JSON string and ensures it is a list"""
    d = loads(__obj)
    if not isinstance(d, list):
        raise JSONDecodeError(f"JSON string was not a list, is {d.__class__.__name__}", 2002)
    return d


def load_set(__obj: ct.JsonListString, /) -> set[ct.SupportsNativeJson]:
    """Loads an object from a JSON string and ensures it is a list"""
    d = loads(__obj)
    if not isinstance(d, list):
        raise JSONDecodeError(f"JSON string was not a set, is {d.__class__.__name__}", 2003)
    return set(d)
