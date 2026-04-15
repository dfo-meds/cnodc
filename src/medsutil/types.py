""" Type hints that are widely used throughout the application. """
import io as _io
import typing as _t
import datetime as _datetime
import collections.abc as _abc
import uuid as _uuid
import decimal as _decimal
import numpy as _np
import enum as _enum
import os as _os

from uncertainties import UFloat

# Types to support localization
LanguageName = _t.Literal[
    "und",  # Language not adequately defined or equally valid in French and English (really this should be mis probably)
    "en",  # Canadian English
    "fr"  # Canadian French
]
""" Type hint for a supported BCP-47 language codes. """

LanguageDict = dict[str, str]
""" Type hint for a dictionary of language codes to localized values. """

AcceptAsLanguageDict = str | LanguageDict
""" Type hint for a string (of undefined locale) or dictionary. """

# Types to support JSON objects
type _SupportsNativeJson = bool | str | float | int | None | _enum.IntEnum
type _SupportsNativeJsonAll= _SupportsNativeJson | dict[str, _SupportsNativeJson] | list[_SupportsNativeJson]
SupportsNativeJson = _SupportsNativeJsonAll
"""Type hint for objects that can be serialized with Python's native json library. """

type _SupportsExtendedJson = _SupportsNativeJson | _datetime.datetime | _datetime.date | _datetime.time | _uuid.UUID | _enum.Enum
type _SupportsExtendedJsonAll = _SupportsExtendedJson | _abc.Mapping[str, _SupportsExtendedJson] | _abc.Iterable[_SupportsExtendedJson]
SupportsExtendedJson = _SupportsExtendedJsonAll
"""Type hint for objects that can be serialzed with our own cnodc.util.json library. """

JsonString = str | bytes | bytearray
""" Type hint for a string that can be decoded as JSON. """

JsonDictString = str | bytes | bytearray
""" Type hint for a string that can be decoded as JSON into a dictionary. """

JsonListString = str | bytes | bytearray
""" Type hint for a string that can be decoded as JSON into a list. """

# Types to support numeric values
NumberLike = int | str | float | _decimal.Decimal
""" Type hint for a number-like value or a string that can be converted to a number. """

ISOString = str
""" Type hint for a string that is a date in ISO format. """

type _AcceptAsJsonDict[X: SupportsString,Y: SupportsExtendedJson] = _t.Mapping[X, Y] | JsonDictString
AcceptAsJsonDict = _AcceptAsJsonDict
""" Type hint for something that is a mapping or one that can be loaded from JSON. """

type _AcceptAsJsonList[X: SupportsExtendedJson] = _t.Iterable[X] | JsonListString
AcceptAsJsonList = _AcceptAsJsonList
""" Type hint for something that is an iterable or one that can be loaded from JSON. """

type _AcceptAsJsonSet[X: SupportsExtendedJson] = _t.Iterable[X] | JsonListString
AcceptAsJsonSet = _AcceptAsJsonSet
""" Type hint for something that is an iterable or one that can be loaded from JSON. """

AcceptAsDateTime = _datetime.date | ISOString
""" Type hint for something that is a datetime.date or a string that can be loaded. """

type _AcceptAsEnum[EnumType] = EnumType | _t.Any
AcceptAsEnum = _AcceptAsEnum
""" Type hint for an enum or its value. """

AcceptAsInteger = int | _t.SupportsIndex | _t.SupportsInt | str | _abc.Buffer | UFloat
""" Type hint for something that can be accepted as an integer. """

AcceptAsFloat = int | _t.SupportsIndex | _t.SupportsFloat | str | _abc.Buffer | float | UFloat
""" Type hint for something that can be accepted as a float. """

NumpyNumberLike = NumberLike | _np.int8 | _np.int16 | _np.int32 | _np.int64 | _np.float16 | _np.float32 | _np.float64
""" Type hint for a number-like value but including numpy options. """

# Binary type hints
ByteStrings = _t.Iterable[_t.ByteString]
""" Type hint for a iterable of byte strings. """

# Time-based type hints
TimeZoneString = str
""" Type hint for a string that exists in the tz database (e.g. Etc/UTC or America/Toronto). """

TimeZoneInfo = TimeZoneString | _datetime.tzinfo | _datetime.timedelta
""" Type hint for an object that can be interpreted as a time zone. """

# Path type hints
PathString = str
""" Type hint for a string that represents a local path. """

PathLike = PathString | _os.PathLike
""" Type hint for a path object. """

SupportsBinaryRead = _io.Reader[bytes]
""" Type hint for something that supports reading binary data via read(). """

SupportsBinaryWrite = _io.Writer[bytes]
""" Type hint for something that supports writing binary data via write(). """

# Protocols for basic operations
class SupportsBool(_t.Protocol):
    """ Type hint for an object that supports being converted into a boolean value. """
    def __bool__(self) -> bool: ...

class SupportsString(_t.Protocol):
    """ Type hint for an object that supports being converted into a string value. """
    def __str__(self) -> str: ...

class SupportsEvent(_t.Protocol):
    """ Type hint for an event object such as threading.Event or multiprocessing.Event. """

    def is_set(self) -> bool: ...
    def clear(self): ...
    def set(self): ...

class SupportsHashUpdate(_t.Protocol):
    def update(self, b: bytes): ...

def is_binary_writable(obj: _t.Any) -> _t.TypeIs[SupportsBinaryWrite]:
    return hasattr(obj, 'write')

def is_binary_readable(obj: _t.Any) -> _t.TypeIs[SupportsBinaryRead]:
    return hasattr(obj, 'read')