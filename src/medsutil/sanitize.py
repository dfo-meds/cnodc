import datetime
import enum
import math
import typing as t

import netCDF4 as nc
import unicodedata
import numpy as np
import numpy.typing as npt
from numpy.ma.core import MaskedConstant, MaskedArray
from uncertainties import UFloat
from decimal import Decimal

from uncertainties.core import AffineScalarFunc

from medsutil import json as json
from medsutil.awaretime import AwareDateTime
import medsutil.types as ct


UNICODE_SPACES = "\t\u00A0\u180E\u2002\u2000\u2003\u2004\u2005\u2006\u2008\u2007\u2009\u200A\u200B\u202F\u205F\u3000\uFEFF"
""" All space characters in Unicode """

UNICODE_DASHES = "\u058A\u05BE\u1806\u2010\u2011\u2012\u2013\u2014\u2015\u2E3A\u2E3B\uFE58\uFE63\uFF0D"
""" All dash characters in Unicode. """


def netcdf_bytes_to_string(byte_sequence: str | t.ByteString, encoding='utf-8') -> str:
    """ Converts NetCDF characters to a Python string object, if necessary. """
    return DataCoercer.nc_bytes_as_string(byte_sequence, encoding)


def netcdf_string_to_vlen_bytes(s: t.Sequence[str] | str) -> npt.NDArray:
    """ Converts a string (or a sequence of strings) into a variable-length NetCDF-compatible array. """
    return DataCoercer.string_as_nc_bytes(s)

def netcdf_string_to_bytes(s: t.Sequence[str] | str, fixed_len: int) -> npt.NDArray:
    """ Converts a string (or a sequence of strings) into a fixed-length NetCDF-compatible array. """
    return DataCoercer.string_as_nc_bytes(s, fixed_len)

def utf_normalize_string(value: str) -> str:
    """ Normalizes a string to be consistent.

    The exact procedure is:
    - Convert to UTF-8 NFC encoding
    - Replace Windows new lines with Linux new lines
    - Replace tabs with spaces
    - Replace all forms of spaces with the simple ASCII space
    - Replace all forms of dashes with the simple ASCII hyphen
    - Remove control and null characters
    - Remove leading and trailing whitespace
    - Replace two or more consecutive spaces with single spaces
    """
    return DataCoercer.as_normalized_string(value)


def unnumpy(numpy_val):
    """ Converts any numpy-like objects into native Python objects, and replaces masked values with None. """
    return DataCoercer.numpy_as_native(numpy_val)


class DataCoercer:

    @staticmethod
    def as_float(f: ct.AcceptAsFloat) -> float:
        if isinstance(f, AffineScalarFunc):
            return f.nominal_value
        else:
            return float(f)

    @staticmethod
    def nc_bytes_as_string(byte_sequence: str | t.ByteString, encoding='utf-8') -> str:
        if isinstance(byte_sequence, str):
            return DataCoercer.as_normalized_string(byte_sequence)
        return DataCoercer.as_normalized_string(
            b''.join(
                bytes(x)
                for x in byte_sequence
            ).replace(b'\x00', b'').decode(encoding)
        )

    @staticmethod
    def string_as_nc_bytes(s: t.Sequence[str] | str, fixed_len: int = None) -> npt.NDArray:
        """ Converts a string (or a sequence of strings) into a fixed- or variable-length NetCDF-compatible array. """
        if isinstance(s, str):
            s = (s,)
        if fixed_len is not None:
            return nc.stringtochar(np.array(s, dtype=f'U{fixed_len}'), encoding='none', n_strlen=fixed_len)
        return np.array(s, dtype=object)

    @t.overload
    @staticmethod
    def numpy_as_native(x: np.float16 | np.float32 | np.float64) -> float: ...

    @t.overload
    @staticmethod
    def numpy_as_native(x: npt.NDArray[np.float16 | np.float32 | np.float64]) -> list[float | None]: ...

    @t.overload
    @staticmethod
    def numpy_as_native(x: np.uint8 | np.uint16 | np.uint32 | np.uint64 | np.int8 | np.int16 | np.int32 | np.int64) -> int: ...

    @t.overload
    @staticmethod
    def numpy_as_native(x: npt.NDArray[np.uint8 | np.uint16 | np.uint32 | np.uint64 | np.int8 | np.int16 | np.int32 | np.int64]) -> list[int | None]: ...

    @t.overload
    @staticmethod
    def numpy_as_native(x: np.bool) -> bool: ...

    @t.overload
    @staticmethod
    def numpy_as_native(x: npt.NDArray[np.bool]) -> list[bool | None]: ...

    @t.overload
    @staticmethod
    def numpy_as_native(x: MaskedConstant) -> None: ...

    @t.overload
    @staticmethod
    def numpy_as_native[T: (int, float, str, bool, Decimal, UFloat)](x: T) -> T: ...

    @t.overload
    @staticmethod
    def numpy_as_native[T: (int, float, str, bool, Decimal, UFloat)](x: t.Sequence[T]) -> list[T]: ...

    @t.overload
    @staticmethod
    def numpy_as_native[T](x: T) -> T: ...

    @staticmethod
    def numpy_as_native(x: t.Any) -> t.Any:
        if x is None:
            return None
        elif isinstance(x, (np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64, np.bool, np.float16, np.float32, np.float64)):
            return DataCoercer.numpy_as_native(x.item())
        elif isinstance(x, MaskedConstant):
            return None
        elif isinstance(x, str):
            return utf_normalize_string(x)
        elif isinstance(x, (int, bool)):
            return x
        elif isinstance(x, float):
            return x if not math.isnan(x) else None
        elif isinstance(x, Decimal):
            return x if not x.is_nan() else None
        elif isinstance(x, MaskedArray):
            if x.ndim > 0:
                return [DataCoercer.numpy_as_native(x) for x in x.tolist(None)]
            return DataCoercer.numpy_as_native(x.item())
        elif isinstance(x, np.ndarray):
            if x.ndim > 0:
                return [DataCoercer.numpy_as_native(x) for x in x.tolist()]
            return DataCoercer.numpy_as_native(x.item())
        elif isinstance(x, t.Sequence):
            return [DataCoercer.numpy_as_native(x) for x in x]
        else:
            return x

    @staticmethod
    def as_datetime(x: ct.AcceptAsDateTime) -> datetime.datetime:
        if isinstance(x, datetime.datetime):
            return x
        if isinstance(x, datetime.date):
            return datetime.datetime(x.year, x.month, x.day)
        return datetime.datetime.fromisoformat(x)

    @staticmethod
    def as_awaretime(x: ct.AcceptAsDateTime) -> AwareDateTime:
        if isinstance(x, datetime.datetime):
            return AwareDateTime.from_datetime(x)
        elif isinstance(x, datetime.date):
            return AwareDateTime(x.year, x.month, x.day)
        else:
            return AwareDateTime.fromisoformat(x)

    @staticmethod
    def as_date(x: ct.AcceptAsDateTime) -> datetime.date:
        if isinstance(x, datetime.datetime):
            return x.date()
        elif isinstance(x, datetime.date):
            return x
        else:
            return datetime.date.fromisoformat(x)

    @staticmethod
    def as_list[AcceptType, StoreType](
            obj: t.Iterable[AcceptType],
            value_coerce: t.Callable[[AcceptType], StoreType] = None,
            str_coerce: t.Callable[[str], t.Iterable[AcceptType]] = None) -> list[StoreType]:
        if isinstance(obj, str):
            if str_coerce is not None:
                obj = str_coerce(obj)
            else:
                raise ValueError('Strings not accepted without string coercion method')
        if value_coerce is None:
            if isinstance(obj, list): return obj
            return list(x for x in obj)
        return list(value_coerce(x) for x in obj)

    @staticmethod
    def as_set[AcceptType, StoreType](
            obj: t.Iterable[AcceptType],
            value_coerce: t.Callable[[AcceptType], StoreType] = None,
            str_coerce: t.Callable[[str], t.Iterable[AcceptType]] = None) -> set[StoreType]:
        if isinstance(obj, str):
            if str_coerce is not None:
                obj = str_coerce(obj)
            else:
                raise ValueError('Strings not accepted without string coercion method')
        if value_coerce is None:
            if isinstance(obj, set): return obj
            return set(x for x in obj)
        return set(value_coerce(x) for x in obj)

    @staticmethod
    def as_dict[AcceptKeyType, AcceptType, StoreKeyType, StoreType](
            obj: t.Mapping[AcceptKeyType | StoreKeyType, AcceptType | StoreType] | str,
            key_coerce: t.Callable[[AcceptKeyType | StoreKeyType], StoreKeyType] = None,
            value_coerce: t.Callable[[AcceptType | StoreType], StoreType] = None,
            str_coerce: t.Callable[[str], dict[AcceptKeyType, AcceptType]] = None) -> dict[StoreKeyType, StoreType]:
        if str_coerce is not None and isinstance(obj, str):
            obj = str_coerce(obj)
        if key_coerce is None and value_coerce is None:
            if isinstance(obj, dict):
                return obj
            return {x: obj[x] for x in obj}
        elif key_coerce is None:
            return {x: value_coerce(obj[x]) for x in obj}
        elif value_coerce is None:
            return {key_coerce(x): obj[x] for x in obj}
        return {key_coerce(x): value_coerce(obj[x]) for x in obj}

    @staticmethod
    def as_enum[X](obj: ct.AcceptAsEnum[X], enum_type: type[X]) -> X:
        if obj is None or obj == '':
            return None
        if isinstance(obj, enum_type):
            return obj
        return enum_type(obj)

    @staticmethod
    def date_as_iso_string(x: datetime.date) -> str:
        return x.isoformat()

    @staticmethod
    def enum_as_value(obj: enum.Enum) -> t.Any:
        return obj.value

    @t.overload
    @staticmethod
    def as_json_string(d: t.Mapping[ct.SupportsString, ct.SupportsExtendedJson]) -> ct.JsonDictString: ...

    @t.overload
    @staticmethod
    def as_json_string(d: t.Iterable[ct.SupportsExtendedJson]) -> ct.JsonListString: ...

    @t.overload
    @staticmethod
    def as_json_string(d: ct.SupportsExtendedJson) -> str: ...

    @staticmethod
    def as_json_string(d):
        return json.dumps(d)

    @t.overload
    @staticmethod
    def as_json_safe(d: t.Mapping[ct.SupportsString, ct.SupportsExtendedJson]) -> dict[str, ct.SupportsNativeJson]: ...

    @t.overload
    @staticmethod
    def as_json_safe(d: t.Iterable[ct.SupportsExtendedJson]) -> list[ct.SupportsNativeJson]: ...

    @t.overload
    @staticmethod
    def as_json_safe(d: ct.SupportsExtendedJson) -> ct.SupportsNativeJson: ...

    @staticmethod
    def as_json_safe(d):
        return json.clean_for_json(d)

    @staticmethod
    def as_i18n_text(v: ct.AcceptAsLanguageDict) -> ct.LanguageDict:
        if isinstance(v, str):
            return {'und': v}
        return v

    @staticmethod
    def as_memoryview(b: t.ByteString) -> memoryview:
        return memoryview(b)

    @staticmethod
    def as_bytes(b: t.ByteString) -> bytes:
        return bytes(b)

    @staticmethod
    def as_bytearray(b: t.ByteString) -> bytearray:
        return bytearray(b)

    @staticmethod
    def iterable_as_memoryview(b: t.Iterable[t.ByteString]) -> memoryview:
        return memoryview(DataCoercer.iterable_as_bytearray(b))

    @staticmethod
    def iterable_as_bytes(b: t.Iterable[t.ByteString]) -> bytes:
        return b''.join(b)

    @staticmethod
    def iterable_as_bytearray(b: t.Iterable[t.ByteString]) -> bytearray:
        ba = bytearray()
        for byte_ in b:
            ba.extend(byte_)
        return ba

    @staticmethod
    def as_normalized_string(value: str) -> str:
        value = unicodedata.normalize('NFC', value)
        value = value.replace("\r\n", "\n")
        value = value.replace("\t", " ")
        for c in UNICODE_SPACES:
            value = value.replace(c, " ")
        for c in UNICODE_DASHES:
            value = value.replace(c, "-")
        value = ''.join(c for c in value if (not unicodedata.category(c) == 'Cc') or c == "\n")
        value = value.replace("\x00", "")
        value = value.strip(" ")
        while "  " in value:
            value = value.replace("  ", " ")
        return value


class DataValidator:

    @staticmethod
    def type_is[T](__obj: t.Any, required_type: type[T] | tuple[type[T], ...] = None) -> t.TypeGuard[T]:
        if required_type is not None and not isinstance(__obj, required_type):
            raise ValueError('Invalid object type')
        return True

    @staticmethod
    def iterable_meets_all[T](__obj: t.Iterable[T], validators: list[t.Callable[[T], t.NoReturn]]):
        for item in __obj:
            for v in validators:
                v(item)
        return True

    @staticmethod
    def mapping_meets_all[X, Y](
            __obj: t.Mapping[X, Y],
            key_validators: list[t.Callable[[X], t.NoReturn]] = None,
            value_validators: list[t.Callable[[Y], t.NoReturn]] = None,
    ):
        key_validators = key_validators or []
        value_validators = value_validators or []
        for key, value in __obj.items():
            for key_v in key_validators:
                key_v(key)
            for value_v in value_validators:
                value_v(value)
        return True

coerce = DataCoercer
require = DataValidator