import enum
import typing as _t

LanguageName = _t.Literal["en", "und", "fr"]
LanguageDict = dict[LanguageName, str]

if _t.TYPE_CHECKING:
    import datetime as _datetime
    import collections.abc as _abc
    import uuid as _uuid
    import decimal as _decimal
    import numpy as _np

    # Types to support JSON objects
    type _SupportsNativeJson = bool | str | float | int | None | enum.IntEnum
    type _SupportsExtendedJson = _SupportsNativeJson | _datetime.datetime | _datetime.date | _datetime.time | _uuid.UUID

    type SupportsNativeJson = _SupportsNativeJson | dict[str, _SupportsNativeJson] | list[_SupportsNativeJson]
    type SupportsExtendedJson = _SupportsExtendedJson | _abc.Mapping[str, _SupportsExtendedJson] | _abc.Iterable[_SupportsExtendedJson]
    JsonString = _t.AnyStr | memoryview
    JsonDictString = JsonString
    JsonListString = JsonString

    type AcceptAsLanguageDict = str | LanguageDict

    NumberLike = int | str | float | _decimal.Decimal
    NumpyNumberLike = NumberLike | _np.int8 | _np.int16 | _np.int32 | _np.int64 | _np.float16 | _np.float32 | _np.float64

    class SupportsBool(_t.Protocol):
        def __bool__(self) -> bool: ...

    class SupportsString(_t.Protocol):
        def __str__(self) -> str: ...