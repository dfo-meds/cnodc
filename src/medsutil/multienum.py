import typing as t
import collections.abc
import sys
import enum

if sys.version_info[0] == 3 and sys.version_info[1] >= 13:

    class MultiValuedEnum(enum.Enum):

        def __new__(cls, value, *aliases):
            self = object.__new__(cls)
            self._value_ = value
            for v in aliases:
                self._add_value_alias_(v)
            return self

        @classmethod
        def _missing_(cls, value):
            if hasattr(cls, '_convert_value'):
                value = cls._convert_value(value)
            if isinstance(value, cls):
                return value
            if isinstance(value, collections.abc.Hashable) and value in cls._value2member_map_:
                return cls._value2member_map_[value]
            for x in cls:
                if x.value == value:
                    return x

        @classmethod
        def _convert_value(cls, v: t.Any) -> t.Self: ...

else:

    class MultiValuedEnum(enum.Enum):

        _aliases: dict[str, t.Self] = enum.nonmember({})

        def __new__(cls, value, *aliases):
            self = object.__new__(cls)
            self._value_ = value
            for v in aliases:
                self._add_value_alias(v)
            return self

        @classmethod
        def _missing_(cls, value):
            if hasattr(cls, '_convert_value'):
                value = cls._convert_value(value)
            if isinstance(value, collections.abc.Hashable) and hasattr(cls, '_aliases'):
                aliases = cls._aliases
                if value in aliases:
                    value = aliases[value]
            if isinstance(value, cls):
                return value
            if isinstance(value, collections.abc.Hashable) and value in cls._value2member_map_:
                return cls._value2member_map_[value]
            for x in cls:
                if x.value == value:
                    return x

        @classmethod
        def _convert_value(cls, v: t.Any) -> t.Self: return v

        @classmethod
        def _compare_search(cls, v: t.Any, coerce: t.Callable[[t.Any], t.Any]):
            coerced_v = coerce(v)
            for value in cls._value2member_map_:
                if coerced_v == coerce(value):
                    return cls._value2member_map_[value]
            return v


        def _add_value_alias(self, x: str):
            self.__class__._aliases[x] = self._value_


