""" Provides an enum that accepts a tuple of values which are all equivalent."""
import typing as t
import collections.abc
import sys
import enum

if sys.version_info[0] == 3 and sys.version_info[1] >= 13:
    # this is a lot easier to do in 3.13+

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
    # Our more complicated hack
    # TODO: we can probably remove this? since we're supporting only 3.14+

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


def variants(x: str, lowercase: bool = True, nospace: bool = True) -> tuple[str, ...]:
    """ Given a string and the option to lowercase and/or remove spaces, generates all possible variations on it. """

    mods: list[t.Callable[[str], str]] = []
    if lowercase:
        mods.append(lambda y: y.lower())
    if nospace:
        mods.append(lambda y: y.replace(' ', ''))

    return tuple(x for x in _combo_values(x, mods))


def _combo_values(x: str, mods: list[t.Callable[[str], str]]) -> t.Generator[str, None, None]:
    """ Retrieves all possible combination of values """
    yield x
    for y in _mod_combos(mods):
        yield y(x)

def _mod_combos(mods: list[t.Callable[[str], str]]) -> t.Iterable[t.Callable[[str], str]]:
    """ Creates a power set of all the functions.
        TODO: this function has issues, it only works with two functions not the powerset.
    """
    for i in range(0, len(mods)):
        yield mods[i]
        for j in _mod_combos(mods[i+1:]):
            yield lambda x: j(mods[i](x))
