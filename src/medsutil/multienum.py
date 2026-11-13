"""Provides an enum that accepts a tuple of values which are all equivalent."""

import collections.abc
import enum
import typing as t


class MultiValuedEnum(enum.Enum):
    def __new__(cls, value, *aliases):
        self = object.__new__(cls)
        self._value_ = value
        for v in aliases:
            self._add_value_alias_(v)
        return self

    @classmethod
    def _missing_(cls, value):
        if hasattr(cls, "_convert_value"):
            value = cls._convert_value(value)
        if isinstance(value, cls):
            return value
        if (
            isinstance(value, collections.abc.Hashable)
            and value in cls._value2member_map_
        ):
            return cls._value2member_map_[value]
        for x in cls:
            if x.value == value:
                return x

    @classmethod
    def _convert_value(cls, v: t.Any) -> t.Self: ...


def variants(x: str, lowercase: bool = True, nospace: bool = True) -> tuple[str, ...]:
    """Given a string and the option to lowercase and/or remove spaces, generates all possible variations on it."""

    mods: list[t.Callable[[str], str]] = []
    if lowercase:
        mods.append(lambda y: y.lower())
    if nospace:
        mods.append(lambda y: y.replace(" ", ""))

    return tuple(x for x in _combo_values(x, mods))


def _combo_values(x: str, mods: list[t.Callable[[str], str]]) -> t.Generator[str]:
    """Retrieves all possible combination of values"""
    yield x
    for y in _mod_combos(mods):
        yield y(x)


def _mod_combos(
    mods: list[t.Callable[[str], str]],
) -> t.Iterable[t.Callable[[str], str]]:
    """Creates a power set of all the functions.
    TODO: this function has issues, it only works with two functions not the powerset.
    """
    for i in range(len(mods)):
        yield mods[i]
        for j in _mod_combos(mods[i + 1 :]):
            yield lambda x: j(mods[i](x))
