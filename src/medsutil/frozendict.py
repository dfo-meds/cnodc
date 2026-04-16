import collections.abc
import copy
import typing as t
from typing import Hashable


class FrozenDict[X: Hashable, Y: Hashable](collections.abc.Mapping[X, Y], collections.abc.Hashable):

    __slots__ = ('__keys', '__values', '__hash')

    def __init__(self, d: t.Mapping[X, Y] = None, **kwargs: Y):
        fd = {}
        for key in kwargs:
            self._add_hashable(key, kwargs[key], fd)
        if d:
            for key in d:
                self._add_hashable(key, d[key], fd)
        self.__values: dict[X, Y] = fd
        self.__keys: list[X] = sorted(list(fd.keys()))
        self.__hash: t.Optional[int] = None

    def __repr__(self) -> str:
        s = f'<{self.__class__.__name__}' + '{'
        s += ', '.join(f"{repr(x)}: {repr(self.__values[x])}" for x in self.__keys)
        s += "}>"
        return s

    def __str__(self) -> str:
        s = '{'
        s += ', '.join(f"{repr(x)}: {repr(self.__values[x])}" for x in self.__keys)
        s += "}"
        return s

    def __getitem__(self, item: X) -> Y:
        return copy.deepcopy(self.__values[item])

    def __iter__(self) -> t.Iterator[X]:
        return self.keys().__iter__()

    def __len__(self) -> int:
        return self.__keys.__len__()

    def __contains__(self, item: X) -> bool:
        return self.__keys.__contains__(item)

    def __eq__(self, other: t.Mapping) -> bool:
        try:
            return self.items() == other.items()
        except AttributeError:
            return False

    def __ne__(self, other: t.Mapping) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        if self.__hash is None:
            data = []
            for key in self.__keys:
                data.append(hash(key))
                data.append(hash(self.__values[key]))
            self.__hash = hash(tuple(data))
        return t.cast(int, self.__hash)

    def __copy__(self) -> FrozenDict[X,Y]:
        return FrozenDict(self.__values)

    def __deepcopy__(self, memo) -> FrozenDict[X,Y]:
        return FrozenDict(copy.deepcopy(self.__values, memo))

    def keys(self) -> list[X]:
        return copy.deepcopy(self.__keys)

    def values(self) -> list[Y]:
        return copy.deepcopy(list(self.__values.values()))

    def items(self) -> t.Iterable[tuple[X, Y]]:
        for key in self.__keys:
            yield copy.deepcopy(key), copy.deepcopy(self.__values[key])

    def get[Z](self, item: X, /, default: Z = ...) -> Y | Z:
        if item not in self.__keys:
            if default is ...:
                raise KeyError(f'Missing key {item}')
            return default
        return self.__keys[item]

    @staticmethod
    def _add_hashable(key: X, value: Y, d: dict[X, Y]):
        if not isinstance(key, t.Hashable):
            raise TypeError(f'Key [{key}] is not hashable')
        if not isinstance(value, t.Hashable):
            raise TypeError(f'Value [{key}={value}] is not hashable')
        d[key] = value
