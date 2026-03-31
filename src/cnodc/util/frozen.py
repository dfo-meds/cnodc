from __future__ import annotations

import collections.abc
import typing as t


class FrozenDict(collections.abc.Mapping, collections.abc.Hashable):

    __slots__ = ('__keys', '__values', '__hash')

    def __init__(self, **kwargs):
        keys = []
        for key in kwargs:
            if not hasattr(key, '__hash__'):
                raise TypeError(f'Key [{key}] is not hashable')
            if not hasattr(kwargs[key], '__hash__'):
                raise TypeError(f'Value [{key}={kwargs[key]}] is not hashable')
            keys.append(key)
        keys.sort()
        self.__keys: tuple = tuple(keys)
        self.__values: tuple = tuple(kwargs[x] for x in keys)
        self.__hash = None

    def __getitem__(self, item):
        index = self.__keys.index(item)
        return self.__values[index]

    def __iter__(self):
        return self.__keys.__iter__()

    def __len__(self):
        return self.__keys.__len__()

    def __contains__(self, item):
        return self.__keys.__contains__(item)

    def keys(self):
        return self.__keys

    def values(self):
        return self.__values

    def items(self):
        for i in range(0, len(self.__keys)):
            yield self.__keys[i], self.__values[i]

    def get(self, item, /, default=...):
        if item not in self.__keys:
            if default is ...:
                raise KeyError(f'Missing key {item}')
            return default
        return self.__keys[item]

    def __eq__(self, other: t.Self) -> bool:
        try:
            return self.keys() == other.keys() and self.values() == other.values()
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if self.__hash is None:
            data = []
            for idx, key in enumerate(self.__keys):
                data.append(key)
                data.append(self.__values[idx])
            self.__hash = hash(tuple(data))
        return self.__hash

