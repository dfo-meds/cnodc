import itertools
import typing as t
from  collections.abc import MutableMapping, MutableSequence
from typing import Mapping

T = t.TypeVar("T")


class LazyLoadDict(MutableMapping, t.Generic[T]):

    __slots__ = ('_dict', '_loaded', '_constructor')

    def __init__(self, constructor: callable):
        self._dict: dict[str, t.Union[t.Any, T]] = {}
        self._loaded: dict[str, bool] = {}
        self._constructor = constructor

    def __bool__(self) -> bool:
        return bool(self._dict)

    def __len__(self):
        return len(self._dict)

    def __iter__(self) -> t.Iterable[T]:
        yield from self.keys()

    def __getitem__(self, item: str) -> T:
        return self._load(item)

    def __setitem__(self, item: str, value: T):
        self._dict[item] = value
        self._loaded[item] = True

    def __delitem__(self, key):
        del self._dict[key]
        del self._loaded[key]

    def __contains__(self, item: str) -> bool:
        return item in self._dict

    def __eq__(self, other: Mapping) -> bool:
        try:
            all_keys = set(itertools.chain(self.keys(), other.keys()))
            return all(self[k] == other[k] for k in all_keys)
        except (KeyError, AttributeError):
            return False

    def keys(self):
        return self._dict.keys()

    def clear(self):
        self._dict.clear()
        self._loaded.clear()

    def _load(self, item: str):
        if not self._loaded[item]:
            self._dict[item] = self._constructor(self._dict[item])
            self._loaded[item] = True
        return self._dict[item]

    def set(self, key, value):
        self[key] = value

    def get(self, key, default=None):
        try:
            return self._load(key)
        except KeyError:
            return default

    def to_mapping(self):
        return {
            x: (self._dict[x] if not self._loaded[x] else self._dict[x].to_mapping()) for x in self._dict
        }

    def from_mapping(self, map_: dict):
        self._dict = map_
        self._loaded = {x: False for x in map_}


class LazyLoadList(MutableSequence, t.Generic[T]):
    """This class holds a list of dictionaries and maps them into objects as they are needed."""

    __slots__ = ('_list', '_loaded', '_constructor')

    def __init__(self, constructor: callable):
        self._list: list[t.Union[t.Any, T]] = []
        self._loaded: list[bool] = []
        self._constructor = constructor

    def __bool__(self):
        return bool(self._list)

    def __iter__(self):
        yield from self.iterate_with_load()

    def __len__(self):
        return len(self._list)

    def __getitem__(self, index: int) -> T:
        return self._load(index)

    def __setitem__(self, key: int, value: T):
        self._list[key] = value
        self._loaded[key] = True

    def __delitem__(self, key):
        del self._list[key]
        del self._loaded[key]

    def clear(self):
        self._list.clear()
        self._loaded.clear()

    def insert(self, index, value):
        self._list.insert(index, value)
        self._loaded.insert(index, True)

    def extend(self, items: t.Iterable[T]):
        for item in items:
            self.append(item)

    def append(self, item: T):
        self._list.append(item)
        self._loaded.append(True)

    def _load(self, idx: int) -> T:
        if not self._loaded[idx]:
            self._list[idx] = self._constructor(self._list[idx])
            self._loaded[idx] = True
        return self._list[idx]

    def iterate_with_load(self) -> t.Iterable[T]:
        for idx in range(0, len(self._list)):
            yield self._load(idx)

    def to_mapping(self) -> list:
        l = []
        for idx in range(0, len(self._list)):
            if not self._loaded[idx]:
                l.append(self._list[idx])
            else:
                l.append(self._list[idx].to_mapping())
        return l

    def from_mapping(self, map_: list):
        self._list = map_
        self._loaded = [False for _ in range(0, len(self._list))]
