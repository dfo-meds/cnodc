import itertools
import typing as t
from typing import Mapping
import medsutil.types as ct



class LazyLoadDict[V]:

    __slots__ = ('_dict', '_loaded', '_constructor')

    def __init__(self, constructor: t.Callable[[t.Any], V]):
        self._dict: dict[str, t.Any | V] = {}
        self._loaded: dict[str, bool] = {}
        self._constructor = constructor

    def __bool__(self) -> bool:
        return bool(self._dict)

    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self) -> t.Iterable[str]:
        yield from self.keys()

    def __getitem__(self, item: str) -> V:
        try:
            return self._load(item)
        except KeyError:
            raise KeyError(f"'{item}'")

    def __setitem__(self, item: str, value: V):
        self._dict[item] = value
        self._loaded[item] = True

    def __delitem__(self, key: str):
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

    def keys(self) -> t.Iterable[str]:
        return self._dict.keys()

    def values(self) -> t.Iterable[V]:
        for key in self._dict.keys():
            yield self._load(key)

    def items(self) -> t.Iterable[tuple[str, V]]:
        for key in self._dict.keys():
            yield key, self._load(key)

    def clear(self):
        self._dict.clear()
        self._loaded.clear()

    def _load(self, item: str) -> V:
        if not self._loaded[item]:
            self._dict[item] = self._constructor(self._dict[item])
            self._loaded[item] = True
        return self._dict[item]

    def set(self, key: str, value: V):
        self[key] = value

    def get[X](self, key: str, default: X = None) -> V | X:
        try:
            return self._load(key)
        except KeyError:
            return default

    def to_mapping(self) -> dict[str, ct.SupportsNativeJson]:
        return {
            x: (self._dict[x] if not self._loaded[x] else self._dict[x].to_mapping()) for x in self._dict
        }

    def update(self, d: dict[str, V]):
        for k in d:
            self.set(k, d[k])

    def from_mapping(self, map_: dict[str, ct.SupportsNativeJson]):
        self._dict = map_
        self._loaded = {x: False for x in map_}


class LazyLoadList[V]:
    """This class holds a list of dictionaries and maps them into objects as they are needed."""

    __slots__ = ('_list', '_loaded', '_constructor')

    def __init__(self, constructor: t.Callable[[t.Any], V]):
        self._list: list[t.Any | V] = []
        self._loaded: list[bool] = []
        self._constructor = constructor

    def __bool__(self) -> bool:
        return bool(self._list)

    def __iter__(self) -> t.Iterable[V]:
        yield from self.iterate_with_load()

    def __len__(self) -> int:
        return len(self._list)

    def __getitem__(self, index: int) -> V:
        return self._load(index)

    def __setitem__(self, key: int, value: V):
        self._list[key] = value
        self._loaded[key] = True

    def __delitem__(self, key: int):
        del self._list[key]
        del self._loaded[key]

    def clear(self):
        self._list.clear()
        self._loaded.clear()

    def insert(self, index, value):
        self._list.insert(index, value)
        self._loaded.insert(index, True)

    def extend(self, items: t.Iterable[V]):
        for item in items:
            self.append(item)

    def append(self, item: V):
        self._list.append(item)
        self._loaded.append(True)

    def _load(self, idx: int) -> V:
        if not self._loaded[idx]:
            self._list[idx] = self._constructor(self._list[idx])
            self._loaded[idx] = True
        return self._list[idx]

    def iterate_with_load(self) -> t.Iterable[V]:
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
