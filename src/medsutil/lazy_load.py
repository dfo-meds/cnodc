"""
Provides a lazy-loading dictionary and list class.

These objects only load their values upon demand.
"""
import itertools
import typing as t



class LazyLoadDict[KeyType, UnloadedType, LoadedType]:

    __slots__ = ('_dict', '_loaded')

    def __init__(self):
        self._dict: dict[KeyType, UnloadedType | LoadedType] = {}
        self._loaded: dict[str, bool] = {}

    def __bool__(self) -> bool:
        return bool(self._dict)

    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self) -> t.Iterable[KeyType]:
        yield from self.keys()

    def __getitem__(self, item: KeyType) -> LoadedType:
        try:
            return self._load(item)
        except KeyError:
            raise KeyError(f"'{item}'")

    def __setitem__(self, item: KeyType, value: LoadedType):
        self._dict[item] = value
        self._loaded[item] = True

    def __delitem__(self, key: KeyType):
        del self._dict[key]
        del self._loaded[key]

    def __contains__(self, item: KeyType) -> bool:
        return item in self._dict

    def __eq__(self, other: t.Mapping) -> bool:
        try:
            all_keys = set(itertools.chain(self.keys(), other.keys()))
            return all(self[k] == other[k] for k in all_keys)
        except (KeyError, AttributeError):
            return False

    def keys(self) -> t.KeysView[KeyType]:
        return self._dict.keys()

    def values(self) -> t.Generator[LoadedType, None, None]:
        for key in self._dict.keys():
            yield self._load(key)

    def items(self) -> t.Iterable[tuple[KeyType, LoadedType]]:
        for key in self._dict.keys():
            yield key, self._load(key)

    def clear(self):
        self._dict.clear()
        self._loaded.clear()

    def _load(self, item: KeyType) -> LoadedType:
        if not self._loaded[item]:
            self._dict[item] = self._load_item(self._dict[item])
            self._loaded[item] = True
        return self._dict[item]

    def set(self, key: KeyType, value: LoadedType):
        self[key] = value

    def get[X](self, key: KeyType, default: X = None) -> LoadedType | X:
        try:
            return self._load(key)
        except KeyError:
            return default

    def update(self, d: t.Mapping[KeyType, LoadedType]):
        for k, v in d.items():
            self.set(k, v)

    def to_mapping(self) -> dict[KeyType, UnloadedType]:
        return {
            x: (self._dict[x] if not self._loaded[x] else self._unload_item(self._dict[x])) for x in self._dict
        }

    def _load_item(self, value: UnloadedType) -> LoadedType:
        raise NotImplementedError

    def _unload_item(self, value: LoadedType) -> UnloadedType:
        raise NotImplementedError

    def from_mapping(self, map_: dict[KeyType, UnloadedType]):
        self._dict = map_
        self._loaded = {x: False for x in map_}


class LazyLoadList[UnloadedType, LoadedType]:
    """This class holds a list of dictionaries and maps them into objects as they are needed."""

    __slots__ = ('_list', '_loaded')

    def __init__(self):
        self._list: list[LoadedType | UnloadedType] = []
        self._loaded: list[bool] = []

    def __bool__(self) -> bool:
        return bool(self._list)

    def __iter__(self) -> t.Iterable[LoadedType]:
        # TODO: this is implemented wrong I think.
        yield from self.iterate_with_load()

    def __len__(self) -> int:
        return len(self._list)

    def __getitem__(self, index: int) -> LoadedType:
        return self._load(index)

    def __setitem__(self, key: int, value: LoadedType):
        self._list[key] = value
        self._loaded[key] = True

    def __delitem__(self, key: int):
        del self._list[key]
        del self._loaded[key]

    def clear(self):
        self._list.clear()
        self._loaded.clear()

    def insert(self, index: int, value: LoadedType):
        self._list.insert(index, value)
        self._loaded.insert(index, True)

    def extend(self, items: t.Iterable[LoadedType]):
        for item in items:
            self.append(item)

    def append(self, item: LoadedType):
        self._list.append(item)
        self._loaded.append(True)

    def _load(self, idx: int) -> LoadedType:
        if not self._loaded[idx]:
            self._list[idx] = self._load_item(self._list[idx])
            self._loaded[idx] = True
        return self._list[idx]

    def _load_item(self, item: UnloadedType) -> LoadedType:
        raise NotImplementedError

    def iterate_with_load(self) -> t.Iterable[LoadedType]:
        for idx in range(0, len(self._list)):
            yield self._load(idx)

    def to_mapping(self) -> list[UnloadedType]:
        l = []
        for idx in range(0, len(self._list)):
            if not self._loaded[idx]:
                l.append(self._list[idx])
            else:
                l.append(self._unload_item(self._list[idx]))
        return l

    def _unload_item(self, item: LoadedType) -> UnloadedType:
        raise NotImplementedError

    def from_mapping(self, map_: list):
        self._list = map_
        self._loaded = [False for _ in range(0, len(self._list))]
