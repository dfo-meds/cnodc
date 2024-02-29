import typing as t


T = t.TypeVar("T")


class LazyLoadDict(t.Generic[T]):

    __slots__ = ('_dict', '_loaded', '_constructor')

    def __init__(self, constructor: callable):
        self._dict: dict[str, t.Union[dict, T]] = {}
        self._loaded: dict[str, bool] = {}
        self._constructor = constructor

    def __bool__(self) -> bool:
        return bool(self._dict)

    def __iter__(self) -> t.Iterable[T]:
        yield from self.keys()

    def __getitem__(self, item: str) -> T:
        return self.load(item)

    def __setitem__(self, item: str, value: T):
        self.set(item, value)

    def __contains__(self, item: str) -> bool:
        return item in self._dict

    def __eq__(self, other) -> bool:
        k1 = self.keys()
        k2 = other.keys()
        if k1 != k2:
            return False
        return all(self[k] == other[k] for k in k1)

    def keys(self):
        return self._dict.keys()

    def load(self, item: str):
        if not self._loaded[item]:
            self._dict[item] = self._constructor(self._dict[item])
            self._loaded[item] = True
        return self._dict[item]

    def get(self, item: str):
        try:
            return self.load(item)
        except KeyError:
            return None

    def set(self, item: str, value: T):
        self._dict[item] = value
        self._loaded[item] = True

    def update(self, other = None, **kwargs):
        if other is not None:
            for key in other:
                self.set(key, other[key])
        if kwargs:
            for key in kwargs:
                self.set(key, kwargs[key])

    def to_mapping(self):
        return {
            x: (self._dict[x] if not self._loaded[x] else self._dict[x].to_mapping()) for x in self._dict
        }

    def from_mapping(self, map_: dict):
        self._dict = map_
        self._loaded = {x: False for x in map_}


class LazyLoadList(t.Generic[T]):
    """This class holds a list of dictionaries and maps them into objects as they are needed."""

    __slots__ = ('_list', '_list_len', '_loaded', '_constructor')

    def __init__(self, constructor: callable):
        self._list: list[t.Union[dict, T]] = []
        self._list_len = 0
        self._loaded: list[bool] = []
        self._constructor = constructor

    def __bool__(self):
        return self._list_len > 0

    def __iter__(self):
        yield from self.iterate_with_load()

    def __len__(self):
        return self._list_len

    def __getitem__(self, index: int) -> T:
        return self.load(index)

    def __setitem__(self, key: int, value: T):
        self._list[key] = value
        self._loaded[key] = True

    def extend(self, items: t.Iterable[T]):
        self._list.extend(items)
        new_len = len(self._list)
        self._loaded.extend(True for i in range(self._list_len, new_len))
        self._list_len = new_len

    def append(self, item: T):
        self._list.append(item)
        self._loaded.append(True)
        self._list_len += 1

    def load(self, idx: int) -> T:
        if not self._loaded[idx]:
            self._list[idx] = self._constructor(self._list[idx])
            self._loaded[idx] = True
        return self._list[idx]

    def iterate_with_load(self) -> t.Iterable[T]:
        for idx in range(0, self._list_len):
            if not self._loaded[idx]:
                self._list[idx] = self._constructor(self._list[idx])
                self._loaded[idx] = True
            yield self._list[idx]

    def to_mapping(self) -> list:
        l = []
        for idx in range(0, self._list_len):
            if not self._loaded[idx]:
                l.append(self._list[idx])
            else:
                l.append(self._list[idx].to_mapping())
        return l

    def from_mapping(self, map_: list):
        self._list = map_
        self._list_len = len(self._list)
        self._loaded = [False for _ in range(0, self._list_len)]
