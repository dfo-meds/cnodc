import typing as t

class CachedObjectMixin:

    def __init__(self):
        super().__init__()
        self.__cache = {}

    def _with_cache[X](self, key: t.Hashable, cb: t.Callable[..., X], *args, _clear_cache: bool = False, **kwargs) -> X:
        if key not in self.__cache or _clear_cache:
            self.__cache[key] = cb(*args, **kwargs)
        return self.__cache[key]

    def _set_cache(self, key: t.Hashable, obj: t.Any):
        self.__cache[key] = obj

    def clear_cache(self, key: t.Optional[str] = None):
        if key is None:
            self.__cache.clear()
        elif key in self.__cache:
            del self.__cache[key]
