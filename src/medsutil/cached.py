import typing as t

class CachedObjectMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache = {}

    def _with_cache[X](self, key: t.Hashable, cb: t.Callable[..., X], *args, _invalidate: bool = False, **kwargs) -> X:
        if key not in self._cache or _invalidate:
            self._cache[key] = cb(*args, **kwargs)
        return self._cache[key]

    def _set_cache(self, key: t.Hashable, obj: t.Any):
        self._cache[key] = obj

    def clear_cache(self, key: t.Optional[str] = None):
        if key is None:
            self._cache.clear()
        elif key in self._cache:
            del self._cache[key]
