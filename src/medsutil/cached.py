import functools
import typing as t

CacheParameterType = t.Hashable | t.Iterable[t.Hashable]


def cached_method[**P, Q](func: t.Callable[P,Q] = None, *, limit_args: set[int] | None = None, limit_kwargs: set[str] | None = None) -> t.Callable[P, Q]:
    if func is not None:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            cached_list = []
            cached_list.extend(
                (idx, arg) for idx, arg in enumerate(args[1:]) if limit_args is None or idx in limit_args)
            cached_list.extend((name, kwargs[name]) for name in kwargs if limit_kwargs is None or name in kwargs[name])
            return args[0]._with_cache(f"_function_{func.__name__}", func, *args, **kwargs, cache_parameters=cached_list)
        return wrapper
    else:
        return functools.partial(cached_method, limit_args=limit_args, limit_kwargs=limit_kwargs)


class CachedObjectMixin:

    def __init__(self, *args, **kwargs):
        super(CachedObjectMixin, self).__init__(*args, **kwargs)
        self._cache: dict[t.Hashable, t.Any] = {}
        self._cache_with_parameters: dict[t.Hashable, dict[int, t.Any]] = {}

    def _from_cache_only(self, key: t.Hashable, cache_parameters: CacheParameterType = None) -> t.Any | None:
        if cache_parameters:
            if key in self._cache_with_parameters:
                if isinstance(cache_parameters, t.Hashable):
                    cache_key = hash(cache_parameters)
                else:
                    cache_key = hash(tuple(x for x in t.cast(t.Iterable[t.Hashable], cache_parameters)))
                if cache_key in self._cache_with_parameters[key]:
                    return self._cache_with_parameters[key][cache_key]
        elif key in self._cache:
            return self._cache[key]
        return None

    def _with_cache[X](self, key: t.Hashable, cb: t.Callable[..., X], *args, _invalidate: bool = False, cache_parameters: CacheParameterType = None, **kwargs) -> X:
        if cache_parameters:
            if key not in self._cache_with_parameters:
                self._cache_with_parameters[key] = {}
            if isinstance(cache_parameters, t.Hashable):
                cache_key = hash(cache_parameters)
            else:
                cache_key = hash(tuple(x for x in t.cast(t.Iterable[t.Hashable], cache_parameters)))
            if _invalidate or cache_key not in self._cache_with_parameters[key]:
                self._cache_with_parameters[key][cache_key] = cb(*args, **kwargs)
            return self._cache_with_parameters[key][cache_key]
        else:
            if _invalidate or key not in self._cache:
                self._cache[key] = cb(*args, **kwargs)
            return self._cache[key]

    def _set_cache(self, key: t.Hashable, obj: t.Any, cache_parameters: CacheParameterType = None):
        self._with_cache(key, cb=lambda: obj, cache_parameters=cache_parameters, _invalidate=True)

    def clear_cache(self, key: t.Optional[str] = None):
        if key is None:
            self._cache.clear()
            self._cache_with_parameters.clear()
        else:
            if key in self._cache:
                del self._cache[key]
            if key in self._cache_with_parameters:
                del self._cache[key]
