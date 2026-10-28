""" Caching helpers """
import functools
import threading
import typing as t

from medsutil.awaretime import AwareDateTime


CacheParameterType = t.Hashable | t.Iterable[t.Hashable]


def cached_method[**P, Q](func: t.Callable[P,Q] = None, *, limit_args: t.Container[int] | None = None, limit_kwargs: t.Container[str] | None = None) -> t.Callable[P, Q]:
    """ Implements caching for a method on an object that includes CachedObjectMixin.

        Use this as a decorator:

        @cached_method
        def my_method(self):
            ...

        :param func: The function to wrap (or None to get a wrapper back).
        :param limit_args: If provided, a set of the argument indexes which are used as a caching key (default all of them).
        :param limit_kwargs: If provided, a set of keyword argument names which are used as a caching key (default all of them).

    """
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
    """ Adds an object-based caching option.

        Classes can mix this in and then either use the `@cached_method` decorator
        or directly call _with_cache(). Provides clear_cache() to clear the cache.
    """

    def __init__(self, *args, **kwargs):
        super(CachedObjectMixin, self).__init__(*args, **kwargs)
        self._cache: dict[t.Hashable, t.Any] = {}
        self._cache_with_parameters: dict[t.Hashable, dict[int, t.Any]] = {}

    def _from_cache_only(self, key: t.Hashable, cache_parameters: CacheParameterType = None) -> t.Any | None:
        """ Retrieves the cached value if and only if it exists in the cache already.
            :param key: The key used for caching
            :param cache_parameters: A hashable value or a sequence of them (used for function parameters usually).
            :returns: The cached value only if it is already set.
        """
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
        """ Retrieves the cached value if it has been set, otherwise calls `cb(*args, **kwargs)` to get a value and cache it.

            The cache key is a mix of the key given and cache_parameters which distinguish different arguments
            with the same key.

            :param key: The key the cached value is stored under.
            :param cb: The callback to call to get a value if the cache is invalidated.
            :param args: Arguments to pass to `cb`
            :param _invalidate: Set to true to invalidate the cache
            :param cache_parameters: A hashable or an iterable of hashables (usually the arguments to the function).
            :param kwargs: Keyword arguments to pass to `cb`
        """
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
        """ Sets the cache value to the given object. """
        self._with_cache(key, cb=lambda: obj, cache_parameters=cache_parameters, _invalidate=True)

    def clear_cache(self, key: t.Optional[str] = None):
        """ Clears the cache, optionally just for a specific cache key. """
        if key is None:
            self._cache.clear()
            self._cache_with_parameters.clear()
        else:
            if key in self._cache:
                del self._cache[key]
            if key in self._cache_with_parameters:
                del self._cache[key]


class LeastRecentCache[X, Y]:
    """ A least recently used cache which keeps only so many items in memory.

        This exists because functools.lru_cache() wasn't working well for me, so I made one.

        It is basically a dictionary that also stores the last time each value was accessed.

        When `prune_cache()` is called, the oldest entries are removed until there are at most
        `max_size` entries in the cache.
    """

    def __init__(self, max_size: int = 200):
        self._values: dict[X, Y] = {}
        self._value_used_times: dict[X, AwareDateTime] = {}
        self._max_size = max_size
        self._lock = threading.RLock()

    def __contains__(self, key: X) -> bool:
        with self._lock:
            return key in self._values

    def __getitem__(self, key: X) -> Y:
        with self._lock:
            self._value_used_times[key] = AwareDateTime.now()
            return self._values[key]

    def __setitem__(self, key: X, value: Y):
        with self._lock:
            self._values[key] = value
            self._value_used_times[key] = AwareDateTime.now()

    def clear(self):
        with self._lock:
            self._values.clear()
            self._value_used_times.clear()

    def get[Z](self, item: X, default: Z = None) -> Y | Z:
        with self._lock:
            self._value_used_times[item] = AwareDateTime.now()
            return self._values.get(item, default)

    def set(self, item: X, value: Y):
        with self._lock:
            self._values[item] = value
            self._value_used_times[item] = AwareDateTime.now()

    def keys(self) -> t.KeysView[X]:
        return self._values.keys()

    def values(self) -> t.ValuesView[Y]:
        return self._values.values()

    def items(self) -> t.ItemsView[X, Y]:
        return self._values.items()

    def with_cache(self,
                      cache_key: X,
                      cb: t.Callable[..., Y],
                      *args,
                      _prune_cache: bool = True,
                      **kwargs) -> Y:
        with self._lock:
            if cache_key not in self._values:
                self.set(cache_key, result := cb(*args, **kwargs))
                if _prune_cache:
                    self.prune_cache()
                return result
            else:
                return self.get(cache_key)

    def prune_cache(self):
        """ Prunes the cache, removing all older items. """
        with self._lock:
            if len(self._values) > self._max_size:
                d = AwareDateTime(1980, 1, 1, 0, 0, 0)
                keys = [(k, self._value_used_times.get(k, d)) for k in self._values.keys()]
                keys.sort(key=lambda x: x[1], reverse=True)
                for k, _ in keys[self._max_size:]:
                    del self._values[k]
                    del self._value_used_times[k]
