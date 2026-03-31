import typing as t
import functools

def wrapper[**P, R](wrapped: t.Callable, wrapper_: t.Callable[P, R]) -> t.Callable[P, R]:
    @functools.wraps(wrapper_)
    def _inner(*args, **kwargs):
        return wrapped(*args, **kwargs)
    return _inner
