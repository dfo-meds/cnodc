import functools
import inspect
import typing as t
import uuid


class _DelayedDefaultValue[T]:
    """ Represents a default value that needs to be built to avoid pass-by-reference errors when a new object is built
        (like a dictionary).
    """

    def __init__(self, cb: t.Callable[[], T]):
        self._cb = cb

    def __call__(self) -> T:
        return self._cb()


def _resolve_default_values(func_sig: inspect.Signature, args: tuple, kwargs: dict) -> tuple[list, dict]:
    ba = func_sig.bind(*args, **kwargs)
    ba.apply_defaults()
    return [
        x() if isinstance(x, _DelayedDefaultValue) else x
        for x in ba.args
    ], {
        k: (x() if isinstance(x, _DelayedDefaultValue) else x)
        for k, x in ba.kwargs.items()
    }

def resolve_delayed(f):
    func_sig = inspect.signature(f)
    @functools.wraps(f)
    def _inner(*args, **kwargs):
        nargs, nkwargs = _resolve_default_values(func_sig, args, kwargs)
        return f(*nargs, **nkwargs)
    return _inner


newdict = _DelayedDefaultValue[dict](dict)
newlist = _DelayedDefaultValue[list](list)
newset = _DelayedDefaultValue[set](set)
newuuid = _DelayedDefaultValue[str](lambda: str(uuid.uuid4()))
