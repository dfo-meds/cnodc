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


def _resolve_default_values(func_sig: inspect.Signature, args, kwargs):
    arg_index = 0
    for pname in func_sig.parameters:
        param = func_sig.parameters[pname]
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        if param.kind != inspect.Parameter.KEYWORD_ONLY and arg_index < len(args):
            if isinstance(args[arg_index], _DelayedDefaultValue):
                args[arg_index] = args[arg_index]()
            arg_index += 1
        elif param.kind != inspect.Parameter.POSITIONAL_ONLY and param.name in kwargs and isinstance(kwargs[param.name], _DelayedDefaultValue):
            kwargs[param.name] = kwargs[param.name]()
        elif not param.default == inspect.Parameter.empty:
            if param.kind == inspect.Parameter.POSITIONAL_ONLY:
                args.append(param.default() if isinstance(param.default, _DelayedDefaultValue) else param.default)
                arg_index += 1
            elif isinstance(param.default, _DelayedDefaultValue):
                kwargs[param.name] = param.default()


def resolve_delayed(f):
    func_sig = inspect.signature(f)
    @functools.wraps(f)
    def _inner(*args, **kwargs):
        args = list(args or [])
        _resolve_default_values(func_sig, args, kwargs)
        return f(*args, **kwargs)
    return _inner


newdict = _DelayedDefaultValue[dict](dict)
newlist = _DelayedDefaultValue[list](list)
newset = _DelayedDefaultValue[set](set)
newuuid = _DelayedDefaultValue[str](lambda: str(uuid.uuid4()))
