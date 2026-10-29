""" Provides a sentinel value that indicates a new object should be built. """
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


newdict = _DelayedDefaultValue[dict](dict)
newlist = _DelayedDefaultValue[list](list)
newset = _DelayedDefaultValue[set](set)
newuuid = _DelayedDefaultValue[str](lambda: str(uuid.uuid4()))
