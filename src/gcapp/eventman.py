import threading
import typing as t

import zirconium as zr
import zrlog
from autoinject import injector

from medsutil.dynamic import dynamic_object


@injector.injectable_global
class EventManager:

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._lock = threading.RLock()
        self._hooks: dict[str, list[t.Callable | str | None]] = {}
        self._log = zrlog.get_logger('gcapp.events')

    def on(self, event_name: str, cb: t.Callable | str):
        """Register an event callback function"""
        with self._lock:
            if event_name not in self._hooks:
                self._hooks[event_name] = []
            self._hooks[event_name].append(cb)

    def fire(self, event_name: str, *args, **kwargs):
        """Call all registered event callback functions with the given arguments"""
        self._log.debug(f"Firing {event_name}")
        if event_name in self._hooks:
            for idx, cb in enumerate(self._hooks[event_name]):
                if cb is None:
                    continue
                try:
                    try:
                        cb(*args, **kwargs)
                    except TypeError:
                        self._hooks[event_name][idx] = cb = dynamic_object(t.cast(str, cb))
                        cb(*args, **kwargs)
                except Exception:
                    self._log.exception(f"Error loading or firing [{cb}]")
                    self._hooks[event_name][idx] = None
                    raise
