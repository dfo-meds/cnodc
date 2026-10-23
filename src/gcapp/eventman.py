""" Basic event manager class

    You can inject this class anywhere, register handlers, and then fire them.

    The class has a global scope. Event handler registration is thread-safe. Note that the registered
    callback functions will be called from the same thread that fired the event. Care should be taken
    when registering event handlers that they can handle this.

    Usage is generally

    ::

        def event_handler(*args, **kwargs):
          ...

        manager = injector.get(EventManager)
        manager.on("my_event", event_handler)

        manager.fire("my_event", *args, **kwargs)
        # -> will call event_handler(*args, **kwargs)

"""
import threading
import typing as t

import zirconium as zr
import zrlog
from autoinject import injector, auto

from medsutil.dynamic import dynamic_object


@injector.injectable_global
class EventManager:
    """ Manager for event callbacks. """

    config: zr.ApplicationConfig = auto()

    @injector.construct
    def __init__(self):
        self._lock = threading.RLock()
        self._hooks: dict[str, list[t.Callable | str | None]] = {}
        self._log = zrlog.get_logger('gcapp.events')

    def on(self, event_name: str, cb: t.Callable | str):
        """ Registers an event callback function

        :param event_name: The name of the event to register.
        :param cb: The callback function to register.
        """
        with self._lock:
            if event_name not in self._hooks:
                self._hooks[event_name] = []
            self._log.debug("Registering callback [%s] for [%s]", cb, event_name)
            self._hooks[event_name].append(cb)

    def fire(self, event_name: str, *args, **kwargs):
        """ Calls all registered event callback functions with the given arguments. """
        self._log.debug("Firing event [%s]", event_name)
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
                except Exception as ex:
                    ex.add_note(f"Exception occurred while firing callback [{cb}] for event [{event_name}]")
                    self._hooks[event_name][idx] = None
                    raise
