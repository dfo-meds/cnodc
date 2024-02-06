from autoinject import injector
import queue
import tkinter as tk
import cnodc.desktop.translations as i18n


@injector.injectable_global
class CrossThreadMessenger:

    def __init__(self):
        self._messages = queue.SimpleQueue()

    def send(self, msg: str):
        self._messages.put_nowait(msg)

    def send_translatable(self, key: str, **kwargs):
        self._messages.put_nowait(i18n.get_text(key, **kwargs))

    def receive_into_label(self, label: tk.Label):
        try:
            label.configure(text=self._messages.get_nowait())
        except queue.Empty:
            pass

