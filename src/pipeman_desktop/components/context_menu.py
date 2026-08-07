import functools
import tkinter as tk
import typing as t

from pipeman_desktop.components.tooltip import Popup


class ContextMenuWithHover(Popup):

    def __init__(self, root):
        super().__init__()
        self._root = root
        self._menu_items: list[tuple] = []
        self._frame = None
        self._items: list[tk.Label] | None = None
        self._current_item: int | None = None
        self._selected_foreground = "#FFFFFF"
        self._selected_background = "#1976D2"

    def body(self, window: tk.Toplevel):
        self._root.bind_all("<Button-1>", self._on_click_anywhere)
        self._root.bind_all("<Button-2>", self._on_click_anywhere)
        self._root.bind_all("<Button-3>", self._on_click_anywhere)
        self._root.bind_all('<Escape>', self._on_escape)
        self._frame = tk.Frame(window)
        self._frame.pack(ipadx=2, ipady=2)
        items = []
        index = 0
        for text, command, on_mouse_in, on_mouse_out in self._menu_items:
            label = tk.Label(self._frame, text=text, anchor="w", activebackground=self._selected_background, activeforeground=self._selected_foreground)
            label.grid(row=index, column=0, sticky="EW", ipadx=2, ipady=2)
            items.append(label)
            label.bind("<Enter>", functools.partial(self._on_enter, extra=on_mouse_in, index=index))
            label.bind("<Leave>", functools.partial(self._on_leave, extra=on_mouse_out, index=index))
            label.bind("<Button-1>", functools.partial(self._on_click, extra=command, index=index, on_leave=on_mouse_out))
            label.bind("<Button-3>", functools.partial(self._on_click, extra=command, index=index, on_leave=on_mouse_out))
            index += 1
        self._items = items
        window.grab_set()

    def _on_click_anywhere(self, e):
        if self._window is not None:
            x1, y1 = self._window.winfo_rootx(), self._window.winfo_rooty()
            x2 = x1 + self._window.winfo_width()
            y2 = y1 + self._window.winfo_height()
            if not (x1 <= e.x_root <= x2 and y1 <= e.y_root <= y2):
                self.close()

    def _on_enter(self, e, index: int, extra: t.Callable | None = None):
        if self._items is not None:
            self._items[index].configure(state="active")
        if extra is not None:
            extra()

    def _on_leave(self, e, index: int, extra: t.Callable | None = None):
        if self._items is not None:
            self._items[index].configure(state="normal")
        if extra is not None:
            extra()

    def _on_click(self, e, index: int, extra: t.Callable | None = None, on_leave: t.Callable | None = None):
        self._on_leave(e, index, on_leave)
        if extra is not None:
            extra()
        self.close()

    def _on_escape(self):
        self.close()

    def close(self):
        self.hide()
        self._root.unbind_all("")

    def add_command(self,
                    label: str,
                    command: t.Callable,
                    on_mouse_in: t.Callable | None = None,
                    on_mouse_out: t.Callable | None = None):
        self._menu_items.append((label, command, on_mouse_in, on_mouse_out))

    def popup(self, x_root, y_root):
        self.show(None, x_root, y_root)
        self._window.wait_window()

    def popup_from_event(self, e):
        self.popup(e.x_root, e.y_root)
