import tkinter as tk
from cnodc_app import translations as i18n


class MenuManager:

    def __init__(self, parent):
        self._top_bar = tk.Menu(parent, tearoff=0)
        parent.configure(menu=self._top_bar)
        self._top_count = 0
        self._menu_items: dict[str, list] = {}
        self._commands: dict[str, tuple] = {}

    def add_sub_menu(self, path: str, text_key: str):
        if path in self._menu_items:
            raise ValueError('path already defined')
        if '/' not in path:
            next_item = tk.Menu(self._top_bar, tearoff=0)
            self._top_bar.add_cascade(label=i18n.get_text(text_key), menu=next_item)
            self._menu_items[path] = [next_item, text_key, 0, self._top_count]
            self._top_count += 1
        else:
            parent_key = path[:path.rfind('/')]
            parent = self._menu_items[parent_key][0]
            next_item = tk.Menu(parent, tearoff=0)
            parent.add_cascade(label=i18n.get_text(text_key), menu=next_item)
            self._menu_items[path] = [next_item, text_key, 0, self._menu_items[parent_key][2]]
            self._menu_items[parent_key][2] += 1

    def disable_command(self, path: str):
        parent_key = path[:path.rfind('/')]
        menu = self._menu_items[parent_key][0]
        menu.entryconfigure(self._commands[path][1], state=tk.DISABLED)

    def enable_command(self, path: str):
        parent_key = path[:path.rfind('/')]
        menu = self._menu_items[parent_key][0]
        menu.entryconfigure(self._commands[path][1], state=tk.NORMAL)

    def add_command(self, path: str, text_key: str, command: callable, start_disabled: bool = False):
        if path in self._commands:
            raise ValueError('path already defined')
        parent_key = path[:path.rfind('/')]
        parent = self._menu_items[parent_key][0]
        parent.add_command(
            command=command,
            label=i18n.get_text(text_key),
            state=tk.DISABLED if start_disabled else tk.NORMAL
        )
        self._commands[path] = (text_key, self._menu_items[parent_key][2])
        self._menu_items[parent_key][2] += 1

    def update_languages(self):
        for x in self._menu_items:
            parent = self._menu_items[x[0:x.rfind('/')]][0] if '/' in x else self._top_bar
            parent.entryconfigure(self._menu_items[x][3], label=i18n.get_text(self._menu_items[x][1]))
        for x in self._commands:
            parent = self._menu_items[x[0:x.rfind('/')]][0]
            parent.entryconfigure(self._commands[x][1], label=i18n.get_text(self._commands[x][0]))
