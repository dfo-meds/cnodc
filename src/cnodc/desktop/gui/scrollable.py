import tkinter as tk
import tkinter.ttk as ttk
import typing as t


class ScrollableFrame(tk.Frame):

    def __init__(self, *args, height=None, width=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._canvas = tk.Canvas(self, height=height, width=width)
        self._canvas.grid(row=0, column=0, sticky="nsew")
        self._scrollbar = tk.Scrollbar(self, orient="vertical", command=self._canvas.yview)
        self._canvas.configure(yscrollcommand=self._scrollbar.set)
        self._scrollbar.grid(row=0, column=1, sticky="nsew")
        self.content = tk.Frame(self)
        self._canvas.create_window((0, 0), window=self.content, anchor="nw")
        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=0)
        self.rowconfigure(0, weight=1)
        self._canvas.bind("<Configure>", self.on_configure)

    def on_configure(self, e):
        self._canvas.configure(scrollregion=self._canvas.bbox('all'))


class ScrollableTreeview(tk.Frame):

    def __init__(self,
                 parent,
                 headers: list[str],
                 height=None,
                 width=None,
                 padding=0,
                 displaycolumns: t.Optional[t.Union[tuple, list]] = None,
                 selectmode: t.Optional[str] = "browse",
                 on_select: t.Optional[callable] = None,
                 on_click: t.Optional[callable] = None,
                 on_right_click: t.Optional[callable] = None,
                 show: t.Optional[str] = None):
        super().__init__(parent)
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=0)
        self.table = ttk.Treeview(self)
        if selectmode is not None:
            self.table.configure(selectmode=selectmode)
        if show is not None:
            self.table.configure(show=show)
        self.set_headers(headers)
        if displaycolumns is not None:
            self.set_display_columns(displaycolumns)
        self.table.configure(padding=padding)
        self.table.grid(row=0, column=0, sticky="nsew")
        self.table.bind('<<TreeviewSelect>>', self._on_select)
        self.table.bind('<Button-3>', self._on_button3)
        self._scroll = ttk.Scrollbar(self, orient="vertical", command=self.table.yview)
        self.table.configure(yscrollcommand=self._scroll.set)
        self._scroll.grid(row=0, column=1, sticky="nsew")
        self._current_selection = None
        self._on_select_call = on_select
        self._on_right_call = on_right_click
        self._on_click_call = on_click

    def clear_items(self):
        for item in self.table.get_children():
            self.table.delete(item)

    def tag_configure(self, tag_name: str, **kwargs):
        self.table.tag_configure(tag_name, **kwargs)

    def set_display_columns(self, columns: t.Union[list, tuple]):
        self.table.configure(displaycolumns=columns or "#all")

    def set_headers(self, headers: list[str]):
        self.table.configure(columns=(f"c{x+1}" for x in range(0, len(headers))))
        for i in range(0, len(headers)):
            key = f"# {i + 1}"
            self.table.column(key, anchor=tk.CENTER)
            self.table.heading(key, text=headers[i])

    def extend_items(self, values: t.Iterable[tuple[str, tuple, tuple]]):
        for txt, v, tags in values:
            self.table.insert('', 'end', text=txt, values=v, tags=tags, open=False)

    def _on_select(self, e: tk.Event):
        if self._on_select_call is not None or self._on_click_call is not None:
            iid = self.table.selection()
            if iid:
                item = self.table.item(iid[0])
                is_new = self._current_selection != iid[0]
                self._current_selection = iid[0]
                if is_new and self._on_select_call is not None:
                    self._on_select_call(item, e)
                if self._on_click_call is not None:
                    self._on_click_call(item, is_new, e)

    def _on_button3(self, e):
        if self._on_right_call is not None:
            iid = self.table.identify('item', e.x, e.y)
            if iid:
                self.table.selection_set(iid)
                self._on_right_call(self.table.item(self.table.selection()), e)


