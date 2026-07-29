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
                 columns: list[str],
                 padding=0,
                 selectmode: t.Optional[str] = "browse",
                 on_select: t.Optional[t.Callable] = None,
                 on_click: t.Optional[t.Callable] = None,
                 on_right_click: t.Optional[t.Callable] = None,
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
        self.table.configure(columns=columns, padding=padding)
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
        self._ignoring_callback = False

    def clear_items(self):
        for item in self.table.get_children():
            self.table.delete(item)

    def set_selection(self, iids: list[str], _ignore_callback: bool = True):
        self._ignoring_callback = _ignore_callback
        self.table.selection_set(iids)
        self._ignoring_callback = False

    def tag_configure(self, tag_name: str, **kwargs):
        self.table.tag_configure(tag_name, **kwargs)

    def set_header_text(self, column_id: str, header: str):
        self.table.heading(column_id, text=header)

    def append_item(self, iid: str, values: tuple, parent: str = '', text: str = '', tags: tuple | None = None):
        self.table.insert(parent, 'end', iid=iid, text=text, values=values, tags=tags or tuple(), open=False)

    def open_item(self, iid: str):
        self.table.item(iid, open=True)

    def extend_items(self, values: t.Iterable[tuple]):
        for args in values:
            self.append_item(*args)

    def get_item(self, iid: str):
        return {
            **self.table.item(iid),
            "iid": iid
        }

    def _on_select(self, e: tk.Event):
        if self._ignoring_callback:
            return
        if self._on_select_call is not None or self._on_click_call is not None:
            iid = self.table.selection()
            if iid:
                item = self.get_item(iid[0])
                is_new = self._current_selection != iid[0]
                self._current_selection = iid[0]
                item['iid'] = iid[0]
                if is_new and self._on_select_call is not None:
                    self._on_select_call(item, e)
                if self._on_click_call is not None:
                    self._on_click_call(item, is_new, e)

    def _on_button3(self, e):
        if self._on_right_call is not None:
            iid = self.table.identify('item', e.x, e.y)
            if iid:
                self.table.selection_set([iid])
                self._on_right_call(self.get_item(iid), e)


