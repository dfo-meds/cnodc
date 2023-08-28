import tkinter as tk
import tkinter.ttk as ttk


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

    def __init__(self, *args, height=None, width=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=0)
        self.table = ttk.Treeview(self)
        self.table.grid(row=0, column=0, sticky="nsew")
        self.table.bind('<<TreeviewSelect>>', self.on_select)
        self.table.bind('<Button-3>', self._on_button3)
        self._scroll = ttk.Scrollbar(self, orient="vertical", command=self.table.yview)
        self.table.configure(yscrollcommand=self._scroll.set)
        self._scroll.grid(row=0, column=1, sticky="nsew")

    def on_select(self, e):
        pass

    def on_right_click(self, iid, e):
        pass

    def _on_button3(self, e):
        iid = self.table.identify_row(e.y)
        if iid:
            self.table.selection_set(iid)
            self.on_right_click(iid, e)


