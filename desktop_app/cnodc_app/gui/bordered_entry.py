import tkinter.ttk as ttk


class BorderedEntry(ttk.Frame):

    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent, style='BorderedEntry.TFrame')
        self.entry = ttk.Entry(self, *args, **kwargs)
        self.entry.pack(padx=2, pady=2)

    def set_errored(self, is_error: bool):
        if is_error:
            self.configure(style='Errored.BorderedEntry.TFrame')
        else:
            self.configure(style='BorderedEntry.TFrame')

