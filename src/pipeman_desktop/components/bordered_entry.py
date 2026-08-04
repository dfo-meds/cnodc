import tkinter.ttk as ttk

class BorderedControl(ttk.Frame):

    def __init__(self, parent, widget):
        super().__init__(parent, style='BorderedEntry.TFrame')
        self.entry = widget
        self.entry.pack(padx=2, pady=2)

    def set_errored(self, is_error: bool):
        if is_error:
            self.configure(style='Errored.BorderedEntry.TFrame')
        else:
            self.configure(style='BorderedEntry.TFrame')



class BorderedEntry(BorderedControl):

    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent, ttk.Entry(self, *args, **kwargs))


class BorderedChoice(BorderedControl):

    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent, ttk.Combobox(self, *args, **kwargs))


class BorderedCheckbox(BorderedControl):

    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent, ttk.Checkbutton(self, *args, **kwargs))
