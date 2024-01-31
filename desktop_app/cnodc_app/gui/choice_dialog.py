import tkinter as tk
import tkinter.ttk as ttk
import tkinter.simpledialog as tksd
import typing as t
import cnodc_app.translations as i18n


def ask_choice(*args, **kwargs):
    dialog = ChoiceDialog(*args, **kwargs)
    return dialog.result


class ChoiceDialog(tksd.Dialog):

    def __init__(self,
                 parent,
                 options: dict[t.Any, str],
                 button_label: t.Optional[str] = None,
                 title: t.Optional[str] = None,
                 default: t.Any = None):
        self.options = options
        self.button_label = button_label or i18n.get_text('choice_dialog_ok')
        self.option_display = list(options.values())
        self.result_var = tk.StringVar(parent)
        self.result = None
        if default is None:
            default = self.option_display[0] if self.option_display else None
        else:
            default = self.options[default] if default in self.options else None
        if default is not None:
            self.result_var.set(default)
        super().__init__(parent=parent, title=title)

    def body(self, parent):
        opt_menu = ttk.OptionMenu(parent, self.result_var, self.result_var.get(), *self.options.values())
        opt_menu.pack(expand=True, fill=tk.BOTH, padx=5, pady=5, ipadx=5, ipady=3)
        return opt_menu

    def buttonbox(self):
        box = tk.Frame(self)
        w = ttk.Button(box, text=self.button_label, command=self.ok, default=tk.ACTIVE)
        w.pack(side=tk.LEFT, padx=5, pady=5, ipadx=10, ipady=3)
        self.bind('<Return>', self.ok)
        box.pack()

    def apply(self):
        self.result = None
        for v in self.options:
            if self.options[v] == self.result_var.get():
                self.result = v
                break
