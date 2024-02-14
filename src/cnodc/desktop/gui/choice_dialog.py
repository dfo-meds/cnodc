import tkinter as tk
import tkinter.ttk as ttk
import tkinter.simpledialog as tksd
import typing as t
import cnodc.desktop.translations as i18n


def ask_choice(*args, **kwargs):
    dialog = ChoiceDialog(*args, **kwargs)
    res = dialog.result
    return res


class ChoiceDialog(tksd.Dialog):

    def __init__(self,
                 parent,
                 options: dict[t.Any, str],
                 button_label: t.Optional[str] = None,
                 title: t.Optional[str] = None,
                 prompt: t.Optional[str] = None,
                 default: t.Any = None):
        self.options = options
        self.button_label = button_label or i18n.get_text('choice_dialog_ok')
        self.option_display = list(options.values())
        self._prompt = prompt
        self.result_var = tk.StringVar(parent)
        self.result = None
        if default is None:
            default = self.option_display[0] if self.option_display else None
        else:
            default = self.options[default] if default in self.options else None
        if default is not None:
            self.result_var.set(default)
        super().__init__(parent=parent, title=title)
        self.parent = None

    def body(self, parent):
        if self._prompt is not None:
            label = ttk.Label(parent, text=self._prompt, wraplength=300, justify=tk.LEFT)
            label.pack(expand=True, fill=tk.BOTH, padx=2, pady=2)
        opt_menu = ttk.Combobox(
            parent,
            textvariable=self.result_var,
            values=self.option_display
        )
        opt_menu.pack(expand=True, fill=tk.BOTH, padx=2, pady=2)
        return opt_menu

    def apply(self):
        self.result = None
        for v in self.options:
            if self.options[v] == self.result_var.get():
                self.result = v
                break
