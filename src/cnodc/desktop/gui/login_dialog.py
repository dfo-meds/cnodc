import tkinter as tk
import tkinter.ttk as ttk
import tkinter.simpledialog as tksd
import typing as t
import cnodc.desktop.translations as i18n
from cnodc.desktop.gui.bordered_entry import BorderedEntry


def ask_login(*args, **kwargs):
    dialog = LoginDialog(*args, **kwargs)
    return dialog.result


class LoginDialog(tksd.Dialog):

    def __init__(self,
                 parent,
                 title: t.Optional[str] = None):
        if title is None:
            title = i18n.get_text('login_dialog_title')
        self.username_var = tk.StringVar(parent)
        self.password_var = tk.StringVar(parent)
        self._username_entry: t.Optional[BorderedEntry] = None
        self._password_entry: t.Optional[BorderedEntry] = None
        self.result = None
        super().__init__(parent=parent, title=title)

    def body(self, parent):
        ttk.Label(parent, text=i18n.get_text('login_dialog_username')).grid(row=0, column=0)
        self._username_entry = BorderedEntry(parent, textvariable=self.username_var)
        self._username_entry.grid(row=0, column=1)
        ttk.Label(parent, text=i18n.get_text('login_dialog_password')).grid(row=1, column=0)
        self._password_entry = BorderedEntry(parent, show='*', textvariable=self.password_var)
        self._password_entry.grid(row=1, column=1)
        return self._username_entry

    def apply(self):
        un = self.username_var.get()
        pw = self.password_var.get()
        if un != '' and pw != '':
            self.result = (un, pw)
        else:
            self.result = None

    def validate(self) -> bool:
        un = self.username_var.get()
        pw = self.password_var.get()
        self._username_entry.set_errored(un == '')
        self._password_entry.set_errored(pw == '')
        return un != '' and pw != ''
