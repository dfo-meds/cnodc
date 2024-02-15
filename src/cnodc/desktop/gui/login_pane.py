from __future__ import annotations
from cnodc.desktop.gui.base_pane import BasePane, ApplicationState, DisplayChange, QCBatchCloseOperation, \
    CloseBatchResult, BatchOpenState
import typing as t
import tkinter as tk
import tkinter.ttk as ttk
import cnodc.desktop.translations as i18n
import tkinter.simpledialog as tksd
from cnodc.desktop.gui.bordered_entry import BorderedEntry


class PasswordDialog(tksd.Dialog):

    def __init__(self, parent):
        self.password1_var = tk.StringVar(parent)
        self.password2_var = tk.StringVar(parent)
        self._password1_entry: t.Optional[BorderedEntry] = None
        self._password2_entry: t.Optional[BorderedEntry] = None
        super().__init__(parent, title=i18n.get_text('change_password_dialog_title'))

    def body(self, parent):
        ttk.Label(parent, text=i18n.get_text('password_change_message')).grid(row=0, column=0, columnspan=2)
        ttk.Label(parent, text=i18n.get_text('password_change_1')).grid(row=1, column=0, sticky='W')
        self._password1_entry = BorderedEntry(parent, textvariable=self.password1_var, show='*')
        self._password1_entry.grid(row=1, column=1)
        ttk.Label(parent, text=i18n.get_text('password_change_2')).grid(row=2, column=0, sticky='W')
        self._password2_entry = BorderedEntry(parent, textvariable=self.password2_var, show='*')
        self._password2_entry.grid(row=2, column=1)
        return self._password1_entry

    def validate(self) -> bool:
        pw1 = self.password1_var.get()
        pw2 = self.password2_var.get()
        self._password1_entry.set_errored(len(pw1) < 15)
        self._password2_entry.set_errored(pw2 != pw1)
        return pw1 == pw2 and len(pw1) >= 15

    def apply(self):
        pw1 = self.password1_var.get()
        pw2 = self.password2_var.get()
        if pw1 == pw2 and len(pw1) > 15:
            self.result = pw1
        else:
            self.result = None


class LoginPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._username = None
        self._access_list = None
        self._user_status_bar = None

    def on_init(self):
        self.app.menus.add_command('file/login', 'menu_login', self.do_login)
        self.app.menus.add_command('file/change_password', 'menu_change_password', self.change_password, True)
        self.app.menus.add_command('file/logout', 'menu_logout', self.do_logout, True)
        self._user_status_bar = ttk.Label(self.app.bottom_bar, text="", relief=tk.SOLID, borderwidth=2, width=15, anchor=tk.E)
        self._user_status_bar.grid(row=0, column=2, ipadx=5, ipady=2, sticky='NSEW')

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.BATCH:
            self.app.menus.set_state('file/logout', app_state.can_logout())

    def do_logout(self):
        self.app.close_current_batch(
            QCBatchCloseOperation.LOGOUT,
            load_next=False,
            after_close=self._real_logout
        )

    def _real_logout(self):
        self.app.menus.disable_command('file/logout')
        self.app.menus.disable_command('file/change_password')
        self.app.dispatcher.submit_job(
            'cnodc.desktop.client.api_client.logout',
            on_success=self.on_logout,
            on_error=self.on_logout_fail
        )

    def on_logout(self, result: tuple[str, list[str]]):
        self.app.show_user_info(
            i18n.get_text('logout_success_title'),
            i18n.get_text('logout_success_message')
        )
        self._username = None
        self._access_list = None
        self.update_user_state()

    def on_logout_fail(self, ex: Exception):
        self.app.show_user_exception(ex)
        self.update_user_state()

    def change_password(self):
        pwd = PasswordDialog(self.app.root)
        if pwd.result:
            self.app.menus.disable_command('file/change_password')
            self.app.menus.disable_command('file/logout')
            self.app.dispatcher.submit_job(
                'cnodc.desktop.client.api_client.change_password',
                job_kwargs={
                    'password': pwd.result,
                },
                on_success=self._on_password_change,
                on_error=self._on_password_change_fail
            )

    def _on_password_change(self, res):
        self.app.show_user_info(
            i18n.get_text('password_change_success_title'),
            i18n.get_text('password_change_success_message')
        )
        self.app.menus.enable_command('file/change_password')
        self.app.menus.enable_command('file/logout')

    def _on_password_change_fail(self, ex):
        self.app.show_user_exception(ex)
        self.app.menus.enable_command('file/change_password')
        self.app.menus.enable_command('file/logout')

    def do_login(self):
        from cnodc.desktop.gui.login_dialog import ask_login
        unpw = ask_login(self.app.root)
        if unpw is not None:
            self.app.menus.disable_command('file/login')
            self.app.dispatcher.submit_job(
                'cnodc.desktop.client.api_client.login',
                job_kwargs={
                    'username': unpw[0],
                    'password': unpw[1]
                },
                on_success=self.on_login,
                on_error=self.on_login_fail
            )

    def on_login(self, result: tuple[str, list[str]]):
        self._username = result[0]
        self._access_list = result[1]
        self.update_user_state()
        self.app.root.after(5000, self.auto_refresh_session)

    def on_login_fail(self, ex: Exception):
        self.app.show_user_exception(ex)
        if self._username is not None:
            self._username = None
            self._access_list = None
        self.update_user_state()

    def auto_refresh_session(self):
        if self._username is not None:
            self.app.dispatcher.submit_job(
                'cnodc.desktop.client.api_client.refresh',
                on_success=self.after_refresh,
                on_error=self.after_refresh
            )

    def after_refresh(self, res: t.Union[int, Exception]):
        if res < 0:
            self._username = None
            self._access_list = None
            self.update_user_state()
            return
        elif isinstance(res, Exception):
            self.app.show_user_exception(res)
            res = 5000
        else:
            res = max(res * 1000, 5000)
        self.app.root.after(res, self.auto_refresh_session)

    def on_language_change(self, language: str):
        if self._username is None:
            self._user_status_bar.configure(text=i18n.get_text('no_user_logged_in'))
        else:
            self._user_status_bar.configure(text=i18n.get_text('user_logged_in', username=self._username))

    def update_user_state(self):
        if self._username is None:
            self.app.menus.enable_command('file/login')
            self.app.menus.disable_command('file/logout')
            self.app.menus.disable_command('file/change_password')
        else:
            self.app.menus.disable_command('file/login')
            self.app.menus.enable_command('file/logout')
            self.app.menus.enable_command('file/change_password')
        self.on_language_change(i18n.current_language())
        self.app.update_user_info(self._username or None, self._access_list or {})


