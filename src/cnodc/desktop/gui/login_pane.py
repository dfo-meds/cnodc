from __future__ import annotations
from cnodc.desktop.gui.base_pane import BasePane
import typing as t
import tkinter as tk
import tkinter.ttk as ttk
import cnodc.desktop.translations as i18n


class LoginPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._username = None
        self._access_list = None
        self._user_status_bar = None

    def on_init(self):
        self.app.menus.add_command('file/login', 'menu_login', self.do_login)
        self._user_status_bar = ttk.Label(self.app.bottom_bar, text="", relief=tk.SOLID, borderwidth=2, width=15, anchor=tk.E)
        self._user_status_bar.grid(row=0, column=2, ipadx=5, ipady=2, sticky='NSEW')

    def do_login(self):
        from cnodc.desktop.gui.login_dialog import ask_login
        unpw = ask_login(self.app.root)
        if unpw is not None:
            self.app.menus.disable_command('file/login')
            self.app.dispatcher.submit_job(
                'desktop.client.api_client.login',
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
            self._access_list = []
        self.update_user_state()

    def auto_refresh_session(self):
        if self._username is not None:
            self.app.dispatcher.submit_job(
                'desktop.client.api_client.refresh',
                on_success=self.after_refresh,
                on_error=self.after_refresh
            )

    def after_refresh(self, res: t.Union[bool, Exception]):
        if res is False:
            self._username = None
            self._access_list = None
            self.update_user_state()
            return
        elif isinstance(res, Exception):
            self.app.show_user_exception(res)
        self.app.root.after(5000, self.auto_refresh_session)

    def on_language_change(self):
        if self._username is None:
            self._user_status_bar.configure(text=i18n.get_text('no_user_logged_in'))
        else:
            self._user_status_bar.configure(text=i18n.get_text('user_logged_in', username=self._username))

    def update_user_state(self):
        if self._username is None:
            self.app.menus.enable_command('file/login')
        else:
            self.app.menus.disable_command('file/login')
        self.on_language_change()
        self.app.update_user_access(self._access_list or [])

