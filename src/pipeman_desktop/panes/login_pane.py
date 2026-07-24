from pipeman_desktop.panes.base_pane import BasePane
from pipeman_desktop.util import ApplicationState, DisplayChange
import tkinter.ttk as ttk
import gcapp.i18n as i18n


class LoginPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._user_status_bar = None

    def on_init(self):
        self.app.menus.add_command('file/login', 'menu_login', self.do_login)
        self.app.menus.add_command('file/logout', 'menu_logout', self.do_logout, True)
        self._user_status_bar = ttk.Label(self.app.bottom_bar, text="", relief="solid", borderwidth=2, width=15, anchor="e")
        self._user_status_bar.grid(row=0, column=2, ipadx=5, ipady=2, sticky='NSEW')

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & (DisplayChange.USER | DisplayChange.BATCH):
            self.update_user_state()

    def do_logout(self):
        self.app.menus.disable_command('file/logout')
        self.app.state.logout(
            after_close=self._real_logout,
            after_cancel=self.update_user_state
        )

    def _real_logout(self):
        self.app.dispatcher.submit_job(
            'pipeman_desktop.client.api_client.logout',
            on_success=self.on_logout,
            on_error=self.on_logout_fail
        )

    def on_logout(self, result):
        self.app.show_user_info(
            i18n.tr('logout_success_title'),
            i18n.tr('logout_success_message')
        )
        self.app.state.update_user_info(None, None)
        self.update_user_state()

    def on_logout_fail(self, ex: Exception):
        self.app.show_user_exception(ex)
        self.update_user_state()

    def do_login(self):
        from pipeman_desktop.components.login_dialog import ask_login
        unpw = ask_login(self.app.root)
        if unpw is not None:
            self.app.menus.disable_command('file/login')
            self.app.dispatcher.submit_job(
                'pipeman_desktop.client.api_client.login',
                job_kwargs={
                    'username': unpw[0],
                    'password': unpw[1]
                },
                on_success=self.on_login,
                on_error=self.on_login_fail
            )

    def on_login(self, result: tuple[str, list[str]]):
        self.app.state.update_user_info(result[0], result[1])
        self.app.root.after(5000, self.auto_refresh_session)

    def on_login_fail(self, ex: Exception):
        self.app.show_user_exception(ex)
        self.app.state.update_user_info(None, None)

    def auto_refresh_session(self):
        if self.app.state.username is not None:
            self.app.dispatcher.submit_job(
                'pipeman_desktop.client.api_client.refresh',
                on_success=self.after_refresh,
                on_error=self.after_refresh_error
            )

    def after_refresh_error(self, ex: Exception):
        self.app.show_user_exception(ex)
        self.after_refresh(5000)

    def after_refresh(self, result: int):
        if result < 0:
            self.app.state.update_user_info(None, None)
        elif self.app.state.username is not None:
            res = max(result * 1000, 5000)
            self.app.after(res, self.auto_refresh_session)

    def on_language_change(self):
        if self.app.state.username is None:
            self._user_status_bar.configure(text=i18n.tr('no_user_logged_in'))
        else:
            self._user_status_bar.configure(text=i18n.tr('user_logged_in', username=self.app.state.username))

    def update_user_state(self):
        if self.app.state.username is None:
            self.app.menus.enable_command('file/login')
            self.app.menus.disable_command('file/logout')
        else:
            self.app.menus.disable_command('file/login')
            if self.app.state.can_logout():
                self.app.menus.enable_command('file/logout')
            else:
                self.app.menus.disable_command('file/logout')
        self.on_language_change()


