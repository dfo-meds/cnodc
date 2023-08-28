from .child import ChildFrame
import tkinter.ttk as ttk
import tkinter as tk
import tkinter.messagebox as mb


class LoginFrame(ChildFrame):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, title="login_title", child_name="login", height=120, width=300, **kwargs)
        self.username_label = ttk.Label(self.contents, text=self.main_app.get_text("username"))
        self.password_label = ttk.Label(self.contents, text=self.main_app.get_text("password"))
        self.username = tk.StringVar()
        self.password = tk.StringVar()
        self.username_field = ttk.Entry(self.contents, textvariable=self.username)
        self.password_field = ttk.Entry(self.contents, textvariable=self.password, show="*")
        self.login_button = ttk.Button(self.contents, text=self.main_app.get_text("login"), command=self.do_login)
        self.contents.columnconfigure(0, weight=1)
        self.contents.columnconfigure(1, weight=1)
        self.contents.rowconfigure(0, weight=1)
        self.contents.rowconfigure(1, weight=1)
        self.contents.rowconfigure(2, weight=1)
        self.username_label.grid(column=0, row=0, sticky="e")
        self.username_field.grid(column=1, row=0, sticky="w")
        self.password_label.grid(column=0, row=1, sticky="e")
        self.password_field.grid(column=1, row=1, sticky="w")
        self.login_button.grid(column=1, row=2, sticky="w")

    def do_login(self):
        self.login_button.configure(state='disabled')
        self.main_app.make_request(
            endpoint="login",
            method="POST",
            data={
                "username": self.username.get(),
                "password": self.password.get()
            },
            cb=self.login_results
        )

    def login_results(self, res, status_code):
        if status_code == -1:
            ex = res['exception']
            mb.showerror(
                title=self.main_app.get_text("app_error"),
                message=f"{ex.__class__.__name__}: {str(ex)}"
            )
            self.login_button.configure(state='enabled')
        elif status_code != 200:
            mb.showerror(
                title=self.main_app.get_text("request_error"),
                message=res['error'] if 'error' in res else 'unknown'
            )
            self.login_button.configure(state='enabled')
        elif 'token' not in res or 'expiry' not in res:
            mb.showerror(
                title=self.main_app.get_text("request_error"),
                message=self.main_app.get_text("malformed_response")
            )
            self.login_button.configure(state='enabled')
        else:
            mb.showinfo(
                title=self.main_app.get_text("success"),
                message=self.main_app.get_text("login_success")
            )
            self.main_app.set_user_token(
                self.username.get(),
                res['token'],
                res['expiry']
            )
            self.close_window()

