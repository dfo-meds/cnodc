import tkinter as tk
import tkinter.messagebox as mb
import tkinter.ttk as ttk
from i18n import Translator
import typing as t
import datetime

from business import NODBRequestController

START_WIDTH = 800
START_HEIGHT = 600


class _MainAppProtocol(t.Protocol):

    def set_username(self, s: str):
        raise NotImplementedError()

    def set_message(self, m: str):
        raise NotImplementedError()

    def get_text(self, resource_name: str, lang: t.Optional[str] = None):
        raise NotImplementedError()

    def set_language(self, lang_name: str):
        raise NotImplementedError()

    def set_login_token(self, token: str, expiry: datetime.datetime):
        raise NotImplementedError()

    def make_request(self, endpoint: str, method: str, data: dict, cb: callable):
        raise NotImplementedError()


class ChildFrame(ttk.Frame):

    def __init__(self, parent: tk.Frame, main_app: _MainAppProtocol = None, title: str = "", title_raw: str = "", allow_close: bool = True, **kwargs):
        super().__init__(parent, style="ChildFrame.TFrame", **kwargs)
        self.parent: tk.Frame = parent
        self.main_app: _MainAppProtocol = main_app or parent
        self.bind("<Motion>", self.on_motion)
        self.bind("<ButtonPress>", self.on_press)
        self.bind("<ButtonRelease>", self.on_release)
        self.bind("<Configure>", self.on_configure)
        self._offset_x = None
        self._offset_y = None
        self._original_height = None
        self._original_width = None
        self._dnd_in_progress = False
        self._resize_in_progress = False
        if title and self.main_app:
            title_raw = self.main_app.get_text(title)
        self.label = ttk.Label(self, text=title_raw, style='TitleBar.TLabel', padding=(10, 0, 0, 0))
        self.label.bind("<ButtonPress>", self.on_press)
        self.label.bind("<ButtonRelease>", self.on_release)
        self.label.bind("<Motion>", self.on_motion)
        self.inner_frame = ttk.Frame(self, style="ChildInnerFrame.TFrame", borderwidth=5, relief="sunken")
        self.inner_frame.bind("<ButtonPress>", self.on_press)
        self.inner_frame.bind("<ButtonRelease>", self.on_release)
        self.inner_frame.bind("<Motion>", self.on_motion)
        self.close_button = None
        if allow_close:
            self.close_button = tk.Button(
                self,
                text="X",
                bg="#FF0000",
                fg="#FFFFFF",
                font=(None, 8),
                command=self.on_close
            )

    def on_close(self):
        pass

    def on_configure(self, e):
        w = self.winfo_width()
        h = self.winfo_height()
        self.label.place(x=0, y=0, width=w, height=25)
        self.inner_frame.place(x=0, y=25, width=w, height=h - 25)
        if self.close_button is not None:
            self.close_button.place(x=w - 25, y=4, w=17, h=17)

    def on_press(self, e):
        if e.widget == self.inner_frame:
            e.y = e.y + 25
        hot_spot = self._hot_spot_area(e)
        if hot_spot == 'bottom_right_resize':
            self._offset_x = e.x
            self._offset_y = e.y
            self._original_width = self.winfo_width()
            self._original_height = self.winfo_height()
            self._resize_in_progress = 'xy'
        elif hot_spot == 'bottom_resize':
            self._offset_x = e.x
            self._offset_y = e.y
            self._original_width = self.winfo_width()
            self._original_height = self.winfo_height()
            self._resize_in_progress = 'y'
        elif hot_spot == 'right_resize':
            self._offset_x = e.x
            self._offset_y = e.y
            self._original_width = self.winfo_width()
            self._original_height = self.winfo_height()
            self._resize_in_progress = 'x'
        elif hot_spot == 'drag':
            self._offset_x = e.x
            self._offset_y = e.y
            self._dnd_in_progress = True

    def on_motion(self, e):
        if e.widget == self.inner_frame:
            e.y = e.y + 25
        if self._dnd_in_progress:
            new_x = e.x - self._offset_x + self.winfo_x()
            new_y = e.y - self._offset_y + self.winfo_y()
            self.place(
                x=new_x,
                y=new_y
            )
        elif self._resize_in_progress:
            if self._resize_in_progress == 'xy' or self._resize_in_progress == 'x':
                self.config(width=self._original_width + (e.x - self._offset_x))
            if self._resize_in_progress == 'xy' or self._resize_in_progress == 'y':
                self.config(height=self._original_height + (e.y - self._offset_y))
        else:
            hot_spot = self._hot_spot_area(e)
            if hot_spot == 'bottom_right_resize':
                self.config(cursor="bottom_right_corner")
            elif hot_spot == 'bottom_resize':
                self.config(cursor='sb_v_double_arrow')
            elif hot_spot == 'right_resize':
                self.config(cursor='sb_h_double_arrow')
            elif hot_spot == 'drag':
                self.config(cursor='hand2')
            else:
                self.config(cursor="arrow")

    def _hot_spot_area(self, e):
        w, h = self.winfo_width(), self.winfo_height()
        if (w - e.x) <= 5:
            return 'bottom_right_resize' if (h - e.y) < 5 else 'right_resize'
        elif (h - e.y) < 5:
            return 'bottom_resize'
        elif e.y < 25:
            return 'drag'
        return None

    def on_release(self, e):
        if e.widget == self.inner_frame:
            e.y = e.y + 25
        if self._dnd_in_progress:
            self._dnd_in_progress = False
            x, y = self.winfo_x(), self.winfo_y()
            w, h = self.winfo_width(), self.winfo_height()
            pw = self.parent.winfo_width()
            ph = self.parent.winfo_height()
            if x < 0:
                x = 0
            elif (x + w) > pw:
                x = pw - w
            if y < 0:
                y = 0
            elif (y + h) > ph:
                y = ph - h
            self.place(x=x, y=y)
        if self._resize_in_progress:
            self._resize_in_progress = False


class LanguageSelectionFrame(ChildFrame):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, title='language_title', height=80, width=200, allow_close=False, **kwargs)
        self.language_en = ttk.Button(self.inner_frame, text=self.main_app.get_text('english', lang='en'), command=self.choose_en)
        self.language_fr = ttk.Button(self.inner_frame, text=self.main_app.get_text('french', lang='fr'), command=self.choose_fr)
        self.inner_frame.columnconfigure(0, weight=1)
        self.inner_frame.columnconfigure(1, weight=1)
        self.inner_frame.rowconfigure(1, weight=1)
        self.language_fr.grid(column=0, row=1)
        self.language_en.grid(column=1, row=1)

    def choose_en(self):
        self.main_app.set_language('en')

    def choose_fr(self):
        self.main_app.set_language('fr')


class LoginFrame(ChildFrame):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, title="login_title", height=120, width=300, allow_close=False, **kwargs)
        self.username_label = ttk.Label(self.inner_frame, text=self.main_app.get_text("username"))
        self.password_label = ttk.Label(self.inner_frame, text=self.main_app.get_text("password"))
        self.username = tk.StringVar()
        self.password = tk.StringVar()
        self.username_field = ttk.Entry(self.inner_frame, textvariable=self.username)
        self.password_field = ttk.Entry(self.inner_frame, textvariable=self.password, show="*")
        self.login_button = ttk.Button(self.inner_frame, text=self.main_app.get_text("login"), command=self.do_login)
        self.inner_frame.columnconfigure(0, weight=1)
        self.inner_frame.columnconfigure(1, weight=1)
        self.inner_frame.rowconfigure(0, weight=1)
        self.inner_frame.rowconfigure(1, weight=1)
        self.inner_frame.rowconfigure(2, weight=1)
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
            self.main_app.set_login_token(self.username.get(), res['token'], datetime.datetime.fromisoformat(res['expiry']))


class MainApplication(tk.Frame):

    def __init__(self, parent, *args, service_path: str, **kwargs):
        super().__init__(parent, *args, **kwargs)
        self.parent = parent
        self._translator = Translator()
        self._controller = NODBRequestController()
        self._controller.start()
        self.language = 'und'
        self._language_sel = LanguageSelectionFrame(self)
        self._language_sel.place(
            x=(START_WIDTH - 200) / 2,
            y=(START_HEIGHT - 80) / 2
        )
        self._services_path = service_path
        self.parent.after(100, self._check_request_queue)
        self._token = None
        self._expiry = None
        self._auto_renew_sent = False

        self.status_frame = ttk.Frame(self)
        self.status_message = tk.StringVar()
        self.username = tk.StringVar()
        self.status_label = ttk.Label(self.status_frame, textvariable=self.status_message)
        self.status_label.grid(row=0, column=0, sticky="w")
        self.username_label = ttk.Label(self.status_frame, textvariable=self.username)
        self.username_label.grid(row=0, column=1, sticky="e")
        self.status_frame.rowconfigure(0, weight=1)
        self.status_frame.columnconfigure(0, weight=1)

        self.menu = tk.Menu(self.parent)
        self.bind("<Configure>", self.on_configure)

    def build_menu(self):
        self.menu.children.clear()
        #menu_a = tk.Menu(self.menu, tearoff=0)
        #self.menu.add_cascade(label=self.get_text('file_menu'), menu=menu_a)
        #menu_a.add_command(label=self.get_text('file_item'), command=self.hello)

    def on_configure(self, e):
        w, h = self.winfo_width(), self.winfo_height()
        self.status_frame.place(width=w, height=25, x=0, y=h -25)

    def _check_request_queue(self):
        if self._expiry:
            expiry_time = (self._expiry - datetime.datetime.utcnow()).total_seconds()
            if expiry_time < 0:
                mb.showerror(
                    self.get_text("session_expired"),
                    self.get_text("session_expired_long")
                )
                # TODO: handle open dialogs
                self._show_login_frame()
            elif expiry_time < 10 and not self._auto_renew_sent:
                # Attempt to auto-renew
                self._auto_renew_sent = True
                self.set_message(self.get_text("renewing_session"))
                self.make_request("renew", "POST", None, cb=self._auto_renew_callback)
        self._controller.check_results(5)
        self.parent.after(100, self._check_request_queue)

    def set_message(self, msg):
        self.status_message.set(f"{datetime.datetime.now().strftime('%H:%M:%S')}: {msg}")

    def _auto_renew_callback(self, response, status):
        if status == 200 and 'token' in response and 'expiry' in response:
            self._token = response['token']
            self._expiry = datetime.datetime.fromisoformat(response['expiry'])
            self.set_message(self.get_text("session_renewed"))
        self._auto_renew_sent = False

    def cleanup(self):
        self._controller.halt.set()
        self._controller.join()

    def make_request(self, endpoint: str, method: str, data: t.Optional[dict], cb: callable):
        self.set_message(self.get_text('request_info').format(endpoint=endpoint))
        if data is None:
            data = {}
        if self._token:
            data['token'] = self._token
        self._controller.request(
            self._services_path + endpoint,
            method,
            data,
            cb
        )

    def set_language(self, lang: str):
        self.language = lang
        self.winfo_toplevel().title(self.get_text("window_title"))
        self.build_menu()
        self._language_sel.place_forget()
        self._language_sel.destroy()
        del self._language_sel
        if self._token is None:
            self._show_login_frame()

    def _show_login_frame(self):
        self._login_frame = LoginFrame(self)
        self._login_frame.place(
            x=(self.winfo_width() - 300) / 2,
            y=(self.winfo_height() - 120) / 2
        )

    def set_login_token(self, username: str, token: str, expiry: datetime.datetime):
        self.set_message(self.get_text("login_success_name").format(username=username))
        self.username.set(username)
        self._token = token
        self._expiry = expiry
        self._login_frame.place_forget()
        self._login_frame.destroy()
        del self._login_frame

    def get_text(self, resource_name, lang=None):
        return self._translator.translate(lang or self.language, resource_name)


if __name__ == "__main__":
    from styles import set_styles
    root = tk.Tk()
    root.title('')
    root.style = ttk.Style()
    set_styles(root.style)
    root.geometry(f"{START_WIDTH}x{START_HEIGHT}")
    main_app = MainApplication(root, background="#555555", service_path="http://localhost:5000/test000/")
    main_app.pack(side="top", fill="both", expand=True)
    root.config(menu=main_app.menu)
    try:
        root.mainloop()
    finally:
        main_app.cleanup()

