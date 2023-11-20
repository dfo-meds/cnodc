import tkinter as tk
import tkinter.ttk as ttk
from .child import ChildFrame

from nodb_desktop.business import DesktopAppController
from nodb_desktop.ui.langselect import LanguageSelectionFrame
from nodb_desktop.ui.login import LoginFrame


class DesktopApp(tk.Tk):

    def __init__(self, controller: DesktopAppController, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.controller = controller
        self.title('')
        self.style = ttk.Style()
        from .styles import set_styles
        set_styles(self.style)
        self.geometry(f"1200x700")
        self.top_frame = MainApplication(
            self,
            self.controller,
            background="#555555"
        )
        self.top_frame.pack(side="top", fill="both", expand=True)
        self.top_menu = tk.Menu(self)
        self.menu_items: list[tk.Menu] = []
        self.config(menu=self.top_menu)
        self.after(100, self._timer_check)

    def _timer_check(self):
        self.controller.timed_check()
        self.after(100, self._timer_check)

    def start(self):
        self.mainloop()

    def close(self):
        pass

    def refresh_interface(self):
        self.top_frame.refresh_interface()

    def clear_menu(self):
        for x in self.menu_items:
            self.top_menu.delete(0)
            x.destroy()
        self.top_menu.destroy()
        del self.top_menu
        self.top_menu = tk.Menu()
        self.config(menu=self.top_menu)

    def add_menu_item(self, label: str, menu: tk.Menu, **kwargs):
        self.top_menu.add_cascade(label=label, menu=menu, **kwargs)
        self.menu_items.append(menu)


class MainApplication(tk.Frame):

    def __init__(self, parent: DesktopApp, controller: DesktopAppController, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)
        self.parent = parent
        self.controller = controller
        self.username = tk.StringVar()
        self.status_message = tk.StringVar()
        self.status_frame = ttk.Frame(self)
        self.status_label = ttk.Label(self.status_frame, textvariable=self.status_message)
        self.username_label = ttk.Label(self.status_frame, textvariable=self.username)
        self.status_label.grid(row=0, column=0, sticky="w")
        self.username_label.grid(row=0, column=1, sticky="e")
        self.status_frame.rowconfigure(0, weight=1)
        self.status_frame.columnconfigure(0, weight=1)

        self.open_frames: dict[str, ChildFrame] = {}

        self._booted = False
        self.bind("<Configure>", self.on_configure)

    def refresh_interface(self):
        self._build_menu()
        for name in self.open_frames:
            self.open_frames[name].refresh_interface()

    def _build_menu(self):
        self.parent.clear_menu()
        file_menu = tk.Menu(self.parent.top_menu, tearoff=False)
        file_menu.add_command(
            label=self.controller.get_text("menu_login"),
            command=self.show_login
        )
        file_menu.add_command(
            label=self.controller.get_text("test"),
            command=self.test_qc
        )
        self.parent.add_menu_item(
            label=self.controller.get_text("menu_file"),
            menu=file_menu
        )

    def on_configure(self, e):
        w, h = self.winfo_width(), self.winfo_height()
        if w > 1 and not self._booted:
            self._booted = True
            self.on_boot()
        self.status_frame.place(width=w, height=25, x=0, y=h -25)

    def on_boot(self):
        self.refresh_interface()
        self.show_language_select()

    def open_child_frame(self, f: ChildFrame):
        if f.child_name not in self.open_frames:
            self.open_frames[f.child_name] = f
            f.place(
                x=(self.winfo_width() - f.width) / 2,
                y=(self.winfo_height() - f.height) / 2
            )

    def close_child_frame(self, name):
        if name in self.open_frames:
            self.open_frames[name].place_forget()
            self.open_frames[name].destroy()
            del self.open_frames[name]

    def show_language_select(self):
        self.open_child_frame(
            LanguageSelectionFrame(self, self.controller)
        )

    def show_login(self):
        self.open_child_frame(
            LoginFrame(self, self.controller)
        )

    def test_qc(self):
        from .qc_base import QualityControlFrame, QCBatchController
        self.open_child_frame(
            QualityControlFrame(self, self.controller, child_name="hello_world", controller=QCBatchController(self.controller))
        )
