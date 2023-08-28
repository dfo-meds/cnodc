import tkinter as tk
import tkinter.ttk as ttk
from nodb_desktop.business import DesktopAppController


class ChildFrame(ttk.Frame):

    def __init__(self, parent: tk.Frame, main_app: DesktopAppController, child_name: str, title: str = "", title_raw: str = "", allow_close: bool = True, **kwargs):
        super().__init__(parent, style="ChildFrame.TFrame", **kwargs)
        self.parent: tk.Frame = parent
        self.child_name = child_name
        self.main_app: DesktopAppController = main_app
        self.width = kwargs['width'] if 'width' in kwargs else 1
        self.height = kwargs['height'] if 'height' in kwargs else 1
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
        self.contents = ttk.Frame(self, style="ChildInnerFrame.TFrame", borderwidth=5, relief="sunken")
        self.contents.bind("<ButtonPress>", self.on_press)
        self.contents.bind("<ButtonRelease>", self.on_release)
        self.contents.bind("<Motion>", self.on_motion)
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

    def refresh_interface(self):
        pass

    def on_close(self):
        self.close_window()

    def close_window(self):
        self.parent.close_child_frame(self.child_name)

    def on_configure(self, e):
        self.width = self.winfo_width()
        self.height = self.winfo_height()
        self.label.place(x=0, y=0, width=self.width, height=25)
        self.contents.place(x=0, y=25, width=self.width, height=self.height - 25)
        if self.close_button is not None:
            self.close_button.place(x=self.width - 25, y=4, w=17, h=17)

    def on_press(self, e):
        if e.widget == self.contents:
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
        if e.widget == self.contents:
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
        if e.widget == self.contents:
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
