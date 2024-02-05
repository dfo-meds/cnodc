import pathlib
from PIL import Image, ImageTk
import tkinter as tk
import tkinter.ttk as ttk



class LoadingWheel(ttk.Label):

    def __init__(self, root: tk.Tk, parent):
        super().__init__(parent)
        self._image_path = pathlib.Path(__file__).absolute().parent.parent / 'resources' / 'loading.gif'
        self._images = []
        self._max_height = 20
        self._halt = False
        self._blank = ImageTk.PhotoImage(Image.new('RGBA', (self._max_height, self._max_height)))
        with Image.open(self._image_path) as image:
            for i in range(0, image.n_frames):
                try:
                    image.seek(i)
                    subimage = image.resize((self._max_height, self._max_height))
                    self._images.append(ImageTk.PhotoImage(subimage))
                except tk.TclError:
                    break
        self._current_frame = None
        self._max_frame = len(self._images)
        self._root = root
        self.configure(image=self._blank, relief=tk.SOLID, borderwidth=2)
        self._delay = 50

    def destroy(self):
        self._halt = True
        self._current_frame = None
        del self._images
        del self._blank

    def enable(self):
        if self._current_frame is None:
            self._current_frame = 0
            self.configure(image=self._images[self._current_frame])
            self._root.after(self._delay, self._update_frame)

    def _update_frame(self):
        if self._current_frame is not None:
            self._current_frame += 1
            if self._current_frame >= self._max_frame:
                self._current_frame = 0
            self.configure(image=self._images[self._current_frame])
            self._root.after(self._delay, self._update_frame)

    def disable(self):
        self._current_frame = None
        if not self._halt:
            self.configure(image=self._blank)




