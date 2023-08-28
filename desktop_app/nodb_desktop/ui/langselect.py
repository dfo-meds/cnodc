
from .child import ChildFrame
import tkinter.ttk as ttk


class LanguageSelectionFrame(ChildFrame):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, title='language_title', child_name='language_select', height=80, width=200, allow_close=False, **kwargs)
        self.language_en = ttk.Button(self.contents, text=self.main_app.get_text('english', lang='en'), command=self.choose_en)
        self.language_fr = ttk.Button(self.contents, text=self.main_app.get_text('french', lang='fr'), command=self.choose_fr)
        self.contents.columnconfigure(0, weight=1)
        self.contents.columnconfigure(1, weight=1)
        self.contents.rowconfigure(1, weight=1)
        self.language_fr.grid(column=0, row=1)
        self.language_en.grid(column=1, row=1)

    def choose_en(self):
        self.main_app.set_language('en')
        self.close_window()

    def choose_fr(self):
        self.main_app.set_language('fr')
        self.close_window()
