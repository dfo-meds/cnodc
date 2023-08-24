import tkinter.ttk as ttk


def set_styles(style: ttk.Style):
    # Main frame body
    style.configure(
        "ChildFrame.TFrame",
        background="#CCCCCC",
    )
    style.configure(
        "TLabel",
        #font=("Helvetica", 10, 'bold'),
    )
    style.configure(
        "TitleBar.TLabel",
        font=("TkSmallCaptionFont"),
        background="#AAAAAA",
        foreground="#333333"
    )
