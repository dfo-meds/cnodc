from cnodc.desktop.gui.base_pane import BasePane, ApplicationState, DisplayChange
from cnodc.desktop.gui.scrollable import ScrollableTreeview
import typing as t
import tkinter as tk
import datetime
import tkinter.ttk as ttk
import cnodc.desktop.translations as i18n
from cnodc.ocproc2 import MessageType


class HistoryPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._history_list: t.Optional[ScrollableTreeview] = None

    def on_init(self):
        history_frame = ttk.Frame(self.app.bottom_notebook)
        history_frame.rowconfigure(0, weight=1)
        history_frame.columnconfigure(0, weight=1)
        self.app.bottom_notebook.add(history_frame, text='History', sticky='NSEW')
        self._history_list = ScrollableTreeview(
            parent=history_frame,
            selectmode="browse",
            show="headings",
            headers=[
                i18n.get_text('history_time'),
                i18n.get_text('history_message'),
                i18n.get_text('history_source'),
                i18n.get_text('history_type')
            ],
            displaycolumns=(0, 1, 2, 3)
        )
        self._history_list.table.column('#1', width=150, stretch=tk.NO, anchor='w')
        self._history_list.table.column('#2', anchor='w')
        self._history_list.table.column('#3', anchor='w')
        self._history_list.table.column('#4', width=125, stretch=tk.NO, anchor='w')
        self._history_list.grid(row=0, column=0, sticky='NSEW')

    def on_language_change(self, language: str):
        # TODO: treeview headings
        # TODO: notebook label
        # TODO: column 3 (message type) and maybe date/time format?
        pass

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & (DisplayChange.RECORD | DisplayChange.ACTION):
            self._history_list.clear_items()
            if app_state.record is not None:
                for history in app_state.record.history:
                    tags = []
                    if history.message_type == MessageType.ERROR:
                        tags.append('error')
                    if history.message_type == MessageType.WARNING:
                        tags.append('warning')
                    self._history_list.table.insert(
                        parent='',
                        index='end',
                        values=[
                            datetime.datetime.fromisoformat(history.timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                            history.message,
                            f"{history.source_name} {history.source_version} [{history.source_instance}]",
                            i18n.get_text(f'message_type_{history.message_type.value.lower()}')
                        ],
                        tags=tags
                    )


