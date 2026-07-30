from pipeman_desktop.panes.base_pane import BasePane
from pipeman_desktop.state import DisplayChange, ApplicationState
from pipeman_desktop.components.scrollable import ScrollableTreeview
import typing as t
import tkinter as tk
import datetime
import tkinter.ttk as ttk
import gcapp.i18n.base as i18n
from medsutil.ocproc2 import MessageType


class HistoryPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._history_list: t.Optional[ScrollableTreeview] = None
        self._pane_id: str | None = None

    def on_init(self):
        history_frame = ttk.Frame(self.app.middle_bottom)
        history_frame.rowconfigure(0, weight=1)
        history_frame.columnconfigure(0, weight=1)
        self._history_list = ScrollableTreeview(
            parent=history_frame,
            selectmode="browse",
            show="headings",
            columns=["time", "message", "source", "type"],
        )
        self._history_list.set_header_text("time", i18n.tr("tree.history.time"))
        self._history_list.set_header_text("message", i18n.tr("tree.history.message"))
        self._history_list.set_header_text("source", i18n.tr("tree.history.source"))
        self._history_list.set_header_text("type", i18n.tr("tree.history.type"))
        self._history_list.table.column('#1', width=150, stretch=tk.NO, anchor='w')
        self._history_list.table.column('#2', anchor='w')
        self._history_list.table.column('#3', anchor='w')
        self._history_list.table.column('#4', width=125, stretch=tk.NO, anchor='w')
        self._history_list.grid(row=0, column=0, sticky='NSEW')
        self.app.middle_bottom.add(history_frame, text=i18n.tr("pane.history"), sticky='NSEW')
        self._pane_id = self.app.middle_bottom.tabs()[-1]

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & (DisplayChange.RECORD | DisplayChange.ACTION):
            if self._history_list is not None:
                self.update_history_display()
        if change_type & DisplayChange.LANGUAGE:
            if self._history_list is not None:
                self._history_list.set_header_text("time", i18n.tr("tree.history.time"))
                self._history_list.set_header_text("message", i18n.tr("tree.history.message"))
                self._history_list.set_header_text("source", i18n.tr("tree.history.source"))
                self._history_list.set_header_text("type", i18n.tr("tree.history.type"))
                self.update_history_display()
            if self._pane_id is not None:
                self.app.middle_bottom.tab(self._pane_id, text=i18n.tr("pane.history"))

    def update_history_display(self):
        if self._history_list is not None:
            self._history_list.clear_items()
            if self.app.state.current_parent is not None:
                for history in self.app.state.current_parent.history:
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
                            i18n.tr(f'message_type.{history.message_type.value.lower()}')
                        ],
                        tags=tags
                    )


