from gcapp import i18n
from medsutil.awaretime import AwareDateTime
from pipeman_desktop.client.local_db import LocalDatabase
from pipeman_desktop.components.scrollable import ScrollableTreeview
from pipeman_desktop.panes.base_pane import BasePane
import typing as t
import tkinter as tk
import tkinter.ttk as ttk

from autoinject import injector

from pipeman_desktop.state import ApplicationState, DisplayChange


class SourceInfoPane(BasePane):

    local_db: LocalDatabase = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._frames: dict[str, ttk.Frame] = {}
        self._tab_frame: ttk.Frame | None = None
        self._messages: dict[str, ScrollableTreeview] = {}
        self._info: dict[str, ScrollableTreeview] = {}
        self._pane_id: str | None = None

    def on_init(self):
        self._build_frame(self.app.decode_qc_mode, "decode")
        self._build_frame(self.app.merge_qc_mode, "merge")
        self._tab_frame = ttk.Frame(self.app.batch_bottom)
        self._build_frame(self._tab_frame, "batch")
        self.app.batch_bottom.add(self._tab_frame, text=i18n.tr("pane.source_file"))
        self._pane_id = self.app.batch_bottom.tabs()[-1]

    def _build_frame(self, parent, stream: str):
        frame = ttk.Frame(parent)
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=3)
        frame.columnconfigure(1, weight=1)
        frame.grid(row=1, column=0, sticky=tk.NSEW)
        self._frames[stream] = frame

        treeview = ScrollableTreeview(
            parent=frame,
            selectmode="browse",
            show="headings",
            columns=["time", "message", "source", "type"],
        )
        treeview.set_header_text("time", i18n.tr("tree.source_messages.time"))
        treeview.set_header_text("message", i18n.tr("tree.source_messages.message"))
        treeview.set_header_text("source", i18n.tr("tree.source_messages.source"))
        treeview.set_header_text("type", i18n.tr("tree.source_messages.type"))
        treeview.grid(row=0, column=0, sticky=tk.NSEW)
        self._messages[stream] = treeview

        info = ScrollableTreeview(
            parent=frame,
            selectmode="browse",
            show="headings",
            columns=["property", "value"],
        )
        info.set_header_text("property", i18n.tr("tree.source_info.property"))
        info.set_header_text("value", i18n.tr("tree.source_info.value"))
        info.grid(row=0, column=1, sticky=tk.NSEW)
        self._info[stream] = info

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.LANGUAGE:
            for _, treeview in self._messages.items():
                treeview.set_header_text("time", i18n.tr("tree.source_messages.time"))
                treeview.set_header_text("message", i18n.tr("tree.source_messages.message"))
                treeview.set_header_text("source", i18n.tr("tree.source_messages.source"))
                treeview.set_header_text("type", i18n.tr("tree.source_messages.type"))
            for _, info in self._info.items():
                info.set_header_text("property", i18n.tr("tree.source_info.property"))
                info.set_header_text("value", i18n.tr("tree.source_info.value"))
            if self._pane_id is not None:
                self.app.batch_bottom.tab(self._pane_id, text=i18n.tr("pane.source_file"))
            self._rebuild_source_info()
        if change_type & DisplayChange.SOURCE_FILE:
            self._rebuild_source_info()

    def _add_message(self, idx: int, history_entry: dict[str, str]):
        row = (
            AwareDateTime.fromisoformat(history_entry['rpt']).strftime("%Y-%m-%d %H:%M"),
            history_entry["msg"],
            f"{history_entry['src']} {history_entry['ver']} [{history_entry['ins']}]",
            i18n.tr(f"source_message.{history_entry['lvl'].lower()}")
        )
        for _, treeview in self._messages.items():
            treeview.append_item(str(idx), values=row)

    def _add_property(self, property_name: str, value: str):
        for _, info in self._info.items():
            info.append_item(property_name, values=(property_name, value))

    def _rebuild_source_info(self):
        for _, treeview in self._messages.items():
            treeview.clear_items()
        for _, info in self._info.items():
            info.clear_items()
        if self.app.state.current_file_id is not None:
            with self.local_db.cursor() as cur:
                cur.execute("SELECT source_uuid, filename, file_path, source, program, history, received_date, metadata, is_payload from files WHERE rowid = ?", (self.app.state.current_file_id,))
                row = cur.fetchone()
                if row:
                    history = json.loads(row[5])
                    for idx, history_entry in enumerate(history):
                        self._add_message(idx, history_entry)
                    self._add_property(i18n.tr("file_property.source_uuid"), row[0])
                    self._add_property(i18n.tr("file_property.received_date"), row[6])
                    self._add_property(i18n.tr("file_property.file_name"), row[1])
                    self._add_property(i18n.tr("file_property.file_path"), row[2])
                    self._add_property(i18n.tr("file_property.source"), row[3])
                    self._add_property(i18n.tr("file_property.program"), row[4])



