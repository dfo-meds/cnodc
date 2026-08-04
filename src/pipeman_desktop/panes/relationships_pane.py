import functools
import pathlib

import gcapp.i18n as i18n
from medsutil.ocproc2 import ParentRecord
from pipeman_desktop.components.scrollable import ScrollableTreeview

from pipeman_desktop.panes.base_pane import BasePane
from pipeman_desktop.util import BatchOpenState
from pipeman_desktop.state import DisplayChange, ApplicationState
import tkintermapview as tkmv
import typing as t
import tkinter.ttk as ttk
from autoinject import injector
from pipeman_desktop.client.local_db import LocalDatabase
import PIL.Image as Image
import PIL.ImageTk as ImageTk


class RelationshipsPane(BasePane):

    local_db: LocalDatabase = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._panel: ttk.Frame | None = None
        self._platform_pane: ttk.Frame | None = None
        self._pane_id: str | None = None
        self._platform_list_label: ttk.Label | None = None
        self._platform_list: ScrollableTreeview | None = None

    def on_init(self):
        self._panel = ttk.Frame(self.app.middle)
        self.app.middle.add(self._panel, text=i18n.tr("pane.relationships", sticky="NSEW"))
        self._pane_id = self.app.middle.tabs()[-1]

        self._platform_list_label = ttk.Label(self._panel, text=i18n.tr("tree.record_platform_list.title"))
        self._platform_list_label.grid(row=0, column=0, sticky="NSEW")
        self._platform_list = ScrollableTreeview(
            parent=self._panel,
            selectmode="browse",
            show="headings",
            columns=["uuid", "wmo_id", "wigos_id", "name", "id", "start", "end"],
            on_right_click=self._on_platform_right_click
        )
        self._platform_list.set_header_text("uuid", i18n.tr("tree.platform_list.uuid"))
        self._platform_list.set_header_text("wmo_id", i18n.tr("tree.platform_list.wmo_id"))
        self._platform_list.set_header_text("wigos_id", i18n.tr("tree.platform_list.wigos_id"))
        self._platform_list.set_header_text("name", i18n.tr("tree.platform_list.name"))
        self._platform_list.set_header_text("id", i18n.tr("tree.platform_list.id"))
        self._platform_list.set_header_text("start", i18n.tr("tree.platform_list.start"))
        self._platform_list.set_header_text("end", i18n.tr("tree.platform_list.end"))
        self._platform_list.grid(row=1, column=0, sticky="NSEW")

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.LANGUAGE:
            if self._pane_id is not None:
                self.app.middle.tab(self._pane_id, text=i18n.tr("pane.relationships"))
            if self._platform_list is not None:
                self._platform_list.set_header_text("uuid", i18n.tr("tree.platform_list.uuid"))
                self._platform_list.set_header_text("wmo_id", i18n.tr("tree.platform_list.wmo_id"))
                self._platform_list.set_header_text("wigos_id", i18n.tr("tree.platform_list.wigos_id"))
                self._platform_list.set_header_text("name", i18n.tr("tree.platform_list.name"))
                self._platform_list.set_header_text("id", i18n.tr("tree.platform_list.id"))
                self._platform_list.set_header_text("start", i18n.tr("tree.platform_list.start"))
                self._platform_list.set_header_text("end", i18n.tr("tree.platform_list.end"))
        if change_type & (DisplayChange.RECORD | DisplayChange.PLATFORMS):
            self._update_platform_options(self.app.state.current_parent)

    def _update_platform_options(self, record: ParentRecord | None):
        self._platform_list.clear_items()
        if record is not None:
            platform_uuids: list[str] = record.metadata.best("CNODCPlatformCandidates", default=[], coerce=list)
            if platform_uuids:
                with self.local_db.cursor() as cur:
                    query = """
                        SELECT platform_uuid, wmo_id, wigos_id, platform_name, platform_id, service_start_date, service_end_date, actions 
                        FROM platforms
                        WHERE platform_uuid IN ( 
                    """
                    query += ",".join("?" for _ in platform_uuids)
                    query += ")"
                    cur.execute(query, platform_uuids)
                    for row in cur.fetchall():
                        self._platform_list.append_item(
                            iid=row[0],
                            parent='',
                            values=tuple(x or "" for x in row[:-1]),
                            text=''
                        )


    def _on_platform_right_click(self, item_info: dict, e):
        ...

