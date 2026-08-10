import functools

from pipeman_desktop.client.local_db import LocalDatabase
from pipeman_desktop.panes.base_pane import BasePane
from pipeman_desktop.state import DisplayChange, ApplicationState
import typing as t
import gcapp.i18n.base as i18n
from pipeman_desktop.components.scrollable import ScrollableTreeview
from medsutil.ocproc2.operations import RecordAction
import tkinter as tk
import tkinter.ttk as ttk
from autoinject import injector


class ActionPane(BasePane):

    local_db: LocalDatabase = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._action_list: t.Optional[ScrollableTreeview] = None
        self._pane_id: str | None = None

    def on_init(self):
        action_frame = ttk.Frame(self.app.batch_bottom)
        action_frame.rowconfigure(0, weight=1)
        action_frame.columnconfigure(0, weight=1)
        self._action_list = ScrollableTreeview(
            parent=action_frame,
            selectmode='browse',
            show='headings',
            columns=["name", "object", "value"],
            on_right_click=self._on_action_right_click,
        )
        self._action_list.set_header_text("name", i18n.tr("tree.action_items.name"))
        self._action_list.set_header_text("object", i18n.tr("tree.action_items.object"))
        self._action_list.set_header_text("value", i18n.tr("tree.action_items.value"))
        self._action_list.grid(row=0, column=0, sticky='NEWS')
        self._action_list.table.column('#1', width=50, anchor='w', stretch=tk.NO)
        self._action_list.table.column('#2', width=250, anchor='w')
        self._action_list.table.column('#3', width=150, anchor='w')
        self.app.batch_bottom.add(action_frame, text=i18n.tr("pane.action_items"), sticky='NSEW')
        self._pane_id = self.app.batch_bottom.tabs()[-1]

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.ACTION:
            if self._action_list is not None:
                self._rebuild_action_list()
        if change_type & DisplayChange.LANGUAGE:
            if self._action_list is not None:
                self._action_list.set_header_text("name", i18n.tr("tree.action_items.name"))
                self._action_list.set_header_text("object", i18n.tr("tree.action_items.object"))
                self._action_list.set_header_text("value", i18n.tr("tree.action_items.value"))
                self._rebuild_action_list()
            if self._pane_id is not None:
                self.app.batch_bottom.tab(self._pane_id, text=i18n.tr("pane.action_items"))

    def _rebuild_action_list(self):
        self._action_list.clear_items()
        actions = self.app.state.record_actions
        for action_id in sorted(list(actions.keys())):
            self._add_action_item(action_id, actions[action_id])

    def _add_action_item(self, action_id: int, action: RecordAction):
        self._action_list.table.insert(
            parent='',
            index='end',
            # TODO: better format action names?
            values=[i18n.tr(action.name), action.object, action.value, action_id],
            iid=str(action_id)
        )

    def _on_action_right_click(self, item, e):
        menu = tk.Menu(self.app.root, tearoff=0)
        menu.add_command(
            label=i18n.tr('desktop.goto'),
            command=functools.partial(self._goto_item, path=item['values'][1])
        )
        menu.add_command(
            label=i18n.tr('desktop.remove'),
            command=functools.partial(self._remove_item, db_index=item['values'][-1])
        )
        try:
            menu.tk_popup(e.x_root, e.y_root, 0)
        finally:
            menu.grab_release()

    def _remove_item(self, db_index: int):
        self.app.state.delete_action(db_index)

    def _goto_item(self, path: str):
        self.app.state.load_closest(path)
