import functools

from cnodc.desktop.client.local_db import LocalDatabase
from cnodc.desktop.gui.base_pane import BasePane, QCBatchCloseOperation, ApplicationState
import typing as t
import cnodc.desktop.translations as i18n
from cnodc.desktop.gui.scrollable import ScrollableTreeview
from cnodc.ocproc2.operations import QCOperator
import tkinter as tk
from autoinject import injector


class ActionPane(BasePane):

    local_db: LocalDatabase = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._action_list: t.Optional[ScrollableTreeview] = None

    def on_init(self):
        self.app.middle_frame.columnconfigure(0, weight=1)
        self.app.middle_frame.rowconfigure(0, weight=4)
        self.app.middle_frame.rowconfigure(1, weight=1)
        self._action_list = ScrollableTreeview(
            parent=self.app.middle_frame,
            selectmode='browse',
            show='headings',
            headers=[
                i18n.get_text('action_item_name'),
                i18n.get_text('action_item_object'),
                i18n.get_text('action_item_value')
            ],
            on_right_click=self._on_action_right_click,
            displaycolumns=(0, 1, 2)
        )
        self._action_list.grid(row=1, column=0, sticky='NEWS')
        self._action_list.table.column('#1', width=50, anchor='w', stretch=tk.NO)
        self._action_list.table.column('#2', width=250, anchor='w')
        self._action_list.table.column('#3', width=150, anchor='w')

    def refresh_display(self, app_state: ApplicationState):
        self._action_list.clear_items()
        for action_id in sorted(app_state.actions.keys()):
            self._add_action_item(action_id, app_state.actions[action_id])

    def _add_action_item(self, action_id: int, action: QCOperator):
        self._action_list.table.insert(
            parent='',
            index='end',
            values=[action.name, action.object, action.value, action_id],
            iid=str(action_id)
        )

    def _on_action_right_click(self, item, e):
        menu = tk.Menu(self.app.root, tearoff=0)
        menu.add_command(
            label=i18n.get_text('remove'),
            command=functools.partial(self._remove_item, db_index=item['values'][-1])
        )
        try:
            menu.tk_popup(e.x_root, e.y_root, 0)
        finally:
            menu.grab_release()

    def _remove_item(self, db_index: int):
        self.app.delete_operation(db_index)
