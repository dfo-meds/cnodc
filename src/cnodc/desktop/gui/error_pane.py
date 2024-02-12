from cnodc.desktop.gui.base_pane import BasePane, ApplicationState, DisplayChange
from cnodc.desktop.gui.scrollable import ScrollableTreeview
import typing as t
import tkinter as tk
import datetime
import tkinter.ttk as ttk
import cnodc.desktop.translations as i18n
from cnodc.ocproc2.structures import MessageType


class ErrorPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._error_list: t.Optional[ScrollableTreeview] = None

    def on_init(self):
        error_frame = ttk.Frame(self.app.bottom_notebook)
        error_frame.rowconfigure(0, weight=1)
        error_frame.columnconfigure(0, weight=1)
        self.app.bottom_notebook.add(error_frame, text='QC Errors', sticky='NSEW')
        self._error_list = ScrollableTreeview(
            parent=error_frame,
            selectmode='browse',
            show='headings',
            headers=[
                i18n.get_text('qc_test_name'),
                i18n.get_text('qc_test_time'),
                i18n.get_text('qc_test_error_name'),
                i18n.get_text('qc_test_element_name')
            ],
            displaycolumns=(0, 1, 2, 3),
            on_click=self._on_click
        )
        self._error_list.grid(row=0, column=0, sticky='NSEW')

    def on_language_change(self, language: str):
        # TODO: treeview headings
        # TODO: notebook label
        # columns 0 and 2 (test name, error name)
        pass

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & (DisplayChange.RECORD | DisplayChange.ACTION):
            self._error_list.clear_items()
            if app_state.record is not None and app_state.batch_test_names:
                for test_name in app_state.batch_test_names:
                    result = app_state.record.latest_test_result(test_name, True)
                    if result is not None:
                        for message in result.messages:
                            self._error_list.table.insert(
                                parent='',
                                index='end',
                                values=[
                                    i18n.get_text(f'qc_test_{result.test_name.lower()}'),
                                    result.test_date,
                                    i18n.get_text(f'qc_error_{message.code.lower()}'),
                                    message.record_path
                                ]
                            )

    def _on_click(self, item, *args):
        self.app.load_closest_child(item['values'][3])






