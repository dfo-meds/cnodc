from pipeman_desktop.panes.base_pane import BasePane
from pipeman_desktop.state import DisplayChange, ApplicationState
from pipeman_desktop.components.scrollable import ScrollableTreeview
import typing as t
import tkinter.ttk as ttk
import gcapp.i18n as i18n


class ErrorPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._error_list: t.Optional[ScrollableTreeview] = None
        self._pane_id: str | None = None

    def on_init(self):
        error_frame = ttk.Frame(self.app.bottom_notebook)
        error_frame.rowconfigure(0, weight=1)
        error_frame.columnconfigure(0, weight=1)
        self._error_list = ScrollableTreeview(
            parent=error_frame,
            selectmode='browse',
            show='headings',
            headers=[
                i18n.tr('qc_test_name'),
                i18n.tr('qc_test_time'),
                i18n.tr('qc_test_error_name'),
                i18n.tr('qc_test_element_name')
            ],
            displaycolumns=(0, 1, 2, 3),
            on_click=self._on_click
        )
        self._error_list.grid(row=0, column=0, sticky='NSEW')
        self.app.bottom_notebook.add(error_frame, text=i18n.tr("pane_qc_errors"), sticky='NSEW')
        self._pane_id = self.app.bottom_notebook.tabs()[-1]

    def on_language_change(self):
        if self._error_list is not None:
            self._error_list.set_headers([
                i18n.tr('qc_test_name'),
                i18n.tr('qc_test_time'),
                i18n.tr('qc_test_error_name'),
                i18n.tr('qc_test_element_name')
            ])
        if self._pane_id is not None:
            self.app.bottom_notebook.tab(self._pane_id, text=i18n.tr("pane_qc_errors"))
        self.update_errors()

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.RECORD:
            self.update_errors()

    def update_errors(self):
        if self._error_list is not None:
            self._error_list.clear_items()
            if self.app.state.current_parent is not None:
                for idx, result in enumerate(self.app.state.current_parent.qc_tests.iterate_with_load()):
                    for message in result.messages:
                        self._error_list.table.insert(
                            parent='',
                            index='end',
                            values=[
                                i18n.tr(f'qc_test_{result.test_name.lower()}'),
                                result.test_date,
                                i18n.tr(f'qc_error_{message.code.lower()}'),
                                message.record_path
                            ]
                        )

    def _on_click(self, item, *args):
        self.app.state.load_closest(item['values'][3])






