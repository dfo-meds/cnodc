from medsutil.awaretime import AwareDateTime
from pipeman_desktop.panes.base_pane import BasePane
from pipeman_desktop.state import DisplayChange, ApplicationState
from pipeman_desktop.components.scrollable import ScrollableTreeview
import typing as t
import tkinter.ttk as ttk
import gcapp.i18n.base as i18n


class ErrorPane(BasePane):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._error_list: t.Optional[ScrollableTreeview] = None
        self._pane_id: str | None = None

    def on_init(self):
        error_frame = ttk.Frame(self.app.middle_bottom)
        error_frame.rowconfigure(0, weight=1)
        error_frame.columnconfigure(0, weight=1)
        self._error_list = ScrollableTreeview(
            parent=error_frame,
            selectmode='browse',
            show='headings',
            columns=["name", "review", "time", "error", "element"],
            on_click=self._on_click
        )
        self._error_list.set_header_text("name", i18n.tr("tree.qc_errors.name"))
        self._error_list.set_header_text("review", i18n.tr("tree.qc_errors.review"))
        self._error_list.set_header_text("time", i18n.tr("tree.qc_errors.time"))
        self._error_list.set_header_text("error", i18n.tr("tree.qc_errors.error"))
        self._error_list.set_header_text("element", i18n.tr("tree.qc_errors.element"))
        self._error_list.grid(row=0, column=0, sticky='NSEW')
        self.app.middle_bottom.add(error_frame, text=i18n.tr("pane.qc_errors"), sticky='NSEW')
        self._pane_id = self.app.middle_bottom.tabs()[-1]

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.RECORD:
            if self._error_list is not None:
                self.update_errors()
        if change_type & DisplayChange.LANGUAGE:
            if self._error_list is not None:
                self._error_list.set_header_text("name", i18n.tr("tree.qc_errors.name"))
                self._error_list.set_header_text("review", i18n.tr("tree.qc_errors.review"))
                self._error_list.set_header_text("time", i18n.tr("tree.qc_errors.time"))
                self._error_list.set_header_text("error", i18n.tr("tree.qc_errors.error"))
                self._error_list.set_header_text("element", i18n.tr("tree.qc_errors.element"))
                self.update_errors()
            if self._pane_id is not None:
                self.app.middle_bottom.tab(self._pane_id, text=i18n.tr("pane.qc_errors"))

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
                                i18n.tr(f'qc_test.{result.test_name.lower()}', default=result.test_name),
                                i18n.tr(f"qc_review.{message.review_name}", default=message.review_name),
                                AwareDateTime.fromisoformat(result.test_date).strftime("%Y-%m-%d %H:%M"),
                                i18n.tr(f'qc_error_code.{message.code.lower()}', default=message.code),
                                message.record_path
                            ]
                        )

    def _on_click(self, item, *args):
        self.app.state.load_closest(item['values'][3])






