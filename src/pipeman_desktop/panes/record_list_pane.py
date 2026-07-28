from pipeman_desktop.i18n import OCProc2Translator
from pipeman_desktop.panes.base_pane import BasePane
from pipeman_desktop.state import DisplayChange, SimpleRecordInfo, ApplicationState
from pipeman_desktop.client.local_db import LocalDatabase
from autoinject import injector
import typing as t
from pipeman_desktop.components.scrollable import ScrollableTreeview
import gcapp.i18n as i18n
import medsutil.ocproc2 as ocproc2
import tkinter.ttk as ttk

from medsutil.ocproc2 import OCProc2Ontology


class RecordListPane(BasePane):

    local_db: LocalDatabase = None
    ontology: OCProc2Ontology = None
    translator: OCProc2Translator

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._group: t.Optional[ttk.Frame] = None
        self._record_list: t.Optional[ScrollableTreeview] = None
        self._subrecord_list: t.Optional[ScrollableTreeview] = None
        self._record_label: t.Optional[ttk.Label] = None
        self._subrecord_label: t.Optional[ttk.Label] = None
        self._current_record_info = None
        self._current_subrecord_info = None

    def on_init(self):
        self._group = ttk.Frame(self.app.left_frame)
        self._group.grid(row=0, column=0, sticky='NSEW')
        self._group.rowconfigure(0, weight=0)
        self._group.rowconfigure(1, weight=3)
        self._group.rowconfigure(2, weight=0)
        self._group.rowconfigure(3, weight=1)
        self._group.columnconfigure(0, weight=1)
        # TODO: label styling
        self._record_label = ttk.Label(self._group, text=i18n.tr("record_list_title")).grid(row=0, column=0, sticky='NSEW')
        self._subrecord_label = ttk.Label(self._group, text=i18n.tr("child_record_list_title")).grid(row=2, column=0, sticky='NSEW')
        self._record_list = ScrollableTreeview(
            parent=self._group,
            selectmode="browse",
            show="",
            headers=[
                i18n.tr('record_list_index'),
                i18n.tr('record_list_title'),
            ],
            on_click=self._on_record_click,
            displaycolumns=(0, 1)
        )
        self._record_list.tag_configure('has-error', foreground='red')
        self._record_list.grid(row=1, column=0, sticky='EWNS')
        self._record_list.table.column('#1', anchor='w', stretch=False, width=45)
        self._record_list.table.column('#2', anchor='w')
        self._subrecord_list = ScrollableTreeview(
            parent=self._group,
            selectmode="browse",
            show="tree",
            headers=[
                i18n.tr('subrecord_list_title'),
            ],
            on_click=self._on_subrecord_click,
            displaycolumns=(0,)
        )
        self._subrecord_list.tag_configure('has-error', foreground='red')
        self._subrecord_list.grid(row=3, column=0, sticky='EWNS')
        self._subrecord_list.table.column('#0', width=30, stretch=False)
        self._subrecord_list.table.column('#1', anchor='w')

    def on_language_change(self):
        if self._record_label is not None:
            self._record_label.configure(text=i18n.tr('record_list_title'))
        if self._subrecord_label is not None:
            self._subrecord_label.configure(text=i18n.tr('child_record_list_title'))

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.BATCH_STATE:
            if self._record_list is not None:
                self._record_list.clear_items()
            if self._subrecord_list is not None:
                self._subrecord_list.clear_items()
            self._build_record_list()
        if change_type & DisplayChange.RECORD:
            if self._record_list is not None:
                self._record_list.set_selection([self.app.state.current_working_uuid], _ignore_callback=True)
            if self._subrecord_list is not None:
                self._subrecord_list.clear_items()
            if app_state.current_parent is not None:
                self._build_subrecord_list(self.app.state.current_parent)
        if change_type & DisplayChange.RECORD_CHILD:
            if self._subrecord_list is not None and self.app.state.current_child_path is not None:
                self._subrecord_list.set_selection([self.app.state.current_child_path])

    def _build_record_list(self):
        for sr in self.app.state.batch_records.values():
            self._record_list.append_item(
                parent='',
                iid=sr.record_uuid,
                values=(sr.index, self._build_top_record_display(sr)),
                tags=('has-error' if sr.has_errors else 'no-error',)
            )

    def _build_top_record_display(self, sr: SimpleRecordInfo):
        if sr.timestamp is not None:
            return f"{sr.timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
        return f'{sr.record_uuid}'

    def _build_subrecord_list(self, record: ocproc2.BaseRecord, parent_text: str = '', depth: int = 0):
        if self._subrecord_list is not None:
            for srt in record.subrecords:
                srt_text = f'{parent_text}/subrecords/{srt}' if parent_text else f'subrecords/{srt}'
                for rs_idx in record.subrecords[srt]:
                    rs_text = f'{srt_text}/{rs_idx}'
                    # TODO: profile flagging of errors
                    self._subrecord_list.append_item(
                        iid=rs_text,
                        parent=parent_text,
                        values=(self._build_record_set_display(srt, rs_idx, depth), rs_text)
                    )
                    for idx, record in enumerate(record.subrecords[srt][rs_idx].records.iterate_with_load()):
                        record_text = f"{srt_text}/{rs_idx}/{idx}"
                        # TODO: row flagging of errors
                        self._subrecord_list.append_item(
                            iid=record_text,
                            parent=rs_text,
                            values=(self._build_record_display(record, srt, idx, depth + 1), record_text)
                        )
                        self._build_subrecord_list(record, record_text, depth + 2)

    def _build_record_set_display(self, subrecord_set_type: str, record_set_idx: int, depth: int):
        return f'{(" " * (depth * 2))}{self.translator.translate_recordset_type(subrecord_set_type)} #{record_set_idx}'

    def _build_record_display(self, record: ocproc2.BaseRecord, srt: str, idx: int, depth: int):
        display = i18n.tr(f"record_label", index=str(idx))
        c_names = list(x for x in record.coordinates.keys())
        c_names.sort()
        for c_name in c_names:
            ideal = record.coordinates.ideal(c_name)
            if ideal and not ideal.is_empty():
                value = ideal.to_float()
                units = ideal.units()
                if units:
                    display += f" [{value} {units}]"
                else:
                    display += f" [{value}]"
        return f'{(" " * (depth * 2))}{display}'

    def _on_subrecord_click(self, item_info, is_change: bool, event):
        self.app.state.update_subrecord(item_info['values'][1])

    def _on_record_click(self, item_info, is_change: bool, event):
        self.app.state.update_record(item_info['iid'])







