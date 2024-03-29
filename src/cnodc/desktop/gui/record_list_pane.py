import json

from cnodc.desktop.gui.base_pane import BasePane, QCBatchCloseOperation, ApplicationState, DisplayChange, \
    SimpleRecordInfo
from cnodc.desktop.client.local_db import LocalDatabase
from autoinject import injector
import typing as t
from cnodc.desktop.gui.scrollable import ScrollableTreeview
import cnodc.desktop.translations as i18n
import cnodc.ocproc2 as ocproc2
from cnodc.desktop.util import StopAction
from cnodc.ocproc2.operations import QCOperator
import tkinter.messagebox as tkmb
import tkinter.ttk as ttk

from cnodc.ocproc2.validation import OCProc2Ontology


class RecordListPane(BasePane):

    local_db: LocalDatabase = None
    ontology: OCProc2Ontology = None

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
        self._record_label = ttk.Label(self._group, text=i18n.get_text("record_list_title")).grid(row=0, column=0, sticky='NSEW')
        self._subrecord_label = ttk.Label(self._group, text=i18n.get_text("child_record_list_title")).grid(row=2, column=0, sticky='NSEW')
        self._record_list = ScrollableTreeview(
            parent=self._group,
            selectmode="browse",
            show="",
            headers=[
                i18n.get_text('record_list_index'),
                i18n.get_text('record_list_title'),
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
                i18n.get_text('subrecord_list_title'),
            ],
            on_click=self._on_subrecord_click,
            displaycolumns=(0,)
        )
        self._subrecord_list.tag_configure('has-error', foreground='red')
        self._subrecord_list.grid(row=3, column=0, sticky='EWNS')
        self._subrecord_list.table.column('#0', width=30, stretch=False)
        self._subrecord_list.table.column('#1', anchor='w')

    def on_language_change(self, language: str):
        if self._record_label is not None:
            self._record_label.configure(text=i18n.get_text('record_list_title'))
        if self._subrecord_label is not None:
            self._subrecord_label.configure(text=i18n.get_text('child_record_list_title'))

    def refresh_display(self, app_state: ApplicationState, change_type: DisplayChange):
        if change_type & DisplayChange.BATCH:
            self._record_list.clear_items()
            self._subrecord_list.clear_items()
            if app_state.batch_record_info:
                self._build_record_list(app_state)
        if change_type & DisplayChange.RECORD:
            #self._record_list.table.selection(app_state.record_uuid)
            # TODO: set the selection
            self._subrecord_list.clear_items()
            if app_state.record is not None:
                self._build_subrecord_list(app_state.record)
        if change_type & DisplayChange.RECORD_CHILD:
            # TODO: set the selection for child
            pass

    def _build_record_list(self, app_state: ApplicationState):
        for sr in app_state.batch_record_info.values():
            self._record_list.table.insert(
                parent='',
                index='end',
                text=sr.record_uuid,
                values=[sr.index, self._build_top_record_display(sr)],
                tags=['has-error' if sr.has_errors else 'no-error']
            )

    def _build_top_record_display(self, sr: SimpleRecordInfo):
        if sr.timestamp is not None:
            return f"{sr.timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
        return f'{sr.record_uuid}'

    def _build_subrecord_list(self, record: ocproc2.BaseRecord, parent_text: str = '', depth: int = 0):
        for srt in record.subrecords:
            srt_text = f'{parent_text}/subrecords/{srt}' if parent_text else f'subrecords/{srt}'
            for rs_idx in record.subrecords[srt]:
                rs_text = f'{srt_text}/{rs_idx}'
                # TODO: profile flagging of errors
                self._subrecord_list.table.insert(parent_text, 'end', text='', iid=rs_text, values=(self._build_record_set_display(srt, rs_idx, depth), rs_text))
                for idx, record in enumerate(record.subrecords[srt][rs_idx].records):
                    record_text = f"{srt_text}/{rs_idx}/{idx}"
                    # TODO: row flagging of errors
                    self._subrecord_list.table.insert(rs_text, 'end', text='', iid=record_text, values=(self._build_record_display(record, srt, idx, depth + 1), record_text))
                    self._build_subrecord_list(record, record_text, depth + 2)

    def _build_record_set_display(self, subrecord_set_type: str, record_set_idx: int, depth: int):
        name = subrecord_set_type
        einfo = self.ontology.recordset_info(subrecord_set_type)
        if einfo is not None:
            name = einfo.label(i18n.current_language())
        return f'{(" " * (depth * 2))}{name}#{record_set_idx}'

    def _build_record_display(self, record: ocproc2.BaseRecord, srt: str, idx: int, depth: int):
        display = i18n.get_text(f"record_label", index=str(idx))
        einfo = self.ontology.recordset_info(srt)
        if einfo and einfo.coordinates:
            c_names = list(einfo.coordinates)
            c_names.sort()
            for c_name in c_names:
                if record.coordinates.has_value(c_name):
                    value = record.coordinates[c_name]
                    units = value.metadata.best_value('Units')
                    if units:
                        display += f" [{value.to_float()} {units}]"
                    else:
                        display += f" [{value.to_float()}]"
        return f'{(" " * (depth * 2))}{display}'

    def _on_subrecord_click(self, item_info, is_change: bool, event):
        self.app.load_child(item_info['values'][1])

    def _on_record_click(self, item_info, is_change: bool, event):
        self.app.load_record(item_info['text'])







