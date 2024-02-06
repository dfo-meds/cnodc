import json

from cnodc.desktop.gui.base_pane import BasePane
from cnodc.desktop.client.local_db import LocalDatabase
from autoinject import injector
import typing as t
from cnodc.desktop.gui.scrollable import ScrollableTreeview
import cnodc.desktop.translations as i18n
import cnodc.ocproc2.structures as ocproc2


class RecordListPane(BasePane):

    local_db: LocalDatabase = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._record_list: t.Optional[ScrollableTreeview] = None
        self._subrecord_list: t.Optional[ScrollableTreeview] = None

    def on_init(self):
        self.app.middle_top_frame.columnconfigure(0, weight=1)
        self.app.middle_bottom_frame.columnconfigure(0, weight=1)
        self._record_list = ScrollableTreeview(
            parent=self.app.middle_top_frame,
            selectmode="browse",
            show="",
            headers=[
                i18n.get_text('record_list_title'),
            ],
            on_click=self._on_record_click,
            displaycolumns=(1,)
        )
        self._record_list.tag_configure('has-error', foreground='red')
        self._record_list.grid(row=0, column=0, sticky='EWNS')
        self._subrecord_list = ScrollableTreeview(
            parent=self.app.middle_bottom_frame,
            selectmode="browse",
            show="tree",
            headers=[
                i18n.get_text('subrecord_list_title'),
            ],
            on_click=self._on_subrecord_click,
            displaycolumns=(1,)
        )
        self._subrecord_list.tag_configure('has-error', foreground='red')
        self._subrecord_list.grid(row=0, column=0, sticky='EWNS')
        self._subrecord_list.table.column('#0', width=80, stretch=False)
        self._current_record_info = None

    def after_open_batch(self, batch_type: str):
        self._record_list.clear_items()
        self._record_list.extend_items(self._load_record_list())

    def on_record_change(self, record_uuid: str, record: ocproc2.DataRecord):
        self._subrecord_list.clear_items()
        self._build_subrecord_list(record)

    def _on_subrecord_click(self, item_info, is_change: bool, event):
        item = self._current_record_info[1].find_child(item_info['values'][0].split('/'))
        if isinstance(item, ocproc2.DataRecord):
            self.app.show_record(item)
        elif isinstance(item, ocproc2.RecordSet):
            self.app.show_recordset(item)

    def _on_record_click(self, item_info, is_change: bool, event):
        if is_change or self._current_record_info is None:
            with self.local_db.cursor() as cur:
                cur.execute("SELECT record_uuid, record_content FROM records WHERE record_uuid = ?", [item_info['text']])
                row = cur.fetchone()
                record = ocproc2.DataRecord()
                record.from_mapping(json.loads(row[1]))
                self._current_record_info = (row[0], record)
                self.app.on_record_change(row[0], record)
        else:
            self.app.show_record(self._current_record_info[1])

    def _load_record_list(self) -> t.Iterable[tuple[str, tuple, tuple]]:
        with self.local_db.cursor() as cur:
            cur.execute("SELECT record_uuid, display, has_errors FROM records ORDER BY display ASC")
            for idx, row in enumerate(cur.fetchall()):
                yield row[0], (row[0], row[1]), [('has-error' if row[2] == 1 else 'no-error')]

    def _build_subrecord_list(self, current_record: ocproc2.DataRecord, parent_text=''):
        for srt in current_record.subrecords:
            srt_text = f'{parent_text}/subrecords/{srt}' if parent_text else f'subrecords/{srt}'
            self._subrecord_list.table.insert(parent_text, 'end', text='', iid=srt_text, values=(srt_text, srt,))
            for rs_idx in current_record.subrecords[srt]:
                rs_text = f'{srt_text}/{rs_idx}'
                self._subrecord_list.table.insert(srt_text, 'end', text='', iid=rs_text, values=(rs_text, rs_idx,))
                for idx, record in enumerate(current_record.subrecords[srt][rs_idx].records):
                    record_text = f"{srt_text}/{rs_idx}/{idx}"
                    self._subrecord_list.table.insert(rs_text, 'end', text='', iid=record_text, values=(record_text, f'Record {idx}',))
                    self._build_subrecord_list(record, record_text)

    def _build_record_display(self, record: ocproc2.DataRecord):
        return '??'







