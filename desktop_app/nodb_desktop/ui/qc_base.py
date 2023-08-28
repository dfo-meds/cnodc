import functools
import tkinter.ttk as ttk
import tkinter as tk
from .child import ChildFrame
from .scrollable import ScrollableFrame, ScrollableTreeview
from ..business import DesktopAppController
import typing as t

from cnodc.ocproc2 import DataRecord, RecordSet, DataValue


class QCBatchController:

    def __init__(self, app_controller: DesktopAppController):
        self.app: DesktopAppController = app_controller
        self._top_frame: ChildFrame = None
        self._map_frame = None
        self._list_frame = None
        self._subrecord_frame = None
        self._property_metadata_frame = None
        self._property_frame = None
        self._button_frame = None
        self._batch = None
        self._records = None
        self._current_record_idx = None
        self._current_hierarchy_idx = None
        self._current_value_idx = None

    def set_top_frame(self, frame):
        self._top_frame = frame

    def set_button_frame(self, frame):
        self._button_frame = frame

    def set_map_frame(self, frame):
        self._map_frame = frame

    def set_list_frame(self, frame):
        self._list_frame = frame

    def set_subrecord_frame(self, frame):
        self._subrecord_frame = frame

    def set_property_frame(self, frame):
        self._property_frame = frame

    def set_property_metadata_frame(self, frame):
        self._property_metadata_frame = frame

    def load_next_batch(self):
        self._button_frame.set_batch_in_progress(True)
        self._load_records_for_batch()
        self._map_frame.show_batch(self._records)
        self._list_frame.show_batch(self._records)
        self._property_frame.clear()
        self._subrecord_frame.clear()
        self._current_record_idx = None
        self._current_hierarchy_idx = None

    def save_batch(self):
        self._button_frame.set_batch_in_progress(True)
        self._save_records()

    def submit_batch(self):
        self._button_frame.set_batch_in_progress(True)

    def cancel_batch(self):
        self._button_frame.set_batch_in_progress(False)
        self._list_frame.clear()
        self._map_frame.clear()
        self._property_frame.clear()
        self._subrecord_frame.clear()
        self._property_metadata_frame.clear()
        self._current_record_idx = None
        self._current_hierarchy_idx = None
        self._current_value_idx = None

    def show_record(self, idx: int, source: str):
        if idx != self._current_record_idx:
            record = self._records[idx]
            if source != 'list':
                self._list_frame.select_record(idx, record)
            if source != 'map':
                self._map_frame.select_record(idx, record)
            self._subrecord_frame.show_record(idx, record)
            self._current_record_idx = idx
            self._current_hierarchy_idx = None
            self._current_value_idx = None
            self._property_metadata_frame.clear()

    def show_specific_record(self, hierarchy_key: str, record: t.Union[RecordSet, DataRecord]):
        if hierarchy_key != self._current_hierarchy_idx:
            self._current_hierarchy_idx = hierarchy_key
            self._current_value_idx = None
            self._property_metadata_frame.clear()
            self._property_frame.show_specific_record(record)

    def show_data_value(self, value_key, dv: DataValue):
        if self._current_value_idx != value_key:
            self._property_metadata_frame.set_data_value(dv)

    def clear_data_value(self):
        self._current_value_idx = None
        self._property_metadata_frame.clear()

    def _save_records(self):
        pass

    def _submit_records(self):
        pass

    def _load_records_for_batch(self):
        # TODO: real load
        self._records = []
        for i in range(0, 59):
            dr = DataRecord()
            dr.metadata['_NODB_UUID'] = f'12345-67890-12345-67890-1234567890{str(i).zfill(2)}'
            dr.metadata['_NODB_STATION_UUID'] = '12345-67890-12345-67890-123456789012'
            dr.coordinates['LAT'] = -45.12345
            dr.coordinates['LAT'].metadata['UN'] = 'degrees'
            dr.coordinates['LAT'].metadata['PR'] = '0.00001'
            dr.coordinates['LON'] = 45.12345
            dr.coordinates['LON'].metadata['UN'] = 'degrees'
            dr.coordinates['LON'].metadata['PR'] = '0.00001'
            dr.coordinates['TIME'] = f'2023-01-01T00:{str(i).zfill(2)}:00Z'

            for k in range(25, 125, 25):
                sr = DataRecord()
                sr.coordinates['DEPTH'] = k
                sr.coordinates['DEPTH'].metadata['UN'] = 'm'
                sr.coordinates['DEPTH'].metadata['PR'] = '1'

                sr.variables['TEMP'] = 2.3
                sr.variables['TEMP'].metadata['UN'] = 'C'
                sr.variables['TEMP'].metadata['PR'] = 0.1
                sr.variables['SALN'] = 23.123
                sr.variables['SALN'].metadata['UN'] = '0.001'
                sr.variables['SALN'].metadata['PR'] = 0.001
                dr.subrecords.append('PROFILE_0', sr)

            for k in range(100, 1000, 250):
                sr = DataRecord()
                sr.coordinates['DEPTH'] = k
                sr.coordinates['DEPTH'].metadata['UN'] = 'm'
                sr.coordinates['DEPTH'].metadata['PR'] = '1'
                sr.variables['TEMP'] = 3.4
                sr.variables['TEMP'].metadata['UN'] = 'C'
                sr.variables['TEMP'].metadata['PR'] = 0.1
                sr.variables['SALN'] = 25
                sr.variables['SALN'].metadata['UN'] = '0.001'
                sr.variables['SALN'].metadata['PR'] = 0.001
                dr.subrecords.append('PROFILE_1', sr)

            self._records.append(dr)


class QualityControlFrame(ChildFrame):

    def __init__(self, *args, controller: QCBatchController, **kwargs):
        super().__init__(*args, height=600, width=1000, **kwargs)
        self.controller = controller
        self.controller.set_top_frame(self)
        self.contents.rowconfigure(0, weight=0)
        self.contents.rowconfigure(1, weight=1)
        self.contents.rowconfigure(2, weight=0)
        self.contents.columnconfigure(0, weight=1)

        # Button frame
        self.button_frame = QCButtonFrame(self.contents, controller=self.controller)
        self.button_frame.grid(row=0, column=0, sticky="nsew")

        # Top frame
        self.top_frame = tk.Frame(self.contents)
        self.top_frame.columnconfigure(0, weight=0)
        self.top_frame.columnconfigure(1, weight=1)
        self.top_frame.columnconfigure(2, weight=0)
        self.top_frame.rowconfigure(0, weight=1)

        self.record_frame = QCSubrecordFrame(self.top_frame, controller=self.controller)
        self.record_frame.grid(row=0, column=0, sticky="nsew")

        self.inner_contents = tk.Frame(self.top_frame)
        self.inner_contents.grid(row=0, column=1, sticky="nsew")

        self.combined_property = tk.Frame(self.top_frame)
        self.combined_property.grid(row=0, column=2, sticky="nsew")
        self.combined_property.rowconfigure(0, weight=2)
        self.combined_property.rowconfigure(1, weight=1)
        self.combined_property.columnconfigure(0, weight=1)

        self.property_frame = QCPropertyFrame(self.combined_property, controller=self.controller)
        self.property_frame.grid(row=0, column=0, sticky="nsew")

        self.property_metadata_frame = QCPropertyMetadataFrame(self.combined_property, controller=self.controller)
        self.property_metadata_frame.grid(row=1, column=0, sticky="nsew")

        self.top_frame.grid(row=1, column=0, sticky="nsew")

        # Bottom frame
        self.bottom_frame = tk.Frame(self.contents)
        self.bottom_frame.grid_columnconfigure(0, weight=0)
        self.bottom_frame.grid_columnconfigure(1, weight=1)
        self.bottom_frame.grid_rowconfigure(0, weight=1)

        self.map_frame = QCMapFrame(self.bottom_frame, controller=self.controller)
        self.map_frame.grid(row=0, column=0, sticky="nsew")

        self.list_frame = QCListFrame(self.bottom_frame, controller=self.controller)
        self.list_frame.grid(row=0, column=1, sticky="nsew")

        self.bottom_frame.grid(row=2, column=0, sticky="ensw")


class QCButtonFrame(tk.Frame):

    def __init__(self, *args, controller: QCBatchController, **kwargs):
        super().__init__(*args, **kwargs)
        self.controller = controller
        self.controller.set_button_frame(self)
        self.load_button = ttk.Button(self, text=self.controller.app.get_text("load_batch"), command=self.controller.load_next_batch)
        self.load_button.grid(row=0, column=0)
        self.save_button = ttk.Button(self, text=self.controller.app.get_text("save_batch"), command=self.controller.save_batch)
        self.save_button.grid(row=0, column=1)
        self.save_button.config(state="disabled")
        self.submit_button = ttk.Button(self, text=self.controller.app.get_text("submit_batch"), command=self.controller.submit_batch)
        self.submit_button.grid(row=0, column=2)
        self.submit_button.config(state="disabled")
        self.cancel_button = ttk.Button(self, text=self.controller.app.get_text("cancel_batch"), command=self.controller.cancel_batch)
        self.cancel_button.grid(row=0, column=3)
        self.cancel_button.config(state="disabled")

    def set_batch_in_progress(self, in_progress: bool):
        if in_progress:
            self.load_button.config(state="disabled")
            self.save_button.config(state="enabled")
            self.submit_button.config(state="enabled")
            self.cancel_button.config(state="enabled")
        else:
            self.load_button.config(state="enabled")
            self.save_button.config(state="disabled")
            self.submit_button.config(state="disabled")
            self.cancel_button.config(state="disabled")


class QCMapFrame(tk.Frame):

    def __init__(self, *args, controller: QCBatchController, **kwargs):
        super().__init__(*args, height=200, width=200, **kwargs)
        self.controller = controller
        self.controller.set_map_frame(self)

    def show_batch(self, batch: list[DataRecord]):
        # TODO
        pass

    def clear(self):
        # TODO
        pass

    def select_record(self, idx, record):
        # TODO
        pass


class QCListFrame(ScrollableTreeview):

    def __init__(self, *args, controller: QCBatchController, **kwargs):
        super().__init__(*args, height=200, width=800, **kwargs)
        self.controller = controller
        self.controller.set_list_frame(self)
        self.table.configure(
            columns=('uuid', 'lat', 'lon', 'time', 'station_uuid'),
            show='headings',
            selectmode="browse"
        )
        self.table.heading('uuid', text=self.controller.app.get_text('label_uuid'))
        self.table.column('uuid', minwidth=225, width=225)
        self.table.heading('lat', text=self.controller.app.get_text('label_lat'))
        self.table.column('lat', minwidth=70, width=70, stretch=tk.NO)
        self.table.heading('lon', text=self.controller.app.get_text('label_lon'))
        self.table.column('lon', minwidth=70, width=70, stretch=tk.NO)
        self.table.heading('time', text=self.controller.app.get_text('label_time'))
        self.table.column('time', minwidth=150, width=150, stretch=tk.NO)
        self.table.heading('station_uuid', text=self.controller.app.get_text('label_station_uuid'))
        self._lookup = {}

    def clear(self):
        self.table.delete(*self.table.get_children())
        self._lookup = {}

    def show_batch(self, batch: list[DataRecord]):
        self.clear()
        for idx, record in enumerate(batch):
            key = self.table.insert('', tk.END, values=(
                record.metadata.get('_NODB_UUID', ''),
                record.coordinates.get('LAT', ''),
                record.coordinates.get('LON', ''),
                record.coordinates.get('TIME', ''),
                record.metadata.get('_NODB_STATION_UUID', '')
            ))
            self._lookup[key] = idx

    def select_record(self, idx, record):
        # TODO
        pass

    def on_select(self, e):
        if self.table.selection():
            self.controller.show_record(self._lookup[self.table.selection()[0]], 'list')


class QCSubrecordFrame(ScrollableTreeview):

    def __init__(self, *args, controller: QCBatchController, **kwargs):
        super().__init__(*args,  width=200, **kwargs)
        self.controller = controller
        self.controller.set_subrecord_frame(self)
        self.table.configure(
            columns=('name'),
            selectmode="browse"
        )
        self.table.column("#0", width=50)
        self.table.heading('name', text=self.controller.app.get_text('label_item_name'))
        self._current_record = None
        self._lookup = {}

    def clear(self):
        self.table.delete(*self.table.get_children())

    def show_record(self, idx: int, record: DataRecord):
        self.clear()
        self._current_record = record
        top_id = self.table.insert("", iid="top", index="end", values=(self.controller.app.get_text('top_item'),))
        self._lookup[top_id] = [0]
        self.add_subrecords("top", record, [0])

    def add_subrecords(self, parent, record: DataRecord, parent_hierarchy):
        if record.subrecords:
            for srt in record.subrecords:
                srt_key = f"{parent}_{srt}"

                row_id = self.table.insert(parent, iid=srt_key, index="end", values=(srt,))
                self._lookup[row_id] = [*parent_hierarchy, srt]
                for idx, sr in enumerate(record.subrecords[srt]):
                    sr_key = f"{srt_key}_{idx}"
                    row_id = self.table.insert(srt_key, iid=sr_key, index="end", values=(self._record_label(sr),))
                    self._lookup[row_id] = [*parent_hierarchy, srt, idx]
                    self.add_subrecords(sr_key, sr, self._lookup[row_id])

    def _record_label(self, record: DataRecord):
        labels = []
        for cn in record.coordinates:
            labels.append(f"{cn}={record.coordinates[cn].value()}")
        return '; '.join(labels)

    def on_select(self, e):
        if self.table.selection():
            hierarchy = self._lookup[self.table.selection()[0]][1:]
            key = "__".join(str(x) for x in hierarchy)
            record = self._current_record
            while hierarchy:
                x = hierarchy[0]
                hierarchy = hierarchy[1:]
                if isinstance(record, DataRecord):
                    record = record.subrecords[x]
                else:
                    record = record.records[x]
            self.controller.show_specific_record(key, record)


class QCPropertyFrame(ScrollableTreeview):

    def __init__(self, *args, controller: QCBatchController, **kwargs):
        super().__init__(*args, width=200, **kwargs)
        self.controller = controller
        self.controller.set_property_frame(self)
        self.table.configure(
            columns=('name', 'value'),
            selectmode="browse",
            show='headings'
        )
        self.table.column("name", width=175)
        self.table.heading('name', text=self.controller.app.get_text('label_property_name'))
        self.table.column("value", width=175)
        self.table.heading('value', text=self.controller.app.get_text('label_property_value'))
        self._current_record = None
        self._lookup = {}

    def clear(self):
        self.table.delete(*self.table.get_children())

    def show_specific_record(self, record: t.Union[RecordSet, DataRecord]):
        self.clear()
        self._current_record = record
        if isinstance(record, DataRecord):
            if record.coordinates:
                self.table.insert('', index='end', values=(self.controller.app.get_text('label_coordinates')), tags=["h"])
                for k in record.coordinates:
                    key = self.table.insert('', index='end', values=(
                        k,
                        record.coordinates[k].value()
                    ))
                    self._lookup[key] = ['coordinates', k]

            if record.variables:
                self.table.insert('', index='end', values=(self.controller.app.get_text('label_variables'),), tags=["h"])
                for k in record.variables:
                    key = self.table.insert('', index='end', values=(
                        k,
                        record.variables[k].value()
                    ))
                    self._lookup[key] = ['variables', k]

        if record.metadata:
            found = False
            for k in record.metadata:
                if not k.startswith('_'):
                    if not found:
                        self.table.insert('', index='end', values=(self.controller.app.get_text('label_metadata'),), tags=["h"])
                        found = True
                    key = self.table.insert('', index='end', values=(
                        k,
                        record.metadata[k].value()
                    ))
                    self._lookup[key] = ['metadata', k]
        self.table.tag_configure('h', font=('Helvetica', 10, 'bold'))

    def on_select(self, e):
        sel = self.table.selection()
        if sel and sel[0] in self._lookup:
            map_name, map_key = self._lookup[sel[0]]
            key = f"{map_name}__{map_key}"
            dv = getattr(self._current_record, map_name)[map_key]
            self.controller.show_data_value(key, dv)
        else:
            self.controller.clear_data_value()

    def on_right_click(self, iid, e):
        if iid in self._lookup:
            m = tk.Menu(tearoff=0)
            m.add_command(label='Edit', command=functools.partial(self.edit_item, iid=iid))
            try:
                m.tk_popup(e.x_root, e.y_root)
            finally:
                m.grab_release()

    def edit_item(self, iid):
        print(iid)


class QCPropertyMetadataFrame(ScrollableTreeview):

    def __init__(self, *args, controller: QCBatchController, **kwargs):
        super().__init__(*args, width=200, height=100, **kwargs)
        self.controller = controller
        self.controller.set_property_metadata_frame(self)
        self.table.configure(
            columns=('name', 'value'),
            selectmode="browse",
            show='headings'
        )
        self.table.column("name", width=175)
        self.table.heading('name', text=self.controller.app.get_text('label_property_name'))
        self.table.column("value", width=175)
        self.table.heading('value', text=self.controller.app.get_text('label_property_value'))
        self._current_record = None
        self._lookup = {}

    def clear(self):
        self.table.delete(*self.table.get_children())

    def set_data_value(self, dv: DataValue):
        self.clear()
        self._current_record = dv
        for k in dv.metadata:
            if not k.startswith("_"):
                key = self.table.insert('', index='end', values=(k, dv.metadata[k].value()))
                self._lookup[key] = k

    def on_right_click(self, iid, e):
        if iid in self._lookup:
            m = tk.Menu(tearoff=0)
            m.add_command(label='Edit', command=functools.partial(self.edit_item, iid=iid))
            try:
                m.tk_popup(e.x_root, e.y_root)
            finally:
                m.grab_release()

    def edit_item(self, iid):
        print(iid)
