from cnodc.decode.common import BufferedBinaryReader, BaseCodec
from cnodc.ocproc2 import DataRecord
import typing as t
import pathlib
import datetime


CASTAWAY_METADATA_MAP = {
    'Device': ('DEVICE_ID', str),
    'File name': (None, None),
    'Cast time (local)': (None, None),
    'Sample type': (''),
    'Cast data': (),
    'Location source': (),
    'Default latitude': (),
    'Default altitude': (),
    'Start latitude': (),
    'Start longitude': (),
    'Start altitude': (),
    'Start GPS horizontal error(Meter)': (),
    'Start GPS vertical error(Meter)': (),
    'Start GPS number of satellites': (),
    'End latitude': (),
    'End longitude': (),
    'End altitude': (),
    'End GPS horizontal error(Meter)': (),
    'End GPS vertical error(Meter)': (),
    'End GPS number of satellites': (),
    'Cast duration (Seconds)': (),
    'Samples per second': (),
    'Electronics calibration date': (),
    'Conductivity calibration date': (),
    'Temperature calibration date': (),
    'Pressure calibration date': (),
}


class CastawayCtdCodec(BaseCodec):

    def __init__(self):
        super().__init__("Castaway CTD CSV files", ".csv")

    def decode_messages(self, data: t.Iterable[bytes], **kwargs) -> t.Iterable[DataRecord]:
        reader = BufferedBinaryReader(data)
        dr = DataRecord()
        csv_data = []
        for line in reader.consume_by_lines():
            line = line._decode('utf-8').strip()
            if line[0] == '%':
                self._decode_metadata_line(line, dr)
            else:
                csv_data.append(line)
        self._decode_data_lines(csv_data, dr)
        yield dr

    def _decode_metadata_line(self, line: str, record: DataRecord):
        if ',' not in line:
            return
        name, value = line.split(',', maxsplit=1)
        name = name[1:].strip()
        value = value.strip()
        if name in CASTAWAY_METADATA_MAP:
            metadata_name, coerce_fn = CASTAWAY_METADATA_MAP[name]
            if metadata_name is not None:
                record.metadata[metadata_name] = coerce_fn(value) if coerce_fn is not None else value
        elif name == 'Cast time (UTC)':
            dt = datetime.datetime.strptime(value, '%m/%d/%Y %H:%M')
            record.coordinates['TIME'] = dt.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=0), 'UTC'))

    def _decode_data_lines(self, lines: list[str], record: DataRecord):
        pass

    def check_compatibility(self, file_path: pathlib.Path) -> bool:
        if not file_path.name.lower().endswith(".csv"):
            return False
        with open(file_path, "r") as h:
            return h.read(512).startswith("% Device,")
