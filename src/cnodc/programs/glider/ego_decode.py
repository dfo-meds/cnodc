import pathlib
import typing as t

from cnodc.ocproc2.codecs.netcdf import NetCDFCommonMapper
from cnodc.ocproc2 import SingleElement, ParentRecord
import cnodc.programs.glider.ego_convert as ego_convert
from cnodc.util import CNODCError


class GliderEGOMapper(NetCDFCommonMapper):

    DEFAULT_MAPPING_FILE = pathlib.Path(__file__).parent / 'ego_conversion.yaml'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, log_name='cnodc.programs.glider.ego_decode')
        self._sensor_map = None
        self._sensor_info = None
        self._record_metadata = None

    def _on_data_load(self):
        if self._sensor_map is None:
            self._sensor_info, self._sensor_map = ego_convert.ego_sensor_info(self._dataset, self._data['data_maps']['sensors'])
        if self._record_metadata is None:
            self._record_metadata = {}
            if self.has_variable('PLATFORM_TYPE'):
                info = self._data['data_maps']['glider_models'][self.var_to_string('PLATFORM_TYPE').lower()]
                if 'ocproc2_model' in info and info['ocproc2_model']:
                    self._record_metadata['metadata/PlatformModel'] = info['ocproc2_model']
                if 'maker' in info and info['maker']:
                    self._record_metadata['metadata/PlatformMake'] = info['maker']
            if self.has_attribute('platform_name') and self.has_variable('DEPLOYMENT_START_DATE'):
                self._record_metadata['metadata/CruiseID'] = f"{self._dataset.getncattr('platform_name')}_{self.var_to_string('DEPLOYMENT_START_DATE')[0:8]}"

    def _after_record(self, record: ParentRecord, index: int):
        for key in self._record_metadata:
            record.set(key, self._record_metadata[key])

    def _after_element(self, element: SingleElement, minfo: dict, data: t.Optional[dict[str, t.Any]] = None):
        extra_metadata = {}
        if 'source' in minfo and minfo['source'] and minfo['source'] in self._sensor_map:
            info = self._sensor_info[self._sensor_map[minfo['source']]]
            extra_metadata.update({
                'SensorType': info['type'].lower(),
                'SensorMake': info['make'],
                'SensorModel': info['model'],
                'SensorSerial': info['serial'],
                'SensorLocation': info['location'] if 'location' in info else None
            })
        for key in extra_metadata.keys():
            element.metadata.set(key, extra_metadata[key])

    @staticmethod
    def _isoformat_ego_date(value, minfo):
        if not value:
            return None
        match len(value):
            case 8:
                return f'{value[0:4]}-{value[4:6]}-{value[6:8]}'
            case 10:
                return f'{value[0:4]}-{value[4:6]}-{value[6:8]}T{value[8:10]}:00:00'
            case 12:
                return f'{value[0:4]}-{value[4:6]}-{value[6:8]}T{value[8:10]}:{value[10:12]}:00'
            case 14:
                return f'{value[0:4]}-{value[4:6]}-{value[6:8]}T{value[8:10]}:{value[10:12]}:{value[12:14]}'
        raise CNODCError(f'Unknown ISO date format without dashes: [{value}]', 'EGO-DECODE', 1000)

