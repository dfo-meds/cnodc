import pathlib
import typing as t

from cnodc.ocproc2.codecs.netcdf import NetCDFCommonMapper
from cnodc.ocproc2 import SingleElement, ParentRecord
import cnodc.programs.glider.ego_convert as ego_convert


class GliderEGOMapper(NetCDFCommonMapper):

    DEFAULT_MAPPING_FILE = pathlib.Path(__file__).parent / 'ego_conversion.yaml'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, log_name='cnodc.programs.glider.ego_decode')
        self._sensor_map = None
        self._sensor_info = None
        self._record_metadata = None

    def _on_data_load(self):
        if self._sensor_map is None:
            if self._dataset.has_variable('PARAMETER'):
                self._sensor_info, self._sensor_map = ego_convert.ego_new_sensor_info(self._dataset)
            else:
                self._sensor_info, self._sensor_map = ego_convert.ego_old_sensor_info(self._dataset, self._data['sensor_map'])
        if self._record_metadata is None:
            self._record_metadata = {}
            if self._dataset.has_variable('PLATFORM_TYPE'):
                info = self._data['glider_model_map'][self._dataset.variable('PLATFORM_TYPE').as_string().lower()]
                if 'ocproc2_model' in info and info['ocproc2_model']:
                    self._record_metadata['metadata/PlatformModel'] = info['ocproc2_model']
                elif 'model' in info and info['model']:
                    self._record_metadata['metadata/PlatformModel'] = info['model']
                if 'maker' in info and info['maker']:
                    self._record_metadata['metadata/PlatformMake'] = info['maker']
            if self._dataset.has_attribute('platform_name') and self._dataset.has_variable('DEPLOYMENT_START_TIME'):
                self._record_metadata['metadata/CruiseID'] = f"{self._dataset.attribute('platform_name')}_{self._dataset.variable('DEPLOYMENT_START_TIME').as_string()[0:8]}"

    def _after_record(self, record: ParentRecord, index: int):
        for key in self._record_metadata:
            record.set_element(key, self._record_metadata[key])

    def _after_variable_element(self, element: SingleElement, minfo: dict, data: dict[str, t.Any]):
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
            element.metadata.set_element(key, extra_metadata[key])

    def _data_mode_to_level(self, value, var_name):
        value = value.upper()
        if value == "" or value is None:
            return None
        elif value == 'R':
            return 'REAL_TIME'
        else:
            self._log.warning(f'Unknown data mode [{value}]')
            return value

    def _convert_update_level(self, value, var_name):
        value = value.lower()
        if value == "" or value is None:
            return None
        elif value in ('daily', 'hourly'):
            return value
        elif value == 'void':
            return 'notPlanned'
        else:
            self._log.warning(f"Unknown update level [{value}]")
            return value

    def _convert_platform_family(self, value, var_name):
        value = value.upper()
        if value == "" or value is None:
            return None
        elif value == 'OPEN_OCEAN_GLIDER':
            return 'glider_ocean'
        elif value == 'DEEP_GLIDER':
            return 'glider_deep'
        elif value == 'COASTAL_GLIDER':
            return 'glider_coastal'
        else:
            self._log.warning(f"Unknown platform family type: [{value}]")
            return value

    def _convert_battery_type(self, value, var_name):
        value = value.lower()
        if value == "" or value is None:
            return None
        elif value == 'lithiumion':
            return 'lithium'
        else:
            self._log.warning(f'Unknown battery type: [{value}]')
            return value
