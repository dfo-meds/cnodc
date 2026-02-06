import datetime
import math
import typing as t
import pathlib

import yaml

from cnodc.dmd.metadata import GCContentType, GCContentFormat
from cnodc.util import CNODCError, dynamic_object
from cnodc.netcdf.wrapper import Dataset
import cnodc.ocean_math.seawater as seawater
import cnodc.dmd.metadata as metadata




class OpenGliderConverter:

    @staticmethod
    def build(map_file: t.Optional[pathlib.Path] = None, halt_flag=None):
        if map_file is None:
            map_file = pathlib.Path(__file__).absolute().parent / 'og_convert_realtime.yaml'
        with open(map_file, encoding="utf-8") as h:
            return OpenGliderConverter(yaml.safe_load(h), halt_flag)

    def __init__(self, mapping_data, halt_flag=None):
        self._mapping_data = mapping_data
        self._base_time = datetime.datetime.fromisoformat('1970-01-01T00:00:00')
        self._halt = halt_flag

    def breakpoint(self):
        if self._halt is not None:
            self._halt.breakpoint()

    def build_metadata(self, open_file, file_name: str) -> metadata.DatasetMetadata:
        with Dataset(open_file, "r") as nc:
            dmd = metadata.DatasetMetadata()
            dmd.set_meds_defaults()
            dmd.set_from_netcdf_file(nc)
            dmd.set_processing_info("real-time")
            mission_id = nc.attribute('id')
            dist = metadata.DistributionChannel()
            dist.set_guid('direct_link')
            dist.set_display_name({"en": "Direct Link", "fr": "Lien direct"})
            main_link = metadata.Resource(f"https://cnodc-cndoc.azure.cloud-nuage.dfo-mpo.gc.ca/public/data-donnees/glider-planeur/{file_name}")
            main_link.set_display_name({"en": "NetCDF File in OpenGlider format", "fr:": "Ficher NetCDF en format OpenGlider"})
            main_link.set_name({"en": "NetCDF File in OpenGlider format", "fr:": "Ficher NetCDF en format OpenGlider"})
            main_link.set_gc_language(metadata.GCLanguage.Bilingual)
            main_link.set_link_purpose(metadata.ResourcePurpose.FileAccess)
            main_link.set_gc_content_type(GCContentType.Dataset)
            main_link.set_gc_content_format(GCContentFormat.DataNetCDF)
            main_link.set_resource_type(metadata.ResourceType.File)
            dist.set_primary_link(main_link)
            dmd.add_distribution_channel(dist)
            if 'users' in self._mapping_data and self._mapping_data['users']:
                for user in self._mapping_data['users']:
                    dmd.add_user(user)
            dmd.set_erddap_info(
                server=metadata.Common.ERDDAP_Primary,
                dataset_id=mission_id,
                dataset_type=metadata.ERDDAPDatasetType.DSGTable,
                file_path=f"/cloud_data/gliders/{mission_id.lower()}/",
                file_pattern="*.nc.gz"
            )
            return dmd

    def convert(self, ego_file, og_file, file_name: t.Optional[str] = None):
        file_name = file_name or pathlib.Path(ego_file).name
        platform, start_time, data_mode = file_name[:-3].rsplit('_', maxsplit=2)
        with Dataset(ego_file, "r") as original_nc:
            with Dataset(og_file, "w", format="NETCDF4") as open_nc:
                self._create_dimensions(open_nc)
                self._map_static_metadata(open_nc)
                self._copy_metadata(open_nc, original_nc)
                self._set_metadata_from_file_name(open_nc, platform, start_time, data_mode)
                self.breakpoint()
                self._set_geospatial_bounds_metadata(open_nc, original_nc)
                sensor_map = self._set_sensor_metadata(open_nc, original_nc)
                self._build_variables(open_nc, original_nc)
                self._build_parameters(open_nc, original_nc, sensor_map)
                self._build_depths(open_nc, original_nc)
                self._build_times(open_nc, original_nc)
                self._build_contributors(open_nc, original_nc)
                self._build_deployment_info(open_nc, original_nc, platform, start_time)
                self._build_glider_info(open_nc, original_nc, platform)
                self._build_phase_info(original_nc, open_nc)
                open_nc.set_attribute('date_created', datetime.datetime.now().strftime('%Y%m%dT%H%M%SZ'))
                mission_id = open_nc.attribute('id')
        return file_name, mission_id

    def _create_dimensions(self, open_nc: Dataset):
        open_nc.create_dimension("N_MEASUREMENTS", None)

    def _map_static_metadata(self, open_nc: Dataset):
        for key in self._mapping_data['static_metadata']:
            open_nc.set_attribute(key, self._mapping_data['static_metadata'][key])

    def _copy_metadata(self, open_nc: Dataset, original_nc: Dataset):
        for key in self._mapping_data['copy_metadata']:
            if original_nc.has_attribute(key):
                open_nc.set_attribute(key, original_nc.attribute(self._mapping_data['copy_metadata'][key]))

    def _set_metadata_from_file_name(self, open_nc: Dataset, platform, start_time, data_mode):
        if data_mode == 'R':
            open_nc.set_attribute('rtqc_method', 'Real-time QC performed with Coriolis matlab toolbox')
            open_nc.set_attribute('rtqc_method_fr', 'Contrôle qualité en temps réel réalisé avec la boîte à outils Coriolis Matlab')
        else:
            open_nc.set_attribute('rtqc_method', 'No QC applied')
            open_nc.set_attribute('rtqc_method_fr', 'Aucun contrôle qualité appliqué')
        open_nc.set_attribute('title', f'Glider {platform} - {start_time} ({'Real Time' if data_mode == 'R' else 'Unprocessed'})')
        open_nc.set_attribute('title_fr', f'PLaneur {platform} - {start_time} ({'temps réel' if data_mode == 'R' else 'non traité'})')
        open_nc.set_attribute('id', f"{platform}_{start_time}_{data_mode}")

    def _set_geospatial_bounds_metadata(self, open_nc: Dataset, original_nc: Dataset):
        min_lat, max_lat = original_nc.variable('LATITUDE').range()
        self.breakpoint()
        min_lon, max_lon = original_nc.variable('LONGITUDE').range()
        self.breakpoint()
        open_nc.set_attribute('geospatial_lat_min', min_lat)
        open_nc.set_attribute('geospatial_lat_max', max_lat)
        open_nc.set_attribute('geospatial_lon_min', min_lon)
        open_nc.set_attribute('geospatial_lon_max', max_lon)

    def _set_sensor_metadata(self, open_nc: Dataset, original_nc: Dataset):
        if not original_nc.has_variable('PARAMETER'):
            return self._build_old_sensor_metadata(open_nc, original_nc)
        else:
            return self._build_new_sensor_metadata(open_nc, original_nc)

    def _build_old_sensor_metadata(self, open_nc: Dataset, original_nc: Dataset):
        sensors = {}
        sensors_seen = set()
        param_map = {}
        for var in original_nc.variables():
            if not var.has_attribute('sensor_name'):
                continue
            sensor_full_name = var.attribute('sensor_name').strip().lower()
            while '  ' in sensor_full_name:
                sensor_full_name = sensor_full_name.replace('  ', ' ')
            if sensor_full_name not in self._mapping_data['sensor_map']:
                raise CNODCError(f"Unknown sensor [{sensor_full_name}]")
                # TODO: should be an error
                continue
            info = self._mapping_data['sensor_map'][sensor_full_name]
            info['serial'] = var.attribute('sensor_serial_number')
            key = f"SENSOR_{info['type']}_{info['serial']}"
            if key in sensors_seen:
                continue
            sensors_seen.add(key)
            sensors[key] = info
            param_map[var.name] = key
        self._create_openglider_sensor_vars(open_nc, sensors)
        return param_map

    def _create_openglider_sensor_vars(self, open_nc: Dataset, sensors: dict[str, dict[str, str]]):
        for key in sensors:
            info = sensors[key]
            open_nc.create_variable(
                f'SENSOR_{info['type']}_{info['serial']}',
                'f4',
                None,
                {
                    'long_name': f"{info['make']} {info['model']}",
                    'sensor_model': info['model'],
                    'sensor_maker': info['make'],
                    'sensor_serial_number': info['serial'],
                },
            )
            self.breakpoint()

    def _build_new_sensor_metadata(self, open_nc: Dataset, original_nc: Dataset):
        sensor_names = original_nc.variable('SENSOR').all_as_strings()
        self.breakpoint()
        sensor_makers = original_nc.variable('SENSOR_MAKER').all_as_strings()
        self.breakpoint()
        sensor_models = original_nc.variable('SENSOR_MODEL').all_as_strings()
        self.breakpoint()
        sensor_serials = original_nc.variable('SENSOR_SERIAL_NO').all_as_strings()
        self.breakpoint()
        param_names = original_nc.variable('PARAMETER').all_as_strings()
        self.breakpoint()
        param_sensors = original_nc.variable('PARAMETER_SENSOR').all_as_strings()
        self.breakpoint()
        sensor_info = {}
        sensors_seen = set()
        param_map = {}
        for x in range(0, len(sensor_names)):
            if (sensor_models[x], sensor_serials[x]) in sensors_seen:
                continue
            sensors_seen.add((sensor_models[x], sensor_serials[x]))
            if sensor_names[x].startswith('CTD_'):
                sensor_type = 'CTD'
            elif sensor_names[x].startswith('FLUOROMETER_') or sensor_names[x].startswith('BACKSCATTER'):
                sensor_type = 'FLUOROMETER'
            elif sensor_names[x] == 'OPTODE_DOXY':
                sensor_type = 'DOXY'
            else:
                raise CNODCError(f"Unknown glider instrument type: {sensor_names[x]}", 'GLIDER_CONVERT', 1003)
            key = f"SENSOR_{sensor_type}_{sensor_serials[x]}"
            for idx,val in enumerate(param_sensors):
                if val == sensor_names[x]:
                    param_map[param_names[idx]] = key
            if key not in sensor_info:
                sensor_info[key] = {
                    'type': sensor_type,
                    'make': sensor_makers[x],
                    'model': sensor_models[x],
                    'serial': sensor_serials[x],
                }
            self.breakpoint()
        self._create_openglider_sensor_vars(open_nc, sensor_info)
        return param_map

    def _build_variables(self, open_nc: Dataset, original_nc: Dataset):
        for var_name in self._mapping_data['variables']:
            var_config = self._mapping_data['variables'][var_name]
            if 'copy_data_from' in var_config and var_config['copy_data_from'] and not original_nc.has_variable(var_config['copy_data_from']):
                continue
            self._create_variable(open_nc, original_nc, var_name, var_config)

    def _create_variable(self, open_nc: Dataset, original_nc: Dataset, var_name: str, var_config: dict, as_parameter: bool = False):
        var = open_nc.create_variable(
            var_name,
            str if var_config['type'] == 'str' else var_config['type'],
            var_config['dimensions'] if 'dimensions' in var_config and var_config['dimensions'] else None,
            var_config['attributes'] or {}
        )
        if 'copy_data_from' in var_config and var_config['copy_data_from']:
            copy_from = var_config['copy_data_from']
            if not var_name.endswith('_QC'):
                test_name = f'{copy_from}_QC'
                if original_nc.has_variable(test_name):
                    var.set_attribute('ancilliary_variables', f'{var_name}_QC')
            if original_nc.has_variable(copy_from):
                original_var = original_nc.variable(copy_from)
                original_data = original_var.data()
                if 'data_processor' in var_config and var_config['data_processor'] is not None:
                    data_processor = dynamic_object(var_config['data_processor'])
                    temp_data = []
                    for x in original_data:
                        temp_data.append(data_processor(x))
                        self.breakpoint()
                var.set_data(original_data)
                self.breakpoint()
                if 'copy_attributes' in var_config and var_config['copy_attributes']:
                    for attr_name in var_config['copy_attributes']:
                        var.set_attribute(attr_name, original_var.attribute(attr_name))
        self.breakpoint()

    def _build_parameters(self, open_nc: Dataset, original_nc: Dataset, sensor_map: dict[str, str]):
        for param_name in self._mapping_data['parameters']:
            param_config = self._mapping_data['parameters'][param_name]
            if not original_nc.has_variable(param_config['copy_data_from']):
                continue
            data = original_nc.variable(param_config['copy_data_from']).data()
            if all(math.isnan(x) for x in data):
                continue
            self._create_variable(open_nc, original_nc, param_name, param_config, as_parameter=True)
            var = open_nc.variable(param_name)
            var.set_attribute('coordinates', "TIME,LONGITUDE,LATITUDE,DEPTH")
            if param_name in sensor_map:
                var.set_attribute('sensor', sensor_map[param_name])
            if original_nc.has_variable(f'{param_config['copy_data_from']}_QC'):
                self._create_variable(open_nc, original_nc, f"{param_name}_QC", {
                    'type': 'i1',
                    'dimensions': ('N_MEASUREMENTS',),
                    'copy_data_from': f'{param_name}_QC',
                    'attributes': {
                        'long_name': f'Quality flag for {param_config['attributes']['long_name']}',
                        'long_name_fr': f'Drapeau de qualitée pour {param_config['attributes']['long_name_fr']}',
                        'coordinates': "TIME,LONGITUDE,LATITUDE,DEPTH",
                        'rtqc_methodology': "Coriolis MATLAB toolbox",
                        'rtqc_methodology_fr': "Boîte à outils Coriolis Matlab",
                        '_FillValue': -127,
                        'valid_min': 0,
                        'valid_max': 0,
                },
                    'copy_attributes': ['flag_values', 'flag_meanings'],
                })
                var.set_attribute('ancillary_variables', f'{param_name}_QC')
            self.breakpoint()


    def _build_depths(self, open_nc: Dataset, original_nc: Dataset):
        pressures = original_nc.variable('PRES').data()
        self.breakpoint()
        pressures_qc = original_nc.variable('PRES_QC').data()
        self.breakpoint()
        latitudes = original_nc.variable('LATITUDE').data()
        self.breakpoint()
        latitudes_qc = original_nc.variable('POSITION_QC').data()
        self.breakpoint()
        depths = []
        for x in range(0, len(pressures)):
            if pressures_qc[x] in (3,4) or latitudes_qc[x] in (3,4) or math.isnan(pressures[x]) or math.isnan(latitudes[x]):
                depths.append(-9999.9)
            else:
                depths.append(seawater.eos80_depth(float(pressures[x]), float(latitudes[x])))
            self.breakpoint()
        actual_values = [d for d in depths if not math.isnan(d) and d >= 0]
        if actual_values:
            open_nc.set_attribute('geospatial_vertical_min', min(actual_values))
            open_nc.set_attribute('geospatial_vertical_max', max(actual_values))
        open_nc.variable('DEPTH').set_data(depths)
        self.breakpoint()

    def _build_times(self, open_nc: Dataset, original_nc: Dataset):
        times = original_nc.variable('JULD').data()
        seconds = []
        min_time = None
        max_time = None
        for d in times:
            if math.isnan(d):
                seconds.append(None)
                continue
            actual_time = datetime.datetime.fromisoformat("1950-01-01T00:00:00")
            actual_time += datetime.timedelta(days=d)
            time_delta = actual_time - self._base_time
            if actual_time.year > 1970:
                if min_time is None or actual_time < min_time:
                    min_time = actual_time
                if max_time is None or actual_time > max_time:
                    max_time = actual_time
            seconds.append(time_delta.total_seconds())
            self.breakpoint()
        open_nc.variable('TIME').set_data(seconds)
        self.breakpoint()
        open_nc.set_attribute('time_coverage_start', min_time.strftime('%Y%m%dT%H%M%SZ'))
        open_nc.set_attribute('time_coverage_end', max_time.strftime('%Y%m%dT%H%M%SZ'))

    def _build_contact_info(self, contact_name, role):
        info = {}
        if 'contact_info' in self._mapping_data and self._mapping_data['contact_info'] and contact_name.lower() in self._mapping_data['contact_info']:
            info = self._mapping_data['contact_info'][contact_name.lower()]
        return (
            info['proper_name'] if 'proper_name' in info else contact_name,
            info['id'] if 'id' in info else '',
            info['email'] if 'email' in info else '',
            role
        )

    def _build_contributors(self, open_nc: Dataset, original_nc: Dataset):
        contributors = []
        institutions = []
        if original_nc.has_attribute('principal_investigator'):
            for pi in original_nc.attribute('principal_investigator').split(';'):
                contributors.append(self._build_contact_info(pi.strip(), 'CONT0004'))
        for op in original_nc.variable('OPERATING_INSTITUTION').as_string().split(';'):
            institutions.append(self._build_contact_info(op.strip(), 'CONT0003'))
        for owner in original_nc.variable('GLIDER_OWNER').as_string().split(';'):
            institutions.append(self._build_contact_info(owner.strip(), 'CONT0002'))

        open_nc.set_attribute('contributor_name', ','.join(c[0] for c in contributors))
        open_nc.set_attribute('contributor_email', ','.join(c[1] for c in contributors))
        open_nc.set_attribute('contributor_id', ','.join(c[2] for c in contributors))
        open_nc.set_attribute('contributor_id_vocabulary', 'https://orcid.org/')
        open_nc.set_attribute('contributor_role', ','.join(c[3] for c in contributors))
        open_nc.set_attribute('contributor_role_vocabulary','https://vocab.nerc.ac.uk/collection/W08/current/')
        open_nc.set_attribute('contributing_institutions', ','.join(c[0] for c in institutions))
        open_nc.set_attribute('contributing_institutions_id', ','.join(c[1] for c in institutions))
        open_nc.set_attribute('contributing_institutions_id_vocabulary', 'https://ror.org/')
        open_nc.set_attribute('contributing_institutions_role', ','.join(c[3] for c in institutions))
        open_nc.set_attribute('contributing_institutions_role_vocabulary', 'https://vocab.nerc.ac.uk/collection/W08/current/')

    def _build_deployment_info(self, open_nc: Dataset, original_nc: Dataset, platform: str, start_time: str):
        deploy_start = original_nc.variable('DEPLOYMENT_START_DATE').as_string().strip()
        if len(deploy_start) == 8:
            start_date = datetime.datetime.strptime(deploy_start, '%Y%m%d')
        elif len(deploy_start) == 12:
            start_date = datetime.datetime.strptime(deploy_start, '%Y%m%d%H%M')
        elif len(deploy_start) == 14:
            start_date = datetime.datetime.strptime(deploy_start, '%Y%m%d%H%M%S')
        else:
            raise CNODCError(f"Unknown date format for [{deploy_start}]")
        open_nc.set_attribute('start_date', start_date.strftime('%Y%m%dT%H%M%SZ'))
        open_nc.variable('TRAJECTORY').set_data_from_string(f"{platform}_{start_time}")
        open_nc.variable('DEPLOYMENT_TIME').set_data((start_date - self._base_time).total_seconds())

    def _build_glider_info(self, open_nc: Dataset, original_nc: Dataset, platform_name):
        open_nc.variable('PLATFORM_NAME').set_data_from_string(platform_name)
        open_nc.variable('WMO_IDENTIFIER').set_data_from_string(original_nc.attribute('wmo_platform_code'))
        ego_model_code = original_nc.variable('PLATFORM_TYPE').as_string()
        if ego_model_code.lower() not in self._mapping_data['glider_model_map']:
            raise CNODCError(f'Unknown platform model: {ego_model_code}')
        model_info = self._mapping_data['glider_model_map'][ego_model_code.lower()]
        open_nc.variable('PLATFORM_MODEL').set_data_from_string(model_info['model'])
        serial_no = original_nc.variable('GLIDER_SERIAL_NO').as_string()
        open_nc.variable('PLATFORM_SERIAL_NUMBER').set_data_from_string(f"{model_info['prefix']}{serial_no}")
        open_nc.variable('PLATFORM_MAKER').set_data_from_string(model_info['maker'])
        battery_type = original_nc.variable('BATTERY_TYPE').as_string()
        if battery_type:
            if battery_type not in self._mapping_data['battery_type_map']:
                raise CNODCError(f'Unknown battery type: {battery_type}')
            open_nc.variable('BATTERY_TYPE').set_data_from_string(self._mapping_data['battery_type_map'][battery_type])
        # BATTERY_PACKS??
        # FIRMWARE_VERSION_NAVIGATION
        # FIRMWAVE_VERSION_SCIENCE
        # GLIDER_MANUAL_VERSION
        trans_systems = set()
        for sys_name in original_nc.variable('TRANS_SYSTEM').all_as_strings():
            if not sys_name:
                continue
            if sys_name.lower() not in self._mapping_data['trans_system_type_map']:
                raise CNODCError(f'Unknown transmission system: {sys_name}')
            trans_systems.add(self._mapping_data['trans_system_type_map'][sys_name.lower()])
        open_nc.variable('TELECOM_TYPE').set_data_from_string(','.join(trans_systems))
        track_systems = set()
        for sys_name in original_nc.variable('POSITIONING_SYSTEM').all_as_strings():
            if not sys_name:
                continue
            if sys_name.lower() not in self._mapping_data['track_system_type_map']:
                raise CNODCError(f'Unknown positioning system: {sys_name}')
            track_systems.add(self._mapping_data['track_system_type_map'][sys_name.lower()])
        open_nc.variable('TRACKING_SYSTEM').set_data_from_string(','.join(track_systems))

    def _build_phase_info(self, original_nc, open_nc):
        phase_data = original_nc.variable('PHASE').data()
        new_phase_data = []
        new_phase_qc = []
        for phase_info in phase_data:
            if math.isnan(phase_info):
                new_phase_data.append(-128)
                new_phase_qc.append(-128)
            else:
                new_phase_data.append(int(self._mapping_data['phase_map'][int(phase_info)]))
                new_phase_qc.append(0)
        open_nc.variable('PHASE').set_data(new_phase_data)
        open_nc.variable('PHASE_QC').set_data(new_phase_qc)


def validate_glider_file(file: pathlib.Path, metadata: dict):
    return True


