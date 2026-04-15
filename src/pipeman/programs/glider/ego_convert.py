import datetime
import typing as t
import pathlib
from copy import copy
import numpy as np
import yaml
import netCDF4 as nc
import zrlog
import dataclasses

from pipeman.programs.dmd.metadata import MaintenanceFrequency
from pipeman.exceptions import CNODCError
from medsutil.sanitize import utf_normalize_string, unnumpy
from medsutil.first import first, first_i18n
import medsutil.seawater as seawater
import pipeman.programs.dmd.metadata as metadata
from medsutil.sanitize import netcdf_bytes_to_string, netcdf_string_to_vlen_bytes
import medsutil.awaretime as awaretime
import medsutil.types as ct


@dataclasses.dataclass
class ContactInfo:
    proper_name: ct.AcceptAsLanguageDict = ''
    short_name: str = ''
    key_name: str = ''
    email: ct.AcceptAsLanguageDict = ''
    research_id: str = ''
    research_id_type: str = ''
    guid: str = ''
    role: str = ''
    contact_type: str = ''



class GliderError(CNODCError):

    def __init__(self, msg, code, is_transient=False):
        super().__init__(msg, 'GLIDER', code, is_transient)


def ego_sensor_info(ds: nc.Dataset,
                    sensor_map: dict[str, dict[str, str]],
                    model_map: dict[str, str],
                    make_map: dict[str, str]):
    if 'PARAMETER' in ds.variables:
        return _ego_new_sensor_info(ds, model_map, make_map)
    else:
        return _ego_old_sensor_info(ds, sensor_map)

def _ego_old_sensor_info(original_nc: nc.Dataset, sensor_map: dict[str, dict[str, str]]):
    sensors = {}
    sensors_seen = set()
    param_map = {}
    for var_name in original_nc.variables:
        var = original_nc.variables[var_name]
        if not hasattr(var, 'sensor_name'):
            continue
        sensor_full_name = utf_normalize_string(getattr(var, 'sensor_name').strip().lower())
        if sensor_full_name not in sensor_map:
            raise GliderError(f"Unknown sensor [{sensor_full_name}]", 1000)
        info: dict = copy(sensor_map[sensor_full_name])
        info['serial'] = getattr(var, 'sensor_serial_number') if hasattr(var, 'sensor_serial_number') else 'unknown'
        key = f"SENSOR_{info['type']}_{info['serial']}"
        param_map[var.name] = key
        if key in sensors_seen:
            continue
        sensors_seen.add(key)
        sensors[key] = info
    return sensors, param_map


def _ego_new_sensor_info(original_nc: nc.Dataset, model_map: dict[str, str], make_map: dict[str, str]):
    sensor_names = [netcdf_bytes_to_string(x) for x in original_nc.variables['SENSOR'][:]]
    sensor_makers = [netcdf_bytes_to_string(x) for x in original_nc.variables['SENSOR_MAKER'][:]]
    sensor_models = [netcdf_bytes_to_string(x) for x in original_nc.variables['SENSOR_MODEL'][:]]
    sensor_serials = [netcdf_bytes_to_string(x) for x in original_nc.variables['SENSOR_SERIAL_NO'][:]]
    sensor_mounts = [netcdf_bytes_to_string(x) for x in original_nc.variables['SENSOR_MOUNT'][:]]
    sensor_orientations = [netcdf_bytes_to_string(x) for x in original_nc.variables['SENSOR_ORIENTATION'][:]]
    param_names = [netcdf_bytes_to_string(x) for x in original_nc.variables['PARAMETER'][:]]
    param_sensors = [netcdf_bytes_to_string(x) for x in original_nc.variables['PARAMETER_SENSOR'][:]]
    sensor_info = {}
    param_map = {}

    for x in range(0, len(sensor_names)):
        if sensor_names[x].startswith('CTD_'):
            sensor_type = 'CTD'
        elif sensor_names[x].startswith('FLUOROMETER_') or sensor_names[x].startswith('BACKSCATTER'):
            sensor_type = 'FLUOROMETER'
        elif sensor_names[x] == 'OPTODE_DOXY':
            sensor_type = 'DOXY'
        else:
            raise GliderError(f"Unknown glider instrument type: {sensor_names[x]}", 1001)
        key = f"SENSOR_{sensor_type}_{sensor_serials[x]}"
        for idx, val in enumerate(param_sensors):
            if val == sensor_names[x]:
                param_map[param_names[idx]] = key
        if key not in sensor_info:
            make = sensor_makers[x]
            if make:
                if make.lower() in make_map:
                    make = make_map[make.lower()]
                else:
                    zrlog.get_logger('cnodc.glider.sensor_info').notice(f'No make for [{make}]')
            model = sensor_models[x]
            if model:
                if model.lower() in model_map:
                    model = model_map[model.lower()]
                else:
                    zrlog.get_logger('cnodc.glider.sensor_info').notice(f'No model for [{model}]')
            sensor_info[key] = {
                'type': sensor_type,
                'make': make,
                'model': model,
                'serial': sensor_serials[x],
                'location': sensor_mounts[x].lower().replace('_', ' ') + ('' if not sensor_orientations[x] else ' - ' + sensor_orientations[x].lower()),
            }
    return sensor_info, param_map


class OpenGliderConverter:

    @staticmethod
    def build(map_file: t.Optional[pathlib.Path] = None, halt_flag=None):
        if map_file is None:
            map_file: pathlib.Path = pathlib.Path(__file__).absolute().parent / 'ego_conversion.yaml'
        with open(map_file, encoding="utf-8") as h:
            return OpenGliderConverter(yaml.safe_load(h), halt_flag)

    def __init__(self, mapping_data, halt_flag=None):
        self._mapping_data = mapping_data
        self._data_maps = {}
        self._base_time = awaretime.utc_from_isoformat('1970-01-01T00:00:00')
        self._halt = halt_flag
        self._log = zrlog.get_logger('cnodc.gliders.ego_convert')

    def breakpoint(self):
        if self._halt is not None:
            self._halt.breakpoint()

    def build_metadata(self, open_file: pathlib.Path, file_name: str = None) -> metadata.DatasetMetadata:
        with nc.Dataset(open_file, "r") as ds:
            return self._build_metadata(ds, file_name or open_file.name)

    def _build_metadata(self, ds: nc.Dataset, file_name: str):
        glider_name, mission_time, data_mode = self._parse_file_name(file_name)
        dmd = metadata.DatasetMetadata()
        dmd.set_meds_defaults()
        dmd.set_from_netcdf_file(ds)
        if data_mode in 'RAP':
            dmd.processing_level = 'real-time'
        mission_id = ds.getncattr('id')
        for var in dmd.variables:
            if var.source_name in ('LATITUDE', 'LONGITUDE', 'DEPTH', 'TIME'):
                var.destination_name = var.source_name.lower()
        if 'users' in self._mapping_data['netcdf_conversion'] and self._mapping_data['netcdf_conversion']['users']:
            dmd.users.update(self._mapping_data['netcdf_conversion']['users'])
        dmd.erddap_servers.append(metadata.Common.ERDDAP_Primary)
        dmd.erddap_dataset_id = mission_id
        dmd.erddap_data_file_path = f"/cloud_data/gliders/{mission_id.lower()}/"
        dmd.erddap_dataset_type = metadata.ERDDAPDatasetType.DSGTable
        dmd.erddap_data_file_pattern = '*\\.nc\\.gz'
        dmd.add_file_direct_link(
            f"https://cnodc-cndoc.azure.cloud-nuage.dfo-mpo.gc.ca/public/data-donnees/glider-planeur/{file_name}",
            {
                "en": "NetCDF File in OpenGlider format",
                "fr:": "Ficher NetCDF en format OpenGlider"
            },
        )
        return dmd

    def convert(self, ego_file, og_file, file_name: t.Optional[str] = None):
        with nc.Dataset(ego_file, "r") as original_nc:
            with nc.Dataset(og_file, "w", format="NETCDF4") as open_nc:
                return self._convert(open_nc, original_nc, file_name or pathlib.Path(ego_file).name)

    def _parse_file_name(self, file_name: str):
        try:
            gn, mt, dm = file_name[:-3].rsplit('_', maxsplit=2)
            return gn, mt, dm
        except Exception as ex:
            raise GliderError(f'Invalid filename [{file_name}]', 2021) from ex

    def _convert(self, open_nc: nc.Dataset, original_nc: nc.Dataset, file_name: str):
        platform, start_time, data_mode = self._parse_file_name(file_name)
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
        self._build_phase_info(open_nc, original_nc)
        open_nc.setncattr('date_created', awaretime.utc_now().strftime('%Y%m%dT%H%M%SZ'))
        mission_id = open_nc.getncattr('id')
        return file_name, mission_id

    def _create_dimensions(self, open_nc: nc.Dataset):
        open_nc.createDimension("N_MEASUREMENTS", None)

    def _map_static_metadata(self, open_nc: nc.Dataset):
        for key in self._mapping_data['netcdf_conversion']['static_metadata']:
            open_nc.setncattr(key, self._mapping_data['netcdf_conversion']['static_metadata'][key])

    def _copy_metadata(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
        for key in self._mapping_data['netcdf_conversion']['copy_metadata']:
            old_key = self._mapping_data['netcdf_conversion']['copy_metadata'][key]
            if hasattr(original_nc, old_key):
                open_nc.setncattr(key, original_nc.getncattr(old_key))
            else:
                self._log.warning(f"EGO file does not have the attribute [{old_key}]")

    def _set_metadata_from_file_name(self, open_nc: nc.Dataset, platform, start_time, data_mode):
        rtqc_method = None
        if data_mode == 'R':
            rtqc_method = 'Coriolis MATLAB Toolbox'
            summary_prefix = ('Real-time data', 'Données en temps réel')
            title_suffix = ('Real-Time', 'temps réel')
        elif data_mode == 'P':
            rtqc_method = 'Coriolis MATLAB Toolbox'
            summary_prefix = ('Preliminary data', 'Données préliminaire')
            title_suffix = ('Preliminary', 'préliminaire')
        elif data_mode == 'A':
            summary_prefix = ('Adjusted data', 'Données ajustées')
            title_suffix = ('Adjusted', 'ajustées')
        elif data_mode == 'D':
            summary_prefix = ('Delayed-mode data', 'Données en temps différé')
            title_suffix = ('Delayed-Mode', 'différé')
        elif data_mode == 'M':
            summary_prefix = ('Mixed data', 'Données mixte')
            title_suffix = ('Mixed', 'mixte')
        else:
            raise GliderError(f'Invalid glider data mode [{data_mode}]', 2019)
        if rtqc_method:
            open_nc.setncattr('rtqc_method', rtqc_method)
        open_nc.setncattr('title', f'Glider {platform} - {start_time} ({title_suffix[0]})')
        open_nc.setncattr('title_fr', f'Planeur {platform} - {start_time} ({title_suffix[1]})')
        open_nc.setncattr("summary", f"{summary_prefix[0]} from glider mission {platform}_{start_time}")
        open_nc.setncattr("summary_fr", f"{summary_prefix[1]} de la mission du planeur {platform}_{start_time}")
        open_nc.setncattr('id', f"{platform}_{start_time}_{data_mode}")

    def _validate_lon_lat(self, original_nc):
        if 'LATITUDE' not in original_nc.variables:
            raise GliderError('Missing latitude variable', 2000)
        if 'LONGITUDE' not in original_nc.variables:
            raise GliderError('Missing longitude variable', 2001)

    def _set_geospatial_bounds_metadata(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
        self._validate_lon_lat(original_nc)
        latitudes = original_nc.variables['LATITUDE'][:]
        if latitudes.size > 0:
            open_nc.setncattr('geospatial_lat_min', np.min(latitudes))
            open_nc.setncattr('geospatial_lat_max', np.max(latitudes))
        else:
            self._log.warning(f'No latitude values detected')
        longitudes = original_nc.variables['LONGITUDE'][:]
        if longitudes.size > 0:
            open_nc.setncattr('geospatial_lon_min', np.min(longitudes))
            open_nc.setncattr('geospatial_lon_max', np.max(longitudes))
        else:
            self._log.warning(f'No longitude values detected')

    def _set_sensor_metadata(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
        sensor_info, param_map = ego_sensor_info(
            original_nc,
            self._get_data_map('sensors'),
            self._get_data_map('sensor_models'),
            self._get_data_map('sensor_makes')
        )
        self._create_openglider_sensor_vars(open_nc, sensor_info)
        return param_map

    def _create_openglider_sensor_vars(self, open_nc: nc.Dataset, sensors: dict[str, dict[str, str]]):
        for key in sensors:
            info = sensors[key]
            var = open_nc.createVariable(f'SENSOR_{info['type']}_{info['serial']}', 'f4', ())
            var.setncattr('long_name', f"{info['make']} {info['model']}")
            var.setncattr('sensor_model', info['model'])
            var.setncattr('sensor_maker', info['make'])
            var.setncattr('sensor_serial_number', info['serial'])
            self.breakpoint()

    def _build_variables(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
        for var_name in self._mapping_data['netcdf_conversion']['variables']:
            var_config = self._mapping_data['netcdf_conversion']['variables'][var_name]
            if 'copy_data_from' in var_config and var_config['copy_data_from'] and not var_config['copy_data_from'] in original_nc.variables:
                continue
            self._create_variable(open_nc, original_nc, var_name, var_config)

    def _create_variable(self, open_nc: nc.Dataset, original_nc: nc.Dataset, var_name: str, var_config: dict):
        kwargs = {}
        if var_config['attributes'] and 'missing_value' in var_config['attributes']:
            kwargs['fill_value'] = var_config['attributes']['missing_value']
        var = open_nc.createVariable(
            var_name,
            str if var_config['type'] == 'str' else var_config['type'],
            var_config['dimensions'] if 'dimensions' in var_config and var_config['dimensions'] else (),
            **kwargs
        )
        if var_config['attributes']:
            for attr_name in var_config['attributes']:
                var.setncattr(attr_name, var_config['attributes'][attr_name])
        if 'copy_data_from' in var_config and var_config['copy_data_from']:
            copy_from = var_config['copy_data_from']
            if copy_from in original_nc.variables:
                original_var = original_nc.variables[copy_from]
                original_data = original_var[:]
                var[:] = original_data
                if 'copy_attributes' in var_config and var_config['copy_attributes']:
                    for attr_name in var_config['copy_attributes']:
                        if hasattr(original_var, attr_name):
                            setattr(var, attr_name, getattr(original_var, attr_name))
                        else:
                            self._log.warning(f"Variable {original_var.name} is missing attribute {attr_name}")
        return var

    def _build_parameters(self, open_nc: nc.Dataset, original_nc: nc.Dataset, sensor_map: dict[str, str]):
        for param_name in self._mapping_data['netcdf_conversion']['parameters']:
            param_config = self._mapping_data['netcdf_conversion']['parameters'][param_name]
            if param_config['copy_data_from'] not in original_nc.variables:
                continue
            data = unnumpy(original_nc.variables[param_config['copy_data_from']][:])
            if all(x is None for x in data):
                continue
            var = self._create_variable(open_nc, original_nc, param_name, param_config)
            var.setncattr('coordinates', "TIME,LONGITUDE,LATITUDE,DEPTH")
            if param_name in sensor_map:
                var.setncattr('sensor', sensor_map[param_name])
            qc_test_name = f'{param_config['copy_data_from']}_QC'
            if qc_test_name in original_nc.variables:
                self._create_variable(open_nc, original_nc, f"{param_name}_QC", {
                    'type': 'i1',
                    'dimensions': ('N_MEASUREMENTS',),
                    'copy_data_from': qc_test_name,
                    'attributes': {
                        'long_name': f'Quality flag for {param_config['attributes']['long_name']}',
                        'long_name_fr': f'Drapeau de qualitée pour {param_config['attributes']['long_name_fr']}',
                        'coordinates': "TIME,LONGITUDE,LATITUDE,DEPTH",
                        'rtqc_methodology': "Coriolis MATLAB toolbox",
                        'rtqc_methodology_fr': "Boîte à outils Coriolis Matlab",
                        'missing_value': -127,
                        'valid_min': 0,
                        'valid_max': 0,
                },
                    'copy_attributes': ['flag_values', 'flag_meanings'],
                })
                setattr(var, 'ancillary_variables', f'{param_name}_QC')
            self.breakpoint()

    def _validate_build_depths(self, original_nc: nc.Dataset):
        if 'LATITUDE' not in original_nc.variables:
            raise GliderError('Missing LATITUDE variable', 2020)
        if 'PRES' not in original_nc.variables:
            raise GliderError('Missing PRES variable', 2002)
        if 'PRES_QC' not in original_nc.variables:
            raise GliderError('Missing PRES_QC variable', 2003)
        if 'POSITION_QC' not in original_nc.variables:
            raise GliderError('Missing POSITION_QC variable', 2004)

    def _build_depths(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
        self._validate_build_depths(original_nc)
        pressures = original_nc.variables['PRES'][:]
        pressures_qc = original_nc.variables['PRES_QC'][:]
        latitudes = original_nc.variables['LATITUDE'][:]
        latitudes_qc = original_nc.variables['POSITION_QC'][:]
        depths = []
        min_depth = None
        max_depth = None
        for x in range(0, len(pressures)):
            pressure = unnumpy(pressures[x])
            pressure_qc = unnumpy(pressures_qc[x])
            latitude = unnumpy(latitudes[x])
            latitude_qc = unnumpy(latitudes_qc[x])
            if pressure_qc in (4, 9) or latitude_qc in (4, 9) or pressure is None or latitude is None:
                depths.append(-9999.9)
            else:
                depth = float(seawater.eos80_depth(pressure, latitude))
                if min_depth is None or depth < min_depth:
                    min_depth = depth
                if max_depth is None or depth > max_depth:
                    max_depth = depth
                depths.append(depth)
        if min_depth is not None:
            open_nc.setncattr('geospatial_vertical_min', min_depth)
        if max_depth is not None:
            open_nc.setncattr('geospatial_vertical_max', max_depth)
        open_nc.variables['DEPTH'][:] = depths

    def _validate_build_times(self, original_nc):
        if 'JULD' not in original_nc.variables:
            raise GliderError('Missing JULD variable', 2005)
        juld_var = original_nc.variables['JULD']
        try:
            period, _, epoch = getattr(juld_var, 'units').split(' ', maxsplit=2)
            if period not in ('seconds', 'minutes', 'hours', 'days'):
                raise ValueError(f'invalid period [{period}]')
            return period, awaretime.utc_from_isoformat(epoch)
        except Exception as ex:
            raise GliderError('JULD variable is missing or has invalid units', 2006) from ex

    def _build_times(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
        period, local_base_time = self._validate_build_times(original_nc)
        times = original_nc.variables['JULD'][:]
        seconds = []
        min_time = None
        max_time = None
        for d in times:
            d = unnumpy(d)
            if d is None:
                seconds.append(None)
                continue
            actual_time = (local_base_time + datetime.timedelta(**{period: d}))
            time_delta = actual_time - self._base_time
            if actual_time.year > 1970:
                if min_time is None or actual_time < min_time:
                    min_time = actual_time
                if max_time is None or actual_time > max_time:
                    max_time = actual_time
            seconds.append(time_delta.total_seconds())
            self.breakpoint()
        open_nc.variables['TIME'][:] = seconds
        if min_time is not None or max_time is not None:
            open_nc.setncattr('time_coverage_start', min_time.strftime('%Y%m%dT%H%M%S%:z'))
            open_nc.setncattr('time_coverage_end', min_time.strftime('%Y%m%dT%H%M%S%:z'))

    def _get_data_map(self, name):
        if name not in self._data_maps:
            self._data_maps[name] = {}
            if name in self._mapping_data['data_maps'] and self._mapping_data['data_maps'][name] and isinstance(self._mapping_data['data_maps'][name], dict):
                self._data_maps[name] = self._mapping_data['data_maps'][name]
        return self._data_maps[name]

    def _build_contact_info(self, contact_name, role, default_type = 'individual') -> ContactInfo:
        contact_map = self._get_data_map('contacts')
        contact_name_key = contact_name.lower().replace(' ', '')
        info = {
            'proper_name': contact_name,
            'key_name': contact_name_key,
            'guid': '',
            'short_name': '',
            'email': '',
            'research_id': '',
            'research_id_type': '',
            'role': role,
            'contact_type': default_type
        }
        if contact_name_key in contact_map:
            values = contact_map[contact_name_key]
            # We use this as an alias to the real entry
            if isinstance(values, str):
                info['key_name'] = values
                values = contact_map[values]
            info.update(values)
        else:
            self._log.notice(f'No contact information for {contact_name}')
        info['guid'] = first(
            info['guid'],
            info['research_id'],
            first_i18n(info['email']),
            info['key_name'],
            default=''
        )
        info['short_name'] = first(
            info['short_name'],
            first_i18n(info['proper_name'])
        )
        if info['research_id']:
            info['research_id_type'] = first(
                info['research_id_type'],
                'https://ror.org/' if info['contact_type'] == 'institution' else 'https://orcid.org/'
            )
        else:
            info['research_id_type'] = ''
        return ContactInfo(**info)

    def _validate_contributors(self, original_nc: nc.Dataset):
        if not hasattr(original_nc, 'principal_investigator'):
            raise GliderError('Missing mandatory [principal_investigator] attribute', 2012)
        if 'OPERATING_INSTITUTION' not in original_nc.variables:
            raise GliderError('Missing mandatory [OPERATING_INSTITUTION] variable', 2013)

    @staticmethod
    def _get_multilingual_contact_info(info_list: t.Iterable[str | dict[str, str]]):
        result = {
            'en': [],
            'fr': []
        }
        for info in info_list:
            if isinstance(info, str):
                result['en'].append(info)
                result['fr'].append(info)
            elif 'und' in info and info['und']:
                result['en'].append(info['und'])
                result['fr'].append(info['und'])
            else:
                result['en'].append(info['en'] if 'en' in info else '')
                result['fr'].append(info['fr'] if 'fr' in info else '')
        return result

    def _compress_join(self, values: t.Iterable[str], allow_blanks: bool = False) -> str:
        values = list(values)
        if (not values) or not any(values):
            return ''
        first = next((x for x in values if x), '')
        if all(x == first or (allow_blanks and x == '') for x in values):
            return first
        return ','.join(values)

    def _build_contributors(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
        self._validate_contributors(original_nc)
        contributors: list[ContactInfo] = []
        institutions: list[ContactInfo] = []
        for pi in original_nc.getncattr('principal_investigator').split(';'):
            contributors.append(self._build_contact_info(pi.strip(), 'CONT0004'))
        for op in netcdf_bytes_to_string(original_nc.variables['OPERATING_INSTITUTION'][:]).split(';'):
            institutions.append(self._build_contact_info(op.strip(), 'CONT0003'))
        if 'GLIDER_OWNER' in original_nc.variables:
            for owner in netcdf_bytes_to_string(original_nc.variables['GLIDER_OWNER'][:]).split(';'):
                institutions.append(self._build_contact_info(owner.strip(), 'CONT0002'))
        else:
            self._log.warning('Missing suggested [GLIDER_OWNER] variable')
        info_url = self._get_info_url(
            open_nc.getncattr('network') if hasattr(open_nc, 'network') else '',
            open_nc.getncattr('id') if hasattr(open_nc, 'id') else '',
            institutions
        )
        if info_url:
            open_nc.setncattr('infoUrl', info_url)
        else:
            self._log.warning(f'No infoUrl found')
        if contributors:
            cont_emails = self._get_multilingual_contact_info(c.email for c in contributors)
            open_nc.setncattr('contributor_name', self._compress_join(c.short_name for c in contributors))
            open_nc.setncattr('contributor_id', self._compress_join(c.research_id for c in contributors))
            open_nc.setncattr('contributor_email', self._compress_join(cont_emails['en']))
            if any(x for x in cont_emails['fr']):
                open_nc.setncattr('contributor_email_fr', self._compress_join(cont_emails['fr']))
            open_nc.setncattr('contributor_id_vocabulary', self._compress_join((c.research_id_type for c in contributors), True))
            open_nc.setncattr('contributor_role', self._compress_join(c.role for c in contributors))
            open_nc.setncattr('contributor_role_vocabulary','https://vocab.nerc.ac.uk/collection/W08/current/')
            open_nc.setncattr('contributor_cnodc_guid', self._compress_join(c.guid for c in contributors))
            open_nc.setncattr('contributor_type', self._compress_join(c.contact_type for c in contributors))
        if institutions:
            open_nc.setncattr('contributing_institutions', self._compress_join(c.short_name for c in institutions))
            open_nc.setncattr('contributing_institutions_id', self._compress_join(c.research_id for c in institutions))
            open_nc.setncattr('contributing_institutions_cnodc_guid', self._compress_join(c.guid for c in institutions))
            open_nc.setncattr('contributing_institutions_id_vocabulary', self._compress_join((c.research_id_type for c in institutions), True))
            open_nc.setncattr('contributing_institutions_role', self._compress_join(c.role for c in institutions))
            open_nc.setncattr('contributing_institutions_role_vocabulary', 'https://vocab.nerc.ac.uk/collection/W08/current/')

    def _get_info_url(self, network: str, mission_id: str, institutions: list[ContactInfo]):
        glider_names = self._get_data_map('institution_glider_names')
        institution_urls = self._get_data_map('institution_urls')
        for inst in institutions:
            names: list[str] = list(inst.proper_name.values()) if isinstance(inst.proper_name, dict) else [inst.proper_name]
            names.append(inst.short_name)
            names.append(inst.contact_type)
            for name in names:
                if name is not None and name.lower() in institution_urls:
                    return institution_urls[name.lower()]
        mission_id = mission_id.lower()
        for glider_name in glider_names:
            if mission_id.startswith(glider_name):
                return institution_urls[glider_names[glider_name]]
        network = network.lower()
        for inst_name in institution_urls:
            if inst_name in network:
                return institution_urls[inst_name]
        return None

    def _validate_deployment_info(self, original_nc: nc.Dataset) -> awaretime.AwareDateTime:
        if 'DEPLOYMENT_START_DATE' not in original_nc.variables:
            raise GliderError(f"Missing mandatory variable [DEPLOYMENT_START_DATE]", 2014)
        deploy_start = netcdf_bytes_to_string(original_nc.variables['DEPLOYMENT_START_DATE'][:]).strip()
        if len(deploy_start) == 8:
            return awaretime.utc_from_string(deploy_start, '%Y%m%d')
        elif len(deploy_start) == 12:
            return awaretime.utc_from_string(deploy_start, '%Y%m%d%H%M')
        elif len(deploy_start) == 14:
            return awaretime.utc_from_string(deploy_start, '%Y%m%d%H%M%S')
        else:
            raise GliderError(f"Unknown date format for [{deploy_start}]", 2008)

    def _validate_deployment_end_info(self, original_nc: nc.Dataset) -> awaretime.AwareDateTime | None:
        if 'DEPLOYMENT_END_DATE' not in original_nc.variables:
            return None
        deploy_start = netcdf_bytes_to_string(original_nc.variables['DEPLOYMENT_END_DATE'][:]).strip()
        if not deploy_start:
            return None
        if len(deploy_start) == 8:
            return awaretime.utc_from_string(deploy_start, '%Y%m%d')
        elif len(deploy_start) == 12:
            return awaretime.utc_from_string(deploy_start, '%Y%m%d%H%M')
        elif len(deploy_start) == 14:
            return awaretime.utc_from_string(deploy_start, '%Y%m%d%H%M%S')
        else:
            raise GliderError(f"Unknown date format for [{deploy_start}]", 2023)

    def _build_deployment_info(self, open_nc: nc.Dataset, original_nc: nc.Dataset, platform: str, start_time: str):
        start_date = self._validate_deployment_info(original_nc)
        end_date = self._validate_deployment_end_info(original_nc)
        if end_date is None:
            open_nc.setncattr('is_ongoing', 'Y')
            open_nc.setncattr('data_maintenance_frequency', MaintenanceFrequency.AsNeeded.value)
            open_nc.setncattr('metadata_maintenance_frequency', MaintenanceFrequency.AsNeeded.value)
        else:
            open_nc.setncattr('is_ongoing', 'N')
            open_nc.setncattr('data_maintenance_frequency', MaintenanceFrequency.NotPlanned.value)
            open_nc.setncattr('metadata_maintenance_frequency', MaintenanceFrequency.NotPlanned.value)
        open_nc.setncattr('start_date', start_date.isoformat())
        open_nc.setncattr('end_date', end_date.isoformat() if end_date else '')
        open_nc.variables['DEPLOYMENT_TIME'][:] = [(start_date - self._base_time).total_seconds()]
        open_nc.variables['TRAJECTORY'][:] = netcdf_string_to_vlen_bytes(f"{platform}_{start_time}")

    def _validate_glider_info(self, original_nc: nc.Dataset):
        if not hasattr(original_nc, 'wmo_platform_code'):
            raise GliderError('No wmo_platform_code', 2009)
        if 'PLATFORM_TYPE' not in original_nc.variables:
            raise GliderError('No PLATFORM_TYPE variable', 2010)
        if 'GLIDER_SERIAL_NO' not in original_nc.variables:
            raise GliderError('No GLIDER_SERIAL_NO variable', 2015)
        model_map = self._get_data_map('glider_models')
        ego_model_code = netcdf_bytes_to_string(original_nc.variables['PLATFORM_TYPE'][:])
        if ego_model_code.lower() not in model_map:
            raise GliderError(f'Unknown platform model: {ego_model_code}', 2011)
        return model_map[ego_model_code.lower()]

    def _build_glider_info(self, open_nc: nc.Dataset, original_nc: nc.Dataset, platform_name: str):
        model_info = self._validate_glider_info(original_nc)
        open_nc.variables['PLATFORM_NAME'][:] = netcdf_string_to_vlen_bytes(platform_name)
        open_nc.variables['WMO_IDENTIFIER'][:] = netcdf_string_to_vlen_bytes(original_nc.getncattr('wmo_platform_code'))
        open_nc.variables['PLATFORM_MODEL'][:] = netcdf_string_to_vlen_bytes(model_info['model'])
        serial_no = netcdf_bytes_to_string(original_nc.variables['GLIDER_SERIAL_NO'][:])
        open_nc.variables['PLATFORM_SERIAL_NUMBER'][:] = netcdf_string_to_vlen_bytes(f"{model_info['prefix']}{serial_no}")
        open_nc.variables['PLATFORM_MAKER'][:] = netcdf_string_to_vlen_bytes(model_info['maker'])
        if 'BATTERY_TYPE' in original_nc.variables:
            battery_type = netcdf_bytes_to_string(original_nc.variables['BATTERY_TYPE'][:])
            if battery_type:
                battery_types = self._get_data_map('battery_types')
                if battery_type.lower() not in battery_types:
                    raise GliderError(f'Unknown battery type: {battery_type}', 2016)
                open_nc.variables['BATTERY_TYPE'][:] = netcdf_string_to_vlen_bytes(battery_types[battery_type.lower()])
        else:
            self._log.warning(f'Missing BATTERY_TYPE variable')

        # BATTERY_PACKS??
        # FIRMWARE_VERSION_NAVIGATION
        # FIRMWAVE_VERSION_SCIENCE
        # GLIDER_MANUAL_VERSION
        if 'TRANS_SYSTEM' in original_nc.variables:
            trans_systems = set()
            trans_system_map = self._get_data_map('transmission_systems')
            for sys_name in original_nc.variables['TRANS_SYSTEM'][:]:
                sys_name = netcdf_bytes_to_string(sys_name)
                if not sys_name:
                    continue
                if sys_name.lower() not in trans_system_map:
                    raise GliderError(f'Unknown transmission system: {sys_name}', 2017)
                trans_systems.add(trans_system_map[sys_name.lower()])
            open_nc.variables['TELECOM_TYPE'][:] = netcdf_string_to_vlen_bytes(','.join(sorted(list(trans_systems))))
        else:
            self._log.warning('Missing variable TRANS_SYSTEM')
        if 'POSITIONING_SYSTEM' in original_nc.variables:
            track_systems = set()
            pos_system_map = self._get_data_map('positioning_systems')
            for sys_name in original_nc.variables['POSITIONING_SYSTEM'][:]:
                sys_name = netcdf_bytes_to_string(sys_name)
                if not sys_name:
                    continue
                if sys_name.lower() not in pos_system_map:
                    raise GliderError(f'Unknown positioning system: {sys_name}', 2018)
                track_systems.add(pos_system_map[sys_name.lower()])
            open_nc.variables['TRACKING_SYSTEM'][:] = netcdf_string_to_vlen_bytes(','.join(sorted(list(track_systems))))
        else:
            self._log.warning('Missing variable POSITIONING_SYSTEM')

    def _prevalidate_glider_system_names(self, original_nc: nc.Dataset):
        if 'BATTERY_TYPE' in original_nc.variables:
            battery_type = netcdf_bytes_to_string(original_nc.variables['BATTERY_TYPE'][:])
            if battery_type:
                battery_types = self._get_data_map('battery_types')
                if battery_type.lower() not in battery_types:
                    raise GliderError(f'Unknown battery type: {battery_type}', 2016)
        if 'TRANS_SYSTEM' in original_nc.variables:
            trans_system_map = self._get_data_map('transmission_systems')
            for sys_name in original_nc.variables['TRANS_SYSTEM'][:]:
                sys_name = netcdf_bytes_to_string(sys_name)
                if sys_name and sys_name.lower() not in trans_system_map:
                    raise GliderError(f'Unknown transmission system: {sys_name}', 2017)
        if 'POSITIONING_SYSTEM' in original_nc.variables:
            pos_system_map = self._get_data_map('positioning_systems')
            for sys_name in original_nc.variables['POSITIONING_SYSTEM'][:]:
                sys_name = netcdf_bytes_to_string(sys_name)
                if sys_name and sys_name.lower() not in pos_system_map:
                    raise GliderError(f'Unknown positioning system: {sys_name}', 2018)

    def _build_phase_info(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
        if 'PHASE' not in original_nc.variables:
            self._log.warning(f"No PHASE variable detected")
            return
        phase_data = original_nc.variables['PHASE'][:]
        new_phase_data = []
        new_phase_qc = []
        phase_map = self._get_data_map('og_phases')
        for phase_info in phase_data:
            phase_info = unnumpy(phase_info)
            if phase_info is None:
                new_phase_data.append(-128)
                new_phase_qc.append(-128)
            else:
                new_phase_data.append(int(phase_map[int(phase_info)]))
                new_phase_qc.append(0)
        open_nc.variables['PHASE'][:] = new_phase_data
        open_nc.variables['PHASE_QC'][:] = new_phase_qc

    def _validate_file_name(self, filename: str):
        _, mission_time, data_mode = self._parse_file_name(filename)
        if data_mode not in ('R', 'P', 'A', 'M', 'D'):
            raise ValueError(f'Bad data mode [{data_mode}]')
        if len(mission_time) == 8:
            _ = datetime.datetime.strptime(mission_time, '%Y%m%d')
        elif len(mission_time) == 10:
            _ = datetime.datetime.strptime(mission_time, '%Y%m%d%H')
        elif len(mission_time) == 12:
            _ = datetime.datetime.strptime(mission_time, '%Y%m%d%H%M')
        elif len(mission_time) == 14:
            _ = datetime.datetime.strptime(mission_time, '%Y%m%d%H%M%S')
        else:
            raise ValueError(f'Bad mission time [{mission_time}]')

    def validate_ego_glider_file(self, file: pathlib.Path, filename: t.Optional[str] = None):
        self._validate_file_name(filename or file.name)
        with nc.Dataset(file, 'r') as ds:
            self._validate_glider_info(ds)
            self._validate_deployment_info(ds)
            self._validate_contributors(ds)
            self._validate_build_times(ds)
            self._validate_build_depths(ds)
            self._validate_lon_lat(ds)
            self._prevalidate_glider_system_names(ds)



def validate_ego_glider_file(file: pathlib.Path, filename, metadata: dict):
    OpenGliderConverter.build().validate_ego_glider_file(file, filename)
    return True



