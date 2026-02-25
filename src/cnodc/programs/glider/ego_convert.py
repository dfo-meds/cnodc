import datetime
import math
import typing as t
import pathlib
from copy import copy

import numpy as np
import yaml
import netCDF4 as nc

from cnodc.ocproc2.codecs.netcdf import NetCDFCommonDecoderError
from cnodc.programs.dmd.metadata import GCContentType, GCContentFormat
from cnodc.util import CNODCError, dynamic_object, normalize_string, unnumpy
import cnodc.science.seawater as seawater
import cnodc.programs.dmd.metadata as metadata
from cnodc.util.sanitize import netcdf_bytes_to_string, str_to_netcdf_vlen


def ego_old_sensor_info(original_nc: nc.Dataset, sensor_map: dict[str, dict[str, str]]):
    sensors = {}
    sensors_seen = set()
    param_map = {}
    for var_name in original_nc.variables:
        var = original_nc.variables[var_name]
        var_attrs = [x for x in var.ncattrs()]
        if 'sensor_name' in var_attrs:
            continue
        sensor_full_name = normalize_string(getattr(var, 'sensor_name').strip().lower())
        if sensor_full_name not in sensor_map:
            raise NetCDFCommonDecoderError(f"Unknown sensor [{sensor_full_name}]", 3000)
        info: dict = copy(sensor_map[sensor_full_name])
        info['serial'] = getattr(var, 'sensor_serial_number') if 'sensor_serial_number' in var_attrs else 'unknown'
        key = f"SENSOR_{info['type']}_{info['serial']}"
        if key in sensors_seen:
            continue
        sensors_seen.add(key)
        sensors[key] = info
        param_map[var.name] = key
    return sensors, param_map


def ego_new_sensor_info(original_nc: nc.Dataset):
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
            raise NetCDFCommonDecoderError(f"Unknown glider instrument type: {sensor_names[x]}", 3001)
        key = f"SENSOR_{sensor_type}_{sensor_serials[x]}"
        for idx, val in enumerate(param_sensors):
            if val == sensor_names[x]:
                param_map[param_names[idx]] = key
        if key not in sensor_info:
            sensor_info[key] = {
                'type': sensor_type,
                'make': sensor_makers[x],
                'model': sensor_models[x],
                'serial': sensor_serials[x],
                'location': sensor_mounts[x].lower().replace('_', ' ') + ('' if not sensor_orientations[x] else ' - ' + sensor_orientations[x].lower()),
            }
    return sensor_info, param_map


class OpenGliderConverter:

    @staticmethod
    def build(map_file: t.Optional[pathlib.Path] = None, halt_flag=None):
        if map_file is None:
            map_file = pathlib.Path(__file__).absolute().parent / 'ego_conversion.yaml'
        with open(map_file, encoding="utf-8") as h:
            return OpenGliderConverter(yaml.safe_load(h), halt_flag)

    def __init__(self, mapping_data, halt_flag=None):
        self._mapping_data = mapping_data
        self._data_maps = {}
        self._base_time = datetime.datetime.fromisoformat('1970-01-01T00:00:00')
        self._halt = halt_flag

    def breakpoint(self):
        if self._halt is not None:
            self._halt.breakpoint()

    def build_metadata(self, open_file, file_name: str) -> metadata.DatasetMetadata:
        with nc.Dataset(open_file, "r") as ds:
            return self._build_metadata(ds, file_name)

    def _build_metadata(self, ds: nc.Dataset, file_name: str):
            dmd = metadata.DatasetMetadata()
            dmd.set_meds_defaults()
            dmd.set_from_netcdf_file(ds)
            dmd.set_processing_info("real-time")
            mission_id = ds.getncattr('id')
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
            for var in dmd.get_variables():
                if var.get_source_name() in ('LATITUDE', 'LONGITUDE', 'DEPTH', 'TIME'):
                    var.set_destination_name(var.get_source_name().lower())
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
        with nc.Dataset(ego_file, "r") as original_nc:
            with nc.Dataset(og_file, "w", format="NETCDF4") as open_nc:
                return self._convert(original_nc, open_nc, file_name or pathlib.Path(ego_file).name)

    def _convert(self, original_nc: nc.Dataset, open_nc: nc.Dataset, file_name: str):
        platform, start_time, data_mode = file_name[:-3].rsplit('_', maxsplit=2)
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
        open_nc.setncattr('date_created', datetime.datetime.now().strftime('%Y%m%dT%H%M%SZ'))
        mission_id = open_nc.getncattr('id')
        return file_name, mission_id

    def _create_dimensions(self, open_nc: nc.Dataset):
        open_nc.createDimension("N_MEASUREMENTS", None)

    def _map_static_metadata(self, open_nc: nc.Dataset):
        for key in self._mapping_data['static_metadata']:
            open_nc.setncattr(key, self._mapping_data['static_metadata'][key])

    def _copy_metadata(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
        attrs = [x for x in original_nc.ncattrs()]
        for key in self._mapping_data['copy_metadata']:
            if key in attrs:
                open_nc.setncattr(key, original_nc.getncattr(self._mapping_data['copy_metadata'][key]))

    def _set_metadata_from_file_name(self, open_nc: nc.Dataset, platform, start_time, data_mode):
        if data_mode == 'R':
            open_nc.setncattr('rtqc_method', 'Real-time QC performed with Coriolis matlab toolbox')
            open_nc.setncattr('rtqc_method_fr', 'Contrôle qualité en temps réel réalisé avec la boîte à outils Coriolis Matlab')
            open_nc.setncattr("summary", f"Real-time data from glider mission {platform}_{start_time}")
            open_nc.setncattr("summary_fr", f"Données en temps réel de la mission du planeur {platform}_{start_time}")
        else:
            open_nc.setncattr('rtqc_method', 'No QC applied')
            open_nc.setncattr('rtqc_method_fr', 'Aucun contrôle qualité appliqué')
            open_nc.setncattr("summary", f"Preliminary data from glider mission {platform}_{start_time}")
            open_nc.setncattr("summary_fr",f"Données préliminaire de la mission du planeur {platform}_{start_time}")
        open_nc.setncattr('title', f'Glider {platform} - {start_time} ({'Real Time' if data_mode == 'R' else 'Preliminary'})')
        open_nc.setncattr('title_fr', f'PLaneur {platform} - {start_time} ({'temps réel' if data_mode == 'R' else 'préliminaire'})')
        open_nc.setncattr('id', f"{platform}_{start_time}_{data_mode}")

    def _set_geospatial_bounds_metadata(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
        latitudes = original_nc.variables['LATITUDE'][:]
        open_nc.setncattr('geospatial_lat_min', np.min(latitudes))
        open_nc.setncattr('geospatial_lat_max', np.max(latitudes))
        longitudes = original_nc.variables['LONGITUDE'][:]
        open_nc.setncattr('geospatial_lon_min', np.min(longitudes))
        open_nc.setncattr('geospatial_lon_max', np.max(longitudes))

    def _set_sensor_metadata(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
        if 'PARAMETER' not in original_nc.variables:
            sensor_info, param_map = ego_old_sensor_info(original_nc, self._get_data_map('sensors'))
        else:
            sensor_info, param_map = ego_new_sensor_info(original_nc)
        self._create_openglider_sensor_vars(open_nc, sensor_info)
        return param_map

    def _create_openglider_sensor_vars(self, open_nc: nc.Dataset, sensors: dict[str, dict[str, str]]):
        for key in sensors:
            info = sensors[key]
            var = open_nc.createVariable(
        f'SENSOR_{info['type']}_{info['serial']}',
        'f4'
            )
            setattr(var, 'long_name', f"{info['make']} {info['model']}")
            setattr(var, 'sensor_model', info['model'])
            setattr(var, 'sensor_maker', info['make'])
            setattr(var, 'sensor_serial_number', info['serial'])
            self.breakpoint()

    def _build_variables(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
        for var_name in self._mapping_data['variables']:
            var_config = self._mapping_data['variables'][var_name]
            if 'copy_data_from' in var_config and var_config['copy_data_from'] and not var_config['copy_data_from'] in original_nc.variables:
                continue
            self._create_variable(open_nc, original_nc, var_name, var_config)

    def _create_variable(self, open_nc: nc.Dataset, original_nc: nc.Dataset, var_name: str, var_config: dict):
        var = open_nc.createVariable(
            var_name,
            str if var_config['type'] == 'str' else var_config['type'],
            var_config['dimensions'] if 'dimensions' in var_config and var_config['dimensions'] else None,
        )
        if var_config['attributes']:
            for attr_name in var_config['attributes']:
                setattr(var, attr_name, var_config['attributes'][attr_name])
        if 'copy_data_from' in var_config and var_config['copy_data_from']:
            copy_from = var_config['copy_data_from']
            if copy_from in original_nc.variables:
                original_var = original_nc.variables[copy_from]
                original_data = original_var[:]
                if 'data_processor' in var_config and var_config['data_processor'] is not None:
                    data_processor = dynamic_object(var_config['data_processor'])
                    temp_data = []
                    for x in original_data:
                        temp_data.append(data_processor(x))
                    original_data = temp_data
                var[:] = original_data
                if 'copy_attributes' in var_config and var_config['copy_attributes']:
                    for attr_name in var_config['copy_attributes']:
                        setattr(var, attr_name, getattr(original_var, attr_name))
        return var

    def _build_parameters(self, open_nc: nc.Dataset, original_nc: nc.Dataset, sensor_map: dict[str, str]):
        for param_name in self._mapping_data['parameters']:
            param_config = self._mapping_data['parameters'][param_name]
            if param_config['copy_data_from'] not in original_nc.variables:
                continue
            data = original_nc.variables[param_config['copy_data_from']][:]
            if all(math.isnan(x) for x in data):
                continue
            var = self._create_variable(open_nc, original_nc, param_name, param_config)
            setattr(var, 'coordinates', "TIME,LONGITUDE,LATITUDE,DEPTH")
            if param_name in sensor_map:
                setattr(var, 'sensor', sensor_map[param_name])
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
                        '_FillValue': -127,
                        'valid_min': 0,
                        'valid_max': 0,
                },
                    'copy_attributes': ['flag_values', 'flag_meanings'],
                })
                setattr(var, 'ancillary_variables', f'{param_name}_QC')
            self.breakpoint()

    def _build_depths(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
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
                depth = seawater.eos80_depth(pressure, latitude)
                if min_depth is None or depth < min_depth:
                    min_depth = depth
                if max_depth is None or depth > max_depth:
                    max_depth = depth
                depths.append(depth)
        if min_depth is not None or max_depth is not None:
            open_nc.setncattr('geospatial_vertical_min', min_depth)
            open_nc.setncattr('geospatial_vertical_max', max_depth)
        open_nc.variables['DEPTH'][:] = depths

    def _build_times(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
        juld_var = original_nc.variables['JULD']
        period, _, epoch = getattr(juld_var, 'units').split(' ', maxsplit=2)
        local_base_time = datetime.datetime.fromisoformat(epoch)
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
            open_nc.setncattr('time_coverage_start', min_time.strftime('%Y%m%dT%H%M%SZ'))
            open_nc.setncattr('time_coverage_end', max_time.strftime('%Y%m%dT%H%M%SZ'))

    def _get_data_map(self, name):
        if name not in self._data_maps:
            self._data_maps[name] = {}
            if name in self._mapping_data['data_maps'] and self._mapping_data['data_maps'][name] and isinstance(self._mapping_data['data_maps'][name], dict):
                self._data_maps[name] = self._mapping_data['data_maps'][name]
        return self._data_maps[name]

    def _build_contact_info(self, contact_name, role) -> tuple[str, str, str, str]:
        info = {}
        contact_map = self._get_data_map('contacts')
        if contact_name.lower() in contact_map:
            info.update(contact_map[contact_name.lower()])
        return (
            info['proper_name'] if 'proper_name' in info else contact_name,
            info['id'] if 'id' in info else '',
            info['email'] if 'email' in info else '',
            role
        )

    def _build_contributors(self, open_nc: nc.Dataset, original_nc: nc.Dataset):
        contributors = []
        institutions = []

        if hasattr(original_nc, 'principal_investigator'):
            for pi in original_nc.getncattr('principal_investigator').split(';'):
                contributors.append(self._build_contact_info(pi.strip(), 'CONT0004'))
        for op in netcdf_bytes_to_string(original_nc.variables['OPERATING_INSTITUTION'][:]).split(';'):
            institutions.append(self._build_contact_info(op.strip(), 'CONT0003'))
        for owner in netcdf_bytes_to_string(original_nc.variables['GLIDER_OWNER'][:]).split(';'):
            institutions.append(self._build_contact_info(owner.strip(), 'CONT0002'))
        for inst in institutions:
            if inst[0] == 'C-PROOF':
                # CPROOF
                open_nc.setncattr('infoUrl', 'https://cproof.uvic.ca/')
                break
            elif inst[0] == 'CEOTR':
                # CEOTR
                open_nc.setncattr('infoUrl', 'https://ceotr.ocean.dal.ca/gliders/')
                break
            elif inst[0] == 'BIO':
                # BIO
                open_nc.setncattr('infoUrl', '')
        else:
            mission_id = open_nc.getncattr('id')
            network = open_nc.getncattr('network') if open_nc.has_attribute('network') else ''
            if 'C-PROOF' in network or any(mission_id.startswith(x) for x in ('hal_1002', 'k_999', 'marvin_1003', 'rosie_713', 'Wall_E_652', 'mike_rorider',  'SEA035', 'SEA046')):
                # CRPOOF
                open_nc.setncattr("infoUrl", "https://cproof.uvic.ca/")
            elif any(mission_id.startswith(x) for x in ('pearldiver', 'sunfish', 'Unit_334', 'unit_473')):
                # MEMORIAL
                open_nc.setncattr('infoUrl', 'https://www.mun.ca/creait/autonomous-ocean-systems-centre/gliders--small-auvs/')
            elif any(mission_id.startswith(x) for x in ('SEA019', 'SEA021', 'SEA022', 'SEA024', 'SEA032')):
                # BIO
                open_nc.setncattr('infoUrl', '')


        open_nc.setncattr('contributor_name', ','.join(c[0] for c in contributors))
        open_nc.setncattr('contributor_email', ','.join(c[1] for c in contributors))
        open_nc.setncattr('contributor_id', ','.join(c[2] for c in contributors))
        open_nc.setncattr('contributor_id_vocabulary', 'https://orcid.org/')
        open_nc.setncattr('contributor_role', ','.join(c[3] for c in contributors))
        open_nc.setncattr('contributor_role_vocabulary','https://vocab.nerc.ac.uk/collection/W08/current/')
        open_nc.setncattr('contributing_institutions', ','.join(c[0] for c in institutions))
        open_nc.setncattr('contributing_institutions_id', ','.join(c[1] for c in institutions))
        open_nc.setncattr('contributing_institutions_id_vocabulary', 'https://ror.org/')
        open_nc.setncattr('contributing_institutions_role', ','.join(c[3] for c in institutions))
        open_nc.setncattr('contributing_institutions_role_vocabulary', 'https://vocab.nerc.ac.uk/collection/W08/current/')

    def _build_deployment_info(self, open_nc: nc.Dataset, original_nc: nc.Dataset, platform: str, start_time: str):
        deploy_start = netcdf_bytes_to_string(original_nc.variable['DEPLOYMENT_START_DATE'][:]).strip()
        if len(deploy_start) == 8:
            start_date = datetime.datetime.strptime(deploy_start, '%Y%m%d')
        elif len(deploy_start) == 12:
            start_date = datetime.datetime.strptime(deploy_start, '%Y%m%d%H%M')
        elif len(deploy_start) == 14:
            start_date = datetime.datetime.strptime(deploy_start, '%Y%m%d%H%M%S')
        else:
            raise CNODCError(f"Unknown date format for [{deploy_start}]", 'EGO_CONVERT', 1002)
        open_nc.setncattr('start_date', start_date.strftime('%Y%m%dT%H%M%SZ'))
        open_nc.variables['TRAJECTORY'][:] = str_to_netcdf_vlen(f"{platform}_{start_time}")
        open_nc.variables['DEPLOYMENT_TIME'][:] = [(start_date - self._base_time).total_seconds()]

    def _build_glider_info(self, open_nc: nc.Dataset, original_nc: nc.Dataset, platform_name: str):
        open_nc.variables['PLATFORM_NAME'][:] = str_to_netcdf_vlen(platform_name)
        open_nc.variables['WMO_IDENTIFIER'][:] = str_to_netcdf_vlen(original_nc.getncattr('wmo_platform_code'))
        ego_model_code = netcdf_bytes_to_string(original_nc.variables['PLATFORM_TYPE'][:])
        model_map = self._get_data_map('glider_models')
        if ego_model_code.lower() not in model_map:
            raise CNODCError(f'Unknown platform model: {ego_model_code}', 'EGO_CONVERT', 1003)
        model_info = model_map[ego_model_code.lower()]
        open_nc.variables['PLATFORM_MODEL'][:] = str_to_netcdf_vlen(model_info['model'])
        serial_no = netcdf_bytes_to_string(original_nc.variables['GLIDER_SERIAL_NO'][:])
        open_nc.variables['PLATFORM_SERIAL_NUMBER'][:] = str_to_netcdf_vlen(f"{model_info['prefix']}{serial_no}")
        open_nc.variables['PLATFORM_MAKER'][:] = str_to_netcdf_vlen(model_info['maker'])
        battery_type = netcdf_bytes_to_string(original_nc.variables['BATTERY_TYPE'][:])
        if battery_type:
            battery_types = self._get_data_map('battery_types')
            if battery_type.lower() not in battery_types:
                raise CNODCError(f'Unknown battery type: {battery_type}', 'EGO_CONVERT', 1004)
            open_nc.variables['BATTERY_TYPE'][:] = str_to_netcdf_vlen(battery_types[battery_type.lower()])
        # BATTERY_PACKS??
        # FIRMWARE_VERSION_NAVIGATION
        # FIRMWAVE_VERSION_SCIENCE
        # GLIDER_MANUAL_VERSION
        trans_systems = set()
        trans_system_map = self._get_data_map('transmission_systems')
        for sys_name in original_nc.variable['TRANS_SYSTEM'][:]:
            sys_name = netcdf_bytes_to_string(sys_name)
            if not sys_name:
                continue
            if sys_name.lower() not in trans_system_map:
                raise CNODCError(f'Unknown transmission system: {sys_name}', 'EGO_CONVERT', 1005)
            trans_systems.add(trans_system_map[sys_name.lower()])
        open_nc.variables['TELECOM_TYPE'] = str_to_netcdf_vlen(','.join(trans_systems))
        track_systems = set()
        pos_system_map = self._get_data_map('positioning_systems')
        for sys_name in original_nc.variable['POSITIONING_SYSTEM'][:]:
            sys_name = netcdf_bytes_to_string(sys_name)
            if not sys_name:
                continue
            if sys_name.lower() not in pos_system_map:
                raise CNODCError(f'Unknown positioning system: {sys_name}', 'EGO_CONVERT', 1006)
            track_systems.add(pos_system_map[sys_name.lower()])
        open_nc.variables['TRACKING_SYSTEM'][:] = str_to_netcdf_vlen(','.join(track_systems))

    def _build_phase_info(self, original_nc: nc.Dataset, open_nc: nc.Dataset):
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


def validate_glider_file(file: pathlib.Path, metadata: dict):
    return True


