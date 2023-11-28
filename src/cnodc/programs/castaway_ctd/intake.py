from cnodc.nodb import NODBControllerInstance, structures
from cnodc.process.queue_worker import QueueWorker
import typing as t
from autoinject import injector
from cnodc.storage import StorageController
from cnodc.util import CNODCError
import tempfile
import pathlib
import netCDF4 as nc
import csv
import numpy as np
import datetime


REF_TIME = datetime.datetime(1950, 1, 1, 0, 0, 0)


METADATA_KEYS = {
    '% Device': ('device__appareil', 'string'),
    '% File name': ('profile_id', 'string'),
    '% Cast time (UTC)': ('time', 'datetime'),
    '% Cast time (local)': ('', ''),
    '% Sample type': ('sample_type__type_echantillon', 'string'),
    '% Cast data': ('processing__traitement', 'string'),
    '% Location source': ('location_source__source_emplacement', 'string'),
    '% Default latitude': ('', ''),
    '% Default altitude': ('', ''),
    '% Start latitude': ('start_lat__lat_depart', 'float'),
    '% Start longitude': ('start_lon__lon_depart', 'float'),
    '% Start altitude': ('start_altitude__altitude_depart', 'float'),
    '% Start GPS horizontal error(Meter)': ('start_horz_error__erreur_horz_depart', 'float'),
    '% Start GPS vertical error(Meter)': ('start_vert_error__erreur_vert_depart', 'float'),
    '% Start GPS number of satellites': ('start_no_satellites__no_satellites_depart', 'int'),
    '% End latitude': ('end_lat__lat_finale', 'float'),
    '% End longitude': ('end_lon__lon_finale', 'float'),
    '% End altitude': ('end_altitude__altitude_finale', 'float'),
    '% End GPS horizontal error(Meter)': ('end_horz_error__erreur_horz_finale', 'float'),
    '% End GPS vertical error(Meter)': ('end_vert_error__erreur_vert_finale', 'float'),
    '% End GPS number of satellites': ('end_no_satellites__no_satellites_finale', 'int'),
    '% Cast duration (Seconds)': ('duration__duree', 'float'),
    '% Samples per second': ('sampling_freq__freq_echantillonnage', 'int'),
    '% Electronics calibration date': ('calib_date_electronics__date_etal_electronique', 'date'),
    '% Conductivity calibration date': ('calib_date_conductivity__date_etal_conductivite', 'date'),
    '% Temperature calibration date': ('calib_date_temperature__date_etal_temperature', 'date'),
    '% Pressure calibration date': ('calib_date_pressure__date_etal_pression', 'date')
}

RAW_VARIABLES = {
    'Time (Seconds)': ('seconds__secondes', 'float'),
    'Pressure (Decibar)': ('pressure__pression', 'float'),
    'Temperature (Celsius)': ('temperature', 'float'),
    'Conductivity (MicroSiemens per Centimeter)': ('conductivity__conductivite', 'float'),
}

PROCESSED_VARIABLES = {
    'Pressure (Decibar)': ('pressure__pression', 'float'),
    'Depth (Meter)': ('depth', 'float'),
    'Temperature (Celsius)': ('temperature', 'float'),
    'Conductivity (MicroSiemens per Centimeter)': ('conductivity__conductivite', 'float'),
    'Specific conductance (MicroSiemens per Centimeter)': ('specific_conductance__conductance_specifique', 'float'),
    'Salinity (Practical Salinity Scale)': ('salinity__salinite', 'float'),
    'Sound velocity (Meters per Second)': ('sound_velocity__vitesse_son', 'float'),
    'Density (Kilograms per Cubic Meter)': ('density__densite', 'float')
}


class CastawayIntakeWorker(QueueWorker):

    storage: StorageController = None

    def __init__(self, *args, **kwargs):
        super().__init__("cnodc.castaway.nc", *args, **kwargs)
        self._storage: t.Optional[StorageController] = None

    @injector.inject
    def on_start(self, storage: StorageController = None):
        self._storage = storage

    def process_queue_item(self,
                            item: structures.NODBQueueItem) -> t.Optional[structures.QueueItemResult]:
        if 'upload_file' not in item.data or not item.data['upload_file']:
            raise CNODCError("Missing [upload_file] in queue item", "CASTAWAY", 2000)
        file_handle = self._storage.get_handle(item.data['upload_file'])
        if not file_handle.exists():
            raise CNODCError(f"Upload file [{item.data['upload_file']} no longer exists", "CASTAWAY", 2001)
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = pathlib.Path(temp_dir)
            local_file = temp_dir / "castaway.csv"
            file_handle.download(local_file, halt_flag=self.halt_flag)
            self.halt_flag.check_continue(True)
            castaway_ctd_data = parse_castaway_ctd_file(local_file)
            self.halt_flag.check_continue(True)
            netcdf_file = temp_dir / "castaway.nc"
            ncf = None
            try:
                ncf = self._create_netcdf_file(netcdf_file, castaway_ctd_data['metadata']['% Cast data'] == 'Raw')
                self.halt_flag.check_continue(True)
                self._load_castaway_data(castaway_ctd_data, ncf)
                ncf.close()
                ncf = None
                self.halt_flag.check_continue(True)

            finally:
                if ncf is not None:
                    ncf.close()
                    ncf = None

    def _create_netcdf_file(self, netcdf_file: pathlib.Path, for_raw_data: bool) -> nc.Dataset:
        ncf = nc.Dataset(netcdf_file, "rb", format="NETCDF4")
        ncf.createDimension("profile", 1)
        ncf.createDimension("obs", None)
        self._create_variable(ncf, "profile_id", str, ("profile",), {
            'cf_role': 'profile_id',
        })
        self._create_variable(ncf, "device__appareil", str, ("profile",), {})
        self._create_variable(ncf, "time", "i4", ("profile",), {
            "axis": "T",
            "calendar": "gregorian"
        })
        self._create_variable(ncf, "sample_type__type_echantillon", str, ("profile",), {})
        self._create_variable(ncf, "processing__traitement", str, ("profile",), {})
        self._create_variable(ncf, "location_source__source_emplacement", str, ("profile",), {})
        self._create_variable(ncf, "start_lat__lat_depart", "f8", ("profile",), {})
        self._create_variable(ncf, "start_lon__lon_depart", "f8", ("profile",), {})
        self._create_variable(ncf, "start_altitude__altitude_depart", "f8", ("profile",), {})
        self._create_variable(ncf, "start_horz_error__erreur_horz_depart", "f8", ("profile",), {})
        self._create_variable(ncf, "start_vert_error__erreur_vert_depart", "f8", ("profile",), {})
        self._create_variable(ncf, "start_no_satellites__no_satellites_depart", "i4", ("profile",), {})
        self._create_variable(ncf, "end_lat__lat_finale", "f8", ("profile",), {})
        self._create_variable(ncf, "end_lon__lon_finale", "f8", ("profile",), {})
        self._create_variable(ncf, "end_altitude__altitude_finale", "f8", ("profile",), {})
        self._create_variable(ncf, "end_horz_error__erreur_horz_finale", "f8", ("profile",), {})
        self._create_variable(ncf, "end_vert_error__erreur_vert_finale", "f8", ("profile",), {})
        self._create_variable(ncf, "end_no_satellites__no_satellites_finale", "i4", ("profile",), {})
        self._create_variable(ncf, "duration__duree", "f8", ("profile", ), {})
        self._create_variable(ncf, "sampling_freq__freq_echantillonnage", "i4", ("profile",), {})
        self._create_variable(ncf, "calib_date_electronics__date_etal_electronique", str, ("profile",), {})
        self._create_variable(ncf, "calib_date_conductivity__date_etal_conductivite", str, ("profile",), {})
        self._create_variable(ncf, "calib_date_temperature__date_etal_temperature", str, ("profile",), {})
        self._create_variable(ncf, "calib_date_pressure__date_etal_pression", str, ("profile",), {})
        self._create_variable(ncf, "latitude", "f8", ("profile",), {})
        self._create_variable(ncf, "longitude", "f8", ("profile",), {})
        self._create_variable(ncf, "row_size", "i4", ("profile",), {
            'sample_dimension': 'obs',
        })
        self._create_variable(ncf, 'pressure__pression', 'f8', ('obs',), {
            'units': 'Pa',
        })
        self._create_variable(ncf, "temperature", "f8", ("obs",), {})
        self._create_variable(ncf, "conductivity_conductivite", "f8", ("obs", ), {})
        if for_raw_data:
            self._create_variable(ncf, 'seconds__secondes', 'f8', ('obs',), {
                'units': 's'
            })
        else:
            self._create_variable(ncf, "depth", "f8", ("obs",), {
                'axis': 'Z',
                'positive': 'down',
                'units': 'm'
            })
            self._create_variable(ncf, "specific_conductance__conductance_specifique", "f8", ("obs", ), {})
            self._create_variable(ncf, "salinity__salinite", "f8", ("obs", ), {})
            self._create_variable(ncf, "sound_velocity__vitesse_son", "f8", ("obs",), {})
            self._create_variable(ncf, "density__densite", "f8", ("obs",), {})
        return ncf

    def _create_variable(self, ncf: nc.Dataset, var_name: str, data_type, dimensions: tuple, attributes: dict = None):
        var = ncf.createVariable(var_name, data_type, dimensions)
        var.setncattrs(attributes)
        return var

    def _load_castaway_data(self, ctd_data: dict, ncf: nc.Dataset):
        ncf.variables['row_size'][:] = [len(ctd_data['data'])]
        for key in ctd_data['metadata']:
            metadata_info = METADATA_KEYS[key]
            ncf.variables[metadata_info[0]][:] = self._convert_data([ctd_data['metadata'][key]], *metadata_info[1:])
        raw_data = {k: [] for k in ctd_data['headers']}
        for datum in ctd_data['data']:
            for idx, k in enumerate(ctd_data['headers']):
                raw_data[k].append(datum[idx])
        lookup = RAW_VARIABLES if ctd_data['metadata']['% Cast data'] == 'Raw' else PROCESSED_VARIABLES
        for key in raw_data:
            variable_info = lookup[key]
            ncf.variables[variable_info[0]][:] = self._convert_data(raw_data[key], *variable_info[1:])

    def _convert_data(self, raw_data, data_type, *args):
        if data_type == 'string':
            return np.array(raw_data, dtype='object')
        elif data_type == 'float':
            return [float(x) if x is not None and x != '' else None for x in raw_data]
        elif data_type == 'int':
            return [int(x) if x is not None and x != '' else None for x in raw_data]
        elif data_type == 'datetime':
            return [self._datetime_str_to_int(x) for x in raw_data]
        elif data_type == 'date':
            return [self._date_to_str(x) for x in raw_data]
        raise CNODCError(f"Data type [{data_type}] not supported", "CASTAWAY", 2002)

    def _datetime_str_to_int(self, val):
        raise NotImplementedError()
        # TODO: typically mm/dd/yyyy HH:MM
        # makes an int (minutes since REF_TIME)

    def _date_to_str(self, val):
        raise NotImplementedError()
        # TODO: typically YYYY-MM-DD or mm/dd/yyyy (watch for YYYY in far past as null)
        # makes a string


def validate_castaway_ctd_file(data_file: pathlib.Path, headers: dict):
    data = parse_castaway_ctd_file(data_file)
    # TODO: verify all the columns are present? and appropriate columns for the % Cast data value?
    return True


def parse_castaway_ctd_file(data_file: pathlib.Path) -> dict:
    state = "metadata"
    ctd_file = {
        'metadata': {},
        'headers': [],
        'data': []
    }
    header_count = None
    check_against = []
    with open(data_file, "r", encoding="utf-8") as h:
        reader = csv.reader(h)
        for line_no, line in enumerate(reader):
            if state == "metadata":
                if line[0].strip() == "%":
                    state = "header"
                    if '% Cast data' not in ctd_file['metadata']:
                        raise CNODCError("Missing % Cast data parameter", "CASTAWAY", 1005)
                    check_against = RAW_VARIABLES if ctd_file['metadata']['% Cast data'] == 'Raw' else PROCESSED_VARIABLES
                elif line[0].strip() not in METADATA_KEYS:
                    raise CNODCError(f"Invalid metadata parameter name [{line[0]}] on line [{line_no}]", "CASTAWAY", 1000)
                else:
                    ctd_file['metadata'][line[0].strip()] = line[1].strip()
            elif state == "header":
                ctd_file['headers'] = line
                for x in ctd_file['headers']:
                    if x not in check_against:
                        raise CNODCError(f"Invalid variable name [{x}] on line [{line_no}]", "CASTAWAY", 1001)
                state = "data"
                header_count = len(ctd_file['headers'])
            elif state == "data":
                if len(line) == 0:
                    state = "eof"
                elif len(line) != header_count:
                    raise CNODCError(f"Missing data columns on line [{line_no}], expected [{header_count}] found [{len(line)}]", "CASTAWAY", 1002)
                else:
                    ctd_file['data'].append(line)
            elif state == "eof":
                if len(line) > 0:
                    raise CNODCError(f"Non-blank line on line [{line_no}] after data", "CASTAWAY", 1003)
    if state not in ("data", "eof"):
        raise CNODCError(f"Missing headers", "CASTAWAY", 1004)
    return ctd_file
