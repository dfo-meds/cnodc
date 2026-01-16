import gzip
import shutil

from cnodc.nodb import NODBControllerInstance, structures
from cnodc.process.payload_worker import FileWorkflowWorker
from cnodc.process.queue_worker import QueueWorker, QueueItemResult
import typing as t
from autoinject import injector
from cnodc.storage import StorageController, BaseStorageHandle
from cnodc.erddap import ErddapController
from cnodc.storage.base import StorageTier
from cnodc.util import CNODCError, HaltFlag
import tempfile
import pathlib
import netCDF4 as nc
import csv
import numpy as np
import math
import datetime

from cnodc.workflow.workflow import FilePayload

REF_TIME = datetime.datetime(1950, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)


METADATA_KEYS = {
    '% Device': ('device__appareil', 'string', ''),
    '% File name': ('profile_id', 'string', ''),
    '% Cast time (UTC)': ('time', 'datetime', -1),
    '% Cast time (local)': ('', ''),
    '% Sample type': ('sample_type__type_echantillon', 'string', ''),
    '% Cast data': ('processing__traitement', 'string', ''),
    '% Location source': ('location_source__source_emplacement', 'string', ''),
    '% Default latitude': ('', ''),
    '% Default altitude': ('', ''),
    '% Start latitude': ('start_lat__lat_depart', 'float', -200),
    '% Start longitude': ('start_lon__lon_depart', 'float', -200),
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
    '% Electronics calibration date': ('calib_date_electronics__date_etal_electronique', 'date', ''),
    '% Conductivity calibration date': ('calib_date_conductivity__date_etal_conductivite', 'date', ''),
    '% Temperature calibration date': ('calib_date_temperature__date_etal_temperature', 'date', ''),
    '% Pressure calibration date': ('calib_date_pressure__date_etal_pression', 'date', '')
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


class CastawayIntakeWorker(FileWorkflowWorker):

    storage: StorageController = None
    erddap: ErddapController = None

    def __init__(self, *args, **kwargs):
        super().__init__(
            process_name='castaway_ctd',
            process_version='1.0',
            defaults={
                'erddap_directory_raw': '',
                'erddap_directory_processed': '',
                'archive_directory_raw': '',
                'archive_directory_processed': '',
                'erddap_dataset_id': None,
                'gzip': True,
                'erddap_cluster': None
            },
            **kwargs
        )
        self._erddap: t.Optional[ErddapController] = None
        self._storage: t.Optional[StorageController] = None
        self._upload_target_raw: t.Optional[BaseStorageHandle] = None
        self._archive_target_raw: t.Optional[BaseStorageHandle] = None
        self._upload_target_processed: t.Optional[BaseStorageHandle] = None
        self._archive_target_processed: t.Optional[BaseStorageHandle] = None

    def on_start(self):
        if self.get_config("erddap_directory_raw") is None:
            raise CNODCError("ERDDAP upload directory not specified", "CASTAWAY", 2003)
        if self.get_config("archive_directory_raw") is None:
            raise CNODCError("Archive directory not specified", "CASTAWAY", 2004)
        if self.get_config("erddap_directory_processed") is None:
            raise CNODCError("ERDDAP upload directory not specified", "CASTAWAY", 2009)
        if self.get_config("archive_directory_processed") is None:
            raise CNODCError("Archive directory not specified", "CASTAWAY", 2010)
        self._upload_target_raw = self.storage.get_handle(self.get_config("erddap_directory_raw"), halt_flag=self._halt_flag)
        if not self._upload_target_raw.exists():
            raise CNODCError(f"Upload directory [{self._upload_target_raw}] does not exist", "CASTAWAY", 2005)
        self._archive_target_raw = self.storage.get_handle(self.get_config("archive_directory_raw"), halt_flag=self._halt_flag)
        if not self._archive_target_raw.exists():
            raise CNODCError(f"Archive directory [{self._archive_target_raw}] does not exist", "CASTAWAY", 2006)
        self._upload_target_processed = self.storage.get_handle(self.get_config("erddap_directory_processed"), halt_flag=self._halt_flag)
        if not self._upload_target_processed.exists():
            raise CNODCError(f"Upload directory [{self._upload_target_processed}] does not exist", "CASTAWAY", 2011)
        self._archive_target_processed = self.storage.get_handle(self.get_config("archive_directory_processed"), halt_flag=self._halt_flag)
        if not self._archive_target_processed.exists():
            raise CNODCError(f"Archive directory [{self._archive_target_processed}] does not exist", "CASTAWAY", 2012)

    def process_payload(self, payload: FilePayload) -> t.Optional[QueueItemResult]:
        # Download the original file
        temp_dir = self.temp_dir()
        local_file = payload.download(temp_dir)
        self._halt_flag.breakpoint()

        # Unpack the original file
        castaway_data = CastawayData(local_file)

        # Determine the file upload paths
        gzip_erddap = bool(self.get_config('gzip', True))
        upload_file_name = castaway_data.netcdf_file_name(gzip_erddap)
        archive_file_name = castaway_data.netcdf_file_name(True)
        upload_file: t.Optional[BaseStorageHandle] = None
        archive_file: t.Optional[BaseStorageHandle] = None
        if castaway_data.is_raw():
            upload_file = self._upload_target_raw.child(upload_file_name)
            archive_file = self._archive_target_raw.child(archive_file_name)
        else:
            upload_file = self._upload_target_processed.child(upload_file_name)
            archive_file = self._archive_target_processed.child(archive_file_name)

        # Check if the file already exists (currently an error)
        # TODO: Check overwrite flag?
        if upload_file.exists():
            raise CNODCError(f"Upload file already exists for this profile [{upload_file}]", "CASTAWAY", 2007)
        if archive_file.exists():
            raise CNODCError(f"Archive file already exists for this profile [{archive_file}]", "CASTAWAY", 2008)
        self._halt_flag.breakpoint()

        # Build the NetCDF file
        netcdf_file = temp_dir / "castaway.nc"
        castaway_data.build_netcdf_file(netcdf_file, self._halt_flag)
        self._halt_flag.breakpoint()

        # Gzip the NetCDF file
        gzip_netcdf_file = temp_dir / "castaway.nc.gz"
        with gzip.open(gzip_netcdf_file, "wb") as dest:
            with open(netcdf_file, "rb") as src:
                shutil.copyfileobj(src, dest)
        self._halt_flag.breakpoint()

        # Do the upload and rollback if there is an issue
        stage = 0
        try:
            upload_file.upload(
                netcdf_file if not gzip_erddap else gzip_netcdf_file,
                storage_tier=StorageTier.FREQUENT,
                metadata={
                    'Program': 'CASTAWAY_CTD',
                    'Dataset': 'RAW' if castaway_data.is_raw() else 'PROCESSED',
                    'CostUnit': 'MARITIMES',
                    'Gzip': 'Y' if gzip_erddap else 'N'
                }
            )
            self._halt_flag.breakpoint()
            # TODO LATER: save profile to database and trigger processing
            stage = 1
            archive_file.upload(
                gzip_netcdf_file,
                storage_tier=StorageTier.ARCHIVAL,
                metadata={
                    'Program': 'CASTAWAY_CTD',
                    'Dataset': 'RAW' if castaway_data.is_raw() else 'PROCESSED',
                    'CostUnit': 'MARITIMES',
                    'Gzip': 'Y'
                }
            )
            self._halt_flag.breakpoint()
            stage = 2
            if self.get_config('erddap_dataset_id'):
                self._erddap.reload_dataset(
                    self.get_config('erddap_dataset_id'),
                    cluster_name=self.get_config('erddap_cluster', None)
                )
            self._current_item.mark_complete(self._db)
            self._db.commit()
        except Exception as ex:
            if stage >= 1:
                upload_file.remove()
            if stage >= 2:
                archive_file.remove()
            raise ex
        return QueueItemResult.HANDLED


class CastawayData:

    def __init__(self, source_file: pathlib.Path):
        self._source_file = source_file
        self._data_load_flag: bool = False
        self._metadata: dict[str, str] = {}
        self._headers: list[str] = []
        self._data: list[list[str]] = []

    def _load_data(self):
        if not self._data_load_flag:
            header_count = None
            check_against = []
            with open(self._source_file, "r", encoding="utf-8") as h:
                reader = csv.reader(h)
                for line_no, line in enumerate(reader):
                    if state == "metadata":
                        if line[0].strip() == "%":
                            state = "header"
                            if '% Cast data' not in self._metadata:
                                raise CNODCError("Missing % Cast data parameter", "CASTAWAY", 1005)
                            check_against = RAW_VARIABLES if self._metadata['% Cast data'] == 'Raw' else PROCESSED_VARIABLES
                        elif line[0].strip() not in METADATA_KEYS:
                            raise CNODCError(f"Invalid metadata parameter name [{line[0]}] on line [{line_no}]", "CASTAWAY", 1000)
                        else:
                            self._metadata[line[0].strip()] = line[1].strip()
                    elif state == "header":
                        self._headers = line
                        for x in self._headers:
                            if x not in check_against:
                                raise CNODCError(f"Invalid variable name [{x}] on line [{line_no}]", "CASTAWAY", 1001)
                        state = "data"
                        header_count = len(self._headers)
                    elif state == "data":
                        if len(line) == 0:
                            state = "eof"
                        elif len(line) != header_count:
                            raise CNODCError(f"Missing data columns on line [{line_no}], expected [{header_count}] found [{len(line)}]", "CASTAWAY", 1002)
                        else:
                            self._data.append(line)
                    elif state == "eof":
                        if len(line) > 0:
                            raise CNODCError(f"Non-blank line on line [{line_no}] after data", "CASTAWAY", 1003)
                if state not in ("data", "eof"):
                    raise CNODCError(f"Missing headers", "CASTAWAY", 1004)

    def validate_file(self) -> bool:
        self._load_data()
        if '% File name' not in self._metadata:
            raise CNODCError("Missing % File name parameter", "CASTAWAY", 1006)
        if '% Cast time (UTC)' not in self._metadata:
            raise CNODCError("Missing % Cast time (UTC) parameter", "CASTAWAY", 1008)
        try:
            _ = datetime.datetime.strptime(self._metadata['% Cast time (UTC)'], '%m/%d/%Y %H:%M')
        except ValueError:
            raise CNODCError("Invalid date/time for cast time", "CASTAWAY", 1009)
        for x in ('% Electronics calibration date', '% Conductivity calibration date', '% Temperature calibration date', '% Pressure calibration date'):
            if x not in self._metadata:
                continue
            if self._metadata[x] == '':
                continue
            if self._metadata[x].count('-') == 2:
                try:
                    _ = datetime.datetime.strptime(self._metadata[x], '%Y-%m-%d')
                except ValueError:
                    raise CNODCError(f"Invalid date/time for [{x}], should be %Y-%m-%d", "CASTAWAY", 1010)
            elif self._metadata[x].count('/') == 2:
                try:
                    _ = datetime.datetime.strptime(self._metadata[x], '%m/%d/%Y')
                except ValueError:
                    raise CNODCError(f"Invalid date/time for [{x}], should be %m/%d/%Y", "CASTAWAY", 1011)
            else:
                raise CNODCError(f"Date field [{x}] does not have a recognizable %m/%d/%Y or %Y-%m-%d format", "CASTAWAY", 1007)
        return True

    def netcdf_file_name(self, gzipped: bool = False) -> str:
        self._load_data()
        suffix = "_raw" if self.is_raw() else '_processed'
        extension = '.nc' if not gzipped else '.nc.gz'
        return f"{self._metadata['% File name']}{suffix}{extension}"

    def is_raw(self) -> bool:
        self._load_data()
        return self._metadata['% Cast data'] == 'Raw'

    def build_netcdf_file(self, netcdf_file: pathlib.Path, halt_flag: HaltFlag = None):
        ncf = None
        try:
            self._load_data()
            if halt_flag: halt_flag.check_continue(True)
            ncf = self._create_netcdf_file(netcdf_file)
            if halt_flag: halt_flag.check_continue(True)
            self._load_castaway_data(ncf, halt_flag)
        finally:
            if ncf is not None:
                ncf.close()

    def _create_netcdf_file(self, netcdf_file: pathlib.Path) -> nc.Dataset:
        ncf = nc.Dataset(netcdf_file, "rb", format="NETCDF4")
        try:
            ncf.createDimension("profile", 1)
            ncf.createDimension("obs", None)
            self._create_variable(ncf, "profile_id", str, ("profile",), {
                'cf_role': 'profile_id',
            })
            self._create_variable(ncf, "device__appareil", str, ("profile",), {})
            self._create_variable(ncf, "time", "i8", ("profile",), {
                "axis": "T",
                "calendar": "gregorian",
                "units": "minutes since 1950-01-01 00:00",
                "_FillValue": -1,
                "valid_min": 0,
            })
            self._create_variable(ncf, "sample_type__type_echantillon", str, ("profile",), {})
            self._create_variable(ncf, "processing__traitement", str, ("profile",), {})
            self._create_variable(ncf, "location_source__source_emplacement", str, ("profile",), {})
            self._create_variable(ncf, "start_lat__lat_depart", "f8", ("profile",), {
                '_FillValue': float('nan'),
            })
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
            if self.is_raw():
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
        except Exception as ex:
            ncf.close()
            raise ex
        return ncf

    def _create_variable(self, ncf: nc.Dataset, var_name: str, data_type, dimensions: tuple, attributes: dict = None):
        if '_FillValue' in attributes:
            attributes['missing_value'] = attributes['_FillValue']
            var = ncf.createVariable(var_name, data_type, dimensions, fill_value=attributes.pop('_FillValue'))
        else:
            if 'missing_value' not in attributes and var_name in nc.default_fillvals:
                attributes['missing_value'] = nc.default_fillvals[var_name]
            var = ncf.createVariable(var_name, data_type, dimensions)
        var.setncattrs(attributes)
        return var

    def _load_castaway_data(self, ncf: nc.Dataset, halt_flag: HaltFlag = None):
        ncf.variables['row_size'][:] = [len(self._data)]
        for key in self._metadata:
            metadata_info = METADATA_KEYS[key]
            ncf.variables[metadata_info[0]][:] = self._convert_data([self._metadata[key]], *metadata_info[1:])
            if halt_flag: halt_flag.check_continue(True)
        raw_data = {k: [] for k in self._headers}
        for datum in self._data:
            for idx, k in enumerate(self._headers):
                raw_data[k].append(datum[idx])
                if halt_flag: halt_flag.check_continue(True)
        lookup = RAW_VARIABLES if self.is_raw() else PROCESSED_VARIABLES
        for key in raw_data:
            variable_info = lookup[key]
            ncf.variables[variable_info[0]][:] = self._convert_data(raw_data[key], *variable_info[1:])
            if halt_flag: halt_flag.check_continue(True)

    def _convert_data(self, raw_data, data_type, fill_value=None, *args):
        if data_type == 'string':
            return np.array([x if x is not None and x != '' else fill_value for x in raw_data], dtype='object')
        elif data_type == 'float':
            return [float(x) if x is not None and x != '' else fill_value for x in raw_data]
        elif data_type == 'int':
            return [int(x) if x is not None and x != '' else fill_value for x in raw_data]
        elif data_type == 'datetime':
            return [self._datetime_str_to_int(x, fill_value) for x in raw_data]
        elif data_type == 'date':
            return [self._date_to_str(x, fill_value) for x in raw_data]
        raise CNODCError(f"Data type [{data_type}] not supported", "CASTAWAY", 2002)

    def _datetime_str_to_int(self, val: t.Optional[str], fill_value=None):
        if val is None or val == '':
            return fill_value
        dt = datetime.datetime.strptime(val, '%m/%d/%Y %H:%M')
        dt = dt.replace(tzinfo=datetime.timezone.utc)
        # Minutes since REF_TIME (reduce towards 0 if seconds present)
        return int(math.floor((dt - REF_TIME).total_seconds() / 60.0))

    def _date_to_str(self, val: t.Optional[str], fill_value=None):
        if val is None or val == '':
            return fill_value
        dt = datetime.datetime.strptime(val, '%Y-%m-%d' if '-' in val else '%m/%d/%Y')
        if dt.year > 1600:
            return dt.strftime('%Y-%m-%d')
        else:
            return fill_value


def validate_castaway_ctd_file(data_file: pathlib.Path, headers: dict):
    file = CastawayData(data_file, headers['gzip'] if 'gzip' in headers else False)
    return file.validate_file()
