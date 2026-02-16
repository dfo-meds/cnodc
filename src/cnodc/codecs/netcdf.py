import datetime
import functools
import logging
import math
import pathlib

import yaml

from cnodc.codecs.base import BaseCodec, ByteIterable, DecodeResult
import typing as t
import netCDF4 as nc
from autoinject import injector

from cnodc.units.units import UnitConverter
from cnodc.util import unnumpy

from cnodc.ocproc2 import ParentRecord, SingleElement
from cnodc.ocproc2.ontology import OCProc2Ontology
from cnodc.util import CNODCError
from cnodc.netcdf.wrapper import Dataset
from draft_work.src.cnodc.util import dynamic_object


class NetCDFDecoder(BaseCodec):
    """ Generic decoder for NetCDF files. """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, is_decoder=True, **kwargs)

    def _decode(self, data: ByteIterable, **kwargs) -> t.Iterable[DecodeResult]:
        nc_data = b''.join(data)
        try:
            with Dataset('inmemory.nc', "r", memory=nc_data) as netcdf:
                yield DecodeResult(records=self._build_from_netcdf(netcdf, **kwargs))
        except Exception as ex:
            yield DecodeResult(exc=ex)

    def _build_from_netcdf(self, dataset: nc.Dataset, **kwargs) -> t.Optional[list[ParentRecord]]:
        raise NotImplementedError


class NetCDFCommonDecoderError(CNODCError):

    def __init__(self, text, number, is_recoverable: bool = False):
        super().__init__(text, 'NETCDF_COMMON_DECODE', number, is_recoverable)


class NetCDFCommonDecoder(NetCDFDecoder):
    """ Glider decoder for the EGO format. """

    def __init__(self, *args, **kwargs):
        super().__init__(log_name="cnodc.netcdf_common_decoder", *args, **kwargs)

    def _build_from_netcdf(self, dataset: Dataset, **kwargs):
        if 'mapping_class' in kwargs:
            map_cls = dynamic_object(kwargs.pop('mapping_class'))
        else:
            map_cls = NetCDFCommonMapper
        if 'mapping_file' in kwargs:
            mapping_file = kwargs.pop('mapping_file')
        elif hasattr(map_cls, 'DEFAULT_MAPPING_FILE'):
            mapping_file = map_cls.DEFAULT_MAPPING_FILE
        else:
            raise NetCDFCommonDecoderError("Missing [mapping_file] keyword", 2000, False)
        mapper = map_cls(dataset, pathlib.Path(mapping_file))
        return [r for r in mapper.build_records()]


class NetCDFCommonMapper:

    units: UnitConverter = None
    ontology: OCProc2Ontology = None

    @injector.construct
    def __init__(self, dataset: Dataset, mapping_file: pathlib.Path, log_name: str = "cnodc.netcdf.common_mapper"):
        self._map_file = mapping_file
        self._dataset = dataset
        self._data = None
        self._cache = {}
        self._log = logging.getLogger(log_name)

    def _load_data(self):
        if self._data is None:
            if not self._map_file.exists():
                raise NetCDFCommonDecoderError(f"No mapping file found at [{self._map_file}]", 1000, True)
            with open(self._map_file, "r") as h:
                self._data = yaml.safe_load(h)
            if not isinstance(self._data, dict):
                raise NetCDFCommonDecoderError("Mapping file is not a YAML dictionary", 1001, True)
            for dict_key in ('variable_map', 'attribute_map', 'single_variable_map'):
                if dict_key in self._data and not isinstance(self._data[dict_key], dict):
                    raise NetCDFCommonDecoderError("variable_map must be a dict if provided", 1009, True)
                elif dict_key not in self._data:
                    self._data[dict_key] = {}
                for key in self._data[dict_key]:
                    if not isinstance(self._data[dict_key][key], dict):
                        self._data[dict_key][key] = {
                            'source': key,
                            'target': self._data[dict_key][key]
                        }
                    if 'source' not in self._data[dict_key][key]:
                        self._data[dict_key][key]['source'] = key
            self._on_data_load()

    def _on_data_load(self):
        pass

    def _get_key_variable(self) -> str:
        if 'key_variable' not in self._data:
            raise NetCDFCommonDecoderError("Key variable is not present in mapping file", 1002, True)
        return self._data['key_variable']

    def _get_netcdf_data(self):
        data = {}
        for var in self._dataset.variables():
            var_name = var.name
            var_data = var.data()
            if var.data_type != '|S1' and not var_data.ndim == 0:
                if not all(math.isnan(d) for d in var_data):
                    data[var_name] = var_data
            elif var_data.ndim != 0:
                data[var_name] = var_data
        return data

    def build_records(self) -> t.Iterable[ParentRecord]:
        self._load_data()
        data = self._get_netcdf_data()
        key_var = self._get_key_variable()
        if key_var not in data:
            raise NetCDFCommonDecoderError("Key variable must be the name of a data variable", 1003, True)
        for i in range(0, len(data[key_var])):
            yield self._build_record({key: (data[key][i] if i < len(data[key]) else None) for key in data}, i)

    def _build_record(self, data: dict[str, t.Any], index):
        record = ParentRecord()
        record.coordinates.set_element('RecordNumber', index + 1)
        for key in self._data['variable_map']:
            map_info = self._data['variable_map'][key]
            element = self._build_element_from_variable(map_info, data)
            if element is not None:
                self._after_variable_element(element, map_info, data)
                self._apply_element(record, element, map_info)
        self._apply_global_elements(record)
        self._after_record(record, index)
        return record

    def _apply_global_elements(self, record):
        if 'global_elements' not in self._cache:
            self._cache['global_elements'] = []
            for key in self._data['attribute_map']:
                map_info = self._data['attribute_map'][key]
                element = self._build_element_from_attribute(map_info)
                if element is not None:
                    self._after_attribute_element(element, map_info)
                    self._cache['global_elements'].append(('attribute_map', key, element))
            for key in self._data['single_variable_map']:
                map_info = self._data['single_variable_map'][key]
                element = self._build_element_from_single_variable(map_info)
                if element is not None:
                    self._after_single_variable_element(element, map_info)
                    self._cache['global_elements'].append(('single_variable_map', key, element))
        for map_name, map_key, element in self._cache['global_elements']:
            minfo = self._data[map_name][map_key]
            self._apply_element(record, element, minfo)

    def _apply_element(self, record, element, map_info, target_name = None):
        if target_name is None:
            target_name = map_info['target']
        if isinstance(target_name, (list, tuple, set)):
            for sub_target in target_name:
                self._apply_element(record, element, map_info, sub_target)
        else:
            action = record.set_element
            if 'allow_multiple' in map_info and map_info['allow_multiple']:
                action = record.add_element
            try:
                action(target_name, element)
            except ValueError as ex:
                if 'allow_missing' in map_info and map_info['allow_missing']:
                    self._log.debug(f"Missing target [{target_name}]")
                else:
                    self._log.exception(f"Missing target [{target_name}]")

    def _after_record(self, record: ParentRecord, index: int):
        pass

    def _after_variable_element(self, element: SingleElement, key: str, data: dict[str, t.Any]):
        pass

    def _after_attribute_element(self, element: SingleElement, key: str):
        pass

    def _after_single_variable_element(self, element: SingleElement, key: str):
        pass

    def _build_element_from_attribute(self, minfo):
        if not self._dataset.has_attribute(minfo['source']):
            return None
        value = self._process_value(self._dataset.attribute(minfo['source']), minfo)
        if value is None:
            return None
        metadata = {}
        if 'metadata' in minfo and minfo['metadata']:
            metadata.update(self._build_metadata(minfo['metadata']))
        element = SingleElement(value)
        if metadata:
            element.metadata.update(metadata)
        return element

    def _build_element_from_single_variable(self, minfo):
        if not self._dataset.has_variable(minfo['source']):
            return None
        var = self._dataset.variable(minfo['source'])
        if var.data_type == '|S1':
            value = var.as_string()
        else:
            value = unnumpy(var.data())
        value = self._process_value(value, minfo)
        if value is None:
            return None
        metadata = {}
        if 'metadata' in minfo and minfo['metadata']:
            metadata.update(self._build_metadata(minfo['metadata']))
        element = SingleElement(value)
        if metadata:
            element.metadata.update(metadata)
        return element

    def _build_element_from_variable(self,
                                     minfo: dict,
                                     data: dict[str, t.Any]):
        # Make sure our source value exists
        if 'source' in minfo and minfo['source'] not in data:
            return None

        # Get the source value and process it as necessary
        value = self._process_value(data[minfo['source']], minfo)
        metadata = {}

        # Check if there was an adjusted value (should not be true)
        adjusted_name = f'{minfo['source']}_ADJUSTED'
        if adjusted_name in data:
            test_value = self._process_value(data[adjusted_name], minfo)
            if test_value is not None:
                metadata['Unadjusted'] = value
                value = test_value

        # Empty values get None
        if value is None and not metadata:
            return None

        # Apply default metadata
        if 'metadata' in minfo:
            metadata.update(self._build_metadata(minfo['metadata']))

        # Check for QC variable
        if 'qc_source' in minfo and minfo['qc_source'] and minfo['qc_source'] in data:
            qual = unnumpy(data[minfo['qc_source']])
            if qual is not None:
                metadata['Quality'] = qual

        # Check for units
        if 'Units' not in metadata and not ('no_units' in minfo and minfo['no_units']):
            units = None
            if self._dataset.variable(minfo['source']).has_attribute('units'):
                units = self._dataset.variable(minfo['source']).attribute('units')
            if units is not None:
                metadata['Units'] = self.units.standardize(units)

        # Some basic validation
        pieces = minfo['target'].split('/', maxsplit=1) if '/' in minfo['target'] else minfo['target']
        ename = pieces[-1]
        element_info = self.ontology.element_info(ename)
        if element_info is not None:
            if 'Units' in metadata and metadata['Units'] and element_info.preferred_unit:
                if not self.units.compatible(metadata['Units'], element_info.preferred_unit):
                    self._log.warning(f"Invalid units [{metadata['Units']}] for element [{element_info.name}] from [{minfo['source']}]")

        # Build the element
        ele = SingleElement(value)
        ele.metadata.update(metadata)
        return ele

    def _build_metadata(self, raw_mdata: dict):
        mdata = {}
        for key in raw_mdata:
            if isinstance(raw_mdata[key], dict):
                mdata[key] = SingleElement(raw_mdata[key].pop('_value', ''))
                mdata[key].metadata.update(self._build_metadata(raw_mdata[key].pop('_metadata', {})))
            else:
                mdata[key] = raw_mdata[key]
        return mdata

    def _process_value(self, value, minfo):
        value = unnumpy(value)
        if value == '' and not ('blank_to_none' in minfo and not minfo['blank_is_none']):
            value = None
        if 'mapping_type' in minfo and minfo['mapping_type']:
            processor = f'_{minfo['mapping_type']}'
            if not hasattr(self, processor):
                raise NetCDFCommonDecoderError(f"Invalid mapping type [{processor}]", 1004, True)
            value = getattr(self, processor)(value, minfo['source'])
        return value

    def _time_since(self, value, var_name):
        key = f'time_since_{var_name}'
        if key not in self._cache:
            if not self._dataset.variable(var_name).has_attribute('units'):
                raise NetCDFCommonDecoderError(f'Time since variable must have untis attribute: [{var_name}]', 1007, True)
            units = self._dataset.variable(var_name).attribute('units')
            pieces = units.split(' ', maxsplit=2)
            if len(pieces) != 3:
                raise NetCDFCommonDecoderError(f"Invalid time since units: [{units}]", 1006, True)
            if pieces[0].lower() not in ('seconds', 'minutes', 'hours', 'days', 'weeks'):
                raise NetCDFCommonDecoderError(f'Invalid first part of time since units: [{units}]', 1005, True)
            try:
                epoch = datetime.datetime.fromisoformat(pieces[2])
            except ValueError as ex:
                raise NetCDFCommonDecoderError(f"Invalid last part of time since units: [{units}]", 1008, True) from ex
            self._cache[key] = functools.partial(NetCDFCommonMapper._decode_time_since, epoch=epoch, increments=pieces[0].lower())
        return self._cache[key](value)

    def _institution(self, inst_name, var_name):
        if ';' in inst_name:
            raw_insts = [self._institution(x.strip(), var_name) for x in inst_name.split(';')]
            return [x for x in raw_insts if x is not None]
        inst_name = inst_name.lower()
        if inst_name == "" or inst_name is None:
            return None
        elif inst_name == 'university of victoria':
            return "university-of-victoria"
        elif inst_name == 'universityofvictoria':
            return "university-of-victoria"
        elif inst_name == "ios":
            return "ios"
        elif inst_name == 'bio':
            return 'bio'
        elif inst_name == 'nafc':
            return 'nafc'
        elif inst_name == 'c-proof':
            return "c-proof"
        else:
            self._log.warning(f"Unrecognized institution name [{inst_name}]")
            return inst_name

    def _comma_list(self, value, var_name):
        return [x.strip() for x in value.split(',') if x.strip() != ""]

    def _semicolon_list(self, value, var_name):
        return [x.strip() for x in value.split(';') if x.strip() != ""]

    @staticmethod
    def _decode_time_since(value, increments: str, epoch: datetime.datetime):
        if value is None:
            return None
        return epoch + datetime.timedelta(**{increments: float(value)})

