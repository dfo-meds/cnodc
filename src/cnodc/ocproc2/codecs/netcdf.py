import datetime
import functools
import logging
import math
import pathlib
import re

import yaml

import typing as t
import netCDF4 as nc
from autoinject import injector

from cnodc.science.units import UnitConverter
from cnodc.science.units.structures import UnitError
from cnodc.util import unnumpy, DynamicObjectLoadError
from cnodc.ocproc2.codecs.base import BaseCodec, ByteIterable, DecodeResult
from cnodc.ocproc2 import ParentRecord, SingleElement, MultiElement
from cnodc.ocproc2.ontology import OCProc2Ontology
from cnodc.util import CNODCError
from cnodc.util.dynamic import dynamic_object
from cnodc.util.sanitize import netcdf_bytes_to_string
import cnodc.util.awaretime as awaretime


class NetCDFDecoder(BaseCodec):
    """ Generic decoder for NetCDF files. """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, is_decoder=True, **kwargs)

    def _decode_records(self, data: ByteIterable, **kwargs) -> t.Iterable[DecodeResult]:
        nc_data = b''.join(data)
        try:
            with nc.Dataset('inmemory.nc', "r", memory=nc_data) as netcdf:
                yield DecodeResult(records=self._build_from_netcdf(netcdf, **kwargs))
        except Exception as ex:
            yield DecodeResult(exc=ex)

    def _build_from_netcdf(self, dataset: nc.Dataset, **kwargs) -> t.Optional[list[ParentRecord]]:
        raise NotImplementedError  # pragma: no coverage


class NetCDFCommonDecoderError(CNODCError):

    def __init__(self, text, number, is_recoverable: bool = False):
        super().__init__(text, 'NETCDF_COMMON_DECODE', number, is_recoverable)


class NetCDFCommonDecoder(NetCDFDecoder):
    """ Glider decoder for the EGO format. """

    def __init__(self, *args, **kwargs):
        super().__init__(log_name="cnodc.netcdf_common_decoder", *args, **kwargs)

    def _build_from_netcdf(self, dataset: nc.Dataset, **kwargs):
        if 'mapping_class' in kwargs:
            map_cls = dynamic_object(kwargs.pop('mapping_class'))
        else:
            map_cls = NetCDFCommonMapper
        if 'mapping_file' in kwargs:
            mapping_file = kwargs.pop('mapping_file')
        elif hasattr(map_cls, 'DEFAULT_MAPPING_FILE'):
            mapping_file = map_cls.DEFAULT_MAPPING_FILE
        else:
            raise NetCDFCommonDecoderError("Missing [mapping_file] keyword", 1000, False)
        mapper = map_cls(dataset, pathlib.Path(mapping_file))
        return [r for r in mapper.build_records()]


class NetCDFCommonMapper:

    units: UnitConverter = None
    ontology: OCProc2Ontology = None

    @injector.construct
    def __init__(self, dataset: nc.Dataset, mapping_file: t.Union[pathlib.Path, dict], log_name: str = "cnodc.netcdf.common_mapper"):
        if isinstance(mapping_file, (str, pathlib.Path)):
            self._map_file = mapping_file
            self._data = None
        else:
            self._map_file = None
            self._data = mapping_file
        self._dataset: nc.Dataset = dataset
        self._data_validated: bool = False
        self._cache = {}
        self._log = logging.getLogger(log_name)


    def _load_data(self):
        if self._data is None:
            try:
                with open(self._map_file, "r") as h:
                    self._data = yaml.safe_load(h)
            except OSError:
                raise NetCDFCommonDecoderError(f"No mapping file or invalid file found at [{self._map_file}]", 2000)
        if not self._data_validated:
            if not isinstance(self._data, dict):
                raise NetCDFCommonDecoderError("Mapping file is not a YAML dictionary", 2001)
            if 'ocproc2_map' not in self._data or not isinstance(self._data['ocproc2_map'], dict):
                raise NetCDFCommonDecoderError("Missing [ocproc2_map] key", 2002)
            if 'data_maps' not in self._data or not isinstance(self._data['data_maps'], dict):
                raise NetCDFCommonDecoderError("Missing [data_maps] key", 2003)
            if self._get_ocproc2_map()['key_var'] is None:
                raise NetCDFCommonDecoderError("Missing a variable with [is_index=yes] in ocproc2_map", 2004)
            self._on_data_load()
            self._data_validated = True

    def _on_data_load(self):
        pass

    def _get_ocproc2_map(self):
        if 'ocproc_map' not in self._cache:
            self._cache['ocproc_map'] = {
                'global': {},
                'record': {},
                'key_var': None,
                'data_vars': [],
            }
            for k in self._data['ocproc2_map'].keys():
                mapping_type, source_name = 'var', k
                if ':' in k:
                    mapping_type, _, source_name = k.partition(':')
                info = self._data['ocproc2_map'][k]
                if isinstance(info, dict):
                    if 'source' in info and info['source']:
                        source_name = info['source']
                    if 'mapping_type' in info and info['mapping_type']:
                        mapping_type = info['mapping_type']
                    if 'is_index' in info and info['is_index'] and mapping_type == 'var' and self._cache['ocproc_map']['key_var'] is None:
                        self._cache['ocproc_map']['key_var'] = source_name
                    if 'unadjusted_source' in info:
                        if info['unadjusted_source'] and info['unadjusted_source'] in self._dataset.variables:
                            self._cache['ocproc_map']['data_vars'].append(info['unadjusted_source'])
                        else:
                            info['unadjusted_source'] = None
                    else:
                        info['unadjusted_source'] = None
                    if 'adjusted_source' in info:
                        if info['adjusted_source'] and info['adjusted_source'] in self._dataset.variables:
                            self._cache['ocproc_map']['data_vars'].append(info['adjusted_source'])
                        else:
                            info['adjusted_source'] = None
                    else:
                        info['adjusted_source'] = None
                    if 'qc_source' in info:
                        if info['qc_source'] and info['qc_source'] in self._dataset.variables:
                            self._cache['ocproc_map']['data_vars'].append(info['qc_source'])
                        else:
                            info['qc_source'] = None
                    else:
                        info['qc_source'] = None
                    if 'data_map' in info:
                        if info['data_map'] and info['data_map'] in self._data and isinstance(self._data['data_maps'][info['data_map']], dict):
                            info['data_map'] = self._data['data_maps'][info['data_map']]
                        else:
                            del info['data_map']
                    if 'data_map_key' in info:
                        if not ('data_map' in info):
                            del info['data_map_key']
                    if 'target' in info:
                        info['_targets'] = [info['target']] if isinstance(info['target'], str) else info['target']
                        del info['target']
                    if 'allow_multiple' not in info:
                        info['allow_multiple'] = False
                    if 'no_units' not in info:
                        info['no_units'] = False
                    if 'metadata' not in info:
                        info['metadata'] = {}
                    if 'separator' in info:
                        if not info['separator']:
                            del info['separator']
                        elif len(info['separator']) > 1:
                            info['regex_separator'] = re.compile(info['separator'])
                            del info['separator']
                    if 'data_processor' in info:
                        if info["data_processor"]:
                            if "." in info["data_processor"]:
                                try:
                                    info["_data_processor"] = dynamic_object(info["data_processor"])
                                except DynamicObjectLoadError:
                                    pass
                            else:
                                try:
                                    info["_data_processor"] = getattr(self.__class__, info["data_processor"])
                                except AttributeError:
                                    pass
                        del info["data_processor"]
                    info.update({
                        'source': source_name,
                        'mapping_type': mapping_type
                    })
                else:
                    info = {
                        'source': source_name,
                        'mapping_type': mapping_type,
                        'allow_multiple': False,
                        'adjusted_source': None,
                        'unadjusted_source': None,
                        'qc_source': None,
                        'no_units': False,
                        'metadata': {},
                        '_targets': [info]
                    }
                if mapping_type == 'var' and info['source'] in self._dataset.variables:
                    self._cache['ocproc_map']['data_vars'].append(source_name)
                    info['_map_call'] = self._build_element_from_variable
                    self._cache['ocproc_map']['record'][k] = info
                elif mapping_type == 'attribute' and hasattr(self._dataset, info['source']):
                    info['_map_call'] = self._build_element_from_attribute
                    self._cache['ocproc_map']['global'][k] = info
                elif mapping_type == 'globalvar' and info['source'] in self._dataset.variables:
                    info['_map_call'] = self._build_element_from_single_variable
                    self._cache['ocproc_map']['global'][k] = info
                elif mapping_type not in ('var', 'attribute', 'globalvar'):
                    self._log.error(f"Invalid mapping type [{mapping_type}] for [{k}], ignoring mapping instructions")
                else:
                    self._log.warning(f"Missing input value [{mapping_type}:{info['source']}], ignoring mapping instructions")
        return self._cache['ocproc_map']

    def _get_netcdf_data(self, data_vars: list[str]):
        data = {}
        for var_name in self._dataset.variables:
            if var_name not in data_vars:
                continue
            var = self._dataset.variables[var_name]
            var_data = var[:]
            # we only want the numeric data that comes in arrays
            if var.dtype != '|S1' and not var_data.ndim == 0:
                var_data = unnumpy(var_data)
                if any(d is not None for d in var_data):
                    data[var_name] = var_data
        return data

    def build_records(self) -> t.Iterable[ParentRecord]:
        self._cache = {}
        self._load_data()
        ocproc_map = self._get_ocproc2_map()

        # Make sure our source value exists
        data = self._get_netcdf_data(ocproc_map['data_vars'])
        for i in range(0, len(data[ocproc_map['key_var']])):
            yield self._build_record(ocproc_map, i, {key: (data[key][i] if i < len(data[key]) else None) for key in data})

    def _build_record(self, ocproc_map: dict, index: int, data: dict[str, t.Any]):
        record = ParentRecord()
        record.coordinates.set('RecordNumber', index + 1)
        for key in ocproc_map['record']:
            map_info = ocproc_map['record'][key]
            element = map_info["_map_call"](map_info, data)
            if element:
                self._after_element(element, map_info, data)
                self._apply_element(record, element, map_info)
        self._apply_global_elements(ocproc_map, record)
        self._after_record(record, index)
        return record

    def _apply_global_elements(self, ocproc_map, record):
        if 'global_elements' not in self._cache:
            self._cache['global_elements'] = []
            for key in ocproc_map['global']:
                map_info = ocproc_map['global'][key]
                element = map_info["_map_call"](map_info)
                if element is not None:
                    self._after_element(element, map_info)
                    self._cache['global_elements'].append((key, element))
        for key, element in self._cache['global_elements']:
            self._apply_element(record, element, ocproc_map['global'][key])

    def _apply_element(self, record, element, map_info):
        for target_name in map_info['_targets']:
            action = record.set if not map_info['allow_multiple'] else record.append_to
            try:
                action(target_name, element)
            except ValueError as ex:
                if 'nowarn_missing_target' in map_info and map_info['nowarn_missing_target']:
                    self._log.info(f"Missing target [{target_name}]: {type(ex)}: {str(ex)}")
                else:
                    self._log.exception(f"Missing target [{target_name}]: {type(ex)}: {str(ex)}")

    def _after_record(self, record: ParentRecord, index: int):
        pass

    def _after_element(self, element: SingleElement, minfo: dict, data: t.Optional[dict[str, t.Any]] = None):
        pass

    def _build_element_from_attribute(self, minfo, data=None):
        value = self._process_value(self._dataset.getncattr(minfo['source']), minfo)
        return self._build_element_common(value, minfo)

    def _build_element_from_single_variable(self, minfo, data=None):
        var = self._dataset.variables[minfo['source']]
        value = netcdf_bytes_to_string(var[:]) if var.dtype == '|S1' else unnumpy(var[:])
        value = self._process_value(value, minfo)
        return self._build_element_common(value, minfo)

    def _build_element_from_variable(self,
                                     minfo: dict,
                                     data: dict[str, t.Any]):

        # Get the source value and process it as necessary
        value, unadjusted_value = None, None
        if minfo['source'] in data:
            value = self._process_value(data[minfo['source']], minfo)
        unadjusted_value = None

        # Check if there was an adjusted value
        if minfo['adjusted_source'] in data:
            test_value = self._process_value(data[minfo['adjusted_source']], minfo)
            if test_value is not None:
                unadjusted_value = value
                value = test_value

        # Check if there was an unadjusted value
        elif minfo['unadjusted_source'] in data:
            test_value = self._process_value(data[minfo['unadjusted_source']], minfo)
            if test_value is not None:
                unadjusted_value = test_value

        element = self._build_element_common(value, minfo, unadjusted_value)
        if element is None:
            return None

        # Check for QC variable
        if minfo['qc_source'] in data:
            qual = unnumpy(data[minfo['qc_source']])
            if qual is not None:
                element.metadata['Quality'] = qual

        # Check for units
        if 'Units' not in element.metadata and not minfo['no_units']:
            units = self.get_units(minfo['source'])
            if units is not None:
                element.metadata['Units'] = units

        return element

    def _build_element_common(self, value, minfo, unadjusted=None):
        if value is None and unadjusted is None:
            return None
        metadata = {}
        metadata.update(self._build_metadata(minfo['metadata']))
        if unadjusted is not None:
            metadata['Unadjusted'] = unadjusted
        if minfo['allow_multiple'] and isinstance(value, (list, set, tuple)):
            element = MultiElement([SingleElement(v) for v in value], _skip_normalization=True)
        else:
            element = SingleElement(value)
        element.metadata.update(metadata)
        return element

    def _build_metadata(self, raw_mdata: dict):
        for key in raw_mdata:
            try:
                new_val = SingleElement(raw_mdata[key].pop('_value'))
                new_val.metadata.update(self._build_metadata(raw_mdata[key].pop('_metadata', {})))
            except (AttributeError, KeyError):
                new_val = raw_mdata[key]
            raw_mdata[key] = new_val
        return raw_mdata

    def _process_value(self, value, minfo):
        value = unnumpy(value)
        if 'separator' in minfo:
            return self._clean_up_list([
                x.strip() for x in value.split(minfo['separator'])
            ], minfo)
        elif 'regex_separator' in minfo:
            return self._clean_up_list([
                x.strip() for x in minfo['regex_separator'].split(value)
            ], minfo)
        else:
            return self._process_individual_value(value, minfo)

    def _clean_up_list(self, l, minfo):
        if not l:
            return None
        if len(l) == 1:
            return self._process_individual_value(l[0], minfo)
        else:
            return [self._process_individual_value(y, minfo) for y in l if y]

    def _process_individual_value(self, value, minfo):
        if 'data_map' in minfo:
            if 'data_map_key' in minfo:
                value = self.map_value(minfo['data_map'], value, minfo['data_map_key'])
            else:
                value = self.map_value(minfo['data_map'], value)
        if '_data_processor' in minfo:
            value = minfo['_data_processor'](self, value, minfo)
        return value

    def map_value(self, data_map: dict, item_name: str, sub_key: t.Optional[str] = None):
        item_name = item_name.lower()
        # check if the item is in the data map
        if item_name in data_map:
            if sub_key:
                if sub_key in data_map[item_name]:
                    return data_map[item_name][sub_key]
                else:
                    self._log.error(f'Missing subkey [{sub_key}] for [{item_name}], defaulting to original value')
                    return item_name
            else:
                return data_map[item_name]
        else:
            self._log.error(f'Unknown value [{item_name}] for data map , defaulting to original value')
            return item_name

    def _time_since(self, value, minfo):
        var_name = minfo['source']
        key = f'time_since_{var_name}'
        if key not in self._cache:
            units = self.get_units(var_name)
            if units is None:
                raise NetCDFCommonDecoderError(f'Time since variable must have units attribute: [{var_name}]', 1007, True)
            pieces = units.split(' ', maxsplit=2)
            if len(pieces) != 3:
                raise NetCDFCommonDecoderError(f"Invalid time since units: [{units}]", 1006, True)
            if pieces[0].lower() not in ('seconds', 'minutes', 'hours', 'days', 'weeks'):
                raise NetCDFCommonDecoderError(f'Invalid first part of time since units: [{units}]', 1005, True)
            try:
                epoch = awaretime.utc_from_isoformat(pieces[2])
            except ValueError as ex:
                raise NetCDFCommonDecoderError(f"Invalid last part of time since units: [{units}]", 1008, True) from ex
            self._cache[key] = functools.partial(NetCDFCommonMapper._decode_time_since, epoch=epoch, increments=pieces[0].lower())
        return self._cache[key](value)

    def has_attribute(self, attr_name):
        return hasattr(self._dataset, attr_name)

    def has_variable(self, var_name):
        return var_name in self._dataset.variables

    def var_to_string(self, var_name):
        return netcdf_bytes_to_string(self._dataset.variables[var_name][:])

    def get_units(self, var_name: str) -> t.Optional[str]:
        key = f'units_{var_name}'
        if key not in self._cache:
            self._cache[key] = None
            var = self._dataset.variables[var_name]
            if hasattr(var, 'units'):
                units = getattr(var, 'units')
                if ' since ' not in units:
                    units = self.units.standardize(units)
                self._cache[key] = units
        return self._cache[key]

    @staticmethod
    def _decode_time_since(value, increments: str, epoch: datetime.datetime):
        return epoch + datetime.timedelta(**{increments: float(value)})

