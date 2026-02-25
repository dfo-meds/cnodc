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
from cnodc.util import unnumpy
from cnodc.ocproc2.codecs.base import BaseCodec, ByteIterable, DecodeResult
from cnodc.ocproc2 import ParentRecord, SingleElement
from cnodc.ocproc2.ontology import OCProc2Ontology
from cnodc.util import CNODCError
from cnodc.util.dynamic import dynamic_object
from cnodc.util.sanitize import netcdf_bytes_to_string


class NetCDFDecoder(BaseCodec):
    """ Generic decoder for NetCDF files. """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, is_decoder=True, **kwargs)

    def _decode(self, data: ByteIterable, **kwargs) -> t.Iterable[DecodeResult]:
        nc_data = b''.join(data)
        try:
            with nc.Dataset('inmemory.nc', "r", memory=nc_data) as netcdf:
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
        if isinstance(mapping_file, dict):
            self._map_file = None
            self._data = mapping_file
        else:
            self._map_file = mapping_file
            self._data = None
        self._dataset: nc.Dataset = dataset
        self._cache = {}
        self._log = logging.getLogger(log_name)

    def _load_data(self):
        if self._data is None:
            try:
                with open(self._map_file, "r") as h:
                    self._data = yaml.safe_load(h)
            except OSError:
                raise NetCDFCommonDecoderError(f"No mapping file or invalid file found at [{self._map_file}]", 2000)
            if not isinstance(self._data, dict):
                raise NetCDFCommonDecoderError("Mapping file is not a YAML dictionary", 2001)
            if 'ocproc2_map' not in self._data or not isinstance(self._data['ocproc2_map'], dict):
                raise NetCDFCommonDecoderError("Missing [ocproc2_map] key", 2002)
            if 'data_maps' not in self._data or not isinstance(self._data['data_maps'], dict):
                raise NetCDFCommonDecoderError("Missing [data_maps] key", 2003)
            if self._get_ocproc2_map()['key_var'] is None:
                raise NetCDFCommonDecoderError("Missing a variable with [is_index=yes] in ocproc2_map", 2004)
            self._on_data_load()

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
                    if 'data_map' in info and info['data_map'] and isinstance(info['data_map'], str):
                        if info['data_map'] not in self._data['data_maps'] or not isinstance(self._data['data_maps'][info['data_map']], dict):
                            self._log.error(f"Invalid data map [{info['data_map']}, ignoring")
                            del info['data_map']
                    info.update({
                        'source': source_name,
                        'mapping_type': mapping_type
                    })
                else:
                    info = {
                        'source': source_name,
                        'mapping_type': mapping_type,
                        'target': info
                    }
                if mapping_type == 'var':
                    self._cache['ocproc_map']['data_vars'].append(source_name)
                    self._cache['ocproc_map']['record'][k] = info
                elif mapping_type in ('attribute', 'globalvar'):
                    self._cache['ocproc_map']['global'][k] = info
                else:
                    self._log.error(f"Invalid mapping type [{mapping_type}] for [{k}], ignoring mapping instructions")
        return self._cache['ocproc_map']

    def _get_netcdf_data(self, data_vars: list[str]):
        data = {}
        for var_name in self._dataset.variables:
            if var_name not in data_vars:
                continue
            var = self._dataset.variables[var_name]
            var_data = var[:]
            if var.dtype != '|S1' and not var_data.ndim == 0:
                if not all(math.isnan(d) for d in var_data):
                    data[var_name] = var_data
            elif var_data.ndim != 0:
                data[var_name] = var_data
        return data

    def build_records(self) -> t.Iterable[ParentRecord]:
        self._cache = {}
        self._load_data()
        ocproc_map = self._get_ocproc2_map()
        data = self._get_netcdf_data(ocproc_map['data_vars'])
        for i in range(0, len(data[ocproc_map['key_var']])):
            yield self._build_record(ocproc_map, i, {key: (data[key][i] if i < len(data[key]) else None) for key in data})

    def _build_record(self, ocproc_map: dict, index: int, data: dict[str, t.Any]):
        record = ParentRecord()
        record.coordinates.set_element('RecordNumber', index + 1)
        for key in ocproc_map['record']:
            map_info = ocproc_map['record'][key]
            element = self._build_element(map_info, data)
            if element:
                self._apply_element(record, element, map_info)
        self._apply_global_elements(ocproc_map, record)
        self._after_record(record, index)
        return record

    def _apply_global_elements(self, ocproc_map, record):
        if 'global_elements' not in self._cache:
            self._cache['global_elements'] = []
            for key in ocproc_map['global']:
                map_info = ocproc_map['global'][key]
                element = self._build_element(map_info)
                if element is not None:
                    self._cache['global_elements'].append((key, element))
        for key, element in self._cache['global_elements']:
            self._apply_element(record, element, self._data[ocproc_map['global'][key]])

    def _apply_element(self, record, element, map_info, target_name=None):
        target_name = target_name or map_info['target']
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

    def _after_element(self, element: SingleElement, minfo: dict, data: t.Optional[dict[str, t.Any]] = None):
        pass

    def _build_element(self, minfo: dict, data: t.Optional[dict] = None):
        if minfo['mapping_type'] == 'var':
            element = self._build_element_from_variable(minfo, data)
        elif minfo['mapping_type'] == 'attribute':
            element = self._build_element_from_attribute(minfo)
        elif minfo['mapping_type'] == 'globalvar':
            element = self._build_element_from_single_variable(minfo)
        else:
            # we should never get here because we flag bad mapping types above
            raise ValueError(f"Invalid mapping type [{minfo['mapping_type']}")
        if element is not None:
            self._after_element(element, minfo, data)
        return element

    def _build_element_from_attribute(self, minfo):
        if not self.has_attribute(minfo['source']):
            return None
        value = self._process_value(self._dataset.getncattr(minfo['source']), minfo)
        return self._build_element_common(value, minfo)

    def _build_element_from_single_variable(self, minfo):
        if not self.has_variable(minfo['source']):
            return None
        var = self._dataset.variables[minfo['source']]
        value = netcdf_bytes_to_string(var[:]) if var.dtype == '|S1' else unnumpy(var[:])
        value = self._process_value(value, minfo)
        return self._build_element_common(value, minfo)

    def _build_element_from_variable(self,
                                     minfo: dict,
                                     data: dict[str, t.Any]):
        # Make sure our source value exists
        if minfo['source'] not in data:
            return None

        # Get the source value and process it as necessary
        value = self._process_value(data[minfo['source']], minfo)
        unadjusted_value = None

        # Check if there was an adjusted value (should not be true)
        adjusted_name = f'{minfo['source']}_ADJUSTED'
        if adjusted_name in data:
            test_value = self._process_value(data[adjusted_name], minfo)
            if test_value is not None:
                unadjusted_value = value
                value = test_value

        element = self._build_element_common(value, minfo, unadjusted_value)

        # Check for QC variable
        if 'qc_source' in minfo and minfo['qc_source'] and minfo['qc_source'] in data:
            qual = unnumpy(data[minfo['qc_source']])
            if qual is not None:
                element.metadata['Quality'] = qual

        # Check for units
        if 'Units' not in element.metadata and not ('no_units' in minfo and minfo['no_units']):
            units = self.get_units(minfo['source'])
            if units is not None:
                element.metadata['Units'] = self.units.standardize(units)

        # Some basic validation
        pieces = minfo['target'].split('/', maxsplit=1) if '/' in minfo['target'] else minfo['target']
        ename = pieces[-1]
        element_info = self.ontology.element_info(ename)
        if element_info is not None:
            current_units = element.metadata.best_value('Units', None)
            if current_units and element_info.preferred_unit:
                if not self.units.compatible(current_units, element_info.preferred_unit):
                    self._log.warning(f"Invalid units [{current_units}] for element [{element_info.name}] from [{minfo['source']}]")

        return element

    def _build_element_common(self, value, minfo, unadjusted=None):
        if value is None and unadjusted is None:
            return None
        metadata = {}
        if unadjusted is not None:
            metadata['Unadjusted'] = unadjusted
        if 'metadata' in minfo and minfo['metadata']:
            metadata.update(self._build_metadata(minfo['metadata']))
        element = SingleElement(value)
        if metadata:
            element.metadata.update(metadata)
        return element

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
        if 'separator' in minfo and minfo['separator'] and value is not None:
            sep = minfo['separator']
            value = [
                y for y in
                (x.strip() for x in (value.split(sep) if len(sep) == 1 else re.split(sep, value)))
                if y
            ]
        if 'data_map' in minfo and minfo['data_map']:
            if 'data_map_key' in minfo and minfo['data_map_key']:
                value = self.map_value(minfo['data_map'], value, minfo['data_map_key'], raise_ex=False)
            else:
                value = self.map_value(minfo['data_map'], value, raise_ex=False)
        if 'data_processor' in minfo and minfo['data_processor']:
            processor = minfo['data_processor']
            if "." not in processor:
                if not hasattr(self, processor):
                    raise NetCDFCommonDecoderError(f"Invalid local data processor [{processor}]", 1004, True)
                value = getattr(self, processor)(value, minfo)
            else:
                obj = dynamic_object(processor)
                value = obj(value, minfo, self)
        return value

    def map_value(self, map_name_or_dict, item_name, sub_key: t.Optional[str] = None, coerce=None, raise_ex: bool = False):
        # dive into sequences
        if isinstance(item_name, (list, tuple, set)):
            return [self.map_value(map_name_or_dict, x, coerce, raise_ex) for x in item_name]
        # empty items are empty
        if item_name is None or item_name == '':
            return None
        # coerce if needed
        if coerce:
            item_name = coerce(item_name)
        # find the appropriate data map
        if isinstance(map_name_or_dict, dict):
            data_map = map_name_or_dict
        elif 'data_maps' in self._data and self._data['data_maps'] and map_name_or_dict in self._data['data_maps'][map_name_or_dict] and isinstance(self._data['data_maps'][map_name_or_dict], dict):
            data_map = self._data['data_maps'][map_name_or_dict]
        elif raise_ex:
            raise NetCDFCommonDecoderError(f"Missing map [{map_name_or_dict}]", 2000)
        else:
            self._log.error(f"Missing map [{map_name_or_dict}], defaulting to original value")
            return item_name
        # check if the item is in the data map
        if item_name in data_map:
            if sub_key:
                if sub_key in data_map[item_name]:
                    return data_map[item_name][sub_key]
                elif raise_ex:
                    raise NetCDFCommonDecoderError(f'Missing subkey [{sub_key}] for [{map_name_or_dict}][{item_name}]', 2001)
                else:
                    self._log.error(f'Missing subkey [{sub_key}] for [{map_name_or_dict}][{item_name}], defaulting to original value')
                return data_map[item_name][sub_key]
            return data_map[item_name]
        elif raise_ex:
            raise NetCDFCommonDecoderError(f'Unknown value [{item_name}] for map [{map_name_or_dict}]', 2002)
        else:
            self._log.error(f'Unknown value [{item_name}] for map [{map_name_or_dict}], defaulting to original value')
            return item_name

    def _time_since(self, value, var_name):
        key = f'time_since_{var_name}'
        if key not in self._cache:
            units = self.get_units(var_name)
            if units is None:
                raise NetCDFCommonDecoderError(f'Time since variable must have untis attribute: [{var_name}]', 1007, True)
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

    def has_attribute(self, attr_name):
        return any(x == attr_name for x in self._dataset.ncattrs())

    def has_variable(self, var_name):
        return var_name in self._dataset.variables

    def var_to_string(self, var_name):
        return netcdf_bytes_to_string(self._dataset.variables[var_name][:])

    def get_units(self, var_name) -> t.Optional[str]:
        var = self._dataset[var_name]
        if any('units' == x for x in var.ncattrs()):
            return var.ncattr('units')
        return None

    @staticmethod
    def _decode_time_since(value, increments: str, epoch: datetime.datetime):
        if value is None:
            return None
        return epoch + datetime.timedelta(**{increments: float(value)})

