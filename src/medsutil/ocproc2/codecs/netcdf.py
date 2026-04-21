import dataclasses
import datetime
import functools
import logging
import pathlib
import re
import yaml
import typing as t

import netCDF4 as nc
from autoinject import injector

from medsutil.units import UnitConverter
from medsutil.awaretime import AwareDateTime
from medsutil.sanitize import unnumpy
from medsutil.ocproc2.codecs.base import BaseCodec
from medsutil.ocproc2 import ParentRecord, SingleElement, MultiElement, AbstractElement
from medsutil.ocproc2.ontology import OCProc2Ontology
from pipeman.exceptions import CNODCError
from medsutil.dynamic import dynamic_object, DynamicObjectLoadError
from medsutil.sanitize import netcdf_bytes_to_string
import medsutil.awaretime as awaretime
import medsutil.types as ct

class NetCDFCommonDecoderError(CNODCError):

    def __init__(self, text, number, is_transient: bool = False):
        super().__init__(text, 'NETCDF_COMMON_DECODE', number, is_transient)


class NetCDFBaseDecoder(BaseCodec):
    """ Generic decoder for NetCDF files. """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, is_decoder=True, force_single_mode=True, **kwargs)

    def _decode_single_message(self, data: t.ByteString, context: dict) -> t.Iterable[ParentRecord]:
        with nc.Dataset('inmemory.nc', "r", memory=data) as netcdf:
            yield from self._build_from_netcdf(netcdf, context)

    def _build_from_netcdf(self, dataset: nc.Dataset, context: dict) -> t.Iterable[ParentRecord]: raise NotImplementedError


class NetCDFCommonDecoder(NetCDFBaseDecoder):
    """ Generalized decoder that uses a mapping file.  """

    def __init__(self, *args, **kwargs):
        super().__init__(log_name="cnodc.netcdf_common_decoder", *args, **kwargs)

    def _build_from_netcdf(self, dataset: nc.Dataset, options: dict) -> t.Iterable[ParentRecord]:
        map_cls = NetCDFCommonMapper
        if 'mapping_class' in options and options['mapping_class']:
            map_cls = dynamic_object(options.pop('mapping_class'))
        if 'mapping_file' in options:
            mapping_file = options.pop('mapping_file')
        elif hasattr(map_cls, 'DEFAULT_MAPPING_FILE'):
            mapping_file = map_cls.DEFAULT_MAPPING_FILE
        else:
            raise NetCDFCommonDecoderError("Missing [mapping_file] keyword", 1000, False)
        mapper = map_cls(dataset, pathlib.Path(mapping_file))
        yield from mapper.build_records()


@dataclasses.dataclass
class MappingInfo:
    source: str
    mapping_type: str
    metadata: dict
    targets: list[str]
    allow_multiple: bool = False
    adjusted_source: str | None = None
    unadjusted_source: str | None = None
    qc_source: str | None = None
    no_units: bool = False
    is_index: bool = False
    data_map: dict[t.Any, t.Any] | None = None
    data_map_key: str | None = None
    separator: str | None = None
    regex_separator: str | None = None
    data_processor: t.Callable | None = None
    map_call: t.Callable[[MappingInfo, dict[str, t.Any]], SingleElement | MultiElement | None] | t.Callable[[MappingInfo, None], SingleElement | MultiElement | None] = None
    nowarn_missing_target: bool = False


class NetCDFMappingDict(t.TypedDict):
    global_vars: dict[str, MappingInfo]
    record_vars: dict[str, MappingInfo]
    data_vars: list[str]
    key_var: str | None


class NetCDFCommonMapper:

    units: UnitConverter = None
    ontology: OCProc2Ontology = None

    @injector.construct
    def __init__(self,
                 dataset: nc.Dataset,
                 mapping_file: ct.PathLike | dict,
                 log_name: str = "cnodc.netcdf.common_mapper"):
        self._map_file: ct.PathLike | None = None
        self._data = None
        if isinstance(mapping_file, dict):
            self._data = mapping_file
        else:
            self._map_file = mapping_file
        self._dataset: nc.Dataset = dataset
        self._data_validated: bool = False
        self._cache = {}
        self._log = logging.getLogger(log_name)

    def _load_data(self):
        if self._data is None:
            if self._map_file is None:
                raise NetCDFCommonDecoderError(f"No mapping file or default dict provided [{self._map_file}]", 2100)
            try:
                with open(self._map_file, "r") as h:
                    self._data = yaml.safe_load(h)
            except (OSError, TypeError):
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

    def _get_ocproc2_map(self) -> NetCDFMappingDict:
        if 'ocproc_map' not in self._cache:
            self._cache['ocproc_map'] = {
                'global_vars': {},
                'record_vars': {},
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
                    else:
                        info['source'] = source_name
                    if 'mapping_type' in info and info['mapping_type']:
                        mapping_type = info['mapping_type']
                    else:
                        info['mapping_type'] = mapping_type
                    if 'target' in info and info['target']:
                        info['targets'] = [info['target']] if isinstance(info['target'], str) else info['target']
                        del info['target']
                    else:
                        self._log.warning(f'No target for [%s], ignoring', source_name)
                        continue
                    if 'is_index' in info and info['is_index'] and mapping_type == 'var' and self._cache['ocproc_map']['key_var'] is None:
                        self._cache['ocproc_map']['key_var'] = source_name
                    if 'unadjusted_source' in info:
                        if info['unadjusted_source'] and info['unadjusted_source'] in self._dataset.variables:
                            self._cache['ocproc_map']['data_vars'].append(info['unadjusted_source'])
                        else:
                            self._log.warning(f'Omitting [unadjusted] for %s, not found in dataset variables', source_name)
                            info['unadjusted_source'] = None
                    else:
                        info['unadjusted_source'] = None
                    if 'adjusted_source' in info:
                        if info['adjusted_source'] and info['adjusted_source'] in self._dataset.variables:
                            self._cache['ocproc_map']['data_vars'].append(info['adjusted_source'])
                        else:
                            self._log.warning(f'Omitting [adjusted_source] for %s, not found in dataset variables', source_name)
                            info['adjusted_source'] = None
                    else:
                        info['adjusted_source'] = None
                    if 'qc_source' in info:
                        if info['qc_source'] and info['qc_source'] in self._dataset.variables:
                            self._cache['ocproc_map']['data_vars'].append(info['qc_source'])
                        else:
                            self._log.warning(f'Omitting [qc_source] for %s, not found in dataset variables', source_name)
                            info['qc_source'] = None
                    else:
                        info['qc_source'] = None
                    if 'data_map' in info:
                        if not info['data_map']:
                            self._log.warning(f'Removing [data_map] from %s, blank value', source_name)
                            del info['data_map']
                        elif isinstance(info['data_map'], str):
                            if info['data_map'] in self._data['data_maps'] and isinstance(self._data['data_maps'][info['data_map']], dict):
                                info['data_map'] = self._data['data_maps'][info['data_map']]
                            else:
                                self._log.warning(f'Removing [data_map] from %s, no data map found for %s', source_name, info['data_map'])
                                del info['data_map']
                        elif not isinstance(info['data_map'], dict):
                            self._log.warning(f'Removing [data_map] from %s, invalid value', source_name)
                            del info['data_map']
                    if 'data_map_key' in info:
                        if not ('data_map' in info):
                            self._log.warning(f'Removing [data_map_key] from %s, no data map provided', source_name)
                            del info['data_map_key']
                    if 'allow_multiple' not in info:
                        info['allow_multiple'] = False
                    if 'no_units' not in info:
                        info['no_units'] = False
                    if 'metadata' not in info:
                        info['metadata'] = {}
                    if 'separator' in info:
                        if not info['separator']:
                            self._log.warning(f'Omitting [separator] for %s, is blank', source_name)
                            del info['separator']
                        elif len(info['separator']) > 1:
                            info['regex_separator'] = re.compile(info['separator'])
                            del info['separator']
                    if 'data_processor' in info:
                        if info["data_processor"]:
                            if "." in info["data_processor"]:
                                try:
                                    info["data_processor"] = dynamic_object(info["data_processor"])
                                except DynamicObjectLoadError:
                                    del info["data_processor"]
                                    self._log.warning(f'Omitting [data_processor] for %s, could not load dynamic object', source_name, exc_info=True)
                            else:
                                try:
                                    info["data_processor"] = getattr(self.__class__, info["data_processor"])
                                except AttributeError:
                                    del info["data_processor"]
                                    self._log.warning(f'Omitting [data_processor] for %s, could not load class attribute', source_name, exc_info=True)
                        else:
                            del info["data_processor"]
                    try:
                        map_info = MappingInfo(**info)
                    except TypeError as ex:
                        raise NetCDFCommonDecoderError("Invalid mapping info", 2000) from ex
                else:
                    map_info = MappingInfo(source=source_name, mapping_type=mapping_type, metadata={}, targets=[info])
                if map_info.mapping_type == 'var' and map_info.source in self._dataset.variables:
                    map_info.map_call = self._build_element_from_variable
                    self._cache['ocproc_map']['record_vars'][k] = map_info
                    self._cache['ocproc_map']['data_vars'].append(map_info.source)
                elif map_info.mapping_type == 'attribute' and hasattr(self._dataset, map_info.source):
                    map_info.map_call = self._build_element_from_attribute
                    self._cache['ocproc_map']['global_vars'][k] = map_info
                elif map_info.mapping_type == 'globalvar' and map_info.source in self._dataset.variables:
                    map_info.map_call = self._build_element_from_single_variable
                    self._cache['ocproc_map']['global_vars'][k] = map_info
                elif mapping_type not in ('var', 'attribute', 'globalvar'):
                    self._log.warning(f"Invalid mapping type [%s] for [%s], ignoring mapping instructions", map_info.mapping_type, k)
                else:
                    self._log.warning(f"Missing input value [%s:%s], ignoring mapping instructions", map_info.mapping_type, map_info.source)
        return self._cache['ocproc_map']

    def _get_netcdf_data(self, data_vars: list[str]) -> dict[str, list[int | float]]:
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
        key_var = ocproc_map['key_var']
        if key_var is None:
            raise NetCDFCommonDecoderError('Missing key variable', 3000)
        else:
            data = self._get_netcdf_data(ocproc_map['data_vars'])
            for i in range(0, len(data[key_var])):
                yield self._build_record(ocproc_map, i, {key: (data[key][i] if i < len(data[key]) else None) for key in data})

    def _build_record(self, ocproc_map: NetCDFMappingDict, index: int, data: dict[str, t.Any]) -> ParentRecord:
        record = ParentRecord()
        record.coordinates.set('RecordNumber', index + 1)
        for key in ocproc_map['record_vars']:
            map_info = ocproc_map['record_vars'][key]
            element = None
            if map_info.map_call is not None:
                element = map_info.map_call(map_info, data)
            if element is not None:
                self._after_element(element, map_info, data)
                self._apply_element(record, element, map_info)
        self._apply_global_elements(ocproc_map, record)
        self._after_record(record, index)
        return record

    def _apply_global_elements(self, ocproc_map: NetCDFMappingDict, record: ParentRecord):
        global_vars: dict[str, MappingInfo] = ocproc_map['global_vars']
        if global_vars is not None:
            if 'global_elements' not in self._cache:
                self._cache['global_elements'] = []
                for key in global_vars:
                    map_info = global_vars[key]
                    element = None
                    if map_info.map_call is not None:
                        element = map_info.map_call(map_info, None)
                    if element is not None:
                        self._after_element(element, map_info)
                        self._cache['global_elements'].append((key, element))
            for key, element in self._cache['global_elements']:
                self._apply_element(record, element, global_vars[key])

    def _apply_element(self, record: ParentRecord, element: AbstractElement, map_info: MappingInfo):
        for target_name in map_info.targets:
            try:
                if map_info.allow_multiple:
                    record.append_element_to(target_name, element)
                else:
                    record.set_element(target_name, element)
            except ValueError as ex:
                if map_info.nowarn_missing_target:
                    self._log.info(f"Missing target [{target_name}]: {type(ex)}: {str(ex)}")
                else:
                    self._log.warning(f"Missing target [{target_name}]: {ex.__class__.__name__}: {str(ex)}", exc_info=True)

    def _after_record(self, record: ParentRecord, index: int):
        pass

    def _after_element(self, element: AbstractElement, minfo: MappingInfo, data: t.Optional[dict[str, t.Any]] = None):
        pass

    def _build_element_from_attribute(self, minfo: MappingInfo, data=None):
        value = self._process_value(self._dataset.getncattr(minfo.source), minfo)
        return self._build_element_common(value, minfo)

    def _build_element_from_single_variable(self, minfo: MappingInfo, data=None):
        var = self._dataset.variables[minfo.source]
        value = netcdf_bytes_to_string(var[:]) if var.dtype == '|S1' else unnumpy(var[:])
        value = self._process_value(value, minfo)
        return self._build_element_common(value, minfo)

    def _build_element_from_variable(self,
                                     minfo: MappingInfo,
                                     data: dict[str, t.Any]):

        # Get the source value and process it as necessary
        value, unadjusted_value = None, None
        if minfo.source in data:
            value = self._process_value(data[minfo.source], minfo)
        unadjusted_value = None

        # Check if there was an adjusted value
        if minfo.adjusted_source is not None and minfo.adjusted_source in data:
            test_value = self._process_value(data[minfo.adjusted_source], minfo)
            if test_value is not None:
                unadjusted_value = value
                value = test_value

        # Check if there was an unadjusted value
        elif minfo.unadjusted_source is not None and minfo.unadjusted_source in data:
            test_value = self._process_value(data[minfo.unadjusted_source], minfo)
            if test_value is not None:
                unadjusted_value = test_value

        element = self._build_element_common(value, minfo, unadjusted_value)
        if element is None:
            return None

        # Check for QC variable
        if minfo.qc_source is not None and minfo.qc_source in data:
            qual = unnumpy(data[minfo.qc_source])
            if qual is not None:
                element.metadata['Quality'] = qual

        # Check for units
        if 'Units' not in element.metadata and not minfo.no_units:
            units = self.get_units(minfo.source)
            if units is not None:
                element.metadata['Units'] = units

        return element

    def _build_element_common(self, value: t.Any, minfo: MappingInfo, unadjusted: t.Any = None):
        if (value is None or value == '') and (unadjusted is None or unadjusted == ''):
            return None
        metadata = {}
        metadata.update(self._build_metadata(minfo.metadata))
        if unadjusted is not None:
            metadata['Unadjusted'] = unadjusted
        if minfo.allow_multiple and isinstance(value, (list, set, tuple)):
            element = MultiElement([SingleElement(v) for v in value])
        else:
            element = SingleElement(value)
        element.metadata.update(metadata)
        return element

    def _build_metadata(self, raw_mdata: dict[str, t.Any] | None):
        if raw_mdata is None:
            return {}
        for key in raw_mdata:
            try:
                new_val = SingleElement(raw_mdata[key].pop('_value'))
                new_val.metadata.update(self._build_metadata(raw_mdata[key].pop('_metadata', {})))
            except (AttributeError, KeyError):
                new_val = raw_mdata[key]
            raw_mdata[key] = new_val
        return raw_mdata

    def _process_value(self, value: t.Any, minfo: MappingInfo) -> t.Any:
        value = unnumpy(value)
        if value is None or value == '':
            return None
        if minfo.separator:
            return self._clean_up_list([
                x.strip() for x in value.split(minfo.separator)
            ], minfo)
        elif minfo.regex_separator:
            return self._clean_up_list([
                x.strip() for x in minfo.regex_separator.split(value)
            ], minfo)
        else:
            return self._process_individual_value(value, minfo)

    def _clean_up_list(self, l: list | None, minfo: MappingInfo):
        if not l:
            return None
        if len(l) == 1:
            return self._process_individual_value(l[0], minfo) if l[0] is not None and l[0] != '' else None
        else:
            return [self._process_individual_value(y, minfo) for y in l if y is not None and y != ''] or None

    def _process_individual_value(self, value: t.Any, minfo: MappingInfo):
        if minfo.data_map is not None:
            if minfo.data_map_key is not None:
                value = self.map_value(minfo.data_map, value, minfo.data_map_key, minfo.source)
            else:
                value = self.map_value(minfo.data_map, value, None, minfo.source)
        if minfo.data_processor is not None:
            value = minfo.data_processor(self, value, minfo)
        return value

    def map_value(self, data_map: dict, item_name: str, sub_key: t.Optional[str] = None, field_name: t.Optional[str] = None):
        check_name = item_name.lower()
        # check if the item is in the data map
        if check_name  in data_map:
            if sub_key:
                if sub_key in data_map[check_name]:
                    return data_map[check_name][sub_key]
                else:
                    self._log.error(f'Missing subkey [%s] for data mapped value [%s] (source [%s]), defaulting to original value', sub_key, check_name, field_name)
                    return check_name
            else:
                return data_map[check_name]
        else:
            self._log.error(f'Unknown value [%s] for data map (source [%s]), defaulting to original value', check_name, field_name)
            return item_name

    def _time_since(self, value, minfo: MappingInfo):
        var_name = minfo.source
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

    def has_attribute(self, attr_name: str) -> bool:
        return hasattr(self._dataset, attr_name)

    def has_variable(self, var_name: str) -> bool:
        return var_name in self._dataset.variables

    def var_to_string(self, var_name: str) -> str:
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
    def _decode_time_since(value: t.SupportsFloat, increments: str, epoch: AwareDateTime) -> AwareDateTime:
        return epoch + datetime.timedelta(**{increments: float(value)})

