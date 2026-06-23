import contextlib
import typing as t
import math
from copy import deepcopy

import yaml
import pathlib

from autoinject import injector
import zrlog
import pybufrkit.descriptors
from pybufrkit.bufr import SectionParameter
from pybufrkit.descriptors import SequenceDescriptor, ElementDescriptor, DelayedReplicationDescriptor
from pybufrkit.encoder import Encoder
from pybufrkit.tables import TableGroupCacheManager, TableGroupKey, BufrTableGroup
from pybufrkit.renderer import NestedTextRenderer, NestedJsonRenderer
from pybufrkit.decoder import Decoder
from pybufrkit.templatedata import TemplateData, SequenceNode, DelayedReplicationNode, FixedReplicationNode, \
    ValueDataNode, NoValueDataNode
from wtforms.validators import none_of

from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2 import ParentRecord, BaseRecord, RecordSet, AbstractElement
from medsutil.ocproc2.codecs.gts import GtsSubDecoder
from medsutil.ocproc2.codecs.base import DecodeResult
from medsutil.byteseq import ByteSequenceReader
import medsutil.ocproc2 as ocproc2
import medsutil.awaretime as awaretime
from medsutil.units import UnitConverter
from medsutil.units.units import convert
from pipeman.exceptions import CNODCError


@injector.injectable_global
class BufrCDSTables:

    converter: UnitConverter = None

    @injector.construct
    def __init__(self):
        root = pathlib.Path(__file__).absolute().parent
        with open(root / "bufr_map.yaml", "r") as h:
            raw = yaml.safe_load(h.read()) or {}
            self._bufr_map = {
                str(x): self.standardize_instruction(raw[x])
                for x in raw
            }

    def lookup(self, descriptor_id : int | str):
        key = str(int(descriptor_id))
        if key in self._bufr_map:
            return self._bufr_map[key]
        return None

    def lookup_encode(self, descriptor_id: int | str) -> dict | list | None:
        key = str(int(descriptor_id))
        if key in self._bufr_map and 'encode' in self._bufr_map[key]:
            return self._bufr_map[key]['encode']
        return None

    def standardize_units(self, unit: str):
        if unit in ('Numeric', 'CCITT IA5', 'CODE TABLE'):
            return None
        return self.converter.standardize(unit)

    def standardize_instruction(self, instruction: str | dict) -> dict | None:
        if isinstance(instruction, str):
            pieces = instruction.split(":")
            if pieces[0] == "noop":
                return {
                    "instruction": "noop",
                    "raw": True
                }
            elif pieces[0] == "recordset":
                return {
                    "instruction": "apply_to_recordset",
                    "name": pieces[1]
                }
            elif pieces[0] == 'next_recs':
                return {
                    "name": pieces[1],
                    "instruction": "apply_to_subrecords"
                }
            elif pieces[0] == "next_vars":
                return {
                    "name": pieces[1],
                    "instruction": "apply_to_parameters"
                }
            else:
                return {
                    "name": pieces[0],
                    "instruction": "apply_to_target"
                }
        elif not isinstance(instruction, dict):  # pragma: no coverage (fallback)
            return None
        else:
            base: dict[str, t.Any] = {
                "instruction": "apply_to_target" if "name" in instruction else "noop"
            }
            base.update(instruction)
            base["raw"] = base["instruction"] in ("instruction_map", "noop", "set_scale_factor", "error", "mapped")
            if 'context' in base and base['context']:
                for x in base['context']:
                    base['context'][x] = self.standardize_instruction(base['context'][x])
            if 'instruction_map' in base and base['instruction_map']:
                for x in base['instruction_map']:
                    base['instruction_map'][x] = self.standardize_instruction(base['instruction_map'][x])
            if 'encode' in base and base['encode']:
                base['encode'] = self.standardize_encode_instruction(base['encode'])
            return base


    def standardize_encode_instruction(self, instruction: str | list | dict):
        if isinstance(instruction, list):
            return [self.standardize_encode_instruction(x) for x in instruction]
        else:
            if isinstance(instruction, str):
                descriptor, instruction = instruction.split(":", maxsplit=1)

                if instruction == "NULL":
                    return {'descriptor': int(descriptor), 'instruction': 'null'}
                if instruction == "record" or instruction == "recordset":
                    return {'descriptor': int(descriptor), 'instruction': 'sequence', 'pass': instruction, 'optional': False}
                if instruction == "record[optional]" or instruction == "recordset[optional]":
                    return {'descriptor': int(descriptor), 'instruction': 'sequence', 'pass': instruction[:-10], 'optional': True}
                if any(instruction.startswith(x) for x in ("metadata/", "parameters/", "coordinates/", "recordset/")):
                    if "[" not in instruction:
                        return {'descriptor': int(descriptor), 'instruction': 'extract_ocproc2', 'source': instruction}
                    else:
                        instruction, filter = instruction.split("[", maxsplit=1)
                        return {'descriptor': int(descriptor), 'instruction': 'extract_ocproc2', 'source': instruction, "filters": self.standardize_filters(filter[:-1])}
                if instruction.startswith("common/"):
                    if "[" not in instruction:
                        return {'descriptor': int(descriptor), 'instruction': 'extract_common_ocproc2', 'source': instruction}
                    else:
                        instruction, filter = instruction.split("[", maxsplit=1)
                        return {'descriptor': int(descriptor), 'instruction': 'extract_common_ocproc2', 'source': instruction, "filters": self.standardize_filters(filter[:-1])}
                if instruction.startswith("common["):
                    element_filter_end = instruction.find("]")
                    element_filter = instruction[7:element_filter_end].split("|")
                    instruction = f"common{instruction[element_filter_end+1:]}"
                    if "[" not in instruction:
                        return {'descriptor': int(descriptor), 'instruction': 'extract_common_ocproc2', 'source': instruction, "elements": element_filter}
                    else:
                        instruction, filter = instruction.split("[", maxsplit=1)
                        return {'descriptor': int(descriptor), 'instruction': 'extract_common_ocproc2', 'source': instruction, "filters": self.standardize_filters(filter[:-1]), "elements": element_filter}
                if instruction.startswith("recordset_size"):
                    return {'descriptor': int(descriptor), 'instruction': 'recordset_size'}
                if instruction.endswith("[ifany]"):
                    return {'descriptor': int(descriptor), 'instruction': 'static', 'value': instruction[:-7], 'optional': True}
                else:
                    return {'descriptor': int(descriptor), 'instruction': 'static', 'value': instruction, 'optional': False}
            else:
                if 'group' in instruction:
                    instruction['group'] = self.standardize_encode_instruction(instruction['group'])
                    instruction['instruction'] = 'group'
                elif 'instruction' not in instruction:
                    instruction['instruction'] = 'extract_ocproc2'
                return instruction

    def standardize_filters(self, filters: str) -> dict:
        new_filters = {}
        for x in filters.split(";"):
            if "=" in x:
                k, v = x.split("=", maxsplit=1)
                new_filters[k] = v
            else:
                new_filters[x] = True
        return new_filters





class Bufr4Decoder(GtsSubDecoder):

    bufr_tables: BufrCDSTables = None

    @injector.construct
    def __init__(self):
        pass

    def get_message_type(self, reader: ByteSequenceReader, header: str) -> t.Hashable:
        content = bytearray()
        try:
            reader.consume(4)
            message_length = int.from_bytes(reader.consume(3), 'big')
            bufr_version = int(reader.consume(1)[0])
            content.extend(b'BUFR')
            content.extend(message_length.to_bytes(3, 'big'))
            content.extend(bufr_version.to_bytes(1, 'big'))
            content.extend(reader.consume(message_length - 8))
            if bufr_version != 4:
                return f"bufr{bufr_version}:error"
            instance = _Bufr4Decoder(header, content, self.bufr_tables)
            return instance.get_message_type()
        except Exception:
            import traceback
            traceback.print_exc()
            return f"bufr4:error"

    def supports_multiple_records(self) -> bool:
        return True

    def encode_from_records(self, records: list[ParentRecord], **kwargs) -> t.Iterable[bytes | bytearray]:
        encoder = _Bufr4Encoder(self.bufr_tables, records, **kwargs)
        content = encoder.encode()
        yield b'BUFR'
        yield (len(content) + 8).to_bytes(3, 'big')
        yield (4).to_bytes(1, 'big')
        yield content

    def decode_from_bytes(self, reader: ByteSequenceReader, header: str, skip_decode: bool) -> DecodeResult:
        content = bytearray()
        try:
            reader.consume(4)
            message_length = int.from_bytes(reader.consume(3), 'big')
            bufr_version = int(reader.consume(1)[0])
            content.extend(b'BUFR')
            content.extend(message_length.to_bytes(3, 'big'))
            content.extend(bufr_version.to_bytes(1, 'big'))
            content.extend(reader.consume(message_length - 8))
            original_data = header.encode('ascii') + b'\n' + content
            if skip_decode:
                return DecodeResult(skipped=True, original=original_data)
            if bufr_version != 4:
                raise CNODCError("Only BUFR4 is supported", "BUFR_DECODE", 2000)
            instance = _Bufr4Decoder(header, content, self.bufr_tables)
            return DecodeResult(
                records=[x for x in instance.convert_to_records()],
                original=original_data,
            )
        except Exception as ex:
            return DecodeResult(
                exc=ex,
                original=header.encode('ascii') + b"\n" + content
            )

class _Bufr4DecoderContext:

    def __init__(self, subset_no=None):
        self.subset = subset_no
        self.hierarchy = []
        self.top: t.Optional[ocproc2.ParentRecord] = None
        self.target: t.Optional[ocproc2.BaseRecord] = None
        self.parent_target = None
        self.var_metadata = {}
        self.record_metadata = {}
        self.node_list = None
        self.current_idx = None
        self.skip = None
        self.child_record_type = None
        self.scale_factor = None
        self.target_subset: t.Optional[ocproc2.RecordSet] = None
        self.recordset_metadata = {}

    def copy(self):
        new = _Bufr4DecoderContext(self.subset)
        new.hierarchy = [x for x in self.hierarchy]
        new.target = self.target
        new.top = self.top
        new.var_metadata = {x: self.var_metadata[x] for x in self.var_metadata}
        new.record_metadata = {x: self.record_metadata[x] for x in self.record_metadata}
        new.recordset_metadata = {x: self.recordset_metadata[x] for x in self.recordset_metadata}
        return new

    def start_iteration(self, node_list):
        self.skip = 0
        self.current_idx = 0
        self.node_list = node_list

    def start_new_recordset(self, rs: ocproc2.RecordSet):
        if self.recordset_metadata:
            rs.metadata.update(self.recordset_metadata)
        self.recordset_metadata = {}

    def peek(self, look_ahead: int):
        if self.node_list:
            new_idx = self.current_idx + look_ahead
            if 0 <= new_idx <= len(self.node_list):
                return self.node_list[new_idx]
        return None

    def start_new_record(self):
        self.close_subrecord()
        self.target = ocproc2.ChildRecord()

    def close_subrecord(self):
        if self.target:
            if self.record_metadata:
                for x in self.record_metadata:
                    if self.record_metadata[x][1] is None or any(
                        x in self.parent_target.parameters or x in self.parent_target.coordinates
                        for x in self.record_metadata[x][1]
                    ):
                        self.target.set(x, self.record_metadata[x][0])
            self.target_subset.records.append(t.cast(ocproc2.ChildRecord, self.target))
            self.target = None

    def set_recordset_property(self, property_name: str, value: ocproc2.AbstractElement):
        if self.target_subset is not None:
            self.target_subset.metadata[property_name] = value if value.value is not None else None
        else:
            self.recordset_metadata[property_name] = value if value.value is not None else None

    def set_record_property(self, property_full_name: str, value: ocproc2.AbstractElement | None):
        if value is None or value.value is None or self.target is None:
            return
        if self.var_metadata:
            pieces = property_full_name.split('/')
            for a, x in self.var_metadata.items():
                if x[2] is None or pieces[-1] in x[2]:
                    value.metadata[x[0]] = x[1]
        self.target.set(property_full_name, value)

    def add_future_parameter_metadata(self, applied_from: int, property_name, value, limit_to_parameters: t.Optional[list[str]] = None):
        if value.value is not None:
            self.var_metadata[applied_from] = (property_name, value, limit_to_parameters or None)
        elif applied_from in self.var_metadata:
            del self.var_metadata[applied_from]

    def add_future_subrecord_data(self, property_name, value, limit_to_subrecord_types: t.Optional[list[str]] = None):
        if value.value is not None:
            self.record_metadata[property_name] = (value, limit_to_subrecord_types)
        elif property_name in self.record_metadata:
            del self.record_metadata[property_name]


class _Bufr4Encoder:

    def __init__(self,
                 cds_tables: BufrCDSTables,
                 records: list[ParentRecord],
                 override_template: str | None = None,
                 default_template: str | None = None,
                 ):
        self._cds_tables = cds_tables
        self._records = records
        self._override_template = override_template
        self._default_template = default_template
        self._default_originating_centre = 0
        self._default_originating_subcentre = 0
        self._always_override_centre = True
        self._default_master_table = 0
        self._default_master_table_version = 42
        self._default_local_table_version = 0
        self._table_group: BufrTableGroup | None = None
        self._context = {}

    @contextlib.contextmanager
    def subcontext(self):
        old_ctx = deepcopy(self._context)
        try:
            yield self._context
        finally:
            self._context = old_ctx

    def determine_encoding_template(self, record_template: list[int | str] | None) -> list[int]:
        if self._override_template:
            return [int(x) for x in self._override_template.split(",")]
        if record_template:
            return [int(x) for x in record_template]
        if self._default_template:
            return [int(x) for x in self._default_template.split(",")]
        raise ValueError("No template found")

    def _build_section_one(self) -> list:
        return [
            "BUFR",     # Indicates bufr message
            0,          # Let pybufrkit handle the length
            4           # version of BUFR
        ]

    def _build_section_two(self,
                           originating_centre: int,
                           originating_subcentre: int,
                           data_category: int,
                           year: int,
                           month: int,
                           day: int,
                           hour: int,
                           minute: int,
                           second: int,
                           data_subcategory: int = 0,
                           master_table_number: int = 0,
                           local_subcategory: int = 0,
                           master_table_version: int = 33,
                           local_table_version: int = 0) -> list:
        return [
            0,          # Let pybufrkit handle the length
            master_table_number,
            originating_centre,
            originating_subcentre,
            0,          # update sequence number
            False,      # is optional sequence present? true/false
            "0000000",      #
            data_category,
            data_subcategory,
            local_subcategory,
            master_table_version,
            local_table_version,
            year,
            month,
            day,
            hour,
            minute,
            second,
            "",         # additional local data
        ]

    def _build_section_three(self,
                             subsets: int,
                             descriptors: list[int],
                             is_observed: bool = True,
                             is_compressed: bool = False) -> list:
        return [
            0,              # let pybufrkit calculate length
            "00000000",     # leave blank
            subsets,
            is_observed,
            is_compressed,
            "000000",       # leave blank
            descriptors
        ]

    def _build_section_four(self,
                            descriptors: list[int]) -> list:
        return [
            0,
            "00000000",
            [
                self._build_section_four_subset(record, descriptors)
                for record in self._records
            ]
        ]

    def _build_section_five(self) -> list:
        return ["7777"]

    def earliest_time(self) -> AwareDateTime:
        earliest = AwareDateTime.utcnow()
        for record in self._records:
            if record.coordinates.has_value("Time"):
                record_time = record.coordinates.ideal("Time").to_datetime()
                if record_time < earliest:
                    earliest = record_time
        return earliest.astimezone("Etc/UTC")

    def encode(self) -> bytes | bytearray:
        metadata = self._records[0].metadata
        earliest = self.earliest_time()
        descriptors = self.determine_encoding_template(metadata.best("BUFRDescriptors", coerce=list, default=None))
        master_table_number = metadata.best("BUFRMasterTable", default=self._default_master_table, coerce=int)
        master_table_version = metadata.best("BUFRMasterTableVersion", default=self._default_master_table_version, coerce=int)
        local_table_version = metadata.best("BUFRLocalTableVersion", default=self._default_local_table_version, coerce=int)
        originating_centre = self._default_originating_centre if self._always_override_centre else metadata.best("BUFROriginCentre", coerce=int, default=self._default_originating_centre)
        originating_subcentre = self._default_originating_subcentre if self._always_override_centre else metadata.best("BUFROriginSubcentre", coerce=int, default=self._default_originating_subcentre)
        self._table_group = TableGroupCacheManager.get_table_group(
            master_table_number=master_table_number,
            originating_centre=originating_centre,
            originating_subcentre=originating_subcentre,
            master_table_version=master_table_version,
            local_table_version=local_table_version
        )
        encoder = Encoder()
        message = encoder.process([
            self._build_section_one(),
            self._build_section_two(
                master_table_number=master_table_number,
                master_table_version=master_table_version,
                local_table_version=local_table_version,
                originating_centre=originating_centre,
                originating_subcentre=originating_subcentre,
                data_category=metadata.best("BUFRDataCategory", default=31, coerce=int),
                year=earliest.year,
                month=earliest.month,
                day=earliest.day,
                hour=earliest.hour,
                minute=earliest.minute,
                second=earliest.second
            ),
            self._build_section_three(
                subsets=len(self._records),
                descriptors=descriptors,
                is_observed=metadata.best("BUFRIsObservation", coerce=int, default=1) == 1,
            ),
            self._build_section_four(
                descriptors=descriptors
            ),
            self._build_section_five(),
        ])
        return message.serialized_bytes

    def _build_section_four_subset(self, record: ParentRecord, descriptors: list[int]) -> list:
        subset = []
        for descriptor in descriptors:
            encoded = self._build_from_descriptor(self.table_group.lookup(descriptor), record=record)
            subset.extend(encoded.assemble())
        return subset

    @property
    def table_group(self) -> BufrTableGroup:
        if self._table_group is None:
            raise ValueError("called too early")
        else:
            return self._table_group

    def _build_from_descriptor(self, descriptor, **kwargs) -> EncodeElement:
        if isinstance(descriptor, SequenceDescriptor):
            yielder = None
            method_name = f"_build_from_{descriptor.id}"
            if hasattr(self, method_name):
                yielder = getattr(self, method_name)(**kwargs)
            else:
                instruction = self._cds_tables.lookup_encode(descriptor.id)
                if instruction is not None:
                    yielder = self._build_from_instruction(instruction, **kwargs)

            if yielder is not None:
                elements = [x for x in self._filter_results(descriptor, yielder)]
                return EncodeElement(descriptor.id, elements)

        raise ValueError(f"Invalid descriptor: {type(descriptor)} ")

    def _filter_results(self, descriptor: SequenceDescriptor, results: t.Iterable[EncodeElement]) -> t.Iterable[EncodeElement]:
        for x in descriptor.members:
            if x.id >= 100000 and x.id < 200000:
                repeats = x.id % 100
                if repeats == 0:
                    result_next = next(results)
                    if result_next.descriptor in (31000, 31001, 31002):
                        repeats = result_next.value
                        yield result_next
                    else:
                        raise TypeError("expecting repeat flag")
                for i in range(0, repeats):
                    yield from self._filter_results(x, results)
            else:
                result_next = next(results)
                while x.id != result_next.descriptor:
                    result_next = next(results)
                yield result_next


    def _build_from_instruction(self, instruction: dict | list, **kwargs) -> t.Iterable[EncodeElement]:
        if isinstance(instruction, list):
            for idx, i in enumerate(instruction):
                if isinstance(i, dict) and i["instruction"] == "extract_common_ocproc2":
                    yield self._build_from_common(idx, instruction, **kwargs)
                else:
                    yield from self._build_from_instruction(i, **kwargs)
        elif instruction["instruction"] == "extract_ocproc2":
            if instruction["source"].startswith("recordset/"):
                _, metadata = instruction["source"].split("/", 1)
                yield EncodeElement(instruction["descriptor"], self._build_from_ocproc2(
                    kwargs["recordset"].metadata.ideal(metadata),
                    instruction["descriptor"],
                    filters=instruction["filters"] if "filters" in instruction else None,
                    value_map=instruction["data_map"] if "data_map" in instruction else None
                ))
            else:
                yield EncodeElement(instruction["descriptor"], self._build_from_ocproc2(
                    kwargs["record"].find_child(instruction["source"]),
                    instruction["descriptor"],
                    filters=instruction["filters"] if "filters" in instruction else None,
                    value_map=instruction["data_map"] if "data_map" in instruction else None
                ))
        elif instruction["instruction"] == "null":
            yield EncodeElement(instruction["descriptor"], None)
        elif instruction["instruction"] == "static":
            yield EncodeElement(
                instruction["descriptor"],
                self._build_from_raw_value(instruction["value"], instruction["descriptor"]),
                optional=instruction["optional"]
            )
        elif instruction["instruction"] == "sequence":
            if instruction["optional"]:
                yield from self._build_optional_block(
                    self._handle_sequence_instruction,
                    instruction=instruction,
                    **kwargs
                )
            else:
                yield self._handle_sequence_instruction(instruction, **kwargs)
        elif instruction["instruction"] == "group":
            if "recordset_type" in instruction:
                kwargs["recordset"] = self._find_first_recordset(
                    record=kwargs["record"],
                    recordset_type=instruction["recordset_type"],
                    output_elements=instruction["requires"] if "requires" in instruction else []
                )
                if "is_optional" in instruction and instruction["is_optional"]:
                    if kwargs["recordset"] is None:
                        yield EncodeElement(31000, 0)
                        return
                    else:
                        yield EncodeElement(31000, 1)
            if "repeats" in instruction:
                yield from self._build_from_repeat_instruction(instruction, **kwargs)
            else:
                yield from self._build_from_instruction(instruction['group'], **kwargs)
        else:
            print(instruction)
            exit(1)

    def _build_from_repeat_instruction(self, instruction, record: BaseRecord, recordset: RecordSet | None) -> t.Iterable[EncodeElement]:
        if recordset is None:
            if "size_descriptor" in instruction and instruction["size_descriptor"]:
                yield EncodeElement(instruction["size_descriptor"], 0, optional=True)
        else:
            record_count = 0
            records = []
            max_records = instruction["repeats"]
            for r in recordset.records:
                subrecord = [x for x in self._build_from_instruction(instruction['group'], record=r)]
                if all(x is None or x.optional for x in subrecord):
                    continue
                else:
                    record_count += 1
                    records.extend(subrecord)
                if max_records != "*" and int(max_records) == record_count:
                    break
            if "size_descriptor" in instruction and instruction["size_descriptor"]:
                yield EncodeElement(instruction["size_descriptor"], record_count, optional=True)
            yield from records

    def _build_from_common(self, current_idx: int, instruction: list[dict], **kwargs):
        common_instruction = instruction[current_idx]
        _, source_name = common_instruction["source"].rsplit("/", maxsplit=1)
        filters = common_instruction["filters"] if "filters" in common_instruction else None
        common_elements = [x for x in self._extract_common_elements(instruction[current_idx+1:], elements=common_instruction["elements"] if "elements" in common_instruction else None, **kwargs)]
        common_value = self._extract_common_metadata_element(
            common_elements,
            attribute_name=source_name,
            filters=filters,
            units=self._cds_tables.standardize_units(self.table_group.lookup(common_instruction["descriptor"]).unit)
        )
        if common_value is not None:
            if source_name == "SensorDepth" and "SensorDepthReference" in filters and filters["SensorDepthReference"] in ("water", "local_ground"):
                common_value = -1 * float(common_value)
        return EncodeElement(common_instruction["descriptor"], common_value)

    def _extract_common_elements(self,
                                 instruction: list[dict],
                                 record: BaseRecord = None,
                                 recordset: RecordSet = None,
                                 elements: list[str] | None = None,
                                 filters: dict[str, t.Any] | None = None,
                                 **kwargs) -> t.Iterable[ocproc2.SingleElement | None]:
        for x in instruction:
            if x["instruction"] == "extract_ocproc2":
                if record is not None:
                    if filters is None or any(y in x["source"] for y in elements):
                        yield self._find_ocproc2(t.cast(AbstractElement | None, record.find_child(x["source"])), filters=x['filters'] if 'filters' in x else None)
            elif x["instruction"] in ("extract_common_ocproc2", "recordset_size", "static"):
                continue
            elif x["instruction"] == "sequence":
                yield from self._extract_common_elements(
                    t.cast(list[dict], self._cds_tables.lookup_encode(x["descriptor"])),
                    record=record,
                    recordset=recordset if x["pass"] == "recordset" else None,
                    elements=elements,
                    filters=filters
                )
            elif x["instruction"] == "group":
                if "repeats" in x:
                    if recordset:
                        for r in recordset.records:
                            yield from self._extract_common_elements(x["group"], record=r, elements=elements, filters=filters)
                else:
                    yield from self._extract_common_elements(
                        x["group"],
                        record=record,
                        recordset=recordset,
                        elements=elements,
                        filters=filters
                    )
            else:
                print(x)
                exit(2)

    def _handle_sequence_instruction(self, instruction, **kwargs) -> EncodeElement:
        if instruction["pass"] == "recordset":
            return self._build_from_descriptor(
                self.table_group.lookup(instruction["descriptor"]),
                record=kwargs["record"],
                recordset=kwargs["recordset"],
            )
        else:
            return self._build_from_descriptor(
                self.table_group.lookup(instruction["descriptor"]),
                record=kwargs["record"],
            )

    def _build_optional_block(self, callback: t.Callable[..., EncodeElement], **kwargs) -> t.Iterable[EncodeElement]:
        optional_content = callback(**kwargs)
        if all(x is None or x.optional for x in optional_content.value):
            yield EncodeElement(31000, 0)
        else:
            yield EncodeElement(31000, 1)
            yield optional_content

    def _build_from_ocproc2(self, element: ocproc2.AbstractElement | None, descriptor_id: int, filters: dict | None = None, value_map: dict | None = None):
        actual_element = self._find_ocproc2(element, filters, value_map)
        if actual_element is not None:
            return self._build_from_single_ocproc2(actual_element, descriptor_id)
        return None

    def _find_ocproc2(self, element: ocproc2.AbstractElement | None, filters: dict | None = None, value_map: dict | None = None) -> ocproc2.SingleElement | None:
        if element is None or element.is_empty():
            return None
        value = None
        if filters is not None:
            for x in ("year", "month", "day", "hour", "minute", "second"):
                if x in filters and filters[x]:
                    v = element.ideal().to_datetime()
                    value = ocproc2.SingleElement(getattr(v, x))
                    break
            else:
                for e in element.all_values():
                    if all(e.metadata.has_value(x) and e.metadata.best(x) == filters[x] for x in filters):
                        value = e
                        break
        else:
            value = element.ideal()
        if value is not None and value_map is not None and value.value in value_map:
            value = ocproc2.SingleElement(value_map[value.value])
        return value

    def _build_from_raw_value(self,
                              value: t.Any,
                              descriptor_id: int,
                              value_units: str | None = None) -> t.Any:
        if value is None or value == "":
            return None
        descriptor = self.table_group.lookup(descriptor_id)
        if descriptor.unit == "CCITT IA5":
            return str(value)
        elif descriptor.unit == "CODE TABLE":
            return int(value)
        elif descriptor.unit == "Numeric" or value_units is None:
            return float(value)
        else:
            units = descriptor.unit
            if units == "degree true":
                units = "degrees"
            return convert(float(value), value_units, units)

    def _build_from_single_ocproc2(self,
                                   value: ocproc2.SingleElement,
                                   descriptor_id: int) -> t.Any:
        if value.is_empty():
            return None
        else:
            descriptor = self.table_group.lookup(descriptor_id)
            if descriptor.unit.upper() == "CCITT IA5":
                return value.to_string()
            elif descriptor.unit.upper() == "CODE TABLE":
                return value.to_int()
            elif descriptor.unit.upper() == "NUMERIC":
                return value.to_float()
            else:
                units = descriptor.unit
                if units.lower() == "degree true":
                    units = "degrees"
                return value.to_float(self._cds_tables.standardize_units(units))

    def _find_first_recordset(self, record, recordset_type: str, output_elements: list[str]) -> RecordSet | None:
        found_rs = None
        matches = 0
        for recordset_id, recordset in record.subrecords.record_sets[recordset_type].items():
            found_elements = set()
            for record in recordset.records:
                found_elements.update(record.parameters.keys())
                found_elements.update(record.coordinates.keys())
            matches_for_rs = sum(1 if x in found_elements else 0 for x in output_elements)
            if matches_for_rs > matches:
                found_rs = recordset
                matches = matches_for_rs
        return found_rs

    def _extract_common_observation_period(self, *elements, descriptor_id):
        return self._convert_duration(self._extract_common_metadata_element(elements, "ObservationPeriod"), descriptor_id)

    def _extract_time_offset(self, time_offset: ocproc2.SingleElement | None, descriptor_id):
        if time_offset is None or time_offset.is_empty():
            return None
        return self._convert_duration(time_offset.to_string(), descriptor_id)

    def _convert_duration(self, obs_time: str, descriptor_id: int):
        if obs_time is None:
            return None
        from medsutil.iso_duration import ISODuration
        iso_duration = ISODuration.from_iso_format(obs_time)
        if iso_duration.years == 0 and iso_duration.months == 0 and iso_duration.days == 0:
            return self._convert_time_period(iso_duration.time_part_total_seconds(), "s", descriptor_id)
        elif iso_duration.hours == 0 and iso_duration.minutes == 0 and iso_duration.seconds == 0:
            if iso_duration.years == 0 and iso_duration.months == 0:
                return self._convert_time_period(iso_duration.days, "d", descriptor_id)
            elif iso_duration.months == 0 and iso_duration.days == 0:
                return self._convert_time_period(iso_duration.years, "a", descriptor_id)
            elif iso_duration.days == 0 and iso_duration.years == 0:
                return self._convert_time_period(iso_duration.months, "mon", descriptor_id)
            else:
                print("we can't combine these easily in BUFR")
                return None
        else:
            print("we can't combine these easily in BUFR")
            return None

    def _convert_time_period(self, duration: int, from_units: str, descriptor_id: int):
        to_units = self.table_group.lookup(descriptor_id).units
        if to_units == "s":
            if from_units == "s": return duration
            elif from_units == "min": return duration * 60
            elif from_units == "h": return duration * 60 * 60
            else: print("can't easily convert days or more to seconds")
        elif to_units == "min":
            if from_units == "min": return duration
            elif from_units == "h": return duration * 60
            elif from_units == "s": return int(duration / 60)
            else:
                print("can't easily convert days or more to minutes")
        elif to_units == "h":
            if from_units == "min": return int(duration / 60)
            elif from_units == "h": return duration
            elif from_units == "s": return int(duration / 3600)
            else:
                print("can't easily convert days or more to hours")
        elif to_units == "d":
            if from_units == "d": return duration
            else: print("can't easily convert anything to days")
        elif to_units == "mon":
            if from_units == "mon": return duration
            elif from_units == "a": return duration * 12
            else: print("can't easily convert days or less to months")
        elif to_units == "a":
            if from_units == "a": return duration
            else: print("can't easily convert anything to years")
        else:
            print("unrecognized units")
        return None

    def _extract_common_metadata_element(self,
                                         elements: t.Iterable[ocproc2.SingleElement | None],
                                         attribute_name: str,
                                         filters: dict[str, str] | None = None,
                                         units: str | None = None) -> t.Any:
        found_code = None
        for element in elements:
            found_element = None
            if element is None or not attribute_name in element.metadata:
                ...
            elif not filters:
                found_element = element.metadata.ideal(attribute_name)
            else:
                for selement in element.metadata[attribute_name].all_values():
                    if all(x in selement.metadata and selement.metadata.best(x, coerce=str, default=None) == filters[x] for x in filters):
                        found_element = selement
                        break
            if found_element is None:
                continue
            if units is not None:
                current_code = found_element.to_decimal(units)
            else:
                current_code = found_element.value
            if found_code is None:
                found_code = current_code
            elif current_code != found_code:
                found_code = None
                break
        return found_code

class EncodeElement:

    def __init__(self, descriptor_id: int, value: t.Any, optional: bool = False):
        self.descriptor = descriptor_id
        self.value = value
        self.optional = optional

    def assemble(self) -> t.Iterable[t.Any]:
        if isinstance(self.value, list):
            for x in self.value:
                if isinstance(x, EncodeElement):
                    yield from x.assemble()
                else:
                    yield x
        else:
            yield self.value










class _Bufr4Decoder:

    BUFR_MESSAGE_CODES = {
        315008,
        315009,
        315004,
        315007,
        315011,
        315003,
        308015,
        307079
    }

    BUFR_EQUIVALENT_CODES = {
        5001: [5002],
        5002: [5001],
        6001: [6002],
        6002: [6001],
        1015: [1019],
        1019: [1015],
    }

    def __init__(self, header, content: t.Union[bytearray, bytes], bufr_tables: BufrCDSTables):
        self.bufr_tables = bufr_tables
        self.header = header
        self.log = zrlog.get_logger("cnodc.bufr_decoder")
        decoder = Decoder()
        self.raw_content = content
        self.message = decoder.process(self.raw_content)
        self.raw_data: TemplateData = self.message.template_data.value
        self.pybufr_tables = TableGroupCacheManager.get_table_group_by_key(self.message.table_group_key)

    def get_message_type(self) -> str:
        return f"bufr4:{';'.join(str(x) for x in self.message.unexpanded_descriptors.value)}"

    def get_text_representation(self) -> str:
        return NestedTextRenderer().render(self.message)

    def error(self, message, ctx: t.Optional[_Bufr4DecoderContext]):
        self.log.error("{txt} [{hierarchy}] [{header}]".format(
                            txt=message,
                            header=self.header,
                            hierarchy='>'.join(str(x) for x in ctx.hierarchy) if ctx else ''
        ))
        ctx.top.report_error(message, 'bufr_decode', '1_0', '')

    def warn(self, message, ctx: _Bufr4DecoderContext = None):
        self.log.warning("{txt} [{hierarchy}] [{header}]".format(
                            txt=message,
                            header=self.header,
                            hierarchy='>'.join(str(x) for x in ctx.hierarchy) if ctx else ''
        ))
        ctx.top.report_warning(message, 'bufr_decode', '1_0', '')

    def convert_to_records(self) -> t.Iterable[ocproc2.ParentRecord]:
        pieces = self.header.split(' ')
        if len(pieces) > 3 and pieces[3][0] in ('C', 'A', 'P'):
            raise CNODCError("BUFR decoder not configured to properly handle CCx AAx or Pxx messages", "BUFR_DECODE", 1000)
        descriptors = list(x for x in self.message.unexpanded_descriptors.value)
        common_metadata = {
            'GTSHeader': self.header,
            'BUFRDescriptors': descriptors,
            'BUFRInferredMessageType': self._identify_bufr_message_type(descriptors),
            'BUFROriginCentre': self.message.originating_centre.value,
            'BUFROriginSubcentre': self.message.originating_subcentre.value,
            'BUFRDataCategory': self.message.data_category.value,
            'BUFRMasterTableVersion': self.message.master_table_version.value,
            'BUFRLocalTableVersion': self.message.local_table_version.value,
            'BUFRMasterTable': self.message.master_table_number.value,
            'BUFRIsObservation': 1 if self.message.is_observation.value else 0,
            'BUFRMessageTime': awaretime.utc_awaretime(
                year=self.message.year.value,
                month=self.message.month.value,
                day=self.message.day.value,
                hour=self.message.hour.value,
                minute=self.message.minute.value,
                second=self.message.second.value
            ).isoformat()
        }
        for n in range(0, self.message.n_subsets.value):
            yield self._convert_subset_to_record(n, common_metadata)

    def _expand_bufr_descriptors(self, descriptors: list[int], pbt):
        def _expand_from_members(d):
            new_d = []
            for descriptor in d:
                if descriptor.id < 100000:
                    new_d.append(descriptor)
                elif descriptor.id < 200000:
                    new_d.append(descriptor)
                    if hasattr(descriptor, 'factor') and descriptor.factor is not None:
                        new_d.append(descriptor.factor)
                    if descriptor.members:
                        new_d.extend(_expand_from_members(descriptor.members))
                elif descriptor.id >= 300000:
                    new_d.extend(_expand_from_members(descriptor.members))
            return new_d
        y = _expand_from_members([pbt.lookup(d) for d in descriptors])
        return [x.id for x in y]

    def _identify_bufr_message_type(self, descriptors: list[int]):
        for x in _Bufr4Decoder.BUFR_MESSAGE_CODES:
            if x in descriptors:
                return [x]
        this_message = self._expand_bufr_descriptors(descriptors, self.pybufr_tables)
        for version in range(39, 5, -1):
            results = []
            pbt = TableGroupCacheManager.get_table_group_by_key(
                TableGroupKey(self.message.table_group_key.tables_root_dir, ('0', '0_0', str(version)), None, None)
            )
            for x in _Bufr4Decoder.BUFR_MESSAGE_CODES:
                unpacked = pbt.lookup(x)
                if isinstance(unpacked, pybufrkit.descriptors.UndefinedSequenceDescriptor):
                    continue
                compare_to = self._expand_bufr_descriptors([x], pbt)
                d = self._descriptor_distance(compare_to, this_message)
                if d > -1:
                    results.append(x)
            if results:
                return results
        return []

    def _descriptor_distance(self, received: list[int], check_against: list[int]):
        if received[0] not in check_against:
            return -1
        current_idx = check_against.index(received[0])
        gaps = 0
        for idx in range(0, len(received)):
            find = [received[idx]]
            if received[idx] in _Bufr4Decoder.BUFR_EQUIVALENT_CODES:
                find.extend(_Bufr4Decoder.BUFR_EQUIVALENT_CODES[received[idx]])
            best_idx: int | None = None
            for x in find:
                if x in check_against[current_idx:]:
                    idx2 = check_against[current_idx:].index(x) + current_idx
                    if best_idx is None or idx2 < best_idx:
                        best_idx = idx2
            if best_idx is None:
                return -1
            gaps += best_idx - current_idx
            current_idx = best_idx + 1
        return gaps

    def _convert_subset_to_record(self, subset_number, common_metadata: dict) -> ocproc2.ParentRecord:
        ctx = _Bufr4DecoderContext(subset_number)
        ctx.target = ctx.top = ocproc2.ParentRecord()
        ctx.target.metadata.update(common_metadata)
        ctx.target.metadata['BUFRSubsetIndex'] = subset_number
        ctx.hierarchy = []
        ctx.hierarchy = [f'M#{subset_number}']
        self._iterate_on_nodes(self.raw_data.decoded_nodes_all_subsets[subset_number], ctx)
        return t.cast(ocproc2.ParentRecord, ctx.target)

    def _iterate_on_nodes(self, nodes: list, ctx: _Bufr4DecoderContext):
        ctx2 = ctx.copy()
        ctx2.start_iteration(nodes)
        ctx2.hierarchy.append("")
        for idx, node in enumerate(nodes):
            ctx2.hierarchy[-1] = f"{str(node.descriptor.id)}[{idx}]"
            ctx2.current_idx = idx
            if ctx2.skip > 0:
                ctx2.skip -= 1
                continue
            self._parse_node(node, ctx2)

    def _parse_node(self, node, ctx, _skip_custom_check: bool = False):
        if not _skip_custom_check:
            test_name = f"_parse_node_{node.descriptor.id}"
            # Custom handling
            if hasattr(self, test_name):
                getattr(self, test_name)(node, ctx)
                return
        # Sequence node
        if isinstance(node, SequenceNode):
            instruction = self.bufr_tables.lookup(node.descriptor.id)
            if instruction:
                self._apply_instruction(instruction, None, ctx, node)
            elif node.members:
                self._iterate_on_nodes(node.members, ctx)
        # Delayed replication
        elif isinstance(node, DelayedReplicationNode):
            if node.members:
                self._parse_replication_node(node, ctx)
        # Fixed replication
        elif isinstance(node, FixedReplicationNode):
            if node.members:
                self._parse_replication_node(node, ctx)
        # Value
        elif isinstance(node, ValueDataNode):
            self._parse_value_node(node, ctx)
        # Other nodes (usually instructions)
        elif isinstance(node, NoValueDataNode):
            descriptor_id = node.descriptor.id
            if 200000 <= descriptor_id < 210000:
                return
            self.warn(f"Unhandled no-value node above 210000: {node.descriptor.id}", ctx)

    def _parse_replication_node(self, node, ctx):
        n_total, n_elements, n_repeats = self._parse_repetition_info(node, ctx)
        descriptors = set()
        work = [n for n in node.members]
        while work:
            n = work.pop()
            if n.descriptor.id not in descriptors:
                descriptors.add(n.descriptor.id)
        map_to = None
        coord_name = None
        for x in descriptors:
            mapping = self.bufr_tables.lookup(x)
            if isinstance(mapping, dict) and 'subrecord_type' in mapping:
                if map_to is not None and mapping['subrecord_type'] != map_to:
                    self.warn(f"Overwriting mapping type [{map_to}] with {mapping['subrecord_type']}", ctx)
                map_to = mapping['subrecord_type']
                coord_name = (x,)
        if map_to is None and n_repeats > 1 and any(x in descriptors for x in (4021, 4022, 4023, 4024, 4025, 4026)):
            map_to = "TIME_SERIES"
            coord_name = (4021, 4022, 4023, 4024, 4025, 4026)
        if map_to is not None and coord_name is not None:
            self._iterate_into_children(node, ctx, map_to, coord_name)
        else:
            if n_repeats > 1:
                self.warn(f"Multiple repetitions without a subrecord key found: [{descriptors}]", ctx)
            self._iterate_on_nodes(node.members, ctx)

    def _parse_repetition_info(self, node: t.Union[DelayedReplicationNode, FixedReplicationNode], ctx):
        n_total = len(node.members)
        if isinstance(node, DelayedReplicationNode):
            n_repeats = self._get_node_value(node.factor, ctx)
        else:
            n_repeats = int(str(node.descriptor.id)[3:])
        n_elements = int(n_total / n_repeats)
        return n_total, n_elements, n_repeats

    def _most_frequent(self, vals: list):
        inc = vals.count(1)
        dec = vals.count(-1)
        if inc > dec:
            return 'I'
        if inc < dec:
            return 'D'

    def _iterate_into_children(self, node, ctx: _Bufr4DecoderContext, child_record_type: str, coord_names: tuple):
        n_total, n_elements, n_repeats = self._parse_repetition_info(node, ctx)
        record_set = ctx.target.subrecords.new_recordset(child_record_type)
        ctx.start_new_recordset(record_set)
        for i in range(0, t.cast(int, n_repeats)):
            ctx2 = ctx.copy()
            ctx2.parent_target = ctx.target
            ctx2.hierarchy.append(f"REPEAT{i}")
            ctx2.target = None
            ctx2.child_record_type = child_record_type
            ctx2.target_subset = record_set
            ctx2.start_new_record()
            self._iterate_on_nodes(node.members[(i*n_elements):((i+1)*n_elements)], ctx2)
            ctx2.close_subrecord()

    def _parse_value_node(self, node: ValueDataNode, ctx: _Bufr4DecoderContext):
        instruction = self.bufr_tables.lookup(node.descriptor.id)
        if instruction is None:
            self.warn(f"Unhandled node descriptor: {node.descriptor.id}: {self._get_node_value(node, ctx)}", ctx)
        else:
            self._apply_instruction(instruction, self._get_node_value(node, ctx), ctx, node)

    def _contextualize_instruction(self, instruction, ctx):
        if 'context' in instruction:
            for x in instruction['context']:
                str_x = str(x)
                if any(str_x in h for h in ctx.hierarchy):
                    return self._contextualize_instruction(instruction['context'][x], ctx)
        return instruction

    def _build_value(self, value: t.Any, instruction: dict, node, ctx) -> t.Any:
        if 'value' in instruction:
            value = instruction['value']
        elif 'value_map' in instruction:
            if value in instruction['value_map']:
                value = instruction['value_map'][value]
        elif 'data_processor' in instruction:
            value = getattr(self, instruction['data_processor'])(value, node, ctx)
        if 'raw' in instruction and instruction['raw']:
            return value
        if not isinstance(value, ocproc2.AbstractElement):
            value = ocproc2.SingleElement(value)
        if 'remove_metadata' in instruction and instruction['remove_metadata']:
            for key in instruction['remove_metadata']:
                if key in value.metadata:
                    del value.metadata[key]
        if 'metadata' in instruction and instruction['metadata']:
            for key in instruction['metadata']:
                if isinstance(instruction['metadata'][key], dict):
                    value.metadata[key] = self._build_value(None, instruction['metadata'][key], None, ctx)
                else:
                    value.metadata[key] = instruction['metadata'][key]
        if node is not None:
            units: str | None = self._get_node_units(node)
            if units != "CCITT IA5" and units != "Code table":
                if units and 'Units' not in value.metadata:
                    value.metadata['Units'] = self.bufr_tables.standardize_units(units)
                scale = self._get_node_scale(node)
                if scale and 'Uncertainty' not in value.metadata:
                    value.metadata['Uncertainty'] = scale
                    value.metadata['Uncertainty'].metadata['UncertaintyType'] = 'limited'
        return value

    def _get_node_units(self, node: ValueDataNode):
        if hasattr(node.descriptor, 'unit'):
            return self.bufr_tables.standardize_units(node.descriptor.unit)

    def _get_node_scale(self, node: ValueDataNode):
        if hasattr(node.descriptor, 'unit'):
            if node.descriptor.unit in ('CCITT IA5', 'Code table'):
                return None
        if hasattr(node.descriptor, 'scale'):
            return math.pow(10, (-1 * node.descriptor.scale)) / 2

    def _apply_instruction(self, instruction, value, ctx: _Bufr4DecoderContext, node=None):
        instruction = self._contextualize_instruction(instruction, ctx)
        if instruction is None:
            return
        value = self._build_value(value, instruction, node, ctx)
        match instruction['instruction']:
            case "noop":
                pass
            case "apply_to_target":
                ctx.set_record_property(instruction['name'], value)
            case "apply_to_parameters":
                ctx.add_future_parameter_metadata(node.descriptor.id if node else -1, instruction['name'], value, instruction['filter'] if 'filter' in instruction else None)
            case "apply_to_subrecords":
                ctx.add_future_subrecord_data(instruction['name'], value, instruction['filter'] if 'filter' in instruction else None)
            case "set_scale_factor":
                ctx.scale_factor = value
            case "apply_to_recordset":
                ctx.set_recordset_property(instruction["name"], value)
            case "mapped":
                map_key = str(value) if value is not None else ""
                for x in instruction["instruction_map"]:
                    if str(x) == map_key:
                        self._apply_instruction(instruction["instruction_map"][x], value, ctx, node)
                        break
                else:
                    self.warn(f"No instruction found for [{map_key}] (really {value}) in {node.descriptor.id if node else 'unknown'}", ctx)
            case "error":
                self.warn(f"No instruction provided for [{node.descriptor.id if node is not None else 'unknown'}]", ctx)
            case _:
                self.warn(f"Unrecognized instruction: {instruction['instruction']}", ctx)
        if 'iterate_after' in instruction and instruction['iterate_after'] and node and hasattr(node, 'members'):
            if node.members:
                self._iterate_on_nodes(node.members, ctx)

    def _get_node_value(self, node: ValueDataNode, ctx: _Bufr4DecoderContext):
        value = self.raw_data.decoded_values_all_subsets[ctx.subset][node.index]
        if isinstance(value, bytes):
            value = bytes([x for x in value if 0 < x < 128]).decode('ascii', errors='replace').strip(' ')
        elif ctx.scale_factor is not None and isinstance(value, (int, float)):
            value *= math.pow(10, ctx.scale_factor)
        if value == '' or value == b'':
            value = None
        return value

    def _parse_wmo_id(self, raw_value, node, ctx):
        if node.descriptor.id == 1087:
            if raw_value is None:
                return None
            raw_value = str(raw_value)
            if len(raw_value) < 2:
                return raw_value
            return f"{raw_value[0:2]}{raw_value[2:].zfill(5)}"
        peek1 = ctx.peek(1)
        if not (peek1 and peek1.descriptor.id in (1002, 1020, 1004)):
            return raw_value
        if peek1.descriptor != 1002:
            peek2 = ctx.peek(2)
            if not (peek2 and peek2.descriptor.id == 1005):
                return raw_value
            elements = [raw_value or "", self._get_node_value(peek1, ctx) or "", self._get_node_value(peek2, ctx) or ""]
            ctx.skip = 2
        else:
            elements = [raw_value or "", "", self._get_node_value(peek1, ctx) or ""]
            ctx.skip = 1
        if all(x is None or x == "" for x in elements):
            return None
        else:
            return f'{elements[0] or ""}{elements[1] or ""}{str(elements[2] or "").zfill(5)}'

    def _parse_node_8080(self, node, ctx: _Bufr4DecoderContext):
        nxt = ctx.peek(1)
        flag_value = None
        applies_to = self._get_node_value(node, ctx)
        if nxt and nxt.descriptor.id == 33050:
            ctx.skip += 1
            v = self._get_node_value(t.cast(ValueDataNode, nxt), ctx)
            flag_value = ocproc2.SingleElement(v) if v is not None else v
        if applies_to == 20:
            ctx.set_record_property("coordinates/Latitude/metadata/Quality", flag_value)
            ctx.set_record_property("coordinates/Longitude/metadata/Quality", flag_value)
        elif applies_to == 4:
            ctx.set_record_property("parameters/SeaDepth/metadata/Quality", flag_value)
        elif applies_to == 10:
            ctx.set_record_property("coordinates/Pressure/metadata/Quality", flag_value)
        elif applies_to == 11:
            ctx.set_record_property("parameters/Temperature/metadata/Quality", flag_value)
        elif applies_to == 12:
            ctx.set_record_property("parameters/PracticalSalinity/metadata/Quality", flag_value)
        elif applies_to == 13:
            ctx.set_record_property("coordinates/Depth/metadata/Quality", flag_value)
        elif applies_to == 14:
            ctx.set_record_property("parameters/CurrentSpeed/metadata/Quality", flag_value)
        elif applies_to == 15:
            ctx.set_record_property("parameters/CurrentDirection/metadata/Quality", flag_value)
        elif applies_to == 16:
            ctx.set_record_property("parameters/DissolvedOxygen/metadata/Quality", flag_value)
        elif applies_to == 25:
            ctx.set_record_property("parameters/Conductivity/metadata/Quality", flag_value)
        elif applies_to == 26:
            ctx.set_record_property("parameters/PotentialDensity/metadata/Quality", flag_value)
        elif applies_to is None:
            if flag_value is not None:
                self.warn(f"GTSPP quality flag applies to was none, but flag value was not-none", ctx)
        else:
            self.warn(f"unhandled GTSPP quality flag [{applies_to}]", ctx)

    def _parse_datetime_sequence(self, raw_value, node, ctx):
        return self._parse_dt_sequence(ctx, 0)

    def _parse_following_datetime_sequence(self, raw_value, node, ctx):
        return self._parse_dt_sequence(ctx, 1)

    def _parse_dt_sequence(self, ctx, start_at: int = 0):
        start = ctx.peek(start_at)
        if start:
            if start.descriptor.id == 301011:
                ctx.skip += start_at
                nxt = ctx.peek(start_at + 1)
                if nxt.descriptor.id in (301012, 301013):
                    ctx.skip += 1
                    return self._node_sequences_to_datetime(ctx, start, nxt)
                else:
                    return self._node_sequences_to_datetime(ctx, start)
            elif start.descriptor.id == 4001:
                seq = [start]
                expected = (4002, 4003, 4004, 4005, 4006)
                ctx.skip += start_at
                for idx, expected_descriptor in enumerate(expected, start=1):
                    check = ctx.peek(start_at + idx)
                    if check and expected_descriptor == check.descriptor.id:
                        ctx.skip += 1
                        seq.append(check)
                    else:
                        break
                return self._node_list_to_datetime(ctx, seq)
            elif start.descriptor.id == 26021:
                seq = [start]
                expected = (26022, 26023)
                ctx.skip += start_at
                for idx, expected_descriptor in enumerate(expected, start=1):
                    check = ctx.peek(start_at + idx)
                    if check and expected_descriptor == check.descriptor.id:
                        ctx.skip += 1
                        seq.append(check)
                    else:
                        break
                return self._node_list_to_datetime(ctx, seq)
        return None

    def _node_sequences_to_datetime(self, ctx, ymd_node, hms_node=None):
        nodes = [*ymd_node.members]
        if hms_node and hms_node.members:
            nodes.extend(hms_node.members)
        return self._node_list_to_datetime(ctx, nodes)

    def _node_list_to_datetime(self, ctx, nodes):
        node_values = [self._get_node_value(n, ctx) for n in nodes]
        while node_values and node_values[-1] is None:
            node_values = node_values[:-1]

        if node_values:
            dt_len = len(node_values)
            date_str = "-".join([str(node_values[0]), str(node_values[1]).zfill(2), str(node_values[2]).zfill(2)])
            if dt_len > 3:
                date_str += "T"
                date_str += ":".join(str(x).zfill(2) for x in node_values[3:])
                date_str += "+00:00"
            return date_str
        else:
            return None

    def _parse_node_4021(self, node, ctx):
        self._parse_time_period_node(node, ctx)

    def _parse_node_4022(self, node, ctx):
        self._parse_time_period_node(node, ctx)

    def _parse_node_4023(self, node, ctx):
        self._parse_time_period_node(node, ctx)

    def _parse_node_4024(self, node, ctx):
        self._parse_time_period_node(node, ctx)

    def _parse_node_4025(self, node, ctx):
        self._parse_time_period_node(node, ctx)

    def _parse_node_4026(self, node, ctx):
        self._parse_time_period_node(node, ctx)

    def _get_timedelta_value(self, node, ctx):
        value = self.raw_data.decoded_values_all_subsets[ctx.subset][node.index]
        if value is None:
            return None
        units = "s"
        if hasattr(node.descriptor, 'unit'):
            units = self.bufr_tables.standardize_units(node.descriptor.unit)
        if units == "s":
            return f"PT{value}S"
        if units == "min":
            return f"PT{value}M"
        if units == "h":
            return f"PT{value}H"
        if units == "d":
            return f"P{value}D"
        if units == "a":
            return f"P{value}Y"
        if units == "mon":
            return f"P{value}M"
        self.warn(f"Value {value} [{units}] could not be converted to ISO duration format", ctx)
        return value, units

    def _process_time_period(self, value, node, ctx):
        return self._get_timedelta_value(node, ctx)

    def _parse_time_period_node(self, node, ctx: _Bufr4DecoderContext):
        val = self._get_timedelta_value(node, ctx)
        if ctx.child_record_type == "TIME_SERIES" and "Time" not in ctx.target.coordinates and "TimeOffset" not in ctx.target.coordinates:
            if "TimeOffset" in ctx.target.coordinates and val != ctx.target.coordinates["TimeOffset"].value:
                ctx.start_new_record()
            self._apply_instruction({
                "name": "coordinates/TimeOffset",
                "instruction": "apply_to_target",
            }, ocproc2.SingleElement(val), ctx, node)
        else:
            self._apply_instruction({
                "name": "metadata/ObservationPeriod",
                "instruction": "apply_to_parameters"
            }, ocproc2.SingleElement(val), ctx, node)

    def _negate_value(self, value, node, ctx):
        value = self.raw_data.decoded_values_all_subsets[ctx.subset][node.index]
        if value is None:
            return None
        if isinstance(value, str):
            value = int(value) if value.isdigit() else float(value)
        return -1 * value

    def _parse_node_8043(self, node, ctx):
        raise NotImplementedError
        # TODO: next node is 15028 and its meaning is this

    def _parse_wigos_id(self, raw_value, node, ctx):
        raise NotImplementedError
        # TODO: 1125, 1126, 1127, 1128 (series, issuer, issue, local CHARACTER)

    def _parse_node_1125(self, node, ctx):
        peek_nodes = [node, ctx.peek(1), ctx.peek(2), ctx.peek(3)]
        expected = [1125, 1126, 1127, 1128]
        if any(n is None or n.descriptor.id != expected[idx] for idx, n in enumerate(peek_nodes)):
            self._parse_node(node, ctx, _skip_custom_check=True)
        node_vals = [self._get_node_value(n, ctx) for n in peek_nodes]

        if all(v is None for v in node_vals):
            self._apply_instruction({
                'name': 'metadata/WIGOSID',
                'instruction': 'apply_to_target'
            }, None, ctx, node)
            ctx.skip = 3
        elif any(v is None for v in node_vals):
            self._parse_node(node, ctx, _skip_custom_check=True)
        else:
            self._apply_instruction({
                'name': 'metadata/WIGOSID',
                'instruction': 'apply_to_target'
            }, '-'.join(str(x) for x in node_vals), ctx, node)
            ctx.skip = 3
