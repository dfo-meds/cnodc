import functools
import typing as t
import math

import yaml
import pathlib

from autoinject import injector
import zrlog
import pybufrkit.descriptors
from pybufrkit.descriptors import SequenceDescriptor
from pybufrkit.encoder import Encoder
from pybufrkit.tables import TableGroupCacheManager, TableGroupKey, BufrTableGroup
from pybufrkit.decoder import Decoder
from pybufrkit.templatedata import TemplateData, SequenceNode, DelayedReplicationNode, FixedReplicationNode, \
    ValueDataNode, DataNode

from medsutil.awaretime import AwareDateTime
from medsutil.dynamic import dynamic_object
from medsutil.ocproc2 import ParentRecord, MessageType
from medsutil.ocproc2.codecs.gts import GtsSubDecoder
from medsutil.ocproc2.codecs.base import DecodeResult
from medsutil.byteseq import ByteSequenceReader
import medsutil.ocproc2 as ocproc2
import medsutil.awaretime as awaretime
from medsutil.ocproc2.codecs.ops import Instruction, EncodeDecodeGroup, OPSContext, DataType, \
    SingleValueInstruction, InstructionGroup, RepeatGroup, NoopInstruction, \
    ContextInstruction, ValueMappedInstruction, ScaleFactorInstruction
from medsutil.sanitize import clean_wmo_id
from medsutil.units import UnitConverter
from pipeman.exceptions import CNODCError


@injector.injectable_global
class BufrCodeMap:

    converter: UnitConverter = None

    @injector.construct
    def __init__(self):
        root = pathlib.Path(__file__).absolute().parent
        with open(root / "bufr_map2.yaml", "r") as h:
            raw = yaml.safe_load(h.read()) or {}
            self._bufr_map: dict[str, dict] = {
                str(x): self.standardize_instruction(raw[x], int(x))
                for x in raw
            }

    def standardize_instruction(self, instruction: str | dict, descriptor: int | None = None) -> dict:
        """ Rewrite incoming instructions to make sure they're compatible with OPS. """
        if isinstance(instruction, str):
            extras = {}
            if instruction.endswith("[optional]"):
                extras['is_optional'] = True
                instruction = instruction[:-10]
            if instruction.endswith("[ifany]"):
                extras['can_omit'] = True
                instruction = instruction[:-7]
            if instruction.isdigit():
                return {
                    "descriptor": int(instruction),
                    "dynamic_load": True,
                    **extras
                }
            if instruction == "noop":
                return {"instruction": "noop"}
            if ":" in instruction:
                d, instruction = instruction.split(":", maxsplit=1)
                descriptor = int(d)
            if "/" in instruction:
                return {
                    "element": instruction,
                    "descriptor": descriptor,
                    **extras
                }
            else:
                return {
                    "value": instruction,
                    "descriptor": descriptor,
                    **extras
                }
        else:
            if descriptor is not None:
                instruction["descriptor"] = descriptor
        return instruction

    def parse_ops_element(self,
                          x: dict | str,
                          c: t.Callable | None,
                          table_group: BufrTableGroup) -> Instruction:
        if isinstance(x, dict):
            if "dynamic_load" in x and x["dynamic_load"]:
                return self.lookup(x["descriptor"], table_group=table_group)
        raise ValueError("Unrecognized BUFR instruction")

    def get_table_group_arguments(self, descriptor_id: int, table_group: BufrTableGroup) -> dict[str, t.Any]:
        kwargs = {}
        lookup = table_group.lookup(descriptor_id)
        if hasattr(lookup, 'unit'):
            if lookup.unit.upper() in ('CCITT IA5',):
                kwargs["data_type"] = DataType.STRING
            elif lookup.unit.upper() in ("CODE TABLE"):
                kwargs["data_type"] = DataType.INTEGER
            elif lookup.unit.upper() in ("NUMERIC",):
                kwargs["data_type"] = DataType.FLOAT
            else:
                kwargs["data_type"] = DataType.FLOAT
                kwargs["units"] = self.standardize_units(lookup.unit)
        if hasattr(lookup, 'scale'):
            kwargs["places"] = lookup.scale
        return kwargs

    def lookup(self,
               descriptor_id : int | str,
               table_group: BufrTableGroup) -> Instruction:
        key = str(int(descriptor_id))
        if key in self._bufr_map:
            base_map = {
                x: y for x, y in self.get_table_group_arguments(int(descriptor_id), table_group)
            }
            base_map.update(self._bufr_map[key])
            instruction = Instruction.parse_instruction(
                base_map,
                functools.partial(self.parse_ops_element, table_group=table_group)
            )
            return instruction
        raise ValueError("No bufr instruction defined")

    def standardize_units(self, unit: str):
        if unit.upper() in ('NUMERIC', 'CCITT IA5', 'CODE TABLE'):
            return None
        if unit == "degree true" or unit == "degrees true":
            unit = "degrees"
        if unit == "mon":
            return None
        return self.converter.standardize(unit)


class Bufr4Decoder(GtsSubDecoder):

    bufr_tables: BufrCodeMap = None

    @injector.construct
    def __init__(self):
        pass

    def supports_multiple_records(self) -> bool:
        return True

    def encode_from_records(self, records: list[ParentRecord], **kwargs) -> t.Iterable[bytes | bytearray]:
        encoder = _Bufr4Encoder(self.bufr_tables, records, **kwargs)
        content = encoder.encode()
        yield b'BUFR'
        yield (len(content) + 8).to_bytes(3, 'big')
        yield (4).to_bytes(1, 'big')
        yield content

    def decode_from_bytes(self, reader: ByteSequenceReader, header: str, skip_decode: bool, received_date: AwareDateTime | None = None) -> DecodeResult:
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
            instance = _Bufr4Decoder(self.bufr_tables, header, content)
            return DecodeResult(
                records=[x for x in instance.convert_to_records()],
                original=original_data,
            )
        except Exception as ex:
            return DecodeResult(
                exc=ex,
                original=header.encode('ascii') + b"\n" + content
            )


class EncodeElement:

    def __init__(self, descriptor_id: int, value: t.Any, optional: bool = False):
        self.descriptor = descriptor_id
        self.value = value
        self.optional = optional

    def can_omit(self) -> bool:
        if self.optional:
            return True
        if isinstance(self.value, list):
            for x in self.value:
                if isinstance(x, EncodeElement):
                    if not x.can_omit():
                        return False
                else:
                    if x is not None:
                        return False
            return True
        else:
            return self.value is None

    def assemble(self) -> t.Iterable[t.Any]:
        if isinstance(self.value, list):
            for x in self.value:
                if isinstance(x, EncodeElement):
                    yield from x.assemble()
                else:
                    yield x
        else:
            yield self.value


class _Bufr4Encoder:

    def __init__(self,
                 cds_tables: BufrCodeMap,
                 records: list[ParentRecord],
                 override_template: str | None = None,
                 default_template: str | None = None):
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

    @property
    def table_group(self) -> BufrTableGroup:
        if self._table_group is None:
            raise ValueError("called too early")
        else:
            return self._table_group

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

    def earliest_time(self) -> AwareDateTime:
        earliest = AwareDateTime.utcnow()
        for record in self._records:
            if record.coordinates.has_value("Time"):
                record_time = record.coordinates.ideal("Time").to_datetime()
                if record_time < earliest:
                    earliest = record_time
        return earliest.astimezone("Etc/UTC")

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

    def _build_section_four_subset(self, record: ParentRecord, descriptors: list[int]) -> list:
        subset = []
        context = OPSContext(record)
        for descriptor in descriptors:
            instruction = self._cds_tables.lookup(descriptor, self.table_group)
            for encoded in self._build_from_instruction(instruction, context):
                subset.extend(encoded.assemble())
        return subset

    def _build_section_five(self) -> list:
        return ["7777"]

    def _build_from_instruction(self, instruction: Instruction, context: OPSContext) -> t.Generator[EncodeElement, None, None]:
        try:
            if isinstance(instruction, EncodeDecodeGroup):
                instruction = instruction.get_instruction(True)

            if isinstance(instruction, SingleValueInstruction):
                yield EncodeElement(
                    int(instruction.extras["descriptor"]),
                    instruction.get_value(context),
                    "can_omit" in instruction.extras and instruction.extras["can_omit"]
                )
            elif isinstance(instruction, InstructionGroup):
                if 'is_optional' in instruction.extras and instruction.extras['is_optional']:
                    output: list[EncodeElement] = [x for x in self._build_from_instruction_group(instruction, context)]
                    if all(x.can_omit() for x in output):
                        yield EncodeElement(0, int(instruction.extras.get("size_descriptor", 31000)))
                    else:
                        yield EncodeElement(1, int(instruction.extras.get("size_descriptor", 31000)))
                        yield from output
                else:
                    yield from self._build_from_instruction_group(instruction, context)
            elif isinstance(instruction, RepeatGroup):
                yield from self._build_from_repeat_group(instruction, context)
            # note: ValueMappedInstruction and ContextInstruction not supported
            elif not isinstance(instruction, NoopInstruction):
                raise ValueError("Invalid instruction")

        except Exception as e:
            if 'descriptor' in instruction.extras:
                e.add_note(f"Error while handling instruction: {instruction.extras['descriptor']} [{instruction}]")
            else:
                e.add_note(f"Error while handling instruction: [{instruction}]")
            raise

    def _build_raw_from_instruction_group(self, instruction: InstructionGroup, context: OPSContext) -> t.Generator[EncodeElement, None, None]:
        for i in instruction.iterate_instructions(context):
            yield from self._build_from_instruction(i, context)

    def _build_from_instruction_group(self, instruction: InstructionGroup, context: OPSContext) -> t.Generator[EncodeElement, None, None]:
        if "descriptor" not in instruction.extras:
            yield from self._build_raw_from_instruction_group(instruction, context)
        else:
            descriptor_id = int(instruction.extras["descriptor"])
            try:
                descriptor = self.table_group.lookup(descriptor_id)
                if isinstance(descriptor, SequenceDescriptor):
                    yield EncodeElement(descriptor.id, [
                        x
                        for x in
                        self._filter_results(descriptor, self._build_raw_from_instruction_group(instruction, context))
                    ])
                else:
                    raise ValueError("Invalid descriptor")

            except Exception as ex:
                ex.add_note(f"Error while building descriptor [{descriptor_id}]")
                raise

    def _filter_results(self, descriptor: SequenceDescriptor, results: t.Generator[EncodeElement, None, None]) -> t.Iterable[EncodeElement]:
        for x in descriptor.members:
            if 100000 <= x.id < 200000:
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

    def _build_from_repeat_group(self, instruction: RepeatGroup, context: OPSContext) -> t.Generator[EncodeElement, None, None]:
        groups = []
        for instruction_list in instruction.iterate_repeats(context):
            group: list[EncodeElement] = []
            for x in instruction_list:
                group.extend(self._build_from_instruction(x, context))
            # skip empty groups
            if all(x.can_omit() for x in group):
                continue
            groups.append(group)
            if instruction.repeats is not None and 0 < instruction.repeats <= len(groups):
                break
        if "size_descriptor" in instruction.extras:
            yield EncodeElement(int(instruction.extras["size_descriptor"]), len(groups), len(groups) == 0)
        for group in groups:
            yield from group





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

    def __init__(self,
                 bufr_tables:
                 BufrCodeMap,
                 header,
                 content: t.Union[bytearray, bytes]):
        self.bufr_tables = bufr_tables
        self.header = header
        self._log = zrlog.get_logger("ocproc2.codecs.wmo.bufr.decode")
        decoder = Decoder()
        self.raw_content = content
        self.message = decoder.process(self.raw_content)
        self.raw_data: TemplateData = self.message.template_data.value
        self.pybufr_tables = TableGroupCacheManager.get_table_group_by_key(self.message.table_group_key)

    def warn(self, message, ctx: OPSContext):
        message = "{txt} [{hierarchy}] [{header}]".format(
            txt=message,
            header=self.header,
            hierarchy=ctx.extras["hierarchy"]
        )
        self._log.warning(message)
        ctx.parent.add_history_entry(message, "bufr_decode", "1.0", "", MessageType.WARNING)

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
        message_types = set()
        for x in _Bufr4Decoder.BUFR_MESSAGE_CODES:
            if x in descriptors:
                message_types.add(x)
        if not message_types:
            this_message = self._expand_bufr_descriptors(descriptors, self.pybufr_tables)
            for version in range(39, 5, -1):
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
                        message_types.add(x)
        return list(message_types)

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

    def convert_to_records(self) -> t.Iterable[ocproc2.ParentRecord]:
        pieces = self.header.split(' ')
        if len(pieces) > 3 and pieces[3][0] in ('C', 'A', 'P'):
            raise CNODCError("BUFR decoder not configured to properly handle CCx AAx or Pxx messages", "BUFR_DECODE", 1000)
        descriptors = list(x for x in self.message.unexpanded_descriptors.value)
        common_metadata = {
            'GTSHeader': self.header,
            'BUFRDescriptors': ocproc2.SingleElement(descriptors),
            'BUFRInferredMessageType': ocproc2.SingleElement(self._identify_bufr_message_type(descriptors)),
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

    def _convert_subset_to_record(self, subset_number: int, common_metadata: dict) -> ocproc2.ParentRecord:
        context = OPSContext(ocproc2.ParentRecord())
        context.extras["subset"] = subset_number
        context.extras["hierarchy"] = [f"SN{subset_number}"]
        context.parent.metadata.update(common_metadata)
        context.parent.metadata["BUFRSubsetIndex"] = subset_number
        self._iterate_on_nodes(
            self.raw_data.decoded_nodes_all_subsets[subset_number],
            context
        )
        return context.parent

    def _iterate_on_nodes(self, nodes: list[DataNode], context: OPSContext):
        with context.subcontext() as ctx:
            ctx.extras["skip"] = 0
            ctx.extras["current_index"] = 0
            ctx.extras["nodes"] = nodes

            for idx, node in enumerate(nodes):
                if ctx.extras["skip"] > 0:
                    ctx.extras["skip"] -= 1
                    continue
                ctx.extras["current_index"] = idx
                self._parse_node(node, ctx)

    def _parse_node(self, node: DataNode, context: OPSContext):
        with context.subcontext() as ctx:
            context.extras["hierarchy"].append(node.descriptor.id)
            test_name = f"_parse_node_{node.descriptor.id}"

            # Custom handling
            if hasattr(self, test_name):
                getattr(self, test_name)(node, context)

            # Basic instructions
            elif isinstance(node, (SequenceNode, ValueDataNode)):
                self._apply_instruction(
                    self.bufr_tables.lookup(node.descriptor.id, self.pybufr_tables),
                    node,
                    context
                )

            # Replication
            elif isinstance(node, (DelayedReplicationNode, FixedReplicationNode)):
                if node.members:
                    self._parse_replication_node(node, context)

            # Other nodes (usually instructions)
            else:
                descriptor_id = node.descriptor.id
                if 200000 <= descriptor_id < 210000:
                    return
                self.warn(f"Unhandled node type: [{node.__class__}]", context)

    def _parse_replication_node(self, node: DelayedReplicationNode | FixedReplicationNode, ctx: OPSContext):
        n_total, n_elements, n_repeats = self._parse_repetition_info(node, ctx)
        descriptors = set(
            n.descriptor.id
            for n in node.members
        )
        map_to = None
        coord_name = None
        for x in descriptors:
            instruction = self.bufr_tables.lookup(x, self.pybufr_tables)
            if 'begin_subrecord_type' in instruction.extras:
                if map_to is not None and instruction.extras["begin_subrecord_type"] != map_to:
                    self.warn(f"Overriding subrecord type {map_to}", ctx)
                map_to = instruction.extras["begin_subrecord_type"]
                coord_name = (x,)
        if map_to is None and n_repeats > 1 and any(x in descriptors for x in (4021, 4022, 4023, 4024, 4025, 4026)):
            map_to = "TIME_SERIES"
            coord_name = (4021, 4022, 4023, 4024, 4025, 4026)
        if map_to is not None and coord_name is not None:
            self._iterate_into_children(node, ctx, map_to, coord_name, n_elements, n_repeats)
        else:
            if n_repeats > 1:
                self.warn(f"No subrecord type found for repeated instruction [{node.descriptor.id}]", ctx)
            self._iterate_on_nodes(node.members, ctx)

    def _parse_repetition_info(self, node: t.Union[DelayedReplicationNode, FixedReplicationNode], ctx) -> tuple[int, int, int]:
        n_total = len(node.members)
        if isinstance(node, DelayedReplicationNode):
            n_repeats = t.cast(int, self._get_node_value(None, node.factor, ctx))
        else:
            n_repeats = int(str(node.descriptor.id)[3:])
        n_elements = int(n_total / n_repeats)
        return n_total, n_elements, n_repeats

    def _iterate_into_children(self,
                               node: DelayedReplicationNode | FixedReplicationNode,
                               context: OPSContext,
                               recordset_type: str,
                               coord_names: tuple[int, ...],
                               n_elements: int,
                               n_repeats: int):
        with context.new_recordset(recordset_type):
            for i in range(0, n_repeats):
                with context.new_subrecord():
                    self._iterate_on_nodes(
                        node.members[(i*n_elements):((i+1)*n_elements)],
                        context
                    )

    def _apply_instruction(self,
                           instruction: Instruction,
                           node: ValueDataNode | SequenceNode,
                           context: OPSContext):
        if isinstance(instruction, ContextInstruction):
            self._apply_instruction(
                instruction.get_instruction(context.extras["hierarchy"]),
                node, context
            )
        elif isinstance(instruction, EncodeDecodeGroup):
            self._apply_instruction(
                instruction.get_instruction(False),
                node, context
            )
        elif isinstance(instruction, ValueMappedInstruction):
            value = self._get_node_value(instruction, node, context)
            self._apply_instruction(
                instruction.get_instruction(value),
                node, context
            )
        elif isinstance(instruction, SingleValueInstruction):
            instruction.set_value(self._get_node_value(instruction, node, context), {}, context)
        elif isinstance(instruction, NoopInstruction):
            ...
        elif isinstance(instruction, ScaleFactorInstruction):
            context.extras["scale_factor"] = self._get_node_value(instruction, node, context)
        else:
            raise ValueError("Unrecognized instruction")
        if instruction.extras.get("iterate_after", False) and isinstance(node, SequenceNode):
            if node.members:
                self._iterate_on_nodes(node.members, context)

    def _get_node_value(self, instruction: Instruction | None, node: ValueDataNode | SequenceNode, context: OPSContext) -> str | None | int | float:
        if instruction is not None and "override_get_node_value" in instruction.extras:
            value = dynamic_object(instruction.extras["override_get_node_value"])(instruction, node, context)
        elif isinstance(node, SequenceNode):
            value = None
        else:
            value = self.raw_data.decoded_values_all_subsets[context.extras["subset"]][node.index]
        if isinstance(value, bytes):
            value = bytes([x for x in value if 0 < x < 128]).decode('ascii', errors='replace').strip(' ')
        elif "scale_factor" in context.extras and context.extras["scale_factor"]:
            value *= math.pow(10, context.extras["scale_factor"])
        if value == '' or value == b'':
            value = None
        return value

    @staticmethod
    def peek(n: int, context: OPSContext) -> DataNode | None:
        if 'node_list' in context.extras:
            new_idx = context.extras["current_index"] + n
            if 0 <= new_idx < len(context.extras["node_list"]):
                return context.extras["node_list"][new_idx]
        return None

    def _parse_node_8080(self, node, context: OPSContext):
        nxt = self.peek(1, context)
        flag_value = None
        applies_to = self._get_node_value(None, node, context)
        if nxt and nxt.descriptor.id == 33050:
            context.extras["skip"] += 1
            v = self._get_node_value(None, t.cast(ValueDataNode, nxt), context)
            flag_value = ocproc2.SingleElement(v) if v is not None else v
        if applies_to == 20:
            context.set_element("coordinates/Latitude/metadata/Quality", flag_value)
            context.set_element("coordinates/Longitude/metadata/Quality", flag_value)
        elif applies_to == 4:
            context.set_element("parameters/SeaDepth/metadata/Quality", flag_value)
        elif applies_to == 10:
            context.set_element("coordinates/Pressure/metadata/Quality", flag_value)
        elif applies_to == 11:
            context.set_element("parameters/Temperature/metadata/Quality", flag_value)
        elif applies_to == 12:
            context.set_element("parameters/PracticalSalinity/metadata/Quality", flag_value)
        elif applies_to == 13:
            context.set_element("coordinates/Depth/metadata/Quality", flag_value)
        elif applies_to == 14:
            context.set_element("parameters/CurrentSpeed/metadata/Quality", flag_value)
        elif applies_to == 15:
            context.set_element("parameters/CurrentDirection/metadata/Quality", flag_value)
        elif applies_to == 16:
            context.set_element("parameters/DissolvedOxygen/metadata/Quality", flag_value)
        elif applies_to == 25:
            context.set_element("parameters/Conductivity/metadata/Quality", flag_value)
        elif applies_to == 26:
            context.set_element("parameters/PotentialDensity/metadata/Quality", flag_value)
        elif applies_to is None:
            if flag_value is not None:
                self.warn(f"Unhandled quality flag with no apply_to value", context)
        else:
            self.warn(f"Unrecognized apply to value [{applies_to}]", context)

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

    def _parse_node_8043(self, node, ctx):
        # TODO: next node is 15028 and its meaning is this
        raise NotImplementedError

    def _parse_time_period_node(self, node: ValueDataNode, ctx: OPSContext):
        if ctx.recordset_type == "TIME_SERIES" and "Time" not in ctx.record.coordinates and "TimeOffset" not in ctx.record.coordinates:
            kwargs = {
                "element": "coordinates/TimeOffset",
            }
        else:
            kwargs = {
                "element": "common/ObservationPeriod",
            }
        kwargs.update(self.bufr_tables.get_table_group_arguments(node.descriptor.id, self.pybufr_tables))
        kwargs.update({
            "data_type": "duration"
        })
        self._apply_instruction(
            Instruction.parse_instruction(kwargs, functools.partial(self.bufr_tables.parse_ops_element, table_group=self.pybufr_tables)),
            node, ctx
        )

    @staticmethod
    def extract_expected_nodes(node_list: list[int | list[int]],
                               context: OPSContext,
                               offset: int = 0) -> list[DataNode]:
        result = []
        for idx, descriptor_id in enumerate(node_list):
            peek = _Bufr4Decoder.peek(idx + offset, context)
            if peek is not None and peek.descriptor.id == descriptor_id:
                result.append(peek)
            else:
                break
        return result

    def parse_wmo_id(self, instruction: Instruction | None, node: ValueDataNode | SequenceNode, context: OPSContext):
        peek1 = self.peek(1, context)
        if not (peek1 and peek1.descriptor.id in (1002, 1020, 1004)):
            raise ValueError("Expecting 1002, 1020, or 1004")
        if peek1.descriptor != 1002:
            peek2 = self.peek(2, context)
            if not (peek2 and peek2.descriptor.id == 1005):
                raise ValueError("Expecting 1005")
            elements = [
                self._get_node_value(None, node, context),
                self._get_node_value(None, t.cast(ValueDataNode, peek1), context),
                self._get_node_value(None, t.cast(ValueDataNode, peek2), context),
            ]
            context.extras["skip"] += 2
        else:
            elements = [
                self._get_node_value(None, node, context),
                "",
                self._get_node_value(None, t.cast(ValueDataNode, peek1), context),
            ]
            context.extras["skip"] += 1
        if all(x is None or x == "" for x in elements):
            value = None
        else:
            value = clean_wmo_id("".join((
                str(elements[0]).zfill(2),
                str(elements[1]).zfill(2),
                str(elements[0]).zfill(5)
            )))
        return value

    def parse_wigos_id(self, instruction: Instruction | None, node: ValueDataNode | SequenceNode, context: OPSContext):
        nodes = self.extract_expected_nodes(
            [1125, 1126, 1127, 1128],
            context
        )
        if len(nodes) != 4:
            raise ValueError("Expecting 4 nodes")
        context.extras["skip"] += 3
        values = [
            self._get_node_value(None, t.cast(ValueDataNode, n), context) for n in nodes
        ]
        return None if all(v is None for v in values) else "-".join(
            str(x)
            if x is not None else ""
            for x in values
        )

    def parse_datetime_sequence(self, instruction: Instruction | None, node: ValueDataNode | SequenceNode, context: OPSContext):
        return self._parse_dt_sequence(context, 0)

    def parse_following_datetime_sequence(self, instruction: Instruction | None, node: ValueDataNode | SequenceNode, context: OPSContext):
        return self._parse_dt_sequence(context, 1)

    def _parse_dt_sequence(self, ctx: OPSContext, start_at: int = 0):
        start = self.peek(start_at, ctx)
        if start is not None:
            if start.descriptor.id == 301011:
                ctx.extras["skip"] += start_at
                nxt = self.peek(start_at + 1, ctx)
                if nxt is not None and nxt.descriptor.id in (301012, 301013):
                    ctx.extras["skip"] += 1
                    return self._node_sequences_to_datetime(ctx, start, nxt)
                else:
                    return self._node_sequences_to_datetime(ctx, start)
            elif start.descriptor.id == 4001:
                expected = self.extract_expected_nodes([
                    4002, 4003, 4004, 4005, 4006
                ], ctx, start_at + 1)
                ctx.extras["skip"] += start_at + len(expected)
                return self._node_list_to_datetime(ctx, start, *expected)
            elif start.descriptor.id == 26021:
                expected = self.extract_expected_nodes([26022, 26023], ctx, start_at + 1)
                ctx.extras["skip"] += start_at + len(expected)
                return self._node_list_to_datetime(ctx, start, *expected)
        return None

    def _node_sequences_to_datetime(self,
                                    ctx: OPSContext,
                                    ymd_node: DataNode,
                                    hms_node: DataNode | None = None):
        nodes = []
        if isinstance(ymd_node, SequenceNode):
            nodes.extend(ymd_node.members)
        else:
            raise ValueError()
        if isinstance(hms_node, SequenceNode):
            nodes.extend(hms_node.members)
        elif hms_node is not None:
            raise ValueError()
        return self._node_list_to_datetime(ctx, *nodes)

    def _node_list_to_datetime(self,
                               ctx: OPSContext,
                               *nodes: DataNode):
        node_values = [
            self._get_node_value(None, t.cast(ValueDataNode, n), ctx) for n in nodes
        ]
        while node_values and node_values[-1] is None:
            node_values = node_values[:-1]

        if node_values:
            dt_len = len(node_values)
            date_str = "-".join([str(node_values[0]), str(node_values[1]).zfill(2), str(node_values[2]).zfill(2)])
            if dt_len > 3:
                date_str += "T"
                date_str += ":".join(str(x).zfill(2) for x in node_values[3:])
                date_str += "+00:00"
            e = ocproc2.SingleElement(date_str)
            if dt_len == 3:
                e.metadata['DatePrecision'] = 'day'
            elif dt_len == 4:
                e.metadata['DatePrecision'] = 'hour'
            elif dt_len == 5:
                e.metadata['DatePrecision'] = 'minute'
            elif dt_len == 6:
                e.metadata['DatePrecision'] = 'second'
            return e
        else:
            return None
