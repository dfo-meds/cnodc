import datetime
import typing as t
import pybufrkit.descriptors
import zrlog

from cnodc.ocproc2 import BaseRecord
from cnodc.ocproc2.codecs.gts import GtsSubDecoder
from cnodc.ocproc2.codecs.base import DecodeResult, ByteIterable, ByteSequenceReader
import math
import yaml
from pybufrkit.tables import TableGroupCacheManager, TableGroupKey
from pybufrkit.renderer import NestedTextRenderer
import cnodc.ocproc2 as ocproc2
import pathlib
from pybufrkit.decoder import Decoder
from pybufrkit.templatedata import TemplateData, SequenceNode, DelayedReplicationNode, FixedReplicationNode, \
    ValueDataNode, NoValueDataNode
from autoinject import injector
import cnodc.util.awaretime as awaretime

from cnodc.science.units import UnitConverter
from cnodc.util import CNODCError


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

    def lookup(self, descriptor_id):
        key = str(int(descriptor_id))
        if key in self._bufr_map:
            return self._bufr_map[key]
        return None

    def standardize_units(self, unit):
        if unit in ('Numeric', 'CCITT IA5', 'CODE TABLE'):
            return None
        return self.converter.standardize(unit)

    def standardize_instruction(self, instruction):
        if isinstance(instruction, str):
            pieces = instruction.split(":")
            if pieces[0] == "noop":
                return {
                    "instruction": "noop",
                    "raw": True
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
            base = {
                "instruction": "apply_to_target",
            }
            base.update(instruction)
            base["raw"] = base["instruction"] in ("instruction_map", "noop", "set_scale_factor", "error", "mapped")
            if 'context' in base and base['context']:
                for x in base['context']:
                    base['context'][x] = self.standardize_instruction(base['context'][x])
            if 'instruction_map' in base and base['instruction_map']:
                for x in base['instruction_map']:
                    base['instruction_map'][x] = self.standardize_instruction(base['instruction_map'][x])
            return base


class Bufr4Decoder(GtsSubDecoder):

    bufr_tables: BufrCDSTables = None

    @injector.construct
    def __init__(self):
        pass

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

    def copy(self):
        new = _Bufr4DecoderContext(self.subset)
        new.hierarchy = [x for x in self.hierarchy]
        new.target = self.target
        new.top = self.top
        new.var_metadata = {x: self.var_metadata[x] for x in self.var_metadata}
        new.record_metadata = {x: self.record_metadata[x] for x in self.record_metadata}
        return new

    def start_iteration(self, node_list):
        self.skip = 0
        self.current_idx = 0
        self.node_list = node_list

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
            self.target_subset.records.append(self.target)
            self.target = None

    def set_record_property(self, property_full_name: str, value: ocproc2.AbstractElement):
        if value.value is None or self.target is None:
            return
        if self.var_metadata:
            pieces = property_full_name.split('/')
            for x in self.var_metadata:
                if self.var_metadata[x][1] is None or pieces[-1] in self.var_metadata[x][1]:
                    value.metadata[x] = self.var_metadata[x][0]
        self.target.set(property_full_name, value)

    def add_future_parameter_metadata(self, property_name, value, limit_to_parameters: t.Optional[list[str]] = None):
        if value.value is not None:
            self.var_metadata[property_name] = (value, limit_to_parameters or None)
        elif property_name in self.var_metadata:
            del self.var_metadata[property_name]

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

    def __init__(self, header, content: t.Union[bytearray, bytes], bufr_tables: BufrCDSTables):
        self.bufr_tables = bufr_tables
        self.header = header
        self.log = zrlog.get_logger("cnodc.bufr_decoder")
        decoder = Decoder()
        self.raw_content = content
        self.message = decoder.process(self.raw_content)
        self.raw_data: TemplateData = self.message.template_data.value
        self.pybufr_tables = TableGroupCacheManager.get_table_group_by_key(self.message.table_group_key)

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
                TableGroupKey(self.message.table_group_key.tables_root_dir, ('0', '0_0', str(version)), None)
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
            best_idx = None
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
        ctx.target = ocproc2.ParentRecord()
        ctx.top = ctx.target
        ctx.target.metadata.update(common_metadata)
        ctx.target.metadata['BUFRSubsetIndex'] = subset_number
        ctx.hierarchy = []
        ctx.hierarchy = [f'M#{subset_number}']
        self._iterate_on_nodes(self.raw_data.decoded_nodes_all_subsets[subset_number], ctx)
        return ctx.target

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
            map_to = "TSERIES"
            coord_name = (4021, 4022, 4023, 4024, 4025, 4026)
        if map_to is not None:
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
        for i in range(0, n_repeats):
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

    def _build_value(self, value, instruction, node, ctx):
        if 'value' in instruction:
            value = instruction['value']
        elif 'value_map' in instruction:
            if value in instruction['value_map']:
                value = instruction['value_map'][value]
            elif value is not None:
                self.warn(f"Instruction provides a value_map but [{value}] is not in it", ctx)
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
                value.metadata[key] = instruction['metadata'][key]
        units = self._get_node_units(node)
        if units:
            value.metadata['Units'] = units
        scale = self._get_node_scale(node)
        if scale:
            value.metadata['Uncertainty'] = scale
            value.metadata['Uncertainty'].metadata['UncertaintyType'] = 'uniform'
        return value

    def _get_node_units(self, node: ValueDataNode):
        if hasattr(node.descriptor, 'unit'):
            return self.bufr_tables.standardize_units(node.descriptor.unit)

    def _get_node_scale(self, node: ValueDataNode):
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
                ctx.add_future_parameter_metadata(instruction['name'], value, instruction['filter'] if 'filter' in instruction else None)
            case "apply_to_subrecords":
                ctx.add_future_subrecord_data(instruction['name'], value, instruction['filter'] if 'filter' in instruction else None)
            case "set_scale_factor":
                ctx.scale_factor = value
            case "mapped":
                map_key = str(value) if value is not None else ""
                for x in instruction["instruction_map"]:
                    if str(x) == map_key:
                        self._apply_instruction(instruction["instruction_map"][map_key], value, ctx, node)
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
            v = self._get_node_value(nxt, ctx)
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

    def _parse_time_period_node(self, node, ctx: _Bufr4DecoderContext):
        val = self._get_timedelta_value(node, ctx)
        nxt = ctx.peek(1)
        if nxt and nxt.descriptor.id == node.descriptor.id:
            val = [val, self._get_timedelta_value(nxt, ctx)]
            ctx.skip += 1
        val = ocproc2.SingleElement(val)
        if ctx.child_record_type == "TSERIES" and "Time" not in ctx.target.coordinates:
            if "TimeOffset" in ctx.target.coordinates and val.value != ctx.target.coordinates["TimeOffset"].value:
                ctx.start_new_record()
            self._apply_instruction({
                "name": "coordinates/TimeOffset",
                "instruction": "apply_to_target",
            }, val, ctx)
        else:
            self._apply_instruction({
                "name": "ObservationPeriod",
                "instruction": "apply_to_parameters"
            }, val, ctx)

    def _parse_node_1125(self, node, ctx):
        peek_nodes = [node, ctx.peek(1), ctx.peek(2), ctx.peek(3)]
        expected = [1125, 1126, 1127, 1128]
        if any(n is None or n.descriptor.id != expected[idx] for idx, n in enumerate(peek_nodes)):
            return self._parse_node(node, ctx, _skip_custom_check=True)
        node_vals = [self._get_node_value(n, ctx) for n in peek_nodes]

        if all(v is None for v in node_vals):
            self._apply_instruction({
                'name': 'metadata/WIGOSID',
                'instruction': 'apply_to_target'
            }, None, ctx)
            ctx.skip = 3
        elif any(v is None for v in node_vals):
            return self._parse_node(node, ctx, _skip_custom_check=True)
        else:
            self._apply_instruction({
                'name': 'metadata/WIGOSID',
                'instruction': 'apply_to_target'
            }, '-'.join(str(x) for x in node_vals), ctx)
            ctx.skip = 3

"""
"""