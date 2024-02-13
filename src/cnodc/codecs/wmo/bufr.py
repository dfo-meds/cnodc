import datetime
import itertools
import typing as t

import pybufrkit.descriptors
import zrlog

from cnodc.codecs.gts import GtsSubDecoder
from cnodc.codecs.base import DecodeResult
import math
import yaml
from pybufrkit.tables import TableGroupCacheManager, TableGroupKey
from pybufrkit.renderer import NestedTextRenderer
from cnodc.ocproc2 import DataRecord, RecordSet
import pathlib
from pybufrkit.decoder import Decoder
from pybufrkit.templatedata import TemplateData, SequenceNode, DelayedReplicationNode, FixedReplicationNode, \
    ValueDataNode, NoValueDataNode
from autoinject import injector

from cnodc.ocproc2.structures import AbstractValue, Value, MultiValue
from cnodc.util import CNODCError


@injector.injectable_global
class BufrCDSTables:

    def __init__(self):
        root = pathlib.Path(__file__).absolute().parent
        with open(root / "bufr_map.yaml", "r") as h:
            raw = yaml.safe_load(h.read()) or {}
            self._bufr_map = {
                str(x): self.standardize_instruction(raw[x])
                for x in raw
            }
        with open(root / "unit_map.yaml", "r") as h:
            self._unit_map = yaml.safe_load(h.read()) or {}

    def lookup(self, descriptor_id):
        key = str(int(descriptor_id))
        if key in self._bufr_map:
            return self._bufr_map[key]
        return None

    def standardize_units(self, unit):
        if unit in ('Numeric', 'CCITT IA5', 'CODE TABLE'):
            return None
        if unit in self._unit_map:
            return self._unit_map[unit]
        return unit

    def standardize_instruction(self, instruction):
        if isinstance(instruction, str):
            pieces = instruction.split(":")
            if pieces[0] == "noop":
                return {
                    "apply_to": "noop",
                    "type": "noop",
                    "name": "noop"
                }
            if pieces[0] in ("metadata", "coordinates", "variables"):
                return {
                    "type": pieces[0],
                    "name": pieces[1],
                    "apply_to": "target"
                }
            elif pieces[0] == 'next_recs':
                return {
                    "type": pieces[1],
                    "name": pieces[2],
                    "apply_to": "subrecords"
                }
            elif pieces[0] == "next_vars":
                return {
                    "type": pieces[1],
                    "name": pieces[2],
                    "apply_to": "following"
                }
            else:
                return None
        elif not isinstance(instruction, dict):
            return None
        base = {
            "apply_to": "target",
            "type": "metadata",
            "name": "noop"
        }
        base.update(instruction)
        if 'context' in base and base['context']:
            for x in base['context']:
                base['context'][x] = self.standardize_instruction(base['context'][x])
        return base


class Bufr4Decoder(GtsSubDecoder):

    bufr_tables: BufrCDSTables = None

    @injector.construct
    def __init__(self):
        pass

    def decode_message(self, header: str, bufr_message: bytearray) -> DecodeResult:
        instance = _Bufr4Decoder(header, bufr_message, self.bufr_tables)
        return DecodeResult(
            records=[x for x in instance.convert_to_records()],
            original=header.encode('ascii') + b'\n' + bufr_message
        )


class _Bufr4DecoderContext:

    def __init__(self, subset_no=None):
        self.subset = subset_no
        self.hierarchy = []
        self.target: t.Optional[DataRecord] = None
        self.parent_target = None
        self.var_metadata = {}
        self.record_metadata = {}
        self.node_list = None
        self.current_idx = None
        self.skip = None
        self.child_record_type = None
        self.scale_factor = None
        self.target_subset: t.Optional[RecordSet] = None

    def copy(self):
        new = _Bufr4DecoderContext(self.subset)
        new.hierarchy = [x for x in self.hierarchy]
        new.target = self.target
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
        ctx.target.report_error(message, 'bufr_decode', '1_0', '')

    def warn(self, message, ctx: _Bufr4DecoderContext = None):
        self.log.warning("{txt} [{hierarchy}] [{header}]".format(
                            txt=message,
                            header=self.header,
                            hierarchy='>'.join(str(x) for x in ctx.hierarchy) if ctx else ''
        ))
        ctx.target.report_warning(message, 'bufr_decode', '1_0', '')

    def convert_to_records(self) -> t.Iterable[DataRecord]:
        pieces = self.header.split(' ')
        if len(pieces) > 3 and pieces[3][0] in ('C', 'A', 'P'):
            raise CNODCError("BUFR decoder not configured to properly handle CCx AAx or Pxx messages", "BUFR_DECODE", 1000)
        descriptors = list(x for x in self.message.unexpanded_descriptors.value)
        common_metadata = {
            'GTSHeader': self.header,
            'BUFRDescriptors': descriptors,
            'BUFRInferredMessageType': self._identify_bufr_message_type(descriptors),
            'BUFROriginCentre': self.message.originating_centre,
            'BUFROriginSubcentre': self.message.originating_subcentre,
            'BUFRDataCategory': self.message.data_category,
            'BUFRIsObservation': 1 if self.message.is_observation else 0,
            'BUFRMessageTime': datetime.datetime(
                year=self.message.year,
                month=self.message.month,
                day=self.message.day,
                hour=self.message.hour,
                minute=self.message.minute,
                second=self.message.second,
                tzinfo=datetime.timezone.utc
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

    def _convert_subset_to_record(self, subset_number, common_metadata: dict) -> DataRecord:
        ctx = _Bufr4DecoderContext(subset_number)
        ctx.target = DataRecord()
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
            n_repeats = self._get_node_value(node.factor, ctx, True)
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
            self._start_new_record(ctx2)
            self._iterate_on_nodes(node.members[(i*n_elements):((i+1)*n_elements)], ctx2)
            self._close_subrecord(ctx2)

    def _start_new_record(self, ctx: _Bufr4DecoderContext):
        if ctx.target is not None:
            self._close_subrecord(ctx)
        ctx.target = DataRecord()

    def _close_subrecord(self, ctx: _Bufr4DecoderContext):
        if ctx.record_metadata:
            for x in ctx.record_metadata:
                if ctx.record_metadata[x][1] is None or any(
                        x in ctx.parent_target.parameters or x in ctx.parent_target.coordinates
                        for x in ctx.record_metadata[x][1]
                ):
                    ctx.target.metadata[x] = ctx.record_metadata[x][0]
        ctx.target_subset.records.append(ctx.target)

    def _parse_value_node(self, node: ValueDataNode, ctx: _Bufr4DecoderContext):
        instruction = self.bufr_tables.lookup(node.descriptor.id)
        if instruction is None:
            self.warn(f"Unhandled node descriptor: {node.descriptor.id}: {self._get_node_value(node, ctx, True)}", ctx)
        else:
            self._apply_instruction(instruction, self._get_node_value(node, ctx), ctx, node)

    def _sequence_to_values(self, node: SequenceNode, ctx: _Bufr4DecoderContext, raw: bool = False):
        return [
            self._get_node_value(n, ctx, raw)
            for n in node.members
        ]

    def _clean_instruction(self, instruction, ctx):
        if not isinstance(instruction, dict):
            self.warn(f"Poorly formatted instruction[{instruction}] ({type(instruction)}", ctx)
            return None
        if 'context' in instruction:
            for x in instruction['context']:
                str_x = str(x)
                if any(str_x in h for h in ctx.hierarchy):
                    return self._clean_instruction(instruction['context'][x], ctx)
        return instruction

    def _apply_instruction(self, instruction, value, ctx, node=None):
        if 'value' in instruction:
            value = instruction['value']
        if not isinstance(value, Value):
            value = Value(value)
        instruction = self._clean_instruction(instruction, ctx)
        if instruction is None:
            return
        if 'value_map' in instruction:
            if value.value in instruction['value_map']:
                value.value = instruction['value_map'][value.value]
            elif value.value is not None:
                self.warn(f"Instruction provides a value_map but [{value.value}] is not in it", ctx)
        if 'remove_metadata' in instruction and instruction['remove_metadata']:
            for key in instruction['remove_metadata']:
                if key in value.metadata:
                    del value.metadata[key]
        if 'metadata' in instruction and instruction['metadata']:
            for key in instruction['metadata']:
                value.metadata[key] = instruction['metadata'][key]
        if 'apply_to' not in instruction:
            self.error(f"Instruction is missing 'apply_to'", ctx)
        elif instruction['apply_to'] == 'target':
            if instruction['type'] == 'metadata':
                self._add_record_metadata(instruction['name'], value, ctx, instruction)
            elif instruction['type'] == 'metadata_map':
                for k in instruction['map']:
                    self._add_record_metadata(k, instruction['map'][k], ctx, instruction)
            elif instruction['type'] == 'coordinates':
                self._add_record_coordinate(instruction['name'], value, ctx, instruction)
            elif instruction['type'] == 'variables':
                self._add_record_variable(instruction['name'], value, ctx, instruction)
            else:
                self.warn(f"Unrecognized target type {instruction['type']}", ctx)
        elif instruction['apply_to'] == 'following':
            if instruction['type'] == 'metadata':
                self._add_future_variable_metadata(instruction['name'], value, ctx, instruction)
            else:
                self.warn(f"Unrecognized following variables application type {instruction['type']}", ctx)
        elif instruction['apply_to'] == 'subrecords':
            if instruction['type'] == 'metadata':
                self._add_future_subrecord_metadata(instruction['name'], value, ctx, instruction)
            else:
                self.warn(f"Unrecognized following subrecords application type {instruction['type']}", ctx)
        elif instruction['apply_to'] == 'noop':
            pass
        elif instruction['apply_to'] == 'raise':
            self.warn(f"No instruction provided for [{node.descriptor.id if node else 'unknown'}]", ctx)
        else:
            self.warn(f"Unrecognized instruction application {instruction['apply_to']}", ctx)
        if 'iterate_after' in instruction and instruction['iterate_after'] and node and hasattr(node, 'members'):
            if node.members:
                self._iterate_on_nodes(node.members, ctx)

    def _set_record_property(self, property_type, property_map, property_name, value, ctx, instruction, set_var_metadata: bool = False):
        if not isinstance(value, Value):
            value = Value(value)
        if value.value is None:
            return
        if set_var_metadata and ctx.var_metadata:
            for x in ctx.var_metadata:
                if ctx.var_metadata[x][1] is None or property_name in ctx.var_metadata[x][1]:
                    value.metadata[x] = ctx.var_metadata[x][0]
        if 'metadata' in instruction and instruction['metadata']:
            value.metadata.update(instruction['metadata'])
        if property_name in property_map:
            current_val = property_map[property_name]
            if isinstance(current_val, MultiValue):
                current_val.append(value)
            elif current_val.value is None:
                property_map[property_name] = value
            elif current_val == value:
                pass
            else:
                new_val = MultiValue()
                new_val.append(current_val)
                new_val.append(value)
                property_map[property_name] = new_val
        else:
            property_map[property_name] = value

    def _add_record_metadata(self, property_name, value, ctx, instruction):
        self._set_record_property("metadata", ctx.target.metadata, property_name, value, ctx, instruction, False)

    def _add_record_coordinate(self, property_name, value, ctx, instruction):
        self._set_record_property("coordinate", ctx.target.coordinates, property_name, value, ctx, instruction, True)

    def _add_record_variable(self, property_name, value, ctx, instruction):
        self._set_record_property("variable", ctx.target.parameters, property_name, value, ctx, instruction, True)

    def _add_future_variable_metadata(self, property_name, value, ctx, instruction):
        if not isinstance(value, Value):
            value = Value(value)
        if value.value is not None:
            ctx.var_metadata[property_name] = (value, instruction['filter'] if 'filter' in instruction else None)
        elif property_name in ctx.var_metadata:
            del ctx.var_metadata[property_name]

    def _add_future_subrecord_metadata(self, property_name, value, ctx, instruction):
        if not isinstance(value, Value):
            value = Value(value)
        if value.value is not None:
            ctx.record_metadata[property_name] = (value, instruction['filter'] if 'filter' in instruction else None)
        elif property_name in ctx.record_metadata:
            del ctx.record_metadata[property_name]

    def _get_node_value(self, node: ValueDataNode, ctx: _Bufr4DecoderContext, raw: bool = False, extra_metadata: dict = None):
        value = self.raw_data.decoded_values_all_subsets[ctx.subset][node.index]
        if isinstance(value, bytes):
            value = bytes([x for x in value if 0 < x < 128]).decode('ascii', errors='replace').strip(' ')
        elif ctx.scale_factor is not None and isinstance(value, (int, float)):
            value *= math.pow(10, ctx.scale_factor)
        if value == '' or value == b'':
            value = None
        if raw:
            return value
        metadata = {}
        if extra_metadata:
            metadata.update(extra_metadata)
        units = None
        if hasattr(node.descriptor, 'unit'):
            units = self.bufr_tables.standardize_units(node.descriptor.unit)
            if units is not None:
                metadata['Units'] = units
        if hasattr(node.descriptor, 'scale') and units is not None:
            metadata['Uncertainty'] = math.pow(10, (-1 * node.descriptor.scale)) / 2
        return Value(value, metadata=metadata)

    def _parse_node_301011(self, node, ctx: _Bufr4DecoderContext):
        self._apply_instruction({
            'type': 'coordinates',
            'name': 'Time',
            'apply_to': 'target'
        }, self._parse_dt_sequence(ctx, 0), ctx)

    def _node_sequences_to_datetime(self, ctx, ymd_node, hms_node=None):
        nodes = [*ymd_node.members]
        if hms_node and hms_node.members:
            nodes.extend(hms_node.members)
        return self._node_list_to_datetime(ctx, nodes)

    def _node_list_to_datetime(self, ctx, nodes):
        node_values = [self._get_node_value(n, ctx, True) for n in nodes]
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

    def _parse_node_1125(self, node, ctx):
        peek_nodes = [ctx.peek(1), ctx.peek(2), ctx.peek(3)]
        expected = [1126, 1127, 1128]
        if any(n is None or n.descriptor.id != expected[idx] for idx, n in enumerate(peek_nodes)):
            return self._parse_node(node, ctx, _skip_custom_check=True)
        node_vals = [self._get_node_value(n, ctx, True) for n in peek_nodes]
        if all(v is None for v in node_vals):
            self._apply_instruction({
                'type': 'metadata',
                'name': 'WIGOSID',
                'apply_to': 'target'
            }, None, ctx)
            ctx.skip = 3
        elif any(v is None for v in node_vals):
            return self._parse_node(node, ctx, _skip_custom_check=True)
        else:
            self._apply_instruction({
                'type': 'metadata',
                'name': 'WIGOSID',
                'apply_to': 'target'
            }, '-'.join(str(x) for x in node_vals), ctx)

    def _parse_node_1087(self, node, ctx):
        val = self._get_node_value(node, ctx, True)
        if val is not None:
            val = str(val)
            if len(val) < 7:
                val = f"{val[0:2]}{val[2:].zfill(5)}"
        self._apply_instruction({
            'type': 'metadata',
            'name': 'WMOID',
            'apply_to': 'target'
        }, val, ctx)

    def _parse_node_1001(self, node, ctx):
        peek1 = ctx.peek(1)
        if not (peek1 and peek1.descriptor.id == 1002):
            return self._parse_node(node, ctx, _skip_custom_check=True)
        elements = [
            self._get_node_value(node, ctx, True),
            self._get_node_value(peek1, ctx, True)
        ]

        if all(x is None for x in elements):
            self._apply_instruction({
                'type': 'metadata',
                'name': 'WMOID',
                'apply_to': 'target'
            }, None, ctx)
            ctx.skip = 1
        elif any(x is None for x in elements):
            self._parse_node(node, ctx, _skip_custom_check=True)
        else:
            self._apply_instruction({
                'type': 'metadata',
                'name': 'WMOID',
                'apply_to': 'target'
            }, f'{elements[0]}{str(elements[1]).zfill(5)}', ctx)
            ctx.skip = 1

    def _parse_node_1003(self, node, ctx):
        peek1 = ctx.peek(1)
        if not (peek1 and peek1.descriptor.id in (1020, 1004)):
            return self._parse_node(node, ctx, _skip_custom_check=True)
        peek2 = ctx.peek(2)
        if not (peek2 and peek2.descriptor.id == 1005):
            return self._parse_node(node, ctx, _skip_custom_check=True)
        elements = [
            self._get_node_value(node, ctx, True),
            self._get_node_value(peek1, ctx, True),
            self._get_node_value(peek2, ctx, True)
        ]
        if all(x is None for x in elements):
            self._apply_instruction({
                'type': 'metadata',
                'name': 'WMOID',
                'apply_to': 'target'
            }, None, ctx)
            ctx.skip = 2
        elif any(x is None for x in elements):
            self._parse_node(node, ctx, _skip_custom_check=True)
        else:
            self._apply_instruction({
                'type': 'metadata',
                'name': 'WMOID',
                'apply_to': 'target'
            }, f'{elements[0]}{elements[1]}{str(elements[2]).zfill(5)}', ctx)
            ctx.skip = 2

    def _parse_node_8080(self, node, ctx):
        nxt = ctx.peek(1)
        flag_value = None
        applies_to = self._get_node_value(node, ctx, True)
        if nxt and nxt.descriptor.id == 33050:
            ctx.skip += 1
            flag_value = self._get_node_value(nxt, ctx, True)
            if flag_value is not None:
                flag_value = str(flag_value)
        if applies_to == 20:
            self._add_coordinate_quality_flag(ctx, 'Latitude', flag_value)
            self._add_coordinate_quality_flag(ctx, 'Longitude', flag_value)
        elif applies_to == 4:
            self._add_parameter_quality_flag(ctx, 'SeaDepth', flag_value)
        elif applies_to == 10:
            self._add_coordinate_quality_flag(ctx, 'Pressure', flag_value)
        elif applies_to == 11:
            self._add_parameter_quality_flag(ctx, 'Temperature', flag_value)
        elif applies_to == 12:
            self._add_parameter_quality_flag(ctx, 'PracticalSalinity', flag_value)
        elif applies_to == 13:
            self._add_coordinate_quality_flag(ctx, 'Depth', flag_value)
        elif applies_to == 14:
            self._add_parameter_quality_flag(ctx, 'CurrentSpeed', flag_value)
        elif applies_to == 15:
            self._add_parameter_quality_flag(ctx, 'CurrentDirection', flag_value)
        elif applies_to == 16:
            self._add_parameter_quality_flag(ctx, 'DissolvedOxygen', flag_value)
        elif applies_to is None:
            if flag_value is not None:
                self.warn(f"GTSPP quality flag applies to was none, but flag value was not none", ctx)
        else:
            self.warn(f"unhandled GTSPP quality flag [{applies_to}]", ctx)

    def _add_parameter_quality_flag(self, ctx, parameter, quality):
        param = ctx.target.parameters.get(parameter)
        if param:
            param.metadata['Quality'] = quality
        elif quality is not None and quality != "0":
            self.warn(f"cannot find parameter {parameter}", ctx)

    def _add_coordinate_quality_flag(self, ctx, parameter, quality):
        param = ctx.target.coordinates.get(parameter)
        if param:
            param.metadata['Quality'] = quality
        elif quality is not None and quality != "0":
            self.warn(f"cannot find coordinate {parameter}", ctx)

    def _parse_node_8041(self, node, ctx: _Bufr4DecoderContext):
        value = self._get_node_value(node, ctx, True)
        if value == 13:
            self._apply_instruction({
                'type': 'metadata',
                'name': 'InstrumentManufacturingDate',
                'apply_to': 'target'
            }, self._parse_dt_sequence(ctx, 1), ctx)
        elif value is None:
            return
        else:
            self.warn(f"Unhandled 8041 value: {value}", ctx)

    def _parse_node_8021(self, node, ctx):
        value = self._get_node_value(node, ctx, True)
        if value == 2:
            self._apply_instruction({
                'type': 'metadata',
                'name': 'AggregationMethod',
                'apply_to': 'following'
            }, 'AVERAGE', ctx)
        elif value is None:
            self._apply_instruction({
                'type': 'metadata',
                'name': 'AggregationMethod',
                'apply_to': 'following'
            }, None, ctx)
        elif value == 25:
            self._apply_instruction({
                "type": "coordinates",
                "name": "Time",
                "apply_to": "target"
            }, self._parse_dt_sequence(ctx, 1), ctx)
        elif value == 26:
            self._apply_instruction({
                "type": "metadata",
                "name": "LastKnownPositionTime",
                "apply_to": "target"
            }, self._parse_dt_sequence(ctx, 1), ctx)
        else:
            self.warn(f"Unhandled 8021 value: {value}", ctx)

    def _parse_node_8090(self, node, ctx: _Bufr4DecoderContext):
        ctx.scale_factor = self._get_node_value(node, ctx, True)

    def _parse_dt_sequence(self, ctx, start_at: int = 1):
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
        val = Value(val)
        if ctx.child_record_type == "TSERIES" and "Time" not in ctx.target.coordinates:
            if "TimeOffset" in ctx.target.coordinates and val.value != ctx.target.coordinates["TimeOffset"].value:
                self._start_new_record(ctx)
            self._apply_instruction({
                "type": "coordinates",
                "name": "TimeOffset",
                "apply_to": "target"
            }, val, ctx)
        else:
            self._apply_instruction({
                "type": "metadata",
                "name": "ObservationPeriod",
                "apply_to": "following"
            }, val, ctx)

    def _parse_node_4001(self, node, ctx: _Bufr4DecoderContext):
        self._apply_instruction({
            'type': 'coordinates',
            'name': 'Time',
            'apply_to': 'target'
        }, self._parse_dt_sequence(ctx, 0), ctx)
