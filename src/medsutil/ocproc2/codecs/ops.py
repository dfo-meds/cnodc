import datetime
import enum
from contextlib import contextmanager

from medsutil.dynamic import dynamic_name, dynamic_object
from medsutil.exceptions import CodedError
from medsutil.iso_duration import DurationUnit, ISODuration
from medsutil.ocproc2 import ParentRecord, BaseRecord, RecordSet, AbstractElement, ElementMap, SingleElement, \
    MultiElement
import typing as t

from medsutil.seawater import TemperatureScale


class OceanProcessingSchemaError(CodedError): CODE_SPACE = 'OPS'


RawValue = int | float | str | datetime.date | bool | None


class DataType(enum.Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DURATION = "duration"


class Instruction:

    def __init__(self, **kwargs):
        self.extras: dict[str, t.Any] = kwargs


class InstructionGroup(Instruction):

    def __init__(self, instructions: list[Instruction], **kwargs):
        self.instructions = instructions
        super().__init__(**kwargs)

    def iterate_instructions(self, context: OPSContext) -> t.Iterable[Instruction]:
        yield from self.instructions


class RecordSetInstructionGroup(InstructionGroup):

    def __init__(self,
                 recordset_type: str,
                 required_elements: list[str] | None,
                 forbidden_elements: list[str] | None,
                 optional_elements: list[str] | None,
                 no_repeats: bool = True,
                 **kwargs):
        self.no_repeats: bool = no_repeats
        self.recordset_type = recordset_type
        self.elements = optional_elements
        self.required_elements = required_elements
        self.forbidden_elements = forbidden_elements
        super().__init__(**kwargs)

    def iterate_instructions(self, context: OPSContext) -> t.Iterable[Instruction]:
        rs = context.find_recordset(
            self.recordset_type,
            forbidden_elements=self.forbidden_elements,
            required_elements=self.required_elements,
            helpful_elements=self.elements,
            no_repeats=self.no_repeats
        )
        if rs is not None:
            with context.recordset_context(rs, self.recordset_type):
                yield from self.instructions


class RecordRepeatInstructionGroup(Instruction):

    def __init__(self,
                 instructions: list[Instruction],
                 **kwargs):
        self.instructions = instructions
        super().__init__(**kwargs)

    def iterate_records(self, context: OPSContext) -> t.Iterable[list[Instruction]]:
        if context.recordset is not None:
            for record in context.recordset.records.iterate_with_load():
                with context.record_context(record):
                    yield self.instructions

class RecordSetRepeatInstructionGroup(Instruction):

    def __init__(self,
                 recordset_type: str,
                 instructions: list[Instruction],
                 required_elements: list[str] | None = None,
                 forbidden_elements: list[str] | None = None,
                 optional_elements: list[str] | None = None,
                 **kwargs):
        self.required_elements = required_elements
        self.forbidden_elements = forbidden_elements
        self.optional_elements = optional_elements
        self.recordset_type = recordset_type
        self.instructions = instructions
        super().__init__(**kwargs)

    def iterate_recordsets(self, context: OPSContext) -> t.Iterable[list[Instruction]]:
        if context.recordset is not None:
            while rs := context.find_recordset(
                self.recordset_type,
                required_elements=self.required_elements,
                forbidden_elements=self.forbidden_elements,
                helpful_elements=self.optional_elements,
                no_repeats=True
            ):
                with context.recordset_context(rs, self.recordset_type):
                    yield self.instructions

class SingleValueInstruction(Instruction):

    def set_value(self, value: RawValue | AbstractElement, metadata: dict, context: OPSContext, **kwargs):
        ...

    def get_value(self, context: OPSContext) -> RawValue:
        ...


class StaticInstruction(SingleValueInstruction):

    def __init__(self,
                 value: RawValue,
                 **kwargs):
        self._value = value
        super().__init__(**kwargs)

    def get_value(self, context: OPSContext) -> RawValue:
        return self._value

    def set_value(self, value: RawValue | AbstractElement, metadata: dict, context: OPSContext, **kwargs):
        ...


class NoopInstruction(Instruction):
    ...


class ScaleFactorInstruction(Instruction):
    ...


class ContextInstruction(Instruction):

    def __init__(self,
                 context: dict[RawValue, Instruction],
                 default_instruction: Instruction | None = None,
                 **kwargs):
        self._context = context
        self._default = default_instruction or NoopInstruction()
        super().__init__(**kwargs)

    def get_instruction(self, context_options: list[RawValue]) -> Instruction:
        for x in context_options:
            if x in self._context:
                return self._context[x]
        return self._default


class ValueMappedInstruction(Instruction):

    def __init__(self,
                 instruction_map: dict[RawValue, Instruction],
                 default_instruction: Instruction | None = None,
                 **kwargs):
        self._default = default_instruction or NoopInstruction()
        self._instruction_map = instruction_map
        super().__init__(**kwargs)

    def get_instruction(self, value: RawValue):
        if value in self._instruction_map:
            return self._instruction_map[value]
        return self._default






class ElementInstruction(SingleValueInstruction):

    def __init__(self,
                 element: str,
                 data_type: str | DataType,
                 component: str | None = None,
                 filters: dict[str, RawValue] | None = None,
                 metadata: dict[str, RawValue] | None = None,
                 remove_metadata: list[str] | None = None,
                 iterate_into_recordset: bool = False,
                 restrict_recordsets: list[str] | None = None,
                 restrict_elements: list[str] | None = None,
                 places: int | None = None,
                 units: str | None = None,
                 import_map: dict[RawValue, RawValue] | None = None,
                 export_map: dict[RawValue, RawValue] | None = None,
                 export_temperature_scale: str | None = None,
                 future_context: str | int | None = None,
                 import_processor: str | None = None,
                 export_processor: str | None = None,
                 append: bool = False,
                 **kwargs):
        try:
            self.data_type = DataType(data_type) if not isinstance(data_type, DataType) else data_type
        except ValueError as ex:
            raise OceanProcessingSchemaError(f"Invalid data type", 1300) from ex
        self.metadata = metadata
        self.remove_metadata = remove_metadata
        self.component = component
        self.units = units
        self.places = places
        self.filters = filters
        self.element_path = element
        self.restrict_names = restrict_elements
        self.restrict_recordsets = restrict_recordsets
        self.iterate_into_recordset = iterate_into_recordset
        self.future_context = future_context
        self.export_temperature_scale = export_temperature_scale
        self.export_map = export_map
        self.import_map = import_map
        self._import_processor_name = import_processor
        self._export_processor_name = export_processor
        self._import_processor = ...
        self._export_processor = ...
        self.append_mode: bool = append
        super().__init__(**kwargs)

    @property
    def import_processor(self) -> t.Callable | None:
        if self._import_processor is ...:
            self._import_processor = None if self._import_processor_name is None else dynamic_object(self._import_processor_name)
        return self._import_processor

    @property
    def export_processor(self) -> t.Callable | None:
        if self._export_processor is ...:
            self._export_processor = None if self._export_processor_name is None else dynamic_object(self._export_processor_name)
        return self._export_processor

    def clean_input_value(self, value) -> t.Any:
        imp_p = self.import_processor
        if imp_p is not None:
            value = imp_p(value)
        if self.import_map and value in self.import_map:
            value = self.import_map[value]
        if value is None:
            return None
        if self.data_type is DataType.DURATION:
            if isinstance(value, (float | int)) and self.units:
                du = DurationUnit(self.units)
                isod = ISODuration.from_duration(value, du)
                return isod.isoformat()
            elif isinstance(value, str) and value.isdigit() and self.units:
                du = DurationUnit(self.units)
                isod = ISODuration.from_duration(float(value), du)
                return isod.isoformat()

        return value

    def assemble_element(self, value, metadata: dict | None, kwargs: dict):
        value = self.clean_input_value(value)
        e = ElementMap.ensure_element(value, metadata, **kwargs)
        if self.remove_metadata:
            for x in self.remove_metadata:
                if x in e.metadata:
                    del e.metadata[x]
        if self.metadata:
            e.metadata.update(self.metadata)
        return e

    def set_value(self,
                  context: OPSContext,
                  value: RawValue | AbstractElement,
                  metadata: dict | None = None,
                  **kwargs):
        e = self.assemble_element(value, metadata, kwargs)
        if self.element_path.startswith((
            "parameters/",
            "metadata/",
            "coordinates/"
        )):
            if self.append_mode:
                context.record.append_element_to(self.element_path, e)
            else:
                context.record.set_element(self.element_path, e)
        elif self.element_path.startswith("parent/"):
            if self.append_mode:
                context.parent.append_element_to('/'.join(self.element_path.split('/')[1:]), e)
            else:
                context.parent.set_element('/'.join(self.element_path.split('/')[1:]), e)
        elif self.element_path.startswith("recordset/"):
            if context.recordset is not None:
                _, md_name = self.element_path.split('/', maxsplit=1)
                if self.append_mode:
                    context.recordset.metadata.append_element_to(md_name, e)
                else:
                    context.recordset.metadata.set_element(md_name, e)
        elif self.element_path.startswith("common-recordset/"):
            _, md_name = self.element_path.split('/', maxsplit=1)
            context.add_common_recordset_metadata(md_name, e, self.restrict_recordsets, self.future_context)
        elif self.element_path.startswith("common/"):
            _, md_name = self.element_path.split('/', maxsplit=1)
            context.add_common_metadata(md_name, e, self.restrict_recordsets, self.restrict_names, self.iterate_into_recordset, self.future_context)
        else:
            raise OceanProcessingSchemaError("Invalid element path for an element instruction", 1200)

    def get_value(self, context: OPSContext) -> RawValue:
        if self.element_path.startswith((
            "parameters/",
            "metadata/",
            "coordinates/"
        )):
            v = self._get_record_child_element(context)
        elif self.element_path.startswith("parent/"):
            v = self._get_parent_child_element(context)
        elif self.element_path.startswith((
            "recordset/",
            "common-recordset/"
        )):
            v = self._get_recordset_child_element(context)
        elif self.element_path.startswith("common/"):
            v = self._get_common_element(context)
        else:
            raise OceanProcessingSchemaError("Invalid element path for an element instruction", 1200)
        exp_p = self.export_processor
        if exp_p is not None:
            return exp_p(v)
        return v

    def _get_record_child_element(self, context: OPSContext) -> RawValue:
        return self._process_element(
            context.record.find_child(self.element_path),
            context
        )

    def _get_parent_child_element(self, context: OPSContext) -> RawValue:
        return self._process_element(
            context.parent.find_child(self.element_path.split('/')[1:]),
            context
        )

    def _get_recordset_child_element(self, context: OPSContext) -> RawValue:
        if context.recordset is None:
            return self._process_element(None, context)
        return self._process_element(
            context.recordset.find_child(self.element_path.split('/')[1:]),
            context
        )

    def _get_common_element(self, context: OPSContext) -> RawValue:
        values = set()
        _, metadata_name = self.element_path.split('/', maxsplit=1)
        for element in context.iterate_elements(self.restrict_recordsets, self.restrict_names, self.iterate_into_recordset):
            v = self._process_element(element.metadata.get(metadata_name, None), context)
            if v is not None:
                values.add(v)
        if len(values) == 0:
            return None
        elif len(values) == 1:
            return list(values)[0]
        else:
            raise OceanProcessingSchemaError("Multiple common elements detected", 1000)

    def _process_element(self, v: t.Any, context: OPSContext) -> RawValue:
        if v is None:
            return None
        if not isinstance(v, AbstractElement):
            raise OceanProcessingSchemaError("Invalid path for an element instruction", 1100)
        best_value = self._find_best_value(v)
        if best_value is None:
            return None
        if self.data_type is DataType.STRING:
            return best_value.to_string()
        elif self.data_type is DataType.INTEGER:
            return best_value.to_int()
        elif self.data_type is DataType.FLOAT:
            if self.export_temperature_scale is not None:
                from medsutil import ocproc_math
                v = ocproc_math.get_temperature(
                    temperature=best_value,
                    obs_date=context.parent.coordinates.ideal("Time"),
                    units=self.units or "",
                    temperature_scale=TemperatureScale(self.export_temperature_scale)
                )
            else:
                v = best_value.to_float(self.units)
            if self.places is not None:
                return round(v)
            else:
                return v
        else:
            raise OceanProcessingSchemaError("Invalid data type", 1101)

    def _find_best_value(self, v: AbstractElement) -> SingleElement | None:
        if self.filters is not None and self.filters:
            passed_values = []
            for s in v.all_values():
                if all(s.metadata.best(x) == y for x, y in self.filters.items()):
                    passed_values.append(s)
            if len(passed_values) == 0:
                return None
            elif len(passed_values) == 1:
                v = passed_values[0]
            else:
                v = MultiElement(passed_values, _skip_normalization=True)
        value = v.ideal()
        if value.is_empty():
            return value
        if self.component is not None:
            if self.component in ("year", "month", "day", "hour", "minute", "second"):
                return SingleElement(
                    getattr(v.to_datetime(), self.component)
                )
            elif self.component in ("wigos1", "wigos2", "wigos3", "wigos4"):
                p = value.to_string().split("-", maxsplit=3)
                return SingleElement(p[int(self.component[5])])
            elif self.component in ("years", "months", "days", "hours", "minutes", "seconds"):
                return SingleElement(self.convert_duration(value.to_string(), self.component))
            else:
                raise OceanProcessingSchemaError("Invalid component", 1400)
        else:
            return value

    @staticmethod
    def convert_duration(duration: str, output_units: str) -> float:
        from medsutil.iso_duration import ISODuration, DurationUnit
        isod = ISODuration.from_iso_format(duration)
        try:
            ou = DurationUnit(output_units)
        except ValueError:
            raise OceanProcessingSchemaError("Invalid output units", 1900)
        return isod.to_duration(ou)


class OPSContext:

    class FutureMetadata:
        def __init__(self,
                     element: AbstractElement,
                     rs_types: list[str] | None,
                     names: list[str] | None | None = None,
                     iterate_into_recordset: bool = False):
            self.element = element
            self.rs_types = rs_types
            self.names = names
            self.iterate_into_recordset = iterate_into_recordset

    def __init__(self, record: ParentRecord):
        self.parent: ParentRecord = record
        self.record: BaseRecord = record
        self.recordset_type: str | None = None
        self.recordset: RecordSet | None = None
        self._future_metadata: dict[str | int | None, dict[str, OPSContext.FutureMetadata]] = {}
        self._ignore_rsids: list[int] = []

    @contextmanager
    def record_context(self, r: BaseRecord):
        old_record = self.record
        old_rsids = self._ignore_rsids
        try:
            self.record = r
            self._ignore_rsids = []
            yield self
        finally:
            self.record = old_record
            self._ignore_rsids = old_rsids

    @contextmanager
    def recordset_context(self, rs: RecordSet, rs_type: str):
        old_rs_type = self.recordset_type
        old_rs = self.recordset
        try:
            self.recordset = rs
            self.recordset_type = rs_type
            yield self
        finally:
            self.recordset = old_rs
            self.recordset_type = old_rs_type

    def add_common_recordset_metadata(self,
                                      metadata_name: str,
                                      element: AbstractElement,
                                      restrict_recordsets: list[str] | None = None,
                                      future_context: str | int | None = None,):
        if element.value is not None:
            if future_context not in self._future_metadata:
                self._future_metadata[future_context] = {}
            self._future_metadata[future_context][metadata_name] = OPSContext.FutureMetadata(
                element, restrict_recordsets
            )
        else:
            if future_context in self._future_metadata and metadata_name in self._future_metadata[future_context]:
                del self._future_metadata[future_context][metadata_name]

    def add_common_metadata(self,
                            metadata_name: str,
                            element: AbstractElement,
                            restrict_recordsets: list[str] | None = None,
                            restrict_names: list[str] | None = None,
                            iterate_into_recordset: bool = False,
                            future_context: str | int | None = None):
        if element.value is not None:
            if future_context not in self._future_metadata:
                self._future_metadata[future_context] = {}
            self._future_metadata[future_context][metadata_name] = OPSContext.FutureMetadata(
                element, restrict_recordsets, restrict_names, iterate_into_recordset
            )
        else:
            if future_context in self._future_metadata and metadata_name in self._future_metadata[future_context]:
                del self._future_metadata[future_context][metadata_name]

    def find_recordset(self,
                       recordset_type: str,
                       required_elements: list[str] | None,
                       forbidden_elements: list[str] | None,
                       helpful_elements: list[str] | None,
                       no_repeats: bool = True) -> RecordSet | None:
        if recordset_type not in self.record.subrecords.record_sets:
            return None
        best_id, best_rs = None, None
        best_count = -1
        for rs_id, rs in self.record.subrecords.record_sets[recordset_type].items():
            if no_repeats and rs_id in self._ignore_rsids:
                continue
            rs_elements = set()
            for record in rs.records.iterate_with_load():
                rs_elements.update(record.parameters.keys())
                rs_elements.update(record.metadata.keys())
                rs_elements.update(record.coordinates.keys())
            if forbidden_elements is not None and any(x in rs_elements for x in forbidden_elements):
                continue
            if required_elements is not None and any(x not in rs_elements for x in required_elements):
                continue
            if helpful_elements is not None:
                c = sum(1 if x in rs_elements else 0 for x in helpful_elements)
            else:
                c = 0
            if c > best_count:
                best_count = c
                best_id = rs_id
                best_rs = rs
        if best_id is not None:
            self._ignore_rsids.append(best_id)
        return best_rs

    def iterate_elements(self,
                         restrict_name: list[str] | None,
                         restrict_recordsets: list[str] | None = None,
                         iterate_into_recordset: bool = False) -> t.Iterable[AbstractElement]:
        yield from self._iterate_record_elements(self.record, restrict_name)
        if iterate_into_recordset:
            if self.recordset is not None and ((not restrict_recordsets) or self.recordset_type in restrict_recordsets):
                yield from self._iterate_recordset_elements(self.recordset, restrict_recordsets, restrict_name)
            yield from self._iterate_record_recordset_elements(self.record, restrict_recordsets, restrict_name)

    @staticmethod
    def _iterate_record_recordset_elements(record: BaseRecord,
                                           restrict_recordsets: list[str] | None,
                                           restrict_names: list[str] | None) -> t.Iterable[AbstractElement]:
        for rs_type in record.subrecords:
            if restrict_recordsets is None or rs_type in restrict_recordsets:
                for _, rs in record.subrecords.record_sets[rs_type].items():
                    yield from OPSContext._iterate_recordset_elements(rs, restrict_recordsets, restrict_names)

    @staticmethod
    def _iterate_recordset_elements(rs: RecordSet,
                                    restrict_recordsets: list[str] | None,
                                    restrict_name: list[str] | None) -> t.Iterable[AbstractElement]:
        yield from OPSContext._iterate_element_map_elements(rs.metadata, restrict_name)
        for record in rs.records.iterate_with_load():
            yield from OPSContext._iterate_record_elements(record, restrict_name)
            yield from OPSContext._iterate_record_recordset_elements(record, restrict_recordsets, restrict_name)

    @staticmethod
    def _iterate_record_elements(record: BaseRecord,
                                 restrict_name: list[str] | None) -> t.Iterable[AbstractElement]:
        yield from OPSContext._iterate_element_map_elements(record.metadata, restrict_name)
        yield from OPSContext._iterate_element_map_elements(record.parameters, restrict_name)
        yield from OPSContext._iterate_element_map_elements(record.coordinates, restrict_name)

    @staticmethod
    def _iterate_element_map_elements(em: ElementMap,
                                      restrict_name: list[str] | None) -> t.Iterable[AbstractElement]:
        if restrict_name is None or not restrict_name:
            yield from em.values()
        else:
            for n in restrict_name:
                v = em.get(n, None)
                if v is not None:
                    yield v
