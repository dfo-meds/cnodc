import copy
import datetime
import enum
from contextlib import contextmanager
from types import EllipsisType

from medsutil.dynamic import dynamic_name, dynamic_object
from medsutil.exceptions import CodedError
from medsutil.iso_duration import DurationUnit, ISODuration
from medsutil.ocproc2 import ParentRecord, BaseRecord, RecordSet, AbstractElement, ElementMap, SingleElement, \
    MultiElement, ChildRecord
import typing as t

from medsutil.ocproc2.util import Quality, find_quality_for_protocol, combine_quality_scores
from medsutil.seawater import TemperatureScale
from medsutil import math as amath
from medsutil.units.units import convert, is_compatible


class OceanProcessingSchemaError(CodedError): CODE_SPACE = 'OPS'

class SkipDecodeInterrupt(Exception): ...

RawValue = int | float | str | datetime.date | bool | None
RSElementList = list[str | list[str]] | None


class DataType(enum.Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DURATION = "duration"


class Instruction:

    _factories: t.ClassVar[list[type[Instruction]] | None] = None

    def __init__(self, **kwargs):
        self.extras: dict[str, t.Any] = kwargs

    @staticmethod
    def register_factory(factory: type[Instruction]):
        if Instruction._factories is None:
            Instruction._factories = []
        Instruction._factories.append(factory)

    @staticmethod
    def factories() -> list[type[Instruction]]:
        return Instruction._factories or []

    @classmethod
    def build(cls,
              instruction: dict[str, t.Any] | None = None,
              my_extras: dict[str, t.Any] | None = None,
              parent_extras: dict[str, t.Any] | None = None,
              omit_keys: t.Container[str] | None = None):
        kwargs = {}
        if parent_extras:
            kwargs.update(parent_extras)
        if my_extras:
            kwargs.update(my_extras)
        if instruction:
            kwargs.update(instruction)
        if omit_keys:
            return cls(**{k: v for k, v in kwargs.items() if k not in omit_keys})
        else:
            return cls(**kwargs)

    @classmethod
    def factory(cls,
                instruction: dict[str, t.Any] | str,
                extras: dict[str, t.Any] | None = None,
                helper: t.Callable | None = None) -> Instruction | None:
        raise NotImplementedError

    @staticmethod
    def parse_instructions(instructions: list[dict | str],
                          helper: t.Callable[[dict | str, dict | None, t.Callable | None], dict | str | Instruction] | None = None) -> list[Instruction]:
        built = []
        for instruction in instructions:
            built.append(Instruction.parse_instruction(instruction, helper))
        return built

    @staticmethod
    def parse_instruction(instruction: dict | str | Instruction,
                          helper: t.Callable[[dict | str, dict | None, t.Callable | None], dict | str | Instruction] | None = None,
                          common_kwargs: dict[str, t.Any] | None = None) -> Instruction:
        try:
            if helper is not None and not isinstance(instruction, Instruction):
                instruction = helper(instruction, common_kwargs, helper)

            if not isinstance(instruction, Instruction):
                for instruction_type in Instruction.factories():
                    res = instruction_type.factory(instruction, common_kwargs, helper)
                    if res is not None:
                        instruction = res
                        break

            if isinstance(instruction, Instruction):
                return instruction

            raise OceanProcessingSchemaError("Unrecognized instruction", 3000)
        except Exception as ex:
            ex.add_note("Instruction: " + str(instruction))
            raise

    @staticmethod
    def parse_element_for_tags(element: str) -> dict:
        if "[" not in element:
            return {"element": element}
        element, tag = element.split("]")
        tag = tag[:-1]
        if "=" in tag:
            k,v = tag.split("=", maxsplit=1)
            return {
                "element": element,
                "filters": {k.strip(): v.strip()},
                "metadata": {k.strip(): v.strip()},
            }
        else:
            return {
                "element": element,
                "component": tag
            }


class SkipDecodeInstruction(Instruction):

    def raise_exception(self):
        raise SkipDecodeInterrupt

    @classmethod
    def factory(cls,
                instruction: dict[str, t.Any] | str,
                extras: dict[str, t.Any] | None = None,
                helper: t.Callable | None = None) -> Instruction | None:
        if isinstance(instruction, dict):
            if instruction.get("instruction", "") == "skip":
                return cls.build(instruction, extras, omit_keys={"instruction"})
        elif instruction == "skip":
            return cls.build(None, extras, omit_keys={"instruction"})
        return None


class InstructionGroup(Instruction):

    def __init__(self, instructions: list[Instruction], **kwargs):
        self.instructions = instructions
        super().__init__(**kwargs)

    def iterate_instructions(self, context: OPSContext) -> t.Iterable[Instruction]:
        yield from self.instructions


    @classmethod
    def factory(cls,
                instruction: dict[str, t.Any] | str,
                extras: dict[str, t.Any] | None = None,
                helper: t.Callable | None = None) -> Instruction | None:
        if isinstance(instruction, dict):
            if "instructions" in instruction and "recordset_type" not in instruction and "repeats" not in instruction:
                return cls.build({
                    "instructions": Instruction.parse_instructions(instruction["instructions"], helper),
                }, instruction, extras)
        return None


class RecordSetInstructionGroup(InstructionGroup):

    def __init__(self,
                 recordset_type: str,
                 required_elements: RSElementList = None,
                 forbidden_elements: RSElementList = None,
                 optional_elements: RSElementList = None,
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

    @classmethod
    def factory(cls,
                instruction: dict[str, t.Any] | str,
                extras: dict[str, t.Any] | None = None,
                helper: t.Callable | None = None) -> Instruction | None:
        if isinstance(instruction, dict):
            if "instructions" in instruction and "recordset_type" in instruction and "repeats" not in instruction:
                return cls.build({
                    "instructions": Instruction.parse_instructions(instruction["instructions"], helper),
                }, instruction, extras)
        return None

class RepeatGroup(Instruction):

    def __init__(self,
                 repeats: int | None = None,
                 **kwargs):
        self.repeats = repeats
        super().__init__(**kwargs)

    def iterate_repeats(self, context: OPSContext) -> t.Iterable[list[Instruction]]:
        ...


class RecordRepeatInstructionGroup(RepeatGroup):

    def __init__(self,
                 instructions: list[Instruction],
                 **kwargs):
        self.instructions = instructions
        super().__init__(**kwargs)

    def iterate_repeats(self, context: OPSContext) -> t.Iterable[list[Instruction]]:
        if context.recordset is not None:
            for record in context.recordset.records.iterate_with_load():
                with context.record_context(record):
                    yield self.instructions

    @classmethod
    def factory(cls,
                instruction: dict[str, t.Any] | str,
                extras: dict[str, t.Any] | None = None,
                helper: t.Callable | None = None) -> Instruction | None:
        if isinstance(instruction, dict):
            if "instructions" in instruction and "recordset_type" not in instruction and "repeats" in instruction:
                return cls.build({
                    "instructions": Instruction.parse_instructions(instruction["instructions"], helper),
                }, instruction, extras)
        return None

class RecordSetRepeatInstructionGroup(RepeatGroup):

    def __init__(self,
                 recordset_type: str,
                 instructions: list[Instruction],
                 required_elements: RSElementList = None,
                 forbidden_elements: RSElementList = None,
                 optional_elements: RSElementList = None,
                 **kwargs):
        self.required_elements = required_elements
        self.forbidden_elements = forbidden_elements
        self.optional_elements = optional_elements
        self.recordset_type = recordset_type
        self.instructions = instructions
        super().__init__(**kwargs)

    def iterate_repeats(self, context: OPSContext) -> t.Iterable[list[Instruction]]:
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

    @classmethod
    def factory(cls,
                instruction: dict[str, t.Any] | str,
                extras: dict[str, t.Any] | None = None,
                helper: t.Callable | None = None) -> Instruction | None:
        if isinstance(instruction, dict):
            if "instructions" in instruction and "recordset_type" in instruction and "repeats" in instruction:
                return cls.build({
                    "instructions": Instruction.parse_instructions(instruction["instructions"], helper),
                }, instruction, extras)
        return None


class SingleValueInstruction(Instruction):

    def set_value(self, value: RawValue | AbstractElement, metadata: dict | None, context: OPSContext, **kwargs):
        ...

    def get_value_with_details(self, context: OPSContext) -> tuple[RawValue, int | None, float | None, SingleElement | None]:
        raise NotImplementedError

    def get_quality(self, context: OPSContext) -> int | None:
        return self.get_value_with_details(context)[1]

    def get_value(self, context: OPSContext) -> RawValue:
        return self.get_value_with_details(context)[0]

    def get_uncertainty(self, context: OPSContext) -> float | None:
        return self.get_value_with_details(context)[2]



class StaticInstruction(SingleValueInstruction):

    def __init__(self,
                 value: RawValue,
                 quality: int | None = None,
                 precision: float | None = None,
                 **kwargs):
        self._value = value
        self._quality = quality
        self._precision = precision
        super().__init__(**kwargs)

    def get_value_with_details(self, context: OPSContext) -> tuple[RawValue, int | None, float | None, float | None]:
        return self._value, self._quality, self._precision, None

    def set_value(self, value: RawValue | AbstractElement, metadata: dict | None, context: OPSContext, **kwargs):
        ...

    @classmethod
    def factory(cls,
                instruction: dict[str, t.Any] | str,
                extras: dict[str, t.Any] | None = None,
                helper: t.Callable | None = None) -> Instruction | None:
        if isinstance(instruction, dict):
            if "value" in instruction:
                return cls.build(instruction, extras)
        return None


class NoopInstruction(Instruction):

    @classmethod
    def factory(cls,
                instruction: dict[str, t.Any] | str,
                extras: dict[str, t.Any] | None = None,
                helper: t.Callable | None = None) -> Instruction | None:
        if isinstance(instruction, dict):
            if instruction.get("instruction", "") == "noop":
                return cls.build(instruction, extras, omit_keys={"instruction"})
        elif instruction == "noop":
            return cls.build(None, extras, omit_keys={"instruction"})
        return None



class ScaleFactorInstruction(Instruction):

    @classmethod
    def factory(cls,
                instruction: dict[str, t.Any] | str,
                extras: dict[str, t.Any] | None = None,
                helper: t.Callable | None = None) -> Instruction | None:
        if isinstance(instruction, dict):
            if instruction.get("instruction", "") == "scale_factor":
                return cls.build(instruction, extras, omit_keys={"instruction"})
        elif instruction == "scale_factor":
            return cls.build(None, extras, omit_keys={"instruction"})
        return None


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

    @classmethod
    def factory(cls,
                instruction: dict[str, t.Any] | str,
                extras: dict[str, t.Any] | None = None,
                helper: t.Callable | None = None) -> Instruction | None:
        if isinstance(instruction, dict):
            if "context" in instruction and instruction["context"]:
                kwargs = {k: d for k, d in instruction.items() if k != "context"}
                return cls.build({
                    "context": {
                        k: Instruction.parse_instruction(d, helper, kwargs)
                        for k, d in instruction["context"].items()
                    }
                })
        return None


class EncodeDecodeGroup(Instruction):

    def __init__(self, encode_instruction: Instruction, decode_instruction: Instruction, **kwargs):
        super().__init__(**kwargs)
        self.encode = encode_instruction
        self.decode = decode_instruction

    def get_instruction(self, for_encode: bool = False) -> Instruction:
        if for_encode:
            return self.encode
        else:
            return self.decode

    @classmethod
    def factory(cls,
                instruction: dict[str, t.Any] | str,
                extras: dict[str, t.Any] | None = None,
                helper: t.Callable | None = None) -> Instruction | None:
        if isinstance(instruction, dict):
            encode_group: str | dict | None = instruction.get("encode", None)
            decode_group: str | dict | None = instruction.get("decode", None)
            if encode_group or decode_group:
                kwargs = {k: d for k, d in instruction.items() if k not in ("encode", "decode",)}
                return cls.build({
                    "encode_instruction": (
                        Instruction.parse_instruction(encode_group, helper, kwargs)
                        if encode_group else
                        NoopInstruction()
                    ),
                    "decode_instruction": (
                        Instruction.parse_instruction(decode_group, helper, kwargs)
                        if decode_group else
                        NoopInstruction()
                    )
                })
        return None


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

    @classmethod
    def factory(cls,
                instruction: dict[str, t.Any] | str,
                extras: dict[str, t.Any] | None = None,
                helper: t.Callable | None = None) -> Instruction | None:
        if isinstance(instruction, dict):
            if "instruction_map" in instruction and instruction["instruction_map"]:
                kwargs = {}
                if extras:
                    kwargs.update(extras)
                kwargs.update({k: d for k, d in instruction.items() if k != "instruction_map"})
                return cls.build({
                    "instruction_map": {
                        k: Instruction.parse_instruction(d, helper, kwargs)
                        for k, d in instruction["instruction_map"].items()
                    }
                })
        return None


class WorstQualityInstruction(SingleValueInstruction):

    def __init__(self,
                 elements: list[str],
                 **kwargs):
        self.elements = elements
        super().__init__(**kwargs)

    def set_value(self,
                  value: RawValue | AbstractElement,
                  metadata: dict | None,
                  context: OPSContext,
                  **kwargs):
        raise NotImplementedError  # TODO: set all the qualities on elements

    def get_value_with_details(self, context: OPSContext) -> tuple[RawValue, int | None, float | None, float | None]:
        def _elements() -> t.Iterable[SingleElement]:
            for element_name in self.elements:
                element = context.record.find_child(element_name)
                if element is None:
                    continue
                if not isinstance(element, AbstractElement):
                    raise OceanProcessingSchemaError("Invalid element path", 2000)
                yield from element.all_values()
        return context.get_quality(*_elements()), None, None, None


class ElementInstruction(SingleValueInstruction):

    def __init__(self,
                 element: str,
                 data_type: str | DataType,
                 component: str | None = None,
                 filters: dict[str, RawValue] | None = None,
                 metadata: dict[str, RawValue] | None = None,
                 remove_metadata: list[str] | None = None,
                 override_value: RawValue | EllipsisType = ...,
                 iterate_into_recordset: bool = False,
                 use_current_record: bool = True,
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
                 ocproc2_export_processor: str | None = None,
                 append: bool = False,
                 **kwargs):
        try:
            self.data_type = DataType(data_type) if not isinstance(data_type, DataType) else data_type
        except ValueError as ex:
            raise OceanProcessingSchemaError("Invalid data type", 1300) from ex
        self.metadata = metadata
        self.remove_metadata = remove_metadata
        self.component = component
        self.units = units
        self.places = places
        self.filters = filters
        self.element_path = element
        self.override_value = override_value
        self.restrict_names = restrict_elements
        self.restrict_recordsets = restrict_recordsets
        self.use_current_record = use_current_record
        self.iterate_into_recordset = iterate_into_recordset
        self.future_context = future_context
        self.export_temperature_scale = export_temperature_scale
        self.export_map = export_map
        self.import_map = import_map
        self._ocproc2_export_processor_name = ocproc2_export_processor
        self._import_processor_name = import_processor
        self._export_processor_name = export_processor
        self._ocproc2_export_processor = ...
        self._import_processor = ...
        self._export_processor = ...
        self.append_mode: bool = append
        super().__init__(**kwargs)

    @property
    def ocproc2_export_processor(self) -> t.Callable[[SingleElement], RawValue] | None:
        if self._ocproc2_export_processor is ...:
            self._ocproc2_export_processor = None if self._ocproc2_export_processor_name is None else dynamic_object(self._ocproc2_export_processor_name)
        return self._ocproc2_export_processor

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
        additional_kwargs = {}
        if self.places is not None:
            additional_kwargs["Uncertainty"] = SingleElement(
                (10 ** (-1 * self.places)) / 2,
                UncertaintyType="uniform"
            )
        if self.units is not None:
            additional_kwargs["Units"] = self.units
        e.metadata.update(additional_kwargs)
        if self.remove_metadata:
            for x in self.remove_metadata:
                if x in e.metadata:
                    del e.metadata[x]
        if self.metadata:
            e.metadata.update(self.metadata)
        return e

    def set_value(self,
                  value: RawValue | AbstractElement,
                  metadata: dict | None,
                  context: OPSContext,
                  **kwargs):
        e = self.assemble_element(value, metadata, kwargs)
        if self.element_path.startswith((
            "parameters/",
            "metadata/",
            "coordinates/"
        )):
            context.set_element(self.element_path, e, self.append_mode)
        elif self.element_path.startswith("parent/"):
            _, element_path = self.element_path.split('/', maxsplit=1)
            context.set_parent_element(element_path, e, self.append_mode)
        elif self.element_path.startswith("recordset/"):
            _, element_path = self.element_path.split('/', maxsplit=1)
            context.set_recordset_metadata(element_path, e, self.append_mode)
        elif self.element_path.startswith("common-recordset/"):
            _, md_name = self.element_path.split('/', maxsplit=1)
            context.add_common_recordset_metadata(md_name, e, self.restrict_recordsets, self.future_context)
        elif self.element_path.startswith("common/"):
            _, md_name = self.element_path.split('/', maxsplit=1)
            context.add_common_metadata(md_name, e, self.restrict_recordsets, self.restrict_names, self.iterate_into_recordset, self.future_context)
        else:
            raise OceanProcessingSchemaError("Invalid element path for an element instruction", 1200)

    def get_value_with_details(self, context: OPSContext) -> tuple[RawValue, int | None, float | None, float | None]:
        if self.element_path.startswith((
            "parameters/",
            "metadata/",
            "coordinates/"
        )):
            v, q, p, se = self._get_record_child_element(context)
        elif self.element_path.startswith("parent/"):
            v, q, p, se = self._get_parent_child_element(context)
        elif self.element_path.startswith((
            "recordset/",
            "common-recordset/"
        )):
            v, q, p, se = self._get_recordset_child_element(context)
        elif self.element_path.startswith("common/"):
            v, q, p, se = self._get_common_element(context)
        else:
            raise OceanProcessingSchemaError("Invalid element path for an element instruction", 1200)
        if self.override_value is not ...:
            v = t.cast(RawValue, self.override_value)
        exp_p = self.export_processor
        if exp_p is not None:
            v = exp_p(v)
        if self.export_map is not None and v in self.export_map:
            v = self.export_map[v]
        return v, q, p, se

    def _get_record_child_element(self, context: OPSContext) -> tuple[RawValue, int | None, float | None, float | None]:
        return self._process_element(
            context.record.find_child(self.element_path),
            context
        )

    def _get_parent_child_element(self, context: OPSContext) -> tuple[RawValue, int | None, float | None, float | None]:
        return self._process_element(
            context.parent.find_child(self.element_path.split('/')[1:]),
            context
        )

    def _get_recordset_child_element(self, context: OPSContext) -> tuple[RawValue, int | None, float | None, float | None]:
        if context.recordset is None:
            return self._process_element(None, context)
        return self._process_element(
            context.recordset.find_child(self.element_path.split('/')[1:]),
            context
        )

    def _get_common_element(self, context: OPSContext) -> tuple[RawValue, int | None, float | None, float | None]:
        values: dict[RawValue, tuple[RawValue, int | None, float | None, float | None]] = {}
        n_values = 0
        _, metadata_name = self.element_path.split('/', maxsplit=1)
        for element in context.iterate_elements(self.restrict_recordsets, self.restrict_names, self.iterate_into_recordset, self.use_current_record):
            v = self._process_element(element.metadata.get(metadata_name, None), context)
            if v[0] is not None:
                values[v[0]] = v
                n_values += 1
        if n_values == 0:
            return None, None, None, None
        elif n_values == 1:
            return values[list(values.keys())[0]]
        else:
            raise OceanProcessingSchemaError("Multiple common elements detected", 1000)

    def _extract_quality(self, element: SingleElement, context: OPSContext) -> int | None:
        return context.get_quality(element)

    def _process_element(self, v: t.Any, context: OPSContext) -> tuple[RawValue, int | None, float | None, float | None]:
        if v is None:
            return None, None, None, None
        if not isinstance(v, AbstractElement):
            raise OceanProcessingSchemaError("Invalid path for an element instruction", 1100)
        best_value = self._find_best_value(v)
        if best_value is None:
            return None, None, None, None
        quality = self._extract_quality(best_value, context)
        _, precision_worst, precision_best = best_value.precision_information()
        if self.ocproc2_export_processor is not None:
            return self.ocproc2_export_processor(best_value), quality, precision_worst, precision_best
        if self.data_type is DataType.STRING:
            return best_value.to_string(), quality, precision_worst, precision_best
        elif self.data_type is DataType.INTEGER:
            return best_value.to_int(), quality, precision_worst, precision_best
        elif self.data_type is DataType.FLOAT:
            if self.export_temperature_scale is not None:
                from medsutil import ocproc_math
                val = ocproc_math.get_temperature(
                    temperature=best_value,
                    obs_date=context.parent.coordinates.ideal("Time"),
                    units=self.units or "",
                    temperature_scale=TemperatureScale(self.export_temperature_scale)
                )
            else:
                val = best_value.to_float(self.units or "")
            if val is None:
                return None, quality, None, None
            # note that precision conversion is easier to handle in K/R than C/F, so we convert the precision appropriately
            # also note that a change of x degrees C is still x degrees K, thus no conversion necessary
            out_units = self.units or ""
            if is_compatible(out_units, "K") or is_compatible(out_units, "degrees_C"):
                precision_out_units = "degrees_K"
            elif is_compatible(out_units, "degrees_R") or is_compatible(out_units, "degrees_F"):
                precision_out_units = "degrees_R"
            else:
                precision_out_units = out_units

            bv_units = best_value.units() or ""
            if is_compatible(bv_units, "degrees_C") or is_compatible(bv_units, "K"):
                precision_in_units = "degrees_K"
            elif is_compatible(bv_units, "degrees_R") or is_compatible(bv_units, "degrees_F"):
                precision_in_units = "degrees_R"
            else:
                precision_in_units = bv_units

            precision_worst = convert(precision_worst, precision_in_units, precision_out_units)
            precision_best = convert(precision_best, precision_in_units, precision_out_units)
            if self.places is not None:
                return float(amath.round_to_place(val, self.places)), quality, precision_worst, precision_best
            else:
                return float(val), quality, precision_worst, precision_best
        else:
            raise OceanProcessingSchemaError("Invalid data type", 1101)

    def _find_best_value(self, v: AbstractElement) -> SingleElement | None:
        if self.filters is not None and self.filters:
            passed_values = []
            n_passed = 0
            for s in v.all_values():
                if all(s.metadata.best(x) == y for x, y in self.filters.items()):
                    passed_values.append(s)
                    n_passed += 1
            if n_passed == 0:
                return None
            elif n_passed == 1:
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
            elif self.component == "uncertainty":
                ... #TODO
            else:
                raise OceanProcessingSchemaError("Invalid component", 1400)
        else:
            return value

    @classmethod
    def factory(cls,
                instruction: dict[str, t.Any] | str,
                extras: dict[str, t.Any] | None = None,
                helper: t.Callable | None = None) -> Instruction | None:
        if isinstance(instruction, dict):
            if "element" in instruction:
                return cls.build(
                    Instruction.parse_element_for_tags(instruction["element"]),
                    instruction,
                    extras,
                )
        return None

    @staticmethod
    def convert_duration(duration: str, output_units: str) -> float:
        from medsutil.iso_duration import ISODuration, DurationUnit
        isod = ISODuration.from_iso_format(duration)
        try:
            ou = DurationUnit(output_units)
        except ValueError:
            raise OceanProcessingSchemaError("Invalid output units", 1900)
        return isod.to_duration(ou)

Instruction.register_factory(ElementInstruction)
Instruction.register_factory(StaticInstruction)

Instruction.register_factory(InstructionGroup)
Instruction.register_factory(RecordSetInstructionGroup)
Instruction.register_factory(RecordRepeatInstructionGroup)
Instruction.register_factory(RecordSetRepeatInstructionGroup)

Instruction.register_factory(ContextInstruction)
Instruction.register_factory(ValueMappedInstruction)
Instruction.register_factory(EncodeDecodeGroup)

Instruction.register_factory(NoopInstruction)
Instruction.register_factory(SkipDecodeInstruction)
Instruction.register_factory(ScaleFactorInstruction)


class OPSContext:

    class FutureMetadata:
        def __init__(self,
                     element: AbstractElement,
                     rs_types: list[str] | None,
                     names: list[str] | None = None,
                     iterate_into_recordset: bool = False):
            self.element = element
            self.rs_types = rs_types
            self.names = names
            self.iterate_into_recordset = iterate_into_recordset

        def __copy__(self):
            return OPSContext.FutureMetadata(
                element=self.element,
                rs_types=self.rs_types,
                names=self.names,
                iterate_into_recordset=self.iterate_into_recordset
            )

    def __init__(self,
                 record: ParentRecord,
                 test_protocols: list[str] | set[str] | tuple[str] | None = None):
        self.parent: ParentRecord = record
        self.record: BaseRecord = record
        self.recordset_type: str | None = None
        self.recordset: RecordSet | None = None
        self.extras = {}
        self.test_protocols: t.Iterable[str] | None = test_protocols
        self._future_rs_metadata: dict[str | int | None, dict[str, OPSContext.FutureMetadata]] = {}
        self._future_metadata: dict[str | int | None, dict[str, OPSContext.FutureMetadata]] = {}
        self._ignore_rsids: list[int] = []

    @contextmanager
    def subcontext(self):
        old_extras = self.extras
        old_futures = self._future_metadata
        old_futures_rs = self._future_rs_metadata
        try:
            self.extras = {x: copy.copy(y) for x, y in self.extras.items()}
            self._future_metadata = {
                k: {
                    sk: copy.copy(sd)
                    for sk, sd in d.items()
                }
                for k, d in self._future_metadata.items()
            }
            self._future_rs_metadata = {
                k: {
                    sk: copy.copy(sd)
                    for sk, sd in d.items()
                }
                for k, d in self._future_rs_metadata.items()
            }
            yield self
        finally:
            self.extras = old_extras
            self._future_metadata = old_futures
            self._future_rs_metadata = old_futures_rs

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
        old_futures = self._future_metadata
        try:
            self._future_metadata = {
                x: {
                    k: v
                    for k, v in self._future_metadata[x].items()
                    if v.iterate_into_recordset
                }
                for x, d in self._future_metadata.items()
            }
            self.recordset = rs
            self.recordset_type = rs_type
            yield self
        finally:
            self._future_metadata = old_futures
            self.recordset = old_rs
            self.recordset_type = old_rs_type

    @contextmanager
    def new_recordset(self, rs_type: str):
        rs = self.record.subrecords.new_recordset(rs_type)
        with self.recordset_context(rs, rs_type):
            for _, values in self._future_rs_metadata.items():
                for key, future in values.items():
                    if future.rs_types is not None and rs_type not in future.rs_types:
                        continue
                    self.set_recordset_metadata(
                        key,
                        future.element
                    )
            yield self

    @contextmanager
    def new_subrecord(self):
        record = ChildRecord()
        with self.record_context(record):
            yield self

    def get_quality(self,
                    *elements: SingleElement):
        def _qualities() -> t.Iterable[int | None]:
            if self.test_protocols:
                for element in elements:
                    for protocol_name in self.test_protocols:
                        yield from find_quality_for_protocol(element, protocol_name)
            else:
                for element in elements:
                    if "Quality" in element.metadata:
                        for qual in element.metadata["Quality"].all_values():
                            if qual.is_integer():
                                yield qual.to_int()
                            else:
                                yield None
                    if "WorkingQuality" in element.metadata:
                        for qual in element.metadata["WorkingQuality"].all_values():
                            if qual.is_integer():
                                yield qual.to_int()
                            else:
                                yield None
        return combine_quality_scores(_qualities())

    def set_element(self,
                    path: str,
                    element: AbstractElement | RawValue,
                    append_mode: bool = False):
        _, name = path.rsplit("/", maxsplit=1)
        forward = self.get_forward_metadata(name)
        if append_mode:
            self.record.append_to(path, element, **forward)
        else:
            self.record.set(path, element, **forward)

    def set_parent_element(self,
                    path: str,
                    element: AbstractElement | RawValue,
                    append_mode: bool = False):
        _, name = path.rsplit("/", maxsplit=1)
        forward = self.get_forward_metadata(name)
        if append_mode:
            self.parent.append_to(path, element, **forward)
        else:
            self.parent.set(path, element, **forward)

    def set_recordset_metadata(self,
                               metadata_name: str,
                               element: AbstractElement | RawValue,
                               append_mode: bool = False):
        forward = self.get_forward_metadata(metadata_name)
        if self.recordset is not None:
            if append_mode:
                self.recordset.metadata.append_to(metadata_name, element, **forward)
            else:
                self.recordset.metadata.set(metadata_name, element, **forward)

    def get_forward_metadata(self, property_name: str) -> dict[str, t.Any]:
        md = {}
        for _, d in self._future_metadata.items():
            for key, future in d.items():
                if future.names is not None and property_name not in future.names:
                    continue
                md[key] = future.element
        return md

    def add_common_recordset_metadata(self,
                                      metadata_name: str,
                                      element: AbstractElement,
                                      restrict_recordsets: list[str] | None = None,
                                      future_context: str | int | None = None,):
        if element.value is not None:
            if future_context not in self._future_rs_metadata:
                self._future_rs_metadata[future_context] = {}
            self._future_rs_metadata[future_context][metadata_name] = OPSContext.FutureMetadata(
                element, restrict_recordsets
            )
        else:
            if future_context in self._future_rs_metadata and metadata_name in self._future_rs_metadata[future_context]:
                del self._future_rs_metadata[future_context][metadata_name]

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
                       required_elements: RSElementList,
                       forbidden_elements: RSElementList,
                       helpful_elements: RSElementList,
                       no_repeats: bool = True) -> RecordSet | None:
        check_counts = set()
        if helpful_elements is not None:
            check_counts.update(helpful_elements)
        if required_elements is not None:
            for required_element in required_elements:
                if isinstance(required_element, str):
                    check_counts.add(required_element)
                else:
                    check_counts.update(required_element)
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
            if required_elements is not None:
                good = True
                for x in required_elements:
                    if isinstance(x, str):
                        if x not in rs_elements:
                            good = False
                            break
                    else:
                        if any(y not in rs_elements for y in x):
                            good = False
                            break
                if not good:
                    continue
            if check_counts:
                c = sum(1 if x in rs_elements else 0 for x in check_counts)
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
                         iterate_into_recordset: bool = False,
                         use_current_record: bool = True) -> t.Iterable[AbstractElement]:
        if use_current_record:
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
