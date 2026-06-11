from autoinject import injector

import medsutil.ocproc2 as ocproc2
from pipeman.programs.nodb.qc.qc import DeepDiveChecker, SingleElementRef, review, RecordSetRef, ChildRecordRef, \
    ElementRef, ElementType, MultiElementRef


class NODBIntegrityChecker(DeepDiveChecker):

    ontology: ocproc2.OCProc2Ontology = None

    VALID_GROUP_NAMES = {
        ElementType.PARAMETERS: ("parameters",),
        ElementType.COORDINATES: ("coordinates",),
        ElementType.PARENT_METADATA: ("metadata", "metadata:record", "metadata:parent", "metadata:platform", "metadata:mission", "metadata:product"),
        ElementType.CHILD_METADATA: ("metadata", "metadata:record", "metadata:child"),
        ElementType.ELEMENT_METADATA: ("metadata", "metadata:element"),
    }

    @injector.construct
    def __init__(self):
        super().__init__('nodb_integrity', '1.0')

    def single_element_check(self, ref: SingleElementRef):
        info = self.ontology.info(ref.element_name)
        if info is not None:
            self.element_data_type_check(ref, info.data_type)
            self.element_compatible_units_check(ref, info.preferred_unit)
            self.element_valid_range_check(ref, info.min_value, info.max_value, info.allowed_values, info.preferred_unit, info.data_type)

    @review("valid_data_type", error_flag=-1, skip_empty=True)
    def element_data_type_check(self, ref: SingleElementRef, data_type: str | None):
        if self.assert_is_not_none(data_type, msg="data type is not defined"):
            element = ref.element
            match data_type.lower():
                case 'datetimestamp':
                    self.assert_true(element.is_iso_datetime(), msg='invalid_datetime')
                case 'date':
                    self.assert_true(element.is_iso_datetime(), msg='invalid_date')
                case 'integer':
                    self.assert_true(element.is_integer(), msg='invalid_integer')
                case 'decimal':
                    self.assert_true(element.is_numeric(), msg='invalid_number')
                case 'string':
                    self.assert_true(element.is_string_like(), msg='invalid_string')
                case 'list':
                    self.assert_true(element.is_list_like(), msg='invalid_list')
                case _:
                    self.raise_qc_error("invalid_data_type")

    @review("compatible_units", error_flag=-1, skip_empty=True)
    def element_compatible_units_check(self, ref: SingleElementRef, preferred_units: str | None):
        if preferred_units is not None:
            element_units = ref.element.metadata.best("Units", coerce=str, default=None)
            if self.assert_is_not_none(element_units, msg="missing_units"):
                self.assert_true(self.converter.compatible(element_units, preferred_units), msg="incompatible_units")

    @review("valid_range", error_flag=-1, skip_empty=True)
    def element_valid_range_check(self, ref: SingleElementRef, min_value: float | None, max_value: float | None, allowed_values: list[str] | None, preferred_units: str | None, data_type: str | None):
        if min_value is not None or max_value is not None:
            value = ref.element.to_numeric(preferred_units)
            if min_value is not None:
                self.assert_greater_or_close(value, min_value, msg="value_too_small")
            if max_value is not None:
                self.assert_less_or_close(value, max_value, msg="value_too_large")
        if allowed_values is not None:
            if data_type == "string":
                self.assert_in(ref.element.to_string(), [str(x) for x in allowed_values], msg="value_not_allowed")
            elif data_type == "integer":
                self.assert_in(ref.element.to_int(), [int(x) for x in allowed_values], msg="value_not_allowed")
            else:
                self.raise_qc_error("invalid_data_type_for_allowed")

    @review("valid_recordset_type", error_flag=-1)
    def recordset_check(self, ref: RecordSetRef):
        self.assert_true(self.ontology.recordset_exists(ref.recordset_type), msg="invalid_recordset_type")

    @review("valid_subrecord_coordinates", error_flag=-1)
    def child_record_check(self, ref: ChildRecordRef):
        rs_info = self.ontology.recordset_info(ref.recordset_type)
        if rs_info is not None:
            self.assert_true(any(ref.record.coordinates.has_value(x) for x in rs_info.coordinates), msg="missing_recordset_coordinates")

    @review("valid_element", error_flag=-1)
    def element_check(self, ref: ElementRef):
        info = self.ontology.info(ref.element_name)
        if self.assert_is_not_none(info, msg="invalid_element_name"):
            self.assert_in(info.group_name, self.VALID_GROUP_NAMES[ref.element_type], msg="invalid_group")

    @review("multivalue_allowed", error_flag=-1)
    def multi_element_check(self, ref: MultiElementRef):
        info = self.ontology.info(ref.element_name)
        if info is not None:
            self.assert_true(info.allow_many, msg="multivalued_not_allowed")

