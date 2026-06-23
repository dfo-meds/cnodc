from autoinject import injector

import medsutil.ocproc2 as ocproc2
from medsutil.ocproc2.util import RequiredQuality, Quality
from pipeman.programs.qc.base import DeepDiveChecker, review
from medsutil.ocproc2.refs import ElementType, ElementRef, SingleElementRef, MultiElementRef, RecordSetRef, \
    ChildRecordRef


class NODBIntegrityChecker(DeepDiveChecker):

    ontology: ocproc2.OCProc2Ontology = None

    VALID_GROUP_NAMES = {
        ElementType.PARAMETERS: ("parameters",),
        ElementType.COORDINATES: ("coordinates",),
        ElementType.PARENT_METADATA: ("metadata", "metadata:record", "metadata:parent", "metadata:platform", "metadata:mission", "metadata:product"),
        ElementType.CHILD_METADATA: ("metadata", "metadata:record", "metadata:child"),
        ElementType.ELEMENT_METADATA: ("metadata", "metadata:element"),
        ElementType.RECORDSET_METADATA: ("metadata", "metadata:recordset"),
    }

    @injector.construct
    def __init__(self):
        super().__init__(test_name='nodb_integrity', test_version='1.0')

    def single_element_check(self, ref: SingleElementRef):
        info = self.ontology.info(ref.element_name)
        if info is not None:
            with self.review("valid_data_type", ref, fail_flag=Quality.BAD_STRUCTURE) as ctx:
                ctx.check_review_already_complete(RequiredQuality.QC_INCOMPLETE | RequiredQuality.HAS_VALUE)
                self.element_data_type_check(ref.element, info.data_type)
            if info.preferred_unit is not None:
                with self.review("compatible_units", ref, fail_flag=Quality.BAD_STRUCTURE) as ctx:
                    ctx.check_review_already_complete(RequiredQuality.QC_INCOMPLETE | RequiredQuality.HAS_VALUE)
                    self.element_compatible_units_check(ref.element, info.preferred_unit)
            with self.review("valid_range", ref, fail_flag=Quality.BAD_STRUCTURE) as ctx:
                ctx.check_review_already_complete(RequiredQuality.QC_INCOMPLETE | RequiredQuality.HAS_VALUE)
                self.element_valid_range_check(ref.element, info.min_value, info.max_value, info.allowed_values, info.preferred_unit, info.data_type)

    def element_data_type_check(self, element: ocproc2.SingleElement, data_type: str | None):
        if self.assert_is_not_none(data_type, msg="missing_data_type"):
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
                case 'duration':
                    self.assert_true(element.is_duration(), msg='invalid_duration')
                case _:
                    self.report_qc_error("invalid_data_type")

    def element_compatible_units_check(self, element: ocproc2.SingleElement, preferred_units: str):
        element_units = element.metadata.best("Units", coerce=str, default=None)
        if self.assert_is_not_none(element_units, msg="missing_units"):
            self.assert_true(self.converter.compatible(element_units, preferred_units), msg="incompatible_units")

    def element_valid_range_check(self, element: ocproc2.SingleElement, min_value: float | None, max_value: float | None, allowed_values: list[str] | None, preferred_units: str | None, data_type: str | None):
        if min_value is not None or max_value is not None:
            value = element.to_numeric(preferred_units)
            if min_value is not None:
                self.assert_greater_or_close(value, min_value, msg="value_too_small")
            if max_value is not None:
                self.assert_less_or_close(value, max_value, msg="value_too_large")
        if allowed_values is not None:
            if data_type == "string":
                self.assert_in(element.to_string(), [str(x) for x in allowed_values], msg="value_not_allowed")
            elif data_type == "integer":
                self.assert_in(element.to_int(), [int(x) for x in allowed_values], msg="value_not_allowed")
            else:
                self.report_qc_error("invalid_data_type_for_allowed")

    @review("valid_recordset_type", fail_flag=Quality.BAD_STRUCTURE)
    def recordset_check(self, ref: RecordSetRef):
        self.recordset_valid_type_check(ref.recordset_type)

    def recordset_valid_type_check(self, recordset_type: str):
        self.assert_true(self.ontology.recordset_exists(recordset_type), msg="invalid_recordset_type")

    @review("valid_subrecord_coordinates", fail_flag=Quality.BAD_STRUCTURE)
    def child_record_check(self, ref: ChildRecordRef):
        self.record_valid_coordinates_for_rs_type_check(ref.record, ref.recordset_type)

    def record_valid_coordinates_for_rs_type_check(self, record: ocproc2.BaseRecord, recordset_type: str):
        rs_info = self.ontology.recordset_info(recordset_type)
        if rs_info is not None:
            for sub_coordinate_group in rs_info.coordinates:
                self.assert_true(any(record.coordinates.has_value(x) for x in sub_coordinate_group), msg="missing_recordset_coordinates")

    @review("valid_element", fail_flag=Quality.BAD_STRUCTURE)
    def element_check(self, ref: ElementRef):
        self.element_exists_and_proper_group_check(ref.element_name, ref.element_type)

    def element_exists_and_proper_group_check(self, element_name: str, element_type: ElementType):
        info = self.ontology.info(element_name)
        if self.assert_is_not_none(info, msg="invalid_element_name"):
            self.assert_in(info.group_name, self.VALID_GROUP_NAMES[element_type], msg="invalid_group")

    @review("multivalue_allowed", fail_flag=Quality.BAD_STRUCTURE)
    def multi_element_check(self, ref: MultiElementRef):
        self.multi_element_verify_allowed_and_present(ref.element, ref.element_name)

    def multi_element_verify_allowed_and_present(self, element: ocproc2.MultiElement, element_name: str):
        info = self.ontology.info(element_name)
        if info is not None:
            self.assert_true(info.allow_many, msg="multivalued_not_allowed")
            for x in element.values():
                self.assert_is_instance(x, ocproc2.AbstractElement, msg="multivalue_must_contain_values")
            self.assert_true(len(element.value) > 1, msg="multivalue_must_contain_at_least_two_values")
