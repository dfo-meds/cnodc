import medsutil.ocproc2 as ocproc2
from pipeman.programs.nodb.qc.qc import BaseTestSuite, TestContext, RecordTest, MetadataTest, QC_METADATA_KEYS
from autoinject import injector
import typing as t


class NODBIntegrityCheck(BaseTestSuite):

    ontology: ocproc2.OCProc2Ontology = None

    @injector.construct
    def __init__(self, strict_mode: bool = False, **kwargs):
        super().__init__(
            'nodb_integrity_check',
            '1.0',
            **kwargs
        )
        self._strict = strict_mode

    @MetadataTest('integrity_units', 'Units')
    def units_check(self, value: ocproc2.SingleElement, context: TestContext):
        if not value.is_empty():
            self.assert_valid_units(value.to_string(), 'integrity_invalid_units')

    @RecordTest('integrity_child_record_coordinates', record_mode=RecordTest.CHILD)
    def child_record_coordinate_check(self, record, context: TestContext):
        rs_info = self.ontology.recordset_info(t.cast(str, context.current_subrecord_type))
        if rs_info is not None:
            self.assert_true(any(record.coordinates.has_value(x) for x in rs_info.coordinates), "integrity_missing_subrecord_coordinate")

    @RecordTest('integrity_ontology')
    def ontology_check(self, record, context: TestContext):
        scope = "record:parent" if context.is_top_level() else "record:child"
        for key in record.metadata:
            with context.metadata_context(key) as ctx:
                self.should_test_value(record.metadata[key], ctx)
                self._verify_element(f"metadata:{scope}", key, record.metadata[key], ctx)
        for key in record.coordinates:
            with context.coordinate_context(key) as ctx:
                self.should_test_value(record.coordinates[key], ctx)
                self._verify_element("coordinates", key, record.coordinates[key], ctx)
        for key in record.parameters:
            with context.parameter_context(key) as ctx:
                self.should_test_value(record.parameters[key], ctx)
                self._verify_element("parameters", key, record.parameters[key], ctx)
        if self._strict:
            for srt in record.subrecords:
                self._verify_record_type(srt, context)
        for srs, srs_ctx in self.iterate_on_subrecord_sets(context):
            for key in srs.metadata:
                with srs_ctx.metadata_context(key) as ctx:
                    self.should_test_value(srs.metadata[key], ctx)
                    self._verify_element("metadata:recordset", key, srs.metadata[key], ctx)
                  
    def _verify_record_type(self, record_type: str, context: TestContext):
        with context.self_context():
            self.assert_true(self.ontology.recordset_exists(record_type), 'integrity_invalid_recordset_type')

    VALID_GROUP_NAMES = {
        "parameters": ("parameters",),
        "coordinates": ("coordinates",),
        "metadata:record:parent": ("metadata", "metadata:record", "metadata:parent", "metadata:platform", "metadata:mission", "metadata:product"),
        "metadata:record:child": ("metadata", "metadata:record", "metadata:child"),
        "metadata:element": ("metadata", "metadata:element"),
    }

    def _verify_element(self, element_group: str, element_name: str, element_value: ocproc2.AbstractElement, context: TestContext):
        # Never check the working quality, it can make infinite loops -  this is all set by the application anyways
        self.should_test_value(element_value, context)

        if element_name in QC_METADATA_KEYS:
            self.skip_test()

        # Check if the element is a defined parameter type in the scheme
        info = self.ontology.info(element_name)
        if info is None:
            if self._strict:
                self.report_for_review('integrity_undefined_element', -1)
            else:
                self.skip_test()

        # Check if the element is of an allowed group
        if info.group_name is not None:
            self.assert_in(info.group_name, NODBIntegrityCheck.VALID_GROUP_NAMES[element_group], 'integrity_invalid_group', -1)
        elif self._strict:
            self.report_for_review('integrity_no_allowed_groups', -1)

        # If the element is multi-valued, check if this is allowed
        if not info.allow_many:
            self.assert_not_multi(element_value, 'integrity_multi_not_allowed', -1)

        # Check all non-empty values against the preferred unit and data type
        self.test_all_subvalues(
            context,
            self._test_element_value,
            preferred_unit=self.ontology.preferred_unit(element_name),
            data_type=self.ontology.data_type(element_name),
            min_value=self.ontology.min_value(element_name),
            max_value=self.ontology.max_value(element_name),
            allowed_values=self.ontology.allowed_values(element_name)
        )

        # Go into the element metadata
        for key in element_value.metadata:
            with context.element_metadata_context(key) as ctx:
                self.should_test_value(element_value.metadata[key], ctx)
                self._verify_element("metadata:element", key, element_value.metadata[key], context)

    def _test_element_value(self,
                            value: ocproc2.SingleElement,
                            ctx: TestContext,
                            *,
                            preferred_unit: t.Optional[str] = None,
                            data_type: t.Optional[str] = None,
                            min_value: t.Optional[float] = None,
                            max_value: t.Optional[float] = None,
                            allowed_values: t.Optional[list] = None):
        if value.is_empty():
            return
        if data_type is None:
            pass
        elif data_type in ('dateTimeStamp', 'date'):
            self.assert_iso_datetime(value, 'integrity_invalid_datetime', -1)
        elif data_type == 'integer':
            self.assert_integer(value, 'integrity_invalid_integer', -1)
        elif data_type == 'decimal':
            self.assert_numeric(value, 'integrity_invalid_decimal', -1)
        elif data_type == 'string':
            self.assert_string_like(value, 'integrity_invalid_string', -1)
        elif data_type == 'List':
            self.assert_list_like(value, 'integrity_invalid_list', -1)
        elif data_type == "Element":
            self.assert_is(value, ocproc2.AbstractElement, "integrity_invalid_element", -1)
        if preferred_unit is not None:
            self.assert_compatible_units(value, preferred_unit, 'integrity_incompatible_units', skip_null=(not self._strict))
            value_in_units = self.value_in_units(value, preferred_unit)
            if value_in_units is not None and min_value is not None:
                self.assert_greater_than_or_close(
                    value_in_units,
                    min_value,
                    error_code='integrity_lower_than_range'
                )
            if value_in_units is not None and max_value is not None:
                self.assert_less_than_or_close(
                    value_in_units,
                    max_value,
                    error_code='integrity_greater_than_range'
                )
        if allowed_values:
            if data_type == "string":
                self.assert_in(value.to_string(), [str(x) for x in allowed_values], 'integrity_value_not_allowed', qc_flag=-1)
            elif data_type == "integer":
                self.assert_in(value.to_int(), [int(x) for x in allowed_values], 'integrity_value_not_allowed', qc_flag=-1)
            else:
                self.report_for_review("integrity_data_type_not_allowed", -1)
