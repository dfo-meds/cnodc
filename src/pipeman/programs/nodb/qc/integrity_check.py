import medsutil.ocproc2 as ocproc2
from pipeman.programs.nodb.qc.qc import BaseTestSuite, TestContext, RecordTest, MetadataTest
from autoinject import injector
import typing as t


class NODBIntegrityCheck(BaseTestSuite):

    ontology: ocproc2.OCProc2Ontology = None

    @injector.construct
    def __init__(self, strict_mode: bool = False, **kwargs):
        super().__init__(
            'nodb_integrity_check',
            '1.0',
            test_tags=['GTSPP_1.2', 'GTSPP_1.3'],
            **kwargs
        )
        self._strict = strict_mode

    @MetadataTest('Units')
    def units_check(self, value: ocproc2.SingleElement, context: TestContext):
        if not value.is_empty():
            self.assert_valid_units(value.to_string(), 'units_invalid_unit_string')

    @RecordTest(record_mode=RecordTest.TOP)
    def latitude_check(self, record, context: TestContext):
        self.assert_has_coordinate(record, 'Latitude', 'lat_missing')

    @RecordTest(record_mode=RecordTest.TOP)
    def longitude_check(self, record, context: TestContext):
        self.assert_has_coordinate(record, 'Longitude', 'lon_missing')

    @RecordTest(record_mode=RecordTest.TOP)
    def time_check(self, record, context: TestContext):
        self.assert_has_coordinate(record, 'Time', 'time_missing')

    @RecordTest(record_mode=RecordTest.CHILD)
    def child_record_coordinate_check(self, record, context: TestContext):
        rs_info = self.ontology.recordset_info(t.cast(str, context.current_subrecord_type))
        if rs_info is not None:
            valid_coordinates = rs_info.coordinates
            self.assert_true(any(record.coordinates.has_value(x) for x in valid_coordinates.coordinates), "subrecord_coordinate_missing")

    @RecordTest()
    def ontology_check(self, record, context: TestContext):
        scope = "record:parent" if context.is_top_level() else "record:child"
        for key in record.metadata:
            with context.metadata_context(key) as ctx:
                self._verify_element(ctx, f"metadata:{scope}", key, record.metadata[key])
        for key in record.coordinates:
            with context.coordinate_context(key) as ctx:
                self._verify_element(ctx, "coordinates", key, record.coordinates[key])
        for key in record.parameters:
            with context.parameter_context(key) as ctx:
                self._verify_element(ctx, "parameters", key, record.parameters[key])
        if self._strict:
            for srt in record.subrecords:
                self._verify_record_type(context, srt)
        for srs, srs_ctx in self.iterate_on_subrecord_sets(context):
            for key in srs.metadata:
                with srs_ctx.metadata_context(key) as ctx:
                    self._verify_element(ctx, "metadata:recordset", key, srs.metadata[key])
                  
    def _verify_record_type(self, context: TestContext, record_type: str):
        with context.self_context():
            self.assert_true(self.ontology.recordset_exists(record_type), 'ontology_invalid_recordset_type')

    def _verify_element(self, context: TestContext, element_group: str, element_name: str, element_value: ocproc2.AbstractElement):
        # Never check the working quality, it can make infinite loops -  this is all set by the application anyways
        if element_name == 'WorkingQuality':
            self.skip_test()

        # Check if the element is a defined parameter type in the scheme
        info = self.ontology.info(element_name)
        if info is None:
            if self._strict:
                self.report_for_review('ontology_undefined_element', 20)
            else:
                self.skip_test()

        # Check if the element is of an allowed group
        if info.group_name is not None:
            self.assert_true(info.group_name.startswith(element_group), 'ontology_invalid_group', 20)
        elif self._strict:
            self.report_for_review('ontology_no_allowed_groups', 20)

        # If the element is multi-valued, check if this is allowed
        if not info.allow_many:
            self.assert_not_multi(element_value, 'ontology_multi_not_allowed')

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
            with context.element_metadata_context(key):
                self._verify_element(context, "metadata:element", key, element_value.metadata[key])

    def _test_element_value(self,
                            value: ocproc2.SingleElement,
                            ctx: TestContext,
                            *,
                            preferred_unit: t.Optional[str],
                            data_type: t.Optional[str],
                            min_value: t.Optional[float],
                            max_value: t.Optional[float],
                            allowed_values: t.Optional[list]):
        if value.is_empty():
            return
        if preferred_unit is not None:
            self.assert_compatible_units(value, preferred_unit, 'ontology_invalid_units', skip_null=(not self._strict))
            value_in_units = self.value_in_units(value, preferred_unit)
            if value_in_units is not None and min_value is not None:
                self.assert_greater_than_or_close(
                    value_in_units,
                    min_value,
                    error_code='ontology_lower_than_range'
                )
            if value_in_units is not None and max_value is not None:
                self.assert_less_than(
                    value_in_units,
                    max_value,
                    error_code='ontology_greater_than_range'
                )
        if data_type is None:
            pass
        elif data_type in ('dateTimeStamp', 'date'):
            self.assert_iso_datetime(value, 'ontology_invalid_datetime', 20)
        elif data_type == 'integer':
            self.assert_integer(value, 'ontology_invalid_integer', 20)
        elif data_type == 'decimal':
            self.assert_numeric(value, 'ontology_invalid_decimal', 20)
        elif data_type == 'string':
            self.assert_string_like(value, 'ontology_invalid_string', 20)
        elif data_type == 'List':
            self.assert_list_like(value, 'ontology_invalid_list', 20)


        if allowed_values:
            self.assert_in(value.value, allowed_values, 'ontology_value_not_allowed')
