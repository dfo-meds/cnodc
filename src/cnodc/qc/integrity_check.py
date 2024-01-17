from cnodc.ocproc2.structures import AbstractValue, MultiValue
from cnodc.ocproc2.validation import OCProc2Ontology
from cnodc.qc.base import BaseTestSuite, TestContext, RecordTest, MetadataTest
from autoinject import injector


class NODBIntegrityCheck(BaseTestSuite):

    ontology: OCProc2Ontology = None

    @injector.construct
    def __init__(self, strict_mode: bool = False, **kwargs):
        super().__init__('nodb_integrity_check', '1.0', **kwargs)
        self._strict = strict_mode

    @MetadataTest('Units')
    def units_check(self, value: AbstractValue, context: TestContext):
        for v in value.all_values():
            if v.is_empty():
                continue
            elif not self.converter.is_valid_unit(v.value):
                context.report_for_review('units_invalid_unit_string')
                v.metadata['WorkingQuality'] = 21

    @RecordTest(top_only=True)
    def coordinate_check(self, record, context: TestContext):
        self.assert_has_coordinate(context, 'Latitude', 'lat_missing')
        self.assert_has_coordinate(context, 'Longitude', 'lon_missing')
        self.assert_has_coordinate(context, 'Time', 'time_missing')

    @RecordTest(subrecord_type='PROFILE')
    def profile_depth_check(self, record, context: TestContext):
        # TODO: use the ontology for this?
        if 'Pressure' in record.coordinates and not record.coordinates['Pressure'].is_empty():
            return
        if 'Depth' in record.coordinates and not record.coordinates['Depth'].is_empty():
            return
        context.report_for_review('level_missing')

    @RecordTest(subrecord_type='SPEC_WAVE')
    def spectral_wave_frequency_check(self, record, context: TestContext):
        if 'CentralFrequency' in record.coordinates and not record.coordinates['CentralFrequency'].is_empty():
            return
        context.report_for_review('frequency_missing')

    @RecordTest(subrecord_type='WAVE_SENSORS')
    def wave_sensor_check(self, record, context: TestContext):
        if 'WaveSensor' in record.coordinates and not record.coordinates['WaveSensor'].is_empty():
            return
        context.report_for_review('wave_sensor_missing')

    @RecordTest(subrecord_type='TSERIES')
    def time_series_coordinate_check(self, record, context: TestContext):
        if 'TimeOffset' in record.coordinates and not record.coordinates['TimeOffset'].is_empty():
            return
        if 'Time' in record.coordinates and not record.coordinates['Time'].is_empty():
            return
        context.report_for_review('time_missing')

    @RecordTest()
    def ontology_check(self, record, context: TestContext):
        scope = "record:parent" if context.is_top_level() else "record:child"
        original_path = context.current_path
        for key in context.current_record.metadata:
            context.current_path = [*original_path, f'metadata/{key}']
            self._verify_element(context, f"metadata:{scope}", key, context.current_record.metadata[key])
        for key in context.current_record.coordinates:
            context.current_path = [*original_path, f'coordinates/{key}']
            self._verify_element(context, "coordinates", key, context.current_record.coordinates[key])
        for key in context.current_record.parameters:
            context.current_path = [*original_path, f'parameters/{key}']
            self._verify_element(context, "parameters", key, context.current_record.parameters[key])
        for record_type in context.current_record.subrecords:
            context.current_path = [*original_path, record_type]
            self._verify_record_type(context, record_type)
            for recordset_idx in context.current_record.subrecords[record_type]:
                context.current_path = [*original_path, f"{record_type}/{recordset_idx}"]
                for key in context.current_record.subrecords[record_type][recordset_idx].metadata:
                    context.current_path = [*original_path, f"{record_type}/{recordset_idx}", f"metadata/{key}"]
                    self._verify_element(context, "metadata:recordset", key, context.current_record.subrecords[record_type][recordset_idx].metadata[key])
                  
    def _verify_record_type(self, context: TestContext, record_type: str):
        if self._strict:
            if not self.ontology.is_defined_recordset_type(record_type):
                context.report_for_review('ontology_invalid_recordset_type')

    def _verify_element(self, context: TestContext, element_group: str, element_name: str, element_value: AbstractValue):
        # Check if the element is a defined parameter type in the scheme
        if not self.ontology.is_defined_parameter(element_name):
            if self._strict:
                context.report_for_review('ontology_undefined_element')
            return

        # Check if the element is of an allowed group
        allowed_groups = self.ontology.element_group(element_name)
        if allowed_groups:
            if not any(element_group.startswith(x) for x in allowed_groups):
                context.report_for_review('ontology_invalid_group')
                element_value.metadata['WorkingQuality'] = 20
        elif self._strict:
            context.report_for_review('ontology_no_allowed_groups')
            element_value.metadata['WorkingQuality'] = 20

        # If the element is multi-valued, check if this is allowed
        if isinstance(element_value, MultiValue) and not self.ontology.allow_multiple_values(element_name):
            context.report_for_review('ontology_multi_not_allowed')
            element_value.metadata['WorkingQuality'] = 20

        # Check all non-empty values against the preferred unit and data type
        preferred_unit = self.ontology.preferred_unit(element_name)
        data_type = self.ontology.data_type(element_name)
        min_value = self.ontology.min_value(element_name)
        max_value = self.ontology.max_value(element_name)
        allowed_values = self.ontology.allowed_values(element_name)
        if preferred_unit is not None or data_type is not None or min_value is not None or max_value is not None or allowed_values:
            for value in element_value.all_values():
                if value.is_empty():
                    continue
                if preferred_unit is not None:
                    if value.metadata.has_value('Units'):
                        if not self.converter.compatible(preferred_unit, value.metadata.best_value('Units')):
                            context.report_for_review('ontology_incompatible_units')
                            value.metadata['WorkingQuality'] = 21
                    elif self._strict:
                        context.report_for_review('ontology_missing_units')
                        value.metadata['WorkingQuality'] = 21
                if data_type is None:
                    pass
                elif data_type in ('dateTimeStamp', 'date') and not value.is_iso_datetime():
                    context.report_for_review('ontology_invalid_datetime')
                    value.metadata['WorkingQuality'] = 20
                elif data_type == 'integer' and not value.is_integer():
                    context.report_for_review('ontology_invalid_integer')
                    value.metadata['WorkingQuality'] = 20
                elif data_type == 'decimal' and not value.is_numeric():
                    context.report_for_review('ontology_invalid_decimal')
                    value.metadata['WorkingQuality'] = 20
                elif data_type == 'string' and isinstance(value.value, (dict, list, tuple, set)):
                    context.report_for_review('ontology_invalid_string')
                    value.metadata['WorkingQuality'] = 20
                elif data_type == 'List' and not isinstance(value.value, (list, tuple, set)):
                    context.report_for_review('ontology_invalid_list')
                    value.metadata['WorkingQuality'] = 20
                if value.is_numeric() and not value.in_range(min_value, max_value):
                    context.report_for_review('ontology_out_of_range', ref_value=[min_value, max_value])
                    value.metadata['WorkingQuality'] = 14
                elif allowed_values:
                    if value.value not in allowed_values:
                        context.report_for_review('ontology_value_not_allowed', ref_value=allowed_values)
                    value.metadata['WorkingQuality'] = 14
        # Loop into the element metadata
        original_path = context.current_path
        for key in element_value.metadata:
            # Never check these just so we avoid an infinite loop
            if key == 'WorkingQuality':
                continue
            context.current_path = [*original_path, f'metadata#{key}']
            self._verify_element(context, "metadata:element", key, element_value.metadata[key])
