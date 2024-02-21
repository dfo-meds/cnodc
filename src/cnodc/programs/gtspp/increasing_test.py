from cnodc.qc.base import BaseTestSuite, TestContext, RecordSetTest
import cnodc.ocproc2 as ocproc2


class GTSPPIncreasingProfileTest(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('gtspp_increasing', '1_0', test_tags=['GTSPP_2.3'], **kwargs)

    @RecordSetTest('PROFILE')
    def increasing_depth_test(self, ctx: TestContext):
        data_map = {
            'last_depth': [None, None],
            'last_pressure': [None, None]
        }
        self.test_all_records_in_recordset(ctx, self._increasing_depth_test, data_map=data_map)

    def _increasing_depth_test(self, record: ocproc2.ChildRecord, ctx: TestContext, data_map: dict):
        with ctx.coordinate_context('Depth'):
            self._check_depth(record, data_map)
        with ctx.coordinate_context('Pressure'):
            self._check_pressure(record, data_map)

    def _check_depth(self, record: ocproc2.ChildRecord, data_map: dict):
        self.precheck_value_in_map(record.coordinates, 'Depth')
        value = record.coordinates['Depth']
        if data_map['last_depth'][1] is None:
            data_map['last_depth'][1] = value.metadata.best_value('Units', 'm')
        current_depth = self.value_in_units(value, data_map['last_depth'][1])
        try:
            if data_map['last_depth'][0] is not None:
                self.assert_greater_than('non_decreasing_depth', data_map['last_depth'][0], current_depth)
        finally:
            data_map['last_depth'][0] = current_depth

    def _check_pressure(self, record: ocproc2.ChildRecord, data_map: dict):
        self.precheck_value_in_map(record.coordinates, 'Pressure')
        value = record.coordinates['Pressure']
        if data_map['last_pressure'][1] is None:
            data_map['last_pressure'][1] = value.metadata.best_value('Units', 'Pa')
        current_pressure = self.value_in_units(value, data_map['last_pressure'][1])
        try:
            if data_map['last_pressure'][0] is not None:
                self.assert_greater_than('non_decreasing_pressure', data_map['last_pressure'][0], current_pressure)
        finally:
            data_map['last_pressure'][0] = current_pressure
