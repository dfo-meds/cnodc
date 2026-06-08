from pipeman.programs.nodb.qc.qc import BaseTestSuite, TestContext, RecordTest
from autoinject import injector

class GTSPPParentCoordinateCheck(BaseTestSuite):

    @injector.construct
    def __init__(self, strict_mode: bool = False, **kwargs):
        super().__init__(
            'nodb_integrity_check',
            '1.0',
            test_tags=['GTSPP_1.2', 'GTSPP_1.3'],
            **kwargs
        )
        self._strict = strict_mode

    @RecordTest('gtspp_require_latitude', record_mode=RecordTest.TOP)
    def latitude_check(self, record, context: TestContext):
        self.assert_has_coordinate(record, 'Latitude', 'lat_missing')

    @RecordTest('gtspp_require_longitude', record_mode=RecordTest.TOP)
    def longitude_check(self, record, context: TestContext):
        self.assert_has_coordinate(record, 'Longitude', 'lon_missing')

    @RecordTest('gtspp_require_time', record_mode=RecordTest.TOP)
    def time_check(self, record, context: TestContext):
        self.assert_has_coordinate(record, 'Time', 'time_missing')

