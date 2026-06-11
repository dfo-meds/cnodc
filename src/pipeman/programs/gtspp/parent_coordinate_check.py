from pipeman.programs.qc import ElementRef, review, DeepDiveChecker, ParentRecordRef


class GTSPPCoordinateCheck(DeepDiveChecker):

    def __init__(self):
        super().__init__(
            'gtspp_coordinates',
            '1.0',
            test_tags=['GTSPP_1.2', 'GTSPP_1.3']
        )

    def parent_record_check(self, ref: ParentRecordRef):
        self.require_element_check(self.get_record_coordinate_ref(ref, "Latitude", True))
        self.require_element_check(self.get_record_coordinate_ref(ref, "Longitude", True))
        self.require_element_check(self.get_record_coordinate_ref(ref, "Time", True))

    @review("element_required", error_flag=9)
    def require_element_check(self, ref: ElementRef):
        self.assert_false(ref.element.is_empty(), msg="element_required")
