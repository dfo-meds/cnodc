import medsutil.ocproc2 as ocproc2
from pipeman.programs.qc.base import review, DeepDiveChecker
from medsutil.ocproc2.refs import ElementRef, ParentRecordRef


class GTSPPCoordinateCheck(DeepDiveChecker):

    def __init__(self):
        super().__init__(
            'gtspp_coordinates',
            '1.0',
            test_tags=['GTSPP_1.2', 'GTSPP_1.3']
        )

    def parent_record_check(self, ref: ParentRecordRef):
        self.require_element_check(ref.setdefault_coordinate_ref("Latitude"))
        self.require_element_check(ref.setdefault_coordinate_ref("Longitude"))
        self.require_element_check(ref.setdefault_coordinate_ref("Time"))

    @review("element_required", fail_flag=9, pass_flag=1)
    def require_element_check(self, ref: ElementRef):
        self.element_require_value(ref.element)

    def element_require_value(self, element: ocproc2.AbstractElement):
        self.assert_false(element.is_empty(), msg="element_required")
