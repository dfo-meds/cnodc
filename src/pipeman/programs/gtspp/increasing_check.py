from pipeman.programs.qc.base import DeepDiveChecker, RecordSetRef, ChildRecordRef, SingleElementRef, review
import medsutil.ocproc2 as ocproc2
import medsutil.math as amath

class GTSPPIncreasingProfileTest(DeepDiveChecker):

    LIMIT_SUBRECORD_TYPES = ("PROFILE",)

    def __init__(self, aggressive_mode: bool = False):
        super().__init__(
            test_name='gtspp_increasing',
            test_version='1.0',
            test_tags=['GTSPP_2.3']
        )
        self._aggressive_mode = aggressive_mode

    def recordset_check(self, ref: RecordSetRef):
        if ref.recordset_type != "PROFILE":
            return
        last_coordinates: dict[str, amath.AnyNumber | None] = {
            'Depth': None,
            'Pressure': None,
        }
        coordinate_units: dict[str, str | None] = {
            'Depth': None,
            'Pressure': None,
        }
        for record_ref in self.iterate_on_recordset_records(ref):
            self.check_increasing(record_ref, last_coordinates, coordinate_units)

    def check_increasing(self, ref: ChildRecordRef, last_coordinates: dict[str, amath.AnyNumber | None], coordinate_units: dict[str, str | None]):
       for coordinate_name in list(last_coordinates.keys()):
            element_ref = self.get_record_coordinate_ref(ref, coordinate_name)
            if element_ref is not None:
                last_value = last_coordinates[coordinate_name]
                current_values: list[amath.AnyNumber] = []
                for element_sref in self.iterate_on_single_elements(element_ref):
                    if coordinate_units[coordinate_name] is None:
                        coordinate_units[coordinate_name] = element_sref.element.metadata.best("Units", coerce=str)
                    self._check_increasing(element_sref, current_values, last_value, coordinate_units[coordinate_name] or "")
                if current_values:
                    current_best_value = max(current_values) if self._aggressive_mode else min(current_values)
                    if last_value is None or current_best_value > last_value:
                        last_coordinates[coordinate_name] = current_best_value

    def _check_increasing(self,
                          ref: SingleElementRef,
                          current_values: list[amath.AnyNumber],
                          last_value: amath.AnyNumber | None,
                          last_value_units: str):
        with self.review("is_deeper", ref, fail_flag=4, pass_flag=1) as ctx:
            self.require_value(ref.element)
            depth_value = ref.element.to_numeric(last_value_units)
            current_values.append(depth_value)
            if last_value is not None:
                ctx.check_review_already_complete()
                self.assert_greater_or_close(depth_value, last_value, msg="not_deeper")
