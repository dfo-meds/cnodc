import typing as t
from cnodc.qc.base import BaseTestSuite, TestContext, RecordSetTest
import cnodc.ocproc2.structures as ocproc2
import cnodc.ocean_math.umath_wrapper as umath


class GTSPPTemperatureInversionTest(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('gtspp_temp_inversion', '1_0', test_tags=['GTSPP_2.12'], **kwargs)
        self._maxima_threshold = 0.1
        self._minima_threshold = -0.1
        self._min_depth = 75
        self._min_temperature = 4
        self._depth_gap = 50

    @RecordSetTest("PROFILE")
    def temperature_inversion_test(self, recordset: ocproc2.RecordSet, context: TestContext):
        # Need at least four points
        if len(recordset.records) < 4:
            self.skip_test()
        ldt1 = None
        ldt2 = None
        ref = {'minima': [], 'maxima': []}
        for ldt in self._get_inversion_test_points(recordset):
            if ldt1 is not None and ldt2 is not None:
                if self._check_for_temperature_inversion(ldt1, ldt2, ldt, ref):
                    for i in range(ldt2[0], len(recordset.records)):
                        if 'Temperature' not in recordset.records[i].parameters:
                            continue
                        for av in recordset.records[i].parameters['Temperature'].all_values():
                            if self.precheck_value(av, raise_ex=False):
                                av.metadata['WorkingQuality'] = 13
                    self.report_for_review('temperature_inversion_detected')
                    break
            ldt1 = ldt2
            ldt2 = ldt

    def _check_for_temperature_inversion(self,
                                         ldt1: tuple[int, float, list[float]],
                                         ldt2: tuple[int, float, list[float]],
                                         ldt3: tuple[int, float, list[float]],
                                         ref: dict[str, list[float]]) -> bool:
        for idx in range(0, min(len(ldt1[2]), len(ldt2[2]), len(ldt3[2]))):
            t1 = ldt1[2][idx]
            t2 = ldt2[2][idx]
            t3 = ldt3[2][idx]
            if t1 is None or t2 is None or t3 is None:
                continue
            t13_avg = (t1 + t3) / 2.0
            diff = t2 - t13_avg
            if umath.is_greater_than(diff, self._maxima_threshold):
                ref['maxima'].append(ldt2[1])
                return ref['minima'] and any(umath.is_greater_than(x, ldt2[1]) and umath.is_less_than(x - ldt2[1], self._depth_gap) for x in ref['minima'])
            elif umath.is_less_than(diff, self._minima_threshold):
                ref['minima'].append(ldt2[1])
                return ref['maxima'] and any(umath.is_greater_than(x, ldt2[1]) and umath.is_less_than(x - ldt2[1], self._depth_gap) for x in ref['maxima'])
        return False

    def _get_inversion_test_points(self, recordset: ocproc2.RecordSet) -> t.Iterable[tuple[int, float, list[float]]]:
        for i in range(0, len(recordset.records)):
            record = recordset.records[i]
            depth = self.value_in_units(record.coordinates.get('Depth'), 'm')
            if depth is None or depth <= self._min_depth:
                continue
            temp_data = self.all_values_in_units(record.parameters.get('Temperature'), '°C', temp_scale='ITS-90')
            if (not temp_data) or all(x is None for x in temp_data):
                continue
            if any(temp < self._min_temperature for temp in temp_data if temp is not None):
                self.skip_test()
            yield i, depth, temp_data
