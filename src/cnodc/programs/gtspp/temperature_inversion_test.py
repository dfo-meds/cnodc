import typing as t
from cnodc.qc.base import BaseTestSuite, TestContext, ProfileTest, SubRecordArray, ProfileLevelTest


class GTSPPTemperatureInversionTest(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('gtspp_temp_inversion', '1_0', test_tags=['GTSPP_2.12'], **kwargs)

    @ProfileTest()
    def temperature_inversion_test(self, profile: SubRecordArray, context: TestContext):
        # Need at least four points
        if profile.length < 4:
            self.skip_test()
        # Need temperature
        if 'Temperature' not in profile.data:
            self.skip_test()
        # Need depth
        if 'Depth' not in profile.data:
            self.skip_test()
        ldt1 = None
        ldt2 = None
        ref = {'minima': [], 'maxima': []}
        for ldt in self._get_inversion_test_points(profile):
            if ldt1 is not None and ldt2 is not None:
                if self._check_for_temperature_inversion(ldt1, ldt2, ldt, ref):
                    for i in range(ldt2[0], profile.length):
                        if profile.data['Temperature'][i] is None:
                            continue
                        for av in profile.data['Temperature'][i].all_values():
                            if av.metadata['WorkingQuality'] in (3, 4, 9, 13, 14, 19):
                                continue
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
            if self.is_greater_than(diff, 0.1):
                ref['maxima'].append(ldt2[1])
                return ref['minima'] and any(self.is_greater_than(x, ldt2[1]) and self.is_less_than(x - ldt2[1], 50) for x in ref['minima'])
            elif self.is_less_than(diff, -0.1):
                ref['minima'].append(ldt2[1])
                return ref['maxima'] and any(self.is_greater_than(x, ldt2[1]) and self.is_less_than(x - ldt2[1], 50) for x in ref['maxima'])
        return False

    def _get_inversion_test_points(self, profile: SubRecordArray) -> t.Iterable[tuple[int, float, list[float]]]:
        for i in range(0, profile.length):
            if not profile.has_good_value('Depth', i):
                continue
            depth = self.value_in_units(profile.data['Depth'][i], 'm')
            if depth <= 75:
                continue
            if not profile.has_good_value('Temperature', i):
                continue
            temp_data = [
                self.value_in_units(v, '°C', temp_scale='ITS-90')
                for v in profile.data['Temperature'][i].all_values()
            ]
            if not temp_data or all(x is None for x in temp_data):
                continue
            if any(temp < 4 for temp in temp_data if temp is not None):
                self.skip_test()
            yield i, depth, temp_data
