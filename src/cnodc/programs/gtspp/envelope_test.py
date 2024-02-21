import itertools
import pathlib
import typing as t

import yaml
from uncertainties import UFloat
from cnodc.qc.base import BaseTestSuite, RecordTest, TestContext, ReferenceRange
import cnodc.ocproc2 as ocproc2
from cnodc.units import UnitConverter
from cnodc.ocean_math.seawater import eos80_pressure, eos80_freezing_point_t90, eos80_density_at_depth_t90


class EnvelopeReference:

    def __init__(self, file: t.Union[str, pathlib.Path], converter: UnitConverter):
        self._envelopes: list[dict[str, ReferenceRange]] = []
        self._converter = converter
        with open(file, 'r') as h:
            envelope_entries = yaml.safe_load(h) or []
            for entry in envelope_entries:
                self._envelopes.append(ReferenceRange.from_map_of_maps(entry))

    def find_level(self, depth: t.Optional[float], depth_units: str = "m") -> dict:
        if depth is None:
            return {}
        for level in self._envelopes:
            if 'Depth' not in level:
                continue
            envelope_depth_units = level['Depth'].units or 'm'
            test_against = float(self._converter.convert(depth, depth_units, envelope_depth_units))
            if level['Depth'].minimum < test_against <= level['Depth'].maximum:
                return {x: level[x] for x in level if x not in ('Depth', 'Pressure')}
        return {}


class GTSPPEnvelopeTest(BaseTestSuite):

    def __init__(self,
                 envelope_file: t.Union[str, pathlib.Path],
                 **kwargs):
        super().__init__('gtspp_envelope', '1_0', test_tags=['GTSPP_2.4'], **kwargs)
        self._envelope_ref = EnvelopeReference(envelope_file, self.converter)

    @RecordTest("PROFILE")
    def envelope_test(self, record: ocproc2.ChildRecord, context: TestContext):
        self.precheck_value_in_map(record.coordinates, 'Depth')
        references = self._envelope_ref.find_level(
            record.coordinates['Depth'].to_float(),
            record.coordinates['Depth'].metadata.best_value('Units')
        )
        self._test_and_loop(record, context, qc_flag=13, error_code='outside_envelope_range', references=references)

    def _test_and_loop(self, record, context: TestContext, **kwargs):
        self.test_all_references_in_record(context, **kwargs)
        self.test_all_subrecords(context, self._test_and_loop, **kwargs)
