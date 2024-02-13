import datetime
import math

from cnodc.ocean_math.geodesy import uhaversine
import cnodc.ocproc2.structures as ocproc2
import typing as t
from uncertainties import ufloat
from cnodc.qc.base import BaseTestSuite, TestContext, RecordTest, QCSkipTest, QCAssertionError, BatchTest


class GTSPPSpeedTest(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__(
            'gtspp_speed_check',
            '1.0',
            test_tags=['GTSPP_1.5'],
            working_sort_by='obs_time_asc',
            **kwargs
        )

    @RecordTest(top_only=True)
    def test_inter_record_speed(self, record: ocproc2.DataRecord, context: TestContext):
        self.precheck_value_in_map(record.metadata, 'CNODCStation', allow_dubious=False)
        self.precheck_value_in_map(record.coordinates, 'Time', allow_dubious=False)
        self.precheck_value_in_map(record.coordinates, 'Latitude', allow_dubious=False)
        self.precheck_value_in_map(record.coordinates, 'Longitude', allow_dubious=False)
        xx = record.coordinates['Longitude'].to_float_with_uncertainty()
        yy = record.coordinates['Latitude'].to_float_with_uncertainty()
        tt = record.coordinates['Time'].to_datetime()
        sid = record.metadata['CNODCStation'].to_string()
        info = (xx, yy, tt)
        if 'previous_positions' not in context.batch_context:
            context.batch_context['previous_positions'] = {}
        if 'top_speeds' not in context.batch_context:
            context.batch_context['top_speeds'] = {}
        try:
            if sid in context.batch_context['previous_positions']:
                if sid not in context.batch_context['top_speeds']:
                    context.batch_context['top_speeds'][sid] = self._get_top_speed(sid)
                self._run_speed_test(
                    info,
                    context.batch_context['previous_positions'][sid],
                    context.batch_context['top_speeds'][sid],
                    context
                )
        finally:
            context.batch_context['previous_positions'][sid] = info

    def _get_top_speed(self, station_id: str) -> t.Optional[float]:
        top_speed = 40
        station = self._load_station(station_id)
        if station is not None:
            if station.get_metadata('skip_speed_check', False):
                return None
            top_speed = station.get_metadata('top_speed', top_speed)
        if isinstance(top_speed, str) and ' ' in top_speed:
            p = top_speed.find(' ')
            return float(self.converter.convert(float(top_speed[:p]), top_speed[p:].strip(), 'm s-1'))
        return float(top_speed)

    def _run_speed_test(self, xyt2: tuple, xyt1: tuple, top_speed, context):
        if top_speed is None:
            return
        distance = uhaversine((xyt2[1], xyt2[0]), (xyt1[1], xyt1[0]))
        time = (xyt2[2] - xyt1[2]).total_seconds()
        with context.two_coordinate_context('Latitude', 'Longitude') as ctx2:
            self.assert_greater_than('speed_too_fast', distance / time, top_speed, qc_flag=13)
