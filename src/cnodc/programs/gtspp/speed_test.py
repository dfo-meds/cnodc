import datetime
import math

from cnodc.ocean_math.geodesy import uhaversine
import cnodc.ocproc2.structures as ocproc2
import typing as t
from uncertainties import ufloat
from cnodc.qc.base import BaseTestSuite, TestContext, RecordTest, QCSkipTest, QCAssertionError, BatchTest


class GTSPPSpeedTest(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('gtspp_speed_check', '1.0', test_tags=['GTSPP_1.5'], **kwargs)

    @BatchTest()
    def test_inter_record_speed(self, batch: dict[str, TestContext]):
        key_info = {}
        for identifier in batch:
            try:
                self.require_good_value(batch[identifier].top_record.metadata, 'CNODCStation', allow_dubious=False)
                self.require_good_value(batch[identifier].top_record.coordinates, 'Time', allow_dubious=False)
                self.require_good_value(batch[identifier].top_record.coordinates, 'Latitude', allow_dubious=False)
                self.require_good_value(batch[identifier].top_record.coordinates, 'Longitude', allow_dubious=False)
                xx = batch[identifier].top_record.coordinates['Longitude'].to_float_with_uncertainty()
                yy = batch[identifier].top_record.coordinates['Latitude'].to_float_with_uncertainty()
                tt = datetime.datetime.fromisoformat(batch[identifier].top_record.coordinates['Time'].to_datetime())
                tu = batch[identifier].top_record.coordinates['Time'].metadata.best_value('Uncertainty', 0)
                sid = batch[identifier].top_record.metadata['CNODCStation'].to_string()
                if sid not in key_info:
                    key_info[sid] = []
                key_info[sid].append((xx, yy, tt, tu, identifier))
            except QCSkipTest:
                continue
        for key in key_info:
            self._process_speed_subbatch(key, key_info[key], batch)

    def _process_speed_subbatch(self,
                                station_uuid: str,
                                records: list[tuple[t.Union[float, ufloat], t.Union[float, ufloat], datetime.datetime, t.Union[float, int], str]],
                                batch: dict[str, TestContext]):
        # Ignore 1 record batches
        if len(records) < 2:
            return
        station = self._load_station(station_uuid)
        top_speed = 40
        if station is not None:
            if station.get_metadata('skip_speed_check', False):
                return
            top_speed = station.get_metadata('top_speed', top_speed)
        if isinstance(top_speed, str) and ' ' in top_speed:
            p = top_speed.find(' ')
            top_speed = self.converter.convert(float(top_speed[:p]), top_speed[p:].strip(), 'm s-1')
        else:
            top_speed = float(top_speed)
        # Sort by time ascending
        records.sort(key=lambda x: x[2])
        for i in range(1, len(records)):
            distance = uhaversine((records[i-1][1], records[i-1][0]), (records[i][1], records[i][0]))
            time = (records[i][2] - records[i-1][2]).total_seconds()
            time_uncertainty = math.sqrt((records[i][3] ** 2) + records[i-1][3] ** 2)
            speed = distance / ufloat(time, time_uncertainty)
            if self.is_greater_than(speed, top_speed):
                batch[records[i][4]].report_for_review('speed_too_fast')
                batch[records[i][4]].top_record.coordinates['Latitude'].metadata['WorkingQuality'] = 13
                batch[records[i][4]].top_record.coordinates['Longitude'].metadata['WorkingQuality'] = 13
