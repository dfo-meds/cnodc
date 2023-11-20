import datetime

import sys
import pathlib
import random
import time
import statistics

sys.path.append(str(pathlib.Path(__file__).absolute().parent.parent / "src"))

from cnodc.ocproc2 import DataRecord, DataValue, RecordSet
from cnodc.decode.ocproc2_bin import OCProc2BinaryCodec

codec = OCProc2BinaryCodec()

ref_coords = [
    (45.012, -46.423),
    (40.143, -47.924),
    (45.015, -49.534),
    (45.234, -135.231),
    (54.934, -52.342),
    (40.231, -47.892),
    (-32.123, 179.023),
    (41.231, -48.923),
    (52.312, -48.234),
    (42.124, -49.455),
]

z_set = [
    69000, 79000, 89000, 99000,
    *[53000 + (100000 * x) for x in range(1, 24)],
    2628000, 2878000, 3130000, 3378000, 3626000, 3878000, 4130000, 4379000, 4635000, 4875000,
    5126000, 5376000, 5625000, 5879000, 6126000, 6375000, 6590000
]

z_set2 = [
    10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
    125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400, 425, 450, 475, 500,
    550, 600, 650, 700, 750, 800, 850, 900, 950, 1000, 1050, 1100, 1150, 1200, 1250, 1300,
    1400, 1500, 1600, 1700, 1800, 1900, 2000,
    2100, 2200, 2300, 2400, 2500, 2600, 2700, 2800, 2900, 3000
]

z_set3 = [x - 1 for x in z_set2]

z_set4 = [x + 2 for x in z_set2]


def fuzz(value, fuzz_scale, precision):
    return round(value + ((random.randint(0, 1000) / 1000) * fuzz_scale) - (fuzz_scale / 2), precision)


def _315007_record(idx, dt):
    dr = DataRecord()
    dr.coordinates['LAT'] = DataValue(
        reported_value=fuzz(ref_coords[idx][0], 0.1, 3),
        metadata={
            'PR': 0.001,
            'UN': 'degrees',
            'QC': 0
        }
    )
    dr.coordinates['LON'] = DataValue(
        reported_value=fuzz(ref_coords[idx][1], 0.1, 3),
        metadata={
            'PR': 0.001,
            'UN': 'degrees',
            'QF': 0
        }
    )
    dr.coordinates['TIME'] = DataValue(
        reported_value=dt
    )
    dr.metadata['STATION_ID'] = f'0000{idx}'
    dr.metadata['PLATFORM_DIR'] = DataValue(fuzz(150, 90, -1), metadata={'UN': 'degrees', 'PR': 10, 'QF': 0})
    dr.metadata['PLATFORM_SPD'] = DataValue(fuzz(2.3, 2, 1), metadata={'UN': 'm s-1', 'PR': 0.1, 'QF': 0})
    dr.metadata['STATION_NAME'] = f'Stuff In Places {idx}'
    dr.metadata['IMO_NUMBER'] = 12345678 + idx
    dr.metadata['WMO_ID'] = f'0000{idx}'
    dr.metadata['WMO_AGENCY_CODE'] = 'CAN'
    dr.metadata['CRUISE_ID'] = f'12345678{idx}'
    dr.metadata['SHIP_LINE_NO'] = 12345 + idx
    dr.metadata['SHIP_TRANSECT_NO'] = 12345 + idx
    dr.metadata['PROFILE_ID'] = random.randint(1, 10000000)
    dr.metadata['SEQUENCE_NO'] = random.randint(1, 100000000)
    dr.variables['SEA_FLOOR_DEPTH'] = DataValue(fuzz(1500, 100, 0), metadata={'UN': 'm', 'PR': 1, 'QF': 0})
    dr.variables['ATMO_PRESSURE'] = DataValue(fuzz(128000, 5000, -3), metadata={'UN': 'Pa', 'PR': 1000, 'QF': 0})
    dr.variables['ATMO_PRESSURE_MSL'] = DataValue(fuzz(131000, 5000, -3), metadata={'UN': 'Pa', 'PR': 1000, 'QF': 0})
    dr.variables['ATMO_PRESSURE_D3H'] = DataValue(fuzz(-500, 100, -1), metadata={'UN': 'Pa', 'PR': 10, 'QF': 0})
    dr.variables['WMO_ATMO_PRESSURE_CCODE'] = 5
    dr.variables['WAVE_DIR'] = DataValue(fuzz(100, 10, 0), metadata={'UN': 'degrees', 'PR': 1, 'QF': 0})
    dr.variables['WAVE_PERIOD'] = DataValue(fuzz(9.3, 2, 1), metadata={'UN': 's', 'PR': 0.1, 'QF': 0})
    dr.variables['WAVE_HEIGHT'] = DataValue(fuzz(12.3, 2, 1), metadata={'UN': 'm', 'PR': 0.1, 'QF': 0})
    dr.variables['AIR_TEMP'] = DataValue(fuzz(287.1, 3, 1), metadata={
        'UN': 'K',
        'PR': 0.1,
        'QF': 0,
        'SENSOR_HEIGHT_LOCAL': DataValue(1.2, metadata={'UN': 'm', 'PR': 0.1})
    })
    dr.variables['WET_TEMP'] = DataValue(fuzz(289.3, 3, 1), metadata={
        'UN': 'K',
        'PR': 0.1,
        'QF': 0,
        'SENSOR_HEIGHT_LOCAL': DataValue(1.2, metadata={'UN': 'm', 'PR': 0.1}),
        'WMO_METHOD_WET_TEMP': 4
    })
    dr.variables['RH'] = DataValue(fuzz(93.1, 5, 1), metadata={
        'UN': '0.01',
        'PR': 0.1,
        'QF': 0,
        'SENSOR_HEIGHT_LOCAL': DataValue(1.2, metadata={'UN': 'm', 'PR': 0.1}),
        'WMO_METHOD_WET_TEMP': 4
    })
    dr.variables['WIND_DIR'] = DataValue(fuzz(50, 90, 0), metadata={
        'UN': 'degrees',
        'PR': 1,
        'QF': 0,
        'SENSOR_HEIGHT_LOCAL': DataValue(2.1, metadata={'UN': 'm', 'PR': 0.1}),
        'AGR_METHOD': 'AVERAGE',
        'OBS_PERIOD': 'P5M',
        'WMO_ITYPE_WIND': 5,
    })
    dr.variables['WIND_SPD'] = DataValue(fuzz(5.4, 2, 1), metadata={
        'UN': 'm s-1',
        'PR': 0.1,
        'QF': 0,
        'SENSOR_HEIGHT_LOCAL': DataValue(2.1, metadata={'UN': 'm', 'PR': 0.1}),
        'AGR_METHOD': 'AVERAGE',
        'OBS_PERIOD': 'P5M',
        'WMO_ITYPE_WIND': 5,
    })
    dr.variables['TEMP'] = DataValue(fuzz(277.15, 2, 2), metadata={
        'UN': 'K',
        'PR': 0.01,
        'SENSOR_DEPTH': DataValue(0.15, metadata={'UN': 'm', 'PR': 0.01}),
        'WMO_METHOD_TS': 4
    })
    dr.variables['SALN'] = DataValue(fuzz(38.123, 0.5, 3), metadata={
        'UN': '0.001',
        'PR': 0.001,
        'SENSOR_DEPTH': DataValue(0.15, metadata={'UN': 'm', 'PR': 0.01}),
        'WMO_METHOD_SD': 4
    })
    dr.variables['CURRENT_DIR'] = DataValue(fuzz(143, 20, 0), metadata={
        'UN': 'degrees',
        'PR': 1,
        'WMO_METHOD_CURRENT_DUR': 5,
        'WMO_METHOD_CURRENT_S': 6,
        'WMO_METHOD_CURRENT_PR': 7,
    })
    dr.variables['CURRENT_SPD'] = DataValue(fuzz(2.1, 1, 1), metadata={
        'UN': 'm s-1',
        'PR': 0.1,
        'WMO_METHOD_CURRENT_DUR': 5,
        'WMO_METHOD_CURRENT_S': 6,
        'WMO_METHOD_CURRENT_PR': 7,
    })
    prof0 = RecordSet()
    dr.subrecords['PROFILE_0'] = prof0
    prof0.metadata['WMO_ITPYE_TPREC'] = 5
    prof0.metadata['WMO_DIGITIND'] = 4
    prof0.metadata['PROFILE_DIRECTION'] = 2
    for z in z_set2:
        sr = DataRecord()
        sr.coordinates['DEPTH'] = DataValue(fuzz(z, 5, 0), metadata={
            'UN': 'm',
            'PR': 1,
            'QF': 0,
            'WMO_METHOD_DEPTH': 5,
            'WMO_METHOD_SD': 1
        })
        sr.coordinates['PRESSURE'] = DataValue(fuzz(z, 5, 0) * 1000, metadata={
            'UN': 'Pa',
            'PR': 1000,
            'QF': 0,
            'WMO_METHOD_DEPTH': 5,
            'WMO_METHOD_SD': 1
        })
        sr.variables['TEMP'] = DataValue(fuzz(287.12, 0.2, 2), metadata={
            'UN': 'K',
            'PR': 0.01,
            'QF': 0,
            'WMO_METHOD_TS': 2,
            'WMO_ITYPE_TSP': 3,
            'INST_SERIAL_NO_TP': '1234-12344-1234',
            'WMO_METHOD_SD': 1
        })
        sr.variables['SALN'] = DataValue(fuzz(38.123, 0.2, 3), metadata={
            'UN': '0.001',
            'PR': 0.001,
            'QF': 0,
            'WMO_METHOD_TS': 2,
            'WMO_ITYPE_TSP': 3,
            'INST_SERIAL_NO_TP': '1234-12344-1234',
            'WMO_METHOD_SD': 1,
        })
        prof0.records.append(sr)
    prof1 = RecordSet()
    prof1.metadata['WMO_DIGITIND'] = 2
    prof1.metadata['PROFILE_DIRECTION'] = 1
    dr.subrecords['PROFILE_1'] = prof1
    for z in z_set3:
        sr = DataRecord()
        sr.coordinates['DEPTH'] = DataValue(fuzz(z, 5, 1), metadata={
            'UN': 'm',
            'PR': 1,
            'QF': 0,
            'WMO_METHOD_DEPTH': 5,
        })
        sr.coordinates['PRESSURE'] = DataValue(fuzz(z, 5, 1) * 1000, metadata={
            'UN': 'Pa',
            'PR': 1000,
            'QF': 0,
            'WMO_METHOD_DEPTH': 5,
        })
        sr.variables['CURRENT_SPD'] = DataValue(fuzz(2.31, 0.1, 2), metadata={
            'UN': 'm s-1',
            'PR': 0.01,
            'QF': 0,
            'WMO_METHOD_CURRENT_S': 2,
            'WMO_METHOD_CURRENT_DUR': 3,
            'WMO_METHOD_CURRENT_PR': 4,
        })
        sr.variables['CURRENT_DIR'] = DataValue(fuzz(180, 20, 1), metadata={
            'UN': 'degrees',
            'PR': 1,
            'QF': 0,
            'WMO_METHOD_CURRENT_S': 2,
            'WMO_METHOD_CURRENT_DUR': 3,
            'WMO_METHOD_CURRENT_PR': 4,
        })
        prof1.records.append(sr)
    prof2 = RecordSet()
    prof2.metadata['WMO_DIGITIND'] = 2
    dr.subrecords['PROFILE_2'] = prof2
    for z in z_set4:
        sr = DataRecord()
        sr.coordinates['DEPTH'] = DataValue(fuzz(z, 2, 0), metadata={
            'UN': 'm',
            'PR': 1,
            'QF': 0,
            'WMO_METHOD_DEPTH': 5,
        })
        sr.coordinates['PRESSURE'] = DataValue(fuzz(z, 2, 0) * 1000, metadata={
            'UN': 'Pa',
            'PR': 1000,
            'QF': 0,
            'WMO_METHOD_DEPTH': 5,
        })
        sr.coordinates['DOXY'] = DataValue(fuzz(23.1, 5, 1), metadata={
            'UN': 'umol kg-1',
            'PR': 0.1,
            'QF': 0
        })
        prof2.records.append(sr)

    return dr



record = _315007_record(0, datetime.datetime(2023, 1, 1, 0, 0, 0))

options = [
    {"text_format": "JSON", "compression": None, "correction": None},
    {"text_format": "JSON", "compression": "LZMA0", "correction": None},
    {"text_format": "JSON", "compression": "LZMA1", "correction": None},
    {"text_format": "JSON", "compression": "LZMA2", "correction": None},
    {"text_format": "JSON", "compression": "LZMA3", "correction": None},
    {"text_format": "JSON", "compression": "LZMA4", "correction": None},
    {"text_format": "JSON", "compression": "LZMA5", "correction": None},
    {"text_format": "JSON", "compression": "LZMA6", "correction": None},
    {"text_format": "JSON", "compression": "LZMA7", "correction": None},
    {"text_format": "JSON", "compression": "LZMA8", "correction": None},
    {"text_format": "JSON", "compression": "LZMA9", "correction": None},
]

for opt in options:
    iterations = 100
    time_values = []
    size_values = []
    for i in range(0, iterations):
        st = time.perf_counter()
        ba = bytearray()
        for x in codec.encode(record, **opt):
            ba.extend(x)
        time_values.append(time.perf_counter() - st)
        size_values.append(len(ba))
    mean_t = statistics.mean(time_values)
    mean_s = statistics.mean(size_values)
    print(f"{opt['text_format']},{opt['compression'] or ''},{opt['correction'] or ''},{iterations},{mean_t},{statistics.stdev(time_values)},{mean_s},{statistics.stdev(size_values)}")
