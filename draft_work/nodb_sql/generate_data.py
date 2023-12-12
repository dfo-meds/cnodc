import datetime
import time

import psycopg2
from psycopg2.extras import Json
import sys
import pathlib
import random

sys.path.append(str(pathlib.Path(__file__).absolute().parent.parent / "src"))

from cnodc.ocproc2 import DataRecord, DataValue, RecordSet
from cnodc.decode.ocproc2_bin import OCProc2BinaryCodec
from cnodc.nodb.structures import _SearchDataAggregator

if len(sys.argv) < 4:
    print("missing postgresql username and password")
    exit(1)

station_idx = int(sys.argv[3])
if station_idx > 9 or station_idx < 0:
    exit(1)

codec = OCProc2BinaryCodec()

conn = None

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


def _315003_record(idx, dt):
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
    dr.metadata['WMO_ID'] = f'0000{idx}'
    dr.metadata['PLATFORM_SERIAL_NO'] = f'0000{idx}012345'
    dr.metadata['PLATFORM_MODEL'] = 'foo' if idx % 3 else 'bar'
    dr.metadata['WMO_BUOY_TYPE'] = 5
    dr.metadata['WMO_DATA_SYSTEM'] = 6
    dr.metadata['WMO_DATA_BUOY_TYPE'] = 7
    dr.metadata['FLOAT_CYCLE_NUMBER'] = 1003 + idx + random.randint(0, 1000000)
    dr.metadata['PROFILE_DIRECTION'] = 1 if idx %3 else 2
    dr.metadata['WMO_ITYPE_TSP'] = 21
    for z in z_set:
        sr = DataRecord()
        sr.coordinates['PRESSURE'] = DataValue(
            reported_value=fuzz(z, 2000, -3),
            metadata={
                'UN': 'Pa',
                'PR': 1000,
                'QF': 0,
            }
        )
        sr.variables['SALN'] = DataValue(
            reported_value=fuzz(39.145, 0.2, 3),
            metadata={
                'UN': '0.001',
                'PR': 0.001,
                'QF': 0,
            }
        )
        sr.variables['TEMP'] = DataValue(
            reported_value=fuzz(287.875, 0.2, 3),
            metadata={
                'UN': 'K',
                'PR': 0.001,
                'QF': 0,
            }
        )
        dr.subrecords.append('PROFILE_0', sr)
    return dr


try:
    conn = psycopg2.connect(dbname="nodb_test", user=sys.argv[1], password=sys.argv[2], host="localhost")

    with conn.cursor() as cur:
        cur.execute("INSERT INTO nodb_station_types (machine_name) VALUES ('argo') ON CONFLICT (machine_name) DO NOTHING")
        cur.execute("INSERT INTO nodb_station_types (machine_name) VALUES ('pfloat') ON CONFLICT (machine_name) DO NOTHING")
        cur.execute("INSERT INTO nodb_source_files (source_path, file_name) VALUES ('C:/local/test.csv', 'test.csv') ON CONFLICT (source_path) DO NOTHING")
        cur.execute("SELECT pkey FROM nodb_source_files WHERE source_path = 'C:/local/test.csv'")
        row = cur.fetchone()
        source_file_uuid = row[0]
        station_uuids = []
        for i in range(0, 10):
            cur.execute(f"SELECT pkey FROM nodb_stations WHERE wmo_id = '0000{i}'")
            row = cur.fetchone()
            if row is not None:
                station_uuids.append(row[0])
            else:
                cur.execute(f"INSERT INTO nodb_stations (wmo_id, station_type_name, status) VALUES ('0000{i}', '{'argo' if i % 2 else 'pfloat'}', 'ACTIVE') RETURNING pkey")
                row = cur.fetchone()
                station_uuids.append(row[0])
        conn.commit()
        cur.execute("TRUNCATE nodb_obs CASCADE")
        conn.commit()
        ref_time = datetime.datetime(2021, 1, 1, 0, 0, 0, 0)
        last_month = None
        message_idx = 1 + station_idx
        record_idx = 0
        st = time.perf_counter()
        for i in range(0, 1052000):
            dt = ref_time + datetime.timedelta(minutes=i * 5)
            station_uuid = station_uuids[station_idx]
            record_idx += 1
            if record_idx > 1000:
                record_idx = 1
                message_idx += 10
                print(f"Record {(message_idx - 1) * 100}, elapsed = {round(time.perf_counter() - st, 1)} s")
            dr = _315003_record(station_idx, dt) if station_idx % 2 else _315007_record(station_idx, dt)
            bin_data = bytearray()
            for byte_ in codec._encode(dr, text_format="JSON", compression="LZMA0", correction=None):
                bin_data.extend(byte_)
            aggr = _SearchDataAggregator(dr)
            search_data = {}
            cur.execute("""
                INSERT INTO nodb_obs (
                    source_file_uuid, 
                    message_idx, 
                    record_idx, 
                    station_uuid, 
                    mission_name, 
                    obs_time, 
                    latitude, 
                    longitude,
                    status,
                    metadata,
                    search_data,
                    data_record
                ) VALUES (
                    %s, %s, %s, %s, 'mission one', %s, %s, %s, 'VERIFIED', %s, %s, %s
                )
            """, (
                source_file_uuid,
                message_idx,
                record_idx,
                station_uuid,
                aggr.statistics['_rpr_time'],
                aggr.statistics['_rpr_lat'],
                aggr.statistics['_rpr_lon'],
                Json({
                    'qc_tests': ['one', 'two', 'three', 'four', 'tell' ,'me', 'that', 'you' ,'love', 'me', 'more', 'sleepless', 'long' 'nights']
                }),
                Json({x: aggr.statistics[x] for x in aggr.statistics if x[0] != '_'}),
                bin_data
            ))
            conn.commit()



finally:
    if conn is not None:
        conn.close()
