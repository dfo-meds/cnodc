import datetime
import time

import psycopg2
from psycopg2.extras import Json
import sys
import pathlib
import random

conn = None
iterations = 0
in_perf = [0, 0, 0]

try:
    conn = psycopg2.connect(dbname="nodb_test", user=sys.argv[1], password=sys.argv[2], host="localhost")

    with conn.cursor() as cur:
        for i in range(0, 100000000):
            if i % 50 == 0:
                print(f"inserting row {i}", end="\r")
            data = []
            if i % 2 == 0:
                data.append('TEMP')
                data.append('SALN')
            else:
                data.append('CURRENT_DIR')
                data.append('CURRENT_SPD')
            if i % 3 == 0:
                data.append('AIR_TEMP')
                data.append('RH')
            if i % 3 == 1:
                data.append('WAVE')
                if i % 2 == 0:
                    data.append('SWAVE')
            if i % 10000 == 6:
                data.append('WIND_GUST_MAX_SPD')
                data.append('WIND_GUST_MAX_DIR')
            if i % 1000 == 6:
                data.append('RAD')
            if i % 100 == 7:
                data.append('DOXY')
            if i % 50 == 8:
                data.append('WIND_SPD')
                data.append('WIND_DIR')
            data.sort()
            iterations += 1
            st = time.perf_counter()
            cur.execute("INSERT INTO test1.test1 (test_data) VALUES (%s)", [Json(data)])
            conn.commit()
            in_perf[0] += (time.perf_counter() - st)
            st = time.perf_counter()
            cur.execute("INSERT INTO test1.test2 (test_data) VALUES (%s)", [data])
            conn.commit()
            in_perf[1] += (time.perf_counter() - st)
            st = time.perf_counter()
            cur.execute("INSERT INTO test1.test3 DEFAULT VALUES RETURNING pkey")
            row = cur.fetchone()
            for val in data:
                cur.execute("INSERT INTO test1.test3b (remote_key, val) VALUES (%s, %s)", [row[0], val])
            conn.commit()
            in_perf[2] += (time.perf_counter() - st)
            iterations += 1

finally:
    if conn is not None:
        conn.close()

print(f"jsonb,{round((1000 * in_perf[0]) / iterations, 3)}")
print(f"varchar[],{round((1000 * in_perf[1]) / iterations, 3)}")
print(f"relation,{round((1000 * in_perf[2]) / iterations, 3)}")
