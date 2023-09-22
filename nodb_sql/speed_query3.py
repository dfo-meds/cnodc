import psycopg2
import sys
import uuid
import statistics
import random
import time

text_total = 2000
text_ranges = {
    'GTS_BUFR': 1000,  # 50.0%
    'ARGO': 250,       # 12.5%
    'MISSION_NO1': 1,  #  0.05%
    'WMO_ID': 10,      #  0.5%
    'DRIBU': 100,      #  5%
    'PFLOAT': 500,     # 25%
    'VIKING': 20,      # 1%
    'GTSPP': 60,       # 3%
    'MISSION_NO2': 2,  # 1%
    'WIGOS_ID': 30,
    'HELLO_WORLD': 5,
    'VERY LONG INDEXABLE VALUE': 22,
}
text_keys = list(text_ranges.keys())

enum_total = 255
enum_ranges = {
    'A': 128,
    'B': 64,
    'C': 32,
    'D': 16,
    'E': 8,
    'F': 4,
    'G': 2,
    'H': 1
}
enum_keys = list(enum_ranges.keys())

uuids = [
    'efd5e4fa-9bc6-4f78-b9e5-0d16150c00fb',
    '7186ae3d-f5ff-4d7b-b234-d521603fde07',
    '6c860184-3f74-4d2b-8bb7-b2e6e49b5f85',
]

results = {
    "enum": {k: {"hash": [[], 0], "btree": [[], 0]} for k in enum_keys},
    "text": {k: {"hash": [[], 0], "btree": [[], 0]} for k in text_keys},
    "uuid": {k: {"hash": [[], 0], "btree": [[], 0]} for k in uuids}
}


def timed_query(cur, query, args, results, k1, k2, k3):
    st = time.perf_counter()
    cur.execute(query, args)
    row = cur.fetchone()
    tt = time.perf_counter() - st
    results[k1][k2][k3][0].append(tt * 1000)
    results[k1][k2][k3][1] = row[0]


performance_options = []
performance_options.extend([
    ("SELECT COUNT(*) FROM test1.btree_indexes WHERE test_c = %s", [e_key], results, "enum", e_key, "btree")
    for e_key in enum_keys

])
performance_options.extend([
    ("SELECT COUNT(*) FROM test1.hash_indexes WHERE test_c = %s", [e_key], results, "enum", e_key, "hash")
    for e_key in enum_keys
])
performance_options.extend([
    ("SELECT COUNT(*) FROM test1.btree_indexes WHERE test_b = %s", [uuid_], results, "uuid", uuid_, "btree")
    for uuid_ in uuids
])
performance_options.extend([
    ("SELECT COUNT(*) FROM test1.hash_indexes WHERE test_b = %s", [uuid_], results, "uuid", uuid_,"hash")
    for uuid_ in uuids
])
performance_options.extend([
    ("SELECT COUNT(*) FROM test1.btree_indexes WHERE test_a = %s", [t_key], results, "text", t_key, "btree")
    for t_key in text_keys
])
performance_options.extend([
    ("SELECT COUNT(*) FROM test1.hash_indexes WHERE test_a = %s", [t_key], results, "text", t_key, "hash")
    for t_key in text_keys
])


iterations = 60
conn = None
try:
    conn = psycopg2.connect(dbname="nodb_test", user=sys.argv[1], password=sys.argv[2], host="localhost")
    with conn.cursor() as cur:
        for i in range(0, iterations):
            print(f"iteration #{i}", end="\r")
            random.shuffle(performance_options)
            for args in performance_options:
                timed_query(cur, *args)

finally:
    if conn is not None:
        conn.close()
    with open("speed_query3.csv", "w") as h:
        h.write("column type,column value,index type,iterations,result size,min speed (ms),average speed (ms),max speed(ms),stdev (ms)\n")
        for column_type in results:
            for column_value in results[column_type]:
                for index_type in results[column_type][column_value]:
                    data = results[column_type][column_value][index_type]
                    mean_ = 0 if len(data[0]) == 0 else statistics.mean(data[0])
                    stdev_ = 0 if len(data[0]) < 2 else statistics.stdev(data[0])
                    h.write(f"{column_type},{column_value},{index_type},{len(data[0])},{data[1]},{round(min(data[0]), 3)},{round(mean_,3)},{round(max(data[0]), 3)},{round(stdev_, 3)}\n")
