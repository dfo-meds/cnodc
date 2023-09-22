import psycopg2
import sys
import uuid
import random
import time

conn = None

enum_ranges = [
    ('A', 128),
    ('B', 64),
    ('C', 32),
    ('D', 16),
    ('E', 8),
    ('F', 4),
    ('G', 2),
    ('H', 1)
]

enum_values = []
for val, count in enum_ranges:
    enum_values.extend(val for _ in range(0, count))

text_ranges = [
    ('GTS_BUFR', 1000),  # 50.0%
    ('ARGO', 250),       # 12.5%
    ('MISSION_NO1', 1),  #  0.05%
    ('WMO_ID', 10),      #  0.5%
    ('DRIBU', 100),      #  5%
    ('PFLOAT', 500),     # 25%
    ('VIKING', 20),      # 1%
    ('GTSPP', 60),       # 3%
    ('MISSION_NO2', 2),  # 1%
    ('WIGOS_ID', 30),
    ('HELLO_WORLD', 5),
    ('VERY LONG INDEXABLE VALUE', 22),
]
text_values = []
for val, count in text_ranges:
    text_values.extend(val for _ in range(0, count))

random.shuffle(enum_values)
random.shuffle(text_values)

results = [[0, 0], [0, 0]]
iterations = 1000000

try:
    conn = psycopg2.connect(dbname="nodb_test", user=sys.argv[1], password=sys.argv[2], host="localhost")
    with conn.cursor() as cur:
        for i in range(0, iterations):
            if i % 50 == 0:
                print(f"inserting row {i}", end="\r")
            data = [
                random.choice(text_values),
                str(uuid.uuid4()),
                random.choice(enum_values)
            ]
            st = time.perf_counter()
            cur.execute("INSERT INTO test1.btree_indexes (test_a, test_b, test_c) VALUES (%s, %s, %s)", data)
            conn.commit()
            results[0][0] += 1
            results[0][1] += time.perf_counter() - st
            st = time.perf_counter()
            cur.execute("INSERT INTO test1.hash_indexes (test_a, test_b, test_c) VALUES (%s, %s, %s)", data)
            conn.commit()
            results[1][0] += 1
            results[1][1] += time.perf_counter() - st
finally:
    if conn is not None:
        conn.close()
    if results[0][0] > 0:
        print(f"btree insert average [{round(1000 * (results[0][1] / results[0][0]), 3)} ms]")
    if results[1][0] > 0:
        print(f"hash insert average [{round(1000 * (results[1][1] / results[1][0]), 3)} ms]")
