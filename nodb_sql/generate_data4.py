import uuid_utils as uuid
import psycopg2
import sys
import time


conn = None
chunks = 100000
chunk_size = 10000
results = [[0, 0], [0, 0], [0, 0]]

try:
    conn = psycopg2.connect(dbname="nodb_test", user=sys.argv[1], password=sys.argv[2], host="localhost")
    with conn.cursor() as cur:
        big_data = []
        for i in range(0, chunk_size * chunks):
            data = [
                str(uuid.uuid7())
            ]
            big_data.append(data)
            if len(big_data) > chunk_size:
                print(f"inserting rows {(i-chunk_size-1)}-{i-1}", end="\r")
                st = time.perf_counter()
                cur.executemany("INSERT INTO test1.brin_index (test_a) VALUES (%s)", big_data)
                conn.commit()
                results[0][0] += 1
                results[0][1] += time.perf_counter() - st
                st = time.perf_counter()
                cur.executemany("INSERT INTO test1.btree_uuid (test_a) VALUES (%s)", big_data)
                conn.commit()
                results[1][0] += 1
                results[1][1] += time.perf_counter() - st
                st = time.perf_counter()
                cur.executemany("INSERT INTO test1.hash_uuid (test_a) VALUES (%s)", big_data)
                conn.commit()
                results[2][0] += 1
                results[2][1] += time.perf_counter() - st
                big_data = []
finally:
    if conn is not None:
        conn.close()
    if results[0][0] > 0:
        print(f"brin insert average [{round(1000 * (results[0][1] / results[0][0]), 3)} ms]")
    if results[1][0] > 0:
        print(f"btree insert average [{round(1000 * (results[1][1] / results[1][0]), 3)} ms]")
    if results[2][0] > 0:
        print(f"hash insert average [{round(1000 * (results[1][1] / results[1][0]), 3)} ms]")
