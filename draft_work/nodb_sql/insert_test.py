import statistics

from psycopg2.extras import execute_values, execute_batch
import psycopg2
import sys
import time
import uuid_utils as uuid
import tempfile
import pathlib
import io
import gc
import tempfile
import random


def escape_copy_value(v):
    if isinstance(v, str):
        return v.replace("\\", "\\\\").replace("\r", "\\r").replace("\t", "\\t").replace("\n", "\\n").replace("\b", "\\b").replace("\f", "\\f").replace("\v", "\\v")
    elif isinstance(v, (bytes, bytearray)):
        return "\\\\x" + (''.join(hex(x)[2:].zfill(2) for x in v))
    elif v is None:
        return "\\N"
    return str(v)


def spooled_copy(cur, copy_query, values, mem_size=80000, column_sep="\t", row_sep="\n"):
    mem_file = tempfile.SpooledTemporaryFile(mem_size, mode="w+", encoding="utf-8")
    for value_list in values:
        s = column_sep.join(escape_copy_value(v) for v in value_list) + row_sep
        mem_file.write(s)
    mem_file.seek(0, 0)
    cur.copy_expert(copy_query, mem_file)


test_sizes = [1, 5, 10, 25, 50, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000, 25000, 50000, 75000, 100000, 250000, 500000, 750000, 1000000, 5000000, 10000000]
test_sizes = [100]
page_sizes = [5000, 10000, 50000, 100000, 500000, 1000000, 2000000, 4000000, 8000000, 16000000]
# over 10,000 rows, copy_from is superior


iterations = 30
full_results = {}

conn = None
try:
    conn = psycopg2.connect(dbname="nodb_test", user=sys.argv[1], password=sys.argv[2], host="localhost")
    with conn.cursor() as cur:
        for ts in test_sizes:
            test_data = [[random.randbytes(1024)] for _ in range(0, ts)]

            results = {'execute_values': [], 'copy_from': [], }
            full_results[ts] = results
            print(f"insert_size = {ts}", end="\r")
            for iter_no in range(0, iterations):
                if ts < 25:
                    st = time.perf_counter()
                    execute_values(cur, "INSERT INTO test1.test_bin (bin_data) VALUES %s", test_data, page_size=7)
                    conn.commit()
                    results['execute_values'].append(time.perf_counter() - st)

                st = time.perf_counter()
                with open("_dummy.csv", "w") as h:
                    for td in test_data:
                        h.write("\t".join(escape_value(x) for x in td))
                        h.write("\n")
                with open("_dummy.csv", "r") as h:
                    cur.copy_expert("COPY test1.test_bin (bin_data) FROM STDIN", h)
                conn.commit()
                results['copy_from'].append(time.perf_counter() - st)

                for ps in page_sizes:
                    key = f"my_copy_{ps}"
                    if key not in results:
                        results[key] = []
                    st = time.perf_counter()
                    spooled_copy(cur, "COPY test1.test_bin (bin_data) FROM STDIN", test_data, mem_size=ps)
                    conn.commit()
                    results[key].append(time.perf_counter() - st)

finally:
    if conn is not None:
        conn.close()
    with open("insert_results3.csv", "w") as h:
        first = True
        for ts in full_results:
            row_data = [ts]
            q_types = [x for x in full_results[ts]]
            if first:
                h.write("no records," + ",".join(q_types) + "\n")
                first = False
            for query_type in q_types:
                if query_type in full_results[ts] and full_results[ts][query_type]:
                    data = full_results[ts][query_type]
                    row_data.append(round(1000 * statistics.mean(data), 2))
                else:
                    row_data.append(0)
            h.write(",".join(str(x) for x in row_data) + "\n")


