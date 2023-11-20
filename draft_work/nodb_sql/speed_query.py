import statistics

query1 = """
SELECT COUNT(*) 
FROM test1.test1
WHERE 
    test_data ? %s
"""

query2 = """
SELECT COUNT(*)
FROM test1.test2
WHERE test_data @> %s
"""

query3 = """
SELECT COUNT(DISTINCT x.pkey)
FROM test1.test3 as x 
    JOIN test1.test3b as Y
    ON x.pkey = y.remote_key
WHERE
    y.val = %s
"""

import psycopg2
import sys
import time
import random

scenarios = [
    'TEMP', 'CURRENT_DIR', 'AIR_TEMP', 'WAVE', 'SWAVE',
    'WIND_GUST_MAX_SPD', 'RAD', 'DOXY', 'WIND_SPD', 'NOT_A_VAR',

]

conn = psycopg2.connect(dbname="nodb_test", user=sys.argv[1], password=sys.argv[2], host="localhost")

iterations = 30
total1 = {}
total2 = {}
total3 = {}


def do_query(cur, query, params, totals):
    st = time.perf_counter()
    cur.execute(query, params)
    res = cur.fetchone()
    totals[1] = res[0]
    totals[0].append(time.perf_counter() - st)


with conn.cursor() as cur:
    for sname in scenarios:
        if sname not in total1:
            total1[sname] = [[], 0]
        if sname not in total2:
            total2[sname] = [[], 0]
        if sname not in total3:
            total3[sname] = [[], 0]
        queries = [
            (cur, query1, [sname], total1[sname]),
            (cur, query2, ["{" + sname + "}"], total2[sname]),
            (cur, query3, [sname], total3[sname])
        ]
        for i in range(0, iterations):
            random.shuffle(queries)
            for qargs in queries:
                do_query(*qargs)


outputs = [
    (
        sname,
        len(total1[sname][0]),
        round((1000 * statistics.mean(total1[sname][0])), 3),
        round((1000 * statistics.stdev(total1[sname][0])), 3) if len(total1[sname][0]) > 1 else "",
        total1[sname][1],
        len(total2[sname][0]),
        round((1000 * statistics.mean(total2[sname][0])), 3),
        round((1000 * statistics.stdev(total2[sname][0])), 3) if len(total2[sname][0]) > 1 else "",
        total2[sname][1],
        len(total3[sname][0]),
        round((1000 * statistics.mean(total3[sname][0])), 3),
        round((1000 * statistics.stdev(total3[sname][0])), 3) if len(total3[sname][0]) > 1 else "",
        total3[sname][1]

    )
    for sname in total1
]

print("scenario,n [jsonb],avg,std,len,n [varchar[]],avg,std,len,n [relational],avg,std,len")
for sname, xn, x, xs, xt, yn, y, ys, yt, zn, z, zs, zt in outputs:
    print(f"{sname},{xn},{x},{xs},{xt},{yn},{y},{ys},{yt},{zn},{z},{zs},{zt}")

