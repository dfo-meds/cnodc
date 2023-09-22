import datetime
import time
import timeit
import typing as t
import datetime


def normalize_data_value1(dv: t.Any):
    return bool(dv)


def normalize_data_value2(dv: t.Any):
    return bool(dv)


test_values = {
    #'str': 'hello world',
    #'int': 5,
    #'float': 10.2,
    #'list': [1, 2, 3],
    #'set': {1, 2, 3},
    'dict': {'one': 1, 'two': 2, 'three': 3},
    'empty': {},
    #'bool': True,
    #'none': None,
    #'tuple': (1, 2, 3),
    #'datetime': datetime.datetime.now(),
    #'date': datetime.date.today()
}

iterations = 1000000
for dtype in test_values:
    test_value = test_values[dtype]
    total1 = 0
    total2 = 0
    for _ in range(0, iterations):
        st = time.perf_counter()
        norm = normalize_data_value1(test_value)
        t = round((time.perf_counter() - st) * 1000000, 1)
        total1 += t
    for _ in range(0, iterations):
        st = time.perf_counter()
        norm = normalize_data_value2(test_value)
        t = round((time.perf_counter() - st) * 1000000, 1)
        total2 += t
    avg1 = round(total1 / iterations, 2)
    avg2 = round(total2 / iterations, 2)
    print(f"{dtype},{avg1},{avg2},{round(avg2 - avg1, 2)}")