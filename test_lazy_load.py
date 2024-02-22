import math
import time


def is_instance_check(d_or_s):
    if not isinstance(d_or_s, dict):
        return 1
    elif '_value' in d_or_s:
        _ = d_or_s['_value']
        return 2
    else:
        _ = d_or_s['_values']
        return 3


def attribute_error_check(d_or_s):
    if not isinstance(d_or_s, dict):
        return 1
    else:
        try:
            _ = d_or_s['_value']
            return 2
        except KeyError:
            _ = d_or_s['_values']
            return 3


n = 1000
p = 3000
classes = [is_instance_check, attribute_error_check]

r_test = 'hello'
s_test = {'_value': 'hello'}
m_test = {'_values': ['hello']}

for cls in classes:
    res = []
    for i in range(0, p):
        st = time.perf_counter()
        for j in range(0, n):
            cls(r_test)
        res.append((time.perf_counter() - st) / n)
    print(f"{cls.__name__},raw,{round(min(res) * 1000000, 5)} us")
    res = []
    for i in range(0, p):
        st = time.perf_counter()
        for j in range(0, n):
            cls(s_test)
        res.append((time.perf_counter() - st) / n)
    print(f"{cls.__name__},single,{round(min(res) * 1000000, 5)} us")
    res = []
    for i in range(0, p):
        st = time.perf_counter()
        for j in range(0, n):
            cls(m_test)
        res.append((time.perf_counter() - st) / n)
    print(f"{cls.__name__},multi,{round(min(res) * 1000000, 5)} us")
