import datetime
import decimal
import math
import typing as t
import unicodedata

import numpy as np


JsonEncodable = t.Union[None, bool, str, float, int, list, dict]


def clean_for_json(data):
    if isinstance(data, dict):
        return {
            x: clean_for_json(data[x]) for x in data
        }
    elif isinstance(data, (set, list, tuple)):
        return [clean_for_json(x) for x in data]
    elif isinstance(data, (datetime.datetime, datetime.date)):
        return data.isoformat()
    else:
        return data


def normalize_string(value: str):
    value = unicodedata.normalize('NFC', value)
    value = ''.join(c for c in value if not unicodedata.category(c) == 'Cc')
    value = value.replace("\x00", "")
    return value


def unnumpy(numpy_val):
    if numpy_val is None:
        return None
    elif isinstance(numpy_val, decimal.Decimal):
        return normalize_string(str(numpy_val))
    elif isinstance(numpy_val, str):
        return normalize_string(numpy_val)
    elif isinstance(numpy_val, np.float64):
        return numpy_val.item()
    elif isinstance(numpy_val, np.int64):
        return int(numpy_val)
    elif np.isscalar(numpy_val):
        if hasattr(numpy_val, "item"):
            item = numpy_val.item()
            return None if math.isnan(item) else item
        return None if math.isnan(numpy_val) else numpy_val
    elif isinstance(numpy_val, np.ndarray):
        if isinstance(numpy_val.dtype, np.dtypes.Int8DType):
            if numpy_val.ndim == 0:
                val = int(numpy_val)
                return None if math.isnan(val) else val
            return [None if math.isnan(int(x)) else int(x) for x in numpy_val]
        elif isinstance(numpy_val.dtype, np.dtypes.Float64DType):
            if numpy_val.ndim == 0:
                val = float(numpy_val)
                return None if math.isnan(val) else val
            return [None if math.isnan(float(x)) else float(x) for x in numpy_val]
    elif isinstance(numpy_val, (int, float)):
        return numpy_val if not math.isnan(numpy_val) else None
    else:
        return numpy_val
