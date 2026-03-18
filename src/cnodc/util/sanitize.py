import datetime
import decimal
import math
import typing as t
import netCDF4 as nc

import unicodedata

import numpy as np


JsonEncodable = t.Union[None, bool, str, float, int, list, dict]

UNICODE_SPACES = "\t\u00A0\u180E\u2002\u2000\u2003\u2004\u2005\u2006\u2008\u2007\u2009\u200A\u200B\u202F\u205F\u3000\uFEFF"
UNICODE_DASHES = "\u058A\u05BE\u1806\u2010\u2011\u2012\u2013\u2014\u2015\u2E3A\u2E3B\uFE58\uFE63\uFF0D"


def netcdf_bytes_to_string(byte_sequence, encoding='utf-8'):
    if isinstance(byte_sequence, str):
        return normalize_string(byte_sequence)
    return normalize_string(b''.join(bytes(x) for x in byte_sequence).replace(b'\x00', b'').decode(encoding))

def str_to_netcdf_vlen(s: t.Union[t.Sequence[str], str]):
    if isinstance(s, str):
        s = [s]
    return np.array([s], dtype=object)

def str_to_netcdf(s: t.Union[t.Sequence[str], str], fixed_len: int):
    if isinstance(s, str):
        s = [s]
    return nc.stringtochar(np.array(s, dtype=f"S{fixed_len}"))

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
    value = value.replace("\r\n", "\n")
    for c in UNICODE_SPACES:
        value = value.replace(c, " ")
    for c in UNICODE_DASHES:
        value = value.replace(c, "-")
    value = ''.join(c for c in value if (not unicodedata.category(c) == 'Cc') or c == "\n")
    value = value.replace("\x00", "")
    value = value.strip(" ")
    while "  " in value:
        value = value.replace("  ", " ")
    return value


def unnumpy(numpy_val):
    if numpy_val is None:
        return None
    if isinstance(numpy_val, np.ma.MaskedArray):
        numpy_val = np.ma.filled(numpy_val, fill_value=np.nan)
    if isinstance(numpy_val, decimal.Decimal):
        return numpy_val
    if isinstance(numpy_val, str):
        return normalize_string(numpy_val)
    if isinstance(numpy_val, np.float64):
        return None if math.isnan(numpy_val) else numpy_val.item()
    if isinstance(numpy_val, np.int64):
        return None if math.isnan(numpy_val) else int(numpy_val)
    if np.isscalar(numpy_val):
        if hasattr(numpy_val, "item"):
            item = numpy_val.item()
            return None if math.isnan(item) else item
        return None if math.isnan(numpy_val) else numpy_val
    if isinstance(numpy_val, np.ndarray):
        if isinstance(numpy_val.dtype, (np.dtypes.Int8DType, np.dtypes.Int16DType, np.dtypes.Int32DType, np.dtypes.Int64DType)):
            if numpy_val.ndim == 0:
                val = int(numpy_val)
                return None if math.isnan(val) else val
            return [None if math.isnan(int(x)) else int(x) for x in numpy_val]
        elif isinstance(numpy_val.dtype, (np.dtypes.Float64DType, np.dtypes.Float16DType, np.dtypes.Float32DType)):
            if numpy_val.ndim == 0:
                val = float(numpy_val)
                return None if math.isnan(val) else val
            return [None if math.isnan(float(x)) else float(x) for x in numpy_val]
        else:
            raise ValueError(f'unknown dtype: [{type(numpy_val.dtype)}]')
    else:
        return numpy_val
