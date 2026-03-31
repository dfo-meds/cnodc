import math
import typing as t
import netCDF4 as nc
import unicodedata
import numpy as np

UNICODE_SPACES = "\t\u00A0\u180E\u2002\u2000\u2003\u2004\u2005\u2006\u2008\u2007\u2009\u200A\u200B\u202F\u205F\u3000\uFEFF"
UNICODE_DASHES = "\u058A\u05BE\u1806\u2010\u2011\u2012\u2013\u2014\u2015\u2E3A\u2E3B\uFE58\uFE63\uFF0D"


def netcdf_bytes_to_string(byte_sequence: t.Union[str, bytes], encoding='utf-8'):
    if isinstance(byte_sequence, str):
        return normalize_string(byte_sequence)
    return normalize_string(b''.join(bytes(x) for x in byte_sequence).replace(b'\x00', b'').decode(encoding))


def str_to_netcdf_vlen(s: t.Union[t.Sequence[str], str]):
    if isinstance(s, str):
        s = [s]
    return np.array(s, dtype=object)

def str_to_netcdf(s: t.Union[t.Sequence[str], str], fixed_len: int):
    if isinstance(s, str):
        s = [s]
    return nc.stringtochar(np.array(s, dtype=f"S{fixed_len}"))


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


UNNUMPY = {
    'NoneType': lambda x: None,
    'MaskedConstant': lambda x: None,
    'Decimal': lambda x: x,
    'int': lambda x: x,
    'float': lambda x: x if not math.isnan(x) else None,
    'int64': lambda x: int(x.item()),
    'int32': lambda x: int(x.item()),
    'int16': lambda x: int(x.item()),
    'int8': lambda x: int(x.item()),
    'float16': lambda x: unnumpy(float(x.item())),
    'float32': lambda x: unnumpy(float(x.item())),
    'float64': lambda x: unnumpy(float(x.item())),
    'MaskedArray': lambda x: [unnumpy(y) for y in x.tolist(None)] if x.ndim > 0 else unnumpy(x.item()),
    'ndarray': lambda x: [unnumpy(y) for y in x.tolist()] if x.ndim > 0 else unnumpy(x.item()),
    'str': lambda x: normalize_string(x),
}

def unnumpy(numpy_val):
    cls = numpy_val.__class__.__name__
    if cls in UNNUMPY:
        numpy_val = UNNUMPY[cls](numpy_val)
    return numpy_val