import typing as t
from uncertainties import ufloat, umath

_EARTH_RADIUS = ufloat(6371000, 10000)


def uhaversine(point1: tuple[t.Union[float, ufloat], t.Union[float, ufloat]],
               point2: tuple[t.Union[float, ufloat], t.Union[float, ufloat]]):
    lat1 = umath.radians(point1[0])
    lon1 = umath.radians(point1[1])
    lat2 = umath.radians(point2[0])
    lon2 = umath.radians(point2[1])
    d_lat = lat2 - lat1
    d_lon = lon2 - lon1
    a = (umath.sin(d_lat * 0.5) ** 2) + (umath.cos(lat1) * umath.cos(lat2) * (umath.sin(d_lon * 0.5) ** 2))
    c = 2 * umath.atan2(umath.sqrt(a), umath.sqrt(1 - a))
    return c * _EARTH_RADIUS
