import umath_wrapper as umath
from uncertainties import ufloat
import typing as t
import shapely


_EARTH_RADIUS = ufloat(6371000, 10000)


def uhaversine(point1: tuple[umath.FLOAT, umath.FLOAT],
               point2: tuple[umath.FLOAT, umath.FLOAT]) -> umath.FLOAT:
    lat1 = umath.radians(point1[0])
    lon1 = umath.radians(point1[1])
    lat2 = umath.radians(point2[0])
    lon2 = umath.radians(point2[1])
    d_lat = lat2 - lat1
    d_lon = lon2 - lon1
    a = (umath.sin(d_lat * 0.5) ** 2) + (umath.cos(lat1) * umath.cos(lat2) * (umath.sin(d_lon * 0.5) ** 2))
    c = 2 * umath.atan2(umath.sqrt(a), umath.sqrt(1 - a))
    return c * _EARTH_RADIUS


def upoint_to_geometry(
        latitude: umath.FLOAT,
        longitude: umath.FLOAT
        ) -> shapely.Geometry:
    lat_no_d = isinstance(latitude, float) or latitude.std_dev == 0
    lon_no_d = isinstance(longitude, float) or longitude.std_dev == 0
    if lat_no_d and lon_no_d:
        return shapely.Point(longitude, latitude)
    elif lat_no_d:
        return shapely.LineString([
            shapely.Point(longitude - longitude.std_dev, latitude),
            shapely.Point(longitude + longitude.std_dev, latitude)
        ])
    elif lon_no_d:
        return shapely.LineString([
            shapely.Point(longitude, latitude - latitude.std_dev),
            shapely.Point(longitude, latitude + latitude.std_dev)
        ])
    else:
        return shapely.Polygon([
            shapely.Point(longitude - longitude.std_dev, latitude - latitude.std_dev),
            shapely.Point(longitude + longitude.std_dev, latitude - latitude.std_dev),
            shapely.Point(longitude + longitude.std_dev, latitude + latitude.std_dev),
            shapely.Point(longitude - longitude.std_dev, latitude + latitude.std_dev),
            shapely.Point(longitude - longitude.std_dev, latitude - latitude.std_dev),
        ])
