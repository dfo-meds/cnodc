"""Geodesy related functions.

    Note that we re-implement some functions here to properly account for uncertainty in the
    measurement value using the uncertainties library.
"""
from uncertainties import UFloat

import medsutil.amath as amath
import medsutil.adecimal as adecimal
import shapely

YXPoint = tuple[amath.AnyNumber, amath.AnyNumber]

_EARTH_RADIUS = adecimal.AccurateDecimal(6371000, 10000)


def haversine(yx1: YXPoint, yx2: YXPoint) -> amath.AnyNumber:
    """Calculate the distance between two points using the haversine function, maintaining
        the uncertainty associated with the coordinates.
    """

    lat1 = amath.radians(yx1[0])
    lon1 = amath.radians(yx1[1])
    lat2 = amath.radians(yx2[0])
    lon2 = amath.radians(yx2[1])
    d_lat = lat2 - lat1
    d_lon = lon2 - lon1
    a = (amath.sin(d_lat * 0.5) ** 2) + (amath.cos(lat1) * amath.cos(lat2) * (amath.sin(d_lon * 0.5) ** 2))
    c = 2 * amath.atan2(amath.sqrt(a), amath.sqrt(1 - a))
    return c * _EARTH_RADIUS


def coordinates_to_geometry(latitude: amath.AnyNumber, longitude: amath.AnyNumber) -> shapely.Geometry:
    """Convert a point to a sensible geometry that depends on the uncertainty."""
    lat_no_d = (not isinstance(latitude, (adecimal.AccurateDecimal, UFloat))) or latitude.std_dev == 0
    lon_no_d = (not isinstance(longitude, (adecimal.AccurateDecimal, UFloat))) or longitude.std_dev == 0
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

