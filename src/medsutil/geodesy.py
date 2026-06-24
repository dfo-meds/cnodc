"""Geodesy related functions.

    Note that we re-implement some functions here to properly account for uncertainty in the
    measurement value using the uncertainties library.
"""
from typing import NamedTuple
from geographiclib.geodesic import Geodesic
import shapely
import typing as t
import medsutil.math as amath


class YXPoint(NamedTuple):
    y: amath.AnyNumber
    x: amath.AnyNumber

class Ellipsoid:

    def __init__(self, major: float | int, minor: float | int):
        self.major: float | int = major
        self.minor: float | int = minor
        self._geodesic: Geodesic | None = None

    @property
    def geodesic(self) -> Geodesic:
        if self._geodesic is None:
            self._geodesic = Geodesic(self.major, (self.major - self.minor) / self.major)
        return t.cast(Geodesic, self._geodesic)

    def measure(self, yx1: YXPoint, yx2: YXPoint) -> float:
        result = self.geodesic.Inverse(
            float(yx1[0]),
            float(yx1[1]),
            float(yx2[0]),
            float(yx2[1]),
        )
        return result['s12']



_EARTH_RADIUS = 6371000  # error: 10000, in m

_ELLIPSOIDS = {
    # from https://en.wikipedia.org/wiki/World_Geodetic_System
    "WGS84": Ellipsoid(6378137, 6356752.3142245)
}

def geodesic_distance(yx1: YXPoint, yx2: YXPoint, ellipsoid="WGS84") -> float:
    # TODO: we should add some error calculations if we ever get to them
    # the calculation itself is -very- accurate, but the input values may not be
    return _ELLIPSOIDS[ellipsoid].measure(yx1, yx2)

def great_circle_distance(yx1: YXPoint, yx2: YXPoint) -> amath.AnyNumber:
    """Calculate the distance between two points using the haversine function, maintaining
        the uncertainty associated with the coordinates.
    """
    return haversine_radians(
        YXPoint(amath.radians(yx1[0]), amath.radians(yx1[1])),
        YXPoint(amath.radians(yx2[0]), amath.radians(yx2[1]))
    )

def haversine_radians(yx1: YXPoint, yx2: YXPoint) -> amath.AnyNumber:
    # thanks to Wikipedia for this
    d_lat = amath.sub(yx2[0], yx1[0])
    d_lon = amath.sub(yx2[1], yx1[1])
    a1 = amath.pow(amath.sin(amath.mul(d_lat, 0.5)), 2)
    a2 = amath.product((
        amath.cos(yx1[0]),
        amath.cos(yx2[0]),
        amath.pow(amath.sin(amath.mul(d_lon, 0.5)), 2)
    ))
    a = amath.add(a1, a2)
    c = amath.mul(2, amath.atan2(amath.sqrt(a), amath.sqrt(amath.sub(1, a))))
    return amath.mul(c, _EARTH_RADIUS)

def buffer_coordinates(latitude: amath.AnyNumber, longitude: amath.AnyNumber):
    # TODO: calculate from error where its bigger than this?
    lat_buffer = 1e-9
    lon_buffer = 1e-9
    lat_max = amath.add(latitude, lat_buffer)
    lat_min = amath.sub(latitude, lat_buffer)
    lon_max = amath.add(longitude, lon_buffer)
    lon_min = amath.sub(longitude, lon_buffer)
    return shapely.Polygon([[
        (lon_min, lat_min),
        (lon_min, lat_max),
        (lon_max, lat_max),
        (lon_max, lat_min),
    ]])
