"""Geodesy related functions.

    Note that we re-implement some functions here to properly account for uncertainty in the
    measurement value using the ScienceNumber class.
"""
import typing as t

from geographiclib.geodesic import Geodesic
import shapely

import medsutil.math as amath
from medsutil.math.numbers import with_error_propagation


class YXPoint(t.NamedTuple):
    """ Two-dimensional coordinates. """
    y: amath.AnyNumber
    x: amath.AnyNumber


class Ellipsoid:
    """ A helper class that supports geodesic measurements. """

    def __init__(self, major: float | int, minor: float | int):
        self.major: float | int = major
        self.minor: float | int = minor
        self._geodesic: Geodesic | None = None
        self._sn_geodesic_inverse: t.Callable[..., amath.AnyNumber] | None = None

    @property
    def geodesic(self) -> Geodesic:
        """ Builds a Geodesic object that is used for measurements. """
        if self._geodesic is None:
            self._geodesic = Geodesic(self.major, (self.major - self.minor) / self.major)
        return t.cast(Geodesic, self._geodesic)

    @property
    def sn_measure(self) -> t.Callable[..., amath.AnyNumber]:
        """ Builds and caches a function that measures the geodesic distance with error propagation. """

        if self._sn_geodesic_inverse is None:
            @with_error_propagation(..., ..., ..., ..., units_func=lambda: "m")
            def func(y1, x1, y2, x2, /):
                return self._measure(y1, x1, y2, x2)
            self._sn_geodesic_inverse = func

        return t.cast(t.Callable, self._sn_geodesic_inverse)

    def measure(self, yx1: YXPoint, yx2: YXPoint) -> amath.AnyNumber:
        """ Measures the geodesic distance, optionally with error propagation if any number provides an uncertainty. """
        y1, x1 = yx1
        y2, x2 = yx2
        if any(amath.is_science_number(n) for n in (y1, x1, y2, x2)):
            return self.sn_measure(y1, x1, y2, x2, fn=self._measure)
        else:
            return self._measure(y1, x1, y2, x2)

    def _measure(self, y1: amath.BasicNumber, x1: amath.BasicNumber, y2: amath.BasicNumber, x2: amath.BasicNumber) -> float:
        """ Direct implementation of measurement. """
        return self.geodesic.Inverse(
            float(y1),
            float(x1),
            float(y2),
            float(x2),
        )["s12"]



_EARTH_RADIUS = 6371000  # error: 10000, in m

_ELLIPSOIDS = {
    # from https://en.wikipedia.org/wiki/World_Geodetic_System
    "WGS84": Ellipsoid(6378137, 6356752.3142245)
}

def geodesic_distance(yx1: YXPoint, yx2: YXPoint, ellipsoid="WGS84") -> amath.AnyNumber:
    """ Geodesic distance using the given ellipsoid (only WGS84 currently supported). """
    return _ELLIPSOIDS[ellipsoid].measure(yx1, yx2)


def great_circle_distance(yx1: YXPoint, yx2: YXPoint) -> amath.AnyNumber:
    """Calculate the distance between two points using the haversine function, maintaining
        the uncertainty associated with the coordinates.

        Recommend you use geodesic_distance() instead for improved accuracy.
    """
    return haversine_radians(
        YXPoint(amath.radians(yx1[0]), amath.radians(yx1[1])),
        YXPoint(amath.radians(yx2[0]), amath.radians(yx2[1]))
    )


def haversine_radians(yx1: YXPoint, yx2: YXPoint) -> amath.AnyNumber:
    """ Implements the haversine function.

        Implementation borrowed from Wikipedia.
    """
    d_lat = amath.sub(yx2[0], yx1[0])
    d_lon = amath.sub(yx2[1], yx1[1])
    a1 = amath.pow_(amath.sin(amath.mul(d_lat, 0.5)), 2)
    a2 = amath.product((
        amath.cos(yx1[0]),
        amath.cos(yx2[0]),
        amath.pow_(amath.sin(amath.mul(d_lon, 0.5)), 2)
    ))
    a = amath.add(a1, a2)
    c = amath.mul(2, amath.atan2(amath.sqrt(a), amath.sqrt(amath.sub(1, a))))
    return amath.mul(c, _EARTH_RADIUS)


def buffer_coordinates(latitude: amath.AnyNumber, longitude: amath.AnyNumber):
    """ Buffers coordinates into a polygon by adding a rectangular buffer around the point. """
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
