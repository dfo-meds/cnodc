import math
import typing as t

EP = 1e-10
EARTH_RADIUS_KM = 6367.4445


def to_vector(lon_degrees, lat_degrees) -> tuple[float, float, float]:
    inc_radians = math.radians(90 - lat_degrees)
    azm_radians = math.radians(lon_degrees)
    inc_sin = math.sin(inc_radians)
    inc_cos = math.cos(inc_radians)
    azm_sin = math.sin(azm_radians)
    azm_cos = math.cos(azm_radians)
    return (
        inc_sin * azm_cos,
        inc_sin * azm_sin,
        inc_cos
    )


def from_vector(x, y, z) -> tuple[t.Optional[float], t.Optional[float]]:
    theta = None
    phi = None
    has_x = abs(x) > EP
    has_y = abs(y) > EP
    has_z = abs(z) > EP
    if has_x or has_y or has_z:
        theta = math.degrees(math.acos(x / math.sqrt(math.pow(x, 2) + math.pow(y, 2) + math.pow(z, 2))))
    if has_x or has_y:
        phi = math.degrees(math.atan2(y, x))
    elif has_z:
        # Default to 0 for polar positions
        phi = 0
    return phi, 90 - theta if theta else None
    # NB: (None, None) is center of the earth (probably you added vectors in opposite directions)
    # NB: (None, 90) or (None, -90) is along a pole and longitude can be arbitrary (set here to 0 for consistency)


def mean_vector(coordinates: list[tuple[float, float]]):
    if not coordinates:
        return None, None
    if len(coordinates) == 1:
        return coordinates[0]
    total = [0, 0, 0]
    for x, y in coordinates:
        vx, vy, vz = to_vector(x, y)
        total[0] += vx
        total[1] += vy
        total[2] += vz
    return from_vector(*total)


def haversine_distance_km(lat1d, lat2d, long1d, long2d):
    lat1 = math.radians(lat1d)
    lat2 = math.radians(lat2d)
    long1 = math.radians(long1d)
    long2 = math.radians(long2d)
    a = math.pow(math.sin((lat2 - lat1) / 2), 2)
    b = math.pow(math.sin((long2 - long1) / 2), 2)
    return math.sqrt(a + (b * math.cos(lat1) * math.cos(lat2))) * 2 * EARTH_RADIUS_KM
