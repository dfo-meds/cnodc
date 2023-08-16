import math

EARTH_RADIUS_KM = 6367.4445


def haversine_distance_km(lat1d, lat2d, long1d, long2d):
    lat1 = math.radians(lat1d)
    lat2 = math.radians(lat2d)
    long1 = math.radians(long1d)
    long2 = math.radians(long2d)
    a = math.pow(math.sin((lat2 - lat1) / 2), 2)
    b = math.pow(math.sin((long2 - long1) / 2), 2)
    return math.sqrt(a + (b * math.cos(lat1) * math.cos(lat2))) * 2 * EARTH_RADIUS_KM
