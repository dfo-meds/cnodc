from uncertainties import UFloat
import typing as t
import shapely


def upoint_to_geometry(
        latitude: t.Union[UFloat, float],
        longitude: t.Union[UFloat, float]
        ):
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

