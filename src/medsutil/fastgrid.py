import math
import pathlib
import typing as t

from medsutil.cached import LeastRecentCache


class FastGeoGrid:

    def __init__(self,
                 file: pathlib.Path,
                 start_x: float,
                 start_y: float,
                 end_x: float,
                 end_y: float,
                 x_increment: float,
                 y_increment: float,
                 z_values: list[float],
                 data_size_bytes: int,
                 cache_size: int = 250):
        self._file = file
        self._start_x = start_x
        self._start_y = start_y
        self._increment_x = x_increment
        self._increment_y = y_increment
        self._end_x = end_x
        self._end_y = end_y
        self._z_values = z_values
        self._data_size_bytes = data_size_bytes
        self._cache = LeastRecentCache(cache_size)
        self._max_x = math.floor((end_x - start_x - (self._increment_x / 2)) / self._increment_x)
        self._max_y = math.floor((end_y - start_y - (self._increment_y / 2)) / self._increment_y)
        self._max_z = len(z_values) - 1

    def get_longitude_coordinate(self, lon: float) -> int | None:
        if lon < self._start_x:
            return None
        if lon >= self._end_x:
            return None
        return math.floor((lon - self._start_x) / self._increment_x)

    def get_latitude_coordinate(self, lat: float) -> int | None:
        if lat < self._start_y:
            return None
        if lat >= self._end_y:
            return None
        return math.floor((lat - self._start_y) / self._increment_y)

    def get_depth_coordinate(self, depth: float) -> int | None:
        if self._max_z == 0:
            if math.isclose(depth, self._z_values[0], abs_tol=1e-6):
                return 0
        else:
            for x in range(0, self._max_z):
                if self._z_values[x] <= depth < self._z_values[x + 1]:
                    return x
        return None

    def get_grid_coordinates(self, lon: float, lat: float, depth: float) -> tuple[int, int, int] | None:
        x_coord = self.get_longitude_coordinate(lon)
        if x_coord is None:
            return None
        y_coord = self.get_latitude_coordinate(lat)
        if y_coord is None:
            return None
        z_coord = self.get_depth_coordinate(depth)
        if z_coord is None:
            return None
        return x_coord, y_coord, z_coord

    def get_bytes_value(self, coords: tuple[int, int, int]) -> bytes:
        return self._cache.with_cache(coords, self._get_value, coords)

    def _get_value(self, coords: tuple[int, int, int]) -> bytes:
        # skip to the right x coordinate
        skip_size = (coords[0] * self._max_y * self._max_z)
        # skip to the right y coordinate
        skip_size += (coords[1] * self._max_z)
        # skip to the right z coordinate
        skip_size += coords[2]
        # calculate the length to skip in bytes
        skip_size *= self._data_size_bytes
        try:
            with open(self._file, "rb") as f:
                f.seek(skip_size)
                return f.read(self._data_size_bytes)
        except OSError as ex:
            ex.add_note(f"Error while trying to open a fastgrid file {self._file}")
            raise

    def build_file(self, cb: t.Callable[[tuple[int, int, int]], bytes], file: pathlib.Path | None = None):
        with open(file or self._file, "wb") as f:
            for x in range(0, self._max_x):
                for y in range(0, self._max_y):
                    for z in range(0, self._max_z):
                        data = cb((x, y, z))
                        if len(data) > self._data_size_bytes:
                            raise ValueError("Too long data")
                        while len(data) < self._data_size_bytes:
                            data = b"\x00" + data
                        f.write(data)


