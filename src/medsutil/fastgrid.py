import math
import pathlib
import typing as t

from medsutil.cached import LeastRecentCache


class FastGeoGrid:
    """

        The fastgrid format stores binary data for a given gridded data file with consistent
        x and y coordinate spacing, and variable z coordinate spacing. It was designed to support
        fast access to the World Ocean Atlas, but can also be used for other gridded data.

        To work with a fastgrid file, you need to know the following:

        * The range of x-coordinates [x_start, x_end] and the increment x_increment
        * The range of y-coordinates [y_start, y_end] and the increment y_increment
        * The ordered list of z-coordinates that are the start of the depth ranges, and the end of the last range
        * The size of data stored in the file (a consistent number of bytes)

        Grid cell numbers are assigned from smallest coordinate to largest coordinate.


        A fastgrid file stores a packet of binary data at each grid cell location. This
        data is a fixed width for every cell. The cells are stored in the following order:

        * x=0, y=0, z=0
        * x=0, y=0, z=1
        * ...
        * x=0, y=0, z=max_z
        * x=0, y=1, z=0
        * ...
        * x=0, y=1, z=max_z
        * ...
        * x=0, y=max_y, z=max_z
        * ...
        * x=max_x,y=max_y,z=max_z

        In essence, the cells are sorted by increasing x coordinate, then increasing y coordinate (where x1=x2),
        then finally by increasing z coordinate (where x1=x2 and y1=y2).

        With the fixed width data storage format, it is very fast to find the data for any given cell by
        calculating (x * max_y * max_z + y * max_z + z) * data_width  to find the start of the data in the binary
        file, then reading the next data_width bytes to obtain the value for that cell.
        
        Interpretation of those bytes is left up to the implementation - for example WOA stores two 
        doubles (16 bytes) corresponding to the mean and standard deviation of the parameter at that location.


    """

    def __init__(self,
                 file: pathlib.Path,
                 start_x: float,
                 start_y: float,
                 end_x: float,
                 end_y: float,
                 x_increment: float | tuple[int, int],
                 y_increment: float | tuple[int, int],
                 z_values: list[float],
                 end_z: float,
                 data_size_bytes: int,
                 cache_size: int = 250):
        self._file = file
        self._start_x = start_x
        self._start_y = start_y
        self._increment_x = x_increment
        self._increment_y = y_increment
        self._end_x = end_x
        self._end_y = end_y
        self._end_z = end_z
        self._z_values = z_values
        self._data_size_bytes = data_size_bytes
        self._cache = LeastRecentCache(cache_size)
        try:
            self._d_x = (self._increment_x[0] / self._increment_x[1]) / 2
        except TypeError:
            self._d_x = self._increment_x / 2
        try:
            self._d_y = (self._increment_y[0] / self._increment_y[1]) / 2
        except TypeError:
            self._d_y = self._increment_y / 2
        self._max_x = self.calculate_cell_number(end_x - self._d_x, start_x, self._increment_x)
        self._max_y = self.calculate_cell_number(end_y - self._d_y, start_y, self._increment_y)
        self._max_z = len(z_values) - 1

    @staticmethod
    def calculate_cell_number(position: float, start: float, increment: float | tuple):
        try:
            return math.floor(((position - start) * increment[1]) / increment[0])
        except TypeError:
            return math.floor((position - start) * increment)

    @staticmethod
    def calculate_coordinate(grid_coordinate, start: float, increment: float | tuple, delta: float):
        try:
            return ((grid_coordinate * increment[0]) / increment[1]) + start + delta
        except TypeError:
            return (grid_coordinate * increment) + start + delta

    def get_longitude_coordinate(self, lon: float) -> int | None:
        if lon < self._start_x:
            return None
        if lon >= self._end_x:
            return None
        return self.calculate_cell_number(lon, self._start_x, self._increment_x)

    def get_longitude(self, grid_x: int, centre: bool = True) -> float | None:
        if grid_x < 0:
            return None
        if grid_x > self._max_x:
            return None
        return self.calculate_coordinate(grid_x, self._start_x, self._increment_x, self._d_x if centre else 0)

    def get_latitude_coordinate(self, lat: float) -> int | None:
        if lat < self._start_y:
            return None
        if lat >= self._end_y:
            return None
        return self.calculate_cell_number(lat, self._start_y, self._increment_y)

    def get_latitude(self, grid_y: int, centre: bool = True) -> float | None:
        if grid_y < 0:
            return None
        if grid_y > self._max_y:
            return None
        return self.calculate_coordinate(grid_y, self._start_y, self._increment_y, self._d_y if centre else 0)

    def get_depth_coordinate(self, depth: float) -> int | None:
        for x in range(0, self._max_z):
            if self._z_values[x] <= depth < self._z_values[x + 1]:
                return x
        if self._z_values[-1] <= depth <= self._end_z:
            return self._max_z
        return None

    def get_depth(self, grid_z: int, centre: bool = True) -> float | None:
        if grid_z < 0:
            return None
        if grid_z > self._max_z:
            return None
        if grid_z < self._max_z:
            min_z, max_z = self._z_values[grid_z], self._z_values[grid_z]
        else:
            min_z, max_z = self._z_values[grid_z] = self._max_z
        if centre:
            return min_z + (max_z - min_z) / 2.0
        else:
            return min_z

    def get_position(self, x: int, y: int, z: int, centre: bool = True) -> tuple[float, float, float] | None:
        longitude = self.get_longitude(x, centre)
        if longitude is None:
            return None
        latitude = self.get_latitude(y, centre)
        if latitude is None:
            return None
        depth = self.get_depth(z, centre)
        if depth is None:
            return None
        return longitude, latitude, depth

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
            for x in range(0, self._max_x + 1):
                for y in range(0, self._max_y + 1):
                    for z in range(0, self._max_z + 1):
                        data = cb((x, y, z))
                        if len(data) > self._data_size_bytes:
                            raise ValueError("Too long data")
                        while len(data) < self._data_size_bytes:
                            data = b"\x00" + data
                        print(f"{x},{y},{z}", end="\r")
                        f.write(data)
        print("\n")


