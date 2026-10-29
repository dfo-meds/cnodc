""" Provides the FastGeoGrid class, designed for rapid access to binary data in a two- or three-dimensional
    grid with consistent x and y sizes and variable z sizes.

"""
import math
import pathlib
import typing as t
import fractions

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

        Note that the increments may be a floating-point number, a fraction, or a tuple representing a fraction (e.g.
        `(1, 60)` represents 1" separations). When the tuple or fraction form is used, care is taken to limit
        multiplication errors - given x increments of size (n, d), the total increment is calculated as (x * n) / d
        instead of x * (n / d).

    """

    def __init__(self,
                 file: pathlib.Path,
                 start_x: float,
                 start_y: float,
                 end_x: float,
                 end_y: float,
                 x_increment: float | tuple[int, int] | fractions.Fraction,
                 y_increment: float | tuple[int, int] | fractions.Fraction,
                 z_values: list[float],
                 end_z: float,
                 data_size_bytes: int,
                 cache_size: int = 250):
        """
        :param file: The file where data is stored
        :param start_x: The smallest position of a grid cell along the x-axis
        :param start_y: The smallest position of a grid cell along the y-axis
        :param end_x: The largest position of a grid cell along the x-axis
        :param end_y: The largest position of a grid cell along the y-axis
        :param x_increment: The size of each grid cell along the x-axis
        :param y_increment: The size of each grid cell along the y-axis
        :param z_values: The smallest position of each grid cell along the z-axis
        :param end_z: The largest position of a grid cell along the z-axis
        :param data_size_bytes: The size of data for each cell
        :param cache_size: The number of values to keep in the cache (defaults to 250)
        """
        self._file = file
        self._start_x = start_x
        self._start_y = start_y
        if isinstance(x_increment, tuple):
            self._increment_x = fractions.Fraction(*x_increment)
        else:
            self._increment_x = x_increment
        if isinstance(y_increment, tuple):
            self._increment_y = fractions.Fraction(*y_increment)
        else:
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
    def calculate_cell_number(position: float, start: float, increment: float | fractions.Fraction) -> int:
        """ Calculates the cell number of the position (0 is the first cell index).

            :param position: The position along the given axis.
            :param start: The smallest value of the axis.
            :param increment: The increment of the axis.
            :returns: grid cell location.
        """
        return math.floor((position - start) * increment)

    @staticmethod
    def calculate_coordinate(cell_number, start: float, increment: float | fractions.Fraction, delta: float | fractions.Fraction) -> float:
        """ Calculates the coordinate position of the grid cell. Defaults to the left-boundary if delta=0.
            :param cell_number: The grid cell number.
            :param start: The smallest value of the axis.
            :param increment: The increment of the axis.
            :param delta: An additional value to add (e.g. add half the increment to get the centroid).
            :returns: The grid cell's position along the axis.
        """
        return float((cell_number * increment) + start + delta)

    def get_x_coordinate(self, lon: float) -> int | None:
        """ Retrieves the x-coordinate from the longitude. """
        if lon < self._start_x:
            return None
        if lon >= self._end_x:
            return None
        return self.calculate_cell_number(lon, self._start_x, self._increment_x)

    def get_longitude(self, grid_x: int, centre: bool = True) -> float | None:
        """ Retrieves the longitude from the x-coordinate. """
        if grid_x < 0:
            return None
        if grid_x > self._max_x:
            return None
        return self.calculate_coordinate(grid_x, self._start_x, self._increment_x, self._d_x if centre else 0)

    def get_y_coordinate(self, lat: float) -> int | None:
        """ Retrieves the y-coordinate from the latitude."""
        if lat < self._start_y:
            return None
        if lat >= self._end_y:
            return None
        return self.calculate_cell_number(lat, self._start_y, self._increment_y)

    def get_latitude(self, grid_y: int, centre: bool = True) -> float | None:
        """ Retrieves the latitude from the y-coordinate. """
        if grid_y < 0:
            return None
        if grid_y > self._max_y:
            return None
        return self.calculate_coordinate(grid_y, self._start_y, self._increment_y, self._d_y if centre else 0)

    def get_z_coordinate(self, depth: float) -> int | None:
        """ Retrieves the z-coordinate from the depth. """
        for x in range(0, self._max_z):
            if self._z_values[x] <= depth < self._z_values[x + 1]:
                return x
        if self._z_values[-1] <= depth < self._end_z:
            return self._max_z
        return None

    def get_depth(self, grid_z: int, centre: bool = True) -> float | None:
        """ Retrieves the depth from the z-coordinate. """
        if grid_z < 0:
            return None
        if grid_z > self._max_z:
            return None
        if grid_z < self._max_z:
            min_z, max_z = self._z_values[grid_z], self._z_values[grid_z + 1]
        else:
            min_z, max_z = self._z_values[grid_z], self._max_z
        if centre:
            return min_z + (max_z - min_z) / 2.0
        else:
            return min_z

    def get_position(self, x: int, y: int, z: int, centre: bool = True) -> tuple[float, float, float] | None:
        """ Given an (x,y,z) grid position, returns the longitude, latitude, and depth. """
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

    def get_coordinates(self, lon: float, lat: float, depth: float) -> tuple[int, int, int] | None:
        """ Given a longitude, latitude, and depth returns the grid x, y, and z coordinates. """
        x_coord = self.get_x_coordinate(lon)
        if x_coord is None:
            return None
        y_coord = self.get_y_coordinate(lat)
        if y_coord is None:
            return None
        z_coord = self.get_z_coordinate(depth)
        if z_coord is None:
            return None
        return x_coord, y_coord, z_coord

    def get_bytes_value(self, coords: tuple[int, int, int]) -> bytes:
        """ Given grid coordinates, retrieves the bytes stored in the file for that position with caching.
            :raises OSError: If there is an error reading from the file.
        """
        return self._cache.with_cache(coords, self._get_bytes_value, coords)

    def _get_bytes_value(self, coords: tuple[int, int, int]) -> bytes:
        """ Given grid coordinates, retrieves the bytes stored in the file for that position (no-caching version).
            :raises OSError: If there is an error reading from the file.
        """

        # skip to the right x coordinate
        skip_size = (coords[0] * (self._max_y + 1) * (self._max_z + 1))

        # skip to the right y coordinate
        skip_size += (coords[1] * (self._max_z + 1))

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

    def build_file(self,
                   cb: t.Callable[[tuple[int, int, int]], bytes],
                   file: pathlib.Path | None = None,
                   output: bool = False):
        """ Builds a file from data. The data is provided as a callback that retrieves data for the given grid cell.
            It will be called once per grid cell.
        """
        with open(file or self._file, "wb") as f:
            for x in range(0, self._max_x + 1):
                for y in range(0, self._max_y + 1):
                    for z in range(0, self._max_z + 1):
                        data = cb((x, y, z))
                        if len(data) > self._data_size_bytes:
                            raise ValueError("Too long data")
                        while len(data) < self._data_size_bytes:
                            data = b"\x00" + data
                        if output:
                            print(f"{x},{y},{z}", end="\r")
                        f.write(data)
        if output:
            print("\n")


