import itertools
import pathlib
import struct

from medsutil.fastgrid import FastGeoGrid
from tests.helpers.base_test_case import BaseTestCase, sub_tests


class DummyGrid(FastGeoGrid):

    def __init__(self, file: pathlib.Path):
        super().__init__(
            file=file,
            start_x=0,
            start_y=-20,
            end_x=10,
            end_y=-10,
            x_increment=1,
            y_increment=1,
            z_values=[0, 5],
            end_z=10,
            data_size_bytes=4
        )


def build_value(x, y, z):
    return (x * 10000) + (y * 100) + z

class TestFastGrid(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        cls.fastgrid = DummyGrid(cls.class_temp_dir / "test.fastgrid")
        cls.fastgrid.build_file(lambda xyz: struct.pack("<i", build_value(*xyz)))

    @sub_tests([
        (-1, None),
        (0, 0),
        (0.5, 0),
        (1, 1),
        (1.99, 1),
        (2, 2),
        (3, 3),
        (4, 4),
        (5, 5),
        (6, 6),
        (7, 7),
        (8, 8),
        (9, 9),
        (9.99, 9),
        (10, None),
        (11, None),
    ])
    def test_get_longitude_coordinate(self, coordinate: float, expected: int | None):
        if expected is None:
            self.assertIsNone(self.fastgrid.get_longitude_coordinate(coordinate))
        else:
            self.assertEqual(expected, self.fastgrid.get_longitude_coordinate(coordinate))

    @sub_tests([
        (-21, None),
        (-20, 0),
        (-19.5, 0),
        (-19, 1),
        (-18.5, 1),
        (-18, 2),
        (-17, 3),
        (-16, 4),
        (-15, 5),
        (-14, 6),
        (-13, 7),
        (-12, 8),
        (-11, 9),
        (-10.01, 9),
        (-10, None),
        (-9, None),
    ])
    def test_get_latitude_coordinate(self, coordinate: float, expected: int | None):
        if expected is None:
            self.assertIsNone(self.fastgrid.get_latitude_coordinate(coordinate))
        else:
            self.assertEqual(expected, self.fastgrid.get_latitude_coordinate(coordinate))

    @sub_tests([
        (0, 0),
        (-1, None),
        (4, 0),
        (5, 1),
        (7, 1),
        (9.9, 1),
        (10, None),
        (11, None),
    ])
    def test_get_depth_coordinate(self, coordinate: float, expected: int | None):
        if expected is None:
            self.assertIsNone(self.fastgrid.get_depth_coordinate(coordinate))
        else:
            self.assertEqual(expected, self.fastgrid.get_depth_coordinate(coordinate))

    @sub_tests([
        (0, -20, 0, (0, 0, 0)),
        (0, -21, 0, None),
        (-1, -20, 0, None),
        (0, -20, -1, None),
    ])
    def test_get_grid_coordinates(self, lon: float, lat: float, depth: float, expected: tuple[int, int, int] | None):
        if expected is None:
            self.assertIsNone(self.fastgrid.get_grid_coordinates(lon, lat, depth))
        else:
            self.assertEqual(self.fastgrid.get_grid_coordinates(lon, lat, depth), expected)

    @sub_tests([
        (x, y, z, build_value(x, y, z))
        for x, y, z in itertools.product(range(0, 10), range(0, 10), range(0, 2))
    ])
    def test_get_value(self, x, y, z, expected):
        value = self.fastgrid.get_bytes_value((x, y, z))
        self.assertEqual(expected, struct.unpack("<i", value)[0])

    @sub_tests([
        (-1, 0, 0, None),
        (0, -1, 0, None),
        (0, 0, -1, None),
        (10, 0, 0, None),
        (0, 10, 0, None),
        (0, 0, 10, None),
        (0, 0, 0, (0.5, -19.5, 2.5)),
    ])
    def test_get_positon(self, x: int, y: int, z: int, expected: None | tuple[float, float, float]):
        if expected is None:
            self.assertIsNone(self.fastgrid.get_position(x, y, z))
        else:
            self.assertEqual(expected, self.fastgrid.get_position(x, y, z))
