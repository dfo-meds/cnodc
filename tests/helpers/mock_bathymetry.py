from medsutil.bathymetry.base import BathymetryModel
import medsutil.math as amath


class MockBathymetryModel(BathymetryModel):

    def __init__(self):
        super().__init__("mock")
        self._grid: list[list[float]] = [
            [
                self._get_real_mock_depth(x, y)
                for y in range(-90, 89, 4)
            ]
            for x in range(-180, 179, 4)
        ]

    def water_depth(self, x: amath.AnyNumber, y: amath.AnyNumber) -> float:
        x_cell, y_cell = self._identify_cell(float(x), float(y))
        return self._get_depth(x_cell, y_cell)

    def _get_depth(self, x_cell: int, y_cell: int) -> float:
        return self._grid[x_cell][y_cell]

    def _identify_cell(self, x: float, y: float) -> tuple[int, int]:
        return int(x / 4), int(y / 4)

    def _get_real_mock_depth(self, x, y):
        # this is a terrible map, but it lets us test without relying on the giant GEBCO files
        if 75 < x < 95 and 45 < y < 64:
            return 5.0
        if -75 < x < -65 and -43 < y < -29:
            return -1000.0
        return -25.0
