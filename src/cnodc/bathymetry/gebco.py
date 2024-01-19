import math
import zarr

from .base import BathymetryModel
import zirconium as zr
from autoinject import injector
import typing as t
import tifffile
from uncertainties import UFloat, ufloat


class GEBCO2023BathymetryModel(BathymetryModel):

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self, base_dir: str, uncertainty: float):
        super().__init__("gebco2023")
        self._gebco_dir = base_dir  # self.config.as_path('gebco2023_directory')
        self._ref_cache: dict[str, tifffile.ZarrTiffStore] = {}
        self._gebco_error = uncertainty

    def close(self):
        for x in self._ref_cache:
            self._ref_cache[x].close()
        self._ref_cache = {}

    def water_depth(self,
                    x: t.Union[float, UFloat],
                    y: t.Union[float, UFloat]) -> t.Union[float, UFloat]:
        min_x = x.nominal_value - x.std_dev if isinstance(x, UFloat) else x
        max_x = x.nominal_value + x.std_dev if isinstance(x, UFloat) else x
        min_y = y.nominal_value - y.std_dev if isinstance(y, UFloat) else y
        max_y = y.nominal_value + y.std_dev if isinstance(y, UFloat) else y
        x_cell_min, y_cell_max = self._identify_cell(min_x, min_y)
        x_cell_max, y_cell_min = self._identify_cell(max_x, max_y)
        min_s = None
        max_s = None
        for xc in range(x_cell_min, x_cell_max + 1):
            for yc in range(y_cell_min, y_cell_max + 1):
                depth = self._get_depth(xc, yc)
                # Given how GEBCO is constructed,
                # results above 0 are in cells identified
                # as land (according to GSHHG). We can therefore
                # exclude them as being unlikely candidates to contain a buoy
                if depth > 0:
                    continue
                # GEBCO actual error vs ship sounding is +/- 150 in deep sea and +/- 180 in coastal areas
                # we assume the larger
                min_cell = depth - self._gebco_error
                max_cell = depth + self._gebco_error
                if min_s is None or min_s > min_cell:
                    min_s = min_cell
                if max_s is None or max_s < max_cell:
                    max_s = max_cell
        if min_s is not None and max_s is not None:
            # Calculate the midpoint between the minimum possible and maximum possible values
            midpoint_s = (max_s + min_s) / 2
            # This represents the range of valid values from
            # the GEBCO chart based on the uncertainties in the cell
            # We return an uncertainty equal to half the range from
            # the lowest possible value to the highest possible value
            return ufloat(midpoint_s, (max_s - midpoint_s))
        else:
            # The result is on land, we don't really care about the elevation,
            # so return something clearly above 0
            return ufloat(100, 0)

    def _get_depth(self, x_cell: int, y_cell: int) -> float:
        if y_cell < 0:
            y_cell = abs(y_cell)
            x_cell += 43200
        elif y_cell > 43200:
            y_cell = 86400 - y_cell
            x_cell += 43200
        if y_cell == 43200:
            y_cell = 43199
        if x_cell < 0:
            x_cell += 86400
        elif x_cell >= 86400:
            x_cell -= 86400
        ns = 'north'
        x_idx = 1
        if y_cell >= 21600:
            y_cell -= 21600
            ns = 'south'
        while x_cell >= 21600:
            x_cell -= 21600
            x_idx += 1
        return self._actual_get_depth(f'{ns}{x_idx}.tif', x_cell, y_cell)

    def _actual_get_depth(self, cell_name: str, x_cell: int, y_cell: int) -> float:
        if cell_name not in self._ref_cache:
            self._ref_cache[cell_name] = tifffile.imread(self._gebco_dir / cell_name, aszarr=True)
        h = zarr.open(self._ref_cache[cell_name], 'r')
        return h[y_cell, x_cell]

    def _identify_cell(self, x: float, y: float) -> tuple[int, int]:
        x_cell = int(math.floor((x + 180) * 240))
        if y > 0:
            y_cell = int(math.floor((90 - y) * 240))
        else:
            y_cell = int(math.floor((-1 * y) * 240)) + 21600
        return x_cell, y_cell







