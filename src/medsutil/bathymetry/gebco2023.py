import decimal
import itertools
import math
import pathlib
import statistics

import zarr

from .base import BathymetryModel
import zirconium as zr
from autoinject import injector
import typing as t
import tifffile
import tifffile.zarr as tzarr
import medsutil.math as amath


class GEBCO2023BathymetryModel(BathymetryModel):

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self,
                 tiff_dir: str | pathlib.Path,
                 uncertainty: float | int | str = 150,
                 use_only_depths: bool = True):
        super().__init__("gebco2023")
        self._gebco_dir = pathlib.Path(tiff_dir) if not isinstance(tiff_dir, pathlib.Path) else tiff_dir
        self._ref_cache: dict[str, tzarr.ZarrTiffStore | tzarr.ZarrFileSequenceStore] = {}
        self._gebco_error = float(uncertainty)
        self._use_only_depths = use_only_depths

    def close(self):
        for x in self._ref_cache:
            self._ref_cache[x].close()
        self._ref_cache = {}

    def water_depth(self, x: amath.AnyNumber, y: amath.AnyNumber) -> amath.AnyNumber | None:
        min_x, max_x = amath.probable_range(x)
        min_y, max_y = amath.probable_range(y)
        x_cell_min, y_cell_max = self._identify_cell(min_x, min_y)
        x_cell_max, y_cell_min = self._identify_cell(max_x, max_y)
        depths = []
        altitudes = []
        for xc in range(x_cell_min, x_cell_max + 1):
            for yc in range(y_cell_min, y_cell_max + 1):
                depth = self._get_depth(xc, yc)
                if depth > 0:
                    altitudes.append(depth)
                else:
                    depths.append(depth)
        # TODO: It's worth noting GEBCO depths vs soundings are +/- 150 m accuracy
        # TODO: We should flag this somehow?
        # Note: we use only depths when any of the possible cells are depths
        # because its far more likely the buoy is properly positioned than not
        # this behaviour can be disabled.
        if self._use_only_depths and depths:
            return statistics.mean(depths)
        elif altitudes or depths:
            return statistics.mean(itertools.chain(depths, altitudes))
        return None

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
        h = zarr.open(self._ref_cache[cell_name], mode='r')
        return t.cast(float, h[y_cell, x_cell])

    def _identify_cell(self, x: amath.BasicNumber, y: amath.BasicNumber) -> tuple[int, int]:
        x_cell = int(math.floor((x + 180) * 240))
        if y > 0:
            y_cell = int(math.floor((90 - y) * 240))
        else:
            y_cell = int(math.floor((-1 * y) * 240)) + 21600
        return x_cell, y_cell







