import math
import pathlib
import struct

from medsutil.fastgrid import FastGeoGrid
from medsutil.math import ScienceNumber


class GreatLakesBathymetry(FastGeoGrid):

    def __init__(self, file: pathlib.Path):
        super().__init__(
            file=file,
            z_values=[0],
            end_z=9999,

            start_x=-93,
            end_x=-76,
            x_increment=(3, 3600),

            start_y=41,
            end_y=49.03,
            y_increment=(3, 3600),

            data_size_bytes=8
        )
        self._glb_range = 1 / math.sqrt(3)  # m - over most years, the lakes vary within +/- 1 m which gives us this sigma

    def get_bathymetry(self, longitude: float, latitude: float) -> ScienceNumber | None:
        coords = self.get_grid_coordinates(longitude, latitude, 0)
        if coords is None:
            return None
        bytes_value = self.get_bytes_value(coords)
        depth = struct.unpack("<d", bytes_value)[0]
        if math.isnan(depth):
            return None
        # note, we convert to depth positive here to align with how we usually handle depth
        return ScienceNumber.from_float(depth * -1, self._glb_range)

    @staticmethod
    def build_from_glb(output_file: pathlib.Path,
                       glb_tiffs: list[pathlib.Path]):
        import rasterio
        from rasterio.transform import AffineTransformer
        import numpy as np
        open_tiffs: list[tuple[rasterio.DatasetReader, np.ndarray, AffineTransformer]] = []
        try:
            for glb_tif in glb_tiffs:
                print(f"source file: {glb_tif}")
                raster = rasterio.open(glb_tif)
                open_tiffs.append((raster, raster.read(1), AffineTransformer(raster.transform)))

            bathymetry = GreatLakesBathymetry(output_file)
            def _get_value(coords: tuple[int, int, int]) -> bytes:
                depth = math.nan
                longitude = bathymetry.get_longitude(coords[0])
                if longitude is not None:
                    latitude = bathymetry.get_latitude(coords[1])
                    if latitude is not None:
                        for _, raster_array, transformer in open_tiffs:
                            offset_y, offset_x = transformer.rowcol(longitude, latitude)
                            try:
                                depth_check = raster_array[0, offset_y, offset_x]
                                if depth_check < 0:
                                    depth = depth_check
                                    break
                            except IndexError:
                                continue
                return struct.pack("<d", depth)
            bathymetry.build_file(_get_value)
        finally:
            for open_tiff, _, _ in open_tiffs:
                if not open_tiff.closed:
                    open_tiff.close()
            del open_tiffs
