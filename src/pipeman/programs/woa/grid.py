import gzip
import math
import pathlib
import struct

from zirconium import ApplicationConfig

from medsutil.fastgrid import FastGeoGrid
from medsutil.math import ScienceNumber

from autoinject import injector, auto


class WorldOceanAtlasOneDegree(FastGeoGrid):

    # depths are interpolated at these depths
    _Z_VALUES = [0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100,125,150,175,200,225,250,275,300,325,
                      350,375,400,425,450,475,500,550,600,650,700,750,800,850,900,950,1000,1050,1100,1150,1200,1250,
                      1300,1350,1400,1450,1500,1550,1600,1650,1700,1750,1800,1850,1900,1950,2000,2100,2200,2300,2400,
                      2500,2600,2700,2800,2900,3000,3100,3200,3300,3400,3500,3600,3700,3800,3900,4000,4100,4200,4300,
                      4400,4500,4600,4700,4800,4900,5000,5100,5200,5300,5400,5500]



    def __init__(self, file: pathlib.Path, units: str | None = None):
        # we'll make bins that surround them since we use basically [an, an+1] to determine the correct value
        # that means the value at 0 will be used for 0 to 2.5 m
        # the value at 5 will be used 2.5 m to 7.5 m
        # etc ...
        self.Z_VALUES = [
            0,
            *(
                # 2.5, 7.5, 12.5, ..., 112.5, 137.5, ... 3650, 3750, ... 5450 (5550 is the cap lets say, specified below)
                (self._Z_VALUES[x - 1] + self._Z_VALUES[x]) / 2
                for x in range(1, len(self._Z_VALUES))
            )
        ]
        super().__init__(
            file=file,
            start_x=-180,
            start_y=-78,
            x_increment=1,
            y_increment=1,
            end_x=180,
            end_y=90,
            end_z=5550,
            z_values=self.Z_VALUES,
            data_size_bytes=16,
        )
        self._units = units

    def get_value(self, longitude: float, latitude: float, depth: float) -> ScienceNumber | None:
        coords = self.get_grid_coordinates(longitude, latitude, depth)
        if coords is None:
            return None
        byte_value = self.get_bytes_value(coords)
        values = struct.unpack("<dd", byte_value)
        if math.isnan(values[0]):
            return None
        return ScienceNumber.from_float(values[0], values[1] if not math.isnan(values[1]) else 0, units=self._units)

    @staticmethod
    def build_atlas_file(output: pathlib.Path,
                         mean_csv_file: pathlib.Path,
                         error_csv_file: pathlib.Path):
        atlas = WorldOceanAtlasOneDegree(output)
        data: dict[tuple[int, int, int], list[float | None]] = {}

        for idx, file_path in enumerate((mean_csv_file, error_csv_file)):
            opener = gzip.open if file_path.name.endswith(".gz") else open
            with opener(file_path, "rb") as h_mean:
                for line in h_mean.readlines():
                    line = line.decode("utf-8").strip()
                    if line.startswith("#"):
                        continue
                    row = line.split(",")
                    y_coord = atlas.get_latitude_coordinate(float(row[0]))
                    if y_coord is None:
                        continue
                    x_coord = atlas.get_longitude_coordinate(float(row[1]))
                    if x_coord is None:
                        continue
                    for depth_idx in range(2, len(row)):
                        if row[depth_idx]:
                            coords = (x_coord, y_coord, depth_idx - 2)
                            if coords not in data:
                                data[coords] = [math.nan, math.nan]
                            data[coords][idx] = float(row[depth_idx])

        def _atlas_value(coords: tuple[int, int, int]) -> bytes:
            if coords not in data:
                values = [math.nan, math.nan]
            else:
                values = data[coords]
            return struct.pack("<dd", values[0], values[1])
        atlas.build_file(_atlas_value)

    @staticmethod
    @injector.inject
    def build_from_file(variable: str, time_period: str, config: ApplicationConfig = auto()):
        base_dir = config.as_path("references", "world_ocean_atlas")
        if base_dir is None:
            raise ValueError("World Ocean Atlas directory not configured")
        units = None
        if variable == "temp":
            units = "degrees_C"
        elif variable == "psal":
            units = "psu"
        return WorldOceanAtlasOneDegree(
            file=base_dir / f"{variable}.{time_period}.fastgrid",
            units=units
        )
