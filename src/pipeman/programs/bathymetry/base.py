import typing as t

from medsutil.math import ScienceNumber


class BathymetryModel(t.Protocol):
    def get_bathymetry(self, longitude: float, latitude: float) -> ScienceNumber | None:
        raise NotImplementedError
