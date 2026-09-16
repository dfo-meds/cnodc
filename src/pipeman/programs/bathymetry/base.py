from medsutil.math import ScienceNumber
import typing as t


class BathymetryModel(t.Protocol):

    def get_bathymetry(self, longitude: float, latitude: float) -> ScienceNumber | None:
        raise NotImplementedError
