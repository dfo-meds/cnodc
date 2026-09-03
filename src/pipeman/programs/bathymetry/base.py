from medsutil.math import ScienceNumber


class BathymetryModel:

    def get_bathymetry(self, longitude: float, latitude: float) -> ScienceNumber | None:
        raise NotImplementedError
