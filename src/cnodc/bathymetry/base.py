import abc
import typing as t
from uncertainties import UFloat


class BathymetryModel(abc.ABC):

    def __init__(self, ref_name: str):
        self.ref_name = ref_name

    @abc.abstractmethod
    def water_depth(self,
                    x: t.Union[float, UFloat],
                    y: t.Union[float, UFloat]) -> t.Union[float, UFloat]:
        raise NotImplementedError

