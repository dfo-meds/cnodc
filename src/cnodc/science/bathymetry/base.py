import abc
import typing as t
from cnodc.science.amath import AnyNumber


class BathymetryModel(abc.ABC):

    def __init__(self, ref_name: str):
        self.ref_name = ref_name

    @abc.abstractmethod
    def water_depth(self, x: AnyNumber, y: AnyNumber) -> t.Optional[AnyNumber]:
        raise NotImplementedError

