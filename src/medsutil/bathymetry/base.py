import abc
from medsutil.math import AnyNumber


class BathymetryModel(abc.ABC):

    def __init__(self, ref_name: str):
        self.ref_name = ref_name

    def close(self):
        ...

    @abc.abstractmethod
    def water_depth(self, x: AnyNumber, y: AnyNumber) -> AnyNumber | None:
        raise NotImplementedError

