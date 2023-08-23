import math
import typing as t
import importlib
from .exc import CNODCError


JsonEncodable = t.Union[None, bool, str, float, int, list, dict]

EARTH_RADIUS_KM = 6367.4445


def haversine_distance_km(lat1d, lat2d, long1d, long2d):
    lat1 = math.radians(lat1d)
    lat2 = math.radians(lat2d)
    long1 = math.radians(long1d)
    long2 = math.radians(long2d)
    a = math.pow(math.sin((lat2 - lat1) / 2), 2)
    b = math.pow(math.sin((long2 - long1) / 2), 2)
    return math.sqrt(a + (b * math.cos(lat1) * math.cos(lat2))) * 2 * EARTH_RADIUS_KM


class HaltInterrupt(KeyboardInterrupt):
    pass


class HaltFlag(t.Protocol):

    def check(self, raise_ex: bool = True) -> bool:
        raise NotImplementedError()


class DynamicClassLoadError(CNODCError):
    pass


def dynamic_class(cls_name):
    if "." not in cls_name:
        raise DynamicClassLoadError(f"cls_name should be in format package.class [actual {cls_name}]", "DCL", 1000)
    package_dot_pos = cls_name.rfind(".")
    package = cls_name[0:package_dot_pos]
    specific_cls_name = cls_name[package_dot_pos + 1:]
    try:
        mod = importlib.import_module(package)
        return getattr(mod, specific_cls_name)
    except ModuleNotFoundError as ex:
        raise DynamicClassLoadError(f"Package or module [{package}] not found", "DCL", 1001) from ex
    except AttributeError as ex:
        raise DynamicClassLoadError(f"Class [{specific_cls_name}] not found in [{package}]", "DCL", 1002) from ex
