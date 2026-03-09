import importlib

from cnodc.util.exceptions import DynamicObjectLoadError


def dynamic_object(cls_name):
    if cls_name is None or "." not in cls_name:
        raise DynamicObjectLoadError(f"cls_name should be in format package.class [actual {cls_name}]", "DOBJ", 1000)
    package_dot_pos = cls_name.rfind(".")
    package = cls_name[0:package_dot_pos]
    specific_cls_name = cls_name[package_dot_pos + 1:]
    try:
        mod = importlib.import_module(package)
        return getattr(mod, specific_cls_name)
    except ModuleNotFoundError as ex:
        raise DynamicObjectLoadError(f"Package or module [{package}] not found", "DOBJ", 1001) from ex
    except AttributeError as ex:
        raise DynamicObjectLoadError(f"Object [{specific_cls_name}] not found in [{package}]", "DOBJ", 1002) from ex

