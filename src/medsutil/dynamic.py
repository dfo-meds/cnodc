import importlib
import types
import typing as t

from medsutil.exceptions import CodedError

class DynamicObjectLoadError(CodedError):  CODE_SPACE = 'DOBJ'


def dynamic_name(__obj: object | type | types.FunctionType | types.ModuleType) -> str:
    if not isinstance(__obj, (type, types.FunctionType, types.ModuleType)):
        __obj = type(__obj)
    module = __obj.__module__
    cls_name = __obj.__name__
    if module != 'builtins':
        return module + "." + cls_name
    return cls_name


def dynamic_object(cls_name: str) -> t.Any:
    if cls_name is None or "." not in cls_name:
        raise DynamicObjectLoadError(f"Object name should be in format package.class [got: {cls_name}]", 1000)
    package_dot_pos = cls_name.rfind(".")
    package = cls_name[0:package_dot_pos]
    specific_cls_name = cls_name[package_dot_pos + 1:]
    try:
        mod = importlib.import_module(package)
        return getattr(mod, specific_cls_name)
    except ModuleNotFoundError as ex:
        raise DynamicObjectLoadError(f"[module: {package}] not found", 1001) from ex
    except AttributeError as ex:
        raise DynamicObjectLoadError(f"[object: {specific_cls_name}] not found in [module: {package}]", 1002) from ex

