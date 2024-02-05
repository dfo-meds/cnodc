import datetime
import importlib


def clean_for_json(data):
    if isinstance(data, dict):
        return {
            x: clean_for_json(data[x]) for x in data
        }
    elif isinstance(data, (set, list, tuple)):
        return [clean_for_json(x) for x in data]
    elif isinstance(data, (datetime.datetime, datetime.date)):
        return data.isoformat()
    else:
        return data


def vlq_encode(number: int) -> bytearray:
    result = bytearray()
    while number >= 0b10000000:
        bits = number & 0b01111111
        number >>= 7
        result.append(bits | 0b10000000)
    result.append(number)
    return result


def vlq_decode(bytes_: bytes) -> tuple[int, int]:
    total = 0
    shift = 0
    pos = 0
    while pos < len(bytes_):
        total += (bytes_[pos] & 0b01111111) << shift
        shift += 7
        if not bytes_[pos] & 0b10000000:
            break
        pos += 1
    return total, pos + 1


class TranslatableException(Exception):

    def __init__(self, key, **kwargs):
        self.key = key
        self.kwargs = kwargs

    @property
    def message(self):
        import cnodc_app.translations as i18n
        return i18n.get_text(self.key, **self.kwargs)

    def __str__(self):
        return self.message


def dynamic_object(cls_name):
    if "." not in cls_name:
        raise ValueError(f"cls_name should be in format package.class [actual {cls_name}]")
    package_dot_pos = cls_name.rfind(".")
    package = cls_name[0:package_dot_pos]
    specific_cls_name = cls_name[package_dot_pos + 1:]
    try:
        mod = importlib.import_module(package)
        return getattr(mod, specific_cls_name)
    except ModuleNotFoundError as ex:
        raise ValueError(f"Package or module [{package}] not found") from ex
    except AttributeError as ex:
        raise ValueError(f"Class [{specific_cls_name}] not found in [{package}]") from ex
