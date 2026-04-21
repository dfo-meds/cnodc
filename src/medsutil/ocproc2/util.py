import decimal
import typing as t
import datetime
import medsutil.types as ct


def normalize_ocproc_path(path: t.Union[None, str, t.Iterable[str]]) -> str:
    """Normalize the path for a QC result."""
    if path is None:
        return ''
    actual_path = ('/'.join(path) if not isinstance(path, str) else path).strip()
    while '//' in actual_path:
        actual_path = actual_path.replace('//', '/')
    return actual_path.strip('/')

type _SupportedStorage = None | str | float | int | bool
type SupportedStorage = _SupportedStorage | list[SupportedStorage] | dict[str, SupportedStorage]
type _SupportedValue = None | str | float | int | bool | datetime.date | decimal.Decimal
type SupportedValue = _SupportedValue | t.Iterable[SupportedValue] | t.Mapping[ct.SupportsString, SupportedValue]


