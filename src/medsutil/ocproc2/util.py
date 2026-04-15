import decimal
import typing as t
import datetime
import medsutil.types as ct
from medsutil.ocproc2 import HistoryEntry, QCTestRunInfo
from medsutil.ocproc2.elements import AnyElementExport, MetadataDict


def normalize_ocproc_path(path: t.Union[None, str, t.Iterable[str]]) -> str:
    """Normalize the path for a QC result."""
    if path is None:
        return ''
    actual_path = ('/'.join(path) if not isinstance(path, str) else path).strip()
    while '//' in actual_path:
        actual_path = actual_path.replace('//', '/')
    return actual_path

type _SupportedStorage = None | str | float | int | bool
type SupportedStorage = _SupportedStorage | list[SupportedStorage] | dict[str, SupportedStorage]
type _SupportedValue = None | str | float | int | bool | datetime.date | decimal.Decimal
type SupportedValue = _SupportedValue | t.Iterable[SupportedValue] | t.Mapping[ct.SupportsString, SupportedValue]


class BaseExport(t.TypedDict):
    _metadata: t.NotRequired[dict[str, AnyElementExport]]
    _coordinates: t.NotRequired[dict[str, AnyElementExport]]
    _parameters: t.NotRequired[dict[str, AnyElementExport]]
    _subrecords: t.NotRequired[dict[str, dict[str, RecordSetExport]]]


class ParentExport(BaseExport):
    _history: t.NotRequired[list[HistoryEntry.Export]]
    _qc_tests: t.NotRequired[list[QCTestRunInfo.Export]]


class RecordSetExport(t.TypedDict):
    _records: list[BaseExport]
    _metadata: t.NotRequired[MetadataDict]
