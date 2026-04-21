"""Structural elements of OCPROC2."""
import hashlib
import typing as t
import datetime

from medsutil.ocproc2.elements import ElementMap, SupportedValueOrElement, AbstractElement, SingleElement, DefaultValueDict, AnyElementExport, MetadataDict
from medsutil.ocproc2.history import HistoryEntry, QCTestRunInfo, QCResult, QCMessage, MessageType
from medsutil.lazy_load import LazyLoadList
import medsutil.awaretime as awaretime
import medsutil.types as ct

type FindType = BaseRecord | RecordSet | AbstractElement | ElementMap | RecordMap | dict[int, RecordSet] | None


class BaseRecord:

    __slots__ = ('_metadata', '_parameters', '_coordinates', '_subrecords')

    def __init__(self):
        self._metadata: t.Optional[ElementMap] = None
        self._parameters: t.Optional[ElementMap] = None
        self._coordinates: t.Optional[ElementMap] = None
        self._subrecords: t.Optional[RecordMap] = None

    @property
    def metadata(self) -> ElementMap:
        if self._metadata is None:
            self._metadata = ElementMap()
        return t.cast(ElementMap, self._metadata)

    @property
    def parameters(self) -> ElementMap:
        if self._parameters is None:
            self._parameters = ElementMap()
        return t.cast(ElementMap, self._parameters)

    @property
    def coordinates(self) -> ElementMap:
        if self._coordinates is None:
            self._coordinates = ElementMap()
        return t.cast(ElementMap, self._coordinates)

    @property
    def subrecords(self) -> RecordMap:
        if self._subrecords is None:
            self._subrecords = RecordMap()
        return t.cast(RecordMap, self._subrecords)

    def set(self, element_full_name: str, value: SupportedValueOrElement, metadata: t.Optional[DefaultValueDict] = None, **kwargs):
        child_parent, element_name = self._find_element_map(element_full_name)
        child_parent.set(element_name, value, metadata, **kwargs)

    def append_to(self, element_full_name: str, value: SupportedValueOrElement, metadata: t.Optional[DefaultValueDict] = None, **kwargs):
        child_parent, element_name = self._find_element_map(element_full_name)
        child_parent.append_to(element_name, value, metadata, **kwargs)

    def set_many(self, element_full_name: str, values: t.Sequence[SupportedValueOrElement], common_metadata: t.Optional[DefaultValueDict] = None, specific_metadata: t.Sequence[t.Optional[DefaultValueDict]] = None, **kwargs):
        child_parent, element_name = self._find_element_map(element_full_name)
        child_parent.set_many(element_name, values, common_metadata, specific_metadata, **kwargs)

    def set_element(self, element_full_name: str, value: AbstractElement):
        child_parent, element_name = self._find_element_map(element_full_name)
        child_parent.set_element(element_name, value)

    def append_element_to(self, element_full_name: str, value: AbstractElement):
        child_parent, element_name = self._find_element_map(element_full_name)
        child_parent.append_element_to(element_name, value)

    def set_many_elements(self, element_full_name: str, values: t.Iterable[SingleElement], metadata: t.Optional[DefaultValueDict] = None):
        child_parent, element_name = self._find_element_map(element_full_name)
        child_parent.set_many_elements(element_name, values, metadata)

    def _find_element_map(self, element_full_name: str) -> tuple[ElementMap, str]:
        pieces = element_full_name.split('/')
        element_name = pieces.pop(-1)
        child_parent = self.find_child(pieces)
        if child_parent is not None and isinstance(child_parent, ElementMap):
            return child_parent, element_name
        else:
            raise ValueError(f'invalid element path: [{element_full_name}]: [{child_parent}]')

    def find_child(self, object_path: t.Union[str, list[str]]) -> FindType:
        opath = [x for x in object_path.split('/') if x != ''] if isinstance(object_path, str) else object_path
        if not opath:
            return self
        first_element = opath.pop(0)
        if first_element == 'metadata':
            return self.metadata.find_child(opath)
        elif first_element == 'parameters':
            return self.parameters.find_child(opath)
        elif first_element == 'coordinates':
            return self.coordinates.find_child(opath)
        elif first_element == 'subrecords':
            return self.subrecords.find_child(opath)
        else:
            return None

    def to_mapping(self) -> BaseExport:
        map_: BaseExport = {}
        md = self._metadata.to_mapping() if self._metadata is not None else None
        if md:
            map_['_metadata'] = md
        pm = self._parameters.to_mapping() if self._parameters is not None else None
        if pm:
            map_['_parameters'] = pm
        cm = self._coordinates.to_mapping() if self._coordinates is not None else None
        if cm:
            map_['_coordinates'] = cm
        sm = self._subrecords.to_mapping() if self._subrecords is not None else None
        if sm:
            map_['_subrecords'] = sm
        return map_

    def from_mapping(self, map_: BaseExport):
        if '_metadata' in map_:
            self.metadata.from_mapping(map_['_metadata'])
        if '_parameters' in map_:
            self.parameters.from_mapping(map_['_parameters'])
        if '_coordinates' in map_:
            self.coordinates.from_mapping(map_['_coordinates'])
        if '_subrecords' in map_:
            self.subrecords.from_mapping(map_['_subrecords'])

    def iter_subrecords(self, subrecord_type: str = None) -> t.Iterable[BaseRecord]:
        if self._subrecords is not None:
            yield from self._subrecords.iter_subrecords(subrecord_type)

    def update_hash(self, h: ct.SupportsHashUpdate):
        if self._metadata is not None:
            self._metadata.update_hash(h)
        if self._parameters is not None:
            self._parameters.update_hash(h)
        if self._coordinates is not None:
            self._coordinates.update_hash(h)
        if self._subrecords is not None:
            self._subrecords.update_hash(h)

    @classmethod
    def build_from_mapping(cls, map_: BaseExport | ParentExport):
        r = cls()
        r.from_mapping(map_)
        return r


class ChildRecord(BaseRecord):
    pass


class ParentRecord(BaseRecord):

    __slots__ = ('_metadata', '_parameters', '_coordinates', '_subrecords', 'history', 'qc_tests')

    def __init__(self):
        super().__init__()
        self.history: LazyLoadList[HistoryEntry] = LazyLoadList(HistoryEntry.from_mapping)
        self.qc_tests: LazyLoadList[QCTestRunInfo] = LazyLoadList(QCTestRunInfo.from_mapping)

    def to_mapping(self) -> ParentExport:
        map_: ParentExport = {}
        map_.update(super().to_mapping())
        if self.history is not None and self.history:
            map_['_history'] = self.history.to_mapping()
        if self.qc_tests is not None and self.qc_tests:
            map_['_qc_tests'] = self.qc_tests.to_mapping()
        return map_

    def from_mapping(self, map_: ParentExport):
        super().from_mapping(map_)
        if '_history' in map_:
            self.history.from_mapping(map_['_history'])
        if '_qc_tests' in map_:
            self.qc_tests.from_mapping(map_['_qc_tests'])

    def test_already_run(self, test_name: str, include_stale: bool = False) -> bool:
        if include_stale:
            return any(x.test_name == test_name for x in self.qc_tests)
        else:
            return any(x.test_name == test_name and not x.is_stale for x in self.qc_tests)

    def latest_test_result(self, test_name: str, include_stale: bool = False) -> t.Optional[QCTestRunInfo]:
        best = None
        for qcr in self.qc_tests:
            if qcr.test_name != test_name:
                continue
            if (not include_stale) and qcr.is_stale:
                continue
            if best is None or qcr.test_date >= best.test_date:
                best = qcr
        return best

    def generate_hash(self) -> str:
        h = hashlib.sha1()
        self.update_hash(t.cast(ct.SupportsHashUpdate, t.cast(object, h)))
        return h.hexdigest()

    def update_hash(self, h: ct.SupportsHashUpdate):
        super().update_hash(h)
        for his in self.history:
            his.update_hash(h)
        for q in self.qc_tests:
            q.update_hash(h)

    def record_qc_test_result(self,
                              test_name: str,
                              test_version: str,
                              outcome: QCResult,
                              messages: list[QCMessage],
                              notes: str = None,
                              test_tags: t.Optional[list[str]] = None,
                              test_time: t.Optional[datetime.datetime] = None):
        self.mark_test_results_stale(test_name)
        self.qc_tests.append(QCTestRunInfo(
            test_name,
            test_version,
            test_time or awaretime.utc_now(),
            outcome,
            messages,
            notes,
            test_tags=test_tags,
        ))

    def mark_test_results_stale(self, test_name: str):
        for qct in self.qc_tests:
            if qct.test_name == test_name:
                qct.is_stale = True

    def add_history_entry(self,
                          message: str,
                          source_name: str,
                          source_version: str,
                          source_instance: str,
                          message_type: MessageType = MessageType.NOTE,
                          change_time: t.Optional[datetime.datetime] = None):
        self.history.append(HistoryEntry(
            message,
            change_time or awaretime.utc_now(),
            source_name,
            source_version,
            source_instance,
            message_type
        ))

    def record_note(self,
                    message: str,
                    source_name: str,
                    source_version: str,
                    source_instance: str):
        self.add_history_entry(message, source_name, source_version, source_instance, MessageType.NOTE)

    def report_error(self,
                     message: str,
                     source_name: str,
                     source_version: str,
                     source_instance: str):
        self.add_history_entry(message, source_name, source_version, source_instance, MessageType.ERROR)

    def report_warning(self,
                       message: str,
                       source_name: str,
                       source_version: str,
                       source_instance: str):
        self.add_history_entry(message, source_name, source_version, source_instance, MessageType.WARNING)

    def internal_note(self,
                      message: str,
                      source_name: str,
                      source_version: str,
                      source_instance: str):
        self.add_history_entry(message, source_name, source_version, source_instance, MessageType.INFO)


class RecordSet:

    __slots__ = ('_metadata', 'records')

    def __init__(self):
        self._metadata = None
        self.records: LazyLoadList[ChildRecord] = LazyLoadList(ChildRecord.build_from_mapping)

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = ElementMap()
        return self._metadata

    def update_hash(self, h: ct.SupportsHashUpdate):
        if self._metadata is not None:
            self.metadata.update_hash(h)
        for r in self.records:
            r.update_hash(h)

    def to_mapping(self) -> RecordSetExport:
        if self.metadata:
            return {
                '_records': self.records.to_mapping(),
                '_metadata': self.metadata.to_mapping()
            }
        else:
            return {
                '_records': self.records.to_mapping()
            }

    def from_mapping(self, map_: RecordSetExport):
        try:
            self.records.from_mapping(map_['_records'])
            if '_metadata' in map_:
                self.metadata.from_mapping(map_['_metadata'])
        except TypeError:
            # probably a list from the older OCPROC2 spec
            self.records.from_mapping(map_)

    def find_child(self, path: list[str]) -> FindType:
        if not path:
            return self
        first_element = path.pop(0)
        if first_element == 'metadata':
            return self.metadata.find_child(path)
        try:
            idx = int(first_element)
            if idx < 0 or idx >= len(self.records):
                return None
            return self.records[idx].find_child(path)
        except (ValueError, TypeError) as ex:
            return None


class RecordMap:

    __slots__ = ('record_sets', )

    def __init__(self):
        self.record_sets: dict[str, dict[int, RecordSet]] = {}

    def __getitem__(self, item: str) -> dict[int, RecordSet]:
        return self.record_sets[item]

    def __contains__(self, key: str):
        return key in self.record_sets

    def iter_subrecords(self, srt: t.Optional[str] = None) -> t.Iterable[BaseRecord]:
        if srt is not None:
            try:
                for rs_idx in self.record_sets[srt]:
                    yield from self.record_sets[srt][rs_idx].records
            except KeyError:
                pass
        else:
            for _srt in self.record_sets.keys():
                for rs_idx in self.record_sets[_srt]:
                    yield from self.record_sets[_srt][rs_idx].records

    def find_child(self, path: list[str]) -> FindType:
        if not path:
            return self
        first_element = path.pop(0)
        if first_element not in self.record_sets:
            return None
        ret = self.record_sets[first_element]
        if path:
            try:
                idx = int(path.pop(0))
                if idx not in ret:
                    return None
                return ret[idx].find_child(path)
            except (TypeError, ValueError):
                return None
        return ret

    def update_hash(self, h: ct.SupportsHashUpdate):
        for srt in self.record_sets:
            h.update(srt.encode('utf-8', 'replace'))
            for idx in self.record_sets[srt]:
                h.update(str(idx).encode('utf-8', 'replace'))
                self.record_sets[srt][idx].update_hash(h)

    def new_recordset(self, record_type: str) -> RecordSet:
        if record_type not in self.record_sets:
            self.record_sets[record_type] = {}
        idx = 0
        while idx in self.record_sets[record_type]:
            idx += 1
        self.record_sets[record_type][idx] = RecordSet()
        return self.record_sets[record_type][idx]

    def to_mapping(self) -> dict[str, dict[str, RecordSetExport]]:
        mapping: dict[str, dict[str, RecordSetExport]] = {
            x: {
                str(y): self.record_sets[x][y].to_mapping()
                for y in self.record_sets[x]
            }
            for x in self.record_sets
        }
        return mapping

    def from_mapping(self, map_):
        for x in map_:
            self.record_sets[x] = {}
            for y in map_[x]:
                self.record_sets[x][int(y)] = RecordSet()
                self.record_sets[x][int(y)].from_mapping(map_[x][y])

    def get(self, record_set_type: str, record_set_index: int) -> RecordSet | None:
        if record_set_type in self.record_sets and record_set_index in self.record_sets[record_set_type]:
            return self.record_sets[record_set_type][record_set_index]
        return None

    def set(self, record_set_type: str, record_set_index: int, record_set: RecordSet):
        if record_set_type not in self.record_sets:
            self.record_sets[record_set_type] = {}
        self.record_sets[record_set_type][record_set_index] = record_set

    def append_to_record_set(self, record_set_type: str, record_set_index: int, record: ChildRecord):
        if record_set_type not in self.record_sets:
            self.record_sets[record_set_type] = {}
        if record_set_index not in self.record_sets[record_set_type]:
            self.record_sets[record_set_type][record_set_index] = RecordSet()
        self.record_sets[record_set_type][record_set_index].records.append(record)


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
