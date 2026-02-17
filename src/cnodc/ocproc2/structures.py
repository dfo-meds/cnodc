"""Structural elements of OCPROC2."""
from __future__ import annotations
import hashlib
import typing as t
import datetime
from cnodc.ocproc2.elements import ElementMap, DefaultValueDict, OCProcValue
from cnodc.ocproc2.history import HistoryEntry, QCTestRunInfo, QCResult, QCMessage, MessageType
from cnodc.ocproc2.lazy_load import LazyLoadList


class BaseRecord:

    __slots__ = ('_metadata', '_parameters', '_coordinates', '_subrecords')

    def __init__(self):
        self._metadata: t.Optional[ElementMap] = None
        self._parameters: t.Optional[ElementMap] = None
        self._coordinates: t.Optional[ElementMap] = None
        self._subrecords = None

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = ElementMap()
        return self._metadata

    @property
    def parameters(self):
        if self._parameters is None:
            self._parameters = ElementMap()
        return self._parameters

    @property
    def coordinates(self):
        if self._coordinates is None:
            self._coordinates = ElementMap()
        return self._coordinates

    @property
    def subrecords(self):
        if self._subrecords is None:
            self._subrecords = RecordMap()
        return self._subrecords

    def set_element(self, element_full_name: str, value: OCProcValue, metadata: t.Optional[DefaultValueDict] = None, **kwargs):
        child_parent, element_name = self._find_element_map(element_full_name)
        child_parent.set_element(element_name, value, metadata, **kwargs)

    def add_element(self, element_full_name: str, value: OCProcValue, metadata: t.Optional[DefaultValueDict] = None, **kwargs):
        child_parent, element_name = self._find_element_map(element_full_name)
        child_parent.add_element(element_full_name, value, metadata, **kwargs)

    def set_multiple(self, element_full_name: str, values: t.Sequence[OCProcValue], common_metadata: t.Optional[DefaultValueDict] = None, specific_metadata: t.Sequence[t.Optional[DefaultValueDict]] = None):
        child_parent, element_name = self._find_element_map(element_full_name)
        child_parent.set_multiple(element_name, values, common_metadata, specific_metadata)

    def _find_element_map(self, element_full_name):
        pieces = element_full_name.split('/')
        child_parent = self.find_child(pieces[:-1])
        if child_parent is not None and isinstance(child_parent, ElementMap):
            return child_parent, pieces[-1]
        else:
            raise ValueError(f'invalid element path: [{element_full_name}]')

    def find_child(self, object_path: t.Union[str, list[str]]) -> t.Optional[t.Union[BaseRecord, ElementMap, RecordSet]]:
        if not object_path:
            return self
        if isinstance(object_path, str):
            object_path = [x for x in object_path.split('/') if x != '']
        if object_path[0] == 'metadata':
            return self.metadata.find_child(object_path[1:])
        elif object_path[0] == 'parameters':
            return self.parameters.find_child(object_path[1:])
        elif object_path[0] == 'coordinates':
            return self.coordinates.find_child(object_path[1:])
        elif object_path[0] == 'subrecords':
            return self.subrecords.find_child(object_path[1:])
        else:
            return None

    def to_mapping(self):
        map_ = {}
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

    def from_mapping(self, map_: dict):
        if '_metadata' in map_:
            self.metadata.from_mapping(map_['_metadata'])
        if '_parameters' in map_:
            self.parameters.from_mapping(map_['_parameters'])
        if '_coordinates' in map_:
            self.coordinates.from_mapping(map_['_coordinates'])
        if '_subrecords' in map_:
            self.subrecords.from_mapping(map_['_subrecords'])

    def iter_subrecords(self, subrecord_type: str = None) -> t.Iterable:
        if self._subrecords is not None:
            yield from self._subrecords.iter_subrecords(subrecord_type)

    def update_hash(self, h):
        if self._metadata is not None:
            self._metadata.update_hash(h)
        if self._parameters is not None:
            self._parameters.update_hash(h)
        if self._coordinates is not None:
            self._coordinates.update_hash(h)
        if self._subrecords is not None:
            self._subrecords.update_hash(h)


class ChildRecord(BaseRecord):

    __slots__ = ('_metadata', '_parameters', '_coordinates', '_subrecords')

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = ElementMap()
        return self._metadata

    @property
    def parameters(self):
        if self._parameters is None:
            self._parameters = ElementMap()
        return self._parameters

    @property
    def coordinates(self):
        if self._coordinates is None:
            self._coordinates = ElementMap()
        return self._coordinates

    @property
    def subrecords(self):
        if self._subrecords is None:
            self._subrecords = RecordMap()
        return self._subrecords

    @staticmethod
    def build_from_mapping(map_: dict):
        r = ChildRecord()
        r.from_mapping(map_)
        return r


class ParentRecord(BaseRecord):

    __slots__ = ('_metadata', '_parameters', '_coordinates', '_subrecords', 'history', 'qc_tests')

    def __init__(self):
        super().__init__()
        self.history: LazyLoadList[HistoryEntry] = LazyLoadList(HistoryEntry.from_mapping)
        self.qc_tests: LazyLoadList[QCTestRunInfo] = LazyLoadList(QCTestRunInfo.from_mapping)

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = ElementMap()
        return self._metadata

    @property
    def parameters(self):
        if self._parameters is None:
            self._parameters = ElementMap()
        return self._parameters

    @property
    def coordinates(self):
        if self._coordinates is None:
            self._coordinates = ElementMap()
        return self._coordinates

    @property
    def subrecords(self):
        if self._subrecords is None:
            self._subrecords = RecordMap()
        return self._subrecords

    def to_mapping(self):
        map_ = super().to_mapping()
        if self.history is not None and self.history:
            map_['_history'] = self.history.to_mapping()
        if self.qc_tests is not None and self.qc_tests:
            map_['_qc_tests'] = self.qc_tests.to_mapping()
        return map_

    def from_mapping(self, map_: dict):
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
            if best is None or qcr.test_date > best.test_date:
                best = qcr
        return best

    def generate_hash(self) -> str:
        h = hashlib.sha1()
        self.update_hash(h)
        return h.hexdigest()

    def update_hash(self, h):
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
                              test_tags: t.Optional[list[str]] = None):
        self.mark_test_results_stale(test_name)
        self.qc_tests.append(QCTestRunInfo(
            test_name,
            test_version,
            datetime.datetime.now(datetime.timezone.utc),
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
                          message_type: MessageType.NOTE,
                          change_time: t.Optional[datetime.datetime] = None):
        self.history.append(HistoryEntry(
            message,
            change_time or datetime.datetime.now(datetime.timezone.utc),
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

    def update_hash(self, h):
        if self._metadata is not None:
            self.metadata.update_hash(h)
        for r in self.records:
            r.update_hash(h)

    def to_mapping(self):
        md = self._metadata.to_mapping() if self._metadata is not None else None
        if md:
            return {
                '_records': self.records.to_mapping(),
                '_metadata': md
            }
        else:
            return {
                '_records': self.records.to_mapping()
            }

    def from_mapping(self, map_):
        if isinstance(map_, (tuple, list)):
            self.records.from_mapping(map_)
        else:
            self.records.from_mapping(map_['_records'])
            if '_metadata' in map_:
                self.metadata.from_mapping(map_['_metadata'])

    def find_child(self, path: list[str]):
        if not path:
            return self
        if path[0] == 'metadata':
            return self.metadata.find_child(path[1:])
        if not path[0].isdigit():
            return None
        idx = int(path[0])
        if idx < 0 or idx >= len(self.records):
            return None
        return self.records[idx].find_child(path[1:])


class RecordMap:

    __slots__ = ('record_sets', )

    def __init__(self):
        self.record_sets: dict[str, dict[int, RecordSet]] = {}

    def __iter__(self):
        return iter(self.record_sets)

    def __getitem__(self, item):
        return self.record_sets[item]

    def __contains__(self, key):
        return key in self.record_sets

    def iter_subrecords(self, srt: t.Optional[str] = None):
        if srt is not None:
            try:
                for rs_idx in self.record_sets[srt]:
                    yield from self.record_sets[srt][rs_idx].records
            except KeyError:
                pass
        else:
            for srt in self.record_sets:
                for rs_idx in self.record_sets[srt]:
                    yield from self.record_sets[srt][rs_idx].records

    def find_child(self, path: list[str]):
        if not path:
            return self
        if path[0] not in self.record_sets:
            return None
        if len(path) < 2:
            return self.record_sets[path[0]]
        if not path[1].isdigit():
            return None
        idx = int(path[1])
        if idx not in self.record_sets[path[0]]:
            return None
        return self.record_sets[path[0]][int(path[1])].find_child(path[2:])

    def update_hash(self, h):
        for srt in self.record_sets:
            h.update(srt.encode('utf-8', 'replace'))
            for idx in self.record_sets[srt]:
                h.update(str(idx).encode('utf-8', 'replace'))
                self.record_sets[srt][idx].update_hash(h)

    def new_recordset(self, record_type: str):
        if record_type not in self.record_sets:
            self.record_sets[record_type] = {}
        idx = 0
        while idx in self.record_sets[record_type]:
            idx += 1
        self.record_sets[record_type][idx] = RecordSet()
        return self.record_sets[record_type][idx]

    def to_mapping(self):
        return {
            x: {
                str(y): self.record_sets[x][y].to_mapping()
                for y in self.record_sets[x]
            }
            for x in self.record_sets
        }

    def from_mapping(self, map_):
        for x in map_:
            self.record_sets[x] = {}
            for y in map_[x]:
                self.record_sets[x][int(y)] = RecordSet()
                self.record_sets[x][int(y)].from_mapping(map_[x][y])

    def get(self, record_set_type: str, record_set_index: int):
        if record_set_type in self.record_sets and record_set_index in self.record_sets[record_set_type]:
            return self.record_sets[record_set_type][record_set_index]
        return None

    def set(self, record_set_type: str, record_set_index: int, record_set: RecordSet):
        if record_set not in self.record_sets:
            self.record_sets[record_set_type] = {}
        self.record_sets[record_set_type][record_set_index] = record_set

    def append_to_record_set(self, record_set_type: str, record_set_index: int, record: ChildRecord):
        if record_set_type not in self.record_sets:
            self.record_sets[record_set_type] = {}
        if record_set_index not in self.record_sets[record_set_type]:
            self.record_sets[record_set_type][record_set_index] = RecordSet()
        self.record_sets[record_set_type][record_set_index].records.append(record)
