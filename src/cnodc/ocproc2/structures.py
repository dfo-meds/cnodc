import decimal
import hashlib
import typing as t
import datetime
import enum

from uncertainties import ufloat

SupportedValue = t.Union[
    None,
    str,
    float,
    int,
    list,
    set,
    tuple,
    dict,
    bool,
    datetime.datetime,
    datetime.date
]


class MessageType(enum.Enum):

    INFO = "I"
    NOTE = "N"
    WARNING = "W"
    ERROR = "E"


class QCResult(enum.Enum):

    PASS = 'P'
    MANUAL_REVIEW = 'R'
    FAIL = 'F'
    SKIP = 'S'


def normalize_data_value(dv: t.Any):
    cls_name = dv.__class__.__name__
    if cls_name == 'str' or cls_name == 'float' or cls_name == 'int' or cls_name == 'bool' or cls_name == 'NoneType':
        return dv
    elif cls_name == 'datetime' or cls_name == 'date':
        return dv.isoformat()
    elif cls_name == 'list' or cls_name == 'set' or cls_name == 'tuple':
        return [normalize_data_value(x) for x in dv]
    elif cls_name == 'dict':
        return {
            str(x): normalize_data_value(dv[x])
            for x in dv
        }
    else:
        raise ValueError(f'Invalid value [{cls_name}]')


class HistoryEntry:

    def __init__(self,
                 message: str,
                 timestamp: t.Union[datetime.datetime, str],
                 source_name: str,
                 source_version: str,
                 source_instance: str,
                 message_type: MessageType):
        self.message = message
        self.timestamp = timestamp.isoformat() if isinstance(timestamp, datetime.datetime) else timestamp
        self.source_name = source_name
        self.source_version = source_version
        self.source_instance = source_instance
        self.message_type = message_type

    def to_mapping(self):
        return {
            '_message': self.message,
            '_timestamp': self.timestamp,
            '_source': (self.source_name, self.source_version, self.source_instance),
            '_message_type': self.message_type.value
        }

    def update_hash(self, h):
        h.update(self.message.encode('utf-8', 'replace'))
        h.update(self.timestamp.encode('utf-8', 'replace'))
        h.update(self.source_name.encode('utf-8', 'replace'))
        h.update(self.source_version.encode('utf-8', 'replace'))
        h.update(self.source_instance.encode('utf-8', 'replace'))
        h.update(self.message_type.value.encode('utf-8', 'replace'))

    @staticmethod
    def from_mapping(map_: dict):
        return HistoryEntry(
            map_['_message'],
            map_['_timestamp'],
            *map_['_source'],
            message_type=MessageType(map_['_message_type'])
        )


def normalize_qc_path(path: t.Union[None, str, list[str]]) -> str:
    if path is None:
        return ''
    if isinstance(path, list):
        path = '/'.join(path)
    path = path.strip('/')
    while '//' in path:
        path = path.replace('//', '/')
    return path


class QCMessage:

    def __init__(self,
                 code: str,
                 record_path: t.Union[str, list[str]],
                 ref_value: SupportedValue = None):
        self.code = code
        self.record_path = normalize_qc_path(record_path)
        self.ref_value = ref_value

    def update_hash(self, h):
        h.update(self.code.encode('utf-8', 'replace'))
        h.update(self.record_path.encode('utf-8', 'replace'))
        if self.ref_value is not None:
            h.update(str(self.ref_value).encode('utf-8', 'replace'))

    def to_mapping(self):
        return {
            '_code': self.code,
            '_path': self.record_path,
            '_ref': self.ref_value
        }

    @staticmethod
    def from_mapping(map_: dict):
        return QCMessage(
            map_['_code'],
            map_['_path'],
            map_['_ref'] if '_ref' in map_ else None
        )


class QCTestRunInfo:

    def __init__(self,
                 test_name: str,
                 test_version: str,
                 test_date: t.Union[datetime.datetime, str],
                 result: QCResult,
                 messages: list[QCMessage] = None,
                 notes: str = None,
                 is_stale: bool = False,
                 test_tags: t.Optional[list[str]] = None):
        self.test_name = test_name
        self.test_tags = test_tags or []
        self.test_version = test_version
        self.test_date = test_date.isoformat() if isinstance(test_date, datetime.datetime) else test_date
        self.result = result
        self.messages = messages or []
        self.notes = notes
        self.is_stale = is_stale

    def update_hash(self, h):
        h.update(self.test_name.encode('utf-8', 'replace'))
        if self.test_tags:
            h.update(str(self.test_tags).encode('utf-8', 'replace'))
        h.update(self.test_version.encode('utf-8', 'replace'))
        h.update(self.test_date.encode('utf-8', 'replace'))
        h.update(self.result.value.encode('utf-8', 'replace'))
        if self.notes is not None:
            h.update(self.notes.encode('utf-8', 'replace'))
        h.update(b'\x01' if self.is_stale else b'\x02')
        for m in self.messages:
            m.update_hash(h)

    def passed(self):
        return self.result == QCResult.PASS

    def to_mapping(self):
        return {
            '_name': self.test_name,
            '_version': self.test_version,
            '_date': self.test_date,
            '_messages': [m.to_mapping() for m in self.messages],
            '_result': self.result.value,
            '_notes': self.notes,
            '_stale': self.is_stale,
            '_tags': self.test_tags
        }

    @staticmethod
    def from_mapping(map_: dict):
        return QCTestRunInfo(
            map_['_name'],
            map_['_version'],
            map_['_date'],
            QCResult(map_['_result']),
            [QCMessage.from_mapping(x) for x in map_['_messages']],
            map_['_notes'],
            map_['_stale'] if '_stale' in map_ else False,
            map_['_tags'] if '_tags' in map_ else None
        )


class AbstractValue:

    def __init__(self, metadata: t.Optional[dict] = None, **kwargs):
        self.metadata: ValueMap = ValueMap(metadata)
        if kwargs:
            self.metadata.update(kwargs)
        self._value = None

    def __repr__(self):
        s = f'{self.__class__.__name__}({str(self)})'
        if self.metadata:
            s += "("
            s += ';'.join(f"{x}={repr(self.metadata[x])}" for x in self.metadata)
            s += ")"
        return s

    def find_child(self, path: list[str]):
        raise NotImplementedError

    def update_hash(self, h):
        raise NotImplementedError

    def is_empty(self) -> bool:
        raise NotImplementedError

    def is_numeric(self) -> bool:
        raise NotImplementedError

    def is_integer(self) -> bool:
        raise NotImplementedError

    def in_range(self, min_value: t.Optional[float] = None, max_value: t.Optional[float] = None) -> bool:
        raise NotImplementedError

    def to_mapping(self) -> dict:
        raise NotImplementedError

    def from_mapping(self, map_: t.Any):
        raise NotImplementedError

    def best_value(self) -> t.Any:
        raise NotImplementedError

    def is_iso_datetime(self) -> bool:
        raise NotImplementedError

    def all_values(self) -> t.Iterable:
        raise NotImplementedError

    def to_decimal(self) -> decimal.Decimal:
        raise NotImplementedError

    def to_float_with_uncertainty(self) -> ufloat:
        raise NotImplementedError

    def to_float(self) -> float:
        raise NotImplementedError

    def to_int(self) -> int:
        raise NotImplementedError

    def to_datetime(self) -> datetime.datetime:
        raise NotImplementedError

    def to_date(self) -> datetime.date:
        raise NotImplementedError

    def to_string(self) -> str:
        raise NotImplementedError

    @property
    def value(self):
        raise NotImplementedError

    @staticmethod
    def value_from_mapping(map_: t.Any):
        if isinstance(map_, list) or (isinstance(map_, dict) and '_value' not in map_):
            dv = MultiValue()
            dv.from_mapping(map_)
            return dv
        else:
            dv = Value()
            dv.from_mapping(map_)
            return dv


class Value(AbstractValue):

    def __init__(self,
                 value: SupportedValue = None,
                 metadata: t.Optional[dict] = None,
                 **kwargs):
        super().__init__(metadata, **kwargs)
        self._value = normalize_data_value(value)

    def __contains__(self, item):
        return False

    def __eq__(self, other):
        if isinstance(other, Value):
            return self._value == other._value and self.metadata == other.metadata
        elif isinstance(other, MultiValue):
            if len(other) == 1:
                return self.__eq__(other[0])
            return False
        else:
            return False

    def __str__(self):
        return str(self._value)

    def find_child(self, path: list[str]):
        if not path or path[0] == "0":
            return self
        elif path[0] == 'metadata':
            return self.metadata.find_child(path[1:])
        else:
            return None

    def best_value(self) -> t.Any:
        return self._value

    def is_empty(self) -> bool:
        return self._value is None or self._value == ''

    def all_values(self) -> t.Iterable:
        yield self

    def to_decimal(self) -> decimal.Decimal:
        return decimal.Decimal(self._value)

    def to_float_with_uncertainty(self) -> t.Union[float, ufloat]:
        if self.metadata.has_value('Uncertainty'):
            return ufloat(self.to_float(), self.metadata['Uncertainty'].to_float())
        return self.to_float()

    def to_float(self) -> float:
        return float(self._value)

    def update_hash(self, h):
        if self._value is None:
            h.update(b'\x00')
        else:
            h.update(str(self._value).encode('utf-8', 'replace'))
        self.metadata.update_hash(h)

    def to_int(self) -> int:
        return int(self._value)

    def to_datetime(self) -> datetime.datetime:
        return datetime.datetime.fromisoformat(self._value)

    def to_date(self) -> datetime.date:
        return datetime.date.fromisoformat(self._value)

    def to_string(self) -> str:
        return str(self._value)

    def is_iso_datetime(self) -> bool:
        try:
            _ = datetime.datetime.fromisoformat(self._value)
            return True
        except (ValueError, TypeError):
            return False

    def is_numeric(self) -> bool:
        if isinstance(self._value, bool):
            return False
        return isinstance(self._value, (int, float))

    def is_integer(self) -> bool:
        return isinstance(self._value, int) and not isinstance(self._value, bool)

    def in_range(self, min_value: t.Optional[float] = None, max_value: t.Optional[float] = None) -> bool:
        if min_value is not None and self._value < min_value:
            return False
        if max_value is not None and self._value > max_value:
            return False
        return True

    @property
    def value(self) -> SupportedValue:
        return self._value

    @value.setter
    def value(self, value: SupportedValue):
        self._value = normalize_data_value(value)

    def to_mapping(self):
        md = self.metadata.to_mapping()
        if not (md or isinstance(self._value, (list, dict))):
            return self._value
        else:
            map_ = {
                '_value': self._value
            }
            if md:
                map_['_metadata'] = md
            return map_

    def from_mapping(self, map_: t.Any):
        if isinstance(map_, dict):
            if '_metadata' in map_:
                self.metadata.from_mapping(map_['_metadata'])
            self._value = map_['_value']
        else:
            self._value = map_


OCProcValue = t.Union[SupportedValue, AbstractValue]
DefaultValueDict = dict[str, OCProcValue]


class MultiValue(AbstractValue):

    def __init__(self,
                 values: t.Sequence[OCProcValue] = None,
                 metadata: t.Optional[dict] = None,
                 **kwargs):
        super().__init__(metadata, **kwargs)
        self._value = [v if isinstance(v, AbstractValue) else Value(v) for v in values] if values else []

    def __str__(self):
        return '\n'.join(str(x) for x in self._value)

    def __len__(self) -> int:
        return len(self._value)

    def __getitem__(self, item) -> AbstractValue:
        return self._value[item]

    def __eq__(self, other):
        if isinstance(other, Value):
            if len(self._value) == 1:
                return other == self._value[0]
            return False
        elif isinstance(other, MultiValue):
            return len(other) == len(self) and all(other[x] == self[x] for x in range(0, len(self)))
        else:
            return False

    def find_child(self, path: list[str]):
        if not path:
            return self
        elif path[0] == 'metadata':
            return self.metadata.find_child(path[1:])
        elif path[0].isdigit():
            idx = int(path[0])
            if 0 <= idx < len(self._value):
                return self._value[idx]
            return None
        else:
            return None

    def update_hash(self, h):
        for v in self.values():
            v.update_hash(h)

    def best_value(self) -> t.Any:
        return self.first_non_empty().value

    def all_values(self) -> t.Iterable:
        for v in self._value:
            yield from v.all_values()

    def is_empty(self) -> bool:
        return all(x.is_empty() for x in self._value)

    def _broadcast_is_check(self, fn_name, *args, **kwargs):
        result = None
        for x in self._value:
            if x.is_empty():
                continue
            if not getattr(x, fn_name)(*args, **kwargs):
                return False
            result = True
        return result if result is not None else False

    def first_non_empty(self):
        for x in self._value:
            if not x.is_empty():
                return x
        return None

    def to_decimal(self) -> decimal.Decimal:
        return self.first_non_empty().to_decimal()

    def to_float_with_uncertainty(self) -> t.Union[float, ufloat]:
        return self.first_non_empty().to_float_with_uncertainty()

    def to_float(self) -> float:
        return self.first_non_empty().to_float()

    def to_int(self) -> int:
        return self.first_non_empty().to_int()

    def to_datetime(self) -> datetime.datetime:
        return self.first_non_empty().to_datetime()

    def to_date(self) -> datetime.date:
        return self.first_non_empty().to_date()

    def to_string(self) -> str:
        return self.first_non_empty().to_string()

    def is_numeric(self) -> bool:
        return self._broadcast_is_check('is_numeric')

    def in_range(self, min_value: t.Optional[float] = None, max_value: t.Optional[float] = None) -> bool:
        return self._broadcast_is_check('in_range', min_value, max_value)

    def is_iso_datetime(self) -> bool:
        return self._broadcast_is_check('is_iso_datetime')

    def values(self):
        return self._value

    @property
    def value(self):
        return self._value

    def append(self, value: AbstractValue):
        self._value.append(value)

    def to_mapping(self):
        md = self.metadata.to_mapping()
        if md:
            return {
                '_metadata': md,
                '_values': [v.to_mapping() for v in self._value]
            }
        else:
            return [v.to_mapping() for v in self._value]

    def from_mapping(self, map_: t.Any):
        if isinstance(map_, dict):
            if '_metadata' in map_:
                self.metadata.from_mapping(map_['_metadata'])
            self._value = [AbstractValue.value_from_mapping(v) for v in map_['values']]
        else:
            self._value = [AbstractValue.value_from_mapping(v) for v in map_]


class ValueMap:

    def __init__(self, defaults: t.Optional[DefaultValueDict] = None):
        self._map: dict[str, AbstractValue] = {}
        if defaults:
            self.update(defaults)

    def __contains__(self, item):
        return item in self._map

    def __eq__(self, other):
        if not isinstance(other, ValueMap):
            return False
        keys1 = list(self._map.keys())
        keys2 = list(other._map.keys())
        if keys1 != keys2:
            return False
        return all(self._map[k] == other._map[k] for k in keys1)

    def find_child(self, path: list[str]):
        if not path:
            return self
        elif path[0] in self._map:
            return self._map[path[0]].find_child(path[1:])
        else:
            return None

    def __delitem__(self, key):
        del self._map[key]

    def __iter__(self) -> t.Iterable[str]:
        yield from self._map.keys()

    def __bool__(self):
        return bool(self._map)

    def __getitem__(self, item: str):
        return self._map[item]

    def update_hash(self, h):
        for k in sorted(self._map.keys()):
            h.update(k.encode('utf-8', 'replace'))
            self._map[k].update_hash(h)

    def keys(self):
        return self._map.keys()

    def best_value(self, item, default=None):
        if item not in self._map:
            return default
        return self._map[item].best_value()

    def has_value(self, item):
        return item in self._map and not self._map[item].is_empty()

    def get(self, parameter_code: str) -> t.Optional[AbstractValue]:
        if parameter_code in self._map:
            return self._map[parameter_code]
        return None

    def __setitem__(self, key: str, value: OCProcValue):
        self.set(key, value)

    def set(self,
            parameter_code: str,
            value: OCProcValue,
            metadata: t.Optional[DefaultValueDict] = None,
            **kwargs):
        if not isinstance(value, AbstractValue):
            value = Value(value, metadata)
        elif metadata:
            value.metadata.update(metadata)
        if kwargs:
            value.metadata.update(kwargs)
        self._map[parameter_code] = value

    def set_multiple(self,
                     parameter_code: str,
                     values: t.Sequence[OCProcValue],
                     common_metadata: t.Optional[DefaultValueDict] = None,
                     specific_metadata: t.Optional[t.Sequence[DefaultValueDict]] = None,
                     metadata: t.Optional[DefaultValueDict] = None
                     ):
        actual_values = []
        for i in range(0, len(values)):
            value_metadata = {}
            if common_metadata:
                value_metadata.update(common_metadata)
            if specific_metadata:
                value_metadata.update(specific_metadata[i])
            if isinstance(values[i], AbstractValue):
                values[i].metadata.update(value_metadata)
                actual_values.append(values[i])
            else:
                actual_values.append(Value(values[i], value_metadata))
        self._map[parameter_code] = MultiValue(actual_values, metadata)

    def update(self, d: t.Optional[DefaultValueDict]):
        for key in d:
            self.set(key, d[key])

    def to_mapping(self):
        return {x: self._map[x].to_mapping() for x in self._map}

    def from_mapping(self, map_: dict):
        for x in map_:
            self._map[x] = AbstractValue.value_from_mapping(map_[x])


class DataRecord:

    def __init__(self):
        self.metadata = ValueMap()
        self.parameters = ValueMap()
        self.coordinates = ValueMap()
        self.subrecords = RecordMap()
        self.history: list[HistoryEntry] = []
        self.qc_tests: list[QCTestRunInfo] = []

    def find_child(self, object_path: t.Union[str, list[str]]):
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
        md = self.metadata.to_mapping()
        if md:
            map_['_metadata'] = md
        pm = self.parameters.to_mapping()
        if pm:
            map_['_parameters'] = pm
        cm = self.coordinates.to_mapping()
        if cm:
            map_['_coordinates'] = cm
        sm = self.subrecords.to_mapping()
        if sm:
            map_['_subrecords'] = sm
        if self.history:
            map_['_history'] = [h.to_mapping() for h in self.history]
        if self.qc_tests:
            map_['_qc_tests'] = [qc.to_mapping() for qc in self.qc_tests]
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
        if '_history' in map_:
            self.history = [
                HistoryEntry.from_mapping(x) for x in map_['_history']
            ]
        if '_qc_tests' in map_:
            self.qc_tests = [
                QCTestRunInfo.from_mapping(x) for x in map_['_qc_tests']
            ]

    def iter_subrecords(self, subrecord_type: str = None) -> t.Iterable:
        for record_type in self.subrecords:
            if subrecord_type is None or subrecord_type == record_type:
                for record_set_key in self.subrecords[record_type]:
                    yield from self.subrecords[record_type][record_set_key].records

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
        self.metadata.update_hash(h)
        self.parameters.update_hash(h)
        self.coordinates.update_hash(h)
        self.subrecords.update_hash(h)
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

    def __init__(self):
        self.metadata = ValueMap()
        self.records: list[DataRecord] = []

    def update_hash(self, h):
        self.metadata.update_hash(h)
        for r in self.records:
            r.update_hash(h)

    def to_mapping(self):
        md = self.metadata.to_mapping()
        if md:
            return {
                '_records': [x.to_mapping() for x in self.records],
                '_metadata': md
            }
        return [x.to_mapping() for x in self.records]

    def from_mapping(self, map_):
        if isinstance(map_, dict):
            if '_metadata' in map_:
                self.metadata.from_mapping(map_['_metadata'])
            map_ = map_['records']
        self.records = []
        for r in map_:
            record = DataRecord()
            record.from_mapping(r)
            self.records.append(record)

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

    def __init__(self):
        self.record_sets: dict[str, dict[int, RecordSet]] = {}

    def __iter__(self):
        return iter(self.record_sets)

    def __getitem__(self, item):
        return self.record_sets[item]

    def __contains__(self, key):
        return key in self.record_sets

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
                y: self.record_sets[x][y].to_mapping()
                for y in self.record_sets[x]
            }
            for x in self.record_sets
        }

    def from_mapping(self, map_):
        self.record_sets = {}
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

    def append_record_set(self, record_set_type: str, record_set_index: int, record: DataRecord):
        if record_set_type not in self.record_sets:
            self.record_sets[record_set_type] = {}
        if record_set_index not in self.record_sets[record_set_type]:
            self.record_sets[record_set_type][record_set_index] = RecordSet()
        self.record_sets[record_set_type][record_set_index].records.append(record)



