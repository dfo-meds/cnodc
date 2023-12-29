import typing as t
import datetime
import enum


SupportedValue = t.Union[
    None,
    str,
    float,
    int,
    list,
    set,
    tuple,
    dict,
    datetime.datetime,
    datetime.date
]


class MessageType(enum.Enum):

    INFO = "I"
    NOTE = "N"
    WARNING = "W"
    ERROR = "E"


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
                 message_type: str):
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
            '_message_type': self.message_type
        }

    @staticmethod
    def from_mapping(map_: dict):
        return HistoryEntry(
            map_['_message'],
            map_['_timestamp'],
            *map_['_source'],
            map_['_message_type']
        )


class QCMessage:

    def __init__(self,
                 code: str,
                 record_path: list[str],
                 ref_value: SupportedValue = None):
        self.code = code
        self.record_path = record_path
        self.ref_value = ref_value

    def to_mapping(self):
        return {
            '_code': self.code,
            '_path': self.record_path or [],
            '_ref': self.ref_value
        }

    @staticmethod
    def from_mapping(map_: dict):
        return QCMessage(
            map_['_code'],
            map_['_path'],
            map_['_ref'] if '_ref' in map_ else None
        )


class QCTestResult:

    def __init__(self,
                 test_name: str,
                 test_version: str,
                 test_date: t.Union[datetime.datetime, str],
                 result: str,
                 messages: list[QCMessage] = None,
                 notes: str = None,
                 is_stale: bool = False):
        self.test_name = test_name
        self.test_version = test_version
        self.test_date = test_date.isoformat() if isinstance(test_date, datetime.datetime) else test_date
        self.result = result
        self.messages = messages or []
        self.notes = notes
        self.is_stale = is_stale

    def passed(self):
        return self.result == 'PASS'

    def to_mapping(self):
        return {
            '_name': self.test_name,
            '_version': self.test_version,
            '_date': self.test_date,
            '_messages': [m.to_mapping() for m in self.messages],
            '_result': self.result,
            '_notes': self.notes,
            '_stale': self.is_stale
        }

    @staticmethod
    def from_mapping(map_: dict):
        return QCTestResult(
            map_['_name'],
            map_['_version'],
            map_['_date'],
            map_['_result'],
            [QCMessage.from_mapping(x) for x in map_['_messages']],
            map_['_notes'],
            map_['_stale'] if '_stale' in map_ else False
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

    def best_value(self) -> t.Any:
        return self._value

    def is_empty(self) -> bool:
        return self._value is None or self._value == ''

    def all_values(self) -> t.Iterable:
        yield self

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

    def best_value(self) -> t.Any:
        for x in self._value:
            bv = x.best_value()
            if bv is not None and bv != '':
                return bv
        return None

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

    def __delitem__(self, key):
        del self._map[key]

    def __iter__(self) -> t.Iterable[str]:
        yield from self._map.keys()

    def __bool__(self):
        return bool(self._map)

    def __getitem__(self, item: str):
        return self._map[item]

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
        self.qc_tests: list[QCTestResult] = []

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
                QCTestResult.from_mapping(x) for x in map_['_qc_tests']
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

    def latest_test_result(self, test_name: str, include_stale: bool = False) -> t.Optional[QCTestResult]:
        best = None
        for qcr in self.qc_tests:
            if qcr.test_name != test_name:
                continue
            if (not include_stale) and qcr.is_stale:
                continue
            if best is None or qcr.test_date > best.test_date:
                best = qcr
        return best

    def record_qc_test_passed(self,
                              test_name: str,
                              test_version: str,
                              notes: str = None):
        self.mark_test_results_stale(test_name)
        self.qc_tests.append(QCTestResult(
            test_name,
            test_version,
            datetime.datetime.now(datetime.timezone.utc),
            'PASS',
            None,
            notes
        ))

    def record_qc_test_failed(self,
                              test_name: str,
                              test_version: str,
                              messages: list[QCMessage],
                              notes: str = None):
        self.mark_test_results_stale(test_name)
        self.qc_tests.append(QCTestResult(
            test_name,
            test_version,
            datetime.datetime.now(datetime.timezone.utc),
            'FAIL',
            messages,
            notes
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
                          message_type: MessageType.NOTE):
        self.history.append(HistoryEntry(
            message,
            datetime.datetime.now(datetime.timezone.utc),
            source_name,
            source_version,
            source_instance,
            message_type.value
        ))

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


class RecordMap:

    def __init__(self):
        self.record_sets: dict[str, dict[int, RecordSet]] = {}

    def __getitem__(self, item):
        return self.record_sets[item]

    def __contains__(self, key):
        return key in self.record_sets

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
                self.record_sets[x][y] = RecordSet()
                self.record_sets[x][y].from_mapping(map_[x][y])

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



