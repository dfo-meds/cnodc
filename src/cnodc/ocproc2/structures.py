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
            map_['message'],
            map_['_timestamp'],
            *map_['_source'],
            map_['_message_type']
        )


class AbstractValue:

    def __init__(self, metadata: t.Optional[dict] = None):
        self.metadata: ValueMap = ValueMap(metadata)
        self._value = None

    def to_mapping(self):
        raise NotImplementedError()

    def from_mapping(self, map_: t.Any):
        raise NotImplementedError()

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

    def __init__(self, value: SupportedValue = None, metadata: t.Optional[dict] = None):
        super().__init__(metadata)
        self._value = normalize_data_value(value)

    def __eq__(self, other):
        if isinstance(other, Value):
            return self._value == other and self.metadata == other.metadata
        elif isinstance(other, MultiValue):
            if len(other) == 1:
                return self.__eq__(other[0])
            return False
        else:
            return False

    def __str__(self):
        return str(self._value)

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
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


class MultiValue(AbstractValue):

    def __init__(self, values: t.Sequence[AbstractValue] = None, metadata: t.Optional[dict] = None):
        super().__init__(metadata)
        self._value = list(values) if values else []

    def __len__(self):
        return self._value

    def __getitem__(self, item):
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

    def values(self):
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


OCProcValue = t.Union[SupportedValue, AbstractValue]
DefaultValueDict = dict[str, OCProcValue]


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

    def get(self, parameter_code: str) -> t.Optional[AbstractValue]:
        if parameter_code in self._map:
            return self._map[parameter_code]
        return None

    def __setitem__(self, key: str, value: OCProcValue):
        self.set(key, value)

    def set(self,
            parameter_code: str,
            value: OCProcValue,
            metadata: t.Optional[DefaultValueDict] = None):
        value = value if isinstance(value, AbstractValue) else Value(value, metadata)
        self._map[parameter_code] = value

    def set_multiple(self,
                     parameter_code: str,
                     values: t.Sequence[OCProcValue],
                     values_metadata: t.Optional[t.Sequence[DefaultValueDict]] = None,
                     metadata: t.Optional[DefaultValueDict] = None
                     ):
        actual_values = []
        for i in range(0, len(values)):
            actual_values.append(
                values[i]
                if isinstance(values[i], AbstractValue) else
                Value(values[i], values_metadata[i] if values_metadata else None)
            )
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
                HistoryEntry.from_mapping(x) for x in map_['history']
            ]

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

    def _generate_compact_map(self):
        pass

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


