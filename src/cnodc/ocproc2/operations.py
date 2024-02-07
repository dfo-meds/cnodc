import datetime
import typing as t
import cnodc.ocproc2.structures as ocproc2


class QCOperator:

    def __init__(self):
        pass

    def to_map(self) -> dict:
        raise NotImplementedError

    def apply(self, record: ocproc2.DataRecord, working_record):
        pass

    @staticmethod
    def from_map(map_: dict):
        if '_type' not in map_:
            raise ValueError('missing type')
        mt = map_['_type']
        if mt == 'set_value':
            return QCSetValue.from_map(map_)
        elif mt == 'add_history':
            return QCAddHistory.from_map(map_)
        raise ValueError(f'invalid operator type: {mt}')


class QCAddHistory(QCOperator):

    def __init__(self,
                 message: str,
                 source_name: str,
                 source_version: str,
                 source_instance: str,
                 message_type: str,
                 change_time: t.Optional[datetime.datetime] = None):
        super().__init__()
        self._message = message
        self._datetime = change_time or datetime.datetime.now(datetime.timezone.utc)
        self._name = source_name
        self._version = source_version
        self._instance = source_instance
        self._type = message_type

    def to_map(self) -> dict:
        return {
            '_type': 'add_history',
            'message': self._message,
            'name': self._name,
            'version': self._version,
            'instance': self._instance,
            'type': self._type,
            'change_time': self._datetime.isoformat()
        }

    def apply(self, record: ocproc2.DataRecord, working_record):
        record.add_history_entry(
            message=self._message,
            source_name=self._name,
            source_version=self._version,
            source_instance=self._instance,
            message_type=ocproc2.MessageType(self._type),
            change_time=self._datetime
        )

    @staticmethod
    def from_map(map_: dict):
        return QCAddHistory(
            map_['message'],
            map_['name'],
            map_['version'],
            map_['instance'],
            map_['type'],
            datetime.datetime.fromisoformat(map_['change_time'])
        )


class QCSetValue(QCOperator):

    def __init__(self,
                 value_path: str,
                 new_value):
        super().__init__()
        self._value_path = ocproc2.normalize_qc_path(value_path)
        self._new_value = new_value

    def apply(self, record: ocproc2.DataRecord, working_record):
        path = self._value_path.split('/')
        v: ocproc2.Value = record.find_child(path)
        if isinstance(v, ocproc2.Value):
            v.value = self._new_value
            return
        parent = record.find_child(path[:-1])
        if isinstance(parent, ocproc2.ValueMap):
            parent[path[-1]] = self._new_value
            return
        raise ValueError('cannot find a value to set')

    def to_map(self) -> dict:
        return {
            '_type': 'set_value',
            'path': self._value_path,
            'value': self._new_value,
        }

    @staticmethod
    def from_map(map_: dict):
        return QCSetValue(
            map_['path'],
            map_['value']
        )
