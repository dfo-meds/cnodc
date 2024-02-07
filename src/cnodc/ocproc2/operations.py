import datetime
import typing as t
import cnodc.ocproc2.structures as ocproc2


class QCOperator:

    def __init__(self, type_: str, children: list = None):
        self._type = type_
        self._children = children or []

    def add_child(self, child):
        self._children.append(child)

    @property
    def name(self):
        raise NotImplementedError

    @property
    def object(self):
        raise NotImplementedError

    @property
    def value(self):
        raise NotImplementedError

    def to_map(self) -> dict:
        map_ = {
            '_type': self._type,
        }
        if self._children:
            map_['children'] = [x.to_map() for x in self._children]
        self._extend_map(map_)
        return map_

    def _extend_map(self, map_):
        pass

    def apply(self, record: ocproc2.DataRecord, working_record):
        self._apply(record, working_record)
        for child in self._children:
            child.apply(record, working_record)

    def _apply(self, record: ocproc2.DataRecord, working_record):
        pass

    @staticmethod
    def from_map(map_: dict):
        if '_type' not in map_:
            raise ValueError('missing type')
        mt = map_['_type']
        kwargs = {
            'children': [QCOperator.from_map(x) for x in map_['children']] if 'children' in map_ else None
        }
        if mt == 'set_value':
            return QCSetValue._from_map(map_, kwargs)
        elif mt == 'add_history':
            return QCAddHistory._from_map(map_, kwargs)
        elif mt == 'set_flag':
            return QCSetWorkingQuality._from_map(map_, kwargs)
        raise ValueError(f'invalid operator type: {mt}')


class QCAddHistory(QCOperator):

    def __init__(self,
                 message: str,
                 source_name: str,
                 source_version: str,
                 source_instance: str,
                 message_type: str,
                 change_time: t.Optional[datetime.datetime] = None,
                 **kwargs):
        super().__init__(type_='history', **kwargs)
        self._message = message
        self._datetime = change_time or datetime.datetime.now(datetime.timezone.utc)
        self._name = source_name
        self._version = source_version
        self._instance = source_instance
        self._type = message_type

    @property
    def name(self):
        return 'HISTORY'

    @property
    def object(self):
        return ''

    @property
    def value(self):
        return self._message

    def _extend_map(self, map_):
        map_.update({
            'message': self._message,
            'name': self._name,
            'version': self._version,
            'instance': self._instance,
            'type': self._type,
            'change_time': self._datetime.isoformat()
        })

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
    def _from_map(map_: dict, kwargs: dict):
        return QCAddHistory(
            map_['message'],
            map_['name'],
            map_['version'],
            map_['instance'],
            map_['type'],
            datetime.datetime.fromisoformat(map_['change_time']),
            **kwargs
        )


class QCSetValue(QCOperator):

    def __init__(self,
                 value_path: str,
                 new_value,
                 change_time: t.Optional[datetime.datetime] = None,
                 **kwargs):
        if 'type_' not in kwargs:
            kwargs['type_'] = 'set_value'
        super().__init__(**kwargs)
        self._value_path = ocproc2.normalize_qc_path(value_path)
        self._new_value = new_value
        self._change_time = change_time or datetime.datetime.now(datetime.timezone.utc)

    @property
    def name(self):
        return 'SET'

    @property
    def object(self):
        return self._value_path

    @property
    def value(self):
        return self._new_value

    def _get_path(self):
        return self._value_path.split('/')

    def apply(self, record: ocproc2.DataRecord, working_record):
        path = self._get_path()
        v: ocproc2.Value = record.find_child(path)
        if isinstance(v, ocproc2.Value):
            v.value = self._new_value
            return
        parent = record.find_child(path[:-1])
        if isinstance(parent, ocproc2.ValueMap):
            parent[path[-1]] = self._new_value
            return
        raise ValueError('cannot find a value to set')

    def _extend_map(self, map_):
        map_.update({
            'path': self._value_path,
            'value': self._new_value,
        })

    @classmethod
    def _from_map(cls, map_: dict, kwargs):
        return cls(
            map_['path'],
            map_['value'],
            **kwargs
        )


class QCSetWorkingQuality(QCSetValue):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, type_='set_flag')

    @property
    def name(self):
        return 'FLAG'

    def _get_path(self):
        path = self._value_path.split('/')
        path.extend(['metadata', 'WorkingQuality'])
        return path
