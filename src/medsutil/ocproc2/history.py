"""OCPROC2 history and test results"""
import medsutil.types as ct
import typing as t
import datetime
import enum

from medsutil.ocproc2 import normalize_ocproc_path
from medsutil.ocproc2.util import SupportedStorage


class MessageType(enum.Enum):
    """Defines the type of message being stored"""

    INFO = "I"
    NOTE = "N"
    WARNING = "W"
    ERROR = "E"


class QCResult(enum.Enum):
    """Defines the outcome of the quality control test"""

    PASS = 'P'      # nosec: B105 # is not a password
    MANUAL_REVIEW = 'R'
    FAIL = 'F'
    SKIP = 'S'


class HistoryEntry:
    """An entry in the history of the record."""

    __slots__ = ('message', 'timestamp', 'source_name', 'source_version', 'source_instance', 'message_type')

    class Export(t.TypedDict):
        _message: str
        _timestamp: str
        _source: list[str]
        _message_type: str

    def __init__(self,
                 message: str,
                 timestamp: datetime.datetime | str,
                 source_name: str,
                 source_version: str,
                 source_instance: str,
                 message_type: MessageType):
        self.message: str = message
        self.timestamp: str = timestamp.isoformat() if isinstance(timestamp, datetime.datetime) else timestamp
        self.source_name: str = source_name
        self.source_version: str = source_version
        self.source_instance: str = source_instance
        self.message_type: MessageType = message_type

    def to_mapping(self) -> Export:
        """Convert this history entry to a map."""
        return {
            '_message': self.message,
            '_timestamp': self.timestamp,
            '_source': [self.source_name, self.source_version, self.source_instance],
            '_message_type': self.message_type.value
        }

    def update_hash(self, h: ct.SupportsHashUpdate):
        """Update a hash with the unique values of this history entry."""
        h.update(self.message.encode('utf-8', 'replace'))
        h.update(self.timestamp.encode('utf-8', 'replace'))
        h.update(self.source_name.encode('utf-8', 'replace'))
        h.update(self.source_version.encode('utf-8', 'replace'))
        h.update(self.source_instance.encode('utf-8', 'replace'))
        h.update(self.message_type.value.encode('utf-8', 'replace'))

    @staticmethod
    def from_mapping(map_: Export) -> HistoryEntry:
        """Convert a map back to a history entry."""
        return HistoryEntry(
            map_['_message'],
            map_['_timestamp'],
            *map_['_source'],
            message_type=MessageType(map_['_message_type'])
        )


class QCMessage:
    """Records the failure of a QC test for an individual element."""

    __slots__ = ('code', 'record_path', 'ref_value')

    class Export(t.TypedDict, total=False):
        _code: t.Required[str]
        _path: t.Required[str]
        _ref: SupportedStorage | None

    def __init__(self,
                 code: str,
                 record_path: str | list[str],
                 ref_value: SupportedStorage = None):
        self.code = code
        self.record_path = normalize_ocproc_path(record_path)
        self.ref_value = ref_value

    def update_hash(self, h):
        """Update a hash with the unique values of this message."""
        h.update(self.code.encode('utf-8', 'replace'))
        h.update(self.record_path.encode('utf-8', 'replace'))
        if self.ref_value is not None:
            h.update(str(self.ref_value).encode('utf-8', 'replace'))

    def to_mapping(self) -> Export:
        """Convert this message to a map."""
        return {
            '_code': self.code,
            '_path': self.record_path,
            '_ref': self.ref_value
        }

    @staticmethod
    def from_mapping(map_: Export) -> QCMessage:
        """Rebuild the message from a map."""
        return QCMessage(
            map_['_code'],
            map_['_path'],
            map_['_ref'] if '_ref' in map_ else None
        )


class QCTestRunInfo:
    """Records the outcome of a QC test run."""

    __slots__ = ('test_name', 'test_tags', 'test_version', 'test_date', 'result', 'messages', 'notes', 'is_stale')

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

    class Export(t.TypedDict, total=False):
        _name: t.Required[str]
        _version: t.Required[str]
        _date: t.Required[str]
        _messages: t.Required[list[QCMessage.Export]]
        _result: t.Required[str]
        _notes: t.Required[str | None]
        _stale: bool
        _tags: list[str] | None

    def update_hash(self, h: ct.SupportsHashUpdate):
        """Update a hash with the unique values for this test run."""
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

    def to_mapping(self) -> Export:
        """Convert the QC test run to a map."""
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
    def from_mapping(map_: Export):
        """Rebuild the QC test run from a map."""
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

