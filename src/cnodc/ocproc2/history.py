"""OCPROC2 history and test results"""
import typing as t
import datetime
import enum
from cnodc.ocproc2.values import SupportedValue


class MessageType(enum.Enum):
    """Defines the type of message being stored"""

    INFO = "I"
    NOTE = "N"
    WARNING = "W"
    ERROR = "E"


class QCResult(enum.Enum):
    """Defines the outcome of the quality control test"""

    PASS = 'P'
    MANUAL_REVIEW = 'R'
    FAIL = 'F'
    SKIP = 'S'


class HistoryEntry:
    """An entry in the history of the record."""

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
        """Convert this history entry to a map."""
        return {
            '_message': self.message,
            '_timestamp': self.timestamp,
            '_source': (self.source_name, self.source_version, self.source_instance),
            '_message_type': self.message_type.value
        }

    def update_hash(self, h):
        """Update a hash with the unique values of this history entry."""
        h.update(self.message.encode('utf-8', 'replace'))
        h.update(self.timestamp.encode('utf-8', 'replace'))
        h.update(self.source_name.encode('utf-8', 'replace'))
        h.update(self.source_version.encode('utf-8', 'replace'))
        h.update(self.source_instance.encode('utf-8', 'replace'))
        h.update(self.message_type.value.encode('utf-8', 'replace'))

    @staticmethod
    def from_mapping(map_: dict):
        """Convert a map back to a history entry."""
        return HistoryEntry(
            map_['_message'],
            map_['_timestamp'],
            *map_['_source'],
            message_type=MessageType(map_['_message_type'])
        )


def normalize_qc_path(path: t.Union[None, str, list[str]]) -> str:
    """Normalize the path for a QC result."""
    if path is None:
        return ''
    if isinstance(path, list):
        path = '/'.join(path)
    path = path.strip('/')
    while '//' in path:
        path = path.replace('//', '/')
    return path


class QCMessage:
    """Records the failure of a QC test for an individual element."""

    def __init__(self,
                 code: str,
                 record_path: t.Union[str, list[str]],
                 ref_value: SupportedValue = None):
        self.code = code
        self.record_path = normalize_qc_path(record_path)
        self.ref_value = ref_value

    def update_hash(self, h):
        """Update a hash with the unique values of this message."""
        h.update(self.code.encode('utf-8', 'replace'))
        h.update(self.record_path.encode('utf-8', 'replace'))
        if self.ref_value is not None:
            h.update(str(self.ref_value).encode('utf-8', 'replace'))

    def to_mapping(self):
        """Convert this message to a map."""
        return {
            '_code': self.code,
            '_path': self.record_path,
            '_ref': self.ref_value
        }

    @staticmethod
    def from_mapping(map_: dict):
        """Rebuild the message from a map."""
        return QCMessage(
            map_['_code'],
            map_['_path'],
            map_['_ref'] if '_ref' in map_ else None
        )


class QCTestRunInfo:
    """Records the outcome of a QC test run."""

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

    def to_mapping(self):
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
    def from_mapping(map_: dict):
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

