"""Provides a core implementation of OCPROC2."""
from .operations import QCOperator, QCSetValue, QCAddHistory, QCSetWorkingQuality
from .structures import BaseRecord, DataRecord, RecordSet, RecordMap, ValueMap
from .values import MultiValue, AbstractValue, Value, ValueMap
from .history import HistoryEntry, QCResult, QCMessage
from .validation import OCProc2Ontology, OCProc2ElementInfo, OCProc2ChildRecordTypeInfo
