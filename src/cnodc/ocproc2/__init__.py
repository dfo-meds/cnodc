"""Provides a core implementation of OCPROC2."""
from .operations import QCOperator, QCSetValue, QCAddHistory, QCSetWorkingQuality
from .structures import BaseRecord, DataRecord, RecordSet, RecordMap, ElementMap
from .elements import MultiElementElement, AbstractElement, SingleElement, ElementMap
from .history import HistoryEntry, QCResult, QCMessage
from .validation import OCProc2Ontology, OCProc2ElementInfo, OCProc2ChildRecordTypeInfo
