import typing as t

import medsutil.datadict as dd
from medsutil.exceptions import CodedError
from medsutil.ocproc2 import ParentRecord, AbstractElement, SingleElement, RecordSet, BaseRecord, MessageType, QCResult
from medsutil.ocproc2.history import ActionType, Organization
from medsutil.ocproc2.util import set_working_quality

if t.TYPE_CHECKING:
    from medsutil.ocproc2.util import SupportedValue


class RecordAction(dd.DataDictObject):
    source_name: str | None = dd.p_str()
    source_version: str | None = dd.p_str()
    source_instance: str | None = dd.p_str()
    organization: Organization = dd.p_enum(Organization)
    username: str | None = dd.p_str()

    @property
    def is_blocker(self) -> bool:
        return False

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def object(self) -> str:
        raise NotImplementedError

    @property
    def value(self) -> str:
        raise NotImplementedError

    def apply(self, record: ParentRecord):
        raise NotImplementedError

    def add_history_action(self,
                           record: ParentRecord,
                           message: str,
                           action_type: ActionType,
                           path: str | None = None):
        proc_id = self.source_instance or 'unknown'
        if self.username is not None:
            proc_id += f" [{self.username}]"
        record.add_history_action(
            message,
            self.source_name or 'unknown',
            self.source_version or 'unknown',
            proc_id,
            action_type,
            path,
            self.organization
        )

    def conflicts_with(self, action: RecordAction) -> bool:
        return False


class RetestRecord(RecordAction):
    qc_test_protocol: str
    qc_test_name: str

    @property
    def name(self) -> str:
        return "action.retest"

    @property
    def object(self) -> str:
        return "record"

    @property
    def value(self) -> str:
        return f"{self.qc_test_protocol}.{self.qc_test_name}"

    def apply(self, record: ParentRecord):
        record.mark_test_results_stale(self.qc_test_protocol, self.qc_test_name)

    def conflicts_with(self, action: RecordAction) -> bool:
        return isinstance(action, RetestRecord) and action.qc_test_name == self.qc_test_name and self.qc_test_protocol == action.qc_test_protocol


class RecordProcessed(RecordAction):

    @property
    def name(self) -> str:
        return "action.processed"

    @property
    def object(self) -> str:
        return "record"

    @property
    def value(self) -> str:
        return self.source_name or 'unknown'

    def apply(self, record: ParentRecord):
        self.add_history_action(
            record,
            "Processed",
            ActionType.PROCESS
        )


class AddHistoryEntry(RecordAction):
    message: str = dd.p_str()
    message_type: MessageType = dd.p_enum(MessageType, default=MessageType.NOTE)

    @property
    def name(self) -> str:
        return "action.add_history"

    @property
    def object(self) -> str:
        return "record"

    @property
    def value(self) -> str:
        return self.message

    def apply(self, record: ParentRecord):
        record.add_history_entry(
            self.message,
            self.source_name or 'unknown',
            self.source_version or 'unknown',
            self.source_instance or 'unknown',
            self.message_type
        )


class Blocker(RecordAction):

    @property
    def is_blocker(self) -> bool:
        return True

    def apply(self, record: ParentRecord):
        pass


class PlatformBlocker(Blocker):

    @property
    def name(self) -> str:
        return "action.require_platform"

    @property
    def object(self) -> str:
        return "record"

    @property
    def value(self) -> str:
        return ''

    def conflicts_with(self, action: RecordAction) -> bool:
        return isinstance(action, AssignPlatform)


class SetRelationships(RecordAction):
    relationships: dict[str, list[list[str | bool]]] = dd.p_dict()

    def apply(self, record: ParentRecord):
        if not self.relationships:
            if "CNODCRelationships" in record.metadata:
                del record.metadata["CNODCRelationships"]
        else:
            record.metadata["CNODCRelationships"] = self.relationships


class SetQualityCheck(RecordAction):
    check_no: int = dd.p_int()

    def apply(self, record: ParentRecord):
        check = record.metadata.best("CNODCQualityFlags", default=0, coerce=int)
        record.metadata["CNODCQualityFlags"] = check | self.check_no


class SetPlatformCandidates(RecordAction):
    platform_uuids: list[str] | None

    def apply(self, record: ParentRecord):
        if not self.platform_uuids:
            if 'CNODCPlatformCandidates' in record.metadata:
                del record.metadata["CNODCPlatformCandidates"]
        else:
            record.metadata["CNODCPlatformCandidates"] = self.platform_uuids


class AssignPlatform(RecordAction):
    test_protocol: str = dd.p_str()
    platform_uuid: str | None = dd.p_str()
    platform_type: str | None = dd.p_str()

    @property
    def name(self) -> str:
        return "action.set_platform"

    @property
    def object(self) -> str:
        return "record"

    @property
    def value(self) -> str:
        return self.platform_uuid or ''

    def apply(self, record: ParentRecord):
        record.metadata["CNODCPlatform"] = SingleElement(self.platform_uuid, Quality=SingleElement(1 if self.platform_uuid else 9, TestProtoccol=self.test_protocol))
        if 'CNODCPlatformCandidates' in record.metadata:
            del record.metadata["CNODCPlatformCandidates"]
        if self.platform_type is not None:
            record.metadata["CNODCPlatformType"] = self.platform_type
        self.add_history_action(
            record,
            f"Platform assigned",
            ActionType.CHANGE_PLATFORM,
            "metadata/CNODCPlatform"
        )

    def conflicts_with(self, action: RecordAction) -> bool:
        return isinstance(action, AssignPlatform)


class PathAction(RecordAction):
    path: str = dd.p_str()


    def conflicts_with(self, action: RecordAction) -> bool:
        return isinstance(action, PathAction) and action.path == self.path

class SetToEmpty(PathAction):

    @property
    def name(self) -> str:
        return "action.set_empty"

    @property
    def object(self) -> str:
        return self.path

    @property
    def value(self) -> str:
        return ""

    def apply(self, record: ParentRecord):
        element = record.find_child(self.path)
        if not isinstance(element, SingleElement):
            raise ValueError("Invalid element path")
        past_value = element.value
        element.value = None
        element.metadata.append_to("PreviousValue", past_value)
        element.metadata["WorkingQuality"] = 9
        self.add_history_action(
            record,
            f"Value removed",
            ActionType.CHANGE_VALUE,
            self.path
        )
        self.add_history_action(
            record,
            f"Quality flag changed to 9",
            ActionType.CHANGE_QUALITY,
            self.path
        )

class ChangeQualityAtLevelAndDeeper(PathAction):
    test_protocol: str = dd.p_str()
    other_paths: list[str] = dd.p_list(value_coerce=str)
    new_flag: int = dd.p_int()

    @property
    def name(self) -> str:
        return "action.set_quality_at_level_and_deeper"

    @property
    def object(self) -> str:
        return self.path

    @property
    def value(self) -> str:
        return str(self.new_flag)

    def apply(self, record: ParentRecord):
        work = [self.path, *self.other_paths]
        for p in work:
            element = record.find_child(p)
            if not isinstance(element, (AbstractElement, BaseRecord, RecordSet)):
                raise ValueError("Invalid element path")
            if set_working_quality(element, self.new_flag, self.test_protocol):
                self.add_history_action(
                    record,
                    f"Quality flag changed to {self.new_flag}",
                    ActionType.CHANGE_QUALITY,
                    self.path
                )
                # TODO: should we change this to a "set at and deeper history action"? I think this is more clear.

class ChangeQuality(PathAction):
    test_protocol: str = dd.p_str()
    new_flag: int = dd.p_int()

    @property
    def name(self) -> str:
        return "action.set_quality"

    @property
    def object(self) -> str:
        return self.path

    @property
    def value(self) -> str:
        return str(self.new_flag)

    def apply(self, record: ParentRecord):
        element = record.find_child(self.path)
        if not isinstance(element, (AbstractElement, BaseRecord, RecordSet)):
            raise ValueError("Invalid element path")
        if set_working_quality(element, self.new_flag, self.test_protocol):
            self.add_history_action(
                record,
                f"Quality flag changed to {self.new_flag}",
                ActionType.CHANGE_QUALITY,
                self.path
            )


class SetManualQCOutcome(RecordAction):
    qc_index: int = dd.p_int()
    actual_result: QCResult = dd.p_enum(QCResult)

    @property
    def name(self) -> str:
        return "action.set_outcome"

    @property
    def object(self) -> str:
        return f"QC Test #{self.qc_index}"

    @property
    def value(self) -> str:
        return self.actual_result.value

    def apply(self, record: ParentRecord):
        qc_test = record.qc_tests[self.qc_index]
        if qc_test.result is QCResult.MANUAL_REVIEW:
            qc_test.result = self.actual_result
            self.add_history_action(
                record,
                f"Manual review of QC test [{self.qc_index}] set to {self.actual_result}",
                ActionType.UPDATE_QC_RESULT
            )

    def conflicts_with(self, action: RecordAction) -> bool:
        return isinstance(action, SetManualQCOutcome) and action.qc_index == self.qc_index


class ChangeValue(PathAction):
    test_protocol: str = dd.p_str()
    path: str = dd.p_str()
    new_value: SupportedValue = dd.p_any()

    @property
    def name(self) -> str:
        return "action.set_value"

    @property
    def object(self) -> str:
        return self.path

    @property
    def value(self) -> str:
        return str(self.new_value)

    def apply(self, record: ParentRecord):
        element = record.find_child(self.path)
        if not isinstance(element, (SingleElement, None)):
            raise ValueError(f"Invalid path: {self.path}")
        if element is not None:
            previous = element.value
            element.value = self.new_value
            element.metadata["WorkingQuality"] = SingleElement(5, TestProtocol=self.test_protocol)
            element.metadata.append_to("PreviousValue", previous)
        else:
            record.set(self.path, self.new_value, WorkingQuality=SingleElement(5, TestProtocol=self.test_protocol))
        self.add_history_action(
            record,
            f"Value changed",
            ActionType.CHANGE_VALUE,
            self.path
        )
