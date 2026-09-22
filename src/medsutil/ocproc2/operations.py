import typing as t

import medsutil.datadict as dd
from medsutil.exceptions import CodedError
from medsutil.ocproc2 import ParentRecord, AbstractElement, SingleElement, RecordSet, BaseRecord, MessageType, QCResult
from medsutil.ocproc2.history import ActionType, Organization
from medsutil.ocproc2.util import set_working_quality

if t.TYPE_CHECKING:
    from medsutil.ocproc2.util import SupportedValue


class OCProc2ActionError(CodedError): CODE_SPACE = "OCPROC2-ACTION"


class OCProc2Action(dd.DataDictObject):

    ACTION_TYPES: dict[str, type] | None = None

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
        return getattr(self, "ACTION_NAME")

    @property
    def object(self) -> str:
        raise NotImplementedError

    @property
    def value(self) -> str:
        return ''

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

    def conflicts_with(self, action: OCProc2Action) -> bool:
        return False

    def export_for_ocproc2(self) -> dict[str, t.Any]:
        export: dict[str, t.Any] = {
            "_action_type_": self.name,
        }
        for prop in self._datadict_props().values():
            export[prop.managed_name] = prop.sanitize(getattr(self, prop.property_name))
        return export

    @staticmethod
    def import_from_ocproc2(map_: dict[str, t.Any]) -> OCProc2Action:
        action_types = OCProc2Action.ACTION_TYPES or {}
        action_type = str(map_.get("_action_type_", ''))
        if action_type not in action_types:
            raise OCProc2ActionError(f"Invalid action type: {action_type}")
        return t.cast(OCProc2Action, action_types[action_type](**{x: y for x, y in map_.items() if x != '_action_type_'}))

    @staticmethod
    def register_type[T](action_type: type[T]) -> type[T]:
        if not hasattr(action_type, "ACTION_NAME"):
            raise TypeError("Must have ACTION_NAME")
        if OCProc2Action.ACTION_TYPES is None:
            OCProc2Action.ACTION_TYPES = {
                str(getattr(action_type, "ACTION_NAME")): action_type
            }
        else:
            OCProc2Action.ACTION_TYPES[str(getattr(action_type, "ACTION_NAME"))] = action_type
        return action_type


class RecordAction(OCProc2Action):

    @property
    def object(self) -> str:
        return "record"


@OCProc2Action.register_type
class RetestRecord(RecordAction):
    ACTION_NAME = "retest"

    qc_test_protocol: str = dd.p_str()
    qc_test_name: str = dd.p_str()

    @property
    def value(self) -> str:
        return f"{self.qc_test_protocol}.{self.qc_test_name}"

    def apply(self, record: ParentRecord):
        record.mark_test_results_stale(self.qc_test_protocol, self.qc_test_name)

    def conflicts_with(self, action: OCProc2Action) -> bool:
        return isinstance(action, RetestRecord) and action.qc_test_name == self.qc_test_name and self.qc_test_protocol == action.qc_test_protocol


@OCProc2Action.register_type
class RecordProcessed(RecordAction):
    ACTION_NAME = "processed"

    @property
    def value(self) -> str:
        return self.source_name or 'unknown'

    def apply(self, record: ParentRecord):
        self.add_history_action(
            record,
            "Processed",
            ActionType.PROCESS
        )


@OCProc2Action.register_type
class AddHistoryEntry(RecordAction):
    ACTION_NAME = "add_history"

    message: str = dd.p_str()
    message_type: MessageType = dd.p_enum(MessageType, default=MessageType.NOTE)

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


@OCProc2Action.register_type
class PlatformBlocker(RecordAction):
    ACTION_NAME = "require_platform"

    @property
    def is_blocker(self) -> bool:
        return True

    @property
    def value(self) -> str:
        return ''

    def conflicts_with(self, action: OCProc2Action) -> bool:
        return isinstance(action, AssignPlatform)


@OCProc2Action.register_type
class SetRelationships(RecordAction):
    ACTION_NAME = "set_relationships"

    relationships: dict[str, list[list[str | bool]]] = dd.p_dict()

    def apply(self, record: ParentRecord):
        if not self.relationships:
            if "CNODCRelationships" in record.metadata:
                del record.metadata["CNODCRelationships"]
        else:
            record.metadata["CNODCRelationships"] = self.relationships

    def conflicts_with(self, action: OCProc2Action) -> bool:
        return isinstance(action, SetRelationships)


@OCProc2Action.register_type
class SetQualityCheck(RecordAction):
    ACTION_NAME = "set_quality_check"

    check_no: int = dd.p_int()

    def apply(self, record: ParentRecord):
        check = record.metadata.best("CNODCQualityFlags", default=0, coerce=int)
        record.metadata["CNODCQualityFlags"] = check | self.check_no


@OCProc2Action.register_type
class SetPlatformCandidates(RecordAction):
    ACTION_NAME = "set_candidates"

    platform_uuids: list[str] | None = dd.p_list(value_coerce=str)

    def value(self) -> str:
        return ','.join(self.platform_uuids) if self.platform_uuids else ''

    def apply(self, record: ParentRecord):
        if not self.platform_uuids:
            if 'CNODCPlatformCandidates' in record.metadata:
                del record.metadata["CNODCPlatformCandidates"]
        else:
            record.metadata["CNODCPlatformCandidates"] = self.platform_uuids

    def conflicts_with(self, action: OCProc2Action) -> bool:
        return isinstance(action, (SetPlatformCandidates, AssignPlatform))


@OCProc2Action.register_type
class AssignPlatform(RecordAction):
    ACTION_NAME = "set_platform"

    test_protocol: str = dd.p_str()
    platform_uuid: str | None = dd.p_str()
    platform_type: str | None = dd.p_str()

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

    def conflicts_with(self, action: OCProc2Action) -> bool:
        return isinstance(action, (SetPlatformCandidates, AssignPlatform))


@OCProc2Action.register_type
class SetManualQCOutcome(OCProc2Action):
    ACTION_NAME = "set_manual_qc_outcome"

    qc_index: int = dd.p_int()
    actual_result: QCResult = dd.p_enum(QCResult)

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

    def conflicts_with(self, action: OCProc2Action) -> bool:
        return isinstance(action, SetManualQCOutcome) and action.qc_index == self.qc_index


class PathAction(OCProc2Action):
    path: str = dd.p_str()

    @property
    def object(self) -> str:
        return self.path

    def conflicts_with(self, action: OCProc2Action) -> bool:
        return isinstance(action, PathAction) and action.path == self.path


@OCProc2Action.register_type
class SetToEmpty(PathAction):
    ACTION_NAME = "set_empty"

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


@OCProc2Action.register_type
class ChangeQualityAtLevelAndDeeper(PathAction):
    ACTION_NAME = "set_quality_level_and_deeper"

    test_protocol: str = dd.p_str()
    other_paths: list[str] = dd.p_list(value_coerce=str)
    new_flag: int = dd.p_int()

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


@OCProc2Action.register_type
class ChangeQuality(PathAction):
    ACTION_NAME = "set_quality"

    test_protocol: str = dd.p_str()
    new_flag: int = dd.p_int()

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


@OCProc2Action.register_type
class ChangeValue(PathAction):
    ACTION_NAME = "set_value"

    test_protocol: str = dd.p_str()
    new_value: SupportedValue = dd.p_any()

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
