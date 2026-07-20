import medsutil.datadict as dd
from medsutil.ocproc2 import ParentRecord, AbstractElement, SingleElement, RecordSet, BaseRecord, MessageType
from medsutil.ocproc2.history import ActionType, Organization
from medsutil.ocproc2.util import set_working_quality


class RecordAction(dd.DataDictObject):
    source_name: str | None = dd.p_str()
    source_version: str | None = dd.p_str()
    process_id: str | None = dd.p_str()
    organization: Organization = dd.p_enum(Organization)

    def apply(self, record: ParentRecord):
        raise NotImplementedError

    def add_history_action(self,
                           record: ParentRecord,
                           message: str,
                           action_type: ActionType,
                           path: str | None = None):
        record.add_history_action(
            message,
            self.source_name or 'unknown',
            self.source_version or 'unknown',
            self.process_id or 'unknown',
            action_type,
            path,
            self.organization
        )


class RetestRecord(RecordAction):
    qc_test_name: str

    def apply(self, record: ParentRecord):
        record.mark_test_results_stale(self.qc_test_name)


class RecordProcessed(RecordAction):

    def apply(self, record: ParentRecord):
        self.add_history_action(
            record,
            "Processed",
            ActionType.PROCESSED
        )


class AddHistoryEntry(RecordAction):
    message: str = dd.p_str()
    message_type: MessageType = dd.p_enum(MessageType, default=MessageType.NOTE)

    def apply(self, record: ParentRecord):
        record.add_history_entry(
            self.message,
            self.source_name or 'unknown',
            self.source_version or 'unknown',
            self.process_id or 'unknown',
            self.message_type
        )


class SetRelationships(RecordAction):
    relationships: dict[str, list[list[str]]]

    def apply(self, record: ParentRecord):
        if not self.relationships:
            if "CNODCRelationships" in record.metadata:
                del record.metadata["CNODCRelationships"]
        else:
            record.metadata["CNODCRelationships"] = self.relationships


class SetPlatformCandidates(RecordAction):
    platform_uuids: list[str] | None

    def apply(self, record: ParentRecord):
        if not self.platform_uuids:
            if 'CNODCPlatformCandidates' in record.metadata:
                del record.metadata["CNODCPlatformCandidates"]
        else:
            record.metadata["CNODCPlatformCandidates"] = self.platform_uuids


class AssignPlatform(RecordAction):
    platform_uuid: str | None = dd.p_str()

    def apply(self, record: ParentRecord):
        record.metadata["CNODCPlatform"] = SingleElement(self.platform_uuid, Quality=1 if self.platform_uuid else 9)
        if 'CNODCPlatformCandidates' in record.metadata:
            del record.metadata["CNODCPlatformCandidates"]
        self.add_history_action(
            record,
            f"Platform assigned",
            ActionType.PLATFORM_ASSIGNED,
            "metadata/CNODCPlatform"
        )


class ChangeQuality(RecordAction):
    path: str = dd.p_str()
    new_flag: int = dd.p_int()

    def apply(self, record: ParentRecord):
        element = record.find_child(self.path)
        if not isinstance(element, (AbstractElement, BaseRecord, RecordSet)):
            raise ValueError("Invalid element path")
        if set_working_quality(element, self.new_flag):
            self.add_history_action(
                record,
                f"Quality flag changed to {self.new_flag}",
                ActionType.CHANGE_QUALITY,
                self.path
            )


class ChangeValue(RecordAction):
    path: str = dd.p_str()
    new_element: dict = dd.p_dict()

    def apply(self, record: ParentRecord):
        element = record.find_child(self.path)
        if not isinstance(element, (AbstractElement, None)):
            raise ValueError(f"Invalid path: {self.path}")
        new_value = AbstractElement.build_from_mapping(self.new_element)
        if element is not None:
            previous = element.to_mapping()
            record.set(self.path, new_value, WorkingQuality=5, PreviousValue=previous)
        else:
            record.set(self.path, new_value, WorkingQuality=5)
        self.add_history_action(
            record,
            f"Value changed",
            ActionType.CHANGE_QUALITY,
            self.path
        )
