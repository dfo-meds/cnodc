import medsutil.datadict as dd
from medsutil.ocproc2 import ParentRecord, AbstractElement, SingleElement, RecordSet, BaseRecord
from medsutil.ocproc2.history import ActionType, Organization
from medsutil.ocproc2.util import set_working_quality


class RecordOperator(dd.DataDictObject):

    source_name: str = dd.p_str()
    source_version: str = dd.p_str()
    source_instance: str | None = dd.p_str()
    organization: Organization = dd.p_enum(Organization)

    def apply(self, record: ParentRecord):
        raise NotImplementedError


class AssignPlatform(RecordOperator):
    platform_uuid: str = dd.p_str()

    def apply(self, record: ParentRecord):
        record.metadata["CNODCPlatform"] = SingleElement(self.platform_uuid, Quality=1)
        if 'CNODCPlatformCandidates' in record.metadata:
            del record.metadata["CNODCPlatformCandidates"]
        record.add_history_action(
            f"Platform assigned",
            self.source_name,
            self.source_version,
            self.source_instance or '',
            ActionType.PLATFORM_ASSIGNED,
            "metadata/CNODCPlatform",
            self.organization
        )


class ChangeQuality(RecordOperator):
    path: str = dd.p_str()
    new_flag: int = dd.p_int()

    def apply(self, record: ParentRecord):
        element = record.find_child(self.path)
        if not isinstance(element, (AbstractElement, BaseRecord, RecordSet)):
            raise ValueError("Invalid element path")
        if set_working_quality(element, self.new_flag):
            record.add_history_action(
                f"Quality flag changed to {self.new_flag}",
                self.source_name,
                self.source_version,
                self.source_instance or '',
                ActionType.CHANGE_QUALITY,
                self.path,
                self.organization
            )


class ChangeValue(RecordOperator):
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
        record.add_history_action(
            f"Value changed",
            self.source_name,
            self.source_version,
            self.source_instance or '',
            ActionType.CHANGE_QUALITY,
            self.path,
            self.organization
        )
