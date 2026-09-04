import enum
import gcapp.i18n as i18n
import typing as t

from autoinject import injector, auto
import zirconium as zr

from dmd.entityfields.base import Container
from dmd.metadata.metadata import MetadataRegistry
from dmd.metadata.workflows import WorkflowRegistry
from gcapp.i18n import MLString
from medsutil.awaretime import AwareDateTime
from pipeman.programs.dmd.metadata import Keyword


class DatasetStatus(enum.Enum):
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    UNDER_REVIEW = "UNDER_REVIEW"


class Dataset(Container):

    metadata_registry: MetadataRegistry = auto()
    workflow_registry: WorkflowRegistry = auto()
    config: zr.ApplicationConfig = auto()

    @injector.construct
    def __init__(self,
                 fields: dict[str, dict[str, t.Any]],
                 field_values: dict[str, t.Any],
                 base_profiles: list[str] | set[str] | tuple[str],
                 display_names: dict[str, str],
                 database_identifier: int | None = None,
                 version_identifier: int | None = None,
                 revision_no: int | None = None,
                 guid: str | None = None,
                 activation_workflow: str | None = None,
                 publication_workflow: str | None = None,
                 activation_item_id: int | None = None,
                 publication_date: AwareDateTime | None = None,
                 created_date: AwareDateTime | None = None,
                 modified_date: AwareDateTime | None = None,
                 metadata_modified_date: AwareDateTime | None = None,
                 status: DatasetStatus = DatasetStatus.DRAFT,
                 security_label: str | None = None,
                 authority: str | None = None):
        super().__init__("dataset", display_names, fields, field_values)
        self.database_identifier = database_identifier
        self.version_identifier = version_identifier
        self.revision_no = revision_no
        self.profiles = self.metadata_registry.extend_profile_list(base_profiles)
        self.base_profiles = base_profiles
        self.guid = guid
        self.publication_date = publication_date
        self.created_date = created_date
        self.modified_date = modified_date
        self.metadata_modified_date = metadata_modified_date
        self.status = status
        self.security_label = security_label
        self._authority = authority
        self.activation_workflow = activation_workflow
        self.activation_item_id = activation_item_id
        self.publication_workflow = publication_workflow

    @property
    def container_id(self) -> int | None:
        return self.database_identifier

    @property
    def naming_authority(self) -> str:
        if self._authority:
            return self._authority
        return str(self.config.as_str(("dmd", "metadata", "naming_authority"), default="pipeman"))

    def activation_workflow_display(self) -> MLString | str:
        if self.activation_workflow:
            return self.workflow_registry.workflow_display("dataset_activation", self.activation_workflow)
        return i18n.tr("gcapp.common.unknown")

    def publication_workflow_display(self) -> MLString | str:
        if self.publication_workflow:
            return self.workflow_registry.workflow_display("dataset_publication", self.publication_workflow)
        return i18n.tr("gcapp.common.unknown")

    def status_display(self) -> str:
        return i18n.tr(f"dmd.dataset.status.{self.status.value.lower()}")

    def security_label_display(self) -> str | MLString:
        if self.security_label:
            return self.metadata_registry.security_label_display(self.security_label)
        return i18n.tr("gcapp.common.unknown")

    def keywords(self) -> set[Keyword]:
        keywords = set()
        for field in self._fields.values():
            keywords.update(field.get_keywords())
        return keywords


