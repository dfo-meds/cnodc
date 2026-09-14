import enum
import re
import uuid

import gcapp.i18n as i18n
import typing as t

from autoinject import injector, auto
import zirconium as zr

from dmd.containers.base import Container
from dmd.containers.keywords import Keyword
from dmd.db import DataManagementDatabase, DatasetKeywords
from dmd.linker import DataManagementLinker
from dmd.metadata.metadata import MetadataRegistry
from dmd.metadata.workflows import WorkflowRegistry, WorkflowItemStatus
from gcapp.i18n import MLString, MLLink
from gcflask.action_list import ActionList
from medsutil.awaretime import AwareDateTime



class DatasetStatus(enum.Enum):
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    UNDER_REVIEW = "UNDER_REVIEW"


class Dataset(Container):

    metadata_registry: MetadataRegistry = auto()
    workflow_registry: WorkflowRegistry = auto()
    linker: DataManagementLinker = auto()
    config: zr.ApplicationConfig = auto()
    database: DataManagementDatabase = auto()

    @injector.construct
    def __init__(self,
                 base_profiles: list[str] | set[str] | tuple[str] | None = None,
                 display_names: dict[str, str] | None = None,
                 field_values: dict[str, t.Any] | None = None,
                 database_identifier: int | None = None,
                 version_identifier: int | None = None,
                 revision_no: int | None = None,
                 organization_id: int | None = None,
                 is_deprecated: bool = False,
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
                 authority: str | None = None,
                 users: list[str] | None = None):
        self.base_profiles = base_profiles
        self.profiles = self.metadata_registry.extend_profile_list(base_profiles or [])
        super().__init__("dataset", display_names or {}, self.metadata_registry.build_field_list(self.profiles), field_values or {})
        self.database_identifier = database_identifier
        self.version_identifier = version_identifier
        self.revision_no = revision_no
        self.users = users or []
        self.is_deprecated = is_deprecated
        self.guid = guid
        self.publication_date = publication_date
        self.created_date = created_date
        self.modified_date = modified_date
        self.organization_id = organization_id
        self.metadata_modified_date = metadata_modified_date
        self.status = status
        self.security_label = security_label
        self._authority = authority
        self.activation_workflow = activation_workflow
        self.activation_item_id = activation_item_id
        self.publication_workflow = publication_workflow
        for field_name, validator in self.metadata_registry.field_validators(self.profiles):
            self.add_field_validator(field_name, validator)
        for validator in self.metadata_registry.container_validators(self.profiles):
            self.add_container_validator(validator)

    @property
    def container_id(self) -> int | None:
        return self.database_identifier

    @property
    def naming_authority(self) -> str:
        if self._authority:
            return self._authority
        return str(self.config.as_str(("dmd", "metadata", "naming_authority"), default="pipeman"))

    def can_access(self, action: str) -> bool:
        match action:
            case "activate":
                if self.status is not DatasetStatus.DRAFT:
                    return False
                if self.is_deprecated:
                    return False
                if self.activation_workflow is None:
                    return False
                if self.database_identifier is None:
                    return False
                with self.database as db:
                    if db.workflow_item_exists(
                        category="dataset_activation",
                        workflow_name=self.activation_workflow,
                        object_type="dataset",
                        object_id=self.database_identifier
                    ):
                        return False

            case "publish":
                if self.status is not DatasetStatus.ACTIVE:
                    return False
                if self.is_deprecated:
                    return False
                if self.publication_workflow is None:
                    return False
                if self.database_identifier is None:
                    return False
                with self.database as db:
                    if db.workflow_item_exists(
                        category="dataset_publication",
                        workflow_name=self.publication_workflow,
                        object_type="dataset",
                        object_id=self.database_identifier
                    ):
                        return False

            case "edit":
                if self.status is DatasetStatus.UNDER_REVIEW:
                    return False
                if self.is_deprecated:
                    return False

            case "copy":
                if self.is_deprecated:
                    return False

            case "remove":
                if self.is_deprecated:
                    return False

            case "validate":
                if self.is_deprecated:
                    return False

            case "restore":
                if not self.is_deprecated:
                    return False

            case "view":
                ...

            case _:
                return False

        return self.linker.has_dataset_access(action, self)

    def actions(self,
                short_list: bool = True,
                for_revision: bool = False) -> ActionList:
        actions = super().actions()
        if self.container_id is not None:
            if short_list:
                actions.add_action(
                    tr_key="dmd.dataset.view",
                    url=self.view_link(),
                    order=0
                )
            if for_revision:
                actions.add_action(
                    tr_key="dmd.dataset.view_current_revision",
                    url=self.view_link(),
                    order=0
                )
            else:
                if self.can_access("edit"):
                    actions.add_action(
                        tr_key="dmd.dataset.edit",
                        url=self.edit_link(),
                        order=1
                    )
                    actions.add_action(
                        tr_key="dmd.dataset.edit_metadata",
                        url=self.edit_metadata_link(),
                        order=2
                    )
                    if not short_list:
                        actions.add_action(
                            tr_key="dmd.dataset.add_attachment",
                            url=self.add_attachment_link(),
                            order=5
                        )
                if not short_list:
                    if self.can_access("activate"):
                        actions.add_action(
                            tr_key="dmd.dataset.activate",
                            url=self.activate_link(),
                            order=10
                        )
                    if self.can_access("publish"):
                        actions.add_action(
                            tr_key="dmd.dataset.publish",
                            url=self.publish_link(),
                            order=11
                        )
                if self.can_access("validate"):
                    actions.add_action(
                        tr_key="dmd.dataset.validate",
                        url=self.validate_link(),
                        order=50
                    )
                if self.can_access("copy"):
                    actions.add_action(
                        tr_key="dmd.dataset.validate",
                        url=self.copy_link(),
                        order=60
                    )
                if self.can_access("remove"):
                    actions.add_action(
                        tr_key="dmd.dataset.remove",
                        url=self.remove_link(),
                        order=99
                    )
                elif self.can_access("restore"):
                    actions.add_action(
                        tr_key="dmd.dataset.restore",
                        url=self.restore_link(),
                        order=100
                    )
        return actions

    def view_link(self) -> str:
        return self.linker.view_dataset_link(self.container_id or -1)

    def edit_link(self) -> str:
        return self.linker.edit_dataset_link(self.container_id or -1)

    def validate_link(self) -> str:
        return self.linker.validate_dataset_link(self.container_id or -1)

    def copy_link(self) -> str:
        return self.linker.copy_dataset_link(self.container_id or -1)

    def edit_metadata_link(self) -> str:
        return self.linker.edit_dataset_metadata_link(self.container_id or -1)

    def add_attachment_link(self) -> str:
        return self.linker.add_attachment_link(self.container_id or -1)

    def remove_link(self) -> str:
        return self.linker.remove_dataset_link(self.container_id or -1)

    def remove(self):
        self.is_deprecated = True

    def activate_link(self) -> str:
        return self.linker.activate_dataset_link(self.container_id or -1)

    def activate(self) -> WorkflowItemStatus:
        if self.container_id is None:
            raise ValueError("Container not saved")
        if self.activation_workflow is None:
            raise ValueError("Activation workflow not assigned")
        return self.workflow_registry.start_workflow(
            category="dataset_activation",
            workflow_name=self.activation_workflow,
            object_type="dataset",
            object_id=self.container_id,
            metadata={
                "dataset_id": self.container_id,
            }
        )

    def publish_link(self) -> str:
        return self.linker.publish_dataset_link(self.container_id or -1)

    def publish(self, auto_approve: bool = False) -> WorkflowItemStatus:
        if self.container_id is None:
            raise ValueError("Container not saved")
        if self.publication_workflow is None:
            raise ValueError("Publication workflow not assigned")
        return self.workflow_registry.start_workflow(
            category="dataset_publication",
            workflow_name=self.publication_workflow,
            object_type="dataset",
            object_id=self.container_id,
            metadata={
                "dataset_id": self.container_id,
                "revision_no": self.revision_no,
                "auto_approve": auto_approve
            }
        )

    def restore_link(self) -> str:
        return self.linker.restore_dataset_link(self.container_id or -1)

    def restore(self):
        self.is_deprecated = False

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

    def formatter_exists(self, profile_name: str, format_name: str) -> bool:
        if profile_name not in self.profiles:
            return False
        return self.metadata_registry.formatter_exists(profile_name, format_name)

    def metadata_links(self) -> t.Iterable[MLLink]:
        for profile_name, format_name in self.metadata_registry.formatter_options(self.profiles):
            link = self.metadata_link(profile_name, format_name)
            if link:
                yield link

    def metadata_link(self,
                      profile_name: str,
                      format_name: str) -> MLLink | None:
        if self.database_identifier is None or self.revision_no is None:
            return None

        return MLLink(self.linker.metadata_link(
            profile_name=profile_name,
            format_name=format_name,
            dataset_id=self.database_identifier,
            revision_no=self.revision_no,
        ), self.metadata_registry.formatter_display(profile_name, format_name))

    def generate_metadata_content(self,
                                  profile_name: str,
                                  format_name: str,
                                  environment: str = "live"):
        kwargs = {
            "dataset": self,
            "environment": environment,
            "authority": self.naming_authority,
        }
        content = self.metadata_registry.render_template(self.profiles, profile_name, format_name, kwargs)
        return re.sub(
            "\n[ \t\n]{0,}\n",
            "\n",
            content.replace("\r", "\n")
        ).strip("\r\n\t ")

    def metadata_content_type(self,
                              profile_name: str,
                              format_name: str) -> tuple[str, str, str]:
        return self.metadata_registry.formatter_content_type(profile_name, format_name)

    def create_copy(self) -> int:
        with self.database as db:
            kwargs = self.dataset_kwargs()
            kwargs["is_deprecated"] = True
            db_id = db.create_dataset(kwargs)
            db.save_metadata(db_id, self.serialize())
            kwargs["is_deprecated"] = False
            db.update_dataset(db_id, kwargs)
            return db_id

    def save(self, save_metadata: bool = False):
        with self.database as db:
            if self.guid is None:
                self.guid = str(uuid.uuid4())
            if self.container_id is None:
                self.database_identifier = db.create_dataset(self.dataset_kwargs())
            else:
                db.update_dataset(self.container_id, self.dataset_kwargs())
            if save_metadata:
                db.save_metadata(self.container_id, self.serialize())

    def save_metadata(self):
        with self.database as db:
            db.save_metadata(self.container_id, self.serialize())

    def dataset_kwargs(self) -> DatasetKeywords:
        return {
            "profiles": self.base_profiles or [],
            "display_names": self._display_names,
            "guid": self.guid or "",
            "is_deprecated": self.is_deprecated,
            "organization_id": self.organization_id,
            "users": self.users,
            "created_date": self.created_date,
            "modified_date": self.modified_date,
        }

    @staticmethod
    def find_by_id(dataset_id: int,
                   revision_no: int | None = None,
                   database: DataManagementDatabase = auto()) -> Dataset | None:
        with database as db:
            kwargs = db.find_dataset_by_id(dataset_id, revision_no)
            if kwargs is None:
                return None
            return Dataset(**kwargs[0], field_values=kwargs[1])

    @staticmethod
    def find_by_guid(guid: str,
                     authority: str | None = None,
                     database: DataManagementDatabase = auto()) -> Dataset | None:
        with database as db:
            kwargs = db.find_dataset_by_guid(guid, authority)
            if kwargs is None:
                return None
            return Dataset(**kwargs[0], field_values=kwargs[1])
