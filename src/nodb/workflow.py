import enum
import pathlib
import typing as t
from autoinject import injector

from nodb import interface
from medsutil.storage.core import StorageController
from medsutil.storage import StorageTier
import medsutil.datadict as ddo
from medsutil.delayed import newdict
import nodb.base as s
import medsutil.types as ct


class OverwriteOption(enum.Enum):
    USER = 'user'
    NEVER = 'never'
    ALWAYS = 'always'


class ProcessingStep(ddo.DataDictObject):
    name: str = ddo.p_str(required=True)
    order: int = ddo.p_int(required=True)
    priority: int = ddo.p_int(default=0)
    worker_config: dict[str, dict[str, ct.SupportsExtendedJson]] = ddo.p_json_dict()

    def validate(self, config: WorkflowConfiguration):
        for key in self.worker_config:
            if not isinstance(self.worker_config[key], dict):
                raise interface.NODBValidationError(f"Worker config [{key}] is not a dict", 2200)
        if not self.name:
            raise interface.NODBValidationError(f"Step is missing a name", 2201)


class WorkflowDirectory(ddo.DataDictObject):
    directory: str = ddo.p_str(required=True)
    allow_overwrite: OverwriteOption = ddo.p_enum(OverwriteOption, default=OverwriteOption.NEVER)
    tier: StorageTier = ddo.p_enum(StorageTier, default=None)
    metadata: dict[str, str] = ddo.p_json_dict()
    gzip: bool = False

    @injector.inject
    def storage_handle(self, storage: StorageController=None):
        return storage.get_filepath(self.directory, raise_ex=True)

    def validate(self, config: WorkflowConfiguration):
        _ = self.storage_handle()


class WorkflowConfiguration(ddo.DataDictObject):
    label: ct.LanguageDict = ddo.p_i18n_text(default=newdict)
    steps: dict[str, ProcessingStep] = ddo.p_json_object_dict(required_type=ProcessingStep)
    validation: t.Callable[[pathlib.Path, dict[str, str], str], None] | None = ddo.p_dynamic_callable()
    working_target: WorkflowDirectory | None = ddo.p_json_object(required_type=WorkflowDirectory)
    additional_targets: list[WorkflowDirectory] = ddo.p_json_object_list(required_type=WorkflowDirectory)
    accept_user_filename: bool = ddo.p_bool(default=False)
    filename_pattern: str | None = ddo.p_str()
    default_metadata: dict[str, str] = ddo.p_json_dict()
    permissions: set[str] = ddo.p_json_str_set()
    max_file_size: int | None = None

    def validate(self, previous: WorkflowConfiguration = None):

        if self.label is None:
            raise interface.NODBValidationError('Workflow label is required', 2100)

        if self.working_target:
            self.working_target.validate(self)
        elif not self.additional_targets:
            raise interface.NODBValidationError('No upload targets specified', 2101)

        for add_target in self.additional_targets:
            add_target.validate(self)

        seen_orders = set()
        for step in self.steps.values():
            if step.order in seen_orders:
                raise interface.NODBValidationError('Duplicate step order', 2102)
            seen_orders.add(step.order)
            step.validate(self)

        if previous is not None:
            self._validate_step_order(previous.ordered_steps(), self.ordered_steps())

    def extend_metadata(self, md: dict[str, str]):
        for x in self.default_metadata:
            if x not in md:
                md[x] = self.default_metadata[x]

    def validate_upload(self, local_path: pathlib.Path, metadata: dict[str, str], filename: str):
        if self.validation:
            self.validation(local_path, metadata, filename)

    def ordered_steps(self) -> list[str]:
        sort_me = [
            (step_key, self.steps[step_key].order)
            for step_key in self.steps
        ]
        return [x[0] for x in sorted(sort_me, key=lambda x: x[1])]

    def check_access(self, user_permissions: t.Union[list, set, tuple]) -> bool:
        """Check if a user has access to this workflow based on their permissions."""
        if '__admin__' in user_permissions:
            return True
        if '__any__' in self.permissions:
            return True
        return any(x in user_permissions for x in self.permissions)

    @staticmethod
    def _validate_step_order(current: list[str], new: list[str]):
        # removing steps leads to issues, dont do it!
        for old_step in current:
            if old_step not in new:
                raise interface.NODBValidationError(f"Step {old_step} cannot be removed", 2000)

        # Reordering steps leads to problems (e.g. a step may be re-executed for an existing workflow item
        for step_name in new:
            # adding steps is fine
            if step_name not in current:
                continue
            current_pos = current.index(step_name)
            new_pos = current.index(step_name)
            for other_step in current:
                if other_step == step_name:
                    continue
                is_before_in_current = current.index(other_step) < current_pos
                is_before_in_new = new.index(other_step) < new_pos
                if is_before_in_new is not is_before_in_current:
                    raise interface.NODBValidationError(f"Step {step_name} cannot be moved before {other_step}", 2001)


class NODBUploadWorkflow(s.NODBBaseObject):

    TABLE_NAME = "nodb_upload_workflows"
    PRIMARY_KEYS = ("workflow_name",)

    workflow_name: str | None = s.StringColumn(readonly=True)
    configuration: WorkflowConfiguration | None = ddo.p_json_object(WorkflowConfiguration, readonly=True)
    is_active: bool = s.BooleanColumn(default=True)

    def check_access(self, user_permissions: t.Union[list, set, tuple]) -> bool:
        if self.configuration is not None:
            return self.configuration.check_access(user_permissions)
        return False

    def ordered_steps(self, ) -> list[str]:
        if self.configuration is not None:
            return self.configuration.ordered_steps()
        return []

    def set_config(self, config: dict[str, t.Any] | WorkflowConfiguration):
        """ Convert and validate the workflow configuration. """
        try:
            if isinstance(config, dict):
                if 'processing_steps' in config:
                    config['steps'] = config['processing_steps']
                    del config['processing_steps']
            new_config = WorkflowConfiguration(**config) if isinstance(config, dict) else config
            new_config.validate(self.configuration)
            with self.readonly_access():
                self.configuration = new_config
        except (ValueError, TypeError, KeyError) as ex:
            raise interface.NODBValidationError(f"{ex.__class__.__name__}: {str(ex)}", 1000) from ex

    @classmethod
    def find_by_name(cls, db: interface.NODBInstance, workflow_name: str, **kwargs):
        """Find a workflow by name."""
        return db.load_object(cls, {"workflow_name": workflow_name},  **kwargs)
