from __future__ import annotations
import typing as t
from autoinject import injector
from cnodc.storage import StorageController, StorageTier
from cnodc.util import CNODCError, dynamic_object, DynamicObjectLoadError
import cnodc.nodb.base as s


class NODBUploadWorkflow(s.NODBBaseObject):

    TABLE_NAME = "nodb_upload_workflows"
    PRIMARY_KEYS = ("workflow_name",)

    workflow_name: str = s.StringColumn("workflow_name")
    configuration: dict[str, t.Any] = s.JsonColumn("configuration")
    is_active: bool = s.BooleanColumn('is_active')

    def permissions(self):
        """Retrieve the permissions associated with this workflow."""
        return self.get_config('permissions') or []

    def get_config(self, config_key: str, default=None):
        """Get a configuration value for this workflow."""
        if self.configuration and config_key in self.configuration:
            return self.configuration[config_key]
        return default

    def ordered_processing_steps(self) -> list[str]:
        if self.configuration is None or 'processing_steps' not in self.configuration:
            return []
        return NODBUploadWorkflow.build_ordered_processing_steps(self.configuration['processing_steps'])

    @staticmethod
    def build_ordered_processing_steps(steps: dict[str, dict[str, t.Any]]) -> list[str]:
        sort_me = []
        for step_name in steps:
            step = steps[step_name]
            if 'name' not in step or not step['name']:
                raise CNODCError(f'Step {step_name} is missing a [name] value', 'NODB_WORKFLOW', 1000)
            if 'order' not in step or step['order'] is None:
                raise CNODCError(f'Step {step_name} is missing a proper [order] value', 'NODB_WORKFLOW', 1001)
            try:
                sort_me.append((step_name, int(step['order'])))
            except (ValueError, TypeError):
                raise CNODCError(f"Step {step_name} is missing an integer [order] value", 'NODB_WORKFLOW', 1002)
        sort_me.sort(key=lambda x: x[1])
        return [x[0] for x in sort_me]

    def update_config(self, config: dict[str, t.Any]):
        if self.configuration is not None and self.configuration['processing_steps'] is not None and 'processing_steps' in config and config['processing_steps']:
            current_steps = self.ordered_processing_steps()
            new_steps = NODBUploadWorkflow.build_ordered_processing_steps(config['processing_steps'])
            NODBUploadWorkflow._validate_step_order(current_steps, new_steps)
        self.configuration = config
        self.check_config()

    @staticmethod
    def _validate_step_order(current: list[str], new: list[str]):
        # removing steps leads to issues, dont do it!
        for old_step in current:
            if old_step not in new:
                raise s.NODBValidationError(f"Step {old_step} cannot be removed", 2020)

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
                if is_before_in_new and not is_before_in_current:
                    raise s.NODBValidationError(f"Step {step_name} cannot be moved before {other_step}", 2016)
                if is_before_in_current and not is_before_in_new:
                    raise s.NODBValidationError(f"Step {step_name} cannot be moved after {other_step}", 2017)

    @injector.inject
    def check_config(self, files: cnodc.storage.core.StorageController = None):
        """Validate the configuration for this workflow."""
        if 'label' not in self.configuration:
            raise s.NODBValidationError(f"A label is required for workflows", 2020)
        lbl = self.configuration['label']
        if not isinstance(lbl, dict):
            raise s.NODBValidationError(f"The workflow label must be a dict", 2021)
        if 'en' not in lbl and 'und' not in lbl:
            raise s.NODBValidationError("An English or language-neutral name must be provided for the workflow", 2022)
        if 'fr' not in lbl and 'und' not in lbl:
            raise s.NODBValidationError("An French or language-neutral name must be provided for the workflow", 2023)
        if 'validation' in self.configuration and self.configuration['validation'] is not None:
            try:
                x = dynamic_object(self.configuration['validation'])
                if not callable(x):
                    raise s.NODBValidationError(f'Invalid value for [validation]: {self.configuration["validation"]}, must be a Python callable', 2000)
            except DynamicObjectLoadError:
                raise s.NODBValidationError(f'Invalid value for [validation]: {self.configuration["validation"]}, must be a Python object', 2001)
        has_upload = False
        if 'working_target' in self.configuration and self.configuration['working_target']:
            has_upload = True
            self._check_upload_target_config(self.configuration['working_target'], files, 'working')
        if 'additional_targets' in self.configuration and self.configuration['additional_targets']:
            if isinstance(self.configuration['additional_targets'], list):
                for idx, target in enumerate(self.configuration['additional_targets']):
                    has_upload = True
                    self._check_upload_target_config(target, files, f'additional_targets[{idx}]')
            elif isinstance(self.configuration['additional_targets'], dict):
                for key in self.configuration['additional_targets']:
                    has_upload = True
                    self._check_upload_target_config(self.configuration['additional_targets'][key], files, f'additional_targets[{key}]')
            else:
                raise s.NODBValidationError(f"invalid value for [additional_targets], must be dict or list", 2027)
        if not has_upload:
            raise s.NODBValidationError(f"Workflow missing either upload or archive URL", 2002)
        if 'processing_steps' in self.configuration and self.configuration['processing_steps'] is not None:
            psteps = self.configuration['processing_steps']
            if not isinstance(psteps, dict):
                raise s.NODBValidationError("Processing steps must be a dictionary", 2010)
            for key in psteps:
                entry = psteps[key]
                if not isinstance(entry, dict):
                    raise s.NODBValidationError(f"Processing step {key} must be a dictionary", 2011)
                if 'order' not in entry or entry['order'] is None:
                    raise s.NODBValidationError(f"Processing step {key} must have an order value", 2012)
                if not isinstance(entry['order'], int):
                    raise s.NODBValidationError(f"Processing step {key} must have an integer order value", 2013)
                if 'name' not in entry or not entry['name']:
                    raise s.NODBValidationError(f"Processing step {key} must have an name value", 2014)
                if 'priority' in entry:
                    try:
                        _ = int(entry['priority'])
                    except (ValueError, TypeError):
                        raise s.NODBValidationError(f"Processing step {key} must have an integer priority value", 2015)
        if 'filename_pattern' in self.configuration and self.configuration['filename_pattern'] is not None:
            if not isinstance(self.configuration['filename_pattern'], str):
                raise s.NODBValidationError("The filename_pattern must be a string", 2018)
        if 'default_metadata' in self.configuration and self.configuration['default_metadata'] is not None:
            dm = self.configuration['default_metadata']
            if not isinstance(dm, dict):
                raise s.NODBValidationError("The default_metadata must be a dictionary", 2019)
        if 'permissions' in self.configuration and self.configuration['permissions'] is not None:
            if not isinstance(self.configuration['permissions'], list):
                raise s.NODBValidationError("Permissions must be a list", 2024)
            for item in self.configuration['permissions']:
                if not isinstance(item, str):
                    raise s.NODBValidationError("Permissions must be a list of strings", 2025)

    def _check_upload_target_config(self, config: dict, files: StorageController, tn: str):
        """Validate an upload target."""
        if not isinstance(config, dict):
            raise s.NODBValidationError(f'Upload target [{tn}] must be a dict', 2026)
        if 'directory' not in config:
            raise s.NODBValidationError(f'Target directory missing in [{tn}]', 2007)
        handle = files.get_handle(config['directory'])
        if handle is None:
            raise s.NODBValidationError(f'Target directory is not supported by storage subsystem in [{tn}]', 2008)
        if 'allow_overwrite' in config and config['allow_overwrite'] not in ('user', 'always', 'never'):
            raise s.NODBValidationError(f'Overwrite setting must be one of [user|always|never] in [{tn}]', 2009)
        if 'tier' in config:
            try:
                _ = StorageTier(config['tier'])
            except Exception as ex:
                raise s.NODBValidationError(f'Tier value [{config["tier"]} is not supported in [{tn}]', 2006) from ex
        if 'metadata' in config and config['metadata']:
            if not isinstance(config['metadata'], dict):
                raise s.NODBValidationError(f"Invalid value for [metadata] in [{tn}]: must be a dictionary", 2005)
            for x in config['metadata'].keys():
                if not isinstance(x, str):
                    raise s.NODBValidationError(f"Invalid key for [metadata] in [{tn}]: {x}, must be a string", 2004)
                if not isinstance(config['metadata'][x], str):
                    raise s.NODBValidationError(f'Invalid value for [metadata.{x}] in [{tn}]: {config["metadata"][x]}, must be a string', 2003)

    def check_access(self, user_permissions: t.Union[list, set, tuple]) -> bool:
        """Check if a user has access to this workflow based on their permissions."""
        if '__admin__' in user_permissions:
            return True
        needed_permissions = self.permissions()
        if '__any__' in needed_permissions:
            return True
        return any(x in user_permissions for x in needed_permissions)

    @classmethod
    def find_by_name(cls, db, workflow_name: str, **kwargs):
        """Find a workflow by name."""
        return db.load_object(cls, {"workflow_name": workflow_name},  **kwargs)
