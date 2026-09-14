import enum
import pathlib

import typing as t
import yaml
from autoinject import injector

from dmd.metadata.registries import BaseRegistry
from gcapp.i18n import MLString


class WorkflowItemStatus(enum.Enum):
    ...

@injector.injectable
class WorkflowRegistry:

    def __init__(self):
        self._steps = BaseRegistry("steps")
        self._workflows = BaseRegistry("workflows")

    def workflow_display(self, category, workflow_name) -> MLString:
        key = f"{category}__{workflow_name}"
        return MLString(self._workflows[key]["display"])

    def is_workflow_available(self, category, workflow_name, permission_checker: t.Callable[[list[str]], bool] | None = None) -> bool:
        key = f"{category}__{workflow_name}"
        if key not in self._workflows:
            return False
        if not self._workflows[key].get("enabled", True):
            return False
        permissions = self._workflows[key].get("permissions", None)
        if permissions is not None and permission_checker is not None:
            return permission_checker(permissions)
        else:
            return True

    def workflow_options(self, category: str, permission_checker: t.Callable[[list[str]], bool] | None = None) -> list[tuple[str, MLString]]:
        options = []
        prefix = f"{category}__"
        prefix_len = len(prefix)
        for key in self._workflows:
            if not key.startswith(prefix):
                continue
            workflow_name = key[prefix_len:]
            if self.is_workflow_available(category, workflow_name, permission_checker):
                options.append((
                    workflow_name,
                    MLString(self._workflows[key]["display"]),
                ))
        return options

    def register_workflow(self, category, workflow_name, **config):
        self._workflows.register(f"{category}__{workflow_name}", **config)

    def register_workflows_from_dict(self, workflows: dict[str, dict[str, dict]]):
        for category in workflows or {}:
            for workflow_name, config in (category or {}).items():
                self.register_workflow(category, workflow_name, **config, _update_db=False)
        self._workflows.save_changes()

    def register_workflows_from_yaml(self, workflow_file: pathlib.Path):
        with open(workflow_file, "r", encoding="utf-8") as f:
            self.register_workflows_from_dict(yaml.safe_load(f.read()) or {})


    def step_display(self, step_name) -> MLString:
        return MLString(self._steps[step_name]["display"])

    def register_step(self, step_name, **config):
        self._steps.register(step_name, **config)

    def register_steps_from_dict(self, steps: dict[str, dict]):
        self._steps.register_from_dict(steps)

    def register_steps_from_yaml(self, step_file: pathlib.Path):
        self._steps.register_from_yaml(step_file)

    def steps_for_workflow(self, category, workflow_name) -> list[str]:
        key = f"{category}__{workflow_name}"
        return self._steps[key].get("steps", [])

    def cleanup_steps_for_workflow(self, category, workflow_name) -> list[str]:
        key = f"{category}__{workflow_name}"
        return self._steps[key].get("cleanup", [])

    def start_workflow(self,
                       category: str,
                       workflow_name: str,
                       metadata: dict[str, t.Any],
                       object_type: str,
                       object_id: int) -> WorkflowItemStatus:
        ...

