import pathlib

import typing as t
import yaml
from autoinject import injector

from dmd.metadata.registries import BaseRegistry
from gcapp.i18n import MLString


@injector.injectable
class MetadataRegistry:

    def __init__(self):
        self._fields = BaseRegistry("fields")
        self._profiles = BaseRegistry("profiles")
        self._display_groups = BaseRegistry("display_groups")
        self._security_labels = BaseRegistry("security_labels")

    def profile_exists(self, profile_name: str) -> bool:
        return profile_name in self._profiles

    def profile_options(self) -> list[tuple[str, MLString]]:
        return [
            (x, MLString(self._profiles[x]["display"]))
            for x in self._profiles
        ]

    def profile_display(self, profile_name: str) -> MLString:
        try:
            return MLString(self._profiles[profile_name]["display"])
        except KeyError:
            raise ValueError(f"Invalid profile: [{profile_name}]")

    def register_profile(self, profile_name: str, **config):
        self._profiles.register(profile_name, **config)

    def register_profiles_from_dict(self, profiles: dict[str, dict]):
        self._profiles.register_from_dict(profiles)

    def register_profiles_from_yaml(self, profile_file: pathlib.Path):
        self._profiles.register_from_yaml(profile_file)


    def security_label_exists(self, security_label_name: str) -> bool:
        return security_label_name in self._security_labels

    def security_label_options(self) -> list[tuple[str, MLString]]:
        return [
            (x, MLString(self._security_labels[x]))
            for x in self._security_labels
        ]

    def security_label_display(self, security_label_name: str) -> MLString:
        try:
            return MLString(self._security_labels[security_label_name])
        except KeyError:
            raise ValueError(f"Invalid security label: [{security_label_name}]")

    def register_security_label(self, name, **config):
        self._security_labels.register(name, **config)

    def register_security_labels_from_dict(self, labels: dict[str, dict]):
        self._security_labels.register_from_dict(labels)

    def register_security_labels_from_yaml(self, label_file: pathlib.Path):
        self._security_labels.register_from_yaml(label_file)


    def display_group_exists(self, display_group_name: str) -> bool:
        return display_group_name in self._display_groups

    def ordered_display_groups(self, limit_groups: list[str]) -> t.Iterable[str]:
        reordered: list[tuple[int, str]] = [
            (
                int(self._display_groups[dg].get("order", 0)),
                self._display_groups[dg].get("name", "")
            )
            for dg in limit_groups
            if dg in self._display_groups
        ]
        reordered.sort()
        for x in reordered:
            yield x[1]


    def register_metadata_fields_from_dict(self, metadata_fields: dict[str, dict[str, t.Any]]):
        for display_group_name, display_group in metadata_fields.items():
            fields = display_group.pop("fields", {}) or {}
            display_group["name"] = display_group_name
            self._display_groups.register(display_group_name, **display_group, _update_db=False)
            for field_name, field_config in fields:
                field_config["display_group"] = display_group_name
                self._fields.register(field_name, **field_config, _update_db=False)
        self._fields.save_changes()
        self._display_groups.save_changes()

    def register_metadata_fields_from_yaml(self, yaml_file: pathlib.Path):
        with open(yaml_file, "r", encoding="utf-8") as f:
            self.register_metadata_fields_from_dict(yaml.safe_load(f) or {})

    def extend_profile_list(self, profile_list: t.Iterable[str]) -> set[str]:
        extended = set()
        work = [x for x in profile_list]
        while work:
            profile = work.pop()
            if profile in extended:
                continue
            if profile not in self._profiles:
                continue
            extended.add(profile)
            extended_from = self._profiles[profile].get("extended", None)
            if extended_from is None:
                continue
            elif isinstance(extended_from, str):
                work.append(extended_from)
            else:
                work.extend(extended_from)
        return extended