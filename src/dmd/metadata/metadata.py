import pathlib

import typing as t
import yaml
import zrlog
from autoinject import injector

from dmd.containers.base import RequiredFieldValidator, RecommendedFieldValidator, \
    ContainerValidator, FieldValidator, CustomContainerValidator
from dmd.metadata.registries import BaseRegistry
from gcapp.i18n import MLString
from medsutil.dynamic import dynamic_object


@injector.injectable
class MetadataRegistry:

    def __init__(self):
        self._preprocessors: list[str] = []
        self._templates: list[str] = []
        self._fields = BaseRegistry("fields")
        self._profiles = BaseRegistry("profiles")
        self._display_groups = BaseRegistry("display_groups")
        self._security_labels = BaseRegistry("security_labels")
        self._log = zrlog.get_logger("dmd.metadata_registry")

    def register_formatter_template_directory(self, path: pathlib.Path):
        self._templates.append(str(path))

    def register_global_preprocessor(self, preprocessor: str):
        self._preprocessors.append(preprocessor)

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

    def build_field_list(self, extended_profiles: set[str]) -> dict[str, dict[str, t.Any]]:
        fields = {}
        for profile in extended_profiles:
            for field_name in self._profiles[profile].get("fields", {}).keys():
                if field_name not in fields:
                    if field_name in self._fields:
                        fields[field_name] = self._fields[field_name]
                    else:
                        self._log.error("Field %s is not defined, skipping", field_name)
        return fields

    def field_validators(self, extended_profiles: set[str]) -> t.Iterable[tuple[str, FieldValidator]]:
        for profile in extended_profiles:
            validators = self._profiles[profile].get("validators", {})
            for required_field_name in validators.get("required", []):
                yield required_field_name, RequiredFieldValidator(profile)
            for recommended_field_name in validators.get("recommended", []):
                yield recommended_field_name, RecommendedFieldValidator(profile)

    def container_validators(self, extended_profiles: set[str]) -> t.Iterable[ContainerValidator]:
        for profile in extended_profiles:
            validators = self._profiles[profile].get("validators", {})
            for callback_name in validators.get("custom", []):
                yield CustomContainerValidator(profile, callback_name)


    def formatter_options(self, extended_profiles: set[str]) -> t.Iterable[tuple[str, str]]:
        for profile_name in extended_profiles:
            for format_name in self._profiles[profile_name].get("formatters", {}).keys():
                yield profile_name, format_name

    def formatter_preprocessors(self,
                                extended_profiles: set[str],
                                profile_name: str,
                                format_name: str) -> t.Iterable[t.Callable]:

        hooks = [x for x in self._preprocessors]
        for profile in extended_profiles:
            preprocess = self._profiles[profile].get("preprocess", [])
            if isinstance(preprocess, str):
                hooks.append(preprocess)
            else:
                hooks.extend(preprocess)

        preprocess = self._profiles[profile_name]["formatters"][format_name].get("preprocess", [])
        if isinstance(preprocess, str):
            hooks.append(preprocess)
        else:
            hooks.extend(preprocess)

        for hook in hooks:
            yield dynamic_object(hook)

    def formatter_exists(self, profile_name: str, format_name: str) -> bool:
        return profile_name in self._profiles and format_name in self._profiles[profile_name].get("formatters", {})

    def formatter_display(self, profile_name: str, format_name: str) -> dict:
        return self._profiles[profile_name]["formatters"][format_name]["label"]

    def formatter_content_type(self, profile_name: str, format_name: str) -> tuple[str, str, str]:
        # Defaults
        encoding = "utf-8"
        mime_type = "text/plain"
        template_name = self._profiles[profile_name]["formatters"][format_name]["template"].lower()
        extension = template_name[template_name.rfind(".") + 1:] if "." in template_name else ""
        # What were we told?
        if "extension" in self._profiles[profile_name]["formatters"][format_name] and self._profiles[profile_name]["formatters"][format_name]["extension"]:
            extension = self._profiles[profile_name]["formatters"][format_name]["extension"]
        if "content_type" in self._profiles[profile_name]["formatters"][format_name] and self._profiles[profile_name]["formatters"][format_name]["content_type"]:
            mime_type = self._profiles[profile_name]["formatters"][format_name]["content_type"]
        elif template_name.endswith(".xml"):
            mime_type = "text/xml"
        elif template_name.endswith(".html") or template_name.endswith(".htm"):
            mime_type = "text/html"
            extension = "html"
        elif template_name.endswith(".rdf"):
            mime_type = "application/rdf+xml"
            extension = "rdf"
        elif template_name.endswith(".jsonld"):
            mime_type = "application/ld+json"
            extension = "jsonlod"
        if "encoding" in self._profiles[profile_name]["formatters"][format_name]:
            encoding = self._profiles[profile_name]["formatters"][format_name] or "utf-8"
        return mime_type, encoding, extension

    def render_template(self, extended_profiles: set[str], profile_name: str, format_name: str, kwargs: dict[str, t.Any]):
        from jinja2 import FileSystemLoader, Environment, select_autoescape
        template_name = self._profiles[profile_name]["formatters"][format_name]["template"]
        env = Environment(
            loader=FileSystemLoader(self._templates),
            autoescape=select_autoescape()
        )
        for processor in self.formatter_preprocessors(extended_profiles, profile_name, format_name):
            results = processor(**kwargs)
            if results:
                kwargs.update(results)
        return env.get_template(template_name).render(**kwargs)
