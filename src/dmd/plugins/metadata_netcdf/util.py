import functools
import typing as t

import zrlog

from dmd.metadata.datasets import Dataset
from dmd.containers.base import Container, Field
from dmd.metadata.vocabularies import VocabularyTerm
from medsutil.dynamic import dynamic_object, DynamicObjectLoadError


def preprocess_for_ncml(**kwargs) -> dict[str, t.Any]:
    return _preprocess_for_netcdf(**kwargs)


def preprocess_for_cdl(**kwargs) -> dict[str, t.Any]:
    return _preprocess_for_netcdf(**kwargs)


def _preprocess_for_netcdf(dataset: Dataset, **kwargs) -> dict[str, t.Any]:
    default_lang_code, default_bcp47, locale_mapping = _build_language_maps(dataset)
    extras = {
        'global_attributes': _build_global_attributes(dataset),
        'variables': _build_variables(dataset),
        'locale_mapping': locale_mapping,
        'default_locale': default_lang_code,
        'get_alternative_languages': functools.partial(_other_languages, default_locale=default_lang_code, locale_mapping=locale_mapping),
    }
    extras['global_attributes'].update({
        "default_locale": default_bcp47,
        "other_locales": " ".join(
            f"{suffix}: {bcp47}"
            for suffix, bcp47 in locale_mapping.values()
        )
    })
    keywords, vocabularies = _build_keywords(dataset)
    if keywords:
        extras['global_attributes']['keywords'] = ", ".join(keywords)
        if vocabularies:
            extras['global_attributes']["keywords_vocabulary"] = ", ".join(vocabularies)
    return extras


def _build_language_maps(dataset: Dataset) -> tuple[str, str, dict[str, tuple[str, str]]]:
    locale_mapping = {}
    default_info = _build_locale_info(dataset.data("default_locale"))
    if default_info is None:
        default_info = ("", "en", "en")

    for other_locale in dataset.data("other_locales", default=[]):
        info = _build_locale_info(other_locale)
        if info is None:
            continue
        locale_mapping[info[2]] = (info[0], info[1])
    return default_info[2], default_info[1], locale_mapping


def _build_locale_info(locale: Container | None) -> tuple[str, str, str] | None:
    if locale is None:
        return None

    netcdf_suffix = locale["netcdf_suffix"]
    if not netcdf_suffix:
        return None

    bcp47_code = locale["ietf_bcp47"]
    if not bcp47_code:
        return None

    return netcdf_suffix, bcp47_code, locale.data("internal", default="en")


def _other_languages(text: str | dict,
                     default_locale: str,
                     locale_mapping: dict[str, tuple[str, str]]) -> t.Iterable[tuple[str, str]]:
    if isinstance(text, dict):
        for key, value in text.items():
            if key == "und" or key == default_locale or key not in locale_mapping:
                continue
            yield locale_mapping[key][0], value


def _build_global_attributes(dataset: Dataset) -> dict[str, t.Any]:
    attrs = {}
    attrs.update(_extract_netcdf_attributes(dataset))
    attrs['id'] = dataset.guid
    attrs['naming_authority'] = dataset.naming_authority
    return attrs


def _build_variables(dataset: Dataset) -> list[tuple[str, str, str, dict[str, t.Any], Container]]:
    variables: list[Container] = dataset.data("variables", default=[])
    return [
        (
            variable["source_name"],
            variable["source_data_type"]["short_name"] if variable["source_data_type"] else "?",
            " ".join(
                x.strip()
                for x in variable["dimensions"].split(",")
            ) if variable["dimensions"] else "",
            _extract_netcdf_attributes(variable),
            variable
        )
        for variable in sorted(variables, key=lambda v: v.data("variable_order", default=0) or 0)
    ]


def _extract_netcdf_attributes(container: Container) -> dict[str, t.Any]:
    attrs = {}
    for fn in container.ordered_field_names():
        field = container.field(fn)
        if field is None or field.is_empty():
            continue
        netcdf_config = field.get_config("netcdf", default=None)
        if not netcdf_config:
            continue
        attrs.update(_extract_netcdf_attribute(
            field=field,
            mapping=netcdf_config.get("attribute_name", fn),
            processor=netcdf_config.get("processor", None),
            config=netcdf_config
        ))
    return attrs


def _extract_netcdf_attribute(field: Field,
                              mapping: str,
                              processor: str | None,
                              config: dict[str, t.Any]) -> dict[str, t.Any]:
    if processor is None:
        processor = _autodetect_processor(field)

    if processor is None:
        return {}
    elif processor == "text" or processor == "numeric":
        return { mapping: field.data() }
    elif processor == "datetime":
        return { mapping: field.data().isoformat() }
    elif processor == "key_value":
        return {
            k: v
            for k, v in field.data().items()
            if k
        }
    elif processor == "vocabulary":
        return _extract_vocabulary_value(field, mapping, config)
    elif processor == "contact":
        return _extract_contact(field, mapping, config)
    elif processor == "contacts_by_role":
        return _extract_contacts_by_role(field, mapping, config)
    elif processor == "licenses":
        return _extract_licenses(field, mapping, config)
    elif processor == "ref_system":
        return _extract_ref_system(field, mapping, config)
    else:
        try:
            obj = dynamic_object(processor)
            return obj(field, mapping, config)
        except DynamicObjectLoadError:
            zrlog.get_logger("dmd.netcdf").exception("Error loading dynamic processor to map to %s", mapping)
            pass
    return {}


def _extract_contact(field: Field,
                     mapping: str,
                     config: dict[str, t.Any]) -> dict[str, t.Any]:
    contact: Container | list[Container] | None = field.data()
    if not contact:
        return {}
    if isinstance(contact, Container):
        return _build_contact_attributes(
            contacts=[contact],
            allow_many=config.get("allow_many", False),
            separator=config.get("separator", ","),
            prefix=mapping
        )
    else:
        return _build_contact_attributes(
            t.cast(list[Container], contact),
            allow_many=config.get("allow_many", False),
            separator=config.get("separator", ","),
            prefix=mapping
        )


def _extract_contacts_by_role(field: Field,
                              mapping: str,
                              config: dict[str, t.Any]) -> dict[str, t.Any]:
    raw_contacts: Container | list[Container] | None = field.data()
    contacts: list[Container]
    if raw_contacts is None:
        contacts = []
    elif isinstance(raw_contacts, Container):
        contacts = [raw_contacts]
    else:
        contacts = t.cast(list[Container], raw_contacts)

    if not contacts:
        return {}

    separator = config.get("separator", ",")
    allow_many = config.get("allow_many", False)
    roles: dict[str, tuple[list[Container], dict[str, t.Any]]] = {}
    for role_name, sub_config in config.get("roles", {}).items():
        base_config = {
            "separator": separator,
            "allow_many": allow_many,
        }
        base_config.update(sub_config)
        roles[role_name] = ([], base_config)

    for contact in contacts:
        role = contact.data("role", default={}).get("short_name")
        if role and role in roles:
            roles[role][0].append(contact)

    attrs = {}
    for role_contacts, role_config in roles.values():
        attrs.update(_build_contact_attributes(contacts, **role_config))
    return attrs


def _build_contact_attributes(contacts: list[Container],
                              allow_many: bool,
                              separator: str,
                              prefix: str) -> dict[str, t.Any]:
    attributes: dict[str, str] = {}
    for contact in contacts:
        for key, value in _extract_contact_attributes(contact).items():
            full_key = f"{prefix}_{key}"
            if full_key in attributes:
                attributes[full_key] += separator + value
            else:
                attributes[full_key] = value
        if not allow_many:
            break
    return attributes


def _extract_contact_attributes(contact: Container) -> dict[str, str]:
    attributes: dict[str, str] = {}

    short_name = contact.data("short_name")
    org_name = contact.data("organization_name")
    if individual_name := contact.data("individual_name"):
        attributes["name"] = short_name or individual_name
        attributes["type"] = "individual"
        if org_name:
            attributes["institution"] = org_name
    elif position_name := contact.data("position_name"):
        attributes["name"] = short_name or position_name
        attributes["type"] = "position"
        if org_name:
            attributes["institution"] = org_name
    elif org_name:
        attributes["name"] = short_name or org_name
        attributes["type"] = "organization"

    if email := contact.data("email"):
        attributes["email"] = email

    if web_page := contact.data("web_resource"):
        try:
            url = web_page[0].get("url")
            if url:
                attributes["url"] = url
        except (IndexError, KeyError, TypeError):
            ...

    return attributes


def _extract_ref_system(field: Field,
                        mapping: str,
                        config: dict[str, t.Any]) -> dict[str, t.Any]:
    container: Container | None = field.data()
    if not isinstance(container, Container):
        return {}
    system: Container | None = container.data("id_system")
    if system is not None:
        return { mapping: f"{system.data("code_space", default="")}{container.data("code", default="")}" }
    else:
        return { mapping: container.data("code", default="") }


def _extract_licenses(field: Field,
                      mapping: str,
                      config: dict[str, t.Any]) -> dict[str, t.Any]:
    raw_licenses: Container | list[Container] | None = field.data()
    if raw_licenses is None:
        licenses = []
    elif isinstance(raw_licenses, Container):
        licenses = [raw_licenses]
    else:
        licenses = t.cast(list[Container], raw_licenses)

    if not licenses:
        return {}

    license_text_keys = {"und", }
    for license in licenses:
        description: dict[str, t.Any] | str = license.data("description")
        if isinstance(description, str):
            license_text_keys.add("und")
        else:
            license_text_keys.update(description.keys())

    license_texts = {
        key: []
        for key in license_text_keys
    }
    for license in licenses:
        description: dict[str, t.Any] | str= license.data("description")
        if isinstance(description, str):
            for key in license_text_keys:
                license_texts[key].append(description)
        elif isinstance(description, dict):
            for key in license_text_keys:
                if key in description:
                    license_texts[key].append(description[key])
                elif "und" in description:
                    license_texts[key].append(description["und"])
    separator = config.get("separator", "\n-----\n")
    return {mapping: {
        key: separator.join(values)
        for key, values in license_texts.items()
    }}


def _extract_vocabulary_value(field: Field,
                              mapping: str,
                              config: dict[str, t.Any]) -> dict[str, t.Any]:
    raw_data = field.data()
    if not raw_data:
        return {}
    values = []
    data = [raw_data] if isinstance(raw_data, (dict, VocabularyTerm)) else raw_data
    allow_many = config.get("allow_many", False)
    for term in data:
        if term is None:
            continue
        if not isinstance(term, (dict, VocabularyTerm)):
            zrlog.get_logger("dmd.netcdf").error("Expected a vocabulary term, found %s", term.__class__.__name__)
            continue
        values.append(term["short_name"])
        if not allow_many:
            break
    return {
        mapping: config.get("separator", ",").join(values)
    }


def _autodetect_processor(field: Field) -> str | None:
    data_type = field.get_config("data_type")
    if data_type in ("date", "datetime", "time"):
        return "datetime"
    if data_type in ("decimal", "integer", "float"):
        return "numeric"
    if data_type in ("email", "text", "multitext", "telephone", "url"):
        return "text"
    if data_type == "key_value":
        return "key_value"
    if data_type == "vocabulary":
        return "vocabulary"
    return None


def _build_keywords(dataset: Dataset) -> tuple[list[str], set[str]]:
    keywords = set()
    vocabs = set()
    for keyword in dataset.keywords():
        disp = keyword.to_display("en", use_prefixes=True)
        if disp["primary"]:
            keywords.add(disp["primary"])
        keywords.update(disp["secondary"].values())
        if disp["vocab"]:
            vocabs.add(disp["vocab"])
    keywords = list(kw.replace(",", "") for kw in keywords)
    keywords.sort()
    return keywords, vocabs