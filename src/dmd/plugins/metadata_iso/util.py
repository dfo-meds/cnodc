import functools
import typing as t
from autoinject import injector, auto

from dmd.containers.base import ValidationResult, Container, ContainerLoader
from dmd.containers.keywords import KeywordGroup
from dmd.metadata.datasets import Dataset
from gcapp.i18n import MLString


def validate_citation(profile_name: str,
                      obj_path: list[str | MLString],
                      citation: Container,
                      memo) -> t.Iterable[ValidationResult]:
    if (citation["id_system"] or citation["id_description"]) and not citation["id_code"]:
        yield ValidationResult(profile_name, obj_path, "iso19115.citation_without_id_code", "warning")


def validate_use_constraint(profile_name: str,
                            obj_path: list[str | MLString],
                            use_constraint: Container,
                            memo) -> t.Iterable[ValidationResult]:
    if use_constraint["classification"]:
        if any(use_constraint[x] for x in ("access_constraints", "use_constraints", "other_constraints")):
            yield ValidationResult(profile_name, obj_path, "iso19115.security_constraint_has_legal_property")
    elif any(use_constraint[x] for x in ("user_notes", "classification_system", "handling_description")):
        yield ValidationResult(profile_name, obj_path, "iso19115.security_constraint_missing_classification")
    else:
        if not use_constraint["other_constraints"]:
            if any(x["short_name"] == "otherRestrictions" for x in use_constraint.data("use_constraints", default=[])):
                yield ValidationResult(profile_name, obj_path, "iso19115.legal_constraint_missing_other_for_use")
            if any(x["short_name"] == "otherRestrictions" for x in use_constraint.data("access_constraints", default=[])):
                yield ValidationResult(profile_name, obj_path, "iso19115.legal_constraint_missing_other_for_access")


def validate_contact(profile_name: str,
                     obj_path: list[str | MLString],
                     contact: Container,
                     memo) -> t.Iterable[ValidationResult]:
    if (contact["id_system"] or contact["id_description"]) and not contact["id_code"]:
        yield ValidationResult(profile_name, obj_path, "iso19115.contact_without_id_code", "warning")
    if contact["organization_name"]:
        if contact["individual_name"]:
            yield ValidationResult(profile_name, obj_path, "iso19115.organization_with_individual_name")
        if contact["position_name"]:
            yield ValidationResult(profile_name, obj_path, "iso19115.organization_with_position_name")
        for individual in contact.data("individuals", default=[]):
            if individual["organization_name"]:
                yield ValidationResult(profile_name, obj_path, "iso19115.related_individual_with_organization_name")
            if individual["logo"]:
                yield ValidationResult(profile_name, obj_path, "iso19115.related_individual_with_logo")
    else:
        if contact["logo"]:
            yield ValidationResult(profile_name, obj_path, "iso19115.individual_with_logo")
        if contact["individuals"]:
            yield ValidationResult(profile_name, obj_path, "iso19115.individual_with_individuals")


def validate_dataset(profile_name: str,
                     obj_path: list[str | MLString],
                     dataset: Dataset,
                     memo) -> t.Iterable[ValidationResult]:
    if (dataset["processing_system"] or dataset["processing_desc"]) and not dataset["processing_code"]:
        yield ValidationResult(profile_name, obj_path, "iso19115.processing_system_without_code", "warning")
    if (dataset["dataset_id_desc"] or dataset["dataset_id_system"]) and not dataset["dataset_id_code"]:
        yield ValidationResult(profile_name, obj_path, "iso19115.dataset_system_without_code", "warning")


def preprocess_for_iso19115(dataset: Dataset, **kwargs):
    locale_mapping = {}
    def_loc = dataset.data("default_locale")
    default_locale = def_loc['a2_language'] if def_loc else "en"
    locale_mapping[default_locale] = def_loc['language'] if def_loc else "eng"
    olocales = dataset.data("other_locales") or []
    supported = []
    for other_loc in olocales:
        locale_mapping[other_loc['a2_language']] = other_loc['language']
        supported.append(other_loc['a2_language'])
    dataset_maintenance = []
    metadata_maintenance = []
    for maintenance in dataset.data("iso_maintenance") or []:
        if maintenance['scope']['short_name'] == "dataset":
            dataset_maintenance.append(maintenance)
        elif maintenance['scope']['short_name'] == "metadata":
            metadata_maintenance.append(maintenance)

    # This handles the dataset-wide acquisition information, which is what we usually provide
    # If more specifics are needed, we'll need a new attribute and append the result here.
    acq_info = []
    missions = dataset.data("missions")
    instruments = dataset.data("instruments")
    platforms = dataset.data("platforms")
    if missions or instruments or platforms:
        dataset_wide_info = {
            'missions': missions,
            'platforms': platforms,
            'instruments': instruments,
            'scope': {
                'short_name': 'dataset',
            },
            'dataset': {
                'en': 'current dataset',
                'fr': 'jeu de données actuel'
            }
        }
        acq_info.append(dataset_wide_info)
    refs = {}
    return {
        "acquisition_info": acq_info,
        "responsibilities": [
            {
                "role": {"short_name": "owner"},
                "contact": dataset['metadata_owner']
            }
        ],
        "dates": [
            {
                "type": {"short_name": "created"},
                "date": dataset.created_date
            },
            {
                "type": {"short_name": "revision"},
                "date": dataset.metadata_modified_date
            },
        ],
        "default_locale": default_locale,
        "locale_mapping": locale_mapping,
        "dataset_citation": {
            "title": dataset.data("title"),
            "publication_date": dataset["publication_date"],
            "revision_date": dataset["revision_date"],
            "creation_date": dataset["creation_date"],
            "id_code": dataset["dataset_id_code"],
            "id_system": dataset["dataset_id_system"],
            "id_description": dataset["dataset_id_desc"],
            "responsibles": dataset["responsibles"],
            'resource': dataset['info_link']
        },
        "dataset_maintenance": dataset_maintenance,
        "metadata_maintenance": metadata_maintenance,
        "grouped_keywords": separate_keywords(dataset.keywords()),
        "check_alt_langs": functools.partial(_has_other_languages, supported_locales=supported),
        '_refs': refs,
        'check_platform': functools.partial(_has_type_been_rendered, refs, 'platform'),
        'check_instrument': functools.partial(_has_type_been_rendered, refs, 'instrument'),
        'check_mission': functools.partial(_has_type_been_rendered, refs, 'mission'),
        'clean_id': _clean_xml_id,
        'get_platform_instruments': get_platform_instruments
    }


def separate_keywords(keywords):
    groups = {}
    for keyword in keywords:
        group = keyword.thesaurus_group()
        if group not in groups:
            groups[group] = KeywordGroup(keyword.thesaurus)
        groups[group].append(keyword)
    return groups


def _has_other_languages(language_dict, default_locale, supported_locales):
    for key in language_dict:
        if key == "und" or key == default_locale:
            continue
        if key not in supported_locales:
            continue
        return True
    return False


def _has_type_been_rendered(ref_dict: dict, type_name: str, unique_id):
    if not unique_id:
        return False
    if type_name not in ref_dict:
        ref_dict[type_name] = set()
    if unique_id not in ref_dict[type_name]:
        ref_dict[type_name].add(unique_id)
        return False
    return True


def _clean_xml_id(type_name, xml_id, db_id):
    new_xml_id = ""
    if xml_id:
        for ltr in xml_id:
            if ltr in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-":
                new_xml_id += ltr
    if not new_xml_id:
        new_xml_id = "_" + str(db_id)
    return f"{type_name}_{new_xml_id}"


@injector.inject
def get_platform_instruments(platform: Container, loader: ContainerLoader = auto()):
    for instrument in loader.stream_containers("instrument"):
        if instrument["mounted_on"] and instrument['mounted_on'].container_id == platform.container_id:
            yield instrument
