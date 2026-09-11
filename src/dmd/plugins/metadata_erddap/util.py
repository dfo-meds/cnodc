import zrlog
from dmd.containers.base import Field
import typing as t

from dmd.metadata.datasets import Dataset

TIME_PRECISION_MAP = {
    "month": "1970-01",
    "day": "1970-01-01",
    "hour": "1970-01-01T00Z",
    "minute": "1970-01-01T00:00Z",
    "second": "1970-01-01T00:00:00Z",
    "tenth_second": "1970-01-01T00:00:00.0Z",
    "hundredth_second": "1970-01-01T00:00:00.00Z",
    "millisecond": "1970-01-01T00:00:00.000Z",
}


def export_time_precision(field: Field,
                          mapping: str,
                          config: dict[str, t.Any]) -> dict[str, t.Any]:
    data = field.data()
    if data is not None:
        map_value = data["short_name"]
        if map_value in TIME_PRECISION_MAP:
            return {mapping: TIME_PRECISION_MAP[map_value]}
        else:
            # TODO: logging
            pass
    return {}


def preprocess_metadata_for_all(dataset, **kwargs):
    erddap_ds_id = dataset["erddap_dataset_id"]
    erddap_data_type = dataset['erddap_dataset_type']
    f = dataset.get_field('erddap_servers')
    servers = dataset['erddap_servers']
    if not (erddap_ds_id and erddap_data_type and servers):
        return
    dtype_dir = 'tabledap'
    dtype_format = 'TableDAP'
    if erddap_data_type['short_name'].startswith('EDDGrid'):
        dtype_dir = 'griddap'
        dtype_format = 'GridDAP'
    erddap_distribution_servers = []
    for server in servers:
        base_url = server['base_url']
        if not base_url:
            continue
        erddap_distribution_servers.append({
            'responsibles': server['responsibles'],
            'links': [
                {
                    'url': _assemble_erddap_link(base_url, dtype_dir, erddap_ds_id),
                    'protocol': {
                        'short_name': f'ERDDAP:{dtype_dir}'
                    },
                    'function': {
                        'short_name': 'download'
                    },
                    'name': {
                        'en': 'ERDDAP Web Site',
                        'fr': 'Site web ERDDAP',
                    },
                    'goc_content_type': {
                        'short_name': 'dataset',
                        'display': {
                            'en': 'Dataset',
                            'fr': 'Données'
                        }
                    },
                    'goc_formats': [{
                        # 'short_name': f'ERDDAP {dtype_format} Dataset'
                        # TODO: have to get the new short_name approved first
                        'short_name': 'HTML'
                    }],
                    'goc_languages': ['eng'],
                },
                {
                    'url': _assemble_erddap_link(base_url, dtype_dir, erddap_ds_id, 'fr'),
                    'protocol': {
                        'short_name': f'ERDDAP:{dtype_dir}'
                    },
                    'function': {
                        'short_name': 'download'
                    },
                    'name': {
                        'en': 'ERDDAP Web Site (French)',
                        'fr': 'Site web ERDDAP (français)',
                    },
                    'goc_content_type': {
                        'short_name': 'dataset',
                        'display': {
                            'en': 'Dataset',
                            'fr': 'Données'
                        }
                    },
                    'goc_formats': [{
                        # 'short_name': f'ERDDAP {dtype_format} Dataset'
                        # TODO: have to get the new short_name approved first
                        'short_name': 'HTML'
                    }],
                    'goc_languages': ['fra'],
                }
            ]
        })
    if 'iso19115_custom_distribution_channels' in kwargs:
        kwargs['iso19115_custom_distribution_channels'].extend(erddap_distribution_servers)
    else:
        return {
            'iso19115_custom_distribution_channels': erddap_distribution_servers
        }


def _assemble_erddap_link(erddap_base, dtype_dir, dataset_id, lang='en'):
    if lang != 'en':
        lang = f"{lang}/"
    else:
        lang = ""
    if not erddap_base[-1] == '/':
        return f"{erddap_base}/{lang}{dtype_dir}/{dataset_id}".strip()
    else:
        return f"{erddap_base}{lang}{dtype_dir}/{dataset_id}".strip()


def preprocess_metadata_for_erddap_xml(dataset: Dataset, **kwargs):
    from dmd.plugins.metadata_netcdf.util import _preprocess_for_netcdf
    vars = _preprocess_for_netcdf(dataset, **kwargs)
    subset_vars = []
    altitude_proxy = None
    cdm_profile_vars = []
    cdm_timeseries_vars = []
    cdm_trajectory_vars = []
    for var in dataset['variables']:
        actual_name = var['destination_name']
        if not actual_name:
            actual_name = var['source_name']
        if var['allow_subsets']:
            subset_vars.append(actual_name)
        if var['altitude_proxy']:
            altitude_proxy = actual_name
        if var['cf_role']:
            if var['cf_role']['short_name'] == 'profile_id':
                cdm_profile_vars.append(actual_name)
            if var['cf_role']['short_name'] == 'timeseries_id':
                cdm_timeseries_vars.append(actual_name)
            if var['cf_role']['short_name'] == 'trajectory_id':
                cdm_trajectory_vars.append(actual_name)
        if var['erddap_role']:
            if var['erddap_role']['short_name'] == 'profile_extra':
                cdm_profile_vars.append(actual_name)
            if var['erddap_role']['short_name'] == 'timeseries_extra':
                cdm_timeseries_vars.append(actual_name)
            if var['erddap_role']['short_name'] == 'trajectory_extra':
                cdm_trajectory_vars.append(actual_name)
    if cdm_profile_vars:
        vars['global_attributes']['cdm_profile_variables'] = ','.join(cdm_profile_vars)
    if cdm_trajectory_vars:
        vars['global_attributes']['cdm_trajectory_variables'] = ','.join(cdm_trajectory_vars)
    if cdm_timeseries_vars:
        vars['global_attributes']['cdm_timeseries_variables'] = ','.join(cdm_timeseries_vars)
    if subset_vars:
        vars['global_attributes']['subsetVariables'] = ','.join(subset_vars)
    if altitude_proxy:
        vars['global_attributes']['cdm_altitude_proxy'] = altitude_proxy
    if dataset['info_link'] and dataset['info_link']['url']:
        vars['global_attributes']['infoUrl'] = dataset['info_link']['url']
    else:
        vars['global_attributes']['infoUrl'] = ""
    if 'summary' not in vars['global_attributes']:
        vars['global_attributes']['summary'] = ""
    return vars
