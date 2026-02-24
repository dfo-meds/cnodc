"""Integration with ERDDAP to reload a dataset."""
import requests
import zirconium as zr
import zrlog
from autoinject import injector
import enum
import typing as t

from cnodc.util import CNODCError


class ReloadFlag(enum.Enum):
    """Flags to send to ERDDAPUtil."""

    SOFT = 0
    BAD_FILES = 1
    HARD = 2


@injector.injectable
class ErddapController:
    """Controller class for interacting with ERDDAP"""

    app_config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._erddap_configs = self.app_config.as_dict(("erddaputil",))
        self._valid_configs = {}
        self._log = zrlog.get_logger("cnodc.erddap")

    def _get_config(self, cluster_name: t.Optional[str] = None):
        """Retrieve the configuration for the given cluster name."""
        cluster_name = cluster_name or "__default"
        if cluster_name not in self._valid_configs:
            self._valid_configs[cluster_name] = None
            if cluster_name == '__default' or cluster_name in self._erddap_configs:
                config = self._erddap_configs if cluster_name == '__default' else self._erddap_configs[cluster_name]
                good_config = True
                if 'username' not in config or not config['username']:
                    self._log.error(f"Missing username for ERDDAP cluster {cluster_name}")
                    good_config = False
                if 'password' not in config or not config['password']:
                    self._log.error(f"Missing password for ERDDAP cluster {cluster_name}")
                    good_config = False
                if 'base_url' not in config or not config['base_url']:
                    self._log.error(f"Missing base_url for ERDDAP cluster {cluster_name}")
                    good_config = False
                elif not isinstance(config['base_url'], str):
                    self._log.error(f"base_url is not a string for ERDDAP cluster {cluster_name}")
                    good_config = False
                elif not (config['base_url'].startswith("http://") or config['base_url'].startswith("https://")):
                    self._log.error(f"Invalid base_url for ERDDAP cluster {cluster_name}")
                    good_config = False
                else:
                    config['base_url'] = config['base_url'].rstrip('/')
                if 'broadcast_mode' not in config:
                    config['broadcast_mode'] = 'no'
                elif config['broadcast_mode'] not in ('cluster', 'global', 'no'):
                    self._log.warning(f"Invalid broadcast_mode for ERDDAP cluster {cluster_name}, defaulting to [no]")
                    config['broadcast_mode'] = 'no'
                if good_config:
                    self._valid_configs[cluster_name] = config
        return self._valid_configs[cluster_name]

    def reload_dataset(self,
                       dataset_id: str,
                       flag: ReloadFlag = ReloadFlag.SOFT,
                       cluster_name: str = None) -> bool:
        """Reload a given dataset."""
        try:
            resp = self._make_authenticated_request(
                endpoint="datasets/reload",
                method="POST",
                json_data={
                    'dataset_id': dataset_id,
                    'flag': flag.value
                },
                cluster_name=cluster_name
            )
            if not resp['success']:
                self._log.error(f"Remote error while requesting ERDDAP dataset reload: {resp['message'] if 'message' in resp else 'unknown'}")
                return False
            return True
        except Exception as ex:
            self._log.exception(f"Exception while requesting ERDDAP dataset reload")
            return False

    def _make_authenticated_request(self, endpoint: str, method: str, json_data: dict, cluster_name: str = None):
        """Make an HTTP call to the endpoint with appropriate authentication."""
        config = self._get_config(cluster_name)
        if config is None:
            raise CNODCError(f"Invalid ERDDAP configuration, see logs for more details", "ERDDAPUTIL", 1000, is_recoverable=True)
        if config['broadcast_mode'] == 'cluster':
            json_data['_broadcast'] = 1
        elif config['broadcast_mode'] == 'global':
            json_data['_broadcast'] = 2
        auth_key = f'{config["username"]}:{config["password"]}'.encode('utf-8')
        headers = {
            'Authorization': f'Basic {auth_key}'
        }
        resp = requests.request(method, f"{config['base_url']}/{endpoint}", json=json_data, headers=headers)
        resp.raise_for_status()
        return resp.json()
