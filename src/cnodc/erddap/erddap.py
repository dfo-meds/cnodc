import requests
import zirconium as zr
import zrlog
from autoinject import injector
import enum


class ReloadFlag(enum.Enum):

    SOFT = 0
    BAD_FILES = 1
    HARD = 2


@injector.injectable
class ErddapController:

    app_config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._username = self.app_config.as_str(("erddaputil", "username"))
        self._password = self.app_config.as_str(("erddaputil", "password"))
        self._base_url = self.app_config.as_str(("erddaputil", "base_url"))
        self._broadcast_mode = self.app_config.as_str(('erddaputil', 'broadcast'), default='no')
        self._broadcast_flag = 0
        self._config_good = None
        self._log = zrlog.get_logger("cnodc.erddap")
        self._check_credentials()

    def _check_credentials(self):
        if self._config_good is None:
            self._config_good = True
            if self._username is None or self._username == '':
                self._config_good = False
                self._log.error(f"Missing value for erddaputil.username")
            if self._password is None or self._password == '':
                self._config_good = False
                self._log.error(f"Missing value for erddaputil.password")
            if self._base_url is None or self._base_url == '':
                self._config_good = False
                self._log.error(f"Missing value for erddaputil.base_url")
            elif not (self._base_url.startswith("http://") or self._base_url.startswith("https://")):
                self._config_good = False
                self._log.error(f"Invalid value for erddaputil.base_url")
            else:
                self._base_url = self._base_url.rstrip("/")
            if self._broadcast_mode == 'cluster':
                self._broadcast_flag = 1
            elif self._broadcast_flag == 'global':
                self._broadcast_flag = 2
            elif self._broadcast_flag != 'no':
                self._log.warning(f"Unrecognized value [{self._broadcast_mode}] for erddaputil.broadcast_mode, defaulting to [no]")
        return self._config_good

    def reload_dataset(self,
                       dataset_id: str,
                       flag: ReloadFlag = ReloadFlag.SOFT) -> bool:
        try:
            resp = self._make_authenticated_request("datasets/reload", "POST", {
                'dataset_id': dataset_id,
                'flag': flag.value,
                '_broadcast': self._broadcast_flag
            })
            if not resp['success']:
                self._log.error(f"Remote error while requesting ERDDAP dataset reload: {resp['message'] if 'message' in resp else 'unknown'}")
                return False
            return True
        except Exception as ex:
            self._log.exception(f"Exception while requesting ERDDAP dataset reload")
            return False

    def _make_authenticated_request(self, endpoint: str, method: str, json_data: dict):
        auth_key = f'{self._username}:{self._password}'.encode('utf-8')
        headers = {
            'Authorization': f'Basic {auth_key}'
        }
        resp = requests.request(method, f"{self._base_url}/{endpoint}", json=json_data, headers=headers)
        resp.raise_for_status()
        return resp.json()
