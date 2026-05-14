import zrlog
import zirconium as zr
from autoinject import injector

from medsutil.metrics import Counter
from medsutil.web import request

@injector.injectable
class DataManagerController:

    app_config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._log = zrlog.get_logger("cnodc.dmd")
        self._counter = Counter("requests_total", namespace="pipeman", subsystem="dmd", labelnames=("outcome",))

    def create_dataset(self, request_body: dict):
        return self._create_upsert_dataset(request_body, False)

    def upsert_dataset(self, request_body: dict):
        return self._create_upsert_dataset(request_body, True)

    def _create_upsert_dataset(self, metadata: dict, allow_upsert: bool = False):
        try:
            headers = {
                'Authorization' : f"Bearer {self._get_auth_header()}"
            }
            endpoint = self._get_api_endpoint('api/create-dataset' if not allow_upsert else 'api/upsert-dataset')
            result = request('POST', endpoint, json=metadata, headers=headers)
            res = result.json()['guid']
            self._counter.labels(outcome="success").inc()
            return res
        except Exception:
            self._counter.labels(outcome="error").inc()
            raise

    def _get_api_endpoint(self, ep) -> str:
        return f"{self.app_config.as_str(("dmd", "base_url"), default='').rstrip('/')}/{ep.lstrip('/')}"

    def _get_auth_header(self) -> str:
        return self.app_config.as_str(("dmd", "auth_token"), default='')
