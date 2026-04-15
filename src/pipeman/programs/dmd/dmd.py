import zrlog
import zirconium as zr
from autoinject import injector

from pipeman.programs.dmd.metadata import DatasetMetadata
from medsutil.web import web_request


@injector.injectable
class DataManagerController:

    app_config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._log = zrlog.get_logger("cnodc.dmd")

    def create_dataset(self, metadata: DatasetMetadata):
        return self._create_upsert_dataset(metadata, False)

    def upsert_dataset(self, metadata: DatasetMetadata):
        return self._create_upsert_dataset(metadata, True)

    def _create_upsert_dataset(self, metadata: DatasetMetadata, allow_upsert: bool = False):
        headers = {
            'Authorization' : self._get_auth_header()
        }
        endpoint = self._get_api_endpoint('api/create-dataset' if not allow_upsert else 'api/upsert-dataset')
        result = web_request('POST', endpoint,
            json=metadata.build_request_body(),
            headers=headers
        )
        return result.json()['guid']

    def _get_api_endpoint(self, ep) -> str:
        return f"{self.app_config.as_str(("dmd", "base_url"), default='').rstrip('/')}/{ep.lstrip('/')}"

    def _get_auth_header(self) -> str:
        return self.app_config.as_str(("dmd", "auth_token"), default='')
