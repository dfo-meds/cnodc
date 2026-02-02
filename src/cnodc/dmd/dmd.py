import json

import zrlog
import requests
import zirconium as zr
from autoinject import injector

from cnodc.dmd.metadata import DatasetMetadata


@injector.injectable
class DataManagerController:

    app_config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._log = zrlog.get_logger("cnodc.dmd")

    def create_dataset(self, metadata: DatasetMetadata):
        try:
            headers = {
                'Authorization' : self._get_auth_header()
            }
            result = requests.post(
                self._get_api_endpoint("api/create-dataset"),
                json=metadata.build_request_body(),
                headers=headers
            )
            result.raise_for_status()
            return json.loads(result.content.decode('utf-8'))['guid']
        except Exception as ex:
            self._log.exception("An exception occurred while creating a new dataset")
            return None

    def upsert_dataset(self, metadata: DatasetMetadata):
        try:
            headers = {
                'Authorization' : self._get_auth_header()
            }
            result = requests.post(
                self._get_api_endpoint("api/upsert-dataset"),
                json=metadata.build_request_body(),
                headers=headers
            )
            result.raise_for_status()
            return json.loads(result.content.decode('utf-8'))['guid']
        except Exception as ex:
            self._log.exception("An exception occurred while creating a new dataset")
            return None

    def _get_api_endpoint(self, ep) -> str:
        return self.app_config.as_str(("dmd", "base_url"), default=None)

    def _get_auth_header(self) -> str:
        return self.app_config.as_str(("dmd", "auth_token"), default=None)
