import json

import zrlog
import requests
import zirconium as zr
from autoinject import injector
from requests import HTTPError, ConnectionError, Timeout, TooManyRedirects

from cnodc.programs.dmd.metadata import DatasetMetadata
from cnodc.util import CNODCError


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
        result = None
        try:
            headers = {
                'Authorization' : self._get_auth_header()
            }
            endpoint = self._get_api_endpoint('api/create-dataset' if not allow_upsert else 'api/upsert-dataset')
            result = requests.post(
                endpoint,
                json=metadata.build_request_body(),
                headers=headers
            )
            result.raise_for_status()
            return json.loads(result.content.decode('utf-8'))['guid']
        except Exception as ex:
            self._handle_response(result, ex, isinstance(ex, (HTTPError, ConnectionError, Timeout, TooManyRedirects)))

    def _handle_response(self, result, ex, is_recoverable):
            raise CNODCError(f"An error occurred while trying to create a new DMD entry: {type(ex)}: {str(ex)}", 'DMD', 1000, is_recoverable=is_recoverable) from ex

    def _get_api_endpoint(self, ep) -> str:
        return f"{self.app_config.as_str(("dmd", "base_url"), default='').rstrip('/')}/{ep.lstrip('/')}"

    def _get_auth_header(self) -> str:
        return self.app_config.as_str(("dmd", "auth_token"), default=None)
