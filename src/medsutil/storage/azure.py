import functools
from abc import ABC
import typing as t

import urllib3.exceptions
import zirconium as zr
from autoinject import injector
from azure.core import exceptions as ace
from azure.storage.blob import ContainerClient, BlobClient
from azure.storage.fileshare import ShareClient, ShareDirectoryClient, ShareFileClient
from azure.core.pipeline.transport._requests_basic import RequestsTransport

from medsutil.exceptions import CodedError
from medsutil.storage.base import UrlBaseHandle
from medsutil.storage.interface import StorageError

import atexit

class MyRequestsTransport(RequestsTransport):

    def __init__(self):
        super().__init__()
        self._open_count = 0

    def __enter__(self):
        self._open_count += 1
        return super().__enter__()

    def __exit__(self, *args, **kwargs):
        self._open_count -= 1
        if self._open_count <= 0:
            self._open_count = 0
            super().__exit__()

    def open(self):
        super().open()
        self._has_been_opened = False

    def send(self, request, *args, **kwargs):
        return super().send(request, *args, **kwargs)

def long_lived_requests_transport():
    x = MyRequestsTransport()
    x.__enter__()
    atexit.register(x.__exit__)
    return x

@injector.injectable
class AzureClientPool:


    def __init__(self):
        self._share_cache: dict[str, ShareClient] = {}
        self._container_cache: dict[str, ContainerClient] = {}
        self._transport = MyRequestsTransport()

    def _get_account_name(self, conn_string: str) -> str:
        pieces = conn_string.split(';')
        for piece in pieces:
            if piece.startswith('AccountName='):
                return piece[12:]
        raise ValueError('Invalid connection string')

    def get_share(self, conn_string: str, share_name: str) -> ShareClient:
        account_name = self._get_account_name(conn_string)
        key = f"{account_name}_{share_name}"
        if key not in self._share_cache:
            self._share_cache[key] = self._build_share_client(conn_string, share_name)
        return self._share_cache[key]

    def get_container(self, conn_string: str, container_name: str) -> ContainerClient:
        account_name = self._get_account_name(conn_string)
        key = f"{account_name}_{container_name}"
        if key not in self._container_cache:
            self._container_cache[key] = self._build_container_client(conn_string, container_name)
        return self._container_cache[key]

    def _build_share_client(self, conn_string: str, share_name: str) -> ShareClient:
        return ShareClient.from_connection_string(conn_string, share_name, transport=self._transport)

    def _build_container_client(self, conn_string: str, container_name: str) -> ContainerClient:
        return ContainerClient.from_connection_string(conn_string, container_name, transport=self._transport)



class AzureBaseHandle(UrlBaseHandle, ABC):

    config: zr.ApplicationConfig = None
    client_pool: AzureClientPool = None

    @injector.construct
    def __init__(self, *args, essential_domain: str, **kwargs):
        super().__init__(*args, **kwargs)
        paths = [x for x in self.url_path().split('/') if x]
        self._hard_prefix = str(paths[0]) if paths else None
        self._essential_domain = essential_domain

    def _get_client(self, client_type: t.Literal["container", "blob", "directory", "file"]):
        return self._with_cache('_build_client', self._build_client, client_type, cache_parameters=(client_type,))

    def _build_client(self, client_type: t.Literal["container", "blob", "directory", "file"]):
        try:
            account_name, share_or_container_name = self._parse_url_for_account_info()
            conn_string = self._get_connection_string(account_name)
            if client_type == 'container':
                return self.client_pool.get_container(conn_string, share_or_container_name)
            elif client_type == 'share':
                return self.client_pool.get_share(conn_string, share_or_container_name)
            else:
                raise ValueError(f"Unsupported client type [{client_type}]")
        except Exception as ex:
            if isinstance(ex, CodedError):
                raise
            else:
                raise StorageError(str(ex), 9000) from ex

    def _parse_url_for_account_info(self) -> tuple[str, str]:
        url_parts = self.parse_url()
        domain = url_parts.hostname
        if domain is None or not domain.endswith(self._essential_domain):
            raise StorageError(f"Invalid hostname", 4001)
        account_name = domain[:-1 * len(self._essential_domain)]
        path_parts = [x.strip() for x in url_parts.path.lstrip('/').split('/') if x.strip()]
        if len(path_parts) < 1 or path_parts[0] == "":
            raise StorageError(f"Missing share name", 4002)
        return account_name, path_parts[0]

    def _get_full_path(self):
        url_parts = self.parse_url()
        path_parts = [x.strip() for x in url_parts.path.lstrip('/').split('/') if x.strip()]
        return '/'.join(path_parts[1:]) if len(path_parts) > 1 else ""


    def _get_connection_string(self, account_name: str) -> str:
        return t.cast(str, self.config.as_str(('storage', 'azure', account_name, 'connection_string'), default=''))

    def _replace_path(self, new_path: str, as_dir: bool | None) -> str:
        if self._hard_prefix is not None and not new_path.startswith((self._hard_prefix, '/' + self._hard_prefix)):
            new_path = f'/{self._hard_prefix}/{new_path.lstrip('/')}'
        return self._replace_path_in_url(self._path, new_path, as_dir)


def wrap_azure_errors(cb):
    """Converts Azure storage errors into CNODCErrors with an appropriate is_recoverable setting."""

    @functools.wraps(cb)
    def _inner(*args, **kwargs):
        try:
            return cb(*args, **kwargs)
        except ace.AzureError as ex:
            if ex.inner_exception is not None:
                if isinstance(ex.inner_exception, urllib3.exceptions.ConnectTimeoutError):
                    raise StorageError(f"Azure: Connection timeout error: {ex.__class__.__name__}: {str(ex)}", 3001, is_transient=True) from ex
                elif isinstance(ex.inner_exception, ConnectionError):
                    raise StorageError(f"Azure: Connection error: {ex.__class__.__name__}: {str(ex)}", 3002, is_transient=True) from ex
            elif isinstance(ex, ace.ClientAuthenticationError):
                raise StorageError(f"Azure: client authentication error: {ex.__class__.__name__}: {str(ex)}", 3003, is_transient=False) from ex
            elif isinstance(ex, ace.ResourceNotFoundError):
                raise StorageError(f"Azure: Resource not found error: {ex.__class__.__name__}: {str(ex)}", 3004, is_transient=False) from ex
            elif isinstance(ex, ace.ResourceExistsError):
                raise StorageError(f"Azure: Resource already exists error: {ex.__class__.__name__}: {str(ex)}", 3005, is_transient=False) from ex
            raise StorageError(f"Azure: {ex.__class__.__name__}: {str(ex)}", 3000) from ex

    return _inner
