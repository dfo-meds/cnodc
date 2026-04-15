import functools
from abc import ABC

import urllib3.exceptions
import zirconium as zr
from autoinject import injector
from azure.core import exceptions as ace

from medsutil.storage.base import UrlBaseHandle
from medsutil.storage.interface import StorageError


class AzureBaseHandle(UrlBaseHandle, ABC):

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _get_storage_account_connection_details(self, domain: str) -> dict:
        return {
            "storage_account": domain[:-22],
            "storage_url": domain,
            "connection_string": self.config.as_str(("azure", "storage", domain[:-22], "connection_string"), default=None),
        }

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
