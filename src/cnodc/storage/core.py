from autoinject import injector
from cnodc.storage.base import BaseStorageHandle
from cnodc.storage.azure_files import AzureFileHandle
from cnodc.storage.azure_blob import AzureBlobHandle
from cnodc.storage.ftp import FTPHandle
from cnodc.storage.sftp import SFTPHandle
from cnodc.storage.local import LocalHandle
import typing as t
import pathlib
from cnodc.util import HaltFlag


@injector.injectable_global
class StorageController:
    """Controller class that identifies the correct handler for a given string.

        https://STORAGE.blob.core.windows.net/CONTAINER -> AzureBlobHandle
        https://STORAGE.files.core.windows.net/SHARE -> AzureFileHandle
        ftp://PATH -> FTPHandle
        ftps://PATH -> FTPHandle
        sftp://PATH -> SFTPHandle
        (default or path-like) -> LocalHandle
    """

    def __init__(self):
        self.handle_classes = [
            AzureFileHandle,
            AzureBlobHandle,
            # FTPHandle,
            # SFTPHandle,
        ]
        self.default_handle = LocalHandle

    def get_handle(self, file_path: t.Union[str, pathlib.Path], halt_flag: HaltFlag = None) -> BaseStorageHandle:
        """Build an appropriate handle for the given file path."""
        if isinstance(file_path, pathlib.Path):
            return LocalHandle(file_path.resolve(), halt_flag=halt_flag)
        for cls in self.handle_classes:
            if cls.supports(file_path):
                return cls.build(file_path, halt_flag=halt_flag)
        return self.default_handle.build(file_path, halt_flag=halt_flag)
