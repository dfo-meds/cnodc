from autoinject import injector
from .base import DirFileHandle
from .azure_files import AzureFileHandle
from .azure_blob import AzureBlobHandle
from .ftp import FTPHandle
from .sftp import SFTPHandle
from .local import LocalHandle


@injector.injectable_global
class FileController:

    def __init__(self):
        self.handle_classes = [
            AzureFileHandle,
            AzureBlobHandle,
            FTPHandle,
            SFTPHandle,
        ]
        self.default_handle = LocalHandle

    def get_handle(self, file_path: str) -> DirFileHandle:
        for cls in self.handle_classes:
            if cls.supports(file_path):
                return cls.build(file_path)
        return self.default_handle.build(file_path)
