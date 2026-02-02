from __future__ import annotations
from autoinject import injector

from cnodc.storage.base import BaseStorageHandle, StorageTier
from cnodc.storage.azure_files import AzureFileHandle
from cnodc.storage.azure_blob import AzureBlobHandle
from cnodc.storage.ftp import FTPHandle
from cnodc.storage.sftp import SFTPHandle
from cnodc.storage.local import LocalHandle
import datetime
import typing as t
import pathlib
import enum
from cnodc.util import HaltFlag


class AccessLevel(enum.Enum):
    General = "GENERAL"
    Embargoed = "EMBARGOED"
    Controlled = "CONTROLLED"


class SecurityLevel(enum.Enum):
    Unclassified = "UNCLASSIFIED"
    ProtectedA = "PROTECTED A"



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

    def build_metadata(self,
                       program_name: str = "??",
                       dataset_name: str = "??",
                       cost_unit: str = "MEDS",
                       gzip: bool = False,
                       access_level: AccessLevel = AccessLevel.General,
                       security_label: SecurityLevel = SecurityLevel.Unclassified,
                       release_date: t.Optional[datetime.datetime] = None,
                       automate_release: bool = False,
                       storage_tier: t.Optional[StorageTier] = StorageTier.FREQUENT):
        metadata = {
            'Program': program_name,
            'Dataset': dataset_name,
            'CostUnit': cost_unit,
            'Gzip': 'YES' if gzip else 'NO',
            'AccessLevel': access_level.value,
            'SecurityLabel': security_label.value,
            'AutomatedRelease': 'YES' if automate_release else 'NO',
            'ReleaseDate': release_date.isoformat() if release_date else '',
            'StorageTier': storage_tier.value if storage_tier else ''
        }
        return metadata