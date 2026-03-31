from __future__ import annotations
from autoinject import injector

from cnodc.storage.base import BaseStorageHandle, StorageTier, StorageError
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
import cnodc.util.awaretime as awaretime


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
        ftpse://PATH -> FTPHandle
        sftp://PATH -> SFTPHandle
        (default or path-like) -> LocalHandle
    """

    def __init__(self):
        self.handle_classes = [
            AzureFileHandle,
            AzureBlobHandle,
            FTPHandle,
            # SFTPHandle,
        ]
        self.default_handle = LocalHandle

    def get_handle(self,
                   file_path: t.Union[str, pathlib.Path, None],
                   halt_flag: HaltFlag = None,
                   raise_ex: bool = False) -> t.Optional[BaseStorageHandle]:
        """Build an appropriate handle for the given file path."""
        if file_path is not None and file_path != '':
            if isinstance(file_path, pathlib.Path):
                return LocalHandle(file_path.resolve(), halt_flag=halt_flag)
            for cls in self.handle_classes:
                if cls.supports(file_path):
                    return cls.build(file_path, halt_flag=halt_flag)
            if self.default_handle.supports(file_path):
                return self.default_handle.build(file_path, halt_flag=halt_flag)
        if raise_ex:
            raise StorageError(f'No handle supported for [{file_path}]', 9000)
        return None

    @staticmethod
    def build_metadata(program_name: str = "UNKNOWN",
                       dataset_name: str = "UNKNOWN",
                       cost_unit: str = "MEDS",
                       gzip: bool = False,
                       access_level: AccessLevel = None,
                       security_label: SecurityLevel = SecurityLevel.Unclassified,
                       release_date: t.Optional[datetime.datetime] = None,
                       automate_release: bool = False,
                       storage_tier: t.Optional[StorageTier] = StorageTier.FREQUENT):
        if access_level is None:
            if security_label == SecurityLevel.ProtectedA:
                access_level = AccessLevel.Controlled
            elif release_date:
                access_level = AccessLevel.Embargoed
            else:
                access_level = AccessLevel.General
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

    @staticmethod
    def apply_default_metadata(md: dict, **kwargs):
        if 'AccessLevel' in md and md['AccessLevel']:
            kwargs['access_level'] = AccessLevel(md['AccessLevel'])
        if 'SecurityLabel' in md and md['SecurityLabel']:
            kwargs['security_label'] = SecurityLevel(md['SecurityLabel'])
        if 'CostUnit' in md and md['CostUnit']:
            kwargs['cost_unit'] = md['CostUnit']
        if 'Program' in md and md['Program']:
            kwargs['program_name'] = md['Program']
        if 'Dataset' in md and md['Dataset']:
            kwargs['dataset_name'] = md['Dataset']
        if 'Gzip' in md:
            kwargs['gzip'] = md['Gzip'] == 'YES'
        if 'AutomatedRelease' in md:
            kwargs['automate_release'] = md['AutomatedRelease'] == 'YES'
        if 'ReleaseDate' in md and md['ReleaseDate']:
            kwargs['release_date'] = awaretime.utc_from_isoformat(md['ReleaseDate'])
        if 'StorageTier' in md and md['StorageTier']:
            kwargs['storage_tier'] = StorageTier(md['StorageTier'])
        md.update(StorageController.build_metadata(**kwargs))
