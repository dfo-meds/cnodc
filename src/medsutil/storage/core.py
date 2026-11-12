import datetime
import enum
import pathlib
import typing as t
from contextlib import contextmanager

import zirconium
from autoinject import injector

from medsutil import awaretime
from medsutil.halts import HaltFlag
from medsutil.storage.azure_blob import AzureBlobHandle
from medsutil.storage.azure_files import AzureFileHandle
from medsutil.storage.ftp import FTPHandle
from medsutil.storage.interface import FilePath, StorageError, StorageTier
from medsutil.storage.local import LocalHandle


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

    config: zirconium.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self.handle_classes = [
            AzureFileHandle,
            AzureBlobHandle,
            FTPHandle,
            # SFTPHandle,
        ]
        self.default_handle = LocalHandle

    @contextmanager
    def handle(
        self, file_path: str | pathlib.Path | None, halt_flag: HaltFlag = None
    ) -> t.Generator[FilePath]:
        fp = t.cast(FilePath, self.get_filepath(file_path, halt_flag, True))
        with fp as fp:
            yield fp

    def get_filepath(
        self,
        file_path: str | pathlib.Path | None,
        halt_flag: HaltFlag | None = None,
        raise_ex: bool = False,
    ) -> FilePath | None:
        """Build an appropriate handle for the given file path."""
        if file_path is not None and file_path != "":
            if isinstance(file_path, str):
                file_path = self.normalize_file_path(file_path)
            if isinstance(file_path, pathlib.Path):
                return LocalHandle.build(str(file_path), halt_flag=halt_flag)
            for cls in self.handle_classes:
                if cls.supports(file_path):
                    return cls.build(file_path, halt_flag=halt_flag)
            if self.default_handle.supports(file_path):
                return self.default_handle.build(file_path, halt_flag=halt_flag)
        if raise_ex:
            raise StorageError(f"No handle supported for [{file_path}]", 9000)
        return None

    def normalize_file_path(self, file_path: str) -> str:
        if "://" in file_path and file_path.startswith("base:"):
            pos = file_path.find("://")
            base_name = file_path[5:pos]
            rel_path = file_path[pos + 3 :]
            actual_base = self.config.as_str(
                ("storage", "base_names", base_name), default=None
            )
            if actual_base is None:
                raise StorageError(f"No root path defined for {base_name}")
            if not actual_base.endswith("/"):
                actual_base += "/"
            return f"{actual_base}{rel_path}"
        else:
            return file_path

    @staticmethod
    def build_metadata(
        program_name: str = "UNKNOWN",
        dataset_name: str = "UNKNOWN",
        cost_unit: str = "MEDS",
        gzip: bool = False,
        access_level: AccessLevel = None,
        security_label: SecurityLevel = SecurityLevel.Unclassified,
        release_date: datetime.datetime | None = None,
        automate_release: bool = False,
        storage_tier: StorageTier | None = StorageTier.FREQUENT,
    ):
        if access_level is None:
            if security_label == SecurityLevel.ProtectedA:
                access_level = AccessLevel.Controlled
            elif release_date:
                access_level = AccessLevel.Embargoed
            else:
                access_level = AccessLevel.General
        metadata = {
            "Program": program_name,
            "Dataset": dataset_name,
            "CostUnit": cost_unit,
            "Gzip": "YES" if gzip else "NO",
            "AccessLevel": access_level.value,
            "SecurityLabel": security_label.value,
            "AutomatedRelease": "YES" if automate_release else "NO",
            "ReleaseDate": release_date.isoformat() if release_date else "",
            "StorageTier": storage_tier.value if storage_tier else "",
        }
        return metadata

    @staticmethod
    def apply_default_metadata(md: dict, **kwargs):
        if md.get("AccessLevel"):
            kwargs["access_level"] = AccessLevel(md["AccessLevel"])
        if md.get("SecurityLabel"):
            kwargs["security_label"] = SecurityLevel(md["SecurityLabel"])
        if md.get("CostUnit"):
            kwargs["cost_unit"] = md["CostUnit"]
        if md.get("Program"):
            kwargs["program_name"] = md["Program"]
        if md.get("Dataset"):
            kwargs["dataset_name"] = md["Dataset"]
        if "Gzip" in md:
            kwargs["gzip"] = md["Gzip"] == "YES"
        if "AutomatedRelease" in md:
            kwargs["automate_release"] = md["AutomatedRelease"] == "YES"
        if md.get("ReleaseDate"):
            kwargs["release_date"] = awaretime.AwareDateTime.utcfromisoformat(
                md["ReleaseDate"]
            )
        if md.get("StorageTier"):
            kwargs["storage_tier"] = StorageTier(md["StorageTier"])
        md.update(StorageController.build_metadata(**kwargs))
