from .base import UrlBaseHandle, StorageTier, BaseStorageHandle


class SFTPHandle(UrlBaseHandle):

    @staticmethod
    def supports(file_path: str) -> bool:
        return file_path.startswith("sftp://")
