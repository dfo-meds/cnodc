from .base import UrlBaseHandle, StorageTier, BaseStorageHandle


class FTPHandle(UrlBaseHandle):

    @staticmethod
    def supports(file_path: str) -> bool:
        return file_path.startswith("ftp://") or file_path.startswith("ftps://")

