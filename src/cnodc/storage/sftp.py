from .base import UrlBaseHandle, StorageTier, DirFileHandle


class SFTPHandle(UrlBaseHandle):

    @staticmethod
    def supports(file_path: str) -> bool:
        return file_path.startswith("sftp://")
