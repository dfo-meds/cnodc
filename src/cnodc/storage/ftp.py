from .base import UrlBaseHandle


class FTPHandle(UrlBaseHandle):
    """FTP support"""

    # TODO: implement me

    @staticmethod
    def supports(file_path: str) -> bool:
        return file_path.startswith("ftp://") or file_path.startswith("ftps://")

