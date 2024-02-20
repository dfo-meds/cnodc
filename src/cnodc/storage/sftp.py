from .base import UrlBaseHandle


class SFTPHandle(UrlBaseHandle):
    """SFTP support"""

    # TODO: implement me

    @staticmethod
    def supports(file_path: str) -> bool:
        return file_path.startswith("sftp://")
