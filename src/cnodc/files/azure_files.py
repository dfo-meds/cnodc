import pathlib
import datetime
from .base import UrlBaseHandle, StorageTier, DirFileHandle

from cnodc.util import HaltFlag
import typing as t
from urllib.parse import urlparse


class AzureFileHandle(UrlBaseHandle):

    def __init__(self, url):
        super().__init__(url)
        # TODO

    @staticmethod
    def supports(file_path: str) -> bool:
        if not (file_path.startswith("http://") or file_path.startswith("https://")):
            return False
        pieces = urlparse(file_path)
        return pieces.hostname.endswith(".file.core.windows.net")
