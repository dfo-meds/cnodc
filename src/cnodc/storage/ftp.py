import datetime
import ftplib
import ssl
import typing as t
import atexit
from contextlib import contextmanager

from . import BaseStorageHandle
from .base import UrlBaseHandle, StorageError
from ..util import HaltFlag


class FTPHandle(UrlBaseHandle):
    """FTP support"""

    def __init__(self, *args, use_tls: bool, username: t.Optional[str] = None, password: t.Optional[str] = None, server_tz_name: t.Optional[str] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._use_tls = use_tls
        self._username: str = username or ''
        self._password: str = password or ''
        self._server_timezone = server_tz_name
        self._new_child_args.update({
            'use_tls': use_tls,
            'username': username,
            'password': password,
            'server_tz_name': server_tz_name
        })

    @contextmanager
    def _connection(self):
        url = self.parse_url()
        path = [x for x in url.path.split('/') if x]
        ftp = None
        try:
            ftp = ftplib.FTP() if not self._use_tls else ftplib.FTP_TLS()
            ftp.connect(url.hostname, url.port)
            if self._use_tls:
                ftp.prot_p()
            ftp.login(self._username, self._password)
            if self.is_dir():
                ftp.cwd("/" + '/'.join(path))
            else:
                ftp.cwd("/" + '/'.join(path[:-1]))
            yield ftp
        finally:
            if ftp is not None:
                ftp.close()

    def _read_chunks(self, buffer_size: int = None) -> t.Iterable[bytes]:
        with self._connection() as ftp:
            ftp.voidcmd('TYPE I')
            with ftp.transfercmd(f"RETR {self.name()}") as conn:
                while data := conn.recv(buffer_size):
                    yield data
                    if self._halt_flag:
                        self._halt_flag.breakpoint()
                if isinstance(conn, ssl.SSLSocket):
                    conn.unwrap()

    def _write_chunks(self, chunks: t.Iterable[bytes]):
        with self._connection() as ftp:
            ftp.voidcmd('TYPE I')
            with ftp.transfercmd(f"STOR {self.name()}") as conn:
                for chunk in chunks:
                    conn.sendall(chunk)
                if isinstance(conn, ssl.SSLSocket):
                    conn.unwrap()

    def walk(self, recursive: bool = True) -> t.Iterable[BaseStorageHandle]:
        for name, is_dir in self._walk(recursive):
            yield self.child(name, is_dir)

    def _walk(self, recursive: bool = True, include_dirs: bool = False):
        if not self.is_dir():
            return []
        parts = self.parse_url()
        work = ["/" + "/".join(x for x in parts.path.split('/') if x)]
        with self._connection() as ftp:
            while work:
                dir_name = work.pop()
                for name, file_info in ftp.mlsd(dir_name, ['type']):
                    if recursive and file_info['type'] == 'dir':
                        work.append(dir_name + "/" + name)
                        if include_dirs:
                            yield name, True
                    else:
                        yield name, False

    def _exists(self) -> bool:
        with self._connection() as ftp:
            for name, _ in self._walk(False, True):
                if name == self.name():
                    return True
        return False

    def _is_dir(self) -> bool:
        part1, _ = self._split_url()
        return part1.endswith('/')

    def remove(self):
        if self.is_dir():
            raise StorageError("Cannot remove an FTP directory", 5000)
        with self._connection() as ftp:
            ftp.delete(self.name())

    def _modified_datetime(self) -> t.Optional[datetime.datetime]:
        if self.is_dir():
            return None
        parts = self.parse_url()
        with self._connection() as ftp:
            line = ftp.sendcmd(f"MLst SP {parts.path} \r\n")
            facts_found, _, name = line.rstrip("\r\n").partition(' ')
            entry = {}
            for fact in facts_found[:-1].split(";"):
                key, _, value = fact.partition("=")
                entry[key.lower()] = value
            if 'modify' in entry:
                format_ = '%Y%m%d%H%M%S'
                if '.' in entry['modify']:
                    format_ = '%Y%m%d%H%M%S.%f'
                if self._server_timezone:
                    format_ += "%Z"
                    entry['modify'] += self._server_timezone
                return datetime.datetime.strptime(entry['modify'], format_)
        return None

    def _size(self) -> t.Optional[int]:
        if self.is_dir():
            return None
        with self._connection() as ftp:
            return ftp.size(self.name())

    @staticmethod
    def supports(file_path: str) -> bool:
        return file_path.startswith("ftp://") or file_path.startswith("ftps://")

    @classmethod
    def build(cls, file_path: str, halt_flag: HaltFlag = None) -> BaseStorageHandle:
        # TODO: retrieve information from configuration if available for username/password/server timezone
        if file_path.startswith("ftp://"):
            return cls(file_path, use_tls=False)
        else:
            return cls(file_path, use_tls=True)

