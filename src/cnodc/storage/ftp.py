import datetime
import ftplib
import functools
import logging
import ssl
import typing as t
import urllib.parse
import zoneinfo
from contextlib import contextmanager
from zoneinfo import ZoneInfoNotFoundError

from autoinject import injector
import zirconium as zr

from cnodc.storage.base import BaseStorageHandle, UrlBaseHandle, StorageError
from cnodc.util.halts import HaltFlag


def ftplib_error_wrap(cb):

    @functools.wraps(cb)
    def _inner(*args, **kwargs):
        try:
            return cb(*args, **kwargs)
        except ftplib.error_perm as ex:
            raise StorageError(str(ex), 2000, is_recoverable=True)
        except ftplib.error_temp as ex:
            raise StorageError(str(ex), 2001, is_recoverable=True)
        except ftplib.error_proto as ex:
            raise StorageError(str(ex), 2002, is_recoverable=False)
        except ftplib.error_reply as ex:
            raise StorageError(str(ex), 2003, is_recoverable=False)

    return _inner


@injector.injectable
class FTPConnectionPool:

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._server_notes = self.config.as_dict(('storage', 'servers', 'ftp'), default={})

    def build_connection(self, url: str):
        parts = urllib.parse.urlsplit(url)
        if parts.netloc not in self._server_notes:
            self._server_notes[parts.netloc] = {}
        if parts.scheme in ('ftps', 'ftpse') and 'tls' not in self._server_notes[parts.netloc]:
            self._server_notes[parts.netloc]['tls'] = 'explicit'
        if parts.port and 'port' not in self._server_notes[parts.netloc]:
            self._server_notes[parts.netloc]['port'] = parts.port
        return _FTPWrapper(parts.netloc, self._server_notes[parts.netloc])


class _FTPWrapper:

    def __init__(self, hostname, config: dict):
        self._host = hostname
        if 'username' not in config:
            config['username'] = ''
        if 'password' not in config:
            config['password'] = ''
        if 'tls' not in config:
            config['tls'] = 'none'
        if 'port' not in config:
            config['port'] = 21
        self._config = config
        self._server: t.Optional[t.Union[ftplib.FTP, ftplib.FTP_TLS]] = None
        self._depth = 0

    def connect(self):
        self._server.connect(self._host, self._config['port'])
        self._server.login(self._config['username'], self._config['password'])
        if hasattr(self._server, 'prot_p'):
            self._server.prot_p()
        self.test_features()

    def test_features(self):
        if 'rfc3659_support' not in self._config:
            try:
                self._server.voidcmd('MLST /')
                self._config['rfc3659_support'] = True
            except ftplib.error_perm as ex:
                if ex.args[0][0:2] == '50':
                    self._config['rfc3659_support'] = False
                else:
                    raise ex from ex

    def cwd(self, path: str):
        self._server.cwd(path)

    def binary_mode(self):
        self._server.voidcmd('TYPE I')

    def delete(self, file_path):
        self._server.delete(file_path)

    def transfer_command(self, cmd):
        return self._server.transfercmd(cmd)

    def supports_rfc3659_features(self):
        return 'rfc3659_support' in self._config and self._config['rfc3659_support']

    def extend_info(self, info):
        if 'server_timezone' in self._config and self._config['server_timezone']:
            info['server_timezone'] = self._config['server_timezone']
        return info

    def list_dir(self, dir_path='', facts: t.Optional[list[str]] = None):
        if self.supports_rfc3659_features():
            for name, facts in self._server.mlsd(dir_path, facts):
                yield name, self.extend_info(facts)
        else:
            if dir_path and not dir_path.endswith('/'):
                dir_path += '/'
            pwd = self._server.pwd()
            for file in self._server.nlst(dir_path):
                self.binary_mode()
                test_path = dir_path + file
                try:
                    self._server.cwd(test_path)
                    yield file, self.extend_info({'type': 'dir'})
                except ftplib.error_perm as ex:
                    if ex.args[0][0:3] == '550':
                        yield file, self.extend_info({'type': 'file'})
                    else:
                        raise ex from ex
            self._server.cwd(pwd)

    def stat(self, file_path) -> t.Optional[dict[str, t.Any]]:
        if file_path in ('', '/'):
            return self.extend_info({'type': 'dir'})
        if self.supports_rfc3659_features():
            try:
                result = self._server.voidcmd(f'MLST {file_path}')
                lines = result.strip().split("\n")
                pieces = lines[1].strip().split(' ')
                details = {}
                for x in pieces[0].split(';'):
                    if x.strip():
                        key, value = x.strip().split('=', maxsplit=1)
                        details[key] = value
                return self.extend_info(details)
            except ftplib.error_perm as ex:
                if ex.args[0][0:2] == '55':
                    return None
                raise ex from ex
        else:
            parts = file_path.split('/')
            for file, info in self.list_dir('/'.join(parts[:-1])):
                if file == parts[-1]:
                    return info
            return None

    def __enter__(self):
        if self._server is None:
            if self._config['tls'] == 'explicit':
                self._server = ftplib.FTP_TLS()
            elif self._config['tls'] == 'none':
                self._server = ftplib.FTP()
            else:
                raise StorageError("Invalid tls setting for FTP", 2005, False)
        self._depth += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._depth -= 1
        if self._depth <= 0:
            self._depth = 0
            self._server.quit()
            self._server = None


class FTPHandle(UrlBaseHandle):
    """FTP support"""

    conn_pool: FTPConnectionPool = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def current_dir(self):
        return self._with_cache('current_dir', self._current_dir)

    def _current_dir(self):
        if self.is_dir():
            return self.parse_url().path or '/'
        else:
            path_pieces = [x for x in self.parse_url().path.split('/') if x]
            return '/' + '/'.join(path_pieces[:-1])

    @contextmanager
    def _connection(self):
        with self.conn_pool.build_connection(self._url) as ftp:
            ftp.connect()
            ftp.cwd(self.current_dir())
            yield ftp

    @ftplib_error_wrap
    def _read_chunks(self, buffer_size: int = None) -> t.Iterable[bytes]:
        buffer_size = buffer_size or 1024 * 1024
        with self._connection() as ftp:
            ftp.binary_mode()
            with ftp.transfer_command(f"RETR {self.name()}") as conn:
                while data := conn.recv(buffer_size):
                    yield data
                    if self._halt_flag:
                        self._halt_flag.breakpoint()
                if isinstance(conn, ssl.SSLSocket):
                    conn.unwrap()

    @ftplib_error_wrap
    def _write_chunks(self, chunks: t.Iterable[bytes]):
        with self._connection() as ftp:
            ftp.binary_mode()
            with ftp.transfer_command(f"STOR {self.name()}") as conn:
                for chunk in chunks:
                    conn.sendall(chunk)
                if isinstance(conn, ssl.SSLSocket):
                    conn.unwrap()

    @ftplib_error_wrap
    def walk(self, recursive: bool = True) -> t.Iterable[BaseStorageHandle]:
        if not self.is_dir():
            return []
        with self._connection() as ftp:
            for name, is_dir in self._walk(self.parse_url().path, recursive, _conn=ftp):
                if not is_dir:
                    yield self.child(name, is_dir)

    def _walk(self, directory: str, recursive: bool = True, include_dirs: bool = False, _conn=None):
        work = [directory]
        while work:
            dir_name = work.pop()
            if not dir_name.endswith('/'):
                dir_name += '/'
            for name, file_info in _conn.list_dir(dir_name, ['type']):
                rel_path = dir_name + name
                if file_info['type'] == 'dir':
                    if recursive:
                        work.append(rel_path)
                    if include_dirs:
                        yield rel_path, True
                else:
                    yield rel_path, False

    def _exists(self) -> bool:
        my_name = self.name()
        if my_name == '':
            return True
        return self.stat() is not None

    def stat(self):
        return self._with_cache('stat', self._stat)

    @ftplib_error_wrap
    def _stat(self):
        with self._connection() as ftp:
            return ftp.stat(self.name())

    def _is_dir(self) -> bool:
        if self.name() == '':
            return True
        part1, _ = self._split_url()
        return part1.endswith('/')

    @ftplib_error_wrap
    def remove(self):
        if self.is_dir():
            raise StorageError("Cannot remove an FTP directory", 2004)
        with self._connection() as ftp:
            try:
                ftp.delete(self.name())
            finally:
                self.clear_cache()

    def _modified_datetime(self) -> t.Optional[datetime.datetime]:
        stat = self.stat()
        if stat and 'modify' in stat:
            format_ = '%Y%m%d%H%M%S'
            if '.' in stat['modify']:
                format_ = '%Y%m%d%H%M%S.%f'
            dt = datetime.datetime.strptime(stat['modify'], format_)
            if 'server_timezone' in stat and stat['server_timezone']:
                try:
                    dt = dt.replace(tzinfo=zoneinfo.ZoneInfo(stat['server_timezone']))
                except ZoneInfoNotFoundError as ex:
                    logging.getLogger("cnodc.storage.ftp").error(f"Cannot parse server timezone [{stat['server_timezone']}]")
            return dt
        return None

    def _size(self) -> t.Optional[int]:
        stat = self.stat()
        if stat and 'size' in stat:
            return int(stat['size'])
        return None

    @staticmethod
    def supports(file_path: str) -> bool:
        return any(file_path.startswith(x) for x in ('ftp://', 'ftps://', 'ftpse://'))

    @classmethod
    def build(cls, file_path: str, halt_flag: HaltFlag = None) -> BaseStorageHandle:
        return cls(file_path, halt_flag=halt_flag)

