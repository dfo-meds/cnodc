import ftplib  # nosec B402 # no choice but to support FTP for now
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
import medsutil.storage.interface as interface
from medsutil.awaretime import AwareDateTime

from medsutil.storage.base import UrlBaseHandle
from medsutil.storage.interface import StorageError, FeatureFlag


def ftplib_error_wrap(cb):

    @functools.wraps(cb)
    def _inner(*args, **kwargs):
        try:
            return cb(*args, **kwargs)
        # file doesnt exist, etc
        except ftplib.error_perm as ex:
            raise StorageError(str(ex), 2000, is_transient=True) from ex
        except ftplib.error_temp as ex:
            raise StorageError(str(ex), 2001, is_transient=True) from ex
        except ftplib.error_proto as ex:
            raise StorageError(str(ex), 2002, is_transient=False) from ex
        except ftplib.error_reply as ex:
            raise StorageError(str(ex), 2003, is_transient=False) from ex
        # actual error connecting to server
        except ConnectionError as ex:
            raise StorageError(str(ex), 2004, is_transient=True) from ex

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
            self._server_notes[parts.netloc]['tls'] = 'explicit'    # pragma: no coverage (I have no TLS server to test this on)
        if parts.port and 'port' not in self._server_notes[parts.netloc]:
            self._server_notes[parts.netloc]['port'] = parts.port   # pragma: no coverage
        return _FTPWrapper(parts.netloc, self._server_notes[parts.netloc])


class _FTPWrapper:

    def __init__(self, hostname, config: dict):
        self._host = hostname
        if 'username' not in config:
            config['username'] = ''
        if 'password' not in config:
            config['password'] = ''  # noqa: B105 # default blank password for anon login
        if 'tls' not in config:
            config['tls'] = 'none'
        if 'port' not in config:
            config['port'] = 21
        self._config = config
        self._server: t.Optional[t.Union[ftplib.FTP, ftplib.FTP_TLS]] = None
        self._depth = 0

    @property
    def server(self) -> ftplib.FTP | ftplib.FTP_TLS:
        if self._server is None:
            raise StorageError('Server is not open for connections', 1006)
        return self._server

    def connect(self):
        self.server.connect(self._host, self._config['port'])
        self.server.login(self._config['username'], self._config['password'])
        if hasattr(self.server, 'prot_p'):
            self.server.prot_p()  # pragma: no coverage (I have no TLS server to test this on)
        self.test_features()

    def test_features(self):
        if 'rfc3659_support' not in self._config:
            try:
                self.server.voidcmd('MLST /')
                self._config['rfc3659_support'] = True
            except ftplib.error_perm as ex:  # pragma: no coverage (I have no non-RFC3659 compliant server to test with)
                if ex.args[0][0:2] == '50':
                    self._config['rfc3659_support'] = False
                else:
                    raise ex from ex

    def cwd(self, path: str):
        self.server.cwd(path)

    def binary_mode(self):
        self.server.voidcmd('TYPE I')

    def delete(self, file_path):
        self.server.delete(file_path)

    def transfer_command(self, cmd):
        return self.server.transfercmd(cmd)

    def supports_rfc3659_features(self):
        return 'rfc3659_support' in self._config and self._config['rfc3659_support']

    def extend_info(self, info):
        if 'server_timezone' in self._config and self._config['server_timezone']:
            info['server_timezone'] = self._config['server_timezone']
        return info

    def mkdir(self, dir_path):
        self.server.mkd(dir_path)

    def rmdir(self, dir_path):
        self.server.rmd(dir_path)

    def list_dir(self, dir_path='', facts: t.Optional[list[str]] = None):
        if self.supports_rfc3659_features():
            for name, facts in self.server.mlsd(dir_path, facts or []):
                yield name, self.extend_info(facts)
        else:
            if dir_path and not dir_path.endswith('/'): # pragma: no coverage
                dir_path += '/'
            pwd = self.server.pwd()
            for file in self.server.nlst(dir_path):
                self.binary_mode()
                test_path = dir_path + file
                try:
                    self.server.cwd(test_path)
                    yield file, self.extend_info({'type': 'dir'})
                except ftplib.error_perm as ex:
                    if ex.args[0][0:3] == '550':
                        yield file, self.extend_info({'type': 'file'})
                    else:
                        raise ex from ex  # pragma: no coverage
            self.server.cwd(pwd)

    def stat(self, file_path) -> t.Optional[dict[str, t.Any]]:
        if file_path in ('', '/'):
            return self.extend_info({'type': 'dir'})
        if self.supports_rfc3659_features():
            try:
                result = self.server.voidcmd(f'MLST {file_path}')
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
                raise ex from ex  # pragma: no coverage (difficult to test)
        else:
            parts = file_path.split('/')
            for file, info in self.list_dir('/'.join(parts[:-1])):
                if file == parts[-1]:
                    return info
            return None

    def __enter__(self):
        if self._server is None:
            if self._config['tls'] == 'explicit':
                self._server = ftplib.FTP_TLS()  # pragma: no coverage; no TLS server to test with
            elif self._config['tls'] == 'none':
                self._server = ftplib.FTP()  # nosec B321 # no choice but to support FTP for now
            else:   # pragma: no coverage # no TLS server to test with
                raise StorageError("Invalid tls setting for FTP", 2005, is_transient=False)
        self._depth += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._depth -= 1
        if self._depth <= 0:
            self._depth = 0
            if self._server is not None:
                self._server.quit()
                self._server = None


class FTPHandle(UrlBaseHandle):
    """FTP support"""

    conn_pool: FTPConnectionPool = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args, supports=FeatureFlag.DEFAULT, **kwargs)

    def current_dir(self) -> str:
        return self._with_cache('current_dir', self._current_dir)

    def current_file(self) -> str:
        return self.parse_url().path or '/'

    def _current_dir(self) -> str:
        if self._no_remote_is_dir():
            return self.current_file()
        else:
            path_pieces = [x for x in self.current_file().split('/') if x]
            return '/' + '/'.join(path_pieces[:-1])

    @contextmanager
    def _connection(self):
        with self.conn_pool.build_connection(self._path) as ftp:
            ftp.connect()
            try:
                ftp.cwd(self.current_dir())
            except ftplib.error_perm as ex:
                if str(ex)[0:2] != '55':
                    raise ex from ex
            yield ftp

    @ftplib_error_wrap
    def streaming_read(self, buffer_size: int = None) -> t.Iterable[bytes]:
        buffer_size = buffer_size or 1024 * 1024
        with self._connection() as ftp:
            ftp.binary_mode()
            with ftp.transfer_command(f"RETR {self.name}") as conn:
                while data := conn.recv(buffer_size):
                    yield data
                    if self._halt_flag:
                        self._halt_flag.breakpoint()
                if isinstance(conn, ssl.SSLSocket):  # pragma: no coverage (no TLS server to test with)
                    conn.unwrap()

    @ftplib_error_wrap
    def streaming_write(self, chunks: t.Iterable[bytes]):
        with self._connection() as ftp:
            ftp.binary_mode()
            with ftp.transfer_command(f"STOR {self.name}") as conn:
                for chunk in chunks:
                    conn.sendall(chunk)
                if isinstance(conn, ssl.SSLSocket):  # pragma: no coverage (no TLS to test with)
                    conn.unwrap()

    @ftplib_error_wrap
    def _walk(self) -> t.Iterable[tuple[str, list[str], list[str]]]:
        with self._connection() as ftp:
            yield from self._list_dir(ftp, self._path)

    def _list_dir(self, ftp, dir_name: str):
        files: list[str] = []
        dirs: list[str] = []
        if not dir_name.endswith('/'):
            dir_name = dir_name + '/'
        for name, file_info in ftp.list_dir(dir_name, ['type']):
            if file_info['type'] == 'dir':
                dirs.append(name)
            else:
                files.append(name)
        yield self._path, dirs, files
        for subd in dirs:
            yield from self._list_dir(ftp, dir_name + subd)

    @ftplib_error_wrap
    def _mkdir(self, mode: int = 0o777):
        with self._connection() as ftp:
            ftp.mkdir(self._current_dir())

    @ftplib_error_wrap
    def _stat(self) -> interface.StatResult:
        p = self.parse_url()
        with self._connection() as ftp:
            stat = ftp.stat(p.path)
        return interface.StatResult(
            st_size=stat['size'] if stat is not None and 'size' in stat else None,
            st_mtime=self._build_modified_time(stat),
            exists=stat is not None,
            is_dir=stat['type'] == 'dir' if stat is not None and 'type' in stat else None,
            is_file=stat['type'] != 'dir' if stat is not None and 'type' in stat else None,
        )

    @ftplib_error_wrap
    def _remove(self):
        with self._connection() as ftp:
            try:
                if self.is_dir():
                    ftp.delete(self.name)
                else:
                    ftp.cwd('..')
                    ftp.rmdir(self.current_file())
            finally:
                self.clear_cache()

    def _build_modified_time(self, stat: dict[str, t.Any] | None) -> AwareDateTime | None:
        if stat and 'modify' in stat:
            format_ = '%Y%m%d%H%M%S'
            if '.' in stat['modify']:  # pragma: no coverage (test server doesn't return this)
                format_ = '%Y%m%d%H%M%S.%f'
            tzinfo = None
            if 'server_timezone' in stat and stat['server_timezone']:
                try:
                    tzinfo = zoneinfo.ZoneInfo(stat['server_timezone'])
                except ZoneInfoNotFoundError as ex:  # pragma: no coverage (test server doesn't have timezone)
                    logging.getLogger("cnodc.storage.ftp").error(f"Cannot parse server timezone [{stat['server_timezone']}]")
            return AwareDateTime.strptime(stat['modify'], format_, tzinfo)
        return None

    @staticmethod
    def supports(file_path: str) -> bool:
        return any(file_path.startswith(x) for x in ('ftp://', 'ftps://', 'ftpse://'))
