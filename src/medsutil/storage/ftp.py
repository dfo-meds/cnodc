import ftplib  # nosec B402 # no choice but to support FTP for now
import functools
import logging
import ssl
import threading
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


def ftplib_error_wrap_generator(cb):

    @functools.wraps(cb)
    def _inner(*args, **kwargs):
        try:
            yield from cb(*args, **kwargs)
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
        self._server_notes: dict = self.config.as_dict(('storage', 'ftp'), default={})
        self._connections: dict[tuple[str, str, int | None], _FTPWrapper] = {}
        self._cache_connections: bool = bool(self._server_notes['_cache']) if 'cache' in self._server_notes else True

    def build_connection(self, url: str) -> _FTPWrapper:
        parts = urllib.parse.urlsplit(url)
        if not self._cache_connections:
            return self._build_connection(parts)
        key = (parts.netloc, parts.scheme, parts.port)
        if key not in self._connections:
            self._connections[key] = self._build_connection(parts)
        return self._connections[key]

    def _build_connection(self, parts):
        config = {}
        if parts.netloc in self._server_notes:
            config.update(self._server_notes[parts.netloc])
        if parts.scheme in ('ftps', 'ftpse') and 'tls' not in config:
            config['tls'] = 'explicit'    # pragma: no coverage (I have no TLS server to test this on)
        if parts.port and 'port' not in config:
            config['port'] = parts.port   # pragma: no coverage
        return _FTPWrapper(parts.netloc, config)


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
        if 'timeout' not in config:
            config['timeout'] = 10
        self._config = config
        self._server: t.Optional[t.Union[ftplib.FTP, ftplib.FTP_TLS]] = None
        self._depth = 0
        self._connected = False
        self._cwd = None
        self._lock = threading.Lock()

    @property
    def server(self) -> ftplib.FTP | ftplib.FTP_TLS:
        if self._server is None:
            raise StorageError('Server is not open for connections', 1006)
        return self._server

    def streaming_read(self, name: str, buffer_size=1024*1024) -> t.Iterable[bytes]:
        self.binary_mode()
        with self.transfer_command(f"RETR {name}") as conn:
            try:
                while data := conn.recv(buffer_size):
                    yield data
            finally:
                if isinstance(conn, ssl.SSLSocket): # pragma: no coverage (no TLS server to test with)
                    conn.unwrap()
        self.server.voidresp()

    def streaming_write(self, name, chunks: t.Iterable[bytes]):
        self.binary_mode()
        with self.transfer_command(f"STOR {name}") as conn:
            try:
                for chunk in chunks:
                    conn.sendall(chunk)
            finally:
                if isinstance(conn, ssl.SSLSocket):  # pragma: no coverage (no TLS to test with)
                    conn.unwrap()
        self.server.voidresp()

    def connect(self):
        if not self._connected:
            self.server.connect(self._host, self._config['port'], timeout=self._config['timeout'])
            self.server.login(self._config['username'], self._config['password'])
            if hasattr(self.server, 'prot_p'):
                self.server.prot_p()  # pragma: no coverage (I have no TLS server to test this on)
            self._test_features()
            self._connected = True

    def _test_features(self):
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
        if self._cwd is None or self._cwd != path or not path[0] == "/":
            self.server.cwd(path)
            # Don't guess at relative paths
            self._cwd = path if path[0] == "/" else None

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
                if name not in (".", ".."):
                    yield name, self.extend_info(facts)
        else:
            if dir_path and not dir_path.endswith('/'): # pragma: no coverage
                dir_path += '/'
            pwd = self.server.pwd()
            for file in self.server.nlst(dir_path):
                if file in (".", ".."):
                    continue
                self.binary_mode()
                test_path = dir_path + file
                try:
                    self.cwd(test_path)
                    yield file, self.extend_info({'type': 'dir'})
                except ftplib.error_perm as ex:
                    if ex.args[0][0:3] == '550':
                        yield file, self.extend_info({'type': 'file'})
                    else:
                        raise # pragma: no coverage
            self.cwd(pwd)

    def stat(self, file_path) -> t.Optional[dict[str, t.Any]]:
        if file_path in ('', '/'):
            return self.extend_info({'type': 'dir'})
        if self.supports_rfc3659_features():
            try:
                self.server.sendcmd("OPTS MLST type;size;modify")
                result = self.server.voidcmd(f'MLST {file_path}')
                lines = result.strip().split("\n")
                if len(lines) < 2:
                    return None
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
                raise ex  # pragma: no coverage (difficult to test)
        else:
            parts = file_path.split('/')
            for file, info in self.list_dir('/'.join(parts[:-1]), ["type", "size", "modify"]):
                if file == parts[-1]:
                    return info
            return None

    def __enter__(self):
        self.enter()
        return self

    def enter(self):
        if self._server is None:
            if self._config['tls'] == 'explicit':
                self._server = ftplib.FTP_TLS(context=ssl.create_default_context())  # pragma: no coverage; no TLS server to test with
            elif self._config['tls'] == 'none':
                self._server = ftplib.FTP()  # nosec B321 # no choice but to support FTP for now
            else:   # pragma: no coverage # no TLS server to test with
                raise StorageError("Invalid tls setting for FTP", 2005, is_transient=False)
            self.connect()
        self._depth += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.exit()

    def exit(self, force: bool = False):
        self._depth -= 1
        if force or self._depth <= 0:
            self._depth = 0
            self._connected = False
            if self._server is not None and self._server.sock is not None:
                self._server.quit()
            self._server = None


class FTPHandle(UrlBaseHandle):
    """FTP support"""

    conn_pool: FTPConnectionPool = None

    @injector.construct
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         supports=FeatureFlag.DEFAULT,
                         log_name='ftp',
                         **kwargs)
        self._open_conn: _FTPWrapper | None = None

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

    def _cwd(self, conn: _FTPWrapper):
        try:
            conn.cwd(self.current_dir())
        except ftplib.error_perm as ex:
            if str(ex)[0:2] != '55':
                raise

    @contextmanager
    def _connection(self) -> t.Generator[_FTPWrapper, None, None]:
        with self.persistent_connection(
            "ftp",
            self.conn_pool.build_connection,
            self._path
        ) as x:
            self._cwd(x)
            yield x

    @ftplib_error_wrap_generator
    def _streaming_read(self, buffer_size: int = None) -> t.Iterable[bytes]:
        with self._connection() as ftp:
            yield from self._halt_flag.iterate(
                ftp.streaming_read(self.name, buffer_size or 1024 * 1024)
            )

    @ftplib_error_wrap
    def _streaming_write(self, chunks: t.Iterable[bytes], **kwargs):
        with self._connection() as ftp:
            ftp.streaming_write(self.name, self._halt_flag.iterate(chunks))
        self.clear_cache('stat')

    @ftplib_error_wrap_generator
    def _walk(self) -> t.Iterable[tuple[str, list[str], list[str]]]:
        if self.is_dir():
            with self._connection() as ftp:
                yield from self._list_dir(ftp, self._current_dir())

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
        yield dir_name, dirs, files
        for subd in dirs:
            yield from self._list_dir(ftp, dir_name + subd)

    @ftplib_error_wrap
    def _mkdir(self, mode: int = 0o777):
        with self._connection() as ftp:
            if ftp.stat(self._current_dir()) is None:
                ftp.mkdir(self._current_dir())
                self.clear_cache('stat')

    @ftplib_error_wrap
    def _stat(self) -> interface.StatResult:
        with self._connection() as ftp:
            stat = ftp.stat(self.current_file())
        return interface.StatResult(
            st_size=int(stat['size']) if stat is not None and 'size' in stat else None,
            st_mtime=self._build_modified_time(stat),
            exists=stat is not None,
            is_dir=stat['type'] == 'dir' if stat is not None and 'type' in stat else None,
            is_file=stat['type'] == 'file' if stat is not None and 'type' in stat else None,
        )

    @ftplib_error_wrap
    def _remove(self):
        with self._connection() as ftp:
            s = ftp.stat(self.current_file())
            if s is None:
                return
            if 'type' in s and s['type'] == 'dir':
                ftp.cwd('..')
                ftp.rmdir(self.current_file())
            else:
                ftp.delete(self.name)
            self._update_stat(is_dir=None, is_file=None, exists=False, st_size=None, st_mtime=None)

    def _build_modified_time(self, stat: dict[str, t.Any] | None) -> AwareDateTime | None:
        if stat and 'modify' in stat:
            format_ = '%Y%m%d%H%M%S'
            if '.' in stat['modify']:  # pragma: no coverage (test server doesn't return this)
                format_ = '%Y%m%d%H%M%S.%f'
            tzinfo = 'Etc/UTC'
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
