import json
import os
import pathlib
import shutil
import typing

from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from azure.storage.blob import StandardBlobTier
import datetime

from medsutil.awaretime import AwareDateTime
from medsutil.storage.azure import wrap_azure_errors

AZURE_CONTAINERS = pathlib.Path(__file__).absolute().resolve().parent.parent / 'test_data/azure_containers'
AZURE_FILE_SHARES = pathlib.Path(__file__).absolute().resolve().parent.parent / 'test_data/azure_file_shares'

class AzureMockClientPool:

    containers: dict[str, _AzureContainer] = {}
    shares: dict[str, _AzureFileShare] = {}

    def get_share(self, conn_string: str, share_name: str):
        if conn_string == "ValueError":
            raise ValueError("invalid connection string")
        if share_name == "ValueError":
            raise ValueError("invalid share name")
        if share_name not in self.shares:
            self.shares[share_name] = _AzureFileShare(
                f"https://test.file.core.windows.net/{share_name}",
                AZURE_FILE_SHARES / share_name,
                share_name
            )
        return self.shares[share_name]

    def get_share_directory(self, conn_string: str, share_name: str, directory_path: str):
        return self.get_share(conn_string, share_name).get_directory_client(directory_path)

    def get_share_file(self, conn_string: str, share_name: str, file_path: str):
        return self.get_share(conn_string, share_name).get_file_client(file_path)

    def get_container(self, conn_string: str, container_name: str):
        if conn_string == "ValueError":
            raise ValueError("invalid connection string")
        if container_name == "ValueError":
            raise ValueError("invalid container name")
        if container_name not in self.containers:
            self.containers[container_name] = _AzureContainer(
                f'https://test.blob.core.windows/{container_name}',
                AZURE_CONTAINERS / container_name
            )
        return self.containers[container_name]

    def get_blob(self, conn_string: str, container_name: str, blob_name: str):
        return self.get_container(conn_string, container_name).get_blob_client(blob_name)


class _Downloader:

    def __init__(self, p: pathlib.Path):
        self._file = p

    def chunks(self) -> typing.Iterable[bytes]:
        with open(self._file, 'rb') as h:
            while b := h.read(1024):
                yield b


class _ShareObjectProperties:

    def __init__(self, name, last_modified, metadata):
        self.name = name
        self.last_modified = datetime.datetime.fromtimestamp(last_modified).astimezone()
        self.metadata = metadata
        self.is_directory = True


class _FileProperties(_ShareObjectProperties):

    def __init__(self, name, last_modified, metadata, size):
        super().__init__(name, last_modified, metadata)
        self.size = size
        self.is_directory = False


class _DirProperties(_ShareObjectProperties): pass


def get_metadata(real_path: pathlib.Path):
    md = {}
    md_file = real_path.parent / f"{real_path.name}.metadata"
    if md_file.exists():
        with open(md_file, 'r') as h:
            md = json.loads(h.read())
    return md


def save_metadata(real_path: pathlib.Path, md: dict):
    md_file = real_path.parent / f"{real_path.name}.metadata"
    with open(md_file, 'w') as h:
        h.write(json.dumps(md))


class _AzureFileClient:

    def __init__(self, account_url: str, share_name: str, file_path: str, real_path: pathlib.Path):
        self.file_path = file_path.split('/')
        self.name = self.file_path[-1]
        self.share_name = share_name
        self.url = f"{account_url}/{share_name}/{file_path}"
        self.directory_name = '/'.join(self.file_path[:-1])
        self._real_path = real_path

    def exists(self):
        return self._real_path.exists() and self._real_path.is_file()

    def get_file_properties(self) -> _FileProperties:
        if not (self._real_path.exists() and self._real_path.is_file()):
            raise ResourceNotFoundError
        st = self._real_path.stat()
        return _FileProperties(self._real_path.name, st.st_mtime, get_metadata(self._real_path), st.st_size)

    def delete_file(self):
        if not (self._real_path.exists() and self._real_path.is_file()):
            raise ResourceNotFoundError
        self._real_path.unlink()

    def create_file(self, file_size: int, metadata: dict[str, str] = None):
        if self._real_path.exists():
            raise ResourceExistsError
        self.resize_file(file_size, metadata)

    def resize_file(self, file_size: int, metadata: dict[str, str] = None):
        self._real_path.touch()
        if metadata:
            save_metadata(self._real_path, metadata)

    def upload_range(self, data: bytes | bytearray, offset: int, length: int):
        with open(self._real_path, 'ab+') as h:
            h.seek(offset)
            h.write(data)
        return {
            'last_modified': AwareDateTime.fromtimestamp(self._real_path.stat().st_mtime)
        }

    def upload_file(self, data: typing.Iterable[bytes] | bytes):
        with open(self._real_path, 'wb') as h:
            if isinstance(data, (bytes, bytearray)):
                h.write(data)
            else:
                for b in data:
                    h.write(b)
        return {
            'last_modified': AwareDateTime.fromtimestamp(self._real_path.stat().st_mtime)
        }

    def download_file(self) -> _Downloader:
        if not self._real_path.exists():
            raise ResourceNotFoundError()
        return _Downloader(self._real_path)

    def set_file_metadata(self, md: dict):
        save_metadata(self._real_path, md)


class _AzureDirectoryClient:

    def __init__(self, account_url: str, share_name: str, file_path: str, real_path: pathlib.Path):
        self.share_name = share_name
        self.directory_path = file_path
        self.url = f"{account_url}/{share_name}/{file_path}"
        self._real_path = real_path

    def exists(self):
        return self._real_path.exists() and self._real_path.is_dir()

    def get_directory_properties(self) -> _DirProperties:
        if not (self._real_path.exists() and self._real_path.is_dir()):
            raise ResourceNotFoundError()
        return _DirProperties(self._real_path.name, self._real_path.stat().st_mtime, get_metadata(self._real_path))

    def list_directories_and_files(self) -> typing.Iterable[_ShareObjectProperties]:
        for x in os.scandir(self._real_path):
            stat = x.stat()
            if x.is_file():
                yield _FileProperties(x.name, stat.st_mtime, get_metadata(pathlib.Path(x.path)), stat.st_size)
            else:
                yield _DirProperties(x.name, stat.st_mtime, get_metadata(pathlib.Path(x.path)))

    def delete_directory(self):
        if not (self._real_path.exists() and self._real_path.is_dir()):
            raise ResourceNotFoundError()
        shutil.rmtree(self._real_path)

    def create_directory(self):
        self._real_path.mkdir()

    def set_directory_metadata(self, md: dict):
        save_metadata(self._real_path, md)


class _AzureFileShare:

    def __init__(self, base_url: str, base_path: pathlib.Path, share_name: str):
        self._base_url = base_url.lstrip('/\\')
        self._base_path = base_path
        self._share_name = share_name

    def get_file_client(self, file_path: str):
        file_path = file_path.rstrip('/\\')
        return _AzureFileClient(
            file_path=file_path,
            real_path=self._base_path / file_path,
            share_name=self._share_name,
            account_url='https://test.file.core.windows.net'
        )

    def get_directory_client(self, directory_path: str):
        directory_path = directory_path.rstrip('/\\')
        return _AzureDirectoryClient(
            file_path=directory_path,
            real_path=self._base_path / directory_path,
            share_name=self._share_name,
            account_url='https://test.file.core.windows.net'
        )




class _BlobProperties:

    def __init__(self, name, size, lmt, metadata, blob_tier):
        self.name = name
        self.size = size
        self.last_modified = lmt
        self.metadata = metadata
        self.blob_tier = blob_tier

class _AzureBlob:

    def __init__(self, container, name:str, local_path: pathlib.Path):
        self.url = container._base_url + name
        self.container = container
        self.name = name
        self.blob_name = name
        self.local_path = local_path

    def get_blob_properties(self):
        if not self.local_path.exists():
            raise ResourceNotFoundError
        md = self._load_metadata()
        t = StandardBlobTier.HOT
        if '__tier' in md:
            if md['__tier'] == 'a':
                t = StandardBlobTier.ARCHIVE
            elif md['__tier'] == 'c':
                t = StandardBlobTier.COOL
        stats = self.local_path.stat()
        return _BlobProperties(
            self.blob_name,
            stats.st_size,
            datetime.datetime.fromtimestamp(stats.st_mtime),
            {k: md[k] for k in md if k != '__tier'},
            t
        )

    def exists(self):
        return self.local_path.exists() and self.local_path.is_file()

    def download_blob(self):
        return _Downloader(self.local_path)

    def _load_metadata(self):
        metadata_path = pathlib.Path(str(self.local_path) + ".metadata")
        if not metadata_path.exists():
            return {}
        with open(metadata_path, "r") as h:
            content = h.read()
            if not content:
                return {}
            return json.loads(content)

    def set_blob_metadata(self, md):
        self.update_metadata(md)

    def update_metadata(self, new_metadata):
        md = self._load_metadata()
        md.update(new_metadata)
        metadata_path = pathlib.Path(str(self.local_path) + ".metadata")
        with open(metadata_path, "w") as h:
            h.write(json.dumps(md))

    def upload_blob(self, data, length: int = None, metadata=None, standard_blob_tier=None):
        if not self.local_path.parent.exists():
            self.local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.local_path, 'wb') as h:
            if isinstance(data, (bytes, bytearray)):
                h.write(data)
            else:
                for chunk in data:
                    h.write(chunk)
        if metadata:
            self.update_metadata(metadata)
        if standard_blob_tier:
            self.set_standard_blob_tier(standard_blob_tier)
        return {
            'last_modified': AwareDateTime.fromtimestamp(self.local_path.stat().st_mtime)
        }

    def set_standard_blob_tier(self, tier):
        if tier:
            metadata = self._load_metadata()
            if tier == StandardBlobTier.ARCHIVE:
                metadata['__tier'] = 'a'
            elif tier == StandardBlobTier.COOL:
                metadata['__tier'] = 'c'
            elif tier == StandardBlobTier.HOT:
                metadata['__tier'] = 'h'
            self.update_metadata(metadata)

    def delete_blob(self):
        if self.local_path.is_file():
            self.local_path.unlink(True)
            pathlib.Path(str(self.local_path) + ".metadata").unlink(True)
        else:
            raise ResourceNotFoundError


class _AzureContainer:

    def __init__(self, base_url: str, base_path: pathlib.Path):
        self._base_url = base_url
        self._base_path = base_path
        self._blobs = {}

    def list_blobs(self, name_starts_with: str):
        work = [self._base_path.absolute()]
        bp_len = len(str(work[0]))
        while work:
            dir_path = work.pop()
            for file in os.scandir(dir_path):
                full_path = dir_path / file.name
                if full_path.is_dir():
                    work.append(full_path)
                else:
                    rel_path = str(full_path)[bp_len:].replace("\\", "/")
                    if (not name_starts_with) or rel_path.startswith(name_starts_with):
                        yield self.get_blob_client(rel_path).get_blob_properties()


    def get_blob_client(self, blob_name) -> _AzureBlob:
        if blob_name not in self._blobs:
            fp = self._base_path / blob_name.strip('/')
            self._blobs[blob_name] = _AzureBlob(self, blob_name, fp)
        return self._blobs[blob_name]


@wrap_azure_errors
def make_and_wrap_exception(ex):
    raise ex
