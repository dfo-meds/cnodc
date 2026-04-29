"""Support for Azure Blob Storage.

There are three methods of providing Azure credentials to authenticate:

#1: Provide them in the Zirconium configuration file:

```
[azure.storage.STORAGE_ACCOUNT_NAME]
connection_string = "..."
```


#2: Provide your credentials in a method supported by DefaultAzureCredentials

#3: Include the SAS token in the URL (not recommended)


"""
import base64
import shutil
import sqlite3
import sys
import tempfile
from contextlib import contextmanager

from azure.core.exceptions import ResourceNotFoundError

from medsutil.awaretime import AwareDateTime
from medsutil.byteseq import ByteSequenceReader
from medsutil.storage.azure import wrap_azure_errors, AzureBaseHandle
from medsutil.storage.interface import FeatureFlag, StatResult
from medsutil.storage import StorageTier
from azure.storage.blob import BlobClient, StandardBlobTier, ContainerClient, BlobBlock, BlockState, BlobProperties
import typing as t
from urllib.parse import urlparse


def total_memory_size(__obj) -> int:
    seen = set()
    def _size_of(obj) -> int:
        if id(obj) in seen:
            return 0
        seen.add(id(obj))
        s = sys.getsizeof(obj)
        if isinstance(obj, t.Mapping):
            s += sum(map(_size_of, obj.keys()))
            s += sum(map(_size_of, obj.values()))
        elif isinstance(obj, t.Iterable) and not isinstance(obj, str):
            s += sum(map(_size_of, iter(obj)))
        return s
    return _size_of(__obj)

class BlobWalker:

    def __init__(self, relative_name: str, max_memory: int):
        self._relative_name = relative_name
        self._max_memory_size = max_memory
        self._use_file_system = False
        self._temp_dir: None | str = None
        self._sql_handle: None | sqlite3.Connection = None
        self._memory_dict: dict[str, list | dict | str | None] = {
            '_dirs': {},
            '_files': [],
            '_full_name': relative_name
        }

    def cleanup(self):
        if self._sql_handle:
            self._sql_handle.close()
            del self._sql_handle
        if self._temp_dir:
            shutil.rmtree(self._temp_dir)
            del self._temp_dir
        del self._memory_dict

    def walk_all(self):
        if self._use_file_system:
            yield from self._walk_file_system()
        else:
            yield from self._walk_memory()

    def append(self, bp: BlobProperties):
        relative_name = bp.name[len(self._relative_name):].strip('/')
        dir_list = relative_name.split('/')
        file_name = dir_list.pop(-1)
        if self._use_file_system:
            self._append_file_system(file_name, dir_list)
        else:
            self._append_memory(file_name, dir_list)
            if total_memory_size(self._memory_dict) >= self._max_memory_size:
                self._rollover()

    def _rollover(self):
        self._use_file_system = True
        self._temp_dir: str = tempfile.mkdtemp()
        self._sql_handle = sqlite3.connect(self._temp_dir.rstrip('/\\') + '/walker.db')
        self._sql_handle.execute("""
            CREATE TABLE blob_paths (parent_name TEXT NOT NULL, blob_name TEXT NOT NULL)
        """)
        for dir_name, _, files in self._walk_memory():
            for file_name in files:
                self._insert_row(dir_name, file_name)
        self._memory_dict = {}

    def _insert_row(self, dir_name, file_name):
        self._sql_handle.execute("INSERT INTO blob_paths (parent_name, blob_name) VALUES (?, ?)", [dir_name, file_name])

    def _append_file_system(self, file_name: str, dir_list: list[str]):
        self._insert_row(self._relative_name + '/' + '/'.join(dir_list), file_name)

    def _walk_file_system(self) -> t.Iterable[tuple[str, list[str], list[str]]]:
        cur = self._sql_handle.execute("SELECT DISTINCT parent_name FROM blob_paths ORDER BY length(parent_name) ASC")
        row = cur.fetchone()
        while row is not None:
            sub_dir_cur = self._sql_handle.execute("SELECT DISTINCT parent_name FROM blob_paths WHERE parent_name LIKE ? ORDER BY length(parent_name) ASC", [f"{row[0]}/%"])
            sub_dirs = set()
            sub_dir_row = sub_dir_cur.fetchone()
            while sub_dir_row is not None:
                actual_subdir = sub_dir_row[0][len(row[0]):].strip('/')
                if '/' not in actual_subdir:
                    sub_dirs.add(actual_subdir)
                sub_dir_row = sub_dir_cur.fetchone()
            files = list()
            file_cur = self._sql_handle.execute("SELECT DISTINCT blob_name FROM blob_paths WHERE parent_name = ?", [row[0]])
            file_cur_row = file_cur.fetchone()
            while file_cur_row is not None:
                files.append(file_cur_row[0])
                file_cur_row = file_cur.fetchone()
            yield row[0], list(sub_dirs), files
            row = cur.fetchone()

    def _append_memory(self, file_name: str, dir_list: list[str]):
        self._deep_append(self._memory_dict, dir_list, file_name)

    def _walk_memory(self) -> t.Iterable[tuple[str, list[str], list[str]]]:
        yield from self._recursive_yield(self._memory_dict)

    @staticmethod
    def _recursive_yield(d: dict) -> t.Iterable[tuple[str, list[str], list[str]]]:
        dir_names = list(d['_dirs'].keys())
        yield d['_full_name'], dir_names, d['_files']
        for subd_key in dir_names:
            yield from BlobWalker._recursive_yield(d['_dirs'][subd_key])

    @staticmethod
    def _deep_append(d: dict, dir_names: list, file_name: str):
        if not dir_names:
            d['_files'].append(file_name)
        else:
            next_name = dir_names.pop(0)
            if next_name not in d['_dirs']:
                d['_dirs'][next_name] = {
                    '_dirs': {},
                    '_files': [],
                    '_full_name': f"{d['_full_name']}/{next_name}"
                }
            BlobWalker._deep_append(d['_dirs'][next_name], dir_names, file_name)


class AzureBlobHandle(AzureBaseHandle):
    """Handle class for Azure blobs"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         essential_domain='.blob.core.windows.net',
                         supports=FeatureFlag.DEFAULT | FeatureFlag.TIERING | FeatureFlag.METADATA,
                         log_name='az_blob',
                         **kwargs)
        self.walk_max_memory = 1024 * 1024
        self._open_container = None
        self._open_blob = None

    @contextmanager
    def client(self) -> t.Generator[BlobClient, None, None]:
        """Build a blob client."""
        with self.container_client() as client:
            yield client.get_blob_client(self._get_full_path())

    @contextmanager
    def container_client(self) -> t.Generator[ContainerClient, None, None]:
        """Build a container client."""
        with self.persistent_connection("container", self._get_client, "container") as x:
            yield x

    @staticmethod
    def code_tier(tier: StandardBlobTier | None) -> StorageTier | None:
        if tier == StandardBlobTier.HOT:
            return StorageTier.FREQUENT
        elif tier == StandardBlobTier.COOL:
            return StorageTier.INFREQUENT
        elif tier == StandardBlobTier.ARCHIVE:
            return StorageTier.ARCHIVAL
        return None

    @staticmethod
    def decode_tier(tier: StorageTier | None) -> StandardBlobTier:
        if tier is StorageTier.FREQUENT:
            return StandardBlobTier.HOT
        elif tier == StorageTier.INFREQUENT:
            return StandardBlobTier.COOL
        elif tier == StorageTier.ARCHIVAL:
            return StandardBlobTier.ARCHIVE
        return StandardBlobTier.HOT

    @wrap_azure_errors
    def _stat(self) -> StatResult:

        try:
            with self.client() as client:
                props = client.get_blob_properties()
                return StatResult(
                    exists=True,
                    is_file=True,
                    is_dir=False,
                    metadata=props.metadata,
                    st_size=props.size,
                    st_mtime=AwareDateTime.from_datetime(props.last_modified, 'Etc/UTC'),
                    tier=self.code_tier(props.blob_tier)
                )
        except ResourceNotFoundError:
            if self._no_remote_is_dir():
                return StatResult(exists=True, is_dir=True, is_file=False)
            return StatResult(exists=False)

    @wrap_azure_errors
    def _streaming_read(self, buffer_size: int = None) -> t.Iterable[bytes]:
        with self.client() as client:
            stream = client.download_blob()
            yield from self._halt_flag.iterate(stream.chunks())

    def _fast_blob_upload(self, client: BlobClient, chunk: bytes, metadata: dict[str, str]) -> dict:
        return client.upload_blob(chunk, length=len(chunk), metadata=metadata)

    @staticmethod
    def _blob_streaming_upload(client: BlobClient, chunk: bytes, offset: int) -> BlobBlock:
        block_id = base64.b64encode(f"{offset:032d}".encode('utf-8')).decode('utf-8')
        client.stage_block(block_id, chunk, len(chunk))
        bb = BlobBlock(block_id, BlockState.UNCOMMITTED)
        bb.size = len(chunk)
        return bb

    def _streaming_write(self, chunks: t.Iterable[bytes], **kwargs):
        with self.client() as client_:
            bsr = ByteSequenceReader(chunks)
            block_size: int = 1024 * 1024 * 4  # this should take around 2 seconds at most with a good connection
            first_block = bytes(bsr.consume(block_size))
            metadata = kwargs.pop('metadata', {})
            tier = kwargs.pop('storage_tier', None)
            # fast upload for small blobs
            if bsr.at_eof():
                new_data = self._fast_blob_upload(client_, first_block, metadata)
                if tier is not None:
                    client_.set_standard_blob_tier(self.decode_tier(tier))
                self._update_stat(
                    is_file=True,
                    is_dir=False,
                    exists=True,
                    st_size=len(first_block),
                    metadata=metadata,
                    st_mtime=new_data['last_modified'],
                    tier=tier
                )

            # longer process for bigger blobs
            else:
                uncommitted = [self._blob_streaming_upload(client_, first_block, 0)]
                offset: int = len(first_block)
                while not bsr.at_eof():
                    self.breakpoint()
                    chunk = bytes(bsr.consume(block_size))
                    uncommitted.append(self._blob_streaming_upload(client_, chunk, offset))
                    offset += len(chunk)
                new_data = client_.commit_block_list(uncommitted, metadata=metadata)
                if tier is not None:
                    client_.set_standard_blob_tier(self.decode_tier(tier))
                self._update_stat(
                    is_file=True,
                    is_dir=False,
                    exists=True,
                    st_size=offset,
                    metadata=metadata,
                    st_mtime=new_data['last_modified'],
                    tier=tier
                )

    @wrap_azure_errors
    def _remove(self):
        try:
            with self.client() as client:
                client.delete_blob()
            self._update_stat(exists=False, is_dir=None, is_file=None, metadata=None, tier=None, st_mtime=None, st_size=None)
        except ResourceNotFoundError: ...

    def full_name(self):
        path = [x for x in self.parse_url().path.split('/') if x]
        return '/'.join(path[1:])

    @wrap_azure_errors
    def _walk(self) -> t.Iterable[tuple[str, list[str], list[str]]]:
        full_name = self.full_name()
        walker = BlobWalker(full_name, self.walk_max_memory)
        try:
            with self.container_client() as client:
                for blob_properties in self._halt_flag.iterate(client.list_blobs(name_starts_with=full_name)):
                    walker.append(blob_properties)
                yield from walker.walk_all()
        finally:
            walker.cleanup()
            del walker

    @wrap_azure_errors
    def _set_metadata(self, metadata: dict[str, str]):
        with self.client() as client:
            client.set_blob_metadata(metadata)
        self._update_stat(metadata=metadata)

    def mkdir(self, mode=0o777, parents: bool = True):
        # never need to worry about making directories here.
        pass

    @wrap_azure_errors
    def _set_tier(self, tier: StorageTier):
        if tier is None:
            return
        current_tier = self.get_tier()
        if current_tier != tier:
            with self.client() as client:
                client.set_standard_blob_tier(self.decode_tier(tier))
            self._update_stat(tier=tier)

    @staticmethod
    def supports(file_path: str) -> bool:
        if not (file_path.startswith("http://") or file_path.startswith("https://")):
            return False
        pieces = urlparse(file_path)
        return pieces.hostname is not None and pieces.hostname.endswith(".blob.core.windows.net")
