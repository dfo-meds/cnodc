import pathlib

from cnodc.exc import CNODCError
from cnodc.files.files import DirFileHandle
from cnodc.nodb import NODBSourceFile
import typing as t

from cnodc.nodb.structures import SourceFileStatus
from cnodc.util import dynamic_class
from cnodc.decode.common import CodecProtocol
from cnodc.files import FileController
import tempfile as tf


class DataExtractionController:

    def __init__(self, name, version):
        self.name = name
        self.version = version
        self.file_controller = FileController()

    def intake_source_file(self, source_file: NODBSourceFile):
        """Download and extract all records. """
        with tf.TemporaryDirectory() as tdir:
            tdir = pathlib.Path(tdir)
            local_file = tdir / source_file.file_name
            # Get the file, from persistent or not persistent storage
            src_handle = self._download_source_file(local_file, source_file)
            # Create records
            self._create_records_from_source(local_file, source_file)
            # Mark the file as complete
            self._mark_source_complete(source_file, src_handle)

    def _download_source_file(self, local_file: pathlib.Path, source_file: NODBSourceFile) -> t.Optional[DirFileHandle]:
        """Download and persist the source file."""
        # If the persistent file is specified and exists, use it
        if source_file.persistent_path:
            persistent_handle = self.file_controller.get_handle(source_file.persistent_path)
            if persistent_handle.exists():
                persistent_handle.download(local_file)
                return None
            else:
                source_file.persistent_path = None

        # Download the file
        source_handle = self.file_controller.get_handle(source_file.source_path)
        source_handle.download(local_file)

        # Persist the downloaded file
        target_dir = source_file.get_metadata("target_dir")
        if target_dir is None:
            raise CNODCError("Missing target directory information", "EXTRACT", 1001)
        target_dir_handle = self.file_controller.get_handle(target_dir)
        if not target_dir_handle.exists():
            raise CNODCError(f"Target directory [{target_dir}] does not exist", "EXTRACT", 1002)
        persistent_handle = target_dir_handle.child(source_file.file_name)
        persistent_handle.upload(local_file)
        source_file.persistent_path = str(persistent_handle)
        return source_handle

    def _create_records_from_source(self, local_path: pathlib.Path, source_file: NODBSourceFile):
        """Create all records that don't already exist."""
        decoder = self._get_decoder(source_file)
        for
        yield from decoder.load(local_path)

    def _get_decoder(self, source_file: NODBSourceFile) -> CodecProtocol:
        decoder_cls_name = source_file.get_metadata('decoder_class_name', None)
        if decoder_cls_name is None:
            decoder_cls_name = self._auto_detect_decoder(source_file.file_name)
            if decoder_cls_name is None:
                raise CNODCError("Missing decoder information and could not auto-detect", "EXTRACT", 1000)
        return dynamic_class(decoder_cls_name)()

    def _auto_detect_decoder(self, file_name: str) -> t.Optional[str]:
        file_name = file_name.lower()
        if file_name.endswith(".bufr"):
            return "cnodc.decode.wmo.bufr.GTSBufrStreamCodec"
        return None

    def _mark_source_complete(self, source_file: NODBSourceFile, src_handle: t.Optional[DirFileHandle]):
        """Mark the source file as being completed."""
        source_file.status = SourceFileStatus.COMPLETE
        if source_file.get_metadata('delete_after_download', default=False):
            src_handle.delete()
