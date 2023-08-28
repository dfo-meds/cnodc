from autoinject import injector

import pathlib
import zirconium as zr

from cnodc.exc import CNODCError
from cnodc.files import FileController
from cnodc.nodb import NODBDatabaseProtocol, NODBQueueProtocol, NODBSourceFile
from cnodc.nodb.structures import SourceFileStatus
from cnodc.util import HaltFlag


class DataSearchController:

    file_controller: FileController = None
    database: NODBDatabaseProtocol = None
    queues: NODBQueueProtocol = None

    @injector.construct
    def __init__(self, instance: str, halt_flag: HaltFlag):
        self.name = "NODB_SEARCH"
        self.version = "1_0_0"
        self.instance = instance
        self.halt_flag = halt_flag

    def search_dir(self, search_dir: pathlib.Path, pattern: str, recursive: bool = True):

        # Make sure search_dir is a directory
        handle = self.file_controller.get_handle(search_dir)
        if not handle.is_dir():
            raise CNODCError(f"Directory [{search_dir}] does not exist or is not a directory", "SEARCH", 1000, is_recoverable=True)

        # Loop through all the files that match the given pattern
        for file_handle in handle.search(pattern, recursive, self.halt_flag):

            # Build a source file object
            source_file = NODBSourceFile()
            source_file.source_path = file_handle.path()
            source_file.file_name = file_handle.name()
            source_file.status = SourceFileStatus.NEW
            source_file.add_history("Record created", self.name, self.version, self.instance)
            self.database.save_source_file(source_file)

            # Queue it
            try:
                source_file.status = SourceFileStatus.QUEUED
                self.queues.queue_source_file_download(source_file)
            except Exception as ex:
                source_file.status = SourceFileStatus.QUEUE_ERROR
                source_file.set_metadata("queue_error", f"{ex.__class__.__name__}: {str(ex)}")
                raise ex
            finally:
                self.database.save_source_file(source_file)
