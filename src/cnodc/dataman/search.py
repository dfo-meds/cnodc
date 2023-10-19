import time

from autoinject import injector
import datetime
import pathlib
import typing as t
import zirconium as zr

from cnodc.exc import CNODCError
from cnodc.files import FileController
from cnodc.nodb import NODBController, NODBSourceFile
from cnodc.nodb.structures import SourceFileStatus
from cnodc.util import HaltFlag
from cnodc.dataman.base import BaseController


class DataSearchController(BaseController):

    file_controller: FileController = None
    database: NODBController = None
    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self, instance: str, instance_no: int, halt_flag: HaltFlag):
        super().__init__("nodb_search", "1_0_0", instance, instance_no, halt_flag)
        self.sources = self.config.as_dict(('cnodc', 'sources', str(self.instance_no)), default={})
        self._last_scans = {}

    def check_config(self):
        good_config = True
        allowed_keys = ['search_dir', 'pattern', 'recursive', 'remove_completed', 'qc_workflow_name', 'metadata',
                        'scan_delay_seconds']
        for source_key in self.sources:
            if 'search_dir' not in self.sources[source_key]:
                self.log.error(f"Source [{source_key}] missing 'search_dir'")
                good_config = False
            if 'pattern' not in self.sources[source_key]:
                self.log.warning(f"Source [{source_key}] does not define 'pattern', all files will be downloaded")
            if 'remove_completed' in self.sources[source_key] and self.sources[source_key]['remove_completed']:
                self.log.notice(f"Source [{source_key}] configured to remove files after download")
            for x in self.sources[source_key]:
                if x not in allowed_keys:
                    self.log.error(f"Invalid configuration key '{x}' in source [{source_key}]")
                    good_config = False
        if not self.sources:
            self.log.warning(f"No sources defined")
        return good_config

    def _run(self):
        remove_keys = []
        for source_key in self.sources:
            if self.halt_flag.check(False):
                break
            try:
                self.search_dir(source_key, **self.sources[source_key])
            except CNODCError as ex:
                if ex.is_recoverable:
                    self.log.exception(f"Recoverable error while searching source [{source_key}]")
                else:
                    self.log.exception(f"Unrecoverable error while searching source [{source_key}], removing entry")
                    remove_keys.append(source_key)
        for source_key in remove_keys:
            del self.sources[source_key]
            if not self.sources:
                self.log.warning(f"No more sources defined")

    def search_dir(self,
                   source_key: str,
                   search_dir: pathlib.Path,
                   pattern: str = None,
                   recursive: bool = True,
                   remove_completed: bool = False,
                   qc_workflow_name: t.Optional[bool] = None,
                   scan_delay_seconds: float = None,
                   metadata: t.Optional[dict[str, t.Any]] = None):

        # Check if at least scan_delay_seconds has elapsed since the last scan completed
        if (scan_delay_seconds is not None
                and source_key in self._last_scans
                and ((time.monotonic() - self._last_scans[source_key]) < scan_delay_seconds)):
            return

        # Make sure search_dir is a directory
        handle = self.file_controller.get_handle(search_dir)
        if handle is None:
            raise CNODCError(f"Directory [{search_dir}] is not a recognizable path", "SEARCH", 1001, is_recoverable=False)

        if not handle.is_dir():
            raise CNODCError(f"Directory [{search_dir}] does not exist or is not a directory", "SEARCH", 1000,
                             is_recoverable=True)

        # Loop through all the files that match the given pattern
        with self.database as db:
            self.log.info(
                f"Searching [{pattern}][recursive={recursive}][qc_workflow={qc_workflow_name}][remove_completed={remove_completed}]")
            for file_handle in handle.search(pattern, recursive, self.halt_flag):

                # Check if the file already exists and continue if it does
                db.execute("SELECT COUNT(*) FROM nodb_source_files WHERE source_path = %s", [file_handle.path()])
                results = db.fetchone()
                if results[0] > 0:
                    self.log.debug(f"Skipping [{file_handle.path()}], record already exists")
                    continue

                # Build a source file object
                self.log.debug(f"Creating record for [{file_handle.path()}]")
                source_file = NODBSourceFile()
                mod_date = file_handle.modified_datetime()
                if mod_date is None:
                    # Use current UTC time
                    mod_date = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=0)))
                else:
                    # Convert to UTC time
                    mod_date = mod_date.astimezone(datetime.timezone(datetime.timedelta(hours=0)))
                source_file.partition_key = mod_date.date()
                source_file.source_path = file_handle.path()
                source_file.file_name = file_handle.name()
                source_file.status = SourceFileStatus.QUEUED
                source_file.add_history("Record created", self.name, self.version, self.instance)
                if qc_workflow_name is not None:
                    source_file.qc_workflow_name = qc_workflow_name
                if metadata is not None:
                    source_file.metadata = {x: metadata[x] for x in metadata}

                # Save it and queue it
                self.log.debug(f"Saving source file")
                db.save_source_file(source_file)
                db.create_queue_item(
                    "source_files",
                    {
                        "source_uuid": source_file.source_uuid,
                        "partition_key": source_file.partition_key.strftime("%Y%m%d")
                    },
                )
                db.commit()
                self._last_scans[source_key] = time.monotonic()

                # If desired, remove completed files
                if remove_completed:
                    self.log.info(f"Removing source file [{handle.path()}]")
                    handle.remove()
