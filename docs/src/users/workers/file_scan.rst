File Scanner
============

This scheduled task checks a location that is compatible with the :doc:`../storage`
for new files regularly and queues them for downloader by the :doc:`file_downloader`
which will intake them into the given workflow.





Configuration
-------------
.. confval:: FileScanTask#scan_target
   :type: str
   :required:

   The path to scan for new files, in a form that the :doc:`../storage` will understand.

.. confval:: FileScanTask#workflow_name
   :type: str
   :required:

   The workflow name to queue new files in.

.. confval:: FileScanTask#queue_name
   :type: str
   :default: file_download

   The name of the downloader queue.

.. confval:: FileScanTask#pattern
   :type: str
   :default: *

   A POSIX-style (aka glob) pattern that files must match.
   Note that :py:func:`fnmatch.fnmatch` is used to assess if names match the given pattern.

.. confval:: FileScanTask#recursive
   :type: bool
   :default: False

   If true, all sub-directories will also be searched

.. confval:: FileScanTask#add_file_name_to_source
   :type: bool
   :default: False

   If present, the original name of the file will be used as the ``source-file-identifier`` header.

.. confval:: FileScanTask#source_file_prefix
   :type: str
   :default: None

   If present, the text given will be prepended to the ``source-file-identifier`` header.

.. confval:: FileScanTask#remove_downloaded_files
   :type: bool
   :default: False

   If true, the system will attempt to delete files once they have been queued.

.. confval:: FileScanTask#reprocess_updated_files
   :type: bool
   :default: False

   If true, the system will track the modified dates of files and re-intake any files that have been changed.

.. confval:: FileScanTask#metadata
   :type: dict[str, str]
   :default: None

   Additional metadata to submit alongside the file.

.. confval:: FileScanTask#downloader_config
   :type: dict[str, str]
   :default: None

   Configuration for the downloader, see :doc:`file_downloader`.


See Also
````````

* :confval:`ScheduledTask#schedule_mode`
* :confval:`ScheduledTask#delay_fuzz_milliseconds`
* :confval:`ScheduledTask#run_on_boot`
* :confval:`ScheduledTask#delay_seconds`
* :confval:`ScheduledTask#cron`
* :confval:`ScheduledTask#cron_job_hash`
* :confval:`BaseWorker#save_file`
* :confval:`BaseWorker#max_check_delay_seconds`
