File Download Worker
====================

Expects a :py:class:`pipeman.processing.payloads.NewFilePayload`.

This worker downloads files from a remote source and sends them to a :doc:`../workflow`.

.. note::

   The state of a file is stored in the database and will not be reprocessed unless there is a transient error while downloading it.
   To restart a file download, you need to requeue the item and also clear the error flag from the database.


Configuration
-------------

.. confval:: FileDownloadWorker#allow_file_deletes
   :type: bool
   :default: False

   If true, the worker can delete files from the source once they've been downloaded.


See Also
````````

* :confval:`QueueWorker#queue_name` (default is ``file_download``)
* :confval:`QueueWorker#delay_time_seconds`
* :confval:`QueueWorker#delay_factor`
* :confval:`QueueWorker#max_delay_time_seconds`
* :confval:`QueueWorker#retry_delay_seconds`
* :confval:`QueueWorker#deprioritize_failures`
* :confval:`QueueWorker#allow_queue_item_config`