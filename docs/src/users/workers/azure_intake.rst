Azure Intake Worker
===================

This worker intakes files from the Azure Queue system.
This is mostly designed to be used with Sarracenia for the moment.

Queue items are expected to be JSON dictionaries with at least a ``path`` and a ``workflow`` key.
All other keys are treated as being metadata for :doc:`../workflow`.


Configuration
-------------

.. confval:: AzureQueueWorker#azure_account_name
   :type: str
   :required:

   The account name to connect with.

.. confval:: AzureQueueWorker#azure_queue_name
   :type: str
   :required:

   The name of the Azure Queue to connect to.

.. confval:: AzureQueueWorker#azure_hidden_time_seconds
   :type: int
   :default: 60

   How long to hide queue items for.

.. confval:: AzureQueueWorker#delay_time_seconds
   :type: int
   :default: 5

   The time to wait between checks for more files.
