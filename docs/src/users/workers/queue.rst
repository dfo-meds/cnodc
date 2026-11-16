Queue Worker
============

Common configuration settings for queue workers.


Configuration
-------------

.. confval:: QueueWorker#queue_name
   :default: None
   :type: str

   The queue name to receive items from.

.. confval:: QueueWorker#delay_time_seconds
   :default: 0.25
   :type: float

   The initial time to wait, in seconds, if there are no items in the queue.

.. confval:: QueueWorker#delay_factor
   :default: 2
   :type: float

   The time to multiply the last delay time by for every subsequent time there are no items in the queue.

.. confval:: QueueWorker#max_delay_time_seconds
   :default: 128
   :type: float

   The maximum time to wait between checking for new queue items (i.e. the cap on the delay time).

.. confval:: QueueWorker#retry_delay_seconds
   :default: 0
   :type: float

   The time to wait before retrying a failed queue item.

.. confval:: QueueWorker#deprioritize_failures
   :default: False
   :type: bool

   If true, the priority of failed items is reduced by 1 each time they fail.

.. confval:: allow_queue_item_config
   :type: bool
   :default: True

   If false, workflows and other queue workers cannot update the configuration of this worker.
