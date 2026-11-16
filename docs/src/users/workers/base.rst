Base Worker
===========

All workers have these configuration options.


Configuration
-------------

.. confval:: BaseWorker#save_file
   :type: str
   :default: None

   The path to a save file, in a format that the :doc:`../storage` will recognize.

.. confval:: BaseWorker#max_check_delay_seconds
   :type: float
   :default: 1.0

   The maximum delay between checks to see if the worker should halt.
