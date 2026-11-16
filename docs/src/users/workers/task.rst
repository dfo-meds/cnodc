Scheduled Tasks
===============

All scheduled tasks have these configuration options.


Configuration
-------------
.. confval:: ScheduledTask#schedule_mode
   :type: str
   :default: from_completion

   Must be one of the following options:

   - from_completion: Delays the next run from when the last run ended
   - from_start: Delays the next run from when the last run started (note: runs cannot overlap)
   - cron: The next run is scheduled using a cron expression after the run ends
   - cron_from_start: The next run is scheduled using a cron expression after the run starts
   - boot_only: Runs on boot only (you must set ``run_on_boot`` to ``True`` as well)

.. confval:: ScheduledTask#delay_fuzz_milliseconds
   :type: int
   :default: 1000

   When the ``schedule_mode`` is ``from_completion`` or ``from_start``, a random
   number of milliseconds from 0 to this value are added to the delay time.

.. confval:: ScheduledTask#run_on_boot
   :type: bool
   :default: False

   Set to ``True`` to always run this task once on boot.

.. confval:: ScheduledTask#delay_seconds
   :type: int
   :default: None

   The delay, in seconds, from when the last job completed (if the mode is ``from_completion``)
   or started (if ``from_start``) until the next one should run. Does nothing for cron-based modes.

.. confval:: ScheduledTask#cron
   :type: str
   :default: None

   The cron expression to use with ``cron`` and ``cron_from_start`` modes.

.. confval:: ScheduledTask#cron_job_hash
   :type: str
   :default: None

   A unique value used to seed the random number generator for fuzzing ``cron`` values.
