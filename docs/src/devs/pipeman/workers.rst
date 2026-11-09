Workers
=======

Workers derived from :py:class:`pipeman.processing.base_worker.BaseWorker`. They call
``on_start()`` when the worker starts running, then they enter a loop of calling ``_run_once()``
repeatedly until the worker needs to exit. ``_run_once()`` returns a float, which is the time
to wait until calling the method again. When the worker exits, it calls ``on_exit()`` unless
a :py:class:`KeyboardInterrupt` was raised.

Workers use two flags (which implement :py:class:`medsutil.halts.HaltFlag`) to determine if
they should end. The halt flag is provided by the worker orchestration tool - it gets set
if the system is shutting down. Workers should be very responsive to the halt flag being set.
The other flag is the end flag, which is used to indicate that the worker should restart. Workers
can complete their current task before restarting.

.. warning::

   Workers must regularly check the halt flag by calling ``breakpoint()`` or directly checking the
   halt flag while executing a long running task. Workers are expected to exit within 15 seconds
   of the halt flag being set at most.

Workers are configured using a key-value dictionary. There are three possible sources for this configuration:

1. Configuration provided by a queue item currently being processed.
2. The configuration provided in the worker configuration file
3. The default configuration provided in the source code

All workers have two common configuration values.

.. csv-table::
   :header: "Key","Data Type","Default","Description"

   save_file,str | None,None,The path to a file to save data between runs
   max_check_delay_seconds,float,1,The maximum delay in seconds to wait before checking the halt flag

Worker implementations should call ``before_cycle()`` and ``after_cycle()`` to clear out any
cached information from the last worker execution.

Each worker maintains at least two metrics: ``last_run`` (the time the worker last ran) and ``run_time``
(the time from start-to-finish for an item to run).

.. note::

   You should use the built-in ``metrics`` property to obtain a Prometheus-compatible metrics object.
   Otherwise, the metrics may not be disabled properly during testing or configured properly in production.

Workers can be registered in the database. They then report their current status using ``report()``.
Most of this is built-in for you, it helps provide an overview of the system as it is operating.

If a worker raises an exception while processing, the worker is automatically restarted. The error is
logged to disk.



There are two main types of workers: scheduled tasks and queue-based workers.

Scheduled Tasks
---------------

A schedule task is derived from :py:class:`pipeman.processing.scheduled_task.ScheduledTask`.
It executes on a schedule, which may be either on a delay from its last execution or using a cron-style syntax.
When the task reaches its next execution time, the ``execute()`` method is called on the task object.

Scheduled tasks all have these common configuration options:

.. csv-table::
   :header: "Key","Data Type","Default","Description"

   run_on_boot,bool,False,Whether to run on boot always, regardless of the delay (works in all modes)
   schedule_mode,str,from_completion,"The scheduling mode: one of from_start, from_completion, cron ,cron_from_start, or boot_only"
   delay_seconds,float | None,None,The delay from last task completion or start (from_start or from_completion only)
   delay_fuzz_milliseconds,float,1000,If delay_seconds is used, adds a random value up to this number
   cron,str | None,None,A cron configuration string (cron or cron_from_completion only)
   cron_job_hash,str | None,None,A unique string to seed the random number generator for cron.


Queue-Based Workers
-------------------
Queue-based workers use the queue system to track their work. Each worker is associated with a
queue name that it pulls its work from. The work is stored as payloads, which are structured ways
of storing data for a worker.


Queue-based workers all have these common configuration options:

.. csv-table::
   :header: "Key","Data Type","Default","Description"

   queue_name,str,"",The name of the queue to pull items from
   delay_time_seconds,float,0.25,The initial time to wait after not finding any new item
   delay_factor,float,2,The factor to multiple the delay time by after each subsequent failure.
   max_delay_time_seconds,float,128,The maximum delay time between checking the queue.
   retry_delay_seconds,float,0,The time to wait before retrying an item that failed.
   deprioritize_failures,bool,False,"If true, failures will be reduced in priority."
   allow_queue_item_config,bool,True,"If false, items cannot specify their own configuration settings."

