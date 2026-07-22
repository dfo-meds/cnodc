*******
Pipeman
*******
Pipeman is an in-house designed and built orchestration package in Python.
It is designed to push data through data management workflows in a robust and reliable fashion.

Workers
=======
Pipeman is built around the concept of a **worker** which handles one part of a data management **workflow**.
There are two kinds of workers:

- **Scheduled tasks** run on a schedule.
- **Queue handlers** wait for a queue item to be present, which it then handles.

Queue handlers can work on **payloads** or they can work on queue items that are not tied to a specific payload type.


Workflows
=========
A workflow is a sequence of steps that are taken when a new piece of data is received.
Typically they are triggered by receiving a new **data file** to be processed.

The data being processed is stored as a **payload**.
The type of payload may change as data progresses through the workflow.

A **workflow step** is a queue that the previous payload should be sent to for processing.
When the step completes, it is returned to a special queue that identifies the next step and starts it.

When designing a workflow, it is important to ensure that the output of the previous step is an appropriate input for the next step.

Workflows also may define one or more storage locations.
The incoming file is saved to each of these.
The "working" storage location is used as a ``FilePayload`` to start.

Payloads
========
A payload contains information about one or more objects that are being processed.
These include the following:

- A ``FilePayload`` refers to a specific file that exists in the CNODC's storage system.
- A ``SourceFilePayload`` refers to an entry in the NODB source files table.
- A ``BatchPayload`` refers to one or more entries in the NODB working records table.
- A ``WorkingRecordPayload`` refers to one specific entry in the NODB records table.
- A ``NewObservationsPayload`` refers to one or more new additions in the NODB observations table.
- A ``NewFilePayload`` refers to a specific file that exists outside of the CNODC's storage system.

Payloads are just a specific data structure stored in the data field of a queue item. They are otherwise processed
as queue items.

Pipeman Controller
==================
There are two ways of organizing and running workers in ``pipeman``:

1. A multiprocessing service is provided which can handle multiple workers.
2. A single worker can be run,  handled either by the operating system or by containerization.

Multiprocessing Service
------------------------
The multiprocessing service uses ``multiprocessing`` to spawn multiple processes.
This is the current method used at the CNODC for organizing workers.
Each process runs exactly one worker at a time.

Workers are configured using a YAML file containing a dictionary.
The keys of the dictionary are a unique identifier and the value is a dictionary describing how to configure and run the worker ::

  .. highlight:: yaml
  service_name:
    # fully-qualified name of the worker class
    class_name: my.package.class_name
    # number of instances of the worker to spawn (defaults to 1 if omitted)
    count: 1
    # specific configuration (as a dictionary) for the worker
    config:

If a worker terminates unexpectedly, it will be restarted.

The multiprocessing service also handles a few extra tasks:

- Signal handling is installed to properly shut-down all the sub-processes gracefully.
- It maintains an open pipe for communications which can be used to reload the configuration, restart all workers, or gracefully shut down.
- Subprocesses have their logs sent to the parent process for output.
- The available disk space and other key metrics are monitored for the entire system and added to both Prometheus telemetry and to the NODB process status table.

Single Worker Service
---------------------
A single worker works similarly but takes a YAML configuration file with only a single worker in it.
That worker is run in the same process.
There is no open pipe to communicate and no telemetry for the top-level process is provided.
The signal handling is installed in the same fashion and Prometheus telemetry is available.

System Halt
===========
Pipeman is designed to shutdown in a graceful fashion. There are two methods of shutting down the system:

1. SIGINT (or a Windows equivalent such as SIGBREAK) can be issued to the process (the top-level process for the multiprocessing service). This causes a fast shutdown.
2. The multiprocessing service can be issued a command to gracefully stop. This causes a slow shutdown on every worker.

A reload command to the multiprocessing service triggers a slow shutdown on any workers that have changes in their configuration.
They are then restarted.

In order to guarantee a graceful shutdown, pipeman should be run in a production environment that meets the following criteria:

1. Before a system shutdown is initiated, SIGINT is sent to all processes at least 30 seconds before SIGKILL is sent.
2. The system should have an uninterruptible power supply that provides at least 30 seconds of power. When there is a power failure, a system shutdown should be initiated.

In addition, programmers contributing to a data management pipeline should ensure their algorithms run for no more than 1-2 seconds without checking for a fast shutdown (see below).
They should also ensure, in the event of a fast shutdown, that the work being done is reverted.

Internally, this is handled through two flags - the **halt flag** (fast shutdown) and the **end flag** (slow shutdown).

Halt Flag
---------
There is one halt flag shared between all processes.
All long-running algorithms that take more than a second or two to finish their work are designed to check the halt flag regularly.
If they see the halt flag has been set to ``true``, they stop their work and revert it to a known good state.
They then raise ``HaltInterrupt`` allowing parent algorithms to clean themselves up as well.
Workers should be designed to run no longer than 1-2 seconds between checking the halt flag.

As one example, file transfers have been designed to work on small chunks no bigger than ~1 MiB.
If the halt flag is set during a file upload, the file transfer stops and any data already written is deleted.

End Flag
--------
Each process maintains its own end flag which is available to the top-level process.
The end flag is checked between each queue item for queue handlers and between each scheduled task execution for scheduled tasks.
Setting the end flag to ``true`` allows each process to finish its current task or item, after which it stops processing.

Error Management
================
Pipeman errors usually use ``CodedError`` as a base exception.
It provides a **code space**, a **code** and a text message for every error.
The code space and code should be unique.
This allows the system administrator to quickly locate the cause of an error.

Transient Errors
----------------
The other feature of ``CodedError`` is an ``is_transient`` flag.
This flag should only be set if the error is caused by a temporary condition external to pipeman and we can expect it to resolve at some point in the future.
Some examples include database connection errors, external connection or timeout errors, etc.

When a queue handler encounters a transient error, the queue item will be retried automatically in a few minutes.
Non-transient errors when handling a queue item cause the item to be marked as a failure. It will not be retried.

