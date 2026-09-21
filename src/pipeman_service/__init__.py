"""This package contains the PIPEMAN service code, which orchestrates workers.

Contains the following module. You should use the boot module to run the PIPEMAN service.

* boot contains the code to initialize the multi-process and single-process workers.
* controller contains the base code shared by both
* multiprocess contains code specific to the multi-process implementation
* single contains code specific to the single-process implementation

The PIPEMAN service is managed by a controller.
This controller monitors for changes in configuration and orchestrates workers.
In general, you can view this as:

PARENT CONTROLLER
- Worker 1
- Worker 2
- Worker 3
- ...

The default implementation uses multiprocessing to run workers without the GIL.
"""