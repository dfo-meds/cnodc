"""This package contains the PIPEMAN service code, which orchestrates workers.

Contains the following module. You should use the boot module to run the PIPEMAN service.

* boot contains the code to initialize the multi-process and single-process workers.
* controller contains the base code shared by both
* multiprocess contains code specific to the multi-process implementation
* single contains code specific to the single-process implementation

The PIPEMAN service is managed by a controller.
This controller monitors for changes in configuration and orchestrates workers.

CONTROLLER
- Work Runner Set A
  - Work Runner 1 -> Worker 1
  - Work Runner 2 - > Worker 2
  - ... (to quota)
- Work Runner Set B
- ... (one for each worker defined)

Its worth noting that sometimes "process" was used to describe a "worker" here.
I've worked on cleaning up this terminology but instances of it linger.
Be aware.

The default implementation uses multiprocessing to run workers without the GIL.
"""