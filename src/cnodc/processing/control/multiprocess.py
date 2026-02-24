"""Controller for multiple processes based on the multiprocessing library."""
import multiprocessing as mp

from cnodc.process.base import BaseController, BaseProcess


class _MultiProcessRunner(mp.Process, BaseProcess):
    """Implementation of a process that runs a worker class."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs, end_flag=mp.Event())


class MultiProcessController(BaseController):
    """Controller for running multiple workers based on the multiprocessing library.

        Workers are defined in a configuration file that includes which class to
        load, how many workers to run, and their configuration.

        The controller also defines a flag file that, if set, will cause the controller
        to reload all configuration from the original file and update all
        running processes. This is useful to make configuration changes on the fly without
        restarting the entire system.
    """

    def __init__(self, **kwargs):
        super().__init__(
            process_runner=_MultiProcessRunner,
            log_name="cnodc.multi_process",
            halt_flag=mp.Event(),
            **kwargs
        )

