"""Controller for multiple processes based on the multiprocessing library."""
from autoinject import injector
import multiprocessing as mp
import zirconium as zr
from pipeman_service.controller import BaseController, BaseProcess

class _MultiProcessRunner(BaseProcess, mp.Process):
    """Implementation of a process that runs a worker class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, end_flag=mp.Event(), daemon=True)

    def setup(self):
        from pipeman.boot import init_cnodc
        init_cnodc('cli')
        if self._proc_info.logging_queue is not None:
            import medsutil.logging as ml
            ml.init_as_subprocess(self._proc_info.logging_queue)
        super().setup()



class MultiProcessController(BaseController):
    """Controller for running multiple workers based on the multiprocessing library.

        Workers are defined in a configuration file that includes which class to
        load, how many workers to run, and their configuration.

        The controller also defines a flag file that, if set, will cause the controller
        to reload all configuration from the original file and update all
        running processes. This is useful to make configuration changes on the fly without
        restarting the entire system.
    """

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self, **kwargs):
        #self._man = mp.Manager()
        lq = mp.Queue()
        super().__init__(
            process_creator=_MultiProcessRunner,
            log_name="cnodc.multi_process",
            logging_queue=lq,
            halt_flag=mp.Event(),
            **kwargs
        )
        import medsutil.logging as ml
        ml.init_as_subprocess(lq)
        log_config = dict(self.config.as_dict("logging", default={}))
        self._logging_subprocess = ml.init_subprocess_handler(lq, logging_config=log_config)

    def cleanup(self):
        self._logging_subprocess.stop()
        super().cleanup()
