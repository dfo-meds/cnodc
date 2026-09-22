""" This module provides a controller that works with the multiprocessing library."""
import os
import multiprocessing as mp

from autoinject import injector
import zirconium as zr
from prometheus_client.multiprocess import mark_process_dead

from medsutil.types import SupportsEvent
from nodb.interface import NODB
from pipeman_service.controller import BaseController, BaseRunner


class ImprovedEvent:
    """ Multiprocessing-compatible event

        .. note: In testing, using mp.Event for this purpose caused a lot of deadlocks.
                 So, I designed a replacement that works in a similar fashion to what I expected.
    """

    def __init__(self):
        """ Creates the event object. """
        self._value = mp.Value('b')
        self._value.value = 0

    def is_set(self):
        """ Checks if the event has been set. """
        return self._value.value == 1

    def set(self):
        """ Sets the event. """
        self._value.value = 1

    def clear(self):
        """ Clears the event. """
        self._value.value = 0


class _MultiProcessRunner(BaseRunner, mp.Process):
    """ Multiprocessing implementation of BaseRunner.

        This class subclasses BaseRunner and mp.Process so that
        run() is run in its own process.

        The setup() and teardown() methods have been overridden to
        handle issues with logging and prometheus.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, end_flag=ImprovedEvent(), daemon=True)

    def setup(self):

        # Since this is a sub-process, we need to re-initiated everything
        from pipeman.boot import init_pipeman
        init_pipeman('cli')

        # Handle the logging queue
        if self._proc_info.logging_queue is not None:
            import medsutil.logging as ml
            ml.init_as_subprocess(self._proc_info.logging_queue)

        super().setup()

    def teardown(self):

        # Clean up prometheus directory if specified.
        if os.environ.get("PROMETHEUS_MULTIPROC_DIR", default=None):
            mark_process_dead(os.getpid())

        super().teardown()


class PrometheusMultiProcessServer(mp.Process):
    """ A sub-process that delivers Prometheus statistics on port 80. """

    def __init__(self, *args, halt: SupportsEvent, **kwargs):
        self._halt = halt
        super().__init__(*args, **kwargs)

    def run(self):

        # run the standard WSGI server provided by prometheus
        from prometheus_client import start_http_server

        wsgi, thread = start_http_server(80)

        # wait until halt is set
        import time
        while not self._halt.is_set():
            time.sleep(4)

        # close the server.
        wsgi.shutdown()
        wsgi.server_close()
        thread.join()


class MultiProcessController(BaseController):
    """ Controller for running multiple workers based on the multiprocessing library.

        Workers are defined in a configuration file that includes which class to
        load, how many workers to run, and their configuration.

        The controller also defines a flag file that, if set, will cause the controller
        to reload all configuration from the original file and update all
        running processes. This is useful to make configuration changes on the fly without
        restarting the entire system.
    """

    config: zr.ApplicationConfig = None
    nodb: NODB = None

    @injector.construct
    def __init__(self, **kwargs):

        # A multiprocessing-compatible logging setup so that all
        # logging messages are directed to the top process (otherwise
        # they are not output by Docker).
        import medsutil.logging as ml
        lc = self.config.as_dict("logging", default={})
        lq = mp.Queue()
        self._logging_subprocess = ml.init_subprocess_handler(
            lq,
            logging_config={
                x: lc[x]
                for x in lc.keys()
            }
        )
        ml.init_as_subprocess(lq)

        # Standard setup stuff
        super().__init__(
            process_creator=_MultiProcessRunner,
            log_name="pipeman_service.mp_controller",
            logging_queue=lq,
            halt_flag=ImprovedEvent(),
            _no_report=False,
            **kwargs
        )

        # The prometheus statistics process for HTTP requests
        self._mp_process: PrometheusMultiProcessServer | None = None

    def startup(self):
        super().startup()

        # start the prometheus server as well
        self._mp_process = mp_process = PrometheusMultiProcessServer(halt=self._halt_flag)
        mp_process.start()

    def cleanup(self):
        super().cleanup()

        # end the prometheus server
        if self._mp_process is not None:
            self._mp_process.join()

        # end the logging process
        self._logging_subprocess.stop()
