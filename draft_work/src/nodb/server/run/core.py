import zirconium as zr
from autoinject import injector
import socket
import signal

from cnodc.exc import CNODCError
from cnodc.util import dynamic_object
import zrlog


class LaunchManager:

    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self):
        self._runner = None
        self._log = zrlog.get_logger("cnodc.launch")
        self._break_count = 0

    def launch(self):
        self._register_halt_signal("SIGINT")
        self._register_halt_signal("SIGTERM")
        self._register_halt_signal("SIGBREAK")
        self._register_halt_signal("SIGQUIT")
        controller_classes = self.config.as_dict(("cnodc", "launch", "controllers"), default={})
        instance_name = self.config.as_str(("cnodc", "launch", "instance_name"), default=None)
        if instance_name is None or instance_name == '':
            instance_name = socket.gethostname()
        runner_cls = self.config.as_str(("cnodc", "launch", "runner_class"), default="cnodc.run.threads.ThreadRunner")
        self._runner = dynamic_object(runner_cls)(instance_name)
        if not hasattr(self._runner, 'request_halt'):
            raise CNODCError("Runner class is missing request_halt method", "LAUNCH", 1000)
        if not hasattr(self._runner, 'launch_all'):
            raise CNODCError("Runner class is missing launch_all method", "LAUNCH", 1001)
        if not controller_classes:
            raise CNODCError("No controller classes provided", "LAUNCH", 1002)
        if not instance_name:
            raise CNODCError("No instance name provided", "LAUNCH", 1003)
        self._log.info(f"Starting runner [{runner_cls}]")
        self._runner.launch_all(controller_classes)

    def sig_handle(self, sig_num, frame):
        """Handle signals."""
        self._log.info(f"Signal {sig_num} caught, halting")
        self._runner.request_halt()
        self._break_count += 1
        if self._break_count >= 3:
            self._log.critical(f"Program halting unexpectedly")
            raise KeyboardInterrupt()

    def _register_halt_signal(self, sig_name):
        if hasattr(signal, sig_name):
            signal.signal(getattr(signal, sig_name), self.sig_handle)
