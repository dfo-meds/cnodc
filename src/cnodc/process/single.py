import threading
import signal
import uuid

import zrlog

from cnodc.util import dynamic_object


class SingleProcessController:

    def __init__(self, process_name: str, process_cls_name: str, config: dict = None):
        self._halt_flag = threading.Event()
        self._log = zrlog.get_logger('cnodc.single_processctl')
        self._break_count = 0
        self._process_name = process_name
        self._process_cls_name = process_cls_name
        self._process_config = config

    def _register_halt_signal(self, sig_name):
        if hasattr(signal, sig_name):
            signal.signal(getattr(signal, sig_name), self._handle_halt)

    def _handle_halt(self, sig_num, frame):
        self._log.info(f"Signal {sig_num} caught")
        self._halt_flag.set()
        self._break_count += 1
        if self._break_count >= 3:
            self._log.critical(f"Critical halt")
            raise KeyboardInterrupt()

    def loop(self):
        self._log.debug("Registering halt signals")
        self._register_halt_signal("SIGINT")
        self._register_halt_signal("SIGTERM")
        self._register_halt_signal("SIGBREAK")
        self._register_halt_signal("SIGQUIT")
        process = dynamic_object(self._process_cls_name)(
            process_name=self._process_name,
            process_uuid=str(uuid.uuid4()),
            halt_flag=self._halt_flag,
            config=self._process_config
        )
        # NB: Usually this is process.start() but
        # since we are running a single process, we
        # will run its run() loop in the main process
        # instead.
        process.run()
