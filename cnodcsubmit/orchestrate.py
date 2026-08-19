import logging
import pathlib
import signal
import threading
import typing as t
import yaml
import queue

from cnodcsubmit.scanner import CNODCLocalScanner
from cnodcsubmit.sender import CNODCSubmissionManager


def run():
    config = {}
    for x in ["~/.cnodc.yaml", "./.cnodc.yaml"]:
        file: pathlib.Path = pathlib.Path(x).expanduser().absolute()
        if file.exists():
            with open(file, "r") as h:
                config.update(yaml.safe_load(h) or {})
    orchestrate = CNODCOrchestrator(config)
    orchestrate.run_forever()


class CNODCOrchestrator:

    def __init__(self, config: dict[str, t.Any]):
        self.config = config
        self._halt = threading.Event()
        self._break_count = 0
        self._transfer_queue = queue.Queue()
        self._threads: dict[str, threading.Thread] = {}
        self._log = logging.getLogger("cnodc.orchestrate")

    def run_forever(self):
        self._register_halt_signal("SIGINT")
        self._register_halt_signal("SIGTERM")
        self._register_halt_signal("SIGBREAK")
        self._register_halt_signal("SIGQUIT")
        self._register_halt_signal("SIGABRT")
        self._transfer_queue = queue.Queue()
        while not self._halt.is_set():
            self.spawn_all()

    def _register_halt_signal(self, sig_name):
        """Register a halt signal"""
        if hasattr(signal, sig_name):
            self._log.debug("Registering signal %s", sig_name)
            signal.signal(getattr(signal, sig_name), self._handle_halt)

    def _handle_halt(self, sig_num, frame=None):
        self._log.info("Signal %s caught", sig_num)
        self._halt.set()
        self._break_count += 1
        self._log.debug("Break count [%s], halt flag [%s]", self._break_count, self._halt.is_set())
        if self._break_count >= 3:
            self._log.critical("Critical halt")
            raise KeyboardInterrupt

    def spawn_all(self):
        self.spawn_sender()
        for key in self.config["scanners"].keys():
            self.spawn_scanner(key)

    def spawn_scanner(self, scanner_name: str):
        key = f"_scan_{scanner_name}"
        if key in self._threads:
            if not self._threads[key].is_alive():
                del self._threads[key]
        if key not in self._threads:
            scanner = CNODCLocalScanner(
                transfer_queue=self._transfer_queue,
                halt=self._halt,
                stop=threading.Event(),
                config=self.config["scanners"][scanner_name]
            )
            scanner.start()
            self._threads[key] = scanner

    def spawn_sender(self):
        if "_sender" in self._threads:
            if not self._threads["_sender"].is_alive():
                del self._threads["_sender"]
        if "_sender" not in self._threads:
            sender = CNODCSubmissionManager(
                transfer_queue=self._transfer_queue,
                halt=self._halt,
                stop=threading.Event(),
                config=self.config["sender"]
            )
            sender.start()
            self._threads["_sender"] = sender
