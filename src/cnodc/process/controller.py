import uuid
import multiprocessing as mp
import time
import pathlib
import signal
import yaml
import zrlog

from cnodc.util import dynamic_object


class ProcessController:

    def __init__(self, config_file: pathlib.Path, flag_file: pathlib.Path):
        self._config_file = config_file
        self._flag_file = flag_file
        self._loaded = False
        self._process_info = {}
        self._halt_flag = mp.Event()
        self._break_count = 0
        self._log = zrlog.get_logger("cnodc.processctrl")

    def register_process(self, process_name: str, process_cls_name: str, count: int = 1, config: dict = None):
        if process_name not in self._process_info:
            self._log.debug(f"Registering process {process_name}")
            self._process_info[process_name] = {
                'active': {},
                'process_cls': dynamic_object(process_cls_name),
                'count': count,
                'is_active': True,
                'config': config or {}
            }
        else:
            self._log.debug(f"Updating process {process_name}")
            self._process_info[process_name]["count"] = count
            self._process_info[process_name]['config'] = config or {}
            self._process_info[process_name]["is_active"] = True

    def deregister_process(self, process_name: str):
        if process_name in self._process_info:
            self._log.debug(f"Deactivating process {process_name}")
            self._process_info[process_name]['is_active'] = False

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
        try:
            while not self._halt_flag.is_set():
                if self.check_reload():
                    self.reload_config()
                self.spawn()
                time.sleep(1)
        finally:
            self.terminate_all()
            self.wait_for_all()

    def check_reload(self):
        if not self._loaded:
            return True
        if self._flag_file.exists():
            self._log.debug(f"Flag file detected, reloading config")
            self._flag_file.unlink(True)
            return True
        return False

    def reload_config(self):
        self._log.notice(f"Reloading configuration from disk")
        with open(self._config_file, "r") as h:
            config = yaml.safe_load(h) or {}
            if not isinstance(config, dict):
                self._log.error(f"Config file [{self._config_file}] does not contain a yaml dictionary")
                return
            deregister_list = list(self._process_info.keys())
            for process_name in config:
                if 'class_name' not in config[process_name]:
                    self._log.error(f"Process [{process_name}] is missing a class name")
                    continue
                self.register_process(
                    process_name,
                    config[process_name]['class_name'],
                    int(config[process_name]['count']) if 'count' in config[process_name] and config[process_name]['count'] > 0 else 1,
                    config[process_name]['config'] if 'config' in config[process_name] else {}
                )
                if process_name in deregister_list:
                    deregister_list.remove(process_name)
            for process_name in deregister_list:
                self.deregister_process(process_name)
            self._loaded = True

    def terminate_all(self):
        self._log.debug(f"Requesting all processes to halt")
        for process_name in self._process_info:
            for p_name in self._process_info[process_name]['active']:
                self._process_info[process_name]['active'][p_name].shutdown.set()

    def wait_for_all(self):
        some_alive = True
        while some_alive:
            self._log.debug("Waiting for processes to halt...")
            some_alive = False
            for process_name in self._process_info:
                for p_name in list(self._process_info[process_name]['active'].keys()):
                    process = self._process_info[process_name]['active'][p_name]
                    if process.is_alive():
                        process.join(1)
                        if not process.is_alive():
                            self._reap_worker(process_name, p_name)
                        else:
                            some_alive = True
            if some_alive:
                time.sleep(0.5)

    def spawn(self):
        for process_name in self._process_info:
            self._reap_process(process_name)
            target_count = self._process_info[process_name]['count']
            current_count = len(self._process_info[process_name]['active'])
            if self._process_info[process_name]['is_active']:
                if current_count < target_count:
                    self._spawn_process(process_name, target_count - current_count)
                elif current_count > target_count:
                    self._despawn_process(process_name, current_count - target_count)
            elif current_count > 0:
                self._despawn_process(process_name, current_count)

    def _reap_process(self, process_name):
        for p_name in list(self._process_info[process_name]['active'].keys()):
            process = self._process_info[process_name]['active'][p_name]
            if not process.is_alive():
                self._reap_worker(process_name, p_name)

    def _reap_worker(self, process_name, p_name):
        self._log.debug(f"Reaping {process_name}.{p_name}")
        process = self._process_info[process_name]['active'][p_name]
        while not process._print.empty():
            print(process._print.get_nowait())
        del self._process_info[process_name]['active'][p_name]

    def _spawn_process(self, process_name, count: int):
        for _ in range(0, count):
            key = str(uuid.uuid4())
            while key in self._process_info[process_name]['active']:
                key = str(uuid.uuid4())
            self._log.debug(f"Booting process {process_name}.{key}")
            self._process_info[process_name]['active'][key] = self._process_info[process_name]['process_cls'](
                halt_flag=self._halt_flag,
                config=self._process_info[process_name]['config'] if 'config' in self._process_info[process_name] else {}
            )
            self._process_info[process_name]['active'][key].cnodc_id = key
            self._process_info[process_name]['active'][key].start()

    def _despawn_process(self, process_name, count: int):
        processes_to_check = []
        for p_name in list(self._process_info[process_name]['active'].keys()):
            process = self._process_info[process_name]['active'][p_name]
            if process.shutdown.is_set():
                count -= 1
                continue
            processes_to_check.append(process)
        if count <= 0:
            return
        for process in processes_to_check:
            if not process.is_working.is_set():
                self._log.debug(f"Shutting down process {process_name}.{process.cnodc_id}")
                process.shutdown.set()
                count -= 1
                if count <= 0:
                    return
        for process in processes_to_check:
            self._log.debug(f"Shutting down process {process_name}.{process.cnodc_id}")
            process.shutdown.set()
            count -= 1
            if count <= 0:
                return
