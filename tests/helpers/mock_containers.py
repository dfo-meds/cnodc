import enum
import pathlib
import shutil
import subprocess
import time

import zrlog
from nodb.interface import NODBError
from nodb.controller import NODBPostgresController

TEST_DIR = pathlib.Path(__file__).absolute().resolve().parent.parent
CONTAINER_DIR = TEST_DIR / 'containers'
DB_DIR = TEST_DIR.parent.parent
DOCKER_COMMAND = shutil.which('docker')


class DockerOutput(enum.Enum):
    SILENT = 0
    STDOUT = 1
    CAPTURE = 2


class TestContainer:

    def __init__(self,
                 name,
                 always_rebuild: bool = False,
                 wait_for=None,
                 wait_max=30,
                 wait_sleep=0.5,
                 docker_timeout=30,
                 compose_file = "compose.yaml",
                 no_detach: bool = False):
        self.name = name
        self._no_detach = no_detach
        self.docker_file = CONTAINER_DIR / name / compose_file
        self._rebuild = always_rebuild
        self._timeout = docker_timeout
        self._running = False
        self._wait_for_cb = wait_for
        self._max_delay = wait_max
        self._wait_sleep = wait_sleep
        self._log = zrlog.get_logger(f'cnodc.test_container.{name}')

    def __enter__(self):
        self.up()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.down()

    def _wait_for(self):
        if self._wait_for_cb is not None:
            t = time.monotonic()
            now = t
            while (now - t) < self._max_delay:
                check = self._wait_for_cb()
                if check:
                    self._log.info(f'Container check passed for [{self.name}] after [{time.monotonic() - t:.3f}] seconds')
                    break
                else:
                    time.sleep(self._wait_sleep)
                    now = time.monotonic()
            else:
                self._log.warning(f'Container check did not pass after {self._max_delay} seconds, tests may fail [docker logs are being dumped for review]')
                self._docker_command(['logs'], False)
                return False
        return True

    def up(self):
        up_cmd = ['up', '--quiet-build', '--quiet-pull']
        if not self._no_detach:
            up_cmd.append('--detach')
        else:
            up_cmd.append('--exit-code-from')
            up_cmd.append("up")
            up_cmd.append('--yes')
        if self._rebuild:
            up_cmd.append('--build')
        self._log.debug(f'Booting test container [{self.name}]')
        res = self._docker_command(up_cmd)
        if res.returncode != 0:
            raise RuntimeError(f'Error starting container [{self.name}]')
        boot_res = self._wait_for()
        self._log.debug(f'Test container [{self.name}] is running [boot_check={boot_res}]')
        self._running = True
        self.after_boot()
        return boot_res

    def down(self):
        if self._running:
            self.before_shutdown()
            self._log.debug(f'Shutting down test container [{self.name}]')
            res = self._docker_command(['down', '-v'])
            if res.returncode != 0:
                raise RuntimeError(f'Error shutting down container [{self.name}]')
            self._log.debug(f'Test container [{self.name}] is shut down')
            self._running = False

    def _docker_command(self, cmd: list[str], output: DockerOutput = DockerOutput.STDOUT):
        if DOCKER_COMMAND is None:
            raise RuntimeError('Error finding docker command')
        kwargs = {
            'timeout': self._timeout,
        }
        if output is DockerOutput.SILENT:
            kwargs['stdout'] = subprocess.DEVNULL
            kwargs['stderr'] = subprocess.DEVNULL
        elif output is DockerOutput.CAPTURE:
            kwargs['text'] = True
            kwargs['capture_output'] = True
        args: list[str] = [
            DOCKER_COMMAND,
            'compose',
            '-f',
            str(self.docker_file)
        ] + cmd
        self._log.trace(f'Executing command [{' '.join(args)}]')
        return subprocess.run(args, **kwargs)

    def after_boot(self):
        pass

    def before_shutdown(self):
        pass


class NODBContainer(TestContainer):

    ALL_TABLES = (
        "nodb_queues",
        "nodb_users",
        "nodb_permissions",
        "nodb_logins",
        "nodb_sessions",
        "nodb_upload_workflows",
        "nodb_scanned_files",
        "nodb_qc_batches",
        "nodb_obs_data",
        "nodb_working",
        "nodb_obs",
        "nodb_source_files",
        "nodb_missions",
        "nodb_platforms",

    )

    def __init__(self):
        super().__init__('nodb', True, self._check_nodb_available)
        self.nodb = NODBPostgresController(
            dbname="nodb",
            user = "example",
            password = "example",
            host = "localhost",
            port = 5432,
        )

    def _check_nodb_available(self):
        try:
            with self.nodb as db:

                # Bring us up to the latest version
                from nodb._upgrade import Upgrader
                Upgrader(db).upgrade()

                # Truncate everything to ensure we start fresh
                with db.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {','.join(self.ALL_TABLES)} CASCADE")

            return True
        except NODBError as ex:
            if not ex.is_db_available:
                return False
            else:
                raise


class DMDInstallContainer(TestContainer):

    def __init__(self):
        super().__init__('dmd', always_rebuild=True, compose_file="compose.upgrade.yaml", no_detach=True, docker_timeout=300)


class DMDContainer(TestContainer):


    def __init__(self):
        super().__init__('dmd', always_rebuild=True, wait_for=self._check_dmd_available)

    def base_url(self):
        return 'http://localhost:9100/metadb/'

    def _check_dmd_available(self):
        import requests
        try:
            resp = requests.get(self.base_url())
            resp.raise_for_status()
        except Exception as ex:
            return False

    def create_bot_user(self, username: str, display: str | None = None, email: str | None = None) -> str:
        self._docker_command(["exec", "web", "python", "cli.py", "user", "create", "--no-email", "1", "--password-entry", "random", username, email or f"{username}@example.com", "--display", display or username])
        self._docker_command(["exec", "web", "python", "cli.py", "user", "enable-api-access", username])
        res = self._docker_command(["exec", "web", "python", "cli.py", "user", "create-api-key", username, "api_access", "365"], output=DockerOutput.CAPTURE)
        lines = res.stdout.split("\n")
        result = ""
        for line in lines:
            if line.startswith("Authorization: Bearer "):
                result = line[22:]
        self._docker_command(["exec", "web", "python", "cli.py", "group", "assign", username, "api_access_direct"])
        return result
