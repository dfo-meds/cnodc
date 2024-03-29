import pathlib
import sys
import zirconium as zr
from autoinject import injector

sys.path.append(str(pathlib.Path(__file__).parent / "src"))

from cnodc.boot.boot import init_cnodc
init_cnodc("process")


@injector.inject
def run_processor(app_config: zr.ApplicationConfig = None):
    from cnodc.process.multiprocess import ProcessController
    pc = ProcessController(
        config_file=app_config.as_path(("cnodc", "process_definition_file")),
        flag_file=app_config.as_path(("cnodc", "flag_file"))
    )
    pc.start()


if __name__ == "__main__":
    run_processor()
