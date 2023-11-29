import pathlib
import sys
import zirconium as zr
from autoinject import injector

sys.path.append(str(pathlib.Path(__file__).parent / "src"))

from cnodc.boot.boot import init_cnodc
init_cnodc("process")


@injector.inject
def run_processor(app_config: zr.ApplicationConfig = None):
    from cnodc.process.controller import ProcessController
    pc = ProcessController(
        app_config.as_path(("cnodc", "process_definition_file")),
        app_config.as_path(("cnodc", "flag_file"))
    )
    pc.loop()


run_processor()
