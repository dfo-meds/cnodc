import sys
import pathlib
import zirconium

sys.path.append(str(pathlib.Path(__file__).absolute().parent.parent / "src"))


@zirconium.configure
def add_test_config(app_config: zirconium.ApplicationConfig):
    app_config.register_file("./.cnodc.web.toml")
