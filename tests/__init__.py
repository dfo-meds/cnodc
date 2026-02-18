import sys
import pathlib
import zirconium

TEST_DIR = pathlib.Path(__file__).absolute().parent

sys.path.append(str(TEST_DIR.parent / "src"))

@zirconium.configure
def add_test_config(app_config: zirconium.ApplicationConfig):
    app_config.register_file("./.cnodc.toml")
