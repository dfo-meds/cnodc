import sys, pathlib

sys.path.append(str(pathlib.Path(__file__).parent.absolute() / "src"))

from medsutil.storage import StorageController

from medsutil.halts import HaltFlag, DummyEvent

sc = StorageController()

fp = sc.get_filepath("ftp://ftp.isdm.gc.ca/pub/glider/k_999_20241119_R.nc", halt_flag=HaltFlag(DummyEvent()))

fp.download(pathlib.Path("C:/my/bumble"), allow_overwrite=True)

