import sys

from pipeman_web.boot import build_cnodc_webapp
app = build_cnodc_webapp(
    __name__,
    sys.argv[0].replace("\\", "/").endswith("flask/__main__.py")
)
