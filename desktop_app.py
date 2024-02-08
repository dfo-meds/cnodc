import pathlib
import sys
sys.path.append(str(pathlib.Path(__file__).parent / "src"))

if __name__ == "__main__":
    from cnodc.boot.boot import init_cnodc
    init_cnodc('desktop')
    from cnodc.desktop.main_app import CNODCQCApp
    app = CNODCQCApp()
    app.launch()
