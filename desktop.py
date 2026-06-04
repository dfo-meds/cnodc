import sys, pathlib

sys.path.append(str(pathlib.Path(__file__).parent.absolute() / 'src'))


if __name__ == '__main__':
    from pipeman_desktop.main_app import CNODCQCApp
    app = CNODCQCApp()
    app.launch()
