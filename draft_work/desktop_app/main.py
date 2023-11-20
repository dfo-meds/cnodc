import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).absolute().parent.parent / 'src'))


from nodb_desktop.business import DesktopAppController


if __name__ == "__main__":
    # TODO: service endpoint configuration?
    controller = DesktopAppController("http://localhost:5000/test000/")
    controller.launch()
