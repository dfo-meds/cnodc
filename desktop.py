import sys, pathlib

sys.path.append(str(pathlib.Path(__file__).parent.absolute() / 'src'))


if __name__ == '__main__':
    from pipeman_desktop.boot import boot_pipeman_desktop
    boot_pipeman_desktop()
