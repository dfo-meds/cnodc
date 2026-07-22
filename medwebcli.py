import pathlib
import sys
sys.path.append(str(pathlib.Path(__file__).parent / "src"))


if __name__ == "__main__":
    from medweb.boot import boot_medweb
    system = boot_medweb("cli")

    from gcclick.clicksystem import ClickApp
    app = ClickApp()

    system.init_click_app(app)
    app()
