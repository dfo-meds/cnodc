import pathlib


def boot_pipeman_desktop(test_mode: bool = True):
    from gcapp.boot import boot_system

    overrides = {
        "gcapp.i18n.base.LanguageDetector": "pipeman_desktop.i18n.DesktopLanguageDetector",
        "gcapp.i18n.base.TranslationManager": "gcapp.i18n.filetm.TomlTranslationManager",
    }
    if test_mode:
        overrides["pipeman_desktop.client.api_client.WebAPIClient"] = "pipeman_desktop.client.test_client.TestClient"
    system = boot_system(
        app_name="pipemandesktop",
        manual_overrides=overrides,
        default_config={
            "gcapp.toml_translations.paths": [
                str(pathlib.Path(__file__).absolute().parent / "translations"),
            ]
        }
    )

    from pipeman_desktop.main_app import PipemanDesktop
    app = PipemanDesktop(system)
    app.launch()
