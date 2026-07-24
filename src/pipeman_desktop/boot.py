

def boot_pipeman_desktop(test_mode: bool = True):
    from gcapp.boot import boot_system

    overrides = {}
    if test_mode:
        overrides["pipeman_desktop.api_client.WebAPIClient"] = "pipeman_desktop.client.test_client.TestClient"

    system = boot_system(
        app_name="pipemandesktop",
        manual_overrides=overrides,
    )

    from main_app import PipemanDesktop
    app = PipemanDesktop(system)
    app.launch()
