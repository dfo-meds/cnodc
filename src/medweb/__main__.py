
if __name__ == "__main__":
    from medweb.boot import boot_medweb
    system = boot_medweb("cli")

    from gcclick.clicksystem import ClickApp
    click_app = ClickApp()
    system.init_click_app(click_app)
    click_app()