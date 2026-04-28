
def build_processor(with_init: bool = True):
    if with_init:
        from pipeman.boot import init_cnodc
        init_cnodc("cli")
    from autoinject import injector
    import zirconium as zr
    @injector.inject
    def _build(app_config: zr.ApplicationConfig = None):
        from pipeman_service.multiprocess import MultiProcessController
        pc = MultiProcessController(
            config_file=app_config.as_path(("pipeman", "service", "config_file"), default=None),
            config_file_dir=app_config.as_path(("pipeman", "service", "config_directory"), default=None),
            server_name=app_config.as_str(("pipeman", "service", "server_name"), default=None),
            socket_port=app_config.as_int(("pipeman", "service", "port"), default=9173),
        )
        return pc
    return _build()


def build_single_processor(process_file: str, process_name: str, with_init: bool = True):
    if with_init:
        from pipeman.boot import init_cnodc
        init_cnodc("cli")

    from autoinject import injector
    import zirconium as zr
    @injector.inject
    def _build(app_config: zr.ApplicationConfig = None):
        from pipeman_service.single import SingleProcessController
        pc = SingleProcessController(
            config_file=process_file,
            server_name=process_name,
            process_name=process_name,
            socket_port=app_config.as_int(("pipeman", "service", "port"), default=9173),
        )
        return pc
    return _build()



