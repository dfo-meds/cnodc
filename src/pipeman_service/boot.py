"""Contains core code for building and running the PIPEMAN controllers.

* :func:`~pipeman_service.boot.build_processor` runs a multiprocessing Process for each worker and is recommended.
* :func:`~pipeman_service.boot.build_single_processor()` runs a single process that only runs a single worker. It is only recommended for testing.
"""
from autoinject import injector, auto
import zirconium as zr

def build_processor(with_init: bool = True):
    """ Launches a multiprocessing controller that runs PIPEMAN workers as Process objects.

        :param with_init: Set to FALSE to skip the initialization process, see :func:`~pipeman.boot.init_pipeman`.
        :type with_init: bool

        :return: The controller class that was built
        :rtype: :func:`~pipeman_service.multiprocess.MultiProcessController`
    """

    # this loads the configuration settings and such from standard locations
    # you usually want to do this unless you're running the processor from
    # code where the configuration has already been initialized
    if with_init:
        from pipeman.boot import init_pipeman
        init_pipeman("cli")

    import multiprocessing as mp
    # this standardizes the method across platforms to a very compatible one
    mp.set_start_method("spawn")

    # build the MPC object from configuration
    @injector.inject
    def _build(app_config: zr.ApplicationConfig = auto()):
        # note that we don't do imports before now for good reason - don't change this pattern
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
    """ Launches a single-process controller with no threading that runs a single process.

        :param process_file: The file containing configuration information for a single process.
        :type process_file: str
        :param process_name: The name of the process to run.
        :type process_name: str
        :param with_init: Set to FALSE to skip the initialization process, see :func:`~pipeman.boot.init_pipeman`.
        :type with_init: bool

        :return: The controller class that was build
        :rtype: :func:`~pipeman_service.single.SingleProcessController`
    """

    # See build_processor() for details on how this works
    # the only difference is the controller class we build.

    if with_init:
        from pipeman.boot import init_pipeman
        init_pipeman("cli")

    @injector.inject
    def _build(app_config: zr.ApplicationConfig = auto()):
        from pipeman_service.single import SingleProcessController
        pc = SingleProcessController(
            config_file=process_file,
            server_name=process_name,
            process_name=process_name,
            socket_port=app_config.as_int(("pipeman", "service", "port"), default=9173),
        )
        return pc

    return _build()



