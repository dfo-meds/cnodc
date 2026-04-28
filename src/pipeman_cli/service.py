import click
import zirconium as zr


@click.group()
def service(): ...

@service.command()
@click.argument('process-file')
@click.argument('process-name')
def run_one(process_file: str, process_name: str):
    from pipeman_service.boot import build_single_processor
    spc = build_single_processor(process_file, process_name, with_init=False)
    spc.start()


@service.command()
def run():
    from pipeman_service.boot import build_processor
    pc = build_processor(with_init=False)
    pc.start()


@service.command()
def reload(config: zr.ApplicationConfig = None):
    from medsutil.servicecmd import send_command
    socket_port: int = config.as_int(("pipeman", "service", "port"), default=9173),
    send_command(socket_port, b'reload')


@service.command()
def shutdown(config: zr.ApplicationConfig = None):
    from medsutil.servicecmd import send_command
    socket_port: int = config.as_int(("pipeman", "service", "port"), default=9173),
    send_command(socket_port, b'shutdown')


@service.command()
@click.option('--no-prompt', default=False)
def interrupt(no_prompt: bool, config: zr.ApplicationConfig = None):
    from medsutil.servicecmd import send_command
    if not no_prompt:
        check = input("Are you sure you want to interrupt the current processes? [y/n]: ").lower()
        if check != 'y':
            return
    socket_port: int = config.as_int(("pipeman", "service", "port"), default=9173),
    send_command(socket_port, b'interrupt')


@service.command()
@click.option('--no-prompt', default=False)
def kill(no_prompt: bool, config: zr.ApplicationConfig = None):
    from medsutil.servicecmd import send_command
    if not no_prompt:
        check = input("Are you sure you want to kill the current processes? They may not be shut down properly! [y/n]: ").lower()
        if check != 'y':
            return
    socket_port: int = config.as_int(("pipeman", "service", "port"), default=9173),
    send_command(socket_port, b'kill')
