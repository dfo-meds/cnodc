import click
from pipeman_service.single import SingleProcessController


@click.command()
@click.argument('process-file')
@click.argument('process-name')
def run_process(process_file: str, process_name: str):
    spc = SingleProcessController(
        process_name=process_name,
        config_file=process_file
    )
    spc.start()
