import click
from cnodc.decode import DecoderRegistry
from autoinject import injector
import zirconium as zr
import zrlog


@zr.configure
def register(config: zr.ApplicationConfig):
    config.register_file("./cnodc.toml")


@click.group
def main():
    zrlog.init_logging()


@main.command
@injector.inject
def list(registry: DecoderRegistry = None):
    output = {name: codec.description() for name, codec in registry.list_codecs()}
    ml = max(len(n) for n in output.keys()) + 2
    fstr = "{: <" + str(ml) + "}: {}"
    for name in output:
        print(fstr.format(name, output[name]))


@main.command
@click.argument("source_file")
@click.argument("target_file")
@click.option("--iformat", default=None)
@click.option("--oformat", default=None)
@click.option("--iargs", default=None)
@click.option("--oargs", default=None)
@injector.inject
def transcode(source_file, target_file, iformat, oformat, iargs, oargs, registry: DecoderRegistry = None):
    src_codec = registry.load_codec(source_file, iformat)
    trg_codec = registry.load_codec(target_file, oformat)
    trg_codec.dump(src_codec.load(source_file, **_parse_io_arg_str(iargs)), target_file, **_parse_io_arg_str(oargs))


@main.command
def daemon():
    from cnodc.run.core import LaunchManager
    lm = LaunchManager()
    lm.launch()


def _parse_io_arg_str(args):
    if args is None or args == "":
        return {}
    a = {}
    for kv in args.split(" "):
        k, v = kv.split("=")
        a[k] = v
    return a

