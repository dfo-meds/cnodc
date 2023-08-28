import click
from cnodc.decode import DecoderRegistry
from autoinject import injector


@click.group
def main():
    pass


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


def _parse_io_arg_str(args):
    if args is None or args == "":
        return {}
    a = {}
    for kv in args.split(" "):
        k, v = kv.split("=")
        a[k] = v
    return a

