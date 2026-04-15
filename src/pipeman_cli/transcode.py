import click


@click.command()
@click.argument("source_file")
@click.argument("destination_file")
@click.option("--source-encoding")
@click.option("--destination-encoding")
@click.option("--source-options")
@click.option("--destination-options")
def transcode(source_file, destination_file, source_encoding=None, destination_encoding=None, source_options=None, destination_options=None):
    from medsutil.ocproc2.codecs.transcoder import transcode
    source_kwargs = {}
    destination_kwargs = {}
    if source_options is not None:
        for x in source_options.split(','):
            if '=' in x:
                pieces = x.split('=', maxsplit=1)
                source_kwargs[pieces[0]] = pieces[1]
            else:
                source_kwargs[x] = True
    if destination_options is not None:
        for x in destination_options.split(','):
            if '=' in x:
                pieces = x.split('=', maxsplit=1)
                destination_kwargs[pieces[0]] = pieces[1]
            else:
                destination_kwargs[x] = True
    transcode(source_file, destination_file, source_encoding, destination_encoding, source_kwargs, destination_kwargs)
