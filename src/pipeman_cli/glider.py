import os
import pathlib
import traceback
import medsutil.json as json

import click

@click.group()
def main():
    pass


@main.command
@click.argument("original_file")
@click.argument("output_file")
def convert(original_file, output_file):
    from pipeman.programs.glider.ego_convert import OpenGliderConverter
    OpenGliderConverter.build().convert(original_file, output_file)


@main.command
@click.argument("original_dir")
@click.argument("output_dir")
def convert_all(original_dir, output_dir):
    from pipeman.programs.glider.ego_convert import OpenGliderConverter
    out_dir = pathlib.Path(output_dir)
    for file in os.scandir(original_dir):
        if not file.name.endswith(".nc"):
            continue
        out_path = out_dir / file.name
        if out_path.exists():
            continue
        try:
            OpenGliderConverter.build().convert(file.path, out_path)
        except KeyboardInterrupt as ex:
            out_path.unlink(True)
            raise ex
        except Exception as ex:
            print(f"Error when converting {file.name}")
            with open(out_dir / f"{file.name}.error.txt", "w") as h:
                h.write(f"{type(ex)}: {str(ex)}\n")
                h.write(traceback.format_exc())
            out_path.unlink(True)


@main.command
@click.argument("original_file")
@click.argument("output_file")
def dump_metadata(original_file, output_file):
    from pipeman.programs.glider.ego_convert import OpenGliderConverter
    with open(output_file, "wb", encoding="utf-8") as h:
        h.write(json.dumpb(OpenGliderConverter.build().build_metadata(original_file).build_request_body()))
