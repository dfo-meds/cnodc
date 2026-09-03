import pathlib

import click

@click.group
def fastgrid(): ...


@fastgrid.command()
@click.argument("output_directory")
@click.argument("temp_prefix")
@click.argument("salinity_prefix")
def woa(output_directory, temp_prefix, salinity_prefix):
    from pipeman.programs.woa.grid import WorldOceanAtlasOneDegree
    output_dir = pathlib.Path(output_directory)
    if not output_dir.exists():
        raise ValueError("Invalid output directory")

    for var_name, var_prefix in (("temp", temp_prefix), ("salinity", salinity_prefix)):
        for time_period, file_part in (("annual", "00"), ("jan", "01"), ("feb", "02"), ("mar", "03"), ("apr", "04"),
                                       ("may", "05"), ("jun", "06"), ("jul", "07"), ("aug", "08"), ("sep", "09"),
                                       ("oct", "10"), ("nov", "11"), ("dec", "12"), ("winter", "13"), ("spring", "14"),
                                       ("summer", "15"), ("fall", "16")):
            output_file = output_dir / f"{var_name}_{time_period}.fastgrid"
            mean_file = pathlib.Path(var_prefix.replace("\\", "/") + f"{file_part}mn01.csv.gz")
            error_file = pathlib.Path(var_prefix.replace("\\", "/") + f"{file_part}sd01.csv.gz")
            print(f"Building {output_file}")
            WorldOceanAtlasOneDegree.build_atlas_file(output_file, mean_file, error_file)

@fastgrid.command()
@click.argument("output_file")
@click.argument("input_files", nargs=-1)
def glb(output_file, input_files):
    from pipeman.programs.bathymetry.glb import GreatLakesBathymetry
    output_file = pathlib.Path(output_file)
    input_files = [pathlib.Path(x) for x in input_files]
    GreatLakesBathymetry.build_from_glb(output_file, input_files)


