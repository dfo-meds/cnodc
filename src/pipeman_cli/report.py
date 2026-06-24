import os
import pathlib
import traceback

import click

from medsutil.units.structures import UnitError


@click.group()
def main():
    pass


@main.command
@click.option("--output-file", type=str, default=None)
def units(output_file=None):
    if output_file is not None:
        with open(output_file, "w", encoding="utf-8") as f:
            _write_units(lambda x: f.write(x + "\n"))
    else:
        _write_units(print)


BASE_SI_MAP = {
    "": "Count / Fraction",
    "A": "Electrical Current",
    "A s": "Electrical Charge",
    "A kg-1 s": "Ionizing Radiation (Air)",
    "A2 kg-1 m-2 s3": "Electrical Conductance",
    "A2 kg-1 m-2 s4": "Electrical Capacitance",
    "K": "Temperature",
    "cd": "Luminous Intensity",
    "cd m-2": "Luminous Intensity (per area)",
    "cd rad2": "Luminous Flux",
    "cd m-2 rad2": "Luminous Flux (per area)",
    "kg": "Mass",
    "kg m s-2": "Force",
    "kg m-1": "Density - Linear",
    "kg m-1 s-1": "Viscosity - Dynamic",
    "kg m-1 s-2": "Pressure",
    "kg m2 s-2": "Energy",
    "A-1 kg m2 s-2": "Magnetic Flux",
    "A-2 kg m2 s-2": "Electrical Inductance",
    "kg m2 s-3": "Power",
    "A-1 kg m2 s-3": "Electrical Potential",
    "A-2 kg m2 s-3": "Electrical Resistance",
    "kg s-2": "Heat Transmission",
    "A-1 kg s-2": "Magnetic Flux Density",
    "kg-1 m s": "Fluidity",
    "m": "Length",
    "m s-1": "Speed",
    "m s-2": "Acceleration",
    "m2": "Area",
    "m3": "Volume",
    "rad": "Angle",
    "mol": "Amount",
    "s": "Time",
    "s-1": "Frequency",
}

def _write_units(writer):
    from medsutil.units import UnitConverter
    converter = UnitConverter()
    converter._load_tables()
    unique_names = set(converter._loaded_tables['preferred_units'].values())
    max_len = max(len(x) for x in unique_names)
    to_sort = []
    for x in unique_names:
        try:
            base_units = converter.get_equivalent_base_units(x)
            to_sort.append((x, BASE_SI_MAP[base_units] if base_units in BASE_SI_MAP else base_units))
        except UnitError as ex:
            to_sort.append((x, str(ex)))
    to_sort.sort(key=lambda x: (x[1], x[0]))
    last = ''
    for name, category in to_sort:
        if category != last:
            writer(f"\n### {category} ###")
            last = category
        values = [
            x for x in converter._loaded_tables['preferred_units']
            if converter._loaded_tables['preferred_units'][x] == name and x != name
        ]
        writer(f"{name.ljust(max_len+5, " ")}{'  '.join(str(x) for x in values)}")


