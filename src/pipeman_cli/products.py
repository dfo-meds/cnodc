import pathlib

import click
import yaml
import zrlog
from autoinject import injector

from nodb import NODB
from nodb.interface import NODBInstance
from nodb.products import ProductDefinition


@click.group()
def products():
    ...

@products.command
@click.argument("config_directory")
@injector.inject
def update_all_products(config_directory: str, nodb: NODB = None):
    if not config_directory:
        zrlog.get_logger("cli.upgrade.products").warning("config_directory not specified")
        raise SystemExit(1)
    p = pathlib.Path(config_directory)
    if not p.exists():
        zrlog.get_logger("cli.upgrade.products").warning("config_directory not exists")
        raise SystemExit(1)
    if not p.is_dir():
        zrlog.get_logger("cli.upgrade.products").warning("config_directory not a directory")
        raise SystemExit(1)
    with nodb as db:
        for f in p.iterdir():
            if f.name.endswith(".yaml"):
                _update_from_product_file(f.name[:-5], f, db)
            elif f.name.endswith(".yml"):
                _update_from_product_file(f.name[:-4], f, db)


def _update_from_product_file(product_name: str, product_file: pathlib.Path, db: NODBInstance):
    if not (product_file.exists() and product_file.is_file()):
        zrlog.get_logger("cli.upgrade.products").warning("product_file not exists")
    try:
        config = {}
        with open(product_file, "r") as h:
            config = yaml.safe_load(h) or {}
        existing = ProductDefinition.find_by_name(db, product_name)
        if not existing:
            zrlog.get_logger("cli.upgrade.products").info("Creating product %s", product_name)
            pdef = ProductDefinition(product_name=product_name)
            pdef.update_from_config(config)
            db.insert_object(pdef)
            db.commit()
        else:
            zrlog.get_logger("cli.upgrade.products").info("Updating product %s", product_name)
            existing.update_from_config(config)
            db.update_object(existing)
            db.commit()
    except Exception as ex:
        zrlog.get_logger("cli.upgrade.products").exception("Error while working with product %s", product_name)
        raise SystemExit(1) from ex

