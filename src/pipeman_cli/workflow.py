import pathlib
import click
import yaml
import zrlog
from autoinject import injector
from nodb.interface import NODB, NODBInstance, LockType
import nodb as structures


@click.group()
def main(): ...


@main.command
@click.argument("workflow_name")
@click.argument("config_file")
@injector.inject
def update_workflow(workflow_name, config_file, nodb: NODB):
    with nodb as db:
        _update_from_config_dir_file(workflow_name, pathlib.Path(config_file), db)


@main.command
@click.argument("config_directory")
@injector.inject
def update_all_workflows(config_directory: str, nodb: NODB):
    p = pathlib.Path(config_directory)
    if not p.exists():
        print("config directory doesn't exist")
        exit(1)
    if not p.is_dir():
        print("config directory isn't a directory")
        exit(2)
    with nodb as db:
        for f in p.iterdir():
            if f.name.endswith(".yaml"):
                _update_from_config_dir_file(f.name[:-5], f, db)
            elif f.name.endswith(".yml"):
                _update_from_config_dir_file(f.name[:-4], f, db)


def _update_from_config_dir_file(workflow_name: str, config_file: pathlib.Path, db: NODBInstance):
    if not config_file.exists():
        print("config file doesn't exist")
        exit(1)
    try:
        config = {}
        with open(config_file, "r") as h:
            config = yaml.safe_load(h) or {}
        existing = structures.NODBUploadWorkflow.find_by_name(db, workflow_name, lock_type=LockType.FOR_NO_KEY_UPDATE)
        if existing:
            existing.set_config(config)
            db.update_object(existing)
            db.commit()
        else:
            existing = structures.NODBUploadWorkflow()
            existing.workflow_name = workflow_name
            existing.is_active = True
            existing.set_config(config)
            db.insert_object(existing)
            db.commit()
    except Exception:
        zrlog.get_logger('workflow_import').exception(f"An exception occurred while processing {config_file}")
