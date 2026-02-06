import datetime
import os
import pathlib

import click
import secrets

import yaml
import pathlib
import zrlog
from autoinject import injector
from cnodc.nodb import NODBController, NODBControllerInstance, LockType
from cnodc.process.single import SingleProcessController
import json
import traceback


@click.group
def main():
    pass
    # TODO: missing the sys logging information?

@main.group
def gliders():
    pass


@gliders.command
@click.argument("original_file")
@click.argument("output_file")
def convert_file(original_file, output_file):
    from cnodc.programs.glider.ego_convert import OpenGliderConverter
    OpenGliderConverter.build().convert(original_file, output_file)


@gliders.command
@click.argument("original_dir")
@click.argument("output_dir")
def convert_all_files(original_dir, output_dir):
    from cnodc.programs.glider.ego_convert import OpenGliderConverter
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
def output_netcdf_metadata(original_file, output_file):
    from cnodc.programs.glider.ego_convert import OpenGliderConverter
    with open(output_file, "w", encoding="utf-8") as h:
        json.dump(OpenGliderConverter.build().build_metadata(original_file).build_request_body(), h, indent=2)


@main.command
@click.argument("username")
def create_user(username):
    from cnodc.api.auth import UserController
    uc = UserController()
    password = secrets.token_urlsafe(16)
    uc.create_user(username, password)
    print(f"User created, password: {password}")


@main.command
@click.argument("username")
@click.argument("role_name")
def assign_role(username, role_name):
    from cnodc.api.auth import UserController
    uc = UserController()
    uc.assign_role(username, role_name)
    print(f"User {username} assigned to {role_name}")


@main.command
@click.argument("username")
@click.argument("role_name")
def unassign_role(username, role_name):
    from cnodc.api.auth import UserController
    uc = UserController()
    uc.unassign_role(username, role_name)
    print(f"User {username} unassigned from {role_name}")


@main.command
@click.argument("role_name")
@click.argument("permission")
def grant_permission(role_name, permission):
    from cnodc.api.auth import UserController
    uc = UserController()
    uc.grant_permission(role_name, permission)
    print(f"Role {role_name} granted {permission}")


@main.command
@click.argument("role_name")
@click.argument("permission")
def remove_permission(role_name, permission):
    from cnodc.api.auth import UserController
    uc = UserController()
    uc.remove_permission(role_name, permission)
    print(f"Role {role_name} ungranted {permission}")


@main.command
@click.argument("source_file")
@click.argument("destination_file")
@click.option("--source-encoding")
@click.option("--destination-encoding")
@click.option("--source-options")
@click.option("--destination-options")
def transcode(source_file, destination_file, source_encoding=None, destination_encoding=None, source_options=None, destination_options=None):
    from cnodc.codecs.transcoder import transcode
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

@main.command
@click.argument("workflow_name")
@click.argument("config_file")
@injector.inject
def update_workflow(workflow_name, config_file, nodb: NODBController = None):
    with nodb as db:
        _update_from_config_dir_file(workflow_name, pathlib.Path(config_file), db)


@main.command
@click.argument("config_directory")
@injector.inject
def update_all_workflows(config_directory: str, nodb: NODBController = None):
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


def _update_from_config_dir_file(workflow_name: str, config_file: pathlib.Path, db: NODBControllerInstance):
    import cnodc.nodb.structures as structures
    if not config_file.exists():
        print("config file doesn't exist")
        exit(1)
    try:
        config = {}
        with open(config_file, "r") as h:
            config = yaml.safe_load(h) or {}
        existing = structures.NODBUploadWorkflow.find_by_name(db, workflow_name, lock_type=LockType.FOR_NO_KEY_UPDATE)
        if existing:
            existing.update_config(config)
            db.update_object(existing)
            db.commit()
        else:
            existing = structures.NODBUploadWorkflow()
            existing.workflow_name = workflow_name
            existing.is_active = True
            existing.update_config(config)
            existing.check_config()
            db.insert_object(existing)
            db.commit()
    except Exception as ex:
        zrlog.get_logger('').exception(f"An exception occurred while processing {config_file}")


@main.command
@click.argument('process-file')
@click.argument('process-name')
def run(process_file: str, process_name: str):
    spc = SingleProcessController(
        process_name=process_name,
        config_file=process_file
    )
    spc.start()
