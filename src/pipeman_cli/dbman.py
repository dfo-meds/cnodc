import datetime

import click
from autoinject import injector

from medsutil.awaretime import AwareDateTime
from nodb.interface import NODB, QueueStatus


@click.group("db", help="Database management tools, use with care!")
def db(): ...

@db.command(help="Output queue items that are flagged as errors")
@injector.inject
def queue_errors(nodb: NODB = None):
    with nodb as db:
        with db.cursor() as cur:
            cur.execute("SELECT queue_name, queue_uuid, db_modified_date FROM nodb_queues WHERE status = 'ERROR' ORDER BY queue_name ASC, db_modified_date DESC")
            print("UUID,QUEUE,ERRORED_SINCE")
            for row in cur.fetch_stream(50):
                print(f"{row[1]},{row[0]},{row[2]}")


@db.command(help="Output locked queue")
@click.option("--locked-since", default=None, type=str, help="Enter a time in ISO format - only items that have been locked since before this date will be shown. If the timezone is omitted, the current system time is used. If omitted, defaults to using --locked-for to calculate a time.")
@click.option("--locked-for", default=24*3600, type=int, help="Enter a time duration in seconds - only items that have been locked this long will be shown. Overridden by --locked-since. Defaults to 24 hours")
@injector.inject
def queue_locked(nodb: NODB = None, locked_since: str | None = None, locked_for: int = 24 * 3600):
    if locked_since is None:
        ls = AwareDateTime.now() - datetime.timedelta(seconds=locked_for)
    else:
        ls = AwareDateTime.fromisoformat(locked_since)
    with nodb as db:
        with db.cursor() as cur:
            cur.execute("SELECT queue_name, queue_uuid, locked_since, locked_by FROM nodb_queues WHERE status = 'LOCKED' AND locked_since <= %s ORDER BY queue_name ASC, locked_since DESC", [
                ls
            ])
            print("UUID,QUEUE,LOCKED_SINCE,LOCKED_BY")
            for row in cur.fetch_stream(50):
                print(f"{row[1]},{row[0]},{row[2]},{row[3]}")


@db.command(help="Unlock one or more queue items specified by QUEUE_UUID (separate multiple items with semi-colons)")
@click.argument('queue_uuid')
@injector.inject
def unlock(queue_uuid: str, nodb: NODB = None):
    if ";" in queue_uuid:
        uuids = queue_uuid.split(";")
    else:
        uuids = [queue_uuid]
    with nodb as db:
        for uuid in uuids:
            db.fast_update_queue_status(uuid, QueueStatus.UNLOCKED)
        db.commit()
