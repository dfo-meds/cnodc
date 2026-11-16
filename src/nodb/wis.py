import datetime
import typing as t

import nodb.base as s
from nodb import interface


class WISIncomingMessage(s.NODBBaseObject):
    message_uuid: str = s.UUIDColumn()
    wis_version: str = s.StringColumn()
    message_id: str = s.StringColumn()
    path: str = s.StringColumn()
    hash: str = s.StringColumn()
    received_date: datetime.date = s.DateColumn()
    bundled: int = s.IntColumn(default=0)
    source_file_uuid: str = s.UUIDColumn()

    @classmethod
    def find_by_uuid(
        cls, db: interface.NODBInstance, message_uuid: str, **kwargs
    ) -> WISIncomingMessage | None:
        return db.load_object(
            cls,
            filters={
                "message_uuid": message_uuid,
            },
            **kwargs,
        )

    @classmethod
    def find_by_hash(
        cls, db: interface.NODBInstance, message_hash: str, **kwargs
    ) -> t.Iterable[WISIncomingMessage]:
        yield from db.stream_objects(
            cls,
            filters={
                "hash": message_hash,
            },
            **kwargs,
        )

    @classmethod
    def find_unbundled(cls, db: interface.NODBInstance, **kwargs) -> t.Iterable[WISIncomingMessage]:
        yield from db.stream_objects(
            cls,
            filters={
                "bundled": 0
            },
            **kwargs,
        )
