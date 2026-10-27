import typing as t

from medsutil.awaretime import AwareDateTime
import nodb.base as s
import nodb.interface as interface



class WISIncomingMessage(s.NODBBaseObject):

    message_uuid: str = s.UUIDColumn()
    wis_version: str = s.StringColumn()
    message_id: str = s.StringColumn()
    path: str = s.StringColumn()
    hash: str = s.StringColumn()
    received_date: AwareDateTime = s.DateTimeColumn()
    obs_date: AwareDateTime = s.DateTimeColumn()

    @classmethod
    def find_by_uuid(cls, db: interface.NODBInstance, message_uuid: str, **kwargs) -> WISIncomingMessage | None:
        return db.load_object(cls, filters={
            "message_uuid": message_uuid,
        }, **kwargs)

    @classmethod
    def find_by_hash(cls, db: interface.NODBInstance, message_hash: str, **kwargs) -> t.Iterable[WISIncomingMessage]:
        yield from db.stream_objects(cls, filters={
            "hash": message_hash,
        }, **kwargs)
