import datetime
import enum
import io

from autoinject import injector
from flask import make_response, send_file

from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError
from nodb.interface import NODB, LOCK_EXPIRY_TIME, NODBInstance
from nodb.observations import NODBWorkingRecord
from nodb.queue import NODBQueueItem
from pipeman.processing.payloads import Payload


class NODBAPIError(CodedError): CODE_SPACE = "NODB-API"


class ReviewResult(enum.Enum):
    RECHECK = 'recheck'
    CONTINUE = 'continue'
    ERROR = 'error'
    ESCALATE = 'escalate'
    NO_RESULT = 'no_result'


class NODBController:

    nodb: NODB = None

    @injector.construct
    def __init__(self):
        ...

    def fetch_next_queue_item(self,
                              queue_name: str,
                              escalation_level: int,
                              app_id: str,
                              subqueue_name: str | None = None):
        with self.nodb as db:
            item = db.fetch_next_queue_item(queue_name, app_id, subqueue_name, escalation_level)
            if item is not None:
                db.commit()
                return {
                    "escalation_level": item.escalation_level,
                    "queue_name": item.queue_name,
                    "subqueue_name": item.subqueue_name,
                    "queue_uuid": item.queue_uuid,
                    "data": item.data,
                    "locked_until": (item.locked_since + datetime.timedelta(seconds=LOCK_EXPIRY_TIME)).isoformat() if item.locked_since is not None else None,
                }
            else:
                return {
                    "queue_uuid": None,
                }

    def renew_queue_item(self, queue_uuid: str):
        with self.nodb as db:
            new_expiry = db.fast_renew_queue_item(queue_uuid)
            if new_expiry is not None:
                db.commit()
                return {
                    "success": True,
                    "queue_uuid": queue_uuid,
                    "locked_until": (AwareDateTime.utcnow() + datetime.timedelta(seconds=LOCK_EXPIRY_TIME)).isoformat()
                }
            else:
                return {
                    "success": False,
                    "message": "Unable to renew queue item",
                }

    def _find_queue_item(self, db: NODBInstance, queue_uuid: str, app_id: str) -> NODBQueueItem:
        item: NODBQueueItem | None = NODBQueueItem.find_by_uuid(db, queue_uuid)
        if item is None:
            raise NODBAPIError("No such queue item")
        if item.locked_by != app_id:
            raise NODBAPIError("Queue item is locked by another user")
        return item

    def close_qc_item(self,
                      queue_uuid: str,
                      app_id: str,
                      review_result: ReviewResult):
        with self.nodb as db:
            item = self._find_queue_item(db, queue_uuid, app_id)
            payload = Payload.from_queue_item(item)
            if review_result is ReviewResult.RECHECK:
                payload.enqueue(db, payload.metadata.get("recheck_queue", "missing_next_queue"))
                item.mark_complete(db)
            elif review_result is ReviewResult.CONTINUE:
                payload.enqueue(db, payload.metadata.get("next_queue", "missing_next_queue"))
                item.mark_complete(db)
            elif review_result is ReviewResult.ERROR:
                payload.enqueue(db, payload.metadata.get("error_queue", "missing_next_queue"))
                item.mark_failed(db)
            elif review_result is ReviewResult.ESCALATE:
                item.release(db, escalation_level=1)
            else:
                item.release(db)
            return {"success": True}

    def serve_working_record(self, record_uuid: str):
        with self.nodb as db:
            item = NODBWorkingRecord.find_by_uuid(db, record_uuid)
            if item is None:
                raise NODBAPIError("No such working record")
            return send_file(
                io.BytesIO(item.data_record or b''),
                download_name=f"{record_uuid}.ocproc2",
                mimetype="application/vnd.ca.cnodc.ocproc2",
                as_attachment=True,
            )
