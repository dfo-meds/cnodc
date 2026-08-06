import datetime
import typing as t

import flask
from autoinject import injector

from gcflask.user import current_user
from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError
from medsutil.ocproc2 import QCTestRunInfo, QCResult, RecordAction
from nodb.interface import NODB, LOCK_EXPIRY_TIME, NODBInstance
from nodb.observations import NODBWorkingRecord, NODBPlatform, PlatformStatus
from nodb.queue import NODBQueueItem
from pipeman.processing.payloads import Payload, BatchPayload, SourceFilePayload, WorkingRecordPayload, \
    stream_payload_working_records, WorkflowPayload


class NODBAPIError(CodedError): CODE_SPACE = "NODB-API"

from pipeman_desktop.util import ReviewResult

class NODBController:

    nodb: NODB = None

    @injector.construct
    def __init__(self):
        ...

    def get_queue_report(self) -> dict:
        with self.nodb as db:
            return {
                "success": True,
                "message": "Success",
                "ready": [x for x in db.fetch_queue_ready_summary()]
            }

    def fetch_next_queue_item(self,
                              queue_name: str,
                              escalation_level: int,
                              app_id: str,
                              subqueue_name: str | None = None) -> dict:
        with self.nodb as db:
            item = db.fetch_next_queue_item(queue_name, app_id, subqueue_name, escalation_level)
            if item is not None:
                db.commit()
                return {
                    "success": True,
                    "message": "Success",
                    "escalation_level": item.escalation_level,
                    "queue_name": item.queue_name,
                    "subqueue_name": item.subqueue_name,
                    "queue_uuid": item.queue_uuid,
                    "actions": {
                        "renew": {
                            "endpoint": flask.url_for("desktop.renew_queue_item", _external=True, queue_uuid=item.queue_uuid),
                        },
                        "close": {
                            "endpoint": flask.url_for("desktop.close_qc_queue_item", _external=True, queue_uuid=item.queue_uuid),
                        },
                        "stream": {
                            "endpoint": flask.url_for("desktop.stream_queue_item_records", _external=True, queue_uuid=item.queue_uuid),
                        }
                    },
                    "locked_until": (item.locked_since + datetime.timedelta(seconds=LOCK_EXPIRY_TIME)).isoformat() if item.locked_since is not None else None,
                }
            else:
                return {
                    "success": False,
                    "message": "No queue items available",
                    "actions": {},
                    "queue_uuid": None,
                    "locked_until": None,
                    "subqueue_name": subqueue_name,
                    "queue_name": queue_name,
                    "escalation_level": escalation_level,
                }

    def renew_queue_item(self, queue_uuid: str) -> dict:
        with self.nodb as db:
            new_expiry = db.fast_renew_queue_item(queue_uuid)
            if new_expiry is not None:
                db.commit()
                return {
                    "success": True,
                    "message": "Success",
                    "queue_uuid": queue_uuid,
                    "locked_until": (AwareDateTime.utcnow() + datetime.timedelta(seconds=LOCK_EXPIRY_TIME)).isoformat()
                }
            else:
                return {
                    "success": False,
                    "message": "Unable to renew queue item",
                    "queue_uuid": None,
                    "locked_until": None,
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
                      review_result: ReviewResult) -> dict:
        with self.nodb as db:
            item = self._find_queue_item(db, queue_uuid, app_id)
            payload = WorkflowPayload.from_queue_item(item)
            if review_result is ReviewResult.RECHECK:
                payload.followup_queue = payload.metadata.get("recheck_queue", "missing_next_queue")
                payload.enqueue(db, "qc_forward")
                item.mark_complete(db)
            elif review_result is ReviewResult.CONTINUE:
                payload.followup_queue = payload.metadata.get("next_queue", "missing_next_queue")
                payload.enqueue(db, "qc_forward")
                item.mark_complete(db)
            elif review_result is ReviewResult.ERROR:
                payload.enqueue(db, payload.metadata.get("error_queue", "missing_next_queue"))
                item.mark_failed(db)
            elif review_result is ReviewResult.ESCALATE:
                item.release(db, escalation_level=(item.escalation_level or 0) + 1)
            elif review_result is ReviewResult.DESCALATE:
                item.release(db, escalation_level=(item.escalation_level or 0) - 1)
            else:
                item.release(db)
            return {
                "success": True,
                "message": "Success"
            }

    SEND_KEYS = {
        'working_uuid',
        'received_date',
        'source_file_uuid',
        'message_idx',
        'record_idx',
        'platform_uuid',
        'data_mode',
        'quality_checks',
    }

    def stream_queue_working_records(self, queue_uuid: str, app_id: str) -> dict:
        with self.nodb as db:
            item = self._find_queue_item(db, queue_uuid, app_id)
            payload = Payload.from_queue_item(item)
            content = []
            for record in stream_payload_working_records(db, payload):
                record_data = {
                    key: getattr(record, key)
                    for key in self.SEND_KEYS
                }
                record_data["actions"] = {
                    "fetch": {
                        "endpoint": flask.url_for("desktop.fetch_working_record", record_uuid=record.working_uuid, _external=True),
                    },
                    "save": {
                        "endpoint": flask.url_for("desktop.save_working_record", record_uuid=record.working_uuid, _external=True),
                    }
                }
                content.append(record_data)
            return {
                "success": True,
                "message": "Success",
                "data": content,
            }

    def save_record_actions(self, working_uuid: str, actions: list[dict]):
        _ = [
            RecordAction.from_map(x)
            for x in actions
        ]
        with self.nodb as db:
            record = NODBWorkingRecord.find_by_uuid(db, working_uuid)
            if record is None:
                return {
                    "success": False,
                    "message": "No such record",
                }
            else:
                record.metadata["proposed_actions"] = actions
                db.update_object(record)
                db.commit()
                return {
                    "success": True,
                    "message": "Record updated"
                }

    def stream_working_record(self, record_uuid: str):
        with self.nodb as db:
            record = NODBWorkingRecord.find_by_uuid(db, record_uuid)
            ocproc_record = record.record
            if record is None or ocproc_record is None:
                return {
                    "success": False,
                    "message": "No such record",
                    "data": None,
                    "proposed_actions": None,
                }
            else:
                return {
                    "success": True,
                    "message": "Success",
                    "data": ocproc_record.to_mapping(),
                    "proposed_actions": record.metadata.get("proposed_actions", None),
                }

    def fetch_platform(self, station_uuid: str) -> dict:
        with self.nodb as db:
            platform = NODBPlatform.find_by_uuid(db, station_uuid)
            if platform is None:
                return {
                    "success": False,
                    "message": "No such platform",
                    "data": None,
                }
            else:
                return {
                    "success": True,
                    "message": "Success",
                    "data": {
                        "actions": self._platform_actions(platform.platform_uuid),
                        **platform.to_map()
                    }
                }

    def create_platform(self,
                        wmo_id: str | None,
                        wigos_id: str | None,
                        platform_name: str | None,
                        platform_id: str | None,
                        platform_type: str | None,
                        start_date: AwareDateTime | None,
                        end_date: AwareDateTime | None,
                        status: PlatformStatus,
                        embargo_data_days: int | None,
                        skip_speed_check: bool,
                        skip_land_check: bool,
                        mandatory_review: bool,
                        dedupe_time_window: float | None,
                        dedupe_distance_window: float | None,
                        top_speed: str | float | None,
                        map_to_uuid: str | None) -> dict:
        with self.nodb as db:
            if map_to_uuid is not None:
                other = NODBPlatform.find_by_uuid(db, map_to_uuid)
                if other is None:
                    return {
                        "success": False,
                        "message": "map_to_uuid set to invalid platform",
                        "data": None,
                    }
            platform = NODBPlatform()
            platform.wmo_id = wmo_id
            platform.wigos_id = wigos_id
            platform.platform_name = platform_name
            platform.platform_id = platform_id
            platform.platform_type = platform_type
            platform.service_start_date = start_date
            platform.service_end_date = end_date
            platform.status = status
            platform.map_to_uuid = map_to_uuid
            platform.embargo_data_days = embargo_data_days
            platform.metadata["skip_speed_check"] = bool(skip_speed_check)
            platform.metadata["skip_on_land_check"] = bool(skip_land_check)
            platform.metadata["dedupe_time_window"] = dedupe_time_window
            platform.metadata["dedupe_distance_window"] = dedupe_distance_window
            platform.metadata["mandatory_review"] = bool(mandatory_review)
            platform.metadata["top_speed"] = top_speed
            db.insert_object(platform)
            db.commit()
            return {
                "success": True,
                "message": "Success",
                "data": {
                    "platform_uuid": platform.platform_uuid,
                    "actions": self._platform_actions(platform.platform_uuid),
                }
            }

    def _platform_actions(self, platform_uuid: str) -> dict:
        actions = {}
        if current_user().require_all("pipeman.view_platforms"):
            actions["view"] = {
                "endpoint": flask.url_for(
                    "desktop.fetch_platform_record",
                    platform_uuid=platform_uuid,
                    _external=True
                )
            }
        if current_user().require_all("pipeman.update_platforms"):
            actions["update"] = {
                "endpoint": flask.url_for(
                    "desktop.update_platform_record",
                    platform_uuid=platform_uuid,
                    _external=True
                )
            }
        return actions

    def update_platform(self,
                        platform_uuid: str,
                        wmo_id: str | None,
                        wigos_id: str | None,
                        platform_name: str | None,
                        platform_id: str | None,
                        platform_type: str | None,
                        start_date: AwareDateTime | None,
                        end_date: AwareDateTime | None,
                        status: PlatformStatus,
                        embargo_data_days: int | None,
                        skip_speed_check: bool,
                        skip_land_check: bool,
                        mandatory_review: bool,
                        dedupe_time_window: float | None,
                        dedupe_distance_window: float | None,
                        top_speed: str | float | None,
                        map_to_uuid: str | None):
        with self.nodb as db:
            platform = NODBPlatform.find_by_uuid(db, platform_uuid)
            if platform is None:
                return {
                    "success": False,
                    "message": "No such platform",
                }
            else:
                if map_to_uuid is not None:
                    other = NODBPlatform.find_by_uuid(db, map_to_uuid)
                    if other is None:
                        return {
                            "success": False,
                            "message": "map_to_uuid set to invalid platform"
                        }
                platform.wmo_id = wmo_id
                platform.wigos_id = wigos_id
                platform.platform_name = platform_name
                platform.platform_id = platform_id
                platform.platform_type = platform_type
                platform.service_start_date = start_date
                platform.service_end_date = end_date
                platform.status = status
                platform.map_to_uuid = map_to_uuid
                platform.embargo_data_days = embargo_data_days
                platform.metadata["skip_speed_check"] = bool(skip_speed_check)
                platform.metadata["skip_on_land_check"] = bool(skip_land_check)
                platform.metadata["dedupe_time_window"] = dedupe_time_window
                platform.metadata["dedupe_distance_window"] = dedupe_distance_window
                platform.metadata["mandatory_review"] = bool(mandatory_review)
                platform.metadata["top_speed"] = top_speed
                db.update_object(platform)
                db.commit()
                return {
                    "success": True,
                    "message": "Success",
                }

    def search_stations(self,
                        time_frame: AwareDateTime | None,
                        wmo_id: str | None,
                        wigos_id: str | None,
                        platform_id: str | None,
                        platform_name: str | None) -> dict:
        with self.nodb as db:
            results = []
            for x in NODBPlatform.search(db, time_frame, wmo_id, wigos_id, platform_id, platform_name, key_only=True):
                results.append({
                    "platform_uuid": x.platform_uuid,
                    "actions": self._platform_actions(x.platform_uuid),
                })
            return {
                "success": True,
                "message": "Success",
                "data": results
            }
