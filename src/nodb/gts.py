import enum
import datetime

import nodb.base as s


class GTSOutgoingMessageType(enum.Enum):

    New = "NEW"
    Correction = "CORRECTION"
    Addition = "ADDITION"
    Delayed = "DELAYED"


class GTSOutgoingMessageStatus(enum.Enum):

    Queued = "QUEUED"
    Sent = "SENT"


class GTSOutgoingMessage(s.NODBBaseObject):

    TABLE_NAME = "gts_outgoing_message"
    PRIMARY_KEYS = ("message_id",)

    message_id: str = s.UUIDColumn("message_id")

    message_format: str = s.StringColumn("message_format")
    message_type: GTSOutgoingMessageType = s.EnumColumn("message_type", GTSOutgoingMessageType)
    processing_center: str = s.StringColumn("processing_center")
    obs_uuid: str = s.UUIDColumn("obs_uuid")
    obs_received_date: datetime.datetime = s.DateTimeColumn("obs_received_date")

    status: GTSOutgoingMessageStatus = s.EnumColumn("status", GTSOutgoingMessageStatus)
    queued_date: datetime.datetime = s.DateTimeColumn("queued_date", readonly=True)

    sent_date: datetime.datetime = s.DateTimeColumn("sent_date")
    assigned_header: str = s.StringColumn("assigned_header")
    supplementary_header: str = s.StringColumn("supplementary_header")