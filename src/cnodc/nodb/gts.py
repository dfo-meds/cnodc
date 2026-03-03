
from cnodc.nodb.base import *

class GTSOutgoingMessageType(enum.Enum):

    New = "NEW"
    Correction = "CORRECTION"
    Addition = "ADDITION"
    Delayed = "DELAYED"


class GTSOutgoingMessageStatus(enum.Enum):

    Queued = "QUEUED"
    Sent = "SENT"


class GTSOutgoingMessage(NODBBaseObject):

    TABLE_NAME = "gts_outgoing_message"
    PRIMARY_KEYS = ("message_id",)

    message_id: str = UUIDColumn("message_id")

    message_format: str = StringColumn("message_format")
    message_type: GTSOutgoingMessageType = EnumColumn("message_type", GTSOutgoingMessageType)
    processing_center: str = StringColumn("processing_center")
    obs_uuid: str = UUIDColumn("obs_uuid")
    obs_received_date: datetime.datetime = DateTimeColumn("obs_received_date")

    status: GTSOutgoingMessageStatus = EnumColumn("status", GTSOutgoingMessageStatus)
    queued_date: datetime.datetime = DateTimeColumn("queued_date", readonly=True)

    sent_date: datetime.datetime = DateTimeColumn("sent_date")
    assigned_header: str = StringColumn("assigned_header")
    supplementary_header: str = StringColumn("supplementary_header")