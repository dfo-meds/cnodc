import typing as t

from pipeman.processing.queue_worker import QueueItemResult
from pipeman.processing.payload_worker import ObservationWorkflowWorker
from pipeman.exceptions import CNODCError
from pipeman.processing.payloads import ObservationPayload
import nodb as orm


class GTSQueueWorker(ObservationWorkflowWorker):

    def __init__(self, **kwargs):
        super().__init__(
            process_name="gts_enqueuer",
            process_version="1_0",
            **kwargs
        )
        self.set_defaults({})

    def process_payload(self, payload: ObservationPayload) -> t.Optional[QueueItemResult]:
        if 'gts_info' not in payload.metadata or not payload.metadata['gts_info']:
            raise CNODCError("Missing GTS message data")
        if isinstance(payload.metadata['gts_info'], (list, tuple, set)):
            for info in payload.metadata['gts_info']:
                self._queue_gts_message(payload, info)
        else:
            self._queue_gts_message(payload, payload.metadata['gts_info'])
        return QueueItemResult.SUCCESS


    def _queue_gts_message(self, payload: ObservationPayload, gts_info: dict):
        if not isinstance(gts_info, dict):
            raise CNODCError('Invalid GTS info')
        if 'format' not in gts_info or not gts_info['format']:
            raise CNODCError('Missing GTS format')
        if 'message_type' not in gts_info or not gts_info['message_type']:
            gts_info['message_type'] = 'NEW'
        if 'processing_center' not in gts_info or not gts_info['processing_center']:
            gts_info['processing_center'] = ''
        message = orm.GTSOutgoingMessage()
        message.obs_uuid = payload.obs_uuid
        message.obs_received_date = payload.received_date
        message.message_format = gts_info['format']
        message.message_type = orm.GTSOutgoingMessageType(gts_info['message_type'])

        self.db.insert_object(message)


