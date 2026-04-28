import typing as t

from autoinject import injector
from medsutil.email import DelayedEmailController, EmailController
from nodb import NODB, NODBQueueItem
from pipeman.processing.queue_worker import QueueWorker, QueueItemResult


class DelayedEmailsQueuer:

    nodb: NODB = None

    @injector.construct
    def __init__(self): ...

    def send_delayed(self, kwargs: dict) -> bool:
        with self.nodb as db:
            db.create_queue_item(queue_name="emails", data=kwargs)
            return True


class DelayedEmailsWorker(QueueWorker):

    emails: EmailController = None

    @injector.construct
    def __init__(self):
        super().__init__()
        self.set_defaults({
            'queue_name': 'emails'
        })

    def process_queue_item(self, item: NODBQueueItem) -> t.Optional[QueueItemResult]:
        self.emails.direct_send_email(**item.data)
