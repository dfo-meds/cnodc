import typing as t

from autoinject import injector
from medsutil.email import EmailController
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
    def __init__(self, **kwargs):
        super().__init__(
            process_name="delayed_email_send",
            process_version="1_0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'emails'
        })

    def process_queuee_item(self, item: NODBQueueItem) -> t.Optional[QueueItemResult]:
        self.emails.bare_direct_send_email(**item.data)

