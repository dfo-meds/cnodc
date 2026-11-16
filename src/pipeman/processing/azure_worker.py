"""

"""
from azure.storage.queue import QueueClient, QueueMessage

from medsutil.exceptions import CodedError, HaltInterrupt
from medsutil.storage.azure_queues import AzureQueueWrapper
from pipeman.exceptions import CNODCError
from pipeman.processing.base_worker import BaseWorker
from pipeman.processing.queue_worker import QueueItemResult


class AzureQueueWorker(BaseWorker):
    """Execute a process on every queue item."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_defaults(
            {
                "azure_account_name": None,
                "azure_queue_name": None,
                "azure_hidden_time": 60,
                "azure_delay_time": 5,
            }
        )
        self._client: QueueClient | None = None
        self._message: QueueMessage | None = None

    def _run(self):
        wrapper = AzureQueueWrapper(
            self.get_config("azure_account_name"),
            self.get_config("azure_queue_name")
        )
        with wrapper.client() as queue_client:
            while self.continue_loop():
                messages = queue_client.receive_messages(visibility_timeout=self.get_config("azure_hidden_time", 60, coerce=int))
                for batch in messages.by_page():
                    for message in batch:
                        self._process_next_message(message, queue_client)
                self.responsive_sleep(self.get_config("azure_delay_time", 5, coerce=float))

    def _process_next_message(self,
                              message: QueueMessage,
                              queue_client: QueueClient):
        try:
            self.before_cycle()
            self.set_cycle_config({})
            self._message = message
            self._client = queue_client
            self._process_result(
                message, queue_client, self.process_message(message.content)
            )
        except CodedError as ex:
            # Recoverable errors may be fixable later, so we requeue the item for retrying
            if ex.is_transient:
                self._process_result(message, queue_client, QueueItemResult.RETRY, ex)

            # Non-recoverable mean the entire item should fail
            else:
                self._process_result(message, queue_client, QueueItemResult.FAILED, ex)

        except Exception as ex:
            self._process_result(message, queue_client, QueueItemResult.FAILED, ex)

        except (KeyboardInterrupt, HaltInterrupt) as ex:
            self._process_result(message, queue_client, QueueItemResult.RETRY, ex)
            self._log.critical("HaltInterrupt detected")
            raise

        finally:
            self.after_cycle()
            self._message = None
            self._client = None

    def _process_result(self,
                        message: QueueMessage,
                        queue_client: QueueClient,
                        result: QueueItemResult | None,
                        exception: Exception | None = None):
        self._log.trace(
            "Result of processing [%s:%s] is [%s]",
            queue_client.queue_name,
            message.id,
            result
        )
        if exception is not None:
            self._log.exception("An exception occurred while processing [%s:%s]: %s: %s",
                                queue_client.queue_name,
                                message.id,
                                type(exception),
                                str(exception))
        if result is not QueueItemResult.RETRY:
            queue_client.delete_message(message)

    def renew_item(self, timeout: int | None = None):
        if self._message is not None and self._client is not None:
            self._log.trace("Renewing azure queue item [%s]", self._message.id)
            if timeout is None:
                self._client.update_message(self._message, visibility_timeout=self.get_config("azure_hidden_time", 60, coerce=int))
            else:
                self._client.update_message(self._message, visibility_timeout=timeout)

    def on_start(self):
        if not self.get_config("azure_account_name"):
            raise CNODCError(
                "No account name specified for an azure queue worker", "AZ-QUEUE-WORKER", 1000
            )
        if not self.get_config("azure_queue_name"):
            raise CNODCError(
                "No queue specified for an azure queue worker", "AZ-QUEUE-WORKER", 1000
            )
        super().on_start()

    def process_message(self, body: str) -> QueueItemResult:
        raise NotImplementedError
