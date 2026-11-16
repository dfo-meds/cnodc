"""Provides a plugin for sarracenia to add an entry to an Azure queue.
The queue is monitored to start file processing.

Note: we do it this way so we can keep the sarracenia queue clear. The Azure
queue lives forever, so it can be emptied as needed.

Configuration options (environment variables):

Required:
- AZURE_CONNECTION_STRING: the connection string for Azure
- AZURE_QUEUE_NAME: the name of the queue to send messages to
- AZURE_MOUNT_ROOT: where is the Azure filesystem mounted (case-sensitive)
- AZURE_BASE_URL: the URL of the Azure filesystem that is mounted
- PIPEMAN_WORKFLOW: the workflow to start

Optional:
- AZURE_QUEUE_TTL: the time for the messages to live (default -1 forever)
- AZURE_QUEUE_TIMEOUT: the time for the server to wait to create a queue item (default 30 seconds)
"""
import json
import logging
import os
import pathlib

from sarracenia.flowcb import FlowCB
from azure.storage.queue import QueueClient


class SendToAzureQueue(FlowCB):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._azure_conn = os.environ.get("AZURE_CONNECTION_STRING", "")
        self._queue_name = os.environ.get("AZURE_QUEUE_NAME", "")
        self._ttl = int(os.environ.get("AZURE_QUEUE_TTL", "-1"))
        self._server_timeout = int(os.environ.get("AZURE_QUEUE_TIMEOUT", "30"))
        self._azure_root = os.environ.get("AZURE_MOUNT_ROOT", "")
        self._azure_base = os.environ.get("AZURE_BASE_URL", "")
        self._workflow = os.environ.get("PIPEMAN_WORKFLOW", "")
        self._log = logging.getLogger("cnodcwis.sarracenia")

    def after_work(self, worklist):
        with QueueClient.from_connection_string(
            self._azure_conn,
            self._queue_name
        ) as queue_client:
            new_ok = []
            for message in worklist.ok:
                try:
                    queue_client.send_message(
                        self._build_azure_message(message),
                        time_to_live=self._ttl,
                        timeout=self._server_timeout,
                    )
                    self._log.info(f"Sent message to queue [%s]", message)
                    new_ok.append(message)
                except Exception:
                    self._log.exception("Error while sending message [%s]", message)
            worklist.ok = new_ok

    def _build_azure_message(self, sarra_message: dict) -> str:
        file_path = (pathlib.Path(sarra_message.get("new_dir", "")) / sarra_message.get("new_file", "")).absolute()
        azure_message = {
            "path": str(file_path).replace(self._azure_root, self._azure_base),
            "workflow": self._workflow,
            "filename": file_path.name,
            "last-modified-date": sarra_message.get("mtime", ""),
            "sarra-pub-time": sarra_message.get("pubTime", ""),
            "sarra-topic": sarra_message.get("topic", ""),
            "sarra-source": sarra_message.get("source", ""),
            "source": "sarracenia",
        }
        return json.dumps(azure_message)
