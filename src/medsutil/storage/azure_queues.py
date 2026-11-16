"""Support for Azure Queues

Note: this is not a plugin for the normal storage sub-system, but it
relies on the same configuration items as Files and Blobs so it made
sense to put it here.


There are three methods of providing Azure credentials to authenticate:

#1: Provide them in the Zirconium configuration file:

```
[azure.storage.STORAGE_ACCOUNT_NAME]
connection_string = "..."
```


#2: Provide your credentials in a method supported by DefaultAzureCredentials

#3: Include the SAS token in the URL (not recommended)

"""
from azure.storage.queue import QueueClient
import zirconium as zr
from autoinject import injector, auto



class AzureQueueWrapper:

    config: zr.ApplicationConfig = auto()

    @injector.construct
    def __init__(self,
                 storage_account: str,
                 queue_name: str):
        self._account_name = storage_account
        self._queue_name = queue_name

    def client(self) -> QueueClient:
        return QueueClient.from_connection_string(
            self._get_connection_string(),
            self._queue_name
        )

    def _get_connection_string(self) -> str:
        return self.config.as_str(("storage", "azure", self._account_name, "connection_string"), default="")
