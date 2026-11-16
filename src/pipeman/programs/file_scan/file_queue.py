from autoinject import injector, auto

from medsutil.storage import StorageController
from nodb import interface
from medsutil import json
from medsutil.exceptions import CodedError
from pipeman.processing.azure_worker import AzureQueueWorker
from pipeman.processing.workflow import WorkflowController


class AzureQueueIntakeError(CodedError):
    CODE_SPACE = "AZ-QUEUE-INTAKE"


class AzureQueueIntakeWorker(AzureQueueWorker):

    nodb: interface.NODB = auto()
    storage: StorageController = auto()

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(process_name="azure_queue_intake", process_version="1.0", **kwargs)

    def process_message(self, body: str):
        content = json.load_dict(body)
        path = content.get("path", "")
        if not path:
            raise AzureQueueIntakeError("Missing path", 1000)
        workflow = content.get("workflow", "")
        if not workflow:
            raise AzureQueueIntakeError("Missing workflow", 1001)
        self.start_workflow(
            str(path),
            str(workflow),
            {
                x: y
                for x, y in content.items()
                if x not in ("path", "workflow",)
            }
        )

    def start_workflow(self,
                       file_path: str,
                       workflow_name: str,
                       metadata: dict):
        with self.storage.handle(file_path, self._halt_flag) as handle:
            temp_file = self.temp_dir() / "file"
            handle.download(temp_file)
            with self.nodb as db:
                workflow = WorkflowController.load_controller(db, workflow_name, self._halt_flag)
                workflow.handle_incoming_file(
                    local_path=temp_file,
                    metadata=metadata,
                    db=db,
                )
