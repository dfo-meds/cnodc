from autoinject import injector

@injector.injectable
class IntakeManager:

    def list_workflows(self) -> dict:
        ...

    def workflow_info(self, workflow_name: str) -> dict:
        ...

    def submit_file(self, workflow_name: str, data: bytes, metadata: dict[str, str], request_id: str | None = None) -> dict:
        ...

    def cancel_upload(self, workflow_name: str, request_id: str) -> dict:
        ...
