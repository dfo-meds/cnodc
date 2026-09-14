from autoinject import injector
import typing as t

if t.TYPE_CHECKING:
    from dmd.metadata.datasets import Dataset


@injector.injectable
class DataManagementLinker:

    def has_dataset_access(self,
                           operation: str,
                           dataset: Dataset) -> bool:
        return False


    def metadata_link(self,
                      profile_name: str,
                      format_name: str,
                      dataset_id: int,
                      revision_no: int) -> str:
        raise NotImplementedError

    def view_dataset_link(self,
                          dataset_id: int) -> str:
        raise NotImplementedError

    def copy_dataset_link(self,
                          dataset_id: int) -> str:
        raise NotImplementedError

    def edit_dataset_metadata_link(self,
                          dataset_id: int) -> str:
        raise NotImplementedError

    def edit_dataset_link(self,
                          dataset_id: int) -> str:
        raise NotImplementedError

    def validate_dataset_link(self,
                          dataset_id: int) -> str:
        raise NotImplementedError

    def remove_dataset_link(self,
                          dataset_id: int) -> str:
        raise NotImplementedError

    def add_attachment_link(self,
                          dataset_id: int) -> str:
        raise NotImplementedError

    def activate_dataset_link(self,
                          dataset_id: int) -> str:
        raise NotImplementedError

    def publish_dataset_link(self,
                          dataset_id: int) -> str:
        raise NotImplementedError

    def restore_dataset_link(self,
                          dataset_id: int) -> str:
        raise NotImplementedError