import typing as t

from autoinject import injector

from dmd.containers.base import ContainerLoader
from gcapp.queries import Queryable, QueryManager
from medsutil.awaretime import AwareDateTime

if t.TYPE_CHECKING:
    from dmd.metadata.datasets import Dataset


class DatasetKeywords(t.TypedDict):
    profiles: list[str] | tuple[str] | set[str]
    display_names: dict[str, str]
    guid: str
    is_deprecated: bool
    organization_id: int | None
    users: list[str]
    created_date: AwareDateTime | None
    modified_date: AwareDateTime | None


@injector.injectable
class DataManagementDatabase:

    def __enter__(self) -> DataManagementInterface:
        raise NotImplementedError

    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError

    def queryable_table(self, table_name: str) -> QueryManager:
        raise NotImplementedError


class DataManagementInterface(ContainerLoader):

    def workflow_item_exists(self,
                             category: str,
                             workflow_name: str,
                             object_id: int,
                             object_type: str) -> bool:
        raise NotImplementedError

    def last_setup_run(self) -> AwareDateTime | None:
        raise NotImplementedError

    def load_registry_map(self, registry_name: str) -> t.Iterable[tuple[str, dict]]:
        raise NotImplementedError

    def delete_registry_map(self, registry_name: str):
        raise NotImplementedError

    def upsert_registry_entry(self,
                              registry_name: str,
                              entry_name: str,
                              entry_value: dict):
        raise NotImplementedError

    def bulk_update_registry_map(self, registry_name: str, entry_map: dict[str, dict]):
        raise NotImplementedError

    def update_vocabulary_terms(self, vocabulary_name: str, terms: dict[str, dict], replace_existing: bool = True):
        raise NotImplementedError

    def load_vocabulary_terms(self, vocabulary_name: str) -> dict[str, dict]:
        raise NotImplementedError

    def load_vocabulary_term(self, vocabulary_name: str, short_name: str) -> dict | None:
        raise NotImplementedError

    def create_dataset(self, data: DatasetKeywords) -> int:
        raise NotImplementedError

    def update_dataset(self, container_id: int, data: DatasetKeywords):
        raise NotImplementedError

    def save_metadata(self, container_id: int, metadata: dict[str, t.Any]):
        raise NotImplementedError

    def find_dataset_by_id(self, container_id: int, revision: int | None = None) -> tuple[DatasetKeywords, dict[str, t.Any]] | None:
        raise NotImplementedError

    def find_dataset_by_guid(self, guid: str, authority: str | None = None) -> tuple[DatasetKeywords, dict[str, t.Any]] | None:
        raise NotImplementedError

    def user_organization_ids(self, user_id: int) -> list[int]:
        raise NotImplementedError

    def user_dataset_ids(self, user_id: int) -> list[int]:
        raise NotImplementedError
