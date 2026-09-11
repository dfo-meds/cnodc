import typing as t

from autoinject import injector

from dmd.containers.base import ContainerLoader
from medsutil.awaretime import AwareDateTime


@injector.injectable
class DataManagementDatabase(ContainerLoader):

    def last_setup_run(self) -> AwareDateTime | None: raise NotImplementedError

    def load_registry_map(self, registry_name: str) -> t.Iterable[tuple[str, dict]]: raise NotImplementedError

    def delete_registry_map(self, registry_name: str): raise NotImplementedError

    def upsert_registry_entry(self,
                              registry_name: str,
                              entry_name: str,
                              entry_value: dict): raise NotImplementedError

    def bulk_update_registry_map(self, registry_name: str, entry_map: dict[str, dict]): raise NotImplementedError

    def update_vocabulary_terms(self, vocabulary_name: str, terms: dict[str, dict], replace_existing: bool = True):
        raise NotImplementedError

    def load_vocabulary_terms(self, vocabulary_name: str) -> dict[str, dict]:
        raise NotImplementedError

    def load_vocabulary_term(self, vocabulary_name: str, short_name: str) -> dict | None:
        raise NotImplementedError