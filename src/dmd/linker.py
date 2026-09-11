from autoinject import injector


@injector.injectable
class DataManagementLinker:

    def metadata_link(self,
                      profile_name: str,
                      format_name: str,
                      dataset_id: int,
                      revision_no: int) -> str:
        raise NotImplementedError