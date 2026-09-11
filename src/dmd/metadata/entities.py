from dmd.containers.base import Container
from dmd.metadata.registries import BaseRegistry
from gcapp.i18n import MLString

from autoinject import injector


@injector.injectable
class EntityRegistry(BaseRegistry):

    def __init__(self):
        super().__init__("entity")

    def entity_options(self, show_hidden: bool = False) -> list[tuple[str, MLString]]:
        return [
            (
                entity_type,
                MLString(self[entity_type].get("display", None) or {"und": entity_type})
            )
            for entity_type in self
            if show_hidden or self[entity_type].get("hidden", False)
        ]

    def entity_display(self, entity_type: str) -> MLString:
        return MLString(self[entity_type].get("display", None) or {"und": entity_type})


class Entity(Container):
    ...
