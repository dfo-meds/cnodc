import pathlib
from autoinject import auto, injector

from gcapp.system import System

PLUGIN_DIR = pathlib.Path(__file__).parent.absolute()

@injector.inject
def init_plugin(system: System = auto()):

    from dmd.metadata.metadata import MetadataRegistry
    from dmd.metadata.entities import EntityRegistry
    from dmd.metadata.vocabularies import VocabularyRegistry

    @system.on_setup
    @injector.inject
    def on_setup(mreg: MetadataRegistry = auto(),
                 vreg: VocabularyRegistry = auto(),
                 ereg: EntityRegistry = auto()):
        mreg.register_metadata_fields_from_yaml(PLUGIN_DIR / "metadata.yaml")
        vreg.register_from_yaml(PLUGIN_DIR / "vocabs.yaml")
        ereg.register_from_yaml(PLUGIN_DIR / "entities.yaml")
        mreg.register_security_labels_from_yaml(PLUGIN_DIR / "security.yaml")



