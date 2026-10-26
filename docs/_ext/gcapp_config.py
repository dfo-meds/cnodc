from docutils.statemachine import StringList
from sphinx.application import Sphinx
from sphinx.domains.std import ConfigurationValue
from sphinx.util.docutils import SphinxDirective

from docutils import nodes
from sphinx.util.typing import ExtensionMetadata


class GCConfigList(nodes.General, nodes.Element):
    ...

class GCConfig(nodes.General, nodes.Element):
    ...


class GCConfigListDirective(SphinxDirective):

    def run(self):
        return [GCConfigList('')]


def process_config_nodes(app: Sphinx, doctree, fromdocname):
    app.
    if not hasattr(app.env, "gcconfig_all_config"):
        app.env.gcconfig_all_config = []

    for node in doctree.findall(GCConfigList):
        content = []
        for config in doctree.findall(ConfigurationValue):
            content.append(config)
        node.replace_self(content)


def setup(app: Sphinx) -> ExtensionMetadata:

    app.add_node(GCConfigList)
    app.add_directive("gcconfiglist", GCConfigListDirective)

    app.connect("doctree-resolved", process_config_nodes)

    return {
        "version": "0.1",
        "env_version": 1,
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }