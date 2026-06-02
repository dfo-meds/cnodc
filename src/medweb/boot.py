import typing as t


if t.TYPE_CHECKING:
    from medweb.system import MedWebSystem



def boot_medweb() -> MedWebSystem:
    from gcapp.boot import boot_system
    system: MedWebSystem = boot_system('medweb', system_cls="medweb.system.MedWebSystem", init_hooks=[
        _init_system
    ])
    return system

def _init_system(s: MedWebSystem):
    s.plugins.discover_from_module("medweb.apps")
