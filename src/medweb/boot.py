import typing as t


if t.TYPE_CHECKING:
    from medweb.system import MedWebSystem



def boot_medweb(app_type: str) -> MedWebSystem:
    from gcapp.boot import boot_system
    system: MedWebSystem = boot_system(
        app_name='medweb',
        app_components=[app_type],
        system_cls="medweb.system.MedWebSystem",
        init_hooks=[
            _init_system
        ],
        manual_overrides={
            "nodb.interface.NODB": "nodb.controller.NODBPostgresController",
        }
    )
    return system

def _init_system(s: MedWebSystem):
    s.plugins.discover_from_module("medweb.apps")
    s.plugins.add_plugin("nodb")
