"""

The National Observations DataBase (NODB) stores ocean observations and working copies of them, as well as
ancillary data about the system and its processes.


"""

from autoinject import auto, injector

from gcapp.system import System
from nodb.interface import NODB


def init_plugin(system: System):
    system.on_setup(_upgrade_database)


@injector.inject
def _upgrade_database(nodb: NODB = auto()):
    with nodb as db:
        from nodb._upgrade import Upgrader

        upgrader = Upgrader(db)
        upgrader.upgrade()
