"""
Helper functions for boot
"""

import sys
import typing as t
import logging

import zirconium as zr
import zrlog
from autoinject import injector, auto


@injector.inject
def init_system_logging(version_no: str | None = None):
    """ Initiates the logging sub-system with a version number that can be added to logging messages."""
    # Setup logging
    for h in logging.getLogger().handlers:
        logging.getLogger().removeHandler(h)
    zrlog.init_logging()
    zrlog.set_default_extra('version', version_no or 'unknown')

    # Setup additional info
    from gcapp.requestinfo import RequestInfo
    @injector.inject
    def _init_rinfo(rinfo: RequestInfo = None):
        rinfo.set_logging_defaults()
        rinfo.set_logging_extras_system()
    _init_rinfo()


@injector.inject
def init_overrides(overrides: dict[str | type, str | type | t.Callable | dict] | None = None,
                   config: zr.ApplicationConfig = auto()):
    """Overrides default objects with declared subclasses as required."""
    if not overrides:
        overrides = {}
    overrides.update(config.get("autoinject", default={}))
    if overrides:
        for cls_name, cls_def in overrides.items():
            cls_def = overrides[cls_name]
            if isinstance(cls_def, dict):
                args = cls_def.get('args', [])
                if 'weight' not in cls_def:
                    cls_def['weight'] = sys.maxsize
                injector.override(cls_name, *args, **{x: cls_def[x] for x in cls_def if x != 'args'})
            else:
                injector.override(cls_name, cls_def)
