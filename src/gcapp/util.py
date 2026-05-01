import sys
import typing as t

import zirconium as zr
import zrlog
from autoinject import injector

from gcapp.requestinfo import RequestInfo


@injector.inject
def init_system_logging(rinfo: RequestInfo = None):
    # Setup logging
    zrlog.init_logging()
    rinfo.set_logging_defaults()
    rinfo.set_logging_extras_system()


@injector.inject
def init_overrides(overrides: dict[str | type, str | type | t.Callable | dict] | None = None,
                   config: zr.ApplicationConfig = None):
    """Override default objects with declared sub-classes as required."""
    overrides.update(config.get("autoinject", default={}))
    if overrides:
        for cls_name in overrides:
            cls_def = overrides[cls_name]
            if isinstance(cls_def, str):
                injector.override(cls_name, cls_def, weight=1)
            elif isinstance(cls_def, dict):
                args = cls_def.get('args', [])
                if 'weight' not in cls_def:
                    cls_def['weight'] = sys.maxsize
                injector.override(cls_name, *args, **{x: cls_def[x] for x in cls_def if x != 'args'})
