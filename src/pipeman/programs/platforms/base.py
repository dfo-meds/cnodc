import datetime
import typing as t

from autoinject import injector, auto

from nodb import NODB
from nodb.interface import NODBInstance
from nodb.observations import NODBPlatform


class PlatformInfo:

    def __init__(self,
                 wmo_id: int | None = None,
                 platform_id: str | None = None,
                 country: str | None = None,
                 start_date: datetime.date | None = None,
                 end_date: datetime.date | None = None,
                 metadata: dict[str, t.Any] | None = None):
        self.country = country
        self.wmo_id = wmo_id
        self.start_date = start_date
        self.end_date = end_date
        self.platform_id = platform_id
        self.metadata = {
            x: y for x, y in metadata.items() if y is not None and y != ""
        } if metadata else {}



class PlatformManager:

    nodb: NODB = auto()

    @injector.construct
    def __init__(self):
        self._db = None

    @property
    def db(self) -> NODBInstance:
        if self._db is None:
            raise RuntimeError("called at the wrong time")
        return self._db

    def __enter__(self) -> PlatformManager:
        self._db = self.nodb.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._db.__exit__(exc_type, exc_val, exc_tb)

    def save_platform(self, info: PlatformInfo):
        search_kwargs = {}
        if info.wmo_id is not None:
            search_kwargs["wmo_id"] = info.wmo_id
        if info.platform_id is not None:
            search_kwargs["platform_id"] = info.platform_id
        if not search_kwargs:
            print("invalid platform, no identifiers, skipping")
            return
        platforms = [x for x in NODBPlatform.search(**search_kwargs) if self.service_overlaps(x, info)]
        if not platforms:
            self.create_platform(info)
        elif len(platforms) == 1:
            self.update_platform(platforms[0], info)
        else:
            raise RuntimeError(f"Error saving platform, multiple identifiers found for {search_kwargs}")

    def service_overlaps(self, platform: NODBPlatform, info: PlatformInfo) -> bool:
        ...

    def create_platform(self, info: PlatformInfo):
        ...

    def update_platform(self, platform: NODBPlatform, info: PlatformInfo):
        ...
