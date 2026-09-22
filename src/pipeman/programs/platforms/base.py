import datetime
import typing as t

from autoinject import injector, auto

from nodb import NODB
from nodb.interface import NODBInstance
from nodb.observations import NODBPlatform


class PlatformInfo:

    # glider;unknown;ship;drifting_buoy;profiling_float;animal;moored_buoy

    def __init__(self,
                 wmo_id: int | None = None,
                 platform_name: str | None = None,
                 platform_id: str | None = None,
                 imo_number: str | None = None,
                 mmsi: str | None = None,
                 ship_code: str | None = None,
                 platform_type: str | None = None,
                 registry_ca: str | None = None,
                 country: str | None = None,
                 start_date: str | datetime.date | None = None,
                 end_date: str | datetime.date | None = None,
                 metadata: dict[str, t.Any] | None = None):
        self.platform_type = platform_type
        self.imo_number = imo_number
        self.platform_id = platform_id
        self.mmsi = mmsi
        self.registry_ca = registry_ca
        self.country = country
        self.wmo_id = wmo_id
        self.platform_name = platform_name
        self.ship_code = ship_code
        if isinstance(start_date, str):
            self.start_date = self.parse_date_str(start_date)
        else:
            self.start_date = start_date
        if isinstance(end_date, str):
            self.end_date = self.parse_date_str(end_date)
        else:
            self.end_date = end_date
        self.metadata = {
            x: y for x, y in metadata.items() if y is not None and y != ""
        } if metadata else {}

    @staticmethod
    def parse_date_str(d: str, is_end: bool = False) -> None | datetime.date:
        if d == "":
            return None
        elif len(d) == 4 and d.isdigit():
            return datetime.date(int(d), 12, 31) if is_end else datetime.date(int(d), 1, 1)
        else:
            return datetime.datetime.fromisoformat(d)


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
            raise RuntimeError(f"Error saving platform, multiple options found for {search_kwargs}")

    def service_overlaps(self, platform: NODBPlatform, info: PlatformInfo) -> bool:
        if info.end_date is not None and platform.service_start_date is not None:
            if info.end_date < platform.service_start_date:
                return False
        if info.start_date is not None and platform.service_end_date is not None:
            if info.start_date > platform.service_end_date:
                return False
        return True

    def create_platform(self, info: PlatformInfo):
        ...

    def update_platform(self, platform: NODBPlatform, info: PlatformInfo):
        ...
