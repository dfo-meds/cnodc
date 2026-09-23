import datetime
import typing as t

from autoinject import injector, auto

from medsutil.awaretime import AwareDateTime
from nodb import NODB
from nodb.interface import NODBInstance
from nodb.observations import NODBPlatform, PlatformStatus


class PlatformInfo:

    def __init__(self,
                 wmo_id: str | None = None,
                 platform_name: str | None = None,
                 platform_id: str | None = None,
                 imo_number: str | None = None,
                 mmsi: str | None = None,
                 ship_code: str | None = None,
                 wigos_id: str | None = None,
                 platform_type: str | None = None,
                 registry_ca: str | None = None,
                 country: str | None = None,
                 start_date: str | datetime.date | None = None,
                 end_date: str | datetime.date | None = None,
                 metadata: dict[str, t.Any] | None = None):
        self.platform_type = platform_type
        self.wigos_id = wigos_id
        self.platform_id = platform_id
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
        self.imo_number = imo_number
        self.mmsi = mmsi
        self.registry_ca = registry_ca
        self.country = country
        self.metadata = {
            x: y for x, y in metadata.items() if y is not None and y != ""
        } if metadata else {}

    def search_kwargs(self) -> dict[str, t.Any]:
        search_kwargs = {}
        if self.wmo_id is not None:
            search_kwargs["wmo_id"] = self.wmo_id
        if self.wigos_id is not None:
            search_kwargs["wigos_id"] = self.wigos_id
        if self.platform_name is not None:
            search_kwargs["platform_name"] = self.platform_name
        if self.ship_code is not None:
            search_kwargs["ship_code"] = self.ship_code
        if self.platform_id is not None:
            search_kwargs["platform_id"] = self.platform_id
        return search_kwargs

    def update_platform(self, platform: NODBPlatform):
        if platform.wmo_id is None and self.wmo_id:
            platform.wmo_id = self.wmo_id
        if platform.wigos_id is None and self.wigos_id:
            platform.wigos_id = self.wigos_id
        if platform.platform_id is None and self.platform_id:
            platform.platform_id = self.platform_id
        if platform.platform_name is None and self.platform_name:
            platform.platform_name = self.platform_name
        if platform.service_start_date is None and self.start_date:
            platform.service_start_date = AwareDateTime.from_datetime(self.start_date)
        if platform.service_end_date is None and self.end_date:
            platform.service_end_date = AwareDateTime.from_datetime(self.end_date)
            if platform.status is PlatformStatus.ACTIVE:
                platform.status = PlatformStatus.HISTORICAL
        if platform.platform_type is None and self.platform_type:
            platform.platform_type = self.platform_type
        for key, value in self.metadata.items():
            if key not in platform.metadata:
                platform.metadata[key] = value
        if platform.imo_number is None and self.imo_number:
            platform.imo_number = self.imo_number
        if platform.mmsi is None and self.mmsi:
            platform.mmsi = self.mmsi
        if platform.registry_ca is None and self.registry_ca:
            platform.registry_ca = self.registry_ca
        if platform.country is None and self.country:
            platform.country = self.country

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
        pass

    def save_platform(self, info: PlatformInfo):
        search_kwargs = info.search_kwargs()
        if not search_kwargs:
            print("invalid platform, no identifiers, skipping")
            return
        platforms = [x for x in NODBPlatform.search(self.db, **search_kwargs) if self.service_overlaps(x, info)]
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
        platform = NODBPlatform()
        info.update_platform(platform)
        self.db.insert_object(platform)

    def update_platform(self, platform: NODBPlatform, info: PlatformInfo):
        info.update_platform(platform)
        self.db.update_object(platform)
