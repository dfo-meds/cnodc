import math
import typing as t
import struct
from osdt.common import BufferedBinaryReader
import enum
import datetime
import logging


class MedsEncoding(enum.Enum):
    OCPROC = 1
    MEDS_ASCII = 2


class _MedsEncodable:
    OCPROC_LEN: int
    OCPROC_STRUCT: struct.Struct
    OCPROC_PAD: list

    MEDSASC_LEN: int
    MEDSASC_STRUCT: struct.Struct
    MEDSASC_PAD: list

    DEFAULT_RECORD: list

    def __init__(self, unpacked_record: t.Union[list, tuple, None] = None):
        self.raw_values: list[t.Any] = []
        if unpacked_record is not None:
            self.raw_values = self.unpack_values([self._pre_unpack(x) for x in unpacked_record])
        else:
            self.raw_values = self.__class__.DEFAULT_RECORD.copy()

    @staticmethod
    def sort_and_cap(list_: list, max_size: int, obj_type: str, record_id: str):
        if len(list_) <= max_size:
            yield from list_
        else:
            logging.getLogger("osdt.meds").warning(f"Truncating objects of type {obj_type} for {record_id}")
            for idx, rec in enumerate(sorted(list_, key=lambda x: x.priority, reverse=True)):
                if idx > max_size:
                    break
                yield rec

    def unpack_values(self, values: list):
        return list(values)

    def _pre_unpack(self, v):
        if isinstance(v, bytes):
            return v.decode('ascii').strip(' ')
        return v

    def pack_values(self, enc: MedsEncoding) -> tuple:
        pad_ref = self.__class__.MEDSASC_PAD if enc == MedsEncoding.MEDS_ASCII else self.__class__.OCPROC_PAD
        return tuple([
            self._pack(val, pad_ref[idx] if idx < len(pad_ref) else None)
            for idx, val in enumerate(self.raw_values)
        ])

    def _pack(self, val, pad_ref: t.Optional[list[t.Any, str, str]]):
        if val is None:
            val = ''
        if pad_ref is None:
            if isinstance(val, str):
                return val.encode('ascii')
            return val
        if isinstance(pad_ref, int):
            pad_ref = [pad_ref]
        l = len(pad_ref)
        if pad_ref[0] is not None:
            val = str(val)
            if len(val) >= pad_ref[0]:
                val = val[:pad_ref[0]]
            else:
                spacer = pad_ref[1] if l >= 2 else ' '
                side = pad_ref[2] if l >= 3 else 'L'
                while len(val) < pad_ref[0]:
                    val = f"{spacer}{val}" if side == 'L' else f"{val}{spacer}"
        return val.encode('ascii')

    def encode(self, enc: MedsEncoding) -> t.Iterable[bytes]:
        if enc == MedsEncoding.MEDS_ASCII:
            yield self.__class__.MEDSASC_STRUCT.pack(*self.pack_values(enc))
        else:
            yield self.__class__.OCPROC_STRUCT.pack(*self.pack_values(enc))

    @classmethod
    def decode(cls, data: BufferedBinaryReader, enc: MedsEncoding):
        if enc == MedsEncoding.MEDS_ASCII:
            return cls(unpacked_record=cls.MEDSASC_STRUCT.unpack(data.consume(cls.MEDSASC_LEN)))
        else:
            return cls(unpacked_record=cls.OCPROC_STRUCT.unpack(data.consume(cls.OCPROC_LEN)))


class ProfileLevelGroup(_MedsEncodable):
    OCPROC_LEN = 10
    OCPROC_STRUCT = struct.Struct("<fcfc")
    OCPROC_PAD = []

    MEDSASC_LEN = 17
    MEDSASC_STRUCT = struct.Struct("<6sc9sc")
    MEDSASC_PAD = [6, None, 9]

    DEFAULT_RECORD = [None, "0", None, "0"]

    @property
    def depth_pressure(self) -> float:
        return self.raw_values[0]

    @depth_pressure.setter
    def depth_pressure(self, val: t.Union[float, str]):
        self.raw_values[0] = float(val)

    @property
    def depth_quality(self) -> str:
        return self.raw_values[1]

    @depth_quality.setter
    def depth_quality(self, val: str):
        self.raw_values[1] = val

    @property
    def parameter_value(self) -> float:
        return self.raw_values[2]

    @parameter_value.setter
    def parameter_value(self, val: t.Union[float, str]):
        self.raw_values[2] = float(val)

    @property
    def parameter_quality(self) -> str:
        return self.raw_values[3]

    @parameter_quality.setter
    def parameter_quality(self, val: str):
        self.raw_values[3] = val

    def unpack_values(self, values: tuple):
        return [
            float(values[0].strip(" ")),
            values[1],
            float(values[2].strip(" ")),
            values[3]
        ]


class ProfileRecord(_MedsEncodable):
    OCPROC_LEN = 53
    OCPROC_STRUCT = struct.Struct("<8si10s4s2s2s4s2si4s2sic")
    OCPROC_PAD = [(8, "0"), None, 10, None, None, None, None, None, None, None, (2, '0'), None]

    MEDSASC_LEN = 63
    MEDSASC_STRUCT = struct.Struct("<8s8s10s4s2s2s4s2s12s4s2s4s1s")
    MEDSASC_PAD = [(8, "0"), 8, 10, None, None, None, None, None, 12, None, (2, '0'), 4, None]

    DEFAULT_RECORD = [None, None, None, None, None, None, None, None, None, None, None, 0, None]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.level_groups: list[ProfileLevelGroup] = []

    @property
    def _mkey(self) -> str:
        return self.raw_values[0]

    @_mkey.setter
    def _mkey(self, key: str):
        self.raw_values[0] = str(key)

    @property
    def _meds_1d_sqr(self) -> str:
        return self.raw_values[1]

    @_meds_1d_sqr.setter
    def _meds_1d_sqr(self, sq_id: str):
        self.raw_values[1] = str(sq_id)

    @property
    def _cruise_id(self) -> str:
        return self.raw_values[2]

    @_cruise_id.setter
    def _cruise_id(self, val: str):
        self.raw_values[2] = str(val)

    @property
    def _observation_time(self) -> datetime.datetime:
        return datetime.datetime(
            int(self.raw_values[3]),
            int(self.raw_values[4]),
            int(self.raw_values[5]),
            int(self.raw_values[6][0:2]),
            int(self.raw_values[6][2:4])
        )

    @_observation_time.setter
    def _observation_time(self, dt: t.Union[datetime.datetime, str]):
        if isinstance(dt, str):
            dt = datetime.datetime.fromisoformat(dt)
        self.raw_values[3] = str(dt.year).zfill(4)
        self.raw_values[4] = str(dt.month).zfill(2)
        self.raw_values[5] = str(dt.day).zfill(2)
        self.raw_values[6] = f"{str(dt.hour).zfill(2)}{str(dt.minute).zfill(2)}"

    @property
    def _data_type(self) -> str:
        return self.raw_values[7]

    @_data_type.setter
    def _data_type(self, val: str):
        self.raw_values[7] = str(val)

    @property
    def _iumsgno(self) -> str:
        return self.raw_values[8]

    @_iumsgno.setter
    def _iumsgno(self, val: str):
        self.raw_values[8] = str(val)

    @property
    def _profile_type(self) -> str:
        return self.raw_values[9]

    @_profile_type.setter
    def _profile_type(self, val: str):
        self.raw_values[9] = val

    @property
    def _profile_segment(self) -> int:
        return int(self.raw_values[10])

    @_profile_segment.setter
    def _profile_segment(self, val: int):
        self.raw_values[10] = str(val)

    @property
    def uses_pressure_levels(self) -> bool:
        return self.raw_values[12] == 'P'

    @uses_pressure_levels.setter
    def uses_pressure_levels(self, val: bool):
        self.raw_values[12] = 'P' if val else 'D'

    @property
    def _no_depths(self) -> int:
        return self.raw_values[11]

    @_no_depths.setter
    def _no_depths(self, val: int):
        self.raw_values[11] = int(val)

    def unpack_values(self, values: tuple):
        return [
            str(values[0]).lstrip("0"),
            int(values[1].strip()),
            *values[2:8],
            int(values[8]),
            *values[9:11],
            int(values[11]),
            values[12]
        ]

    def encode(self, enc: MedsEncoding) -> bytes:
        self._no_depths = len(self.level_groups)
        yield from super().encode(enc)
        for plg in self.level_groups:
            yield from plg.encode(enc)
        if enc == MedsEncoding.MEDS_ASCII:
            yield "\n".encode("ascii")

    @classmethod
    def decode(cls, data: BufferedBinaryReader, enc: MedsEncoding):
        profile: ProfileRecord = super().decode(data, enc)
        for _ in range(0, profile._no_depths):
            profile.level_groups.append(ProfileLevelGroup.decode(data, enc))
        return profile


class ProfileInfoGroup(_MedsEncodable):
    OCPROC_LEN = 13
    OCPROC_STRUCT = struct.Struct("<h4s3cf")
    OCPROC_MAX = [32767, 32767]
    OCPROC_PAD = []

    MEDSASC_LEN = 14
    MEDSASC_STRUCT = struct.Struct("<2s4s3c5s")
    MEDSASC_MAX = [99, 1500]
    MEDSASC_PAD = [(2, '0'), None, None, None, None, 5]

    DEFAULT_RECORD = [None, None, 'N', None, None, None]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.profiles: list[ProfileRecord] = []
        self.priority = 0

    def normalize_group(self, record_no, current_idx, worst_quality, station, fmt: MedsEncoding):
        mx = ProfileInfoGroup.OCPROC_MAX if fmt == MedsEncoding.OCPROC else ProfileInfoGroup.MEDSASC_MAX
        _prof = []
        for p in self.profiles:
            if len(p.level_groups) <= mx[1]:
                _prof.append(p)
            else:
                for i in range(0, math.ceil(len(p.level_groups) / mx[1])):
                    new_p = ProfileRecord(p.raw_values)
                    new_p.level_groups = p.level_groups[(i*mx[1]):((i+1)*mx[1])]
                    _prof.append(new_p)
        self.profiles = _prof
        self._no_seg = min(mx[0], len(self.profiles))
        max_depth = 0
        for seg_no, prof in enumerate(self.profiles, start=1):
            current_idx += 1
            prof._mkey = str((record_no * 100) + current_idx).zfill(8)
            prof._cruise_id = station.cruise_id
            prof._meds_1d_sqr = station._meds_1d_sqr
            prof._observation_time = station.observation_time
            prof._data_type = station.data_type
            prof._iumsgno = station.iumsgno
            prof._profile_type = self.profile_type
            prof._profile_segment = seg_no
            prof._no_depths = len(prof.level_groups)
            for obs in prof.level_groups:
                if obs.depth_pressure > max_depth:
                    max_depth = obs.depth_pressure
                for qual in [int(obs.depth_quality), int(obs.parameter_quality)]:
                    if 4 >= qual > worst_quality:
                        worst_quality = qual
        self._deepest_depth = max_depth
        return current_idx, worst_quality

    def encode_all_profile_records(self, fmt: MedsEncoding):
        mx = ProfileInfoGroup.OCPROC_MAX if fmt == MedsEncoding.OCPROC else ProfileInfoGroup.MEDSASC_MAX
        for prof in _MedsEncodable.sort_and_cap(self.profiles, mx[1], 'profile_records', self.profile_type):
            yield from prof.encode(fmt)

    @property
    def _no_seg(self) -> int:
        return self.raw_values[0]

    @_no_seg.setter
    def _no_seg(self, v: int):
        self.raw_values[0] = int(v)

    @property
    def profile_type(self) -> str:
        return self.raw_values[1]

    @profile_type.setter
    def profile_type(self, v: str):
        self.raw_values[1] = str(v)

    @property
    def is_duplicate(self) -> bool:
        return self.raw_values[2] == 'Y'

    @is_duplicate.setter
    def is_duplicate(self, val: bool):
        self.raw_values[2] = 'N' if val in (False, 'N') else 'Y'

    @property
    def digitization_code(self) -> str:
        return self.raw_values[3]

    @digitization_code.setter
    def digitization_code(self, v: str):
        self.raw_values[3] = str(v)

    @property
    def precision_code(self) -> str:
        return self.raw_values[4]

    @precision_code.setter
    def precision_code(self, v: str):
        self.raw_values[4] = v

    @property
    def _deepest_depth(self) -> float:
        return self.raw_values[5]

    @_deepest_depth.setter
    def _deepest_depth(self, val: float):
        self.raw_values[5] = float(val)

    def unpack_values(self, values: tuple):
        return [
            int(values[0]),
            *values[1:5],
            float(values[5])
        ]


class SurfaceParameterGroup(_MedsEncodable):
    OCPROC_LEN = 9
    OCPROC_STRUCT = struct.Struct("<4sfc")
    OCPROD_PAD = []

    MEDSASC_LEN = 15
    MEDSASC_STRUCT = struct.Struct("<4s10sc")
    MEDSASC_PAD = [None, 10]

    DEFAULT_RECORD = [None, None, "0"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.priority = 0

    @property
    def pcode(self) -> str:
        return self.raw_values[0]

    @pcode.setter
    def pcode(self, val: str):
        self.raw_values[0] = str(val)

    @property
    def value(self) -> float:
        return self.raw_values[1]

    @value.setter
    def value(self, val: float):
        self.raw_values[1] = float(val)

    @property
    def quality(self) -> str:
        return self.raw_values[2]

    @quality.setter
    def quality(self, qual: str):
        self.raw_values[2] = qual

    def unpack_values(self, values: tuple):
        return [
            values[0], float(values[1]), values[2]
        ]


class SurfaceCodeGroup(_MedsEncodable):
    OCPROC_LEN = 15
    OCPROC_STRUCT = struct.Struct("<4s10sc")
    OCPROC_PAD = [None, 10]

    MEDSASC_LEN = 15
    MEDSASC_STRUCT = struct.Struct("<4s10sc")
    MEDSASC_PAD = [None, 10]

    DEFAULT_RECORD = [None, None, "0"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.priority = 0

    @property
    def pcode(self) -> str:
        return self.raw_values[0]

    @pcode.setter
    def pcode(self, val: str):
        self.raw_values[0] = str(val)

    @property
    def value(self) -> str:
        return self.raw_values[1]

    @value.setter
    def value(self, val: str):
        self.raw_values[1] = str(val)

    @property
    def quality(self) -> str:
        return self.raw_values[2]

    @quality.setter
    def quality(self, qual: str):
        self.raw_values[2] = qual


class HistoryGroup(_MedsEncodable):
    OCPROC_LEN = 28
    OCPROC_STRUCT = struct.Struct("<2s4s4si2s4s2f")
    OCPROC_PAD = [2, 4, 4, None, 2, 4]

    MEDSASC_LEN = 42
    MEDSASC_STRUCT = struct.Struct("<2s4s4s8s2s4s8s10s")
    MEDSASC_PAD = [2, 4, 4, 8, 2, 4, 8, 10]

    DEFAULT_RECORD = [None, None, None, None, None, None, None, None]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.priority = 0

    @property
    def organization(self) -> str:
        return self.raw_values[0]

    @organization.setter
    def organization(self, val: str):
        self.raw_values[0] = str(val)

    @property
    def program_code(self) -> str:
        return self.raw_values[1]

    @program_code.setter
    def program_code(self, val: str):
        self.raw_values[1] = str(val)

    @property
    def program_version(self) -> str:
        return self.raw_values[2]

    @program_version.setter
    def program_version(self, val: str):
        self.raw_values[2] = str(val)

    @property
    def action_date(self) -> datetime.date:
        return datetime.date(
            int(self.raw_values[3][0:4]),
            int(self.raw_values[3][4:6]),
            int(self.raw_values[3][6:8])
        )

    @action_date.setter
    def action_date(self, dt: t.Union[datetime.date, datetime.datetime, str]):
        if isinstance(dt, str):
            self.raw_values[3] = dt
        else:
            self.raw_values[3] = dt.strftime("%Y%m%d")

    @property
    def action_code(self) -> str:
        return self.raw_values[4]

    @action_code.setter
    def action_code(self, val: str):
        self.raw_values[4] = str(val)

    @property
    def action_pcode(self) -> str:
        return self.raw_values[5]

    @action_pcode.setter
    def action_pcode(self, val: str):
        self.raw_values[5] = str(val)

    @property
    def action_locator(self) -> float:
        return self.raw_values[6]

    @action_locator.setter
    def action_locator(self, val: float):
        self.raw_values[6] = float(val)

    @property
    def previous_value(self) -> float:
        return self.raw_values[7]

    @previous_value.setter
    def previous_value(self, val: float):
        self.raw_values[7] = float(val)

    def unpack_values(self, values: tuple):
        return [
            *values[0:3],
            str(values[3]),
            *values[4:6],
            float(values[6]),
            float(values[7])
        ]


class StationRecord(_MedsEncodable):
    OCPROC_LEN = 102
    OCPROC_STRUCT = struct.Struct("<8si10s4s2s2s4s2si2ch2f3s8s12s6s4s4s4sc4h")
    OCPROC_MAX_SIZES = [32767, 32767, 32767, 32767]
    OCPROC_PAD = [(8, "0"), None, 10, None, None, None, None, None, None, None, None, None, None, None, None,  # long
                  None, None, None, None, None, None, None, None, 4, None,  # avail
                  None, None, None, None
                  ]

    MEDSASC_LEN = 130
    MEDSASC_STRUCT = struct.Struct("<8s8s10s4s2s2s4s2s12s2c8s8s9s3c8s12s6s4s4s4s1s2s2s2s3s")
    MEDSASC_MAX_SIZES = [30, 30, 30, 100]
    MEDSASC_PAD = [(8, "0"), 8, 10, (4, "0"), (2, "0"), (2, "0"), (4, "0"), None, 12, None, None, 8, 8, 9,  # long
                   None, None, None, None, None, None, None, None, 4, None,  # avail
                   (2, ' '), (2, ' '), (2, ' '), (3, ' ')
                   ]

    DEFAULT_RECORD = [None, None, None, None, None, None, None, None, None, None, None, None, None, None, "0",
                      "0", None, None, None, None, None, None, None, None, 0, 0, 0, 0]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.profile_info_groups: list[ProfileInfoGroup] = []
        self.surface_parameter_groups: list[SurfaceParameterGroup] = []
        self.surface_code_groups: list[SurfaceCodeGroup] = []
        self.history_groups: list[HistoryGroup] = []
        self.scratch = {}

    @property
    def _mkey(self) -> str:
        return self.raw_values[0]

    @_mkey.setter
    def _mkey(self, key: str):
        self.raw_values[0] = str(key)

    @property
    def _meds_1d_sqr(self) -> str:
        return self.raw_values[1]

    @_meds_1d_sqr.setter
    def _meds_1d_sqr(self, sq_id: str):
        self.raw_values[1] = str(sq_id)

    @property
    def cruise_id(self) -> str:
        return self.raw_values[2]

    @cruise_id.setter
    def cruise_id(self, val: str):
        self.raw_values[2] = str(val)

    @property
    def observation_time(self) -> datetime.datetime:
        return datetime.datetime(
            int(self.raw_values[3]),
            int(self.raw_values[4]),
            int(self.raw_values[5]),
            int(self.raw_values[6][0:2]),
            int(self.raw_values[6][2:4])
        )

    @observation_time.setter
    def observation_time(self, dt: t.Union[datetime.datetime, str]):
        if not isinstance(dt, datetime.datetime):
            dt = datetime.datetime.fromisoformat(dt)
        self.raw_values[3] = str(dt.year)
        self.raw_values[4] = str(dt.month).zfill(2)
        self.raw_values[5] = str(dt.day).zfill(2)
        self.raw_values[6] = f"{str(dt.hour).zfill(2)}{str(dt.minute).zfill(2)}"

    @property
    def data_type(self) -> str:
        return self.raw_values[7]

    @data_type.setter
    def data_type(self, val: str):
        self.raw_values[7] = str(val)

    @property
    def iumsgno(self) -> str:
        return self.raw_values[8]

    @iumsgno.setter
    def iumsgno(self, val: str):
        self.raw_values[8] = str(val)

    @property
    def stream_source(self) -> str:
        return self.raw_values[9]

    @stream_source.setter
    def stream_source(self, val: str):
        self.raw_values[9] = val

    @property
    def update_action(self) -> str:
        return self.raw_values[10]

    @update_action.setter
    def update_action(self, val: str):
        self.raw_values[10] = val

    @property
    def station_number(self):
        return self.raw_values[11]

    @station_number.setter
    def station_number(self, val: str):
        self.raw_values[11] = val

    @property
    def _latitude(self) -> float:
        return self.raw_values[12]

    @_latitude.setter
    def _latitude(self, val: float):
        self.raw_values[12] = float(val)

    @property
    def _longitude(self) -> float:
        return self.raw_values[13]

    @_longitude.setter
    def _longitude(self, val: float):
        self.raw_values[13] = float(val)

    @property
    def coordinates(self) -> tuple[float, float]:
        return self._longitude, self._latitude

    @coordinates.setter
    def coordinates(self, long_lat: tuple):
        long, lat = long_lat
        self._meds_1d_sqr = f"{int(math.ceil(long))}{str(int(math.ceil(lat))).zfill(3)}"
        self._latitude = lat
        self._longitude = long

    @property
    def quality_position(self) -> str:
        return self.raw_values[14]

    @quality_position.setter
    def quality_position(self, val: str):
        self.raw_values[14] = str(val)

    @property
    def quality_datetime(self) -> str:
        return self.raw_values[15]

    @quality_datetime.setter
    def quality_datetime(self, val: str):
        self.raw_values[15] = str(val)

    @property
    def _quality_worst_record(self) -> str:
        return self.raw_values[16]

    @_quality_worst_record.setter
    def _quality_worst_record(self, val: str):
        self.raw_values[16] = str(val)

    @property
    def update_date(self) -> datetime.date:
        return datetime.date(
            int(self.raw_values[17][0:4]),
            int(self.raw_values[17][4:6]),
            int(self.raw_values[17][6:8])
        )

    @update_date.setter
    def update_date(self, val: t.Union[str, datetime.datetime, datetime.date]):
        if isinstance(val, str):
            val = datetime.datetime.fromisoformat(val)
        self.raw_values[17] = val.strftime("%Y%m%d")

    @property
    def gts_bulletin_time(self) -> datetime.datetime:
        return datetime.datetime(
            int(self.raw_values[18][0:4]),
            int(self.raw_values[18][4:6]),
            int(self.raw_values[18][6:8]),
            int(self.raw_values[18][8:10]),
            int(self.raw_values[18][10:12])
        )

    @gts_bulletin_time.setter
    def gts_bulletin_time(self, val: t.Union[str, datetime.datetime]):
        if isinstance(val, str):
            val = datetime.datetime.fromisoformat(val)
        self.raw_values[18] = val.strftime("%Y%m%d%H%M")

    @property
    def gts_header_info(self) -> str:
        return self.raw_values[19]

    @gts_header_info.setter
    def gts_header_info(self, val: str):
        self.raw_values[19] = str(val)

    @property
    def gts_source_node(self) -> str:
        return self.raw_values[20]

    @gts_source_node.setter
    def gts_source_node(self, val: str):
        self.raw_values[20] = str(val)

    @property
    def stream_identifier(self) -> str:
        return self.raw_values[21]

    @stream_identifier.setter
    def stream_identifier(self, val: str):
        self.raw_values[21] = str(val)

    @property
    def qc_version(self) -> str:
        return self.raw_values[22]

    @qc_version.setter
    def qc_version(self, val: str):
        self.raw_values[22] = str(val)

    @property
    def data_availability(self) -> str:
        return self.raw_values[23]

    @data_availability.setter
    def data_availability(self, val: str):
        self.raw_values[23] = str(val)

    @property
    def _no_prof(self) -> int:
        return self.raw_values[24]

    @_no_prof.setter
    def _no_prof(self, val: int):
        self.raw_values[24] = int(val)

    @property
    def _nparms(self) -> int:
        return self.raw_values[25]

    @_nparms.setter
    def _nparms(self, val: int):
        self.raw_values[25] = int(val)

    @property
    def _sparms(self) -> int:
        return self.raw_values[26]

    @_sparms.setter
    def _sparms(self, val: int):
        self.raw_values[26] = val

    @property
    def _num_hists(self) -> int:
        return self.raw_values[27]

    @_num_hists.setter
    def _num_hists(self, val: int):
        self.raw_values[26] = val

    def unpack_values(self, values: tuple):
        return [
            values[0],
            int(values[1]),
            *values[2:8],
            int(values[8]),
            *values[9:11],
            int(values[11]),
            float(values[12]),
            float(values[13]),
            *values[14:24],
            *[int(x) for x in values[24:28]]
        ]

    def validate_record(self, record_no, fmt: MedsEncoding):
        if record_no >= 1000000:
            raise ValueError("Can only encode 999,999 station records in a single MEDS ASCII/OCPROC file")

    def encode(self, fmt: MedsEncoding, record_no: int = 1) -> t.Iterable[bytes]:
        self.validate_record(record_no, fmt)
        self._meds_1d_sqr = f"{int(math.ceil(self._longitude+180))}{str(int(math.ceil(self._latitude+90))).zfill(3)}"
        max_sizes = StationRecord.OCPROC_MAX_SIZES if fmt == MedsEncoding.OCPROC else StationRecord.MEDSASC_MAX_SIZES
        self._no_prof = min(max_sizes[0], len(self.profile_info_groups))
        self._nparms = min(max_sizes[1], len(self.surface_parameter_groups))
        self._sparms = min(max_sizes[2], len(self.surface_code_groups))
        self._num_hists = min(max_sizes[3], len(self.history_groups))
        self._mkey = str(record_no * 100).zfill(8)
        worst_record = 0
        for scg in self.surface_code_groups:
            qual = int(scg.quality)
            if 4 >= qual > worst_record:
                worst_record = qual
        for spg in self.surface_parameter_groups:
            qual = int(spg.quality)
            if 4 >= qual > worst_record:
                worst_record = qual
        idx = 0
        for pg in self.profile_info_groups:
            idx, worst_record = pg.normalize_group(record_no, idx, worst_record, self, fmt)
        self._quality_worst_record = worst_record
        yield from super().encode(fmt)
        report_profiles = []
        for pg in _MedsEncodable.sort_and_cap(self.profile_info_groups, max_sizes[0], "profile_info", self._mkey):
            yield from pg.encode(fmt)
            report_profiles.append(pg)
        for spg in _MedsEncodable.sort_and_cap(self.surface_parameter_groups, max_sizes[1], "surface_params", self._mkey):
            yield from spg.encode(fmt)
        for scg in _MedsEncodable.sort_and_cap(self.surface_code_groups, max_sizes[2], "surface_codes", self._mkey):
            yield from scg.encode(fmt)
        for hg in _MedsEncodable.sort_and_cap(self.history_groups, max_sizes[3], "history_groups", self._mkey):
            yield from hg.encode(fmt)
        if fmt == MedsEncoding.MEDS_ASCII:
            yield "\n".encode("ascii")
        for pg in report_profiles:
            yield from pg.encode_all_profile_records(fmt)

    @classmethod
    def decode(cls, data: BufferedBinaryReader, fmt: MedsEncoding):
        station: StationRecord = super().decode(data, fmt)
        for _ in range(0, station._no_prof):
            station.profile_info_groups.append(ProfileInfoGroup.decode(data, fmt))
        for _ in range(0, station._nparms):
            station.surface_parameter_groups.append(SurfaceParameterGroup.decode(data, fmt))
        for _ in range(0, station._sparms):
            station.surface_code_groups.append(SurfaceCodeGroup.decode(data, fmt))
        for _ in range(0, station._num_hists):
            station.history_groups.append(HistoryGroup.decode(data, fmt))
        for profile_info in station.profile_info_groups:
            for _ in range(0, profile_info._no_seg):
                data.skip_bytes(b"\r\n")
                profile_info.profiles.append(ProfileRecord.decode(data, fmt))
        return station


def unpack(data: BufferedBinaryReader, enc: MedsEncoding) -> t.Iterable[StationRecord]:
    while not data.is_at_end():
        sr = StationRecord.decode(data, enc)
        if sr is not None:
            yield sr
        if enc == MedsEncoding.MEDS_ASCII:
            data.skip_bytes(b"\r\n")


def pack(station_records: t.Iterable[StationRecord], enc: MedsEncoding) -> t.Iterable[bytes]:
    for idx, station_record in enumerate(station_records, start=1):
        yield from station_record.encode(enc, idx)
