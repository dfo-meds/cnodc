import enum
import math
import pathlib
import typing as t

import yaml
from autoinject import injector

from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2 import BaseRecord, AbstractElement
from medsutil.ocproc2.codecs.ops import Instruction, SingleValueInstruction, OPSContext, EncodeDecodeGroup, \
    NoopInstruction, ElementInstruction
from medsutil.ocproc2.elements import SingleElement
from medsutil.ocproc2.structures import ParentRecord
from medsutil.ocproc2.codecs.meds.structs import StationRecord, MedsEncoding, SurfaceCodeGroup, SurfaceParameterGroup, \
    ProfileInfoGroup, ProfileRecord, ProfileLevelGroup, HistoryGroup
from medsutil.ocproc2.util import combine_quality_scores, find_quality_for_protocol
from medsutil.units import UnitConverter
from medsutil.ocproc2.history import HistoryEntry, ActionType


def extract_anemometer_height(e: SingleElement) -> int | None:
    if not e.is_numeric():
        return None
    v = e.to_int(no_loss=False)
    if e.metadata.best("SensorHeightReference", "") == "local_ground_corrected" and v == 10:
        return 999
    elif v < 999:
        return v
    else:
        return None


@injector.injectable_global
class MedsCodeMap:

    converter: UnitConverter = None

    @injector.construct
    def __init__(self):
        root = pathlib.Path(__file__).absolute().parent
        with open(root / "meds_map.yaml", "r") as h:
            raw = yaml.safe_load(h.read()) or {}
            self._meds_map: dict[str, dict] = {
                str(x): self.prestandardize_instruction(raw[x], pcode=str(x))
                for x in raw
            }
        self._instruction_cache: dict[str, Instruction] = {}

    def convert_source_code(self, source: str) -> str:
        if source.startswith("CA"):
            return "ME"

        if source.startswith("DE"):
            return "GE"

        if source.startswith("JP"):
            return "JA"

        if source == "AU-CSIRO":
            return "CS"
        if source.startswith("AU"):
            return "AD"

        if source == "US-SIO":
            return "SI"
        if source == "US-FNMOC":
            return "FN"
        if source == "US-AOML":
            return "AO"
        if source.startswith("US"):
            return "NO"

        return "  "


    def prestandardize_instruction(self,
                                   instruction: str | dict,
                                   pcode: str | None = None) -> dict:
        inst: dict
        if isinstance(instruction, str):
            if "/" in instruction:
                inst = {
                    "element": instruction,
                }
            else:
                inst = {
                    "value": instruction,
                }
        else:
            inst = instruction
        if pcode:
            inst["pcode"] = pcode
        if "data_type" not in inst:
            if "units" in inst and inst["units"]:
                inst["data_type"] = "float"
            else:
                inst["data_type"] = "string"
        if "meds_group" not in inst:
            inst["meds_group"] = "parameter"
        if "priority" not in inst:
            inst["priority"] = 0
        for key in ("ocproc2_export_processor", "import_processor", "export_processor"):
            if key in inst and inst[key].startswith("[MODULE]"):
                inst[key] = f"{__name__}{inst[key][8:]}"
        return inst

    def pcode_list_for_encode(self, is_surface: bool = False, parameters_only: bool = False) -> t.Iterable[str]:
        codes: list[tuple[str, int]] = []
        for pcode, entry in self._meds_map.items():
            # exclude any fake elements we built to help with decoding
            if len(pcode) != 4:
                continue
            if entry.get("deprecated", False):
                continue
            if parameters_only and entry.get("meds_group", "parameter") != "parameter":
                continue
            location = entry.get("location", "any")
            if location == "surface" and not is_surface:
                continue
            if location == "subsurface" and is_surface:
                continue
            codes.append((pcode, t.cast(int, entry.get("priority", 0))))
        codes.sort(key=lambda x: x[1])
        for code, _ in codes:
            yield code

    def lookup(self, pcode: str) -> Instruction:
        if pcode not in self._instruction_cache:
            if pcode not in self._meds_map:
                raise ValueError(f"No meds pcode instruction defined for [{pcode}]")
            self._instruction_cache[pcode] = Instruction.parse_instruction(self._meds_map[pcode])
        return self._instruction_cache[pcode]

    def find_instruction(self, element_name: str) -> Instruction | None:
        ...

    def convert_activity_code(self, action_type: ActionType | None) -> str | None:
        ...

    def convert_program_code(self, program_code: str) -> str:
        ...


class MedsConverter:

    PRECISION_BOUNDS = {
        # NB: I is explicitly stated as "greater than 0.2"
        # therefore I'm assuming that the ranges are (min, max]
        # i.e the minimum is excluded and the maximum is included
    #   PARAM   CODE    MIN     MAX     TYPE
        "PSAL": [
            (   "5",    0.001,  0.001,  'E'),
            (   "6",    0.0001, 0.0001, 'E'),

            (   "B",    0.001,  0.002,  'K'),
            (   "C",    0.002,  0.005,  'K'),
            (   "D",    0.005,  0.01,   'K'),
            (   "E",    0.01,   0.02,   'K'),
            (   "F",    0.02,   0.05,   'K'),
            (   "G",    0.05,   0.1,    'K'),
            (   "H",    0.1,    0.2,    'K'),
            (   "I",    0.2,    None,   'K'),

            (   "1",    None,   0.02,   'U'),
            (   "2",    0.02,   None,   'U'),
        ],
        "HCDT": [
            (   "1",    1,      1,      'E'),
            (   "2",    10,     10,     'E'),
            (   "5",    0.1,    0.1,    'E'),
            (   "6",    0.0001, 0.0001, 'E'),

            (   "B",    0.001,  0.002,  'K'),
            (   "C",    0.002,  0.005,  'K'),
            (   "D",    0.005,  0.01,   'K'),
            (   "E",    0.01,   0.02,   'K'),
            (   "F",    0.02,   0.05,   'K'),
            (   "G",    0.05,   0.1,    'K'),
            (   "H",    0.1,    0.2,    'K'),
            (   "I",    0.2,    None,   'K'),
        ],
        "CNDC": [
            (   "2",    0.1,    0.1,    'E'),
            (   "5",    0.001,  0.001,  'E'),
            (   "6",    0.0001, 0.0001, 'E'),

            (   "B",    0.001,  0.002,  'K'),
            (   "C",    0.002,  0.005,  'K'),
            (   "D",    0.005,  0.01,   'K'),
            (   "E",    0.01,   0.02,   'K'),
            (   "F",    0.02,   0.05,   'K'),
            (   "G",    0.05,   0.1,    'K'),
            (   "H",    0.1,    0.2,    'K'),
            (   "I",    0.2,    None,   'K'),
        ],
        "TRAN": [
            (   "2",    0.1,    0.1,    'E'),
            (   "3",    1,      1,      'E'),
            (   "5",    0.001,  0.001,  'E'),
            (   "6",    0.0001, 0.0001, 'E'),

            (   "B",    0.001,  0.002,  'K'),
            (   "C",    0.002,  0.005,  'K'),
            (   "D",    0.005,  0.01,   'K'),
            (   "E",    0.01,   0.02,   'K'),
            (   "F",    0.02,   0.05,   'K'),
            (   "G",    0.05,   0.1,    'K'),
            (   "H",    0.1,    0.2,    'K'),
            (   "I",    0.2,    None,   'K'),
        ],
        "HCSP": [
            (   "1",    1,      1,      'E'),
            (   "2",    0.1,    0.1,    'E'),
            (   "5",    0.001,  0.001,  'E'),
            (   "6",    0.0001, 0.0001, 'E'),

            (   "B",    0.001,  0.002,  'K'),
            (   "C",    0.002,  0.005,  'K'),
            (   "D",    0.005,  0.01,   'K'),
            (   "E",    0.01,   0.02,   'K'),
            (   "F",    0.02,   0.05,   'K'),
            (   "G",    0.05,   0.1,    'K'),
            (   "H",    0.1,    0.2,    'K'),
            (   "I",    0.2,    None,   'K'),
        ],
        "TEMP": [
            (   "2",    0.1,    0.1,    'E'),
            (   "5",    0.001,  0.001,  'E'),
            (   "6",    0.0001, 0.0001, 'E'),
            (   "B",    0.001,  0.002,  'K'),
            (   "C",    0.002,  0.005,  'K'),
            (   "D",    0.005,  0.01,   'K'),
            (   "E",    0.01,   0.02,   'K'),
            (   "F",    0.02,   0.05,   'K'),
            (   "G",    0.05,   0.1,    'K'),
            (   "H",    0.1,    0.2,    'K'),
            (   "I",    0.2,    None,   'K'),
            (   "1",    None,   0.01,   'U'),
        ],
        "_":    [
            (   "5",    0.001,  0.001,  'E'),
            (   "6",    0.0001, 0.0001, 'E'),

            (   "B",    0.001,  0.002,  'K'),
            (   "C",    0.002,  0.005,  'K'),
            (   "D",    0.005,  0.01,   'K'),
            (   "E",    0.01,   0.02,   'K'),
            (   "F",    0.02,   0.05,   'K'),
            (   "G",    0.05,   0.1,    'K'),
            (   "H",    0.1,    0.2,    'K'),
            (   "I",    0.2,    None,   'K'),

            (   "1",    None,   0.01,   'U'),
            (   "2",    0.01,   None,   'U'),
        ],

        "OSI$": [],
        "PRP$": [],
        "PRQ$": [],
    }

    code_map: MedsCodeMap = None

    @injector.construct
    def __init__(self):
        ...

    def ocproc2_to_station(self, record: ParentRecord) -> StationRecord:
        sr = StationRecord()

        context = OPSContext(record, test_protocols=["gtspp", "nodb"])

        time = record.coordinates.ideal("Time")
        if time and time.is_iso_datetime():
            sr.observation_time = time.to_datetime()
            sr.quality_datetime = context.get_quality(time)

        lat = record.coordinates.ideal("Latitude")
        lon = record.coordinates.ideal("Longitude")
        if lat and lon and lat.is_numeric() and lon.is_numeric():
            sr.coordinates = (lon.to_float("degrees_east"), lat.to_float("degrees_north"))
            sr.quality_position = context.get_quality(lat, lon)

        header = record.metadata.ideal("GTSHeader")

        if header:
            pieces = header.to_string().split(" ")
            sr.gts_header_info = pieces[0]
            sr.gts_source_node = pieces[1]
            if record.metadata.has_value("GTSHeaderFullDate"):
                if record.metadata["GTSHeaderFullDate"].is_iso_datetime():
                    sr.gts_bulletin_time = record.metadata["GTSHeaderFullDate"].to_datetime().strftime("%Y%m%d%H%M")


        sr.iumsgno = 0

        if record.metadata.has_value("MEDBArchiveAction"):
            sr.update_action = record.metadata["MEDBArchiveAction"].to_string()
        else:
            sr.update_action = "U"

        if record.metadata.has_value("MEDBArchiveSource"):
            sr.stream_source = record.metadata["MEDBArchiveSource"].to_string()
        else:
            sr.stream_source = "I"

        sr.data_availability = 'A'

        if record.metadata.has_value("MEDSCruiseID"):
            sr.cruise_id = record.metadata["MEDSCruiseID"].to_string()

        if record.metadata.has_value("MEDSStationNumber"):
            sr.station_id = record.metadata["MEDSStationNumber"].to_int()

        self._encode_surface_groups(sr, context)
        depth_pcodes = self._encode_profile_info_groups(sr, context)
        data_type = self.identify_data_type(record, depth_pcodes)
        sr.data_type = data_type
        source_name = self.code_map.convert_source_code(record.metadata.best("CNODCSource", coerce=str, default=""))
        sr.stream_identifier = f"{source_name}{data_type}"
        # note: maybe best to update this?
        sr.qc_version = "1.3"

        self._handle_buoy_eng_status(sr, context)

        for x in record.history.iterate_with_load():
            self.add_history(sr, x, record, context)

        return sr

    def add_history(self, sr: StationRecord, x: HistoryEntry, record: ParentRecord, context: OPSContext):
        code = self.code_map.convert_activity_code(x.action_type)
        if code is not None:
            hg = HistoryGroup()
            hg.organization = self.code_map.convert_source_code(x.organization.value)
            hg.program_code = self.code_map.convert_program_code(x.source_name)
            hg.program_version = x.source_version
            hg.action_date = AwareDateTime.fromisoformat(x.timestamp).strftime("%Y%m%d")
            hg.action_code = x.action_type
            hg.action_pcode = "RCRD"
            hg.previous_value = 9999.999
            hg.action_locator = 9999.999
            if x.affected_path:
                pcode, locator, previous = self.extract_history_location(record, x.affected_path, context)
                if pcode is not None:
                    hg.action_pcode = pcode
                if locator is not None:
                    hg.action_locator = locator
                if previous is not None:
                    hg.previous_value = previous
            sr.history_groups.append(hg)

    def extract_history_location(self, record: ParentRecord, path: str, context: OPSContext) -> tuple[str | None, float | None, float | None]:
        obj = record
        depth_or_pressure = None
        path_parts = path.split("/")
        element_names: list[tuple[str, float | None, AbstractElement | None]] = []
        previous_names: list[str] = []
        while obj is not None and path_parts:
            if isinstance(obj, BaseRecord):
                if obj.coordinates.has_value("Depth") and obj.coordinates["Depth"].is_numeric():
                    depth_or_pressure = obj.coordinates["Depth"].to_float("m")
                elif obj.coordinates.has_value("Pressure") and obj.coordinates["Pressure"].is_numeric():
                    depth_or_pressure = obj.coordinates["Pressure"].to_float("dbar")
            if isinstance(obj, AbstractElement):
                if len(previous_names) >= 2 and previous_names[-2] in ("coordinates", "parameters"):
                    element_names.append(("/".join(previous_names[-2:]), depth_or_pressure, obj.metadata.get("Unadjusted", default=None)))
            next_name = path_parts.pop(0)
            obj = obj.find_child([next_name])
            previous_names.append(next_name)
        if element_names:
            return self.correct_element_value(*element_names[-1], context=context)
        return None, None, None

    def correct_element_value(self, element_name: str, depth_or_pressure: float | None, element: AbstractElement | None, context: OPSContext) -> tuple[str | None, float | None, float | None]:
        instruction = self._normalize_encode_instruction(self.code_map.find_instruction(element_name), none_on_missing=True)
        if instruction is None:
            return None, None, None
        if "pcode" not in instruction.extras:
            return None, None, None
        previous = None
        if element is not None:
            if isinstance(instruction, ElementInstruction):
                previous, _, _, _ = instruction.process_element(element, context)
                if not isinstance(previous, float):
                    previous = None
        return instruction.extras["pcode"], depth_or_pressure, previous

    def identify_data_type(self, record: ParentRecord, depth_pcodes: set[str]) -> str:
        data_mode = record.metadata.best("CNODCDataMode", coerce=str, default="??")
        platform_type = record.metadata.best("CNODCPlatformType", coerce=str, default=None)
        if record.metadata.has_value("CNODCInstrumentTypes"):
            instrument_types = record.metadata["CNODCInstrumentTypes"].value
        else:
            instrument_types = []
        no_instruments = len(instrument_types)

        # TODO: More types? this should be good enough for NOAA output
        if data_mode == "RT":
            if platform_type == "drifting_buoy":
                return "DB"
            if "PSAL" in depth_pcodes:
                return "TE"
            if "TEMP" in depth_pcodes:
                return "BA"
        elif data_mode == "DM":
            if platform_type == "drifting_buoy":
                return "DD"
        return "??"

    def _handle_buoy_eng_status(self, sr: StationRecord, context: OPSContext):
        if context.record.metadata.has_value("BuoyEngineeringStatus"):
            val = context.record.metadata["BuoyEngineeringStatus"].to_string()
            idx = 1
            while idx < 4 and val:
                status_group = val[0:4]
                val = val[4:] if len(val) > 4 else ""
                val.ljust(4, "/")
                scg = SurfaceCodeGroup()
                scg.priority = 0
                scg.quality = 0
                scg.value = status_group
                scg.pcode = f"GE{idx}$"
                sr.surface_code_groups.append(scg)
                idx += 1

    def _encode_profile_info_groups(self, sr: StationRecord, context: OPSContext) -> set[str]:
        pcodes = set()
        if "PROFILE" in context.record.subrecords:
            for rs_idx, rs in context.record.subrecords["PROFILE"].items():
                with context.recordset_context(rs, "PROFILE"):
                    for pcode in self.code_map.pcode_list_for_encode(False, True):
                        prof_values, d_values, prof_priority, d_type = self._get_profile_values(pcode, context)
                        if prof_values:
                            pcodes.add(pcode)
                            self._encode_profile_info_group(pcode, prof_values, prof_priority, d_values, d_type, sr, context)
        return pcodes

    def _encode_profile_info_group(self,
                                   pcode: str,
                                   prof_values: list[tuple[t.SupportsFloat, int | None, float | None, float | None]],
                                   prof_priority: int,
                                   depth_values: list[tuple[float, int | None, float | None, float | None]],
                                   depth_type: str,
                                   sr: StationRecord,
                                   context: OPSContext):
        pig = ProfileInfoGroup()
        pig.profile_type = pcode
        pig.is_duplicate = False    #  I don't think we ever get a duplicate profile in the current system, but maybe we should consider?
        digit_indicator = context.recordset.metadata.best("DigitizationMethod", default=None, coerce=str)
        if digit_indicator == "inflection_points":
            pig.digitization_code = "8"
        else:
            pig.digitization_code = "7"
        pig.precision_code = self._get_precision_code(pcode, prof_values)
        pig.priority = prof_priority
        sr.profile_info_groups.append(pig)

        pr = ProfileRecord()
        pr.uses_pressure_levels = depth_type == "P"
        pig.profiles.append(pr)

        for idx in range(0, min(len(prof_values), len(depth_values))):
            if depth_values[idx][0] is not None:
                level = ProfileLevelGroup()
                level.depth_pressure = depth_values[idx][0]
                level.depth_quality = str(depth_values[idx][1] or 0)
                level.parameter_value = float(prof_values[idx][0])
                level.parameter_quality = str(prof_values[idx][1] or 0)
                pr.level_groups.append(level)

    def _get_precision_code(self,
                            pcode: str,
                            prof_values: list[tuple[t.SupportsFloat, int | None, float | None, float | None]]):
        if pcode not in self.PRECISION_BOUNDS:
            pcode = "_"
        worst_sigma, best_sigma = self._get_precision_bounds(prof_values)
        if worst_sigma is not None:
            worst_uniform = worst_sigma * math.sqrt(3) * 2
            best_uniform = None
            if best_sigma is not None:
                best_uniform = best_sigma * math.sqrt(3) * 2
            for code, min_val, max_val, code_type in self.PRECISION_BOUNDS[pcode]:
                if code_type == "E":
                    if best_uniform is not None and math.isclose(min_val, worst_uniform) and math.isclose(min_val, best_uniform):
                        return code
                else:
                    if code_type == "K" and best_uniform is None:
                        continue
                    # TODO: maybe should consider best_uniform here too? IDK. To revisit later.
                    # it should work for GTS reports at least.
                    if min_val is not None:
                        if worst_uniform < min_val:
                            continue
                    if max_val is not None:
                        if worst_uniform > max_val and not math.isclose(worst_uniform, max_val):
                            continue
                    return code
        return "4"

    def _get_precision_bounds(self, prof_values: list[tuple[t.Any, t.Any, float | None, float | None]]) -> tuple[float | None, float | None]:
        worst_sigmas = []
        best_sigmas = []
        for _, _, worst_sigma, best_sigma in prof_values:
            if worst_sigma is not None:
                worst_sigmas.append(worst_sigma)
            if best_sigma is not None:
                best_sigmas.append(best_sigma)
        if worst_sigmas:
            worst_sigma = max(worst_sigmas)
            best_sigmas = [x for x in best_sigmas if x < worst_sigma]
        else:
            worst_sigma = None
        if best_sigmas:
            best_sigma = min(best_sigmas)
        else:
            best_sigma = None
        return worst_sigma, best_sigma

    def _get_profile_values(self, pcode: str, context: OPSContext) -> tuple[list[tuple[t.SupportsFloat, int | None, float | None, float | None]], list[tuple[float, int | None, float | None, float | None]], int, str]:
        instruction = self._get_encode_instruction(pcode)
        depths = []
        n_with_depth = 0
        pressures = []
        n_with_pressure = 0
        values = []
        if instruction is not None:
            for record in context.recordset.records.iterate_with_load():
                with context.record_context(record):
                    value, quality, one_sigma, se = instruction.get_value_with_details(context)
                    if value is None:
                        continue
                    depth = None
                    depth_q = None
                    depth_s_worst = None
                    depth_s_best = None
                    pressure = None
                    pressure_q = None
                    pressure_s_worst = None
                    pressure_s_best = None
                    if record.coordinates.has_value("Depth") and not instruction.extras.get("skip_depth", False):
                        d = record.coordinates["Depth"].ideal()
                        depth = d.to_float("meters")
                        depth_q = context.get_quality(d)
                        _, depth_s_worst, depth_s_best = d.precision_information()
                        n_with_depth += 1
                    if record.coordinates.has_value("Pressure") and not instruction.extras.get("skip_pressure", False):
                        p = record.coordinates["Pressure"].ideal()
                        pressure = p.to_float("dbar")
                        pressure_q = context.get_quality(p)
                        _, pressure_s_worst, pressure_s_best = d.precision_information()
                        n_with_pressure += 1
                    if depth is not None or pressure is not None:
                        depths.append((depth, depth_q, depth_s_worst, depth_s_best))
                        pressures.append((pressure, pressure_q, pressure_s_worst, pressure_s_best))
                        values.append((value, quality, one_sigma, se))
        if n_with_depth >= n_with_pressure:
            return values, depths, instruction.extras.get("priority", 0), "D"
        else:
            return values, pressures, instruction.extras.get("priority", 0), "P"

    def _get_encode_instruction(self, pcode: str) -> SingleValueInstruction | None:
        return self._normalize_encode_instruction(self.code_map.lookup(pcode))

    def _normalize_encode_instruction(self, instruction, none_on_missing: bool = False) -> SingleValueInstruction | None:
        if isinstance(instruction, EncodeDecodeGroup):
            instruction = instruction.get_instruction(True)
        if isinstance(instruction, SingleValueInstruction):
            return instruction
        if isinstance(instruction, NoopInstruction):
            return None
        if none_on_missing:
            return None
        raise ValueError(f"Unrecognized instruction: [{instruction.__class__}]")

    def _encode_surface_groups(self, sr: StationRecord, context: OPSContext):
        for pcode in self.code_map.pcode_list_for_encode(True):
            instruction = self._get_encode_instruction(pcode)
            if instruction is None:
                continue
            priority = instruction.extras.get("priority", 0)
            group = instruction.extras.get("meds_group", "parameter")
            value, quality, _, _ = instruction.get_value_with_details(context)
            if value is None:
                continue
            if group == "code":
                scg = SurfaceCodeGroup()
                scg.pcode = pcode
                scg.value = str(value)
                scg.quality = str(quality or 0)
                scg.priority = priority

                sr.surface_code_groups.append(scg)
            else:
                spg = SurfaceParameterGroup()
                spg.pcode = pcode
                spg.value = float(value)
                spg.quality = str(quality or 0)
                spg.priority = priority
                sr.surface_parameter_groups.append(spg)

    def station_to_ocproc2(self, station: StationRecord, encoding: MedsEncoding) -> ParentRecord:
        pr = ParentRecord()
        pr.coordinates["Time"] = SingleElement(
            station.observation_time,
            DatePrecision="minute",
            Quality=SingleElement(
                station.quality_datetime,
                TestProtocol="meds"
            ),
        )
        pr.coordinates["Longitude"] = SingleElement(
            station.longitude,
            Units="degrees_east",
            Quality=SingleElement(
                station.quality_position,
                TestProtocol="meds"
            ),
            Uncertainty=SingleElement(
                "0.00005" if encoding is MedsEncoding.MEDS_ASCII else (station.longitude * (2 ** -24)),
                UncertaintyType="uniform"
            )
        )
        pr.coordinates["Latitude"] = SingleElement(
            station.latitude,
            Units="degrees_north",
            Quality=SingleElement(
                station.quality_position,
                TestProtocol="meds"
            ),
            Uncertainty=SingleElement(
                "0.00005" if encoding is MedsEncoding.MEDS_ASCII else (station.longitude * (2 ** -24)),
                UncertaintyType="uniform"
            )
        )
        if station.gts_bulletin_time or station.gts_header_info or station.gts_source_node:
            # TODO: should we format bulletin time properly?
            pr.metadata["GTSHeader"] = f"{station.gts_header_info or ""} {station.gts_source_node or ""} {station.gts_bulletin_time or ""}"

        # TODO: surface group decode
        # TODO: profile decode

        # TODO: custom import for GE1$ through GE3$

        return pr

    SPLIT_CODES: dict[str, list[tuple[int, int, str]]] = {
        # TESAC 66...
        "GGC$": [
            (2, 3, "GGK6"),
            (3, 4, "GGEC"),
            (4, 5, "GGCD"),
        ],
        # DRIBU 1 Qpressure Qhousekeeping Qwatertemp Qairtemp
        "GIN$": [
            (1, 2, "_GIN$1"),
            (2, 3, "_GIN$2"),
            (3, 4, "_GIN$3"),
            (4, 5, "_GIN$4"),
        ],
        # 1770 / 4770
        "PFR$": [
            (0, 3, ""),  # TODO, when we figure out what code this is
            (3, 5, "RCT$")
        ]

    }

    def _handle_ggi(self, value: str, context: OPSContext):
        # BATHY/TESAC 88...
        value = value.ljust(5, "/")

        # bathy this is 8 8 8 8 k1 (k1 is digitization method 7 8)
        if value and value[-1] in ("7", "8") and value[-2] == "8":
            self._handle_split_code(value, codes=[(4, 5, "GGDI")], context=context)

        # tesac this is 8 8 8 k1 k2 (k2 is salinity depth which is 0-3)
        else:
            self._handle_split_code(value, codes=[
                (3, 4, "GGDI"),
                (4, 5, "GGSL"),
            ], context=context)

    def _handle_split_code(self, value: str, codes: list[tuple[int, int, str]], context: OPSContext):
        value = value.ljust(5, "/")
        for start_idx, end_idx, pcode in codes:
            instruction = self._get_encode_instruction(pcode)
            if instruction is not None:
                instruction.set_value(value[start_idx:end_idx], None, context)
