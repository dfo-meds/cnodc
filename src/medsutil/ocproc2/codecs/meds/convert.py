import pathlib
import typing as t

import yaml
from autoinject import injector

from medsutil.ocproc2.codecs.ops import Instruction, SingleValueInstruction, OPSContext, EncodeDecodeGroup
from medsutil.ocproc2.elements import SingleElement
from medsutil.ocproc2.structures import ParentRecord
from medsutil.ocproc2.codecs.meds.structs import StationRecord, MedsEncoding, SurfaceCodeGroup, SurfaceParameterGroup, \
    ProfileInfoGroup, ProfileRecord, ProfileLevelGroup
from medsutil.ocproc2.util import combine_quality_scores, find_quality_for_protocol
from medsutil.units import UnitConverter


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
        return inst

    def pcode_list_for_encode(self, is_surface: bool = False, parameters_only: bool = False) -> t.Iterable[str]:
        codes: list[tuple[str, int]] = []
        for pcode, entry in self._meds_map.items():
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


class MedsConverter:

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
            # TODO: we need to add information here or decode this better
            # sr.gts_bulletin_time = ...

        # TODO: cruise ID
        # TODO: data type
        # TODO: iumsgno
        # TODO: stream source
        # TODO: update action
        # TODO: station number
        # TODO: stream identifier
        # TODO: qc version
        # TODO: data availability

        self._encode_surface_groups(sr, context)
        self._encode_profile_info_groups(sr, context)

        return sr

    def _encode_profile_info_groups(self, sr: StationRecord, context: OPSContext):
        if "PROFILE" in context.record.subrecords:
            for rs_idx, rs in context.record.subrecords["PROFILE"].items():
                with context.recordset_context(rs, "PROFILE"):
                    for pcode in self.code_map.pcode_list_for_encode(False, True):
                        prof_values, d_values, prof_priority, d_type = self._get_profile_values(pcode, context)

                        if prof_values:
                            self._encode_profile_info_group(pcode, prof_values, prof_priority, d_values, d_type, sr, context)

    def _encode_profile_info_group(self,
                                   pcode: str,
                                   prof_values: list[tuple[t.SupportsFloat, int | None]],
                                   prof_priority: int,
                                   depth_values: list[tuple[float, int | None]],
                                   depth_type: str,
                                   sr: StationRecord,
                                   context: OPSContext):
        pig = ProfileInfoGroup()
        pig.profile_type = pcode
        # TODO: is duplicate
        # TODO: digitization indicator
        # TODO: precision code
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

    def _get_profile_values(self, pcode: str, context: OPSContext) -> tuple[list[tuple[t.SupportsFloat, int | None]], list[tuple[float, int | None]], int, str]:
        instruction = self._get_encode_instruction(pcode)
        depths = []
        n_with_depth = 0
        pressures = []
        n_with_pressure = 0
        values = []
        for record in context.recordset.records.iterate_with_load():
            with context.record_context(record):
                value, quality = instruction.get_value_with_quality(context)
                if value is None:
                    continue
                depth = None
                depth_q = None
                pressure = None
                pressure_q = None
                if record.coordinates.has_value("Depth") and not instruction.extras.get("skip_depth", False):
                    d = record.coordinates["Depth"].ideal()
                    depth = d.to_float("meters")
                    depth_q = context.get_quality(d)
                    n_with_depth += 1
                if record.coordinates.has_value("Pressure") and not instruction.extras.get("skip_pressure", False):
                    p = record.coordinates["Pressure"].ideal()
                    pressure = p.to_float("dbar")
                    pressure_q = context.get_quality(p)
                    n_with_pressure += 1
                if depth is not None or pressure is not None:
                    depths.append((depth, depth_q))
                    pressures.append((pressure, pressure_q))
                    values.append((value, quality))
        if n_with_depth >= n_with_pressure:
            return values, depths, instruction.extras.get("priority", 0), "D"
        else:
            return values, pressures, instruction.extras.get("priority", 0), "P"

    def _get_encode_instruction(self, pcode: str) -> SingleValueInstruction:
        instruction = self.code_map.lookup(pcode)
        if isinstance(instruction, EncodeDecodeGroup):
            instruction = instruction.get_instruction(True)
        if isinstance(instruction, SingleValueInstruction):
            return instruction
        raise ValueError(f"Unrecognized instruction: [{instruction.__class__}]")

    def _encode_surface_groups(self, sr: StationRecord, context: OPSContext):
        for pcode in self.code_map.pcode_list_for_encode(True):
            instruction = self._get_encode_instruction(pcode)
            priority = instruction.extras.get("priority", 0)
            group = instruction.extras.get("meds_group", "parameter")
            value, quality = instruction.get_value_with_quality(context)
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

        return pr
