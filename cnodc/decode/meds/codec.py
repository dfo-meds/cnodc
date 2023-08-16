import typing as t
from osdt.cds import TranscodingResult, DataRecord, RecordSet, DataValue, CodeTables
from osdt.common import CodecProtocol, BufferedBinaryReader, BaseCodec
from .structs import StationRecord, MedsEncoding, pack, unpack, HistoryGroup, SurfaceCodeGroup, SurfaceParameterGroup, \
    ProfileLevelGroup, ProfileInfoGroup, ProfileRecord
import pathlib
import yaml
from autoinject import injector
import datetime


class MedsLookupTables:

    def __init__(self):
        self._pcode_lookup = {}
        self._precision_lookup = {}
        self._code_lookup = {}
        self._load_tables()

    def pcode(self, p_code: str) -> t.Optional[dict]:
        return self._pcode_lookup[p_code] if p_code in self._pcode_lookup else None

    def precision(self, code: str, p_code: str):
        code = str(code)
        if code in self._precision_lookup:
            val = self._precision_lookup[code]
            if isinstance(val, dict):
                if p_code in val:
                    return val[p_code]
                elif "_default" in val:
                    return val["_default"]
            else:
                return val
        return None

    def reverse_precision_lookup(self, precision, p_code: str):
        if precision is None:
            return "0"
        best_range_match = None
        best_value_match = None
        for code in self._precision_lookup:
            val = self._precision_lookup[code]
            if isinstance(val, dict):
                if p_code in val:
                    val = val[p_code]
                elif "_default" in val:
                    val = val["_default"]
                else:
                    continue
            if val is None:
                continue
            if isinstance(val, (list, tuple)) and best_range_match is None:
                if isinstance(precision, list):
                    if (val[0] is None or val[0] <= precision[0] or precision[0] is None) and (val[1] is None or val[1] >= precision[1] or precision[1] is None):
                        best_range_match = code
                else:
                    if (val[0] is None or val[0] <= precision) and (val[1] is None or val[1] >= precision):
                        best_range_match = code
            elif val is not None and not isinstance(precision, list):
                if val == precision:
                    best_value_match = code
        if best_value_match is not None:
            return best_value_match
        if best_range_match is not None:
            return best_range_match
        return "0"

    def lookup_cds_code(self, meds_code: str, meds_code_table: str):
        if meds_code_table not in self._code_lookup:
            return None, None
        if meds_code not in self._code_lookup[meds_code_table]:
            if meds_code.isdigit() and int(meds_code) in self._code_lookup[meds_code_table]:
                meds_code = int(meds_code)
            else:
                return None, None
        cds_code = self._code_lookup[meds_code_table][meds_code]
        if isinstance(cds_code, dict):
            return cds_code['value'], (cds_code['metadata'] if 'metadata' in cds_code else None)
        return cds_code, None

    def lookup_meds_codes(self, cds_short_code: str, cds_long_code: str, meds_code_table: str) -> t.Iterable[tuple[str, t.Optional[dict]]]:
        if meds_code_table not in self._code_lookup:
            return []
        options = []
        for meds_code in self._code_lookup[meds_code_table]:
            val = self._code_lookup[meds_code_table][meds_code]
            if val == cds_short_code or val == cds_long_code:
                options.append((meds_code, None, 1))
            elif isinstance(val, dict) and 'value' in val:
                if val['value'] == cds_short_code or val['value'] == cds_long_code:
                    md = val['metadata'] if 'metadata' in val else None
                    priority = 0 if md else 1
                    if 'is_active' in val and not val['is_active']:
                        priority = 2
                    options.append((meds_code, None, priority))
        options.sort(key=lambda x: x[2])
        for opt in options:
            yield opt[0], opt[1]

    def _load_tables(self):
        root = pathlib.Path(__file__).absolute().parent
        with open(root / "pcode_map.yaml", "r") as h:
            d = yaml.safe_load(h.read()) or {}
            self._pcode_lookup = {
                str(x): d[x]
                for x in d
            }
        with open(root / "precision_map.yaml", "r") as h:
            d = yaml.safe_load(h.read()) or {}
            self._precision_lookup = {str(x): d[x] for x in d}
        with open(root / "code_map.yaml", "r") as h:
            d = yaml.safe_load(h.read()) or {}
            self._code_lookup = {str(x): d[x] for x in d}

    def lookup_pcode(self, cds_name: str, cds_long_name: str, t: str = 'data'):
        options = []
        for pcode in self._pcode_lookup:
            if 'type' in self._pcode_lookup[pcode]:
                if not self._pcode_lookup[pcode]['type'] == t:
                    continue
            elif t != 'data':
                continue
            info = self._pcode_lookup[pcode]
            if cds_name != info['name'] and cds_long_name != info['name']:
                continue
            options.append([
                pcode,
                info['units'] if 'units' in info else None,
                info['metadata'] if 'metadata' in info else None,
                info['pcode_type'] if 'pcode_type' in info else 'parameter',
                info['priority'] if 'priority' in info else 0,
                0 if 'metadata' in info and info['metadata'] else 0
            ])
        options.sort(key=lambda x: x[-1])
        for a, b, c, d, e, _ in options:
            yield a, b, c, d, e


class MedsCodec(BaseCodec):

    tables: CodeTables = None

    @injector.construct
    def __init__(self, fmt: MedsEncoding):
        d = "MEDS ASCII format for WMO transmission" if fmt == MedsEncoding.MEDS_ASCII else "MEDS OCPROC format for storage"
        ext = ".meds" if fmt == MedsEncoding.MEDS_ASCII else ".ocproc"
        super().__init__(d, ext)
        self.encoding = fmt
        self.lookup = MedsLookupTables()

    def decode(self, data: t.Iterable[bytes], **kwargs) -> t.Iterable[DataRecord]:
        reader = BufferedBinaryReader(data)
        for station_record in unpack(reader, self.encoding):
            yield self.transform_to_cds(station_record)

    def transform_to_cds(self, sr: StationRecord) -> DataRecord:
        dr = DataRecord()
        dr.set("CRUISE_ID", sr.cruise_id)
        dr.set("OBSERVATION_TIME", sr.observation_time)
        long, lat = sr.coordinates
        dr.set("LATITUDE", lat)
        dr.set("LONGITUDE", long)
        dr.set("MEDS_IUMSGNO", sr.iumsgno)
        dr.set("DATA_SOURCE", self.lookup_cds_code(sr.data_type, "data_type", "data_sources"))
        dr.set("MEDS_STREAM_SOURCE", self.lookup_cds_code(sr.stream_source, "upstream_source"))
        dr.set("MEDS_UPDATE_ACTION", self.lookup_cds_code(sr.update_action, "update_action"))
        dr.set("MEDS_STATION_ID", sr.station_number)
        dr.coordinates.get('LATITUDE').set('QUALITY', self.tables.lookup('quality_flags', sr.quality_position))
        dr.coordinates.get('LONGITUDE').set('QUALITY', self.tables.lookup('quality_flags', sr.quality_position))
        dr.coordinates.get('TIME').set('QUALITY', self.tables.lookup('quality_flags', sr.quality_datetime))
        dr.set("LAST_UPDATE_DATE", sr.update_date)
        dr.set("GTS_STIME", sr.gts_bulletin_time)
        dr.set("GTS_INFO", sr.gts_header_info)
        dr.set("GTS_ORIGIN", sr.gts_source_node)
        dr.set("MEDS_STREAM_ORIGINATOR", self.lookup_cds_code(sr.stream_identifier[0:2], 'data_source', 'organizations'))
        dr.set("MEDS_STREAM_DATA_SOURCE", self.lookup_cds_code(sr.stream_identifier[2:4], "data_type", 'data_sources'))
        dr.set("MEDS_QC_VERSION", sr.qc_version)
        dr.set("MEDS_AVAILABILITY", self.lookup_cds_code(sr.data_availability, "data_availability", "meds_data_availabilities"))
        for sc in sr.surface_code_groups:
            self.handle_surface_from_pcode(dr, sc.pcode, sc.value, sc.quality, True)
        for sp in sr.surface_parameter_groups:
            self.handle_surface_from_pcode(dr, sp.pcode, sp.value, sp.quality, False)
        for hr in sr.history_groups:
            self.handle_history_group(dr, hr)
        for pro_info in sr.profile_info_groups:
            var_name, units, meta_extras = self.pcode_map(pro_info.profile_type)
            if var_name is None:
                continue
            digit_method = self.lookup_cds_code(pro_info.digitization_code, "digitization_code", "digitization_methods")
            precision = self.lookup.precision(pro_info.precision_code, pro_info.profile_type)
            is_dupe = pro_info.is_duplicate
            for prof in pro_info.profiles:
                coord = "PRESSURE" if prof.uses_pressure_levels else "DEPTH"
                for obs in prof.level_groups:
                    sub_r = dr.find_child_record("PROFILE_RECORDS", {coord: obs.depth_pressure})
                    if sub_r is None:
                        sub_r = DataRecord()
                        sub_r.set(coord, DataValue(obs.depth_pressure, metadata={
                            "QUALITY": self.tables.lookup("quality_flags", obs.depth_quality),
                            "UNITS": "m" if coord == "DEPTH" else "dbar"
                        }))
                        dr.add_profile_record(sub_r)
                    metadata = {
                        "QUALITY": self.tables.lookup("quality_flags", obs.parameter_quality),
                        "DIGITIZATION": digit_method,
                        "PRECISION": precision,
                        "UNITS": units,
                        "IS_DUPLICATE": is_dupe
                    }
                    metadata.update(meta_extras)
                    sub_r.set(var_name, DataValue(obs.parameter_value, metadata=metadata))
        return dr

    def lookup_cds_code(self, meds_code_value, meds_code_table, cds_code_table=None):
        cds_value, cds_metadata = self.lookup.lookup_cds_code(meds_code_value, meds_code_table)
        if cds_value is None:
            print(f"warning meds code {meds_code_value} does not exist in {meds_code_table}")
            return meds_code_value
        if cds_code_table:
            cv = self.tables.lookup(cds_code_table, cds_value)
            if cv is not None:
                cds_value = cv
            else:
                print(f"warning CVS code {cds_value} does not exist in {cds_code_table}")
        if cds_metadata:
            return DataValue(reported_value=cds_value, metadata=cds_metadata)
        else:
            return cds_value

    def handle_surface_from_pcode(self, dr: DataRecord, pcode, measurement, quality, is_code: bool):
        check = f"_decode_surface_{pcode.replace('$', '_')}"
        if hasattr(self, check):
            return getattr(self, check)(dr, pcode, measurement, quality, is_code)
        else:
            return self._default_surface_pcode(dr, pcode, measurement, quality, is_code)

    def _default_surface_pcode(self, dr: DataRecord, pcode, measurement, quality, is_code: bool):
        var_name, pcode_units, pcode_extra_metadata = self.pcode_map(pcode)
        if var_name is None:
            return
        mdata = {}
        if pcode_units is not None:
            mdata["UNITS"] = pcode_units
        if quality is not None:
            mdata["QUALITY"] = self.tables.lookup(quality, "quality_flags")
        mdata.update(pcode_extra_metadata)
        if is_code:
            check, metadata = self.lookup.lookup_cds_code(measurement, f"pcode_{pcode}")
            if check is not None:
                measurement = check
            if metadata is not None:
                mdata.update(metadata)
        dv = DataValue(measurement, metadata=mdata)
        dr.set(var_name, dv)

    def _map_xbt_probe_eq(self, sr: StationRecord, dr: DataRecord):
        if 'PFR$' not in sr.scratch:
            scg = SurfaceCodeGroup()
            scg.pcode = "PFR$"
            pe = dr.get("XBT_PROBE_EQUATION")
            code = "   "
            if "XBT_PROBE_TYPE" in dr.metadata:
                pt = dr.metadata.get("XBT_PROBE_TYPE")
                code = str(pt.value()).zfill(3)
                if "QUALITY" in pt.metadata:
                    scg.quality = pt.metadata.get("QUALITY").value()
            if "XBT_PROBE_EQUATION" in dr.metadata:
                pe = dr.metadata.get("XBT_PROBE_EQUATION")
                code += str(pe.value()).zfill(2)
                if "QUALITY" in pe.metadata:
                    scg.quality = pe.metadata.get("QUALITY").value()
            else:
                code += "00"
            scg.value = code
            sr.surface_code_groups.append(scg)
            sr.scratch["PFR$"] = True

    def _decode_surface_PFR_(self, dr: DataRecord, pcode, measurement, quality, is_code: bool):
        dr.set("XBT_PROBE_TYPE", DataValue(
            measurement[0:3],
            metadata={
                "QUALITY": quality
            }
        ))
        dr.set("XBT_PROBE_EQUATION", DataValue(
            measurement[3:5],
            metadata={
                "QUALITY": quality
            }
        ))

    def _decode_surface_PDT_(self, dr: DataRecord, pcode, measurement, quality, is_code: bool):
        if not len(measurement) == 8:
            print(measurement)
        if 'POSITION_LAST_TIME' in dr.metadata:
            pos_lt = dr.metadata.get('POSITION_LAST_TIME')
            dt: datetime.datetime = pos_lt.value()
            pos_lt.reported_value = datetime.datetime(
                int(measurement[0:4]),
                int(measurement[4:6]),
                int(measurement[6:8]),
                dt.hour,
                dt.minute,
                dt.second
            )
        else:
            dr.metadata['POSITION_LAST_TIME'] = datetime.datetime(
                int(measurement[0:4]),
                int(measurement[4:6]),
                int(measurement[6:8])
            )

    def _decode_surface_PTM_(self, dr: DataRecord, pcode, measurement, quality, is_code: bool):
        if 'POSITION_LAST_TIME' in dr.metadata:
            pos_lt = dr.metadata.get('POSITION_LAST_TIME')
            dt: datetime.datetime = pos_lt.value()
            pos_lt.reported_value = datetime.datetime(
                dt.year,
                dt.month,
                dt.day,
                int(measurement[0:2]),
                int(measurement[2:4])
            )
        else:
            dr.metadata['POSITION_LAST_TIME'] = datetime.datetime(
                1980,
                1,
                1,
                int(measurement[0:2]),
                int(measurement[2:4])
            )

    def pcode_map(self, pcode):
        p_code = self.lookup.pcode(pcode)
        if p_code is None:
            print(f"Unrecognized pcode {pcode}")
            #raise ValueError(f'Unrecognized pcode {pcode}')
            return None, None, None
        cds_name = None
        if 'type' not in p_code:
            p_code['type'] = 'data'
        if p_code['type'] == 'data':
            cds_name = self.tables.lookup('parameters', p_code['name'])
        elif p_code['type'] == 'metadata':
            cds_name = self.tables.lookup('metadata', p_code['name'])
        else:
            raise ValueError(f'Unrecognized lookup type {p_code["type"]}')
        return (
            cds_name,
            p_code['units'] if 'units' in p_code else None,
            p_code['metadata'] if 'metadata' in p_code else {}
        )

    def handle_history_group(self, dr: DataRecord, hg: HistoryGroup):
        hr = DataRecord()
        hr.set("INSTITUTION", self.lookup_cds_code(hg.organization, "data_source", "organizations"))
        hr.set("HISTORY_PROGRAM_NAME", hg.program_code)
        hr.set("HISTORY_PROGRAM_VERSION", hg.program_version)
        hr.set("HISTORY_ACTION_DATE", hg.action_date)
        hr.set("HISTORY_ACTION_CODE", self.lookup_cds_code(hg.action_code, "history_action", "history_actions"))
        hr.set("HISTORY_ACTION_ORIGINAL_VALUE", hg.previous_value)
        if hg.action_pcode != "RCRD":
            trg, _, _ = self.pcode_map(hg.action_pcode)
            hr.set("HISTORY_TARGET_PARAMETER", trg)
            hr.set("HISTORY_TARGET_DEPTH", hg.action_locator)
        dr.add_history_record(hr)

    def encode(self, records: TranscodingResult, **kwargs) -> t.Iterable[bytes]:
        return pack((self.transform_from_cds(r) for r in records), self.encoding)

    def transform_from_cds(self, dr: DataRecord) -> StationRecord:
        sr = StationRecord()
        if 'CRUISE_ID' in dr.metadata:
            sr.cruise_id = dr.metadata.get('CRUISE_ID').value()
        else:
            sr.cruise_id = ''
        if 'OBSERVATION_TIME' in dr.coordinates:
            obs_time = dr.coordinates.get('OBSERVATION_TIME')
            sr.observation_time = obs_time.value()
            if 'QUALITY' in obs_time.metadata:
                sr.quality_datetime = str(obs_time.metadata.get('QUALITY').value())
        lat = dr.coordinates.get('LATITUDE', None)
        long = dr.coordinates.get('LONGITUDE', None)
        if lat is not None and long is not None:
            sr.coordinates = (long.value(), lat.value())
            qualities = []
            if 'QUALITY' in long.metadata:
                qualities.append(int(long.metadata.get('QUALITY').value()))
            if 'QUALITY' in lat.metadata:
                qualities.append(int(lat.metadata.get('QUALITY').value()))
            if qualities:
                sr.quality_position = str(max(qualities))
        else:
            sr.coordinates = (999.9999, 99.999999)
            sr.quality_position = "0"
        if 'MEDS_IUMSGNO' in dr.metadata:
            sr.iumsgno = dr.metadata.get('MEDS_IUMSGNO').value()
        else:
            sr.iumsgno = 0
        if 'DATA_SOURCE' in dr.metadata:
            sr.data_type = self.lookup_meds_code(dr.metadata.get('DATA_SOURCE'), 'data_sources', 'data_type')
        if 'MEDS_STREAM_SOURCE' in dr.metadata:
            sr.stream_source = self.lookup_meds_code(dr.metadata.get('MEDS_STREAM_SOURCE'), None, 'upstream_source')
        if 'MEDS_UPDATE_ACTION' in dr.metadata:
            sr.update_action = self.lookup_meds_code(dr.metadata.get('MEDS_UPDATE_ACTION'), None, 'update_action')
        if 'MEDS_STATION_ID' in dr.metadata:
            sr.station_number = dr.metadata.get('MEDS_STATION_ID').value()
        if 'LAST_UPDATE_DATE' in dr.metadata:
            sr.update_date = dr.metadata.get('LAST_UPDATE_DATE').value()
        if 'GTS_STIME' in dr.metadata:
            sr.gts_bulletin_time = dr.metadata.get('GTS_STIME').value()
        if 'GTS_INFO' in dr.metadata:
            sr.gts_header_info = dr.metadata.get('GTS_INFO').value()
        if 'GTS_ORIGIN' in dr.metadata:
            sr.gts_source_node = dr.metadata.get('GTS_ORIGIN').value()
        sr_1 = '  '
        sr_2 = '  '
        if 'MEDS_STREAM_ORIGINATOR' in dr.metadata:
            sr_1 = self.lookup_meds_code(dr.metadata.get('MEDS_STREAM_ORIGINATOR'), 'organizations', 'data_source', check_length=2)
        if 'MEDS_STREAM_DATA_SOURCE' in dr.metadata:
            sr_2 = self.lookup_meds_code(dr.metadata.get('MEDS_STREAM_DATA_SOURCE'), 'data_sources', 'data_type')
        sr.stream_identifier = sr_1 + sr_2
        if 'MEDS_QC_VERSION' in dr.metadata:
            sr.qc_version = dr.metadata.get('MEDS_QC_VERSION').value()
        if 'MEDS_AVAILABILITY' in dr.metadata:
            sr.data_availability = self.lookup_meds_code(dr.metadata.get('MEDS_AVAILABILITY'), 'meds_data_availabilities', 'data_availability')
        self.add_surface_values(sr, dr)  # Handles surface codes and parameters
        if 'PROFILE_RECORDS' in dr.subrecords:
            memory = {}
            profile_ref = {}
            for record in dr.subrecords['PROFILE_RECORDS']:
                if 'DEPTH' not in record.coordinates and 'PRESSURE' not in record.coordinates:
                    print('unknown vertical coordinate')
                    continue
                d_p = 'D' if 'DEPTH' in record.coordinates else 'P'
                depth_val = record.coordinates.get('DEPTH' if d_p == 'D' else 'PRESSURE')
                target_units = "m" if d_p == 'D' else 'dbar'
                dr_d_units = depth_val.metadata.get('UNITS', None)
                depth_actual_number = depth_val.value()
                if dr_d_units is not None and dr_d_units != target_units:
                    depth_actual_number = self.tables.convert(depth_actual_number, dr_d_units, target_units)
                for sname in record.variables:
                    param_val = record.variables.get(sname)
                    if sname not in memory:
                        lname = self.tables.lookup_long_name('parameters', sname)
                        for pcode, units, metadata, pcode_type, priority in self.lookup.lookup_pcode(sname, lname, 'data'):
                            if metadata is None or self._compare_metadata(metadata, param_val):
                                memory[sname] = (pcode, units, priority)
                                break
                        else:
                            memory[sname] = (None, None, None)
                            print(f"No pcode match found for {sname} or {lname} data variable at depths")
                    if memory[sname][0] is not None:
                        pcode, units, priority = memory[sname]
                        lg = ProfileLevelGroup()
                        lg.depth_pressure = depth_actual_number
                        dr_units = param_val.metadata.get('UNITS', None)
                        if units is not None and dr_units is not None and dr_units.value() != units:
                            lg.parameter_value = self.tables.convert(param_val.value(), dr_units.value(), units)
                        else:
                            lg.parameter_value = param_val.value()
                        if 'QUALITY' in depth_val.metadata:
                            lg.depth_quality = depth_val.metadata['QUALITY'].value()
                        if 'QUALITY' in param_val.metadata:
                            lg.parameter_quality = param_val.metadata['QUALITY'].value()
                        dm = "0"
                        if "DIGITIZATION" in param_val.metadata:
                            check_dm = self.lookup_meds_code(param_val.metadata.get("DIGITIZATION"), "digitization_methods", "digitization_code")
                            if check_dm is not None:
                                dm = check_dm
                        is_dupe = "N"
                        if "IS_DUPLICATE" in param_val.metadata and param_val.metadata.get("IS_DUPLICATE").value():
                            is_dupe = "Y"
                        precision_code = "0"
                        if "PRECISION" in param_val.metadata:
                            precision_code = self.lookup.reverse_precision_lookup(param_val.metadata.get("PRECISION").value(), pcode)
                        pg_key = f"{pcode}_{d_p}_{dm}_{is_dupe}_{precision_code}"
                        if pg_key not in profile_ref:
                            profile_ref[pg_key] = ProfileInfoGroup()
                            profile_ref[pg_key].profile_type = pcode
                            profile_ref[pg_key].digitization_code = dm
                            profile_ref[pg_key].is_duplicate = is_dupe
                            profile_ref[pg_key].precision_code = precision_code
                            prof_rec = ProfileRecord()
                            prof_rec.uses_pressure_levels = d_p == 'P'
                            profile_ref[pg_key].profiles.append(prof_rec)
                            sr.profile_info_groups.append(profile_ref[pg_key])
                        profile_ref[pg_key].profiles[0].level_groups.append(lg)
        return sr

    def lookup_meds_code(self, cds_dv: DataValue, cds_table: t.Optional[str], meds_table: str, check_length: t.Optional[int] = None):
        short_name = cds_dv.value()
        long_name = None if cds_table is None else self.tables.lookup_long_name(cds_table, short_name)
        for meds_code, md in self.lookup.lookup_meds_codes(short_name, long_name, meds_table):
            if check_length is not None and not len(meds_code) == check_length:
                continue
            if md is None or self._compare_metadata(md, cds_dv):
                return meds_code
        else:
            raise ValueError(f"Cannot map data value {cds_dv.pretty()} to {meds_table}")

    def add_surface_values(self, sr: StationRecord, dr: DataRecord):
        for sname in dr.variables:
            lname = self.tables.lookup_long_name('parameters', sname)
            dv = dr.variables.get(sname)
            for pcode, units, metadata, pcode_type, priority in self.lookup.lookup_pcode(sname, lname, 'data'):
                if metadata is None or self._compare_metadata(metadata, dv):
                    self.add_surface_value(sr, pcode, dv, units, pcode_type == 'code', priority)
                    break
            else:
                print(f"Cannot map cds data value {sname} to meds code")
        for sname in dr.metadata:
            if sname == "XBT_EQ" or sname == "XBT_PROBE":
                self._map_xbt_probe_eq(sr, dr)
                continue
            lname = self.tables.lookup_long_name('metadata', sname)
            dv = dr.metadata.get(sname)
            for pcode, units, metadata, pcode_type, priority in self.lookup.lookup_pcode(sname, lname, 'metadata'):
                if metadata is None or self._compare_metadata(metadata, dv):
                    self.add_surface_value(sr, pcode, dv, units, pcode_type == 'code', priority)
                    break
            else:
                if sname not in (
                    'CRUISE_ID',
                    'MEDS_IUMSGNO',
                    'DATA_SRC',
                    'MEDS_STREAM_SOURCE',
                    'MEDS_UPDATE_ACTION',
                    'MEDS_STATION_ID',
                    'LAST_UPDATE_DATE',
                    'GTS_STIME',
                    'GTS_INFO',
                    'GTS_ORIGIN',
                    'MEDS_STREAM_ORIGINATOR',
                    'MEDS_STREAM_DATA_SOURCE',
                    'MEDS_QC_VERSION',
                    'MEDS_AVAILABILITY'
                ):
                    print(f"Cannot map cds metadata value {sname} to meds code")

    def _compare_metadata(self, d: dict, dv: DataValue) -> bool:
        for key in d:
            val = dv.metadata.get(key)
            if val is None:
                return False
            if val.value() != d[key]:
                return False
        return True

    def add_surface_value(self, sr: StationRecord, pcode: str, dv: DataValue, units, is_code: bool, priority: int):
        if is_code:
            scg = SurfaceCodeGroup()
            scg.pcode = pcode
            qual = dv.metadata.get("QUALITY")
            if qual is not None and qual.value() is not None:
                scg.quality = qual.value()
            else:
                scg.quality = "0"
            scg.value = dv.value()
            scg.priority = priority
            sr.surface_code_groups.append(scg)
        else:
            spg = SurfaceParameterGroup()
            spg.pcode = pcode
            spg.priority = priority
            qual = dv.metadata.get("QUALITY")
            if qual is not None and qual.value() is not None:
                spg.quality = qual.value()
            else:
                spg.quality = "0"
            if "UNITS" in dv.metadata and units is not None and units != dv.metadata.get("UNITS").value():
                spg.value = self.tables.convert(dv.value(), dv.metadata.get("UNITS").value(), units)
            else:
                spg.value = dv.value()
            sr.surface_parameter_groups.append(spg)
