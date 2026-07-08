import typing as t

from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError, with_exception_note
from medsutil.ocproc2 import ParentRecord, SingleElement, ChildRecord, RecordSet
from medsutil.ocproc2.codecs.gts import GtsSubDecoder
from medsutil.ocproc2.codecs.base import DecodeResult
from medsutil.byteseq import ByteSequenceReader


class AsciiDecodeError(CodedError): CODE_SPACE = "WMO-ASCII"


class AsciiDecoder(GtsSubDecoder):

    def decode_from_bytes(self, reader: ByteSequenceReader, header: str, skip_decode: bool, received_date: AwareDateTime | None = None) -> DecodeResult:
        body = reader.consume_until(b'=', include_target=True)
        original_data = header.encode('ascii') + b"\n" + body
        if skip_decode:
            return DecodeResult(skipped=True, original=original_data)
        try:
            return DecodeResult(
                records=self.decode_message(header, body.decode('ascii'), received_date),
                original=original_data
            )
        except Exception as ex:
            return DecodeResult(exc=ex, original=original_data)

    def decode_message(self, header: str, ascii_message: str, received_date: AwareDateTime | None = None) -> list[ParentRecord]:
        # Remove irrelevant trailing characters
        ascii_message = ascii_message.strip()
        if ascii_message.endswith("="):
            ascii_message = ascii_message[:-1]
        ascii_message = ascii_message.strip()

        # Normalize spaces
        ascii_message = ascii_message.replace("\t", " ")
        ascii_message = ascii_message.replace("\n", " ")
        ascii_message = ascii_message.replace("\r", " ")
        while "  " in ascii_message:
            ascii_message = ascii_message.replace("  ", " ")

        # Build and return the record
        record = ParentRecord()
        record.metadata['GTSHeader'] = header
        record.metadata['WMOAsciiCodeForm'] = ascii_message[0:4]
        self._decode_message(record, ascii_message[4:].strip().split(" "), received_date or AwareDateTime.now())
        return [record]

    def _decode_message(self, record: ParentRecord, ascii_message: list[str], received_date: AwareDateTime):
        raise NotImplementedError

    def parse_scinum(self, num: str, whole_places: int, units: str, factor: float | None = None, **kwargs) -> SingleElement | None:
        if num == "" or all(x == '/' or x == '-' for x in num):
            return None
        if num[0] == "-":
            whole_places += 1
        precision = len(num) - whole_places
        k = len(num) - 1
        while num[k] == '/' and k >= 0:
            precision -= 1
            k -= 1
        num = num.replace('/', '0')
        num = f"{num[0:whole_places]}.{num[whole_places:]}".lstrip("0")
        if num[0] == "-":
            part = num[1:].lstrip("0")
            if part[0] == ".":
                num = f"-0{part}"
            else:
                num = f"-{part}"
        if num[-1] == ".":
            num = num[:-1]
        try:
            _ = float(num)
        except ValueError as ex:
            raise AsciiDecodeError(f"Invalid number, received [{num}]", 1200) from ex
        for kwarg in list(kwargs.keys()):
            if kwargs[kwarg] is None:
                kwargs.pop(kwarg)
        if factor is not None:
            return SingleElement(
                float(num) * factor,
                Uncertainty=SingleElement(((10 ** (-1 * precision)) / 2) * factor, UncertaintyType="uniform"),
                Units=units, **kwargs)
        else:
            return SingleElement(num, Uncertainty=SingleElement((10 ** (-1 * precision)) / 2, UncertaintyType="uniform"), Units=units, **kwargs)

    @with_exception_note("Error while parsing A1 bw nb nb nb")
    def parse_wmo_id(self, a1_bw_nbnbnb: str) -> str:
        if not a1_bw_nbnbnb.isdigit():
            raise AsciiDecodeError(f"Invalid WMO code, received [{a1_bw_nbnbnb}]")
        return a1_bw_nbnbnb

    def parse_message_time(self, yymmj: str, gggg_: str, received_date: AwareDateTime) -> SingleElement:
        # If j is "5" and current year is "XXX5" to "XXX9" then year is "XXXj"
        # If j is "5" and current year is "XXX0" to "XXX4" then year is "XXXj" - 10 years
        try:
            year_mod_ten = int(yymmj[4:5])
            year = int(f"{str(received_date.year)[0:3]}{year_mod_ten}")
            if year > received_date.year:
                year -= 10
        except (ValueError, TypeError) as ex:
            raise AsciiDecodeError(f"Invalid year, received [{yymmj}]", 1000) from ex
        try:
            day = int(yymmj[0:2])
        except (ValueError, TypeError) as ex:
            raise AsciiDecodeError(f"Invalid day, received [{yymmj}]", 1001) from ex
        try:
            month = int(yymmj[2:4])
        except (ValueError, TypeError) as ex:
            raise AsciiDecodeError(f"Invalid month, received [{yymmj}]", 1002) from ex
        try:
            hour = int(gggg_[0:2])
        except (ValueError, TypeError) as ex:
            raise AsciiDecodeError(f"Invalid hour, received [{gggg_}]", 1003) from ex
        try:
            minute = int(gggg_[2:4])
        except (ValueError, TypeError) as ex:
            raise AsciiDecodeError(f"Invalid minute, received [{gggg_}]", 1004) from ex
        try:
            return SingleElement(
                AwareDateTime(year, month, day, hour, minute, tzinfo="Etc/UTC"),
                DatePrecision="minute"
            )
        except ValueError as ex:
            raise AsciiDecodeError(f"Invalid date/time, received [{yymmj} {gggg_}]", 1005) from ex

    def parse_dd_coordinates(self, q_la: str, lo: str) -> tuple[SingleElement | None, SingleElement | None]:
        q_la = q_la.rstrip('/')
        lo = lo.rstrip('/')
        try:
            q = int(q_la[0])
        except (ValueError, TypeError) as ex:
            raise AsciiDecodeError(f"Invalid quadrant, received [{q_la[0]}]", 1100) from ex
        except IndexError as ex:
            raise AsciiDecodeError("Invalid quadrant, none provided", 1101) from ex
        try:
            la = q_la[1:]
        except IndexError as ex:
            raise AsciiDecodeError("Invalid latitude, no value provided", 1102)
        if q == 9:
            raise AsciiDecodeError("Invalid quadrant, received [9]", 1103)
        if q in (5,6,7,8):
            la = f"-{la}"
        if q in (0,1,5,6):
            lo = f"-{lo}"
        try:
            latitude = self.parse_scinum(la, 2, "degrees_north")
        except Exception as ex:
            ex.add_note(f"Error while parsing latitude")
            raise
        try:
            longitude = self.parse_scinum(lo, 3, "degrees_east")
        except Exception as ex:
            ex.add_note("Error while parsing longitude")
            raise
        return latitude, longitude

    @with_exception_note("Error while parsing wind direction")
    def parse_wind_direction(self, dd: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(f"{dd}/", 3, "degrees", **kwargs)

    @with_exception_note("Error while parsing wind speed")
    def parse_wind_speed(self, ff: str, units: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(ff, 2, units, **kwargs)

    @with_exception_note("Error while parsing wind speed")
    def parse_wind_speed_ext(self, fff: str, units: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(fff, 3, units, **kwargs)

    @with_exception_note("Error while parsing temperature with sign")
    def parse_temperature(self, sn_td: str, **kwargs) -> SingleElement | None:
        sn = sn_td[0]
        td = sn_td[1:]
        if sn == "1":
            td = f"-{td}"
        return self.parse_scinum(td, 2, "degrees_C", TemperatureScale="ITS-90", **kwargs)

    def extract_first_integer(self, val: str) -> int | None:
        try:
            if val[0] != '/':
                return int(val[0])
            return None
        except (ValueError, IndexError, TypeError):
            return None

    @with_exception_note("Error while parsing temperature, negatives offset by 5000")
    def parse_temperature_5_offset(self, tn, **kwargs) -> SingleElement | None:
        first = self.extract_first_integer(tn)
        if first is not None and first >= 5:
            tn = f"-{first-5}{tn[1:]}"
        return self.parse_scinum(tn, 2, "degrees_C", TemperatureScale="ITS-90", **kwargs)

    @with_exception_note("Error while parsing relative humidity")
    def parse_rh(self, uuu: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(uuu, 3, "1e-2", **kwargs)

    @with_exception_note("Error while parsing air pressure")
    def parse_air_pressure(self, pppp: str, **kwargs) -> SingleElement | None:
        first = self.extract_first_integer(pppp)
        if first is not None and first < 5:
            return self.parse_scinum(f"1{pppp}", 4, "hPa", **kwargs)
        else:
            return self.parse_scinum(pppp, 3, "hPa", **kwargs)

    @with_exception_note("Error while parsing air pressure delta")
    def parse_air_pressure_delta(self, ppp: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(ppp, 2, "hPa", **kwargs)

    @with_exception_note("Error while parsing wave height")
    def parse_wave_height(self, hwa: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(hwa, 2, "m", **kwargs)

    @with_exception_note("Error while parsing wave period")
    def parse_wave_period(self, pwa: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(pwa, 2, "s", **kwargs)

    @with_exception_note("Error while parsing wave height (half meter)")
    def parse_wave_height_half_m(self, hwa: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(hwa, 2, factor=0.5, units="m", **kwargs)

    @with_exception_note("Error while parsing depth")
    def parse_depth(self, zn: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(zn, 4, "m", **kwargs)

    @with_exception_note("Error while parsing practical salinity")
    def parse_psu(self, sn: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(sn, 2, "1e-3", **kwargs)

    @with_exception_note("Error while parsing current direction")
    def parse_current_direction(self, dn: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(f"{dn}/", 3, "degrees", **kwargs)

    @with_exception_note("Error while parsing current speed")
    def parse_current_speed(self, cn: str, units: str = "cm s-1", **kwargs) -> SingleElement | None:
        return self.parse_scinum(cn, 3, "cm s-1", **kwargs)

    @with_exception_note("Error while parsing current speed")
    def parse_current_speed_vc(self, vc: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(vc, 1, "knots", **kwargs)

    @with_exception_note("Error while parsing anemometer height")
    def parse_anemometer_height(self, ah: str, **kwargs) -> SingleElement | None:
        if ah == "999":
            return SingleElement(-10, Units="m", SensorDepthReference="local_ground_corrected", **kwargs)
        else:
            return self.parse_scinum(f"-{ah}/", 3, "m", SensorDepthReference="local_ground", **kwargs)

    @with_exception_note("Error while parsing hydrostatic pressure")
    def parse_hydrostatic_pressure(self, zh: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(zh, 4, "kPa", **kwargs)

    @with_exception_note("Error while parsing thermistor cable length")
    def parse_thermistor_cable_length(self, zc: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(zc, 4, "m", **kwargs)

    @with_exception_note("Error while parsing drogue cable length")
    def parse_drogue_cable_length(self, zd: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(zd, 3, "m", **kwargs)

    @with_exception_note("Error while parsing ship speed")
    def parse_ship_speed(self, vb: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(vb, 2, "cm s-1", **kwargs)

    @with_exception_note("Error while parsing ship direction")
    def parse_ship_direction(self, db: str, **kwargs) -> SingleElement | None:
        return self.parse_scinum(f"{db}/", 3, "degrees", **kwargs)

    def parse_wind_type(self, wind_type: str) -> str:
        match wind_type:
            case "0":
                return "m s-1"
            case "1":
                return "m s-1"
            case "3":
                return "knots"
            case "4":
                return "knots"
            case _:
                raise AsciiDecodeError(f"Invalid wind source type from table 1855, received [{wind_type}]", 1300)

    def parse_wind_type_1853(self, wind_type: str) -> str:
        match wind_type:
            case "0":
                return "m s-1"
            case "1":
                return "knots"
            case "3":
                return "m s-1"
            case "4":
                return "knots"
            case _:
                raise AsciiDecodeError(f"Invalid wind source type from table 1853, received [{wind_type}]", 1700)

    def parse_quality_flag(self, value: str, parameter: str):
        return self.parse_code(value, missing=None, valid={"0", "1", "2", "3", "4", "5", "7", "9"}, error_note=f"Error while parsing quality flag for [{parameter}]")

    def parse_quality_location_class(self, code: str):
        return self.parse_code(code, missing=7, valid={"0", "1", "2", "3"}, error_note="Error while parsing location quality class")

    def parse_wmo_air_pressure_change(self, code: str):
        return self.parse_code(code, missing = 15, valid={"0", "1", "2", "3", "4", "5", "6", "7", "8"}, error_note="Error while parsing characteristic of pressure tendency")

    def parse_salinity_depth_method(self, code: str):
        return self.parse_code(code, missing=7, valid={"0", "1", "2", "3"}, error_note="Error while parsing salinity depth method")

    def parse_pmr_method(self, code: str):
        return self.parse_code(code, missing=15, valid={"0", "1", "2", "3", "4", "5", "6"}, error_note="Error while parsing platform removal method")

    def parse_cm_duration(self, code_2264: str | None = None, code_2265: str | None = None) -> str | int | None:
        if code_2264 is None and code_2265 is None:
            return None
        elif code_2264 is not None and code_2265 is not None:
            good_2264 = self.parse_code(code_2264, missing="31", valid={"1", "2", "3", "4", "5", "6", "7", "8", "9"}, error_note="Error while parsing current measurement duration (doppler)")
            good_2265 = self.parse_code(code_2265, missing="31", code_map={
                "1": "11", "2": "12", "3": "13", "4": "14", "5": "15", "6": "16", "7": "17", "9": "19",
            }, valid={}, error_note="Error while parsing current measurement duration (drift)")
            if good_2265 in ("19", "31"):
                return good_2264
            if good_2264 in ("9", "31"):
                return good_2265
            raise AsciiDecodeError("Doppler and drift method both used (Erin didn't plan for this)", 9000)
        elif code_2264 is not None:
            return self.parse_code(code_2264, missing=31, valid={"1", "2", "3", "4", "5", "6", "7", "8", "9"}, error_note="Error while parsing current measurement duration (doppler)")
        elif code_2265 is not None:
            return self.parse_code(code_2265, missing=31, code_map={
                "1": "11", "2": "12", "3": "13", "4": "14", "5": "15", "6": "16", "7": "17", "9": "19",
            }, valid={}, error_note="Error while parsing current measurement duration (drift)")

    def parse_quality_pressure(self, code: str):
        return self.parse_code(code, missing=None, valid={"0", "1"}, error_note="Error parsing pressure quality")

    def parse_quality_housekeeping(self, code: str):
        return self.parse_code(code, missing=None, valid={"0", "1"}, error_note="Error parsing housekeeping quality")

    def parse_quality_surface_temperature(self, code: str):
        return self.parse_code(code, missing=None, valid={"0", "1"}, error_note="Error parsing quality of surface temperature")

    def parse_quality_air_temperature(self, code: str):
        return self.parse_code(code, missing=None, valid={"0", "1"}, error_note="Error parsing quality of air temperature")

    def parse_quality_satellite(self, code: str):
        return self.parse_code(code, missing=3, valid={"0," ,"1"}, error_note="Error parsing quality of satellite transmission")

    def parse_quality_location(self, code: str):
        return self.parse_code(code, missing=3, valid={"0," ,"1", "2"}, error_note="Error parsing quality of location")

    def parse_hsp_correction(self, code: str):
        return self.parse_code(code, missing=None, valid={"0", "1"}, error_note="Error parsing HSP correction flag")

    def parse_code(self,
                   code: str,
                   missing: str | int | None = None,
                   code_map: dict[str, str | int] | None = None,
                   valid: t.Container[str] | None = None,
                   error_note: str | None = None) -> str | int | None:
        if code == "/":
            return missing
        if code_map is not None and code in code_map:
            return code_map[code]
        if valid is not None and code not in valid:
            ex = AsciiDecodeError(f"Invalid code, received[{code}])", 1400)
            if error_note is not None:
                ex.add_note(error_note)
            raise ex
        return code

    def parse_quality_applies_to(self, code: str, max_sequence: int) -> bool | int:
        try:
            match code:
                case "9":
                    return True
                case "/":
                    return -1
                case _:
                    value = int(code)
                    if 0 <= value <= max_sequence:
                        return value
                    else:
                        raise AsciiDecodeError(f"Invalid Qx code, expecting value in inclusive range 0-{max_sequence}, found [{code}]", 1501)
        except (TypeError, ValueError):
            raise AsciiDecodeError(f"Invalid Qx code, found [{code}]", 1500)

    def require_length(self, line: str, length: int, location: str):
        if len(line) < length:
            raise AsciiDecodeError(f"Minimum length of {length} required for {location}", 1600)

    def next_message_startswith(self, ascii_message: list[str], o: int, starts_with: str | tuple[str, ...]) -> bool:
        try:
            return ascii_message[o].startswith(starts_with)
        except IndexError:
            return False

    def validate_at_end(self, remaining_message: list[str]):
        c = 0
        for x in remaining_message:
            if not (x == "" or all(y == "/" or y == "=" for y in x)):
                c += 1
        if c > 0:
            raise AsciiDecodeError(f"Additional content at end of message [blocks: {c}]: {' '.join(remaining_message)}", 1900)

    def parse_current_method(self, code: str) -> str | int | None:
        return self.parse_code(code, missing=7, code_map={
            "1": "6"
        }, valid={"0", "2", "3", "4", "5", "6"}, error_note="Error while parsing current method")

    def parse_platform_id(self, pid: str) -> str:
        return pid.strip("/")

    def parse_xbt_instrument(self, code: str) -> str:
        return code.strip("/")

    def parse_xbt_recorder(self, code: str) -> str:
        return code.strip("/")

    def parse_digitization_method(self, code: str) -> str:
        if code == "7":
            return "selected_depths"
        elif code == "8":
            return "inflection_points"
        else:
            raise AsciiDecodeError(f"Unknown digitization method, found [{code}]", 1800)



class BuoyZZYY(AsciiDecoder):

    @with_exception_note("Error while parsing ZZYY")
    def _decode_message(self, record: ParentRecord, ascii_message: list[str], received_date: AwareDateTime):
        o, wind_units = self._decode_section_0(record, ascii_message, received_date)
        o = self._decode_section_1(record, ascii_message, wind_units, o)
        o = self._decode_section_2(record, ascii_message, o)
        o = self._decode_section_3(record, ascii_message, o)
        o = self._decode_section_4(record, ascii_message, received_date, o)
        self.validate_at_end(ascii_message[o:])

    @with_exception_note("Error while parsing section 0")
    def _decode_section_0(self, record: ParentRecord, ascii_message: list[str], received_date: AwareDateTime) -> tuple[int, str]:
        # Note: Mi Mi Mj Mj has been stripped already

        # A1 bw nb nb nb
        record.metadata["WMOID"] = self.parse_wmo_id(ascii_message[0])

        # Y Y M M J and G G g g iw
        self.require_length(ascii_message[2], 5, "G G g g iw")
        record.coordinates["Time"] = self.parse_message_time(ascii_message[1], ascii_message[2], received_date)
        wind_source = ascii_message[2][4]

        # Qc La La La La La and Lo Lo Lo Lo Lo Lo
        record.coordinates["Latitude"], record.coordinates["Longitude"] = self.parse_dd_coordinates(ascii_message[3], ascii_message[4])
        o = 5

        # 6 Ql Qt QA / (optional)
        if self.next_message_startswith(ascii_message, o, "6"):
            self.require_length(ascii_message[o], 4, "6 Ql Qt QA /")
            six_group = ascii_message[o]
            o += 1
            q_pos = self.parse_quality_flag(six_group[1], parameter="location")
            q_time = self.parse_quality_flag(six_group[2], parameter="time")
            if q_pos is not None:
                record.coordinates["Latitude"].metadata["Quality"] = q_pos
                record.coordinates["Longitude"].metadata["Quality"] = q_pos
            if q_time is not None:
                record.coordinates["Time"].metadata["Quality"] = q_time
            record.metadata["WMOQualityLocationClass"] = self.parse_quality_location_class(six_group[3])

        return o, wind_source

    @with_exception_note("Error while parsing section 1")
    def _decode_section_1(self, record: ParentRecord, ascii_message: list[str], wind_source: str, o: int) -> int:
        # 1 1 1 Qd Qx
        if self.next_message_startswith(ascii_message, o, "111"):
            self.require_length(ascii_message[o], 5, "1 1 1 Qd Qx")
            quality: str | int | None = self.parse_quality_flag(ascii_message[o][3], "section 1")
            apply_to: bool | int = self.parse_quality_applies_to(ascii_message[o][4], 5)
            o += 1
            # 0 d d f f
            if self.next_message_startswith(ascii_message, o, "0"):
                self.require_length(ascii_message[o], 5, "0 d d f f")
                q = quality if apply_to is True or apply_to == 0 else None
                wind_units = self.parse_wind_type(wind_source)
                record.parameters["WindDirection"] = self.parse_wind_direction(ascii_message[o][1:3], Quality=q, WMOWindSource=wind_source)
                record.parameters["WindSpeed"] = self.parse_wind_speed(ascii_message[o][3:5], wind_units, Quality=q, WMOWindSource=wind_source)
                o += 1

            # 1 sn T T T
            if self.next_message_startswith(ascii_message, o, "1"):
                self.require_length(ascii_message[o], 5, "1 sn T T T")
                q = quality if apply_to is True or apply_to == 1 else None
                record.parameters["AirTemperature"] = self.parse_temperature(ascii_message[o][1:], Quality=q)
                o += 1

            # Either 2 9 U U U or 2 sn Td Td Td
            if self.next_message_startswith(ascii_message, o, "2"):
                self.require_length(ascii_message[o], 5, "2 ? ? ? ?")
                q = quality if apply_to is True or apply_to == 2 else None
                # 2 9 U U U
                if ascii_message[o][1] == "9":
                    record.parameters["RelativeHumidity"] = self.parse_rh(ascii_message[o][2:], Quality=q)
                # 2 sn Td Td Td
                else:
                    record.parameters["DewPointTemperature"] = self.parse_temperature(ascii_message[o][1:], Quality=q)
                o += 1

            # 3 P0 P0 P0 P0
            if self.next_message_startswith(ascii_message, o, "3"):
                self.require_length(ascii_message[o], 5, "3 P0 P0 P0 P0")
                q = quality if apply_to is True or apply_to == 3 else None
                record.parameters["AirPressure"] = self.parse_air_pressure(ascii_message[o][1:], Quality=q)
                o += 1

            # 4 P P P P
            if self.next_message_startswith(ascii_message, o, "4"):
                self.require_length(ascii_message[o], 5, "4 P P P P")
                q = quality if apply_to is True or apply_to == 4 else None
                record.parameters["AirPressureAtSeaLevel"] = self.parse_air_pressure(ascii_message[o][1:], Quality=q)
                o += 1

            # 5 a p p p
            if self.next_message_startswith(ascii_message, o, "5"):
                self.require_length(ascii_message[o], 5, "5 a p p p")
                q = quality if apply_to is True or apply_to == 5 else None
                record.parameters["WMOAirPressureCharacteristic"] = self.parse_wmo_air_pressure_change(ascii_message[o][1])
                record.parameters["AirPressureChange"] = self.parse_air_pressure_delta(ascii_message[o][2:], Quality=q)
                o += 1

        return o

    @with_exception_note("Error while parsing section 2")
    def _decode_section_2(self, record: ParentRecord, ascii_message: list[str], o: int) -> int:
        # 2 2 2 Qd Qx
        if self.next_message_startswith(ascii_message, o, "222"):
            self.require_length(ascii_message[o], 5, "2 2 2 Qd Qx")
            apply_to: bool | int = self.parse_quality_applies_to(ascii_message[o][4], 2)
            quality: str | int | None = self.parse_quality_flag(ascii_message[o][3], "second2")
            o += 1

            # 0 sn Tw Tw Tw
            if self.next_message_startswith(ascii_message, o, "0"):
                self.require_length(ascii_message[o], 5, "0 sn Tw Tw Tw")
                q = quality if apply_to is True or apply_to == 0 else None
                record.parameters["Temperature"] = self.parse_temperature(ascii_message[o][1:], Quality=q)
                o += 1

            # 1 Pwa Pwa Hwa Hwa
            if self.next_message_startswith(ascii_message, o, "1"):
                self.require_length(ascii_message[o], 5, "1 Pwa Pwa Hwa Hwa")
                q = quality if apply_to is True or apply_to == 1 else None
                record.parameters["WavePeriod"] = self.parse_wave_period(ascii_message[o][1:3], Quality=q)
                record.parameters["WaveHeight"] = self.parse_wave_height_half_m(ascii_message[o][3:], Quality=q)
                o += 1

            # 2 0 Pwa Pwa Pwa (do not override above unless its provided)
            if self.next_message_startswith(ascii_message, o, "20"):
                self.require_length(ascii_message[o], 5, "2 O Pwa Pwa Pwa")
                q = quality if apply_to is True or apply_to == 2 else None
                period = self.parse_wave_period(ascii_message[o][2:], Quality=q)
                if period is not None:
                    record.parameters["WavePeriod"] = period
                o += 1

            # 2 1 Hwa Hwa Hwa (do not override above unless it is provided)
            if self.next_message_startswith(ascii_message, o, "21"):
                self.require_length(ascii_message[o], 5, "2 1 Hwa Hwa Hwa")
                q = quality if apply_to is True or apply_to == 2 else None
                height = self.parse_wave_height(ascii_message[o][2:], Quality=q)
                if height is not None:
                    record.parameters["WaveHeight"] = height
                o += 1

        return o

    @with_exception_note("Error while parsing section 3")
    def _decode_section_3(self, record: ParentRecord, ascii_message: list[str], o: int) -> int:
        # 3 3 3 Qd1 Qd2
        if self.next_message_startswith(ascii_message, o, "333"):
            self.require_length(ascii_message[o], 5, "3 3 3 Qd1 Qd2")
            ts_quality = self.parse_quality_flag(ascii_message[o][3], "temperature/salinity")
            cur_quality = self.parse_quality_flag(ascii_message[o][4], "current")
            o += 1

            # 8 8 8 7 k2
            if self.next_message_startswith(ascii_message, o, "8887"):
                self.require_length(ascii_message[o], 5, "8 8 8 7 k2")
                sd_method = self.parse_salinity_depth_method(ascii_message[o][4])
                o += 1
                subrecord = None
                while o < len(ascii_message) - 1:

                    # these are the next expected sections, either 6 6 k6 9 k3 or 4 4 4
                    if ascii_message[o][0:2] == "66" or ascii_message[o] == "444":
                        break

                    # 2 zn zn zn zn
                    if self.next_message_startswith(ascii_message, o, "2"):
                        self.require_length(ascii_message[o], 5, "2 zn zn zn zn")
                        if subrecord is not None:
                            record.subrecords.append_to_record_set("PROFILE", 0, subrecord)
                        subrecord = ChildRecord()
                        subrecord.coordinates["Depth"] = self.parse_depth(ascii_message[o][1:])
                        o += 1

                    # 3 Tn Tn Tn Tn
                    elif self.next_message_startswith(ascii_message, o, "3"):
                        if subrecord is None:
                            raise AsciiDecodeError("Encountered 3 during a T/S profile with no corresponding 2 record", 2000)
                        self.require_length(ascii_message[o], 5, "3 Tn Tn Tn Tn")
                        subrecord.parameters["Temperature"] = self.parse_temperature_5_offset(ascii_message[o][1:], Quality=ts_quality)
                        o += 1

                    # 4 Sn Sn Sn Sn
                    elif self.next_message_startswith(ascii_message, o, "4"):
                        if subrecord is None:
                            raise AsciiDecodeError("Encountered 4 during a T/S profile with no corresponding 2 record", 2001)
                        self.require_length(ascii_message[o], 5, "4 Sn Sn Sn Sn")
                        subrecord.parameters["PracticalSalinity"] = self.parse_psu(ascii_message[o][1:], Quality=ts_quality, WMOSalinityDepthMeasurementMethod=sd_method)
                        o += 1

                    # unexpected sequence
                    else:
                        raise AsciiDecodeError(f"Invalid sequence during a T/S profile: [{ascii_message[o]}]", 2002)
                if subrecord is not None:
                    record.subrecords.append_to_record_set("PROFILE", 0, subrecord)

            # 6 6 k6 9 k3
            if self.next_message_startswith(ascii_message, o, "66"):
                self.require_length(ascii_message[o], 5, "6 6 k6 9 k3")
                pmr_method = self.parse_pmr_method(ascii_message[o][2])
                cm_duration = self.parse_cm_duration(ascii_message[o][4])
                o += 1

                # 2 zn zn zn zn and a following dn dn cn cn cn
                while self.next_message_startswith(ascii_message, o, "2"):
                    self.require_length(ascii_message[o], 5, "2 zn zn zn zn")
                    subrecord = ChildRecord()
                    subrecord.coordinates["Depth"] = self.parse_depth(ascii_message[o][1:])
                    try:
                        self.require_length(ascii_message[o+1], 5, "dn dn cn cn cn")
                        subrecord.parameters["CurrentDirection"] = self.parse_current_direction(ascii_message[o + 1][0:2], Quality=cur_quality, WMOPlatformMotionRemovalMethod=pmr_method, WMOCurrentMeasurementDuration=cm_duration)
                        subrecord.parameters["CurrentSpeed"] = self.parse_current_speed(ascii_message[o + 1][2:], Quality=cur_quality, WMOPlatformMotionRemovalMethod=pmr_method, WMOCurrentMeasurementDuration=cm_duration)
                    except IndexError:
                        raise AsciiDecodeError(f"Missing current direction/speed element following a depth element in the current profile", 2003)
                    record.subrecords.append_to_record_set("PROFILE", 1, subrecord)
                    o += 2
        return o

    @with_exception_note("Error while parsing section 4")
    def _decode_section_4(self, record: ParentRecord, ascii_message: list[str], received_date: AwareDateTime, o: int) -> int:
        # 4 4 4
        if self.next_message_startswith(ascii_message, o, "444"):
            o += 1

            # 1 QP Q2 QTW Q4
            if self.next_message_startswith(ascii_message, o, "1"):
                self.require_length(ascii_message[o], 5, "1 QP Q2 QTW Q4")
                record.metadata["WMOQualityPressure"] = self.parse_quality_pressure(ascii_message[o][1])
                record.metadata["WMOQualityHousekeeping"] = self.parse_quality_housekeeping(ascii_message[o][2])
                record.metadata["WMOQualityWaterTemperature"] = self.parse_quality_surface_temperature(ascii_message[o][3])
                record.metadata["WMOQualityAirTemperature"] = self.parse_quality_air_temperature(ascii_message[o][4])
                o += 1

            # 2 QN QL QA QZ
            if self.next_message_startswith(ascii_message, o, "2"):
                self.require_length(ascii_message[o], 5, "2 QN QL QA QZ")
                record.metadata["WMOQualitySatellite"] = self.parse_quality_satellite(ascii_message[o][1])
                record.metadata["WMOQualityLocation"] = ql = self.parse_quality_location(ascii_message[o][2])
                second_class = self.parse_quality_location_class(ascii_message[o][3])
                if second_class is not None:
                    if record.metadata.has_value("WMOQualityLocationClass") and record.metadata["WMOQualityLocationClass"].to_string() != second_class:
                        raise AsciiDecodeError("WMOQualityLocationClass disagreement", 2004)
                qz = self.parse_hsp_correction(ascii_message[o][4])
                if qz is not None and "PROFILE" in record.subrecords.record_sets:
                    for profile in record.subrecords.record_sets["PROFILE"].values():
                        for sr in profile.records:
                            if sr.coordinates.has_value("Depth"):
                                sr.coordinates["Depth"].metadata["WMOHSPCorrected"] = qz
                o += 1
                # QL = 1 or 2
                try:
                    # Y Y M M J and G G g g /
                    if ql == "1" or ql == 1:
                        self.require_length(ascii_message[o], 5, "Y Y M M J")
                        self.require_length(ascii_message[o+1], 4, "G G g g /")
                        record.metadata["LastKnownPositionTime"] = self.parse_message_time(ascii_message[o], ascii_message[o+1], received_date)
                        o += 2
                    # Qc La La La La La and Lo Lo Lo Lo Lo Lo
                    elif ql == "2" or ql == 2:
                        self.require_length(ascii_message[o], 3, "Qc La La La La La")
                        self.require_length(ascii_message[o+1], 3, "Lo Lo Lo Lo Lo Lo")
                        record.metadata["AlternativeSolutionLatitude"], record.metadata["AlternativeSolutionLongitude"] = self.parse_dd_coordinates(
                            ascii_message[o],
                            ascii_message[o+1],
                        )
                        o += 2
                except IndexError:
                    raise AsciiDecodeError("Expected two fields to follow a Ql of 1 or 2, they were not provided")

            # 3 Zh Zh Zh Zh
            if self.next_message_startswith(ascii_message, o, "3"):
                self.require_length(ascii_message[o], 5, "3 Zh Zh Zh Zh")
                record.parameters["HydrostaticPressure"] = self.parse_hydrostatic_pressure(ascii_message[o][1:])
                o += 1

            # 4 Zc Zc Zc Zc
            if self.next_message_startswith(ascii_message, o, "4"):
                self.require_length(ascii_message[o], 5, "4 Zc Zx Zc Zc")
                record.metadata["ThermistorCableLength"] = self.parse_thermistor_cable_length(ascii_message[o][1:])
                o += 1

            # 5 Bt Bt Xt Xt
            if self.next_message_startswith(ascii_message, o, "5"):
                self.require_length(ascii_message[o], 5, "5 Bt Bt Xt Xt")
                record.metadata["WMODataBuoyType"] = self.parse_code(ascii_message[o][1:3], missing=63)
                record.metadata["WMODrogueType"] = self.parse_code(ascii_message[o][3:5], missing=31)
                o += 1

            # 6 Ah Ah Ah An
            if self.next_message_startswith(ascii_message, o, "6"):
                self.require_length(ascii_message[o], 5, "6 Ah Ah Ah An")
                sd = self.parse_anemometer_height(ascii_message[o][1:4])
                at = self.parse_code(ascii_message[o][4], missing=15)
                for var in ("WindSpeed", "WindDirection"):
                    if record.parameters.has_value(var):
                        if sd is not None:
                            record.parameters[var].metadata["SensorDepth"] = sd
                        if at is not None:
                            record.parameters[var].metadata["WMOAnemometerType"] = at
                o += 1

            # 7 VB VB dB dB
            if self.next_message_startswith(ascii_message, o, "7"):
                self.require_length(ascii_message[o], 5, "7 VB VB dB dB")
                record.metadata["PlatformLastKnownSpeed"] = self.parse_ship_speed(ascii_message[o][1:3])
                record.metadata["PlatformLastKnownDirection"] = self.parse_ship_direction(ascii_message[o][3:5])
                o += 1

            # 8 Vi Vi Vi Vi (note can repeat multiple times)
            b_eng_status = ""
            while self.next_message_startswith(ascii_message, o, "8"):
                self.require_length(ascii_message[o], 1, "8 Vi Vi Vi Vi")
                b_eng_status += ascii_message[o][1:]
                o += 1
            if b_eng_status != "":
                record.metadata["BuoyEngineeringStatus"] = b_eng_status

            # 9 / Zd Zd Zd
            if self.next_message_startswith(ascii_message, o, "9"):
                self.require_length(ascii_message[o], 5, "9 / Zd Zd Zd")
                record.metadata["DrogueCableLength"] = self.parse_drogue_cable_length(ascii_message[o][2:])
                o += 1

        return o




class BathyJJVV(AsciiDecoder):

    @with_exception_note("Error while parsing JJVV")
    def _decode_message(self, record: ParentRecord, ascii_message: list[str], received_date: AwareDateTime):
        o = self._decode_section_1(record, ascii_message, received_date)
        o = self._decode_section_2(record, ascii_message, o)
        o = self._decode_section_3(record, ascii_message, o)
        self.validate_at_end(ascii_message[o:])

    @with_exception_note("Error while parsing section 1")
    def _decode_section_1(self, record: ParentRecord, ascii_message: list[str], received_date: AwareDateTime) -> int:
        # Note: Mi Mi Mj Mj already stripped out
        o = 0

        # Y Y M M J and G G g g /
        record.coordinates["Time"] = self.parse_message_time(ascii_message[o], ascii_message[o+1], received_date)
        o += 2

        # Qc La La La La La and Lo Lo Lo Lo Lo Lo
        record.coordinates["Latitude"], record.coordinates["Longitude"] = self.parse_dd_coordinates(ascii_message[o], ascii_message[o+1])
        o += 2


        # Next section may be (1) iu d d f f or (2) 4 sn T T T or (3) 8 8 8 8 k1
        # 0, 1, 3, 42, 43, and 8 are clear, or next section starts with a 4
        # iu d can be 4 [0|1] [0-9] [0-9] [0-9], wind speed in knots, direction 000 to 190, wind-speed 00 to 99 knots
        # 4 sn can be 4 [0|1] [0-9] [0-9] [0-9], temperature +/- 00.0 to 99.9 degrees C

        current = ascii_message[o]
        next = ascii_message[o+1]

        # iu d d f f
        if next.startswith("4") or current.startswith(("0", "1", "3", "42", "43")):
            self.require_length(ascii_message[o], 5, "iu d d f f")
            wmo_wind_source = ascii_message[o][0]
            units = self.parse_wind_type_1853(ascii_message[o][0])
            record.parameters["WindDirection"] = self.parse_wind_direction(ascii_message[o][1:3], WMOWindInstrumentType=wmo_wind_source)
            if current.endswith("99") and next.startswith("00"):
                self.require_length(ascii_message[o+1], 5, "0 0 f f f")
                record.parameters["WindSpeed"] = self.parse_wind_speed_ext(ascii_message[o+1][2:5], units, WMOWindInstrumentType=wmo_wind_source)
                o += 2
            else:
                record.parameters["WindSpeed"] = self.parse_wind_speed(ascii_message[o][3:5], units, WMOWindInstrumentType=wmo_wind_source)
                o += 1
        else:
            # this is an ambiguous case I think
            # current could be:
            #   (iu=4, land station or ship with certified instrument measured in knots)
            #   (dd=00 to 19)
            #   (ff=any)
            # or
            #   (4, indicating start of air temp)
            #   (sn=0 or 1, indicating positive or negative)
            #   (TTT=000 to 999)
            raise AsciiDecodeError("Ambiguous code form, unclear if [iu d d f f] or [4 sn T T T]", 3000)

        # 4 sn T T T
        if self.next_message_startswith(ascii_message, o, "4"):
            self.require_length(ascii_message[o], 5, "4 sn T T T")
            record.parameters["AirTemperature"] = self.parse_temperature(ascii_message[o][1:])
            o += 1
        return o

    @with_exception_note("Error while parsing section 2")
    def _decode_section_2(self, record: ParentRecord, ascii_message: list[str], o: int) -> int:
        # 8 8 8 8 k1
        if self.next_message_startswith(ascii_message, o, "8888"):
            self.require_length(ascii_message[o], 5, "8 8 8 8 k1")
            rs = RecordSet()
            record.subrecords.record_sets["PROFILE"] = {0: rs}
            rs.metadata["DigitizationMethod"] = self.parse_digitization_method(ascii_message[o][4])
            o += 1

            # IX IX IX XR XR
            self.require_length(ascii_message[o], 5, "IX IX IX XR XR")
            # TODO: review these, one is on the element and teh other on the platform

            wmo_itype = self.parse_xbt_instrument(ascii_message[o][0:3])
            wmo_rtype = self.parse_xbt_recorder(ascii_message[o][3:5])
            o += 1

            hundreds = "00"
            last_depth = None
            while not self.next_message_startswith(ascii_message, o, ("00000", "66666")):
                if self.next_message_startswith(ascii_message, o, "999"):
                    self.require_length(ascii_message[o], 5, "9 9 9 z z")
                    hundreds = ascii_message[o][3:5]
                    o += 1
                else:
                    self.require_length(ascii_message[o], 5, "zn zn Tn Tn Tn")
                    subrecord = ChildRecord()
                    subrecord.coordinates["Depth"] = last_depth = self.parse_depth(f"{hundreds}{ascii_message[o][0:2]}")
                    subrecord.parameters["Temperature"] = self.parse_temperature_5_offset(ascii_message[o][2:5], WMOProfileInstrumentType=wmo_itype, WMOProfileRecorderType=wmo_rtype)
                    rs.records.append(subrecord)
                    o += 1

            if self.next_message_startswith(ascii_message, o, "00000") and last_depth is not None:
                record.parameters["SeaDepth"] = last_depth
                o += 1
        return o

    @with_exception_note("Error while parsing section 3")
    def _decode_section_3(self, record: ParentRecord, ascii_message: list[str], o: int) -> int:

        # 6 6 6 6 6
        if self.next_message_startswith(ascii_message, o, "66666"):
            o += 1

        # 1 Zd Zd Zd Zd
        if self.next_message_startswith(ascii_message, o, "1"):
            self.require_length(ascii_message[o], 5, "1 Zd Zd Zd Zd")
            record.parameters["SeaDepth"] = self.parse_depth(ascii_message[o][1:])
            o += 1

        next = ascii_message[o+1]

        # k5 Dc Dc Vc Vc
        if next == "99999" or (not next.isdigit()) or len(next) < 5 or len(next) > 5:
            self.require_length(ascii_message[o], 5, "k5 Dc Dc Vc Vc")
            cur_method = self.parse_current_method(ascii_message[o][0])
            record.parameters["CurrentDirection"] = self.parse_current_direction(ascii_message[o][1:3], WMOCurrentMeasurementMethod=cur_method)
            record.parameters["CurrentSpeed"] = self.parse_current_speed_vc(ascii_message[o][3:5], WMOCurrentMeasurementMethod=cur_method)
            o += 1

        # D...D or 99999
        if self.next_message_startswith(ascii_message, o, "99999"):
            # A1 bw nb nb nb
            self.require_length(ascii_message[o+1], 5, "A1 bw nb nb nb")
            record.parameters["WMOID"] = self.parse_wmo_id(ascii_message[o+1])
            o += 2
        else:
            record.parameters["PlatformID"] = self.parse_platform_id(ascii_message[o])
            o += 1
            if o < len(ascii_message) - 1 and ascii_message[o] != "/////":
                self.require_length(ascii_message[o], 5, "A1 bw nb nb nb")
                record.parameters["WMOID"] = self.parse_wmo_id(ascii_message[o])
                o += 1
        return o


class TesacKKYY(AsciiDecoder):

    @with_exception_note("Error while parsing KKYY")
    def _decode_message(self, record: ParentRecord, ascii_message: list[str], received_date: AwareDateTime):
        o = self._decode_section_1(record, ascii_message, received_date)
        o = self._decode_section_2(record, ascii_message, o)
        o = self._decode_section_3(record, ascii_message, o)
        self.validate_at_end(ascii_message[o:])

    @with_exception_note("Error while parsing section 1")
    def _decode_section_1(self, record: ParentRecord, ascii_message: list[str], received_date: AwareDateTime) -> int:
        # Note: Mi Mi Mj Mj already stripped out
        o = 0

        # Y Y M M J and G G g g /
        record.coordinates["Time"] = self.parse_message_time(ascii_message[o], ascii_message[o+1], received_date)
        o += 2

        # Qc La La La La La and Lo Lo Lo Lo Lo Lo
        record.coordinates["Latitude"], record.coordinates["Longitude"] = self.parse_dd_coordinates(ascii_message[o], ascii_message[o+1])
        o += 2


        # Next section may be (1) iu d d f f or (2) 4 sn T T T or (3) 8 8 8 8 k1
        # 0, 1, 3, 42, 43, and 8 are clear, or next section starts with a 4
        # iu d can be 4 [0|1] [0-9] [0-9] [0-9], wind speed in knots, direction 000 to 190, wind-speed 00 to 99 knots
        # 4 sn can be 4 [0|1] [0-9] [0-9] [0-9], temperature +/- 00.0 to 99.9 degrees C

        current = ascii_message[o]
        next = ascii_message[o+1]

        # iu d d f f
        if next.startswith("4") or current.startswith(("0", "1", "3", "42", "43")):
            self.require_length(ascii_message[o], 5, "iu d d f f")
            wmo_wind_source = ascii_message[o][0]
            units = self.parse_wind_type_1853(ascii_message[o][0])
            record.parameters["WindDirection"] = self.parse_wind_direction(ascii_message[o][1:3])
            if current.endswith("99") and next.startswith("00"):
                record.parameters["WindSpeed"] = self.parse_wind_speed_ext(ascii_message[o+1][2:5], units, WMOWindInstrumentType=wmo_wind_source)
                o += 1
            else:
                record.parameters["WindSpeed"] = self.parse_wind_speed(ascii_message[o][3:5], units, WMOWindInstrumentType=wmo_wind_source)
            o += 1
        else:
            # this is an ambiguous case I think
            # current could be:
            #   (iu=4, land station or ship with certified instrument measured in knots)
            #   (dd=00 to 19)
            #   (ff=any)
            # or
            #   (4, indicating start of air temp)
            #   (sn=0 or 1, indicating positive or negative)
            #   (TTT=000 to 999)
            raise AsciiDecodeError("Ambiguous code form, unclear if [iu d d f f] or [4 sn T T T]", 3000)

        # 4 sn T T T
        if self.next_message_startswith(ascii_message, o, "4"):
            self.require_length(ascii_message[o], 5, "4 sn T T T")
            record.parameters["AirTemperature"] = self.parse_temperature(ascii_message[o][1:])
            o += 1
        return o

    @with_exception_note("Error while parsing section 2")
    def _decode_section_2(self, record: ParentRecord, ascii_message: list[str], o: int) -> int:
        # 8 8 8 k1 k2
        if self.next_message_startswith(ascii_message, o, "888"):
            self.require_length(ascii_message[o], 5, "8 8 8 k1 k2")
            rs = RecordSet()
            record.subrecords.record_sets["PROFILE"] = {0: rs}
            rs.metadata["DigitizationMethod"] = self.parse_digitization_method(ascii_message[o][3])
            sd_method = self.parse_salinity_depth_method(ascii_message[o][4])
            o += 1

            # IX IX IX XR XR
            self.require_length(ascii_message[o], 5, "IX IX IX XR XR")
            wmo_itype = self.parse_xbt_instrument(ascii_message[o][0:3])
            wmo_rtype = self.parse_xbt_recorder(ascii_message[o][3:5])
            o += 1

            last_depth = None
            subrecord = None
            while o < len(ascii_message):
                if self.next_message_startswith(ascii_message, o, ("00000", "66")):
                    break
                elif self.next_message_startswith(ascii_message, o, "2"):
                    self.require_length(ascii_message[o], 5, "2 zn zn zn zn")
                    if subrecord is not None:
                        rs.records.append(subrecord)
                    subrecord = ChildRecord()
                    subrecord.coordinates["Depth"] = last_depth = self.parse_depth(ascii_message[o][1:])
                elif self.next_message_startswith(ascii_message, o, "3"):
                    if subrecord is None:
                        raise AsciiDecodeError("Invalid location for temperature in profile, expecting depth first", 4001)
                    self.require_length(ascii_message[o], 5, "3 Tn Tn Tn Tn")
                    subrecord.parameters["Temperature"] = self.parse_temperature_5_offset(ascii_message[o][1:], WMOProfileInstrumentType=wmo_itype, WMOProfileRecorderType=wmo_rtype)
                elif self.next_message_startswith(ascii_message, o, "4"):
                    if subrecord is None:
                        raise AsciiDecodeError("Invalid location for salinity in profile, expecting depth first", 4002)
                    self.require_length(ascii_message[o], 5, "4 Sn Sn Sn Sn")
                    subrecord.parameters["PracticalSalinity"] = self.parse_psu(ascii_message[o][1:], WMOProfileInstrumentType=wmo_itype, WMOProfileRecorderType=wmo_rtype)
                else:
                    raise AsciiDecodeError("Invalid element for t/s profile, found: [{ascii_message[o]}] expecting 2 zn, 3 Tn, 4 Sn, 00000, or 66...", 4000)

            if subrecord is not None:
                rs.records.append(subrecord)

            # 0 0 0 0 0
            if self.next_message_startswith(ascii_message, o, "00000") and last_depth is not None:
                record.parameters["SeaDepth"] = last_depth
                o += 1
        return o

    @with_exception_note("Error while parsing section 3")
    def _decode_section_3(self, record: ParentRecord, ascii_message: list[str], o: int) -> int:
        # 6 6 k6 k4 k3
        if self.next_message_startswith(ascii_message, o, "66"):
            self.require_length(ascii_message[o], 5, "6 6 k6 k4 k3")
            pmr_method = self.parse_pmr_method(ascii_message[o][2])
            cm_duration = self.parse_cm_duration(ascii_message[o][4], ascii_message[o][3])

            # 2 zn zn zn zn and a following dn dn cn cn cn
            while self.next_message_startswith(ascii_message, o, "2"):
                self.require_length(ascii_message[o], 5, "2 zn zn zn zn")
                subrecord = ChildRecord()
                subrecord.coordinates["Depth"] = self.parse_depth(ascii_message[o][1:])
                try:
                    self.require_length(ascii_message[o + 1], 5, "dn dn cn cn cn")
                    subrecord.parameters["CurrentDirection"] = self.parse_current_direction(
                        ascii_message[o + 1][0:2],
                        WMOPlatformMotionRemovalMethod=pmr_method,
                        WMOCurrentMeasurementDuration=cm_duration
                    )
                    subrecord.parameters["CurrentSpeed"] = self.parse_current_speed(
                        ascii_message[o + 1][2:],
                        units="knots",
                        WMOPlatformMotionRemovalMethod=pmr_method,
                        WMOCurrentMeasurementDuration=cm_duration
                    )
                except IndexError:
                    raise AsciiDecodeError(f"Missing current direction/speed element following a depth element in the current profile", 4003)
                record.subrecords.append_to_record_set("PROFILE", 1, subrecord)
                o += 2
        return o

    @with_exception_note("Error while parsing section 4")
    def _decode_section_4(self, record: ParentRecord, ascii_message: list[str], o: int) -> int:

        # 5 5 5 5 5
        if self.next_message_startswith(ascii_message, o, "55555"):
            o += 1

        # 1 Zd Zd Zd Zd
        if self.next_message_startswith(ascii_message, o, "1"):
            self.require_length(ascii_message[o], 5, "1 Zd Zd Zd Zd")
            record.parameters["SeaDepth"] = self.parse_depth(ascii_message[o][1:])
            o += 1

        # D...D or 99999
        if self.next_message_startswith(ascii_message, o, "99999"):
            # A1 bw nb nb nb
            self.require_length(ascii_message[o+1], 5, "A1 bw nb nb nb")
            record.parameters["WMOID"] = self.parse_wmo_id(ascii_message[o+1])
            o += 2
        else:
            record.parameters["PlatformID"] = self.parse_platform_id(ascii_message[o])
            o += 1
            if o < len(ascii_message) - 1 and ascii_message[o] != "/////":
                self.require_length(ascii_message[o], 5, "A1 bw nb nb nb")
                record.parameters["WMOID"] = self.parse_wmo_id(ascii_message[o])
                o += 1
        return o





class TrackObNNXX(AsciiDecoder):
    # Not needed for GTSPP
    pass


class WaveObMMXX(AsciiDecoder):
    # Not needed for GTSPP
    pass