"""Helper functions to apply the ocean math equations to OCPROC2 records.

    In general, one would provide an OCProc2 record or a set of
    OCProc2 elements corresponding to the necessary parameters to
    apply the equations of state.

    This module extracts and converts the values to the appropriate
    units and scales, then applies an appropriate equation to obtain a
    result.
"""
import medsutil.ocproc2 as ocproc2
import typing as t
import medsutil.seawater as seawater
from medsutil.awaretime import AwareDateTime
from medsutil.ocproc2.util import check_quality, RequiredQuality
from medsutil.seawater import TemperatureScale
from medsutil.units.units import convert
import medsutil.geodesy as geodesy
import medsutil.math as amath

ValQualUnits = tuple[t.Any, int, t.Optional[str]]


def extract_parameter_value(parameter_name: str,
                            element: ocproc2.SingleElement,
                            units: str | None,
                            required_quality: RequiredQuality = RequiredQuality.GOOD_VALUE,
                            obs_date: AwareDateTime | None = None) -> amath.AnyNumber | None:
    if units is not None:
        required_quality = required_quality | RequiredQuality.HAS_UNITS
    check_quality(element, required_quality)
    if parameter_name == "Temperature":
        return get_temperature(element, obs_date=obs_date, units=units or "degrees_C")
    else:
        return element.to_numeric(units)


def get_density(temperature: ocproc2.SingleElement | None,
                practical_salinity: t.Optional[ocproc2.SingleElement] = None,
                pressure: amath.AnyNumber | None = None,
                depth: amath.AnyNumber | None = None,
                latitude: amath.AnyNumber | None = None,
                obs_date: AwareDateTime | None = None,
                units: t.Optional[str] = "kg m-3") -> amath.AnyNumber | None:
    """Calculate the density of seawater from the relevant OCPROC2 elements. See density_at_depth() for more details."""
    if pressure is None:
        if depth is not None and latitude is not None:
            pressure = seawater.eos80_pressure(depth, latitude)
    t68 = get_temperature(temperature, obs_date=obs_date, temperature_scale=TemperatureScale.TS_1968)
    if pressure is not None and t68 is not None and not practical_salinity.is_empty():
        rho = seawater.eos80_density_at_depth_t68(
            salinity=practical_salinity.to_numeric("0.001"),
            temperature_ipts68=t68,
            pressure=pressure,
        )
        if units != "kg m-3":
            return convert(rho, "kg m-3", units)
        return rho
    return None

def get_temperature(temperature: ocproc2.SingleElement | None,
                    obs_date: ocproc2.SingleElement | AwareDateTime | None = None,
                    units: str = "degrees_C",
                    temperature_scale: TemperatureScale = TemperatureScale.TS_1990) -> amath.AnyNumber | None:
    """Extract the temperature from a temperature element in a given unit and temperature scale.
        The units of the temperature element are assumed to match if they are not set.
        The temperature scale of the temperature element is inferred from the observation date, if
        present, otherwise it is assumed to be ITS-90."""
    if temperature is None or temperature.is_empty():
        return None

    temp_val = temperature.to_numeric(units)
    temp_units = temperature.metadata.best("Units", '', coerce=str)
    temp_scale_str = temperature.metadata.best('TemperatureScale', None, coerce=str)

    if isinstance(obs_date, ocproc2.AbstractElement):
        obs_date_val = obs_date.to_datetime() if obs_date.is_iso_datetime() else None
    else:
        obs_date_val = obs_date
    measured_temperature_scale = seawater.temperature_scale_in_use_on(obs_date_val, temp_scale_str)
    return seawater.eos80_convert_temperature(
        temperature=temp_val,
        input_scale=measured_temperature_scale,
        input_units=temp_units,
        output_units=units,
        output_scale=temperature_scale
    )


def get_freezing_point_from_psal(practical_salinity: ocproc2.SingleElement | None = None,
                                 pressure_dbar: amath.AnyNumber | None = None,
                                 latitude_dd: amath.AnyNumber | None = None,
                                 units: str = "degrees_C",
                                 temperature_scale: TemperatureScale = TemperatureScale.TS_1990) -> amath.AnyNumber | None:
    """Calculate the freezing point of seawater from individual elements corresponding to the necessary information.
        The result is in the given units and temperature scale.
    """
    if pressure_dbar is None or latitude_dd is None or practical_salinity is None:
        return None
    else:
        if practical_salinity.is_empty() or not practical_salinity.is_numeric():
            return None
        psal = practical_salinity.to_numeric("0.001")
        if not amath.between(26, psal, 35):
            return None
        return seawater.eos80_convert_temperature(
            temperature=seawater.eos80_freezing_point_t68(psal, pressure_dbar),
            input_units="degrees_C",
            input_scale=TemperatureScale.TS_1968,
            output_units=units,
            output_scale=temperature_scale
        )





















def calc_speed_record(record1: ocproc2.BaseRecord, record2: ocproc2.BaseRecord, units: str = "m s-1") -> ValQualUnits:
    """Calculate the speed between two records."""
    return calc_speed(
        record1.coordinates.ideal('Latitude'),
        record1.coordinates.ideal('Longitude'),
        record1.coordinates.ideal('Time'),
        record2.coordinates.ideal('Latitude'),
        record2.coordinates.ideal('Longitude'),
        record2.coordinates.ideal('Time'),
        units
    )


def calc_distance_record(record1: ocproc2.BaseRecord, record2: ocproc2.BaseRecord, units: str = "m") -> ValQualUnits:
    """Calculate the distance between two records."""
    return calc_distance(
        record1.coordinates.ideal('Latitude'),
        record1.coordinates.ideal('Longitude'),
        record2.coordinates.ideal('Latitude'),
        record2.coordinates.ideal('Longitude'),
        units
    )


def calc_speed(latitude1: ocproc2.SingleElement | None,
               longitude1: ocproc2.SingleElement | None,
               time1: ocproc2.SingleElement | None,
               latitude2: ocproc2.SingleElement | None,
               longitude2: ocproc2.SingleElement | None,
               time2: ocproc2.SingleElement | None,
               units: str = "m s-1") -> ValQualUnits:
    """Calculate the speed between two points."""
    if (time1 is None
            or time1.is_empty()
            or (not time1.is_iso_datetime())
            or time2 is None
            or time2.is_empty()
            or (not time2.is_iso_datetime())):
        return None, 9, None
    time_elapsed = (time2.to_datetime() - time1.to_datetime()).total_seconds()
    distance, d_qc, _ = calc_distance(latitude1, longitude1, latitude2, longitude2, "m")
    return (
        convert(distance / time_elapsed, "m s-1", units),
        _compress_quality_scores(d_qc, time1.working_quality(), time2.working_quality()),
        units
    )


def calc_distance(latitude1: ocproc2.SingleElement | None,
                  longitude1: ocproc2.SingleElement | None,
                  latitude2: ocproc2.SingleElement | None,
                  longitude2: ocproc2.SingleElement | None,
                  units: str = "m") -> ValQualUnits:
    """Calculate the distance between two points."""
    if (latitude1 is None
            or latitude2 is None
            or longitude1 is None
            or longitude2 is None
            or latitude1.is_empty()
            or latitude2.is_empty()
            or longitude2.is_empty()
            or longitude1.is_empty()):
        return None, 9, None
    return (
        convert(geodesy.great_circle_distance(
            (latitude1.to_ufloat(), longitude1.to_ufloat()),
            (latitude2.to_ufloat(), longitude2.to_ufloat())
        ), "m", units),
        _compress_quality_scores(
            latitude1.working_quality(),
            latitude2.working_quality(),
            longitude2.working_quality(),
            longitude1.working_quality()
        ),
        units
    )



def calc_pressure_record(level_record: ocproc2.BaseRecord,
                         position_record: t.Optional[ocproc2.BaseRecord] = None,
                         units: t.Optional[str] = None) -> ValQualUnits:
    """Calculate the pressure from a level record (containing Pressure or Depth) and a position record
        (containing Latitude). See calc_pressure() for details. """
    return calc_pressure(
        pressure=level_record.coordinates.ideal('Pressure'),
        depth=level_record.coordinates.ideal('Depth'),
        latitude=position_record.coordinates.ideal('Latitude') if position_record else None,
        units=units
    )


def calc_pressure(pressure: t.Optional[ocproc2.SingleElement] = None,
                  depth: t.Optional[ocproc2.SingleElement] = None,
                  latitude: t.Optional[ocproc2.SingleElement] = None,
                  units: t.Optional[str] = None) -> ValQualUnits:
    """Calculate the pressure from appropriate variables in single elements. Prioritize using pressure if available
        and sensible, otherwise uses depth. Returns in the given units."""
    if pressure is not None and not pressure.is_empty():
        return (
            pressure.to_ufloat(units=units),
            pressure.working_quality(),
            units or pressure.units()
        )
    if depth is not None and latitude is not None and not (depth.is_empty() or latitude.is_empty()):
        d_float = depth.to_ufloat(units='m')
        lat_float = latitude.to_ufloat()
        return (
            pressure_from_depth(d_float, lat_float, units),
            _compress_quality_scores(depth.working_quality(), latitude.working_quality()),
            units or 'dbar'
        )
    return None, 9, None



def _compress_quality_scores(*qc_scores) -> int:
    """Converts a set of quality scores into a single score representing the worst value."""
    qc_scores = [x for x in qc_scores if x is not None]
    for x in [9, 4, 3, 2]:
        if any(y == x for y in qc_scores):
            return x
    if all(y == 1 or y == 5 for y in qc_scores):
        return 1
    return 0


def density_at_depth(pressure: amath.AnyNumber,
                     temperature: amath.AnyNumber,
                     absolute_salinity: t.Optional[amath.AnyNumber] = None,
                     practical_salinity: t.Optional[amath.AnyNumber] = None,
                     latitude: t.Optional[amath.AnyNumber] = None,
                     longitude: t.Optional[amath.AnyNumber] = None,
                     units: t.Optional[str] = None) -> t.Optional[amath.AnyNumber]:
    """Calculate density at depth from actual numbers.

        Delegates to gsw.rho_t_exact() if available, otherwise to seawater.dens() (or our own equivalent).

        Note that, at the moment, it is an issue if the practical salinity is not provided and gsw is not installed
        as there is no seawater conversion function for converting practical salinity to absolute salinity in seawater
        or our substitute methods. This should be fixed one day by bringing in a Python-only implementation of
        gsw.SP_from_SA().
    """
    if absolute_salinity is None and practical_salinity is None:
        return None
    absolute_salinity, practical_salinity = _fix_salinities(absolute_salinity, practical_salinity, latitude, longitude,
                                                            pressure)
    density = None
    calc_units = "kg m-3"
    if gsw is not None and absolute_salinity is not None:
        # TODO: calculate uncertainty associated with this value
        density = gsw.rho_t_exact(amath.to_float(absolute_salinity), amath.to_float(temperature), amath.to_float(pressure))
    elif practical_salinity is not None:
        actual_temp = convert_temperature_scale(temperature, 'ITS-90', 'IPTS-68')
        if actual_temp is not None:
            density = seawater_sub.eos80_density_at_depth_t68(practical_salinity, actual_temp, pressure)
    return convert(density, calc_units, units)


def depth_from_pressure(pressure: amath.AnyNumber,
                        latitude: amath.AnyNumber,
                        units: t.Optional[str] = None) -> t.Optional[amath.AnyNumber]:
    """Determine the depth given the pressure and latitude.

        Delegates to gsw.z_from_p() when installed, otherwise seawater.dpth() or our own local equivalent.
    """
    calc_units = 'm'
    if gsw is not None:
        # TODO: calculate uncertainty associated with this value
        depth = gsw.z_from_p(amath.to_float(pressure), amath.to_float(latitude))
    else:
        depth = seawater_sub.eos80_depth(pressure, latitude)
    return convert(depth, calc_units, units)


def pressure_from_depth(depth: amath.AnyNumber,
                        latitude: amath.AnyNumber,
                        units: t.Optional[str] = None) -> t.Optional[amath.AnyNumber]:
    """Calculate pressure from depth and latitude.

        Uses gsw.p_from_z if available, otherwise seawater.pres().
    """
    calc_units = 'dbar'
    # Primary option is to use the GSW TEOS-10 model
    if gsw is not None:
        # TODO: calculate uncertainty associated with this value
        pressure = gsw.p_from_z(amath.to_float(depth) * -1, amath.to_float(latitude))
    # Third option, use our built-in fall-back (also EOS 80 based)
    else:
        pressure = seawater_sub.eos80_pressure(depth, latitude)
    return convert(pressure, calc_units, units)
