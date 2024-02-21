"""Helper functions to apply the ocean math equations to OCPROC2 records.

    In general, one would provide an OCProc2 record or a set of
    OCProc2 elements corresponding to the necessary parameters to
    apply the equations of state.

    This module extracts and converts the values to the appropriate
    units and scales, then applies an appropriate equation to obtain a
    result.
"""
import datetime
import cnodc.ocproc2 as ocproc2
import typing as t
import cnodc.ocean_math.umath_wrapper as umath
import cnodc.ocean_math.seawater as seawater_sub
from cnodc.units.units import convert
import cnodc.ocean_math.geodesy as geodesy

VAL_QC_UNITS = tuple[t.Optional[umath.FLOAT], int, t.Optional[str]]

ITS90_START = datetime.datetime(1990, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
IPTS68_START = datetime.datetime(1968, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)

try:
    import gsw
except ModuleNotFoundError:
    gsw = None

try:
    import seawater
except ModuleNotFoundError:
    seawater = None


def calc_speed_record(record1: ocproc2.BaseRecord, record2: ocproc2.BaseRecord, units: str = "m s-1") -> VAL_QC_UNITS:
    """Calculate the speed between two records."""
    return calc_speed(
        record1.coordinates.get('Latitude'),
        record1.coordinates.get('Longitude'),
        record1.coordinates.get('Time'),
        record2.coordinates.get('Latitude'),
        record2.coordinates.get('Longitude'),
        record2.coordinates.get('Time'),
        units
    )


def calc_distance_record(record1: ocproc2.BaseRecord, record2: ocproc2.BaseRecord, units: str = "m") -> VAL_QC_UNITS:
    """Calculate the distance between two records."""
    return calc_distance(
        record1.coordinates.get('Latitude'),
        record1.coordinates.get('Longitude'),
        record2.coordinates.get('Latitude'),
        record2.coordinates.get('Longitude'),
        units
    )


def calc_speed(latitude1: ocproc2.SingleElement,
               longitude1: ocproc2.SingleElement,
               time1: ocproc2.SingleElement,
               latitude2: ocproc2.SingleElement,
               longitude2: ocproc2.SingleElement,
               time2: ocproc2.SingleElement,
               units: str = "m s-1") -> VAL_QC_UNITS:
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


def calc_distance(latitude1: ocproc2.SingleElement,
                  longitude1: ocproc2.SingleElement,
                  latitude2: ocproc2.SingleElement,
                  longitude2: ocproc2.SingleElement,
                  units: str = "m") -> VAL_QC_UNITS:
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
        convert(geodesy.uhaversine(
            (latitude1.to_float_with_uncertainty(), longitude1.to_float_with_uncertainty()),
            (latitude2.to_float_with_uncertainty(), longitude2.to_float_with_uncertainty())
        ), "m", units),
        _compress_quality_scores(
            latitude1.working_quality(),
            latitude2.working_quality(),
            longitude2.working_quality(),
            longitude1.working_quality()
        ),
        units
    )


def get_temperature(temperature: t.Optional[ocproc2.SingleElement],
                    units: str,
                    obs_date: t.Optional[ocproc2.SingleElement] = None,
                    temperature_scale: str = "ITS-90"):
    """Extract the temperature from a temperature element in a given unit and temperature scale.
        The units of the temperature element are assumed to match if they are not set.
        The temperature scale of the temperature element is inferred from the observation date, if
        present, otherwise it is assumed to be ITS-90."""
    if temperature is None or temperature.is_empty():
        return None
    temp_val = temperature.to_float_with_uncertainty(units)
    c_temp_scale = temperature.metadata.best_value('TemperatureScale', None)
    if c_temp_scale is None:
        if obs_date is not None and not obs_date.is_empty():
            obs_date_val = obs_date.to_datetime()
            if obs_date_val < IPTS68_START:
                c_temp_scale = 'IPTS-48'
            elif obs_date_val < ITS90_START:
                c_temp_scale = 'IPTS-68'
            else:
                c_temp_scale = 'ITS-90'
        else:
            c_temp_scale = 'ITS-90'
    return convert_temperature_scale(temp_val, c_temp_scale, temperature_scale)


def calc_freezing_point_record(level_record: ocproc2.BaseRecord,
                               position_record: t.Optional[ocproc2.BaseRecord] = None,
                               units: t.Optional[str] = None,
                               temperature_scale: t.Optional[str] = None) -> VAL_QC_UNITS:
    """Calculate the freezing point of seawater from a level record (containing one of Depth or Pressure
        and one of PracticalSalinity or AbsoluteSalinity) and a position record (containing Latitude and Longitude).
        See calc_freezing_point for details.
    """
    return calc_freezing_point(
        pressure=level_record.coordinates.get('Pressure'),
        depth=level_record.coordinates.get('Depth'),
        practical_salinity=level_record.parameters.get('PracticalSalinity'),
        absolute_salinity=level_record.parameters.get('AbsoluteSalinity'),
        latitude=position_record.coordinates.get('Latitude') if position_record else None,
        longitude=position_record.coordinates.get('Longitude') if position_record else None,
        units=units,
        temperature_scale=temperature_scale
    )


def calc_freezing_point(pressure: t.Optional[ocproc2.SingleElement] = None,
                        depth: t.Optional[ocproc2.SingleElement] = None,
                        latitude: t.Optional[ocproc2.SingleElement] = None,
                        longitude: t.Optional[ocproc2.SingleElement] = None,
                        practical_salinity: t.Optional[ocproc2.SingleElement] = None,
                        absolute_salinity: t.Optional[ocproc2.SingleElement] = None,
                        units: t.Optional[str] = None,
                        temperature_scale: t.Optional[str] = None) -> VAL_QC_UNITS:
    """Calculate the freezing point of seawater from individual elements corresponding to the necessary information.
        The result is in the given units and temperature scale and the return value includes the actual value,
        the QC flag, and the units. See freezing_point for the process.
    """
    p_val, p_qual, _ = calc_pressure(pressure, depth, latitude)
    if p_val is not None:
        return (
            freezing_point(
                pressure=p_val,
                latitude=latitude.to_float_with_uncertainty() if latitude is not None and not latitude.is_empty() else None,
                longitude=longitude.to_float_with_uncertainty() if longitude is not None and not longitude.is_empty() else None,
                absolute_salinity=absolute_salinity.to_float_with_uncertainty(
                    'g kg-1') if absolute_salinity is not None and not absolute_salinity.is_empty() else None,
                practical_salinity=practical_salinity.to_float_with_uncertainty(
                    '0.001') if practical_salinity is not None and not practical_salinity.is_empty() else None,
                units=units,
                temperature_scale=temperature_scale
            ),
            _compress_quality_scores(p_qual,
                                     absolute_salinity.working_quality() if absolute_salinity is not None else None,
                                     practical_salinity.working_quality() if practical_salinity is not None else None),
            units or '°C'
        )
    return None, 9, None


def calc_pressure_record(level_record: ocproc2.BaseRecord,
                         position_record: t.Optional[ocproc2.BaseRecord] = None,
                         units: t.Optional[str] = None) -> VAL_QC_UNITS:
    """Calculate the pressure from a level record (containing Pressure or Depth) and a position record
        (containing Latitude). See calc_pressure() for details. """
    return calc_pressure(
        pressure=level_record.coordinates.get('Pressure'),
        depth=level_record.coordinates.get('Depth'),
        latitude=position_record.coordinates.get('Latitude') if position_record else None,
        units=units
    )


def calc_pressure(pressure: t.Optional[ocproc2.SingleElement] = None,
                  depth: t.Optional[ocproc2.SingleElement] = None,
                  latitude: t.Optional[ocproc2.SingleElement] = None,
                  units: t.Optional[str] = None) -> VAL_QC_UNITS:
    """Calculate the pressure from appropriate variables in single elements. Prioritize using pressure if available
        and sensible, otherwise uses depth. Returns in the given units."""
    if pressure is not None and not pressure.is_empty():
        return (
            pressure.to_float_with_uncertainty(units=units),
            pressure.working_quality(),
            units or pressure.units()
        )
    if depth is not None and latitude is not None and not (depth.is_empty() or latitude.is_empty()):
        d_float = depth.to_float_with_uncertainty(units='m')
        lat_float = latitude.to_float_with_uncertainty()
        return (
            pressure_from_depth(d_float, lat_float, units),
            _compress_quality_scores(depth.working_quality(), latitude.working_quality()),
            units or 'dbar'
        )
    return None, 9, None


def calc_density_record(level_record: ocproc2.BaseRecord,
                        position_record: t.Optional[ocproc2.BaseRecord] = None,
                        units: t.Optional[str] = None) -> VAL_QC_UNITS:
    """Calculate the density of sea water from a level record (containing Temperature, Pressure or Depth, and
        AbsoluteSalinity or PracticalSalinity) and a position record (containing Latitude and Longitude). See
        calc_density() for more details."""
    return calc_density(
        temperature=level_record.parameters.get('Temperature'),
        pressure=level_record.coordinates.get('Pressure'),
        depth=level_record.coordinates.get('Depth'),
        absolute_salinity=level_record.parameters.get('AbsoluteSalinity'),
        practical_salinity=level_record.parameters.get('PracticalSalinity'),
        latitude=position_record.coordinates.get('Latitude') if position_record else None,
        longitude=position_record.coordinates.get('Longitude') if position_record else None,
        units=units
    )


def calc_density(temperature: ocproc2.SingleElement,
                 pressure: t.Optional[ocproc2.SingleElement] = None,
                 depth: t.Optional[ocproc2.SingleElement] = None,
                 absolute_salinity: t.Optional[ocproc2.SingleElement] = None,
                 practical_salinity: t.Optional[ocproc2.SingleElement] = None,
                 latitude: t.Optional[ocproc2.SingleElement] = None,
                 longitude: t.Optional[ocproc2.SingleElement] = None,
                 units: t.Optional[str] = None) -> VAL_QC_UNITS:
    """Calculate the density of seawater from the relevant OCPROC2 elements. See density_at_depth() for more details."""
    p, p_q, _ = calc_pressure(pressure, depth, latitude, 'dbar')
    t90 = get_temperature(temperature, '°C', temperature_scale='ITS-90')
    if p is not None and t90 is not None:
        return (
            density_at_depth(
                pressure=p,
                temperature=t90,
                practical_salinity=practical_salinity.to_float_with_uncertainty(
                    '0.001') if practical_salinity is not None and not practical_salinity.is_empty() else None,
                absolute_salinity=absolute_salinity.to_float_with_uncertainty(
                    'g kg-1') if absolute_salinity is not None and not absolute_salinity.is_empty() else None,
                latitude=latitude.to_float_with_uncertainty() if latitude is not None and not latitude.is_empty() else None,
                longitude=longitude.to_float_with_uncertainty() if longitude is not None and not longitude.is_empty() else None,
                units=units
            ),
            _compress_quality_scores(
                p_q,
                temperature.working_quality(),
                absolute_salinity.working_quality() if absolute_salinity is not None else None,
                practical_salinity.working_quality() if practical_salinity is not None else None
            ),
            units or 'kg m-3'
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


def density_at_depth(pressure: umath.FLOAT,
                     temperature: umath.FLOAT,
                     absolute_salinity: t.Optional[umath.FLOAT] = None,
                     practical_salinity: t.Optional[umath.FLOAT] = None,
                     latitude: t.Optional[umath.FLOAT] = None,
                     longitude: t.Optional[umath.FLOAT] = None,
                     units: t.Optional[str] = None) -> t.Optional[umath.FLOAT]:
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
    calc_units = "kg m-3"
    if gsw is not None and absolute_salinity is not None:
        # TODO: calculate uncertainty associated with this value
        density = gsw.rho_t_exact(umath.to_float(absolute_salinity), umath.to_float(temperature),
                                  umath.to_float(pressure))
    elif seawater is not None and practical_salinity is not None:
        # TODO: calculate uncertainty associated with this value
        density = seawater.dens(umath.to_float(practical_salinity), umath.to_float(temperature),
                                umath.to_float(pressure))
    elif practical_salinity is not None:
        density = seawater_sub.eos80_density_at_depth_t68(practical_salinity,
                                                          convert_temperature_scale(temperature, 'ITS-90', 'IPTS-68'),
                                                          pressure)
    else:
        density = None
    return convert(density, calc_units, units)


def freezing_point(pressure: umath.FLOAT,
                   absolute_salinity: t.Optional[umath.FLOAT] = None,
                   practical_salinity: t.Optional[umath.FLOAT] = None,
                   latitude: t.Optional[umath.FLOAT] = None,
                   longitude: t.Optional[umath.FLOAT] = None,
                   units: t.Optional[str] = None,
                   temperature_scale: t.Optional[str] = None) -> t.Optional[umath.FLOAT]:
    """Calculate freezing point from actual numbers.

        Delegates to gsw.t_freezing() if available and assumes saturation_fraction=1 (this gives lower results in
        testing, but we should look if we can use the actual saturation if available).

        Otherwise, delegates to seawater.fp() or our own implementation of it.
    """
    if pressure is None:
        return None
    if absolute_salinity is None and practical_salinity is None:
        return None
    calc_units = '°C'
    calc_temp_scale = 'ITS-90'
    absolute_salinity, practical_salinity = _fix_salinities(absolute_salinity, practical_salinity, latitude, longitude,
                                                            pressure)
    if gsw is not None and absolute_salinity is not None:
        # TODO: saturation_fraction from DO if available?
        # TODO: confirm if we have absolute or sea pressures (i.e. are we subtracting 10.1325 dbar?)
        # TODO: calculate uncertainty associated with this value
        fp = gsw.t_freezing(umath.to_float(absolute_salinity), umath.to_float(pressure), 1)
    elif seawater is not None and practical_salinity is not None:
        # TODO: calculate uncertainty associated with this value
        fp = seawater.fp(umath.to_float(practical_salinity), umath.to_float(pressure))
    elif practical_salinity is not None:
        fp = seawater_sub.eos80_freezing_point_t68(practical_salinity, pressure)
        calc_temp_scale = 'IPTS-68'
    else:
        fp = None
    return convert_temperature_scale(convert(fp, calc_units, units), calc_temp_scale, temperature_scale)


def _fix_salinities(SA, SP, lat, lon, p) -> tuple[t.Optional[umath.FLOAT], t.Optional[umath.FLOAT]]:
    """Calculate SA or SP from the other if possible given the available values.

        Note that SA to SP is only useful when gsw is not installed (as otherwise the SA will be used
        in the calculations), so to provide a proper SP number we can't rely on gsw being installed.

        Likewise SP to SA is only useful when gsw is installed (as otherwise SP will be used with the
        seawater toolkit), so we don't need to calculate SA if gsw is not installed.
    """
    if SA is None and gsw is not None and lat is not None and lon is not None and p is not None:
        # TODO: calculate uncertainty associated with this value
        return gsw.SA_from_SP(umath.to_float(SP), umath.to_float(p), umath.to_float(lon), umath.to_float(lat)), SP
    elif SP is None and gsw is None:
        # TODO: convert SA to SP without gsw?
        # could use pure python gsw (can just be a dependency then), but is out-dated
        # or can copy pure python gsw function SP_from_SA into our sub library?
        return SA, SP
    else:
        return SA, SP


def convert_temperature_scale(value, current_scale: t.Optional[str], new_scale: t.Optional[str]):
    """Convert a value from one temperature scale to another."""
    if value is None:
        return None
    if current_scale is None or new_scale is None or current_scale == new_scale:
        return value
    if current_scale == 'IPTS-68' and new_scale == 'ITS-90':
        return _convert_t68_to_t90(value)
    if current_scale == 'IPS-90' and new_scale == 'IPTS-68':
        return _convert_t90_to_t68(value)
    if current_scale == 'IPTS-48' and new_scale == 'IPTS-68':
        return seawater_sub.eos80_t68_from_t48(value)
    if current_scale == 'IPTS-48' and new_scale == 'ITS-90':
        return _convert_t68_to_t90(seawater_sub.eos80_t68_from_t48(value))
    return None


def _convert_t68_to_t90(v):
    """Convert T68 temperatures to T90."""
    if gsw is not None:
        # TODO: calculate uncertainty associated with this value
        return gsw.t90_from_t68(umath.to_float(v))
    elif seawater is not None:
        # TODO: calculate uncertainty associated with this value
        return seawater.library.T90conv(umath.to_float(v))
    else:
        return seawater_sub.eos80_t90_from_t68(v)


def _convert_t90_to_t68(v):
    """Convert T90 temperatures to T68."""
    if seawater is not None:
        # TODO: calculate uncertainty associated with this value
        return seawater.library.T68conv(v)
    else:
        return seawater_sub.eos80_t68_from_t90(v)


def depth_from_pressure(pressure: umath.FLOAT,
                        latitude: umath.FLOAT,
                        units: t.Optional[str] = None) -> t.Optional[umath.FLOAT]:
    """Determine the depth given the pressure and latitude.

        Delegates to gsw.z_from_p() when installed, otherwise seawater.dpth() or our own local equivalent.
    """
    calc_units = 'm'
    if gsw is not None:
        # TODO: calculate uncertainty associated with this value
        depth = gsw.z_from_p(umath.to_float(pressure), umath.to_float(latitude))
    elif seawater is not None:
        # TODO: calculate uncertainty associated with this value
        depth = seawater.dpth(umath.to_float(pressure), umath.to_float(latitude))
    else:
        depth = seawater_sub.eos80_depth(pressure, latitude)
    return convert(depth, calc_units, units)


def pressure_from_depth(depth: umath.FLOAT,
                        latitude: umath.FLOAT,
                        units: t.Optional[str] = None) -> t.Optional[umath.FLOAT]:
    """Calculate pressure from depth and latitude.

        Uses gsw.p_from_z if available, otherwise seawater.pres().
    """
    calc_units = 'dbar'
    # Primary option is to use the GSW TEOS-10 model
    if gsw is not None:
        # TODO: calculate uncertainty associated with this value
        pressure = gsw.p_from_z(umath.to_float(depth) * -1, umath.to_float(latitude))
    # Secondary option is to use the EOS 80 model
    elif seawater is not None:
        # TODO: calculate uncertainty associated with this value
        pressure = seawater.pres(umath.to_float(depth), umath.to_float(latitude))
    # Third option, use our built-in fall-back (also EOS 80 based)
    else:
        pressure = seawater_sub.eos80_pressure(depth, latitude)
    return convert(pressure, calc_units, units)
