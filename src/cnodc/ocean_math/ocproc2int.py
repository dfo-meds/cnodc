import datetime

import cnodc.ocproc2 as ocproc2
import typing as t
import cnodc.ocean_math.umath_wrapper as umath
import cnodc.ocean_math.seawater as seawater_sub
from autoinject import injector
from cnodc.units.units import convert

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


def get_temperature(temperature: t.Optional[ocproc2.SingleElement],
                    units: str,
                    obs_date: t.Optional[ocproc2.SingleElement] = None,
                    temperature_scale: str = "ITS-90"):
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
    p_val, p_qual, _ = calc_pressure(pressure, depth, latitude)
    if p_val is not None:
        if absolute_salinity is not None and not absolute_salinity.is_empty():
            return (
                freezing_point(
                    pressure=p_val,
                    latitude=latitude.to_float_with_uncertainty() if latitude is not None and not latitude.is_empty()  else None,
                    longitude=longitude.to_float_with_uncertainty() if longitude is not None and not longitude.is_empty() else None,
                    absolute_salinity=absolute_salinity.to_float_with_uncertainty('g kg-1'),
                    units=units,
                    temperature_scale=temperature_scale
                ),
                _compress_quality_scores(p_qual, absolute_salinity.working_quality()),
                units or '°C'
            )
        if practical_salinity is not None and not practical_salinity.is_empty():
            return (
                freezing_point(
                    pressure=p_val,
                    latitude=latitude.to_float_with_uncertainty() if latitude is not None and not latitude.is_empty()  else None,
                    longitude=longitude.to_float_with_uncertainty() if longitude is not None and not longitude.is_empty() else None,
                    practical_salinity=practical_salinity.to_float_with_uncertainty('0.001'),
                    units=units,
                    temperature_scale=temperature_scale
                ),
                _compress_quality_scores(p_qual, practical_salinity.working_quality()),
                units or '°C'
            )
    return None, 9, None


def calc_pressure_record(level_record: ocproc2.BaseRecord,
                         position_record: t.Optional[ocproc2.BaseRecord],
                         units: t.Optional[str] = None) -> VAL_QC_UNITS:
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
    p, p_q, _ = calc_pressure(pressure, depth, latitude, 'dbar')
    t90 = get_temperature(temperature, '°C', temperature_scale='ITS-90')
    if p is not None and t90 is not None:
        if absolute_salinity is not None and not absolute_salinity.is_empty():
            return (
                density_at_depth(
                    pressure=p,
                    temperature=t90,
                    absolute_salinity=absolute_salinity.to_float_with_uncertainty('g kg-1'),
                    latitude=latitude.to_float_with_uncertainty() if latitude is not None and not latitude.is_empty()  else None,
                    longitude=longitude.to_float_with_uncertainty() if longitude is not None and not longitude.is_empty() else None,
                    units=units
                ),
                _compress_quality_scores(
                    p_q,
                    temperature.working_quality(),
                    absolute_salinity.working_quality(),
                ),
                units or 'kg m-3'
            )
        elif practical_salinity is not None and not practical_salinity.is_empty():
            return (
                density_at_depth(
                    pressure=p,
                    temperature=t90,
                    practical_salinity=practical_salinity.to_float_with_uncertainty('g kg-1'),
                    latitude=latitude.to_float_with_uncertainty() if latitude is not None and not latitude.is_empty()  else None,
                    longitude=longitude.to_float_with_uncertainty() if longitude is not None and not longitude.is_empty() else None,
                    units=units
                ),
                _compress_quality_scores(
                    p_q,
                    temperature.working_quality(),
                    practical_salinity.working_quality(),
                ),
                units or 'kg m-3'
            )
    return None, 9, None


def _compress_quality_scores(*qc_scores) -> int:
    for x in (9, 4, 3, 2):
        if any(y == x for y in qc_scores):
            return x
    if all(y == 1 for y in qc_scores):
        return 1
    return 0


def density_at_depth(pressure: umath.FLOAT,
                     temperature: umath.FLOAT,
                     absolute_salinity: t.Optional[umath.FLOAT] = None,
                     practical_salinity: t.Optional[umath.FLOAT] = None,
                     latitude: t.Optional[umath.FLOAT] = None,
                     longitude: t.Optional[umath.FLOAT] = None,
                     units: t.Optional[str] = None) -> t.Optional[umath.FLOAT]:
    if absolute_salinity is None and practical_salinity is None:
        return None
    absolute_salinity, practical_salinity = _fix_salinities(absolute_salinity, practical_salinity, latitude, longitude, pressure)
    calc_units = "kg m-3"
    if gsw is not None and absolute_salinity is not None:
        density = gsw.rho_t_exact(_to_float(absolute_salinity), _to_float(temperature), _to_float(pressure))
    elif seawater is not None and practical_salinity is not None:
        density = seawater.dens(_to_float(practical_salinity), _to_float(temperature), _to_float(pressure))
    elif practical_salinity is not None:
        density = seawater_sub.eos80_density_at_depth_t90(practical_salinity, temperature, pressure)
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
    if pressure is None:
        return None
    if absolute_salinity is None and practical_salinity is None:
        return None
    calc_units = '°C'
    calc_temp_scale = 'ITS-90'
    absolute_salinity, practical_salinity = _fix_salinities(absolute_salinity, practical_salinity, latitude, longitude, pressure)
    if gsw is not None and absolute_salinity is not None:
        # TODO: saturation_fraction from DO if available?
        # TODO: confirm if we have absolute or sea pressures (i.e. are we subtracting 10.1325 dbar?)
        fp = gsw.t_freezing(_to_float(absolute_salinity), _to_float(pressure), 1)
    elif seawater is not None and practical_salinity is not None:
        fp = seawater.fp(_to_float(practical_salinity), _to_float(pressure))
    elif practical_salinity is not None:
        fp = seawater_sub.eos80_freezing_point_t90(practical_salinity, pressure)
    else:
        fp = None
    return convert_temperature_scale(convert(fp, calc_units, units), calc_temp_scale, temperature_scale)


def _fix_salinities(SA, SP, lat, lon, p) -> tuple[t.Optional[umath.FLOAT], t.Optional[umath.FLOAT]]:
    if SA is None and gsw is not None and lat is not None and lon is not None and p is not None:
        return gsw.SA_from_SP(_to_float(SP), _to_float(p), _to_float(lon), _to_float(lat)), SP
    elif SP is None and gsw is None:
        # TODO: convert SA to SP without gsw?
        # could use pure python gsw (can just be a dependency then), but is out-dated
        # or can copy pure python gsw function SP_from_SA into our sub library?
        return SA, SP
    else:
        return SA, SP


def convert_temperature_scale(value, current_scale: t.Optional[str], new_scale: t.Optional[str]):
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
    if gsw is not None:
        return gsw.t90_from_t68(_to_float(v))
    elif seawater is not None:
        return seawater.library.T90conv(_to_float(v))
    else:
        return seawater_sub.eos80_t90_from_t68(v)


def _convert_t90_to_t68(v):
    if seawater is not None:
        return seawater.library.T68conv(v)
    else:
        return seawater_sub.eos80_t68_from_t90(v)


def depth_from_pressure(pressure: umath.FLOAT,
                        latitude: umath.FLOAT,
                        units: t.Optional[str] = None) -> t.Optional[umath.FLOAT]:
    calc_units = 'm'
    if gsw is not None:
        depth = gsw.z_from_p(_to_float(pressure), _to_float(latitude))
    elif seawater is not None:
        depth = seawater.dpth(_to_float(pressure), _to_float(latitude))
    else:
        depth = seawater_sub.eos80_depth(pressure, latitude)
    return convert(depth, calc_units, units)


def pressure_from_depth(depth: umath.FLOAT,
                        latitude: umath.FLOAT,
                        units: t.Optional[str] = None) -> t.Optional[umath.FLOAT]:
    calc_units = 'dbar'
    # Primary option is to use the GSW TEOS-10 model
    if gsw is not None:
        pressure = gsw.p_from_z(_to_float(depth) * -1, _to_float(latitude))
    # Secondary option is to use the EOS 80 model
    elif seawater is not None:
        pressure = seawater.pres(_to_float(depth), _to_float(latitude))
    # Third option, use our built-in fall-back (also EOS 80 based)
    else:
        pressure = seawater_sub.eos80_pressure(depth, latitude)
    return convert(pressure, calc_units, units)


def _to_float(f: umath.FLOAT):
    if isinstance(f, umath.UFloat):
        return f.nominal_value
    else:
        return f
