import datetime

import cnodc.ocproc2.structures as ocproc2
import typing as t
import cnodc.ocean_math.umath_wrapper as umath
import cnodc.ocean_math.seawater as seawater_sub
from autoinject import injector
from cnodc.units.units import convert


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


def get_temperature(temperature: t.Optional[ocproc2.Value],
                    units: str,
                    obs_date: t.Optional[ocproc2.Value] = None,
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
    return _convert_temperature_scale(temp_val, c_temp_scale, temperature_scale)


def calc_freezing_point(pressure: t.Optional[ocproc2.Value] = None,
                        depth: t.Optional[ocproc2.Value] = None,
                        latitude: t.Optional[ocproc2.Value] = None,
                        practical_salinity: t.Optional[ocproc2.Value] = None,
                        absolute_salinity: t.Optional[ocproc2.Value] = None,
                        units: t.Optional[str] = None,
                        temperature_scale: t.Optional[str] = None) -> tuple[t.Optional[float], int, t.Optional[str]]:
    p_val, p_qual, _ = calc_pressure(pressure, depth, latitude)
    if p_val is not None:
        if absolute_salinity is not None and not absolute_salinity.is_empty():
            return (
                _freezing_point(
                    pressure=p_val,
                    absolute_salinity=absolute_salinity.to_float_with_uncertainty('g kg-1'),
                    units=units,
                    temperature_scale=temperature_scale
                ),
                _compress_quality_scores(p_qual, absolute_salinity.working_quality()),
                units or '°C'
            )
        if practical_salinity is not None and not practical_salinity.is_empty():
            return (
                _freezing_point(
                    pressure=p_val,
                    practical_salinity=practical_salinity.to_float_with_uncertainty('0.001'),
                    units=units,
                    temperature_scale=temperature_scale
                ),
                _compress_quality_scores(p_qual, practical_salinity.working_quality()),
                units or '°C'
            )
    return None, 9, None


def calc_pressure(pressure: t.Optional[ocproc2.Value] = None,
             depth: t.Optional[ocproc2.Value] = None,
             latitude: t.Optional[ocproc2.Value] = None,
             units: t.Optional[str] = None) -> tuple[t.Optional[float], int, t.Optional[str]]:
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
            _pressure_from_depth(d_float, lat_float, units),
            _compress_quality_scores(depth.working_quality(), latitude.working_quality()),
            units or 'dbar'
        )
    return None, 9, None


def _compress_quality_scores(*qc_scores) -> int:
    for x in (9, 4, 3, 2):
        if any(y == x for y in qc_scores):
            return x
    if all(y == 1 for y in qc_scores):
        return 1
    return 0


def _freezing_point(pressure: umath.FLOAT,
                    absolute_salinity: t.Optional[umath.FLOAT] = None,
                    practical_salinity: t.Optional[umath.FLOAT] = None,
                    units: t.Optional[str] = None,
                    temperature_scale: t.Optional[str] = None) -> t.Optional[umath.FLOAT]:
    if absolute_salinity is None and practical_salinity is None:
        return None
    calc_units = '°C'
    calc_temp_scale = 'ITS-90'
    if gsw is not None:
        if absolute_salinity is None:
            # TODO: convert practical to absolute
            pass
        # TODO: saturation_fraction from DO if available?
        # TODO: confirm if we have absolute or sea pressures (i.e. are we subtracting 10.1325 dbar?)
        fp = gsw.t_freezing(absolute_salinity, pressure, 1)
    elif seawater is not None:
        if practical_salinity is None:
            # TODO: convert absolute to practical salinity?
            pass
        fp = seawater.fp(practical_salinity, pressure)
    else:
        if practical_salinity is None:
            # TODO: convert absolute to practical salinity?
            pass
        fp = seawater_sub.eos80_freezing_point_t90(practical_salinity, pressure)
    return _convert_temperature_scale(convert(fp, calc_units, units), calc_temp_scale, temperature_scale)


def _convert_temperature_scale(value, current_scale: t.Optional[str], new_scale: t.Optional[str]):
    if current_scale is None or new_scale is None or current_scale == new_scale:
        return value
    if current_scale == 'IPTS-68' and new_scale == 'ITS-90':
        return _convert_t68_t90(value)
    if current_scale == 'IPS-90' and new_scale == 'IPTS-68':
        return _convert_t90_t68(value)
    if current_scale == 'IPTS-48' and new_scale == 'IPTS-68':
        return seawater_sub.eos80_t68_from_t48(value)
    if current_scale == 'IPTS-48' and new_scale == 'ITS-90':
        return _convert_t68_t90(seawater_sub.eos80_t68_from_t48(value))
    return None


def _convert_t68_t90(v):
    # TODO: check gsw and seawater packages first?
    return seawater_sub.eos80_t90_from_t68(v)


def _convert_t90_t68(v):
    # TODO: check gsw and seawater packages first?
    return seawater_sub.eos80_t68_from_t90(v)


def _pressure_from_depth(depth: umath.FLOAT,
                         latitude: umath.FLOAT,
                         units: t.Optional[str] = None) -> t.Optional[umath.FLOAT]:
    calc_units = 'dbar'
    # Primary option is to use the GSW TEOS-10 model
    if gsw is not None:
        pressure = gsw.p_from_z(depth, latitude)
    # Secondary option is to use the EOS 80 model
    elif seawater is not None:
        pressure = seawater.pres(depth, latitude)
    # Third option, use our built-in fall-back (also EOS 80 based)
    else:
        pressure = seawater_sub.eos80_pressure(depth, latitude)
    return convert(pressure, calc_units, units)














