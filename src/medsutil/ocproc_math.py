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
                practical_salinity: ocproc2.SingleElement | None,
                pressure: amath.AnyNumber | None = None,
                depth: amath.AnyNumber | None = None,
                latitude: amath.AnyNumber | None = None,
                obs_date: AwareDateTime | None = None,
                output_units: t.Optional[str] = "kg m-3") -> amath.AnyNumber | None:
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
        if output_units != "kg m-3":
            return convert(rho, "kg m-3", output_units)
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
