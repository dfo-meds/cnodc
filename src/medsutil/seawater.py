"""
These wrapper functions work similar to the seawater.eos80 counterparts
but use the amath library to preserve uncertainty across calculations if appropriate
EOS80 paper: https://repository.oceanbestpractices.org/bitstream/handle/11329/109/059832eb.pdf?sequence=1&isAllowed=y
"""
import decimal
import enum
import typing as t

import medsutil.math as amath
from medsutil.awaretime import AwareDateTime
from medsutil.math import AnyNumber, _functions
from medsutil.units.units import convert


ITS90_START = AwareDateTime(1990, 1, 1, 0, 0, 0, tzinfo="Etc/UTC")
IPTS68_START = AwareDateTime(1968, 1, 1, 0, 0, 0, tzinfo="Etc/UTC")


class TemperatureScale(enum.Enum):
    TS_1990 = 'ITS-90'
    TS_1968 = 'IPTS-68'
    TS_1948 = 'IPTS-48'

def temperature_scale_in_use_on(obs_date: AwareDateTime | None, ts_scale_str: str | None = None):
    if ts_scale_str is not None:
        return TemperatureScale(ts_scale_str)
    if obs_date is not None:
        if obs_date < IPTS68_START:
            return TemperatureScale.TS_1948
        elif obs_date < ITS90_START:
            return TemperatureScale.TS_1968
    return TemperatureScale.TS_1990

DEPTH_A = amath.NumberString("-1.82e-15")
DEPTH_B = amath.NumberString("2.279e-10")
DEPTH_C = amath.NumberString("-2.2512e-5")
DEPTH_D = amath.NumberString("9.72659")
DEPTH_E = amath.NumberString("2.184e-6")
DEPTH_F = amath.NumberString("1.092e-6")

# From seawater.eos80.dpth
def eos80_depth[T: AnyNumber](pressure: T, latitude: T) -> T:
    """Calculate depth in meters from pressure in dbars and latitude in decimal degrees."""
    top = amath.calculate_polynomial(pressure, DEPTH_A, DEPTH_B, DEPTH_C, DEPTH_D, DEPTH_E, 0)
    bottom = amath.add(surface_gravity(latitude), amath.mul(DEPTH_F, pressure))
    # Note: 5% uncertainty in depth
    depth = amath.div(top, bottom)
    if amath.is_science_number(depth):
        depth.set_min_error_if_worse(0.05, is_relative=True)


PRESSURE_A = amath.NumberString("5.92e-3")
PRESSURE_B = amath.NumberString("5.25e-3")
PRESSURE_C = amath.NumberString("8.84e-6")
PRESSURE_D = amath.NumberString("4.42e-6")

# From seawater.eos80.pres
def eos80_pressure[T: AnyNumber](depth: T, latitude: T) -> T:
    """Calculate pressure in dbars from depth in meters and latitude in decimal degrees."""
    # dbars
    sin_theta = amath.sin(amath.radians(abs(latitude)))
    c1 = amath.add(PRESSURE_A, amath.mul(PRESSURE_B, amath.pow_(sin_theta, 2)))
    c2 = amath.sub(1, c1)
    # minimum uncertainty?
    return amath.div(
            amath.sub(
                c2,
                amath.sqrt(
                    amath.sub(
                        amath.pow_(c2, 2),
                        amath.mul(PRESSURE_C, depth)
                    )
                )
            ),
            PRESSURE_D
    )

FP_A = amath.NumberString("-7.53e-4")
FP_B = amath.NumberString("-0.0575")
FP_C = amath.NumberString("1.710523e-3")
FP_D = amath.NumberString("-2.154996e-4")

# From seawater.eos80.fp
def eos80_freezing_point_t68[T: AnyNumber](salinity: T, pressure: T) -> T:
    """Calculate freezing point in degrees C (IPTS-68 scale) from practical salinity in psu and pressure in dbars."""
    if _functions.between(4, salinity, 40):
        # Note: 0.3% accuracy
        return amath.sum_((
            amath.mul(FP_A, pressure),
            amath.mul(FP_B, salinity),
            amath.mul(FP_C, amath.pow_(salinity, decimal.Decimal("1.5"))),
            amath.mul(FP_D, amath.pow_(salinity, 2))
        ))
    else:
        raise ValueError("Invalid salinity")


# From seawater.eos80.dens
def eos80_density_at_depth_t68(salinity: amath.AnyNumber, temperature_ipts68: amath.AnyNumber, pressure: amath.AnyNumber) -> amath.AnyNumber:
    """Calculate density in kg m-3 from practical salinity in psu, IPTS-68 temperature in degrees C, and pressure in dbars."""
    surface_density = eos80_surface_density_t68(salinity, temperature_ipts68)
    k = eos80_secant_bulk_modulus(salinity, temperature_ipts68, pressure)
    pressure = amath.div(pressure, 10)
    return amath.div(surface_density, amath.sub(1, amath.div(pressure, k)))


# From seawater.eos80.dens0
def eos80_surface_density_t68(salinity: amath.AnyNumber, temperature_ipts68: amath.AnyNumber) -> amath.AnyNumber:
    """Calculate density at surface in kg m-3 from practical salinity in psu and IPTS-68 temperature in degrees C."""
    smow = eos80_standard_density_t68(temperature_ipts68)
    term1 = (8.24493e-1 + (-4.0899e-3 + (7.6438e-5 + (-8.2467e-7 + 5.3875e-9 * temperature_ipts68) * temperature_ipts68) * temperature_ipts68) * temperature_ipts68) * salinity
    term2 = (-5.72466e-3 + (1.0227e-4 + -1.6546e-6 * temperature_ipts68) * temperature_ipts68) * (salinity ** 1.5)
    term3 = 4.8314e-4 * (salinity ** 2)
    return smow + term1 + term2 + term3


def eos80_standard_density_t68(temperature_ipts68: amath.AnyNumber) -> amath.AnyNumber:
    """Calculate the standard density of mean ocean water in kg m-3 from IPTS-68 temperature in degrees C"""
    return (999.842594 +
            (6.793952e-2 +
             (-9.095290e-3 +
              (1.001685e-4 +
               (-1.120083e-6 + 6.536332e-9 * temperature_ipts68)
               * temperature_ipts68)
              * temperature_ipts68)
             * temperature_ipts68)
            * temperature_ipts68)


def eos80_secant_bulk_modulus(salinity: amath.AnyNumber, temperature_ipts68: amath.AnyNumber, pressure: amath.AnyNumber) -> amath.AnyNumber:
    """Calculate the secant bulk modulus"""
    pressure = pressure / 10.0  # to atm (is this still correct? atmosphere is technically 10.1 dbars ish
    # Pure water terms of the secant bulk modulus at atmos pressure.
    # UNESCO Eqn 19 p 18.
    # h0 = -0.1194975
    h = [3.239908, 1.43713e-3, 1.16092e-4, -5.77905e-7]
    AW = h[0] + (h[1] + (h[2] + h[3] * temperature_ipts68) * temperature_ipts68) * temperature_ipts68

    # k0 = 3.47718e-5
    k = [8.50935e-5, -6.12293e-6, 5.2787e-8]
    BW = k[0] + (k[1] + k[2] * temperature_ipts68) * temperature_ipts68

    # e0 = -1930.06
    e = [19652.21, 148.4206, -2.327105, 1.360477e-2, -5.155288e-5]
    KW = e[0] + (e[1] + (e[2] + (e[3] + e[4] * temperature_ipts68) * temperature_ipts68) * temperature_ipts68) * temperature_ipts68

    # Sea water terms of secant bulk modulus at atmos. pressure.
    j0 = 1.91075e-4
    i = [2.2838e-3, -1.0981e-5, -1.6078e-6]
    A = AW + (i[0] + (i[1] + i[2] * temperature_ipts68) * temperature_ipts68 + j0 * salinity ** 0.5) * salinity

    m = [-9.9348e-7, 2.0816e-8, 9.1697e-10]
    B = BW + (m[0] + (m[1] + m[2] * temperature_ipts68) * temperature_ipts68) * salinity  # Eqn 18.

    f = [54.6746, -0.603459, 1.09987e-2, -6.1670e-5]
    g = [7.944e-2, 1.6483e-2, -5.3009e-4]
    K0 = (KW + (f[0] + (f[1] + (f[2] + f[3] * temperature_ipts68) * temperature_ipts68) * temperature_ipts68 +
                (g[0] + (g[1] + g[2] * temperature_ipts68) * temperature_ipts68) * salinity ** 0.5) * salinity)  # Eqn 16.
    return K0 + (A + B * pressure) * pressure  # Eqn 15.


T68_CONVERSION_FACTOR = amath.NumberString("1.00024")
T48_A = amath.NumberString("4.4e-6")


# From seawater.eos80.T90
# for uncertainty, see https://www.teos-10.org/pubs/gsw/pdf/t90_from_t68.pdf
def eos80_t90_from_t68[T: AnyNumber](temp_68: T) -> T:
    """Convert IPTS-68 to ITS-90 with uncertainty."""
    res = amath.div(temp_68, T68_CONVERSION_FACTOR)
    # uncertainity 0.00003 if -2 >= temp_68 >= 10 else 0.001
    return res


def eos80_t68_from_t90[T](temp_90: T) -> T:
    """Convert ITS-90 to IPTS-68 with uncertainty"""
    return amath.mul(temp_90, T68_CONVERSION_FACTOR)


def eos80_t68_from_t48[T](temp_48: T) -> T:
    """Convert IPTS-48 to IPTS-68 """
    return amath.sub(temp_48, amath.product((
       temp_48,
       T48_A,
       amath.sub(100, temp_48)
    )))


def eos80_t90_from_t48[T](temp_48: T) -> T:
    """Convert IPTS-48 to IPTS-90"""
    return eos80_t90_from_t68(eos80_t68_from_t48(temp_48))


def eos80_convert_temperature[T](temperature: T,
                                 input_scale: TemperatureScale,
                                 output_scale: TemperatureScale,
                                 input_units: str = "degrees_C",
                                 output_units: str = "degrees_C") -> T:
    """Convert temperature from given reference scale to specified reference scale"""

    # Fast convert for when temp refs are the same
    if input_scale is output_scale:
        return convert(temperature, input_units, output_units)

    # Temperatures must be in degrees C for the process to work
    if input_units not in ("degrees_C", "degree_C", "°"):
        temperature = convert(temperature, input_units, "degrees_C")

    # Adjust the scale as necessary
    temperature = eos80_convert_temperature_scale(temperature, input_scale, output_scale)

    # Adjust to output units if necessary - note we're always in degrees_C after conversion
    if output_units not in ("degrees_C", "degree_C", "°"):
        temperature = convert(temperature, "degrees_C", output_units)

    return temperature


def eos80_convert_temperature_scale[T](temp: T, input_scale: TemperatureScale, output_scale: TemperatureScale) -> T:
    if input_scale is output_scale:
        return temp
    elif output_scale is TemperatureScale.TS_1990:
        if input_scale is TemperatureScale.TS_1968:
            return eos80_t90_from_t68(temp)
        elif input_scale is TemperatureScale.TS_1948:
            return eos80_t90_from_t48(temp)
    elif output_scale is TemperatureScale.TS_1968:
        if input_scale == TemperatureScale.TS_1990:
            return eos80_t68_from_t90(temp)
    raise ValueError(f'Undefined temperature conversion {input_scale} to {output_scale}')


SG_A = amath.NumberString("2.36e-5")
SG_B = amath.NumberString("5.2788e-3")
SG_K = amath.NumberString("9.780318")


# From seawater.eos80.dpth (partially)
def surface_gravity[T](latitude: T) -> T:
    """Calculate the surface gravity with uncertainty."""
    sin2_theta = amath.sin(amath.radians(abs(latitude))) ** 2
    return amath.mul(
        SG_K,
        amath.calculate_polynomial(sin2_theta, SG_A, SG_B, 1)
    )
