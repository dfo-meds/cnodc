"""
These wrapper functions work similar to the seawater.eos80 counterparts
but use the umath library to preserve uncertainty across calculations.
"""
from uncertainties import UFloat, ufloat

import cnodc.ocean_math.umath_wrapper as umath

# EOS80 paper: https://repository.oceanbestpractices.org/bitstream/handle/11329/109/059832eb.pdf?sequence=1&isAllowed=y


# From seawater.eos80.dpth
def eos80_depth(pressure: umath.FLOAT, latitude: umath.FLOAT) -> umath.FLOAT:
    # m
    top = ((((((-1.82e-15 * pressure) + 2.279e-10) * pressure) + -2.2512e-5) * pressure) + 9.72659) * pressure
    bottom = surface_gravity(latitude) + (0.5 * 2.184e-6 * pressure)
    return umath.adjust_uncertainty(top / bottom, 0.05)


# From seawater.eos80.pres
def eos80_pressure(depth: umath.FLOAT, latitude: umath.FLOAT) -> umath.FLOAT:
    # dbars
    sin_theta = umath.sin(umath.radians(abs(latitude)))
    c1 = 5.92e-3 + ((sin_theta ** 2) * 5.25e-3)
    # minimum uncertainty?
    return ((1 - c1) - (((1 - c1) ** 2) - (8.84e-6 * depth)) ** 0.5) / 4.42e-6


# From seawater.eos80.fp
def eos80_freezing_point(salinity: umath.FLOAT, pressure: umath.FLOAT) -> umath.FLOAT:
    if 40 >= salinity >= 4:
        fp = (-7.53e-4 * pressure) + (-0.0575 * salinity) + (1.710523e-3 * (salinity ** 1.5)) + (-2.154996e-4 * (salinity ** 2))
        return eos80_t90_from_t68(umath.adjust_uncertainty(fp, 0.003))
    else:
        raise ValueError("Invalid salinity")


# From seawater.eos80.T90
# for uncertainty, see https://www.teos-10.org/pubs/gsw/pdf/t90_from_t68.pdf
def eos80_t90_from_t68(temp_68: umath.FLOAT) -> umath.FLOAT:
    return umath.adjust_uncertainty(temp_68 / 1.00024, 0.00003 if -2 >= temp_68 >= 10 else 0.001)


def eos80_t68_from_t90(temp_90: umath.FLOAT) -> umath.FLOAT:
    return temp_90 * 1.00024


def eos80_t68_from_t48(temp_48: umath.FLOAT) -> umath.FLOAT:
    return temp_48 - (4.4e-6 * temp_48 * (100 - temp_48))


def eos80_t90_from_t48(temp_48: umath.FLOAT) -> umath.FLOAT:
    return eos80_t90_from_t68(eos80_t68_from_t48(temp_48))


def eos80_convert_temperature(temp: umath.FLOAT, temp_current_ref: str, temp_target_ref: str) -> umath.FLOAT:
    if temp_current_ref == temp_target_ref:
        return temp
    elif temp_target_ref == 'ITS-90':
        if temp_current_ref == 'IPTS-68':
            return eos80_t90_from_t68(temp)
        elif temp_current_ref == 'IPTS-48':
            return eos80_t90_from_t48(temp)
    elif temp_target_ref == 'IPTS-68':
        if temp_current_ref == 'ITS-90':
            return eos80_t68_from_t90(temp)
    raise ValueError(f'Undefined temperature conversion {temp_current_ref} to {temp_target_ref}')


# From seawater.eos80.dpth (partially)
def surface_gravity(latitude: umath.FLOAT) -> umath.FLOAT:
    sin2_theta = umath.sin(umath.radians(abs(latitude))) ** 2
    return 9.780318 * (1 + ((5.2788e-3 + (2.36e-5 * sin2_theta)) * sin2_theta))
