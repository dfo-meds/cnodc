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
def eos80_freezing_point_t90(salinity: umath.FLOAT, pressure: umath.FLOAT) -> umath.FLOAT:
    return eos80_t90_from_t68(eos80_freezing_point_t68(salinity, pressure))


def eos80_freezing_point_t68(salinity: umath.FLOAT, pressure: umath.FLOAT) -> umath.FLOAT:
    if 40 >= salinity >= 4:
        fp = (-7.53e-4 * pressure) + (-0.0575 * salinity) + (1.710523e-3 * (salinity ** 1.5)) + (-2.154996e-4 * (salinity ** 2))
        return umath.adjust_uncertainty(fp, 0.003)
    else:
        raise ValueError("Invalid salinity")


def eos80_surface_density_t90(salinity: umath.FLOAT, temperature_its90: umath.FLOAT) -> umath.FLOAT:
    return eos80_surface_density_t68(salinity, eos80_t68_from_t90(temperature_its90))


def eos80_density_at_depth_t90(salinity: umath.FLOAT, temperature_its90: umath.FLOAT, pressure: umath.FLOAT):
    return eos80_density_at_depth_t68(salinity, eos80_t68_from_t90(temperature_its90), pressure)


def eos80_density_at_depth_t68(salinity: umath.FLOAT, temperature_ipts68: umath.FLOAT, pressure: umath.FLOAT):
    surface_density = eos80_surface_density_t68(salinity, temperature_ipts68)
    K = eos80_secant_bulk_modulus(salinity, temperature_ipts68, pressure)
    pressure = pressure / 10.0
    return surface_density / (1 - pressure / K)


def eos80_surface_density_t68(salinity: umath.FLOAT, temperature_ipts68: umath.FLOAT) -> umath.FLOAT:
    smow = eos80_standard_density_t68(temperature_ipts68)
    term1 = (8.24493e-1 + (-4.0899e-3 + (7.6438e-5 + (-8.2467e-7 + 5.3875e-9 * temperature_ipts68) * temperature_ipts68) * temperature_ipts68) * temperature_ipts68) * salinity
    term2 = (-5.72466e-3 + (1.0227e-4 + -1.6546e-6 * temperature_ipts68) * temperature_ipts68) * (salinity ** 1.5)
    term3 = 4.8314e-4 * (salinity ** 2)
    return smow + term1 + term2 + term3


def eos80_standard_density_t68(temperature_ipts68: umath.FLOAT) -> umath.FLOAT:
    return (999.842594 +
            (6.793952e-2 +
             (-9.095290e-3 +
              (1.001685e-4 +
               (-1.120083e-6 + 6.536332e-9 * temperature_ipts68)
               * temperature_ipts68)
              * temperature_ipts68)
             * temperature_ipts68)
            * temperature_ipts68)


def eos80_secant_bulk_modulus(salinity: umath.FLOAT, temperature_ipts68: umath.FLOAT, pressure: umath.FLOAT) -> umath.FLOAT:
    pressure = pressure / 10.0  # to atm (is this still correct? atmosphere is technically 10.1 dbars ish
    # Pure water terms of the secant bulk modulus at atmos pressure.
    # UNESCO Eqn 19 p 18.
    # h0 = -0.1194975
    h = [3.239908, 1.43713e-3, 1.16092e-4, -5.77905e-7]
    AW = h[0] + (h[1] + (h[2] + h[3] *temperature_ipts68) * temperature_ipts68) * temperature_ipts68

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
