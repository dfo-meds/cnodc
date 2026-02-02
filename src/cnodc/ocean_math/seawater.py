"""
These wrapper functions work similar to the seawater.eos80 counterparts
but use the umath library to preserve uncertainty across calculations.
EOS80 paper: https://repository.oceanbestpractices.org/bitstream/handle/11329/109/059832eb.pdf?sequence=1&isAllowed=y
"""
from math import isnan

import cnodc.ocean_math.adecimal as adecimal


# From seawater.eos80.dpth
def eos80_depth(pressure: adecimal.AnyNumber, latitude: adecimal.AnyNumber) -> adecimal.AnyNumber:
    """Calculate depth in meters from pressure in dbars and latitude in decimal degrees."""
    top = ((((((-1.82e-15 * pressure) + 2.279e-10) * pressure) + -2.2512e-5) * pressure) + 9.72659) * pressure
    bottom = surface_gravity(latitude) + (0.5 * 2.184e-6 * pressure)
    res = top / bottom
    if isinstance(res, adecimal.AccurateDecimal):
        res.set_minimum_accuracy(0.05)
    return res


# From seawater.eos80.pres
def eos80_pressure(depth: adecimal.AnyNumber, latitude: adecimal.AnyNumber) -> adecimal.AnyNumber:
    """Calculate pressure in dbars from depth in meters and latitude in decimal degrees."""
    # dbars
    sin_theta = adecimal.sin(adecimal.radians(abs(latitude)))
    c1 = 5.92e-3 + ((sin_theta ** 2) * 5.25e-3)
    # minimum uncertainty?
    return ((1 - c1) - (((1 - c1) ** 2) - (8.84e-6 * depth)) ** 0.5) / 4.42e-6


# From seawater.eos80.fp
def eos80_freezing_point_t68(salinity: adecimal.AnyNumber, pressure: adecimal.AnyNumber) -> adecimal.AnyNumber:
    """Calculate freezing point in degrees C (IPTS-68 scale) from practical salinity in psu and pressure in dbars."""
    if 40 >= salinity >= 4:
        fp = (-7.53e-4 * pressure) + (-0.0575 * salinity) + (1.710523e-3 * (salinity ** 1.5)) + (-2.154996e-4 * (salinity ** 2))
        if isinstance(fp, adecimal.AccurateDecimal):
            fp.set_minimum_accuracy(0.003)
        return fp
    else:
        raise ValueError("Invalid salinity")


# From seawater.eos80.dens
def eos80_density_at_depth_t68(salinity: adecimal.AnyNumber, temperature_ipts68: adecimal.AnyNumber, pressure: adecimal.AnyNumber) -> adecimal.AnyNumber:
    """Calculate density in kg m-3 from practical salinity in psu, IPTS-68 temperature in degrees C, and pressure in dbars."""
    surface_density = eos80_surface_density_t68(salinity, temperature_ipts68)
    k = eos80_secant_bulk_modulus(salinity, temperature_ipts68, pressure)
    pressure = pressure / 10.0
    return surface_density / (1 - pressure / k)


# From seawater.eos80.dens0
def eos80_surface_density_t68(salinity: adecimal.AnyNumber, temperature_ipts68: adecimal.AnyNumber) -> adecimal.AnyNumber:
    """Calculate density at surface in kg m-3 from practical salinity in psu and IPTS-68 temperature in degrees C."""
    smow = eos80_standard_density_t68(temperature_ipts68)
    term1 = (8.24493e-1 + (-4.0899e-3 + (7.6438e-5 + (-8.2467e-7 + 5.3875e-9 * temperature_ipts68) * temperature_ipts68) * temperature_ipts68) * temperature_ipts68) * salinity
    term2 = (-5.72466e-3 + (1.0227e-4 + -1.6546e-6 * temperature_ipts68) * temperature_ipts68) * (salinity ** 1.5)
    term3 = 4.8314e-4 * (salinity ** 2)
    return smow + term1 + term2 + term3


def eos80_standard_density_t68(temperature_ipts68: adecimal.AnyNumber) -> adecimal.AnyNumber:
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


def eos80_secant_bulk_modulus(salinity: adecimal.AnyNumber, temperature_ipts68: adecimal.AnyNumber, pressure: adecimal.AnyNumber) -> adecimal.AnyNumber:
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


T68_CONVERSION_FACTOR = 1.00024

# From seawater.eos80.T90
# for uncertainty, see https://www.teos-10.org/pubs/gsw/pdf/t90_from_t68.pdf
def eos80_t90_from_t68(temp_68: adecimal.AnyNumber) -> adecimal.AnyNumber:
    """Convert IPTS-68 to ITS-90 with uncertainty."""
    res = temp_68 / T68_CONVERSION_FACTOR
    if isinstance(res, adecimal.AccurateDecimal):
        res.set_minimum_accuracy(0.00003 if -2 >= temp_68 >= 10 else 0.001)
    return res


def eos80_t68_from_t90(temp_90: adecimal.AnyNumber) -> adecimal.AnyNumber:
    """Convert ITS-90 to IPTS-68 with uncertainty"""
    return temp_90 * T68_CONVERSION_FACTOR


def eos80_t68_from_t48(temp_48: adecimal.AnyNumber) -> adecimal.AnyNumber:
    """Convert IPTS-48 to IPTS-68 """
    if not isinstance(temp_48, adecimal.AccurateDecimal):
        temp_48 = adecimal.AccurateDecimal(temp_48)
    return temp_48 - (4.4e-6 * temp_48 * (100 - temp_48))


def eos80_t90_from_t48(temp_48: adecimal.AnyNumber) -> adecimal.AnyNumber:
    """Convert IPTS-48 to IPTS-90"""
    return eos80_t90_from_t68(eos80_t68_from_t48(temp_48))


def eos80_convert_temperature(temp: adecimal.AnyNumber, temp_current_ref: str, temp_target_ref: str) -> adecimal.AnyNumber:
    """Convert temperature from given reference scale to specified reference scale"""
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
def surface_gravity(latitude: adecimal.AnyNumber) -> adecimal.AnyNumber:
    """Calculate the surface gravity with uncertainty."""
    sin2_theta = adecimal.sin(adecimal.radians(abs(latitude))) ** 2
    return 9.780318 * (1 + ((5.2788e-3 + (2.36e-5 * sin2_theta)) * sin2_theta))
