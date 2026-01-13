# Fitting functions for g and g_s from Appendix C of Saikawa & Shirai https://arxiv.org/pdf/1803.01038
from math import exp, log
from typing import Mapping

# Fitting functions needed for g(T), taken from Table 1 (p.48) of 1803.01038 v2
a_coeffs = {
    0: 1.0,
    1: 1.11724,
    2: 3.12672e-1,
    3: -4.68049e-2,
    4: -2.65004e-2,
    5: -1.19760e-3,
    6: 1.82812e-4,
    7: 1.36436e-4,
    8: 8.55051e-5,
    9: 1.2284e-5,
    10: 3.82259e-7,
    11: -6.87035e-9,
}

b_coeffs = {
    0: 1.43382e-2,
    1: 1.37559e-2,
    2: 2.92108e-3,
    3: -5.38533e-4,
    4: -1.62496e-4,
    5: -2.87906e-5,
    6: -3.84278e-6,
    7: 2.78776e-6,
    8: 7.40342e-7,
    9: 1.1721e-7,
    10: 3.72499e-9,
    11: -6.74107e-11,
}

# Fitting functions needed for g_S(T), also taken from Table 1 (p.48) of 1803.01038 v2
c_coeffs = {
    0: 1.0,
    1: 6.07869e-1,
    2: -1.54485e-1,
    3: -2.24034e-1,
    4: -2.82147e-2,
    5: 2.9062e-2,
    6: 6.86778e-3,
    7: -1.00005e-3,
    8: -1.69104e-4,
    9: 1.06301e-5,
    10: 1.69528e-6,
    11: -9.33311e-8,
}

d_coeffs = {
    0: 7.07388e1,
    1: 9.18011e1,
    2: 3.31892e1,
    3: -1.39779,
    4: -1.52558,
    5: -1.97857e-2,
    6: -1.60146e-1,
    7: 8.22615e-5,
    8: 2.02651e-2,
    9: -1.82134e-5,
    10: 7.83943e-5,
    11: 7.13518e-5,
}


# Particle masses in GeV, needed fpr fitting function below 120 MeV
M_e = 511e-6  # 511 keV
M_mu = 0.1056
M_pi0 = 0.135
M_1 = 0.5
M_2 = 0.77
M_3 = 1.2
M_4 = 2.0
M_piplus = 0.140
M_piminus = M_piplus

# at high temperature, G_rho and G_S usually have the same value
HIGH_T_GSTAR = 106.75

# G_rho and G_S usually only differ at low temperatures after neutrino decoupling, once e+/e- annihilation
# reheats the photons (but *not* the neutrinos)
# LOW_T_GSTAR = 3.36
# LOW_T_G_S_STAR = 3.91
LOW_T_GSTAR = 3.38

# TODO: check, https://www.astronomy.ohio-state.edu/weinberg.21/A8873/notes7a.pdf quotes instead
# these values look correct to me because e.g.
#   2 + 2 * 3.042 * (7/8) * (4/11)^(4/3) = 3.38172
# so this value of G_rho* includes N_eff from Planck, plus reheating of the photons but not the neutrinos
LOW_T_G_S_STAR = 3.94


# above SAIKAWA_SHIRAI_T_HI (measured in GeV) we assume the asymptotic high temperature degrees of freedom
SAIKAWA_SHIRAI_T_HI = 1e16

# boundary value (measured in GeV) from Saikawa & Shirai
SAIKAWA_SHIRAI_T_120_MEV = 0.12

# below SAIKAWA_SHIRAI_T_LO (measured in GeV) we assume the asymptotic low temperature degrees of freedom.
# Saikawa & Shirai say that their parametrization is valid down to 10 keV = 1E-5 GeV, which
# may well be true for G_rho(T) and G_S(T) alone, but we will get the wrong equation of state
# if we do this, because the neutrinos are already out of thermal equilibrium here.
# We cut to the late-time asymptotic values at 600 keV, just before e+e- annihilation reheats
# the photon temperature.
SAIKAWA_SHIRAI_T_LO = 1e-5


def _polynomial_sum(coeffs: Mapping[int, float], x: float) -> float:
    """Compute sum of polynomial terms"""
    return sum(c * pow(x, i) for i, c in coeffs.items())


# Fitting functions for energy density
def _f_rho(x: float) -> float:
    """Low-temperature fitting function for fermion energy density"""
    x_2 = x * x
    x_3 = x_2 * x
    return exp(-1.04855 * x) * (1.0 + 1.03757 * x + 0.508630 * x_2 + 0.0893988 * x_3)


def _b_rho(x: float) -> float:
    """Low-temperature fitting function for boson energy density"""
    x_2 = x * x
    x_3 = x_2 * x
    return exp(-1.03149 * x) * (1.0 + 1.03317 * x + 0.398264 * x_2 + 0.0648056 * x_3)


# Fitting functions for entropy density
def _f_s(x: float) -> float:
    """Low-temperature fitting function for fermion entropy"""
    x_2 = x * x
    x_3 = x_2 * x
    return exp(-1.04190 * x) * (1.0 + 1.03400 * x + 0.456426 * x_2 + 0.0595249 * x_3)


def _b_s(x: float) -> float:
    """Low-temperature fitting function for boson entropy"""
    x_2 = x * x
    x_3 = x_2 * x
    return exp(-1.03365 * x) * (1.0 + 1.03397 * x + 0.342548 * x_2 + 0.0506182 * x_3)


def _S_fit(x: float) -> float:
    """Combined entropy fitting function"""
    return 1.0 + (7.0 / 4.0) * _f_s(x)


def _raw_G_rho(T_in_GeV: float) -> float:
    if T_in_GeV > SAIKAWA_SHIRAI_T_HI:
        return HIGH_T_GSTAR  # Asymptotic high temperature limit
    elif SAIKAWA_SHIRAI_T_120_MEV <= T_in_GeV <= SAIKAWA_SHIRAI_T_HI:
        log_T_in_GeV = log(T_in_GeV)
        return _polynomial_sum(a_coeffs, log_T_in_GeV) / _polynomial_sum(
            b_coeffs, log_T_in_GeV
        )
    elif SAIKAWA_SHIRAI_T_LO <= T_in_GeV <= SAIKAWA_SHIRAI_T_120_MEV:
        # Eq. (C.3) of 1803.01038 v2
        return (
            2.030
            + 1.353 * pow(_S_fit(M_e / T_in_GeV), 4.0 / 3.0)
            + 3.495 * _f_rho(M_e / T_in_GeV)
            + 3.446 * _f_rho(M_mu / T_in_GeV)
            + 1.05 * _b_rho(M_pi0 / T_in_GeV)
            + 2.08 * _b_rho(M_piplus / T_in_GeV)
            + 4.165 * _b_rho(M_1 / T_in_GeV)
            + 30.55 * _b_rho(M_2 / T_in_GeV)
            + 89.4 * _b_rho(M_3 / T_in_GeV)
            + 8209.0 * _b_rho(M_4 / T_in_GeV)
        )
    else:
        return LOW_T_GSTAR  # Low temperature limit


def _raw_G_s(T_in_GeV: float) -> float:
    if T_in_GeV > SAIKAWA_SHIRAI_T_HI:
        return HIGH_T_GSTAR  # Asymptotic high temperature limit
    elif SAIKAWA_SHIRAI_T_120_MEV <= T_in_GeV <= SAIKAWA_SHIRAI_T_HI:
        log_T_in_GeV = log(T_in_GeV)
        return _raw_G_rho(T_in_GeV) / (
            1.0
            + _polynomial_sum(c_coeffs, log_T_in_GeV)
            / _polynomial_sum(d_coeffs, log_T_in_GeV)
        )
    elif SAIKAWA_SHIRAI_T_LO <= T_in_GeV <= SAIKAWA_SHIRAI_T_120_MEV:
        # Eq. (C.4) of 1803.01038 v2
        return (
            2.008
            + 1.923 * _S_fit(M_e / T_in_GeV)
            + 3.442 * _f_s(M_e / T_in_GeV)
            + 3.468 * _f_s(M_mu / T_in_GeV)
            + 1.034 * _b_s(M_pi0 / T_in_GeV)
            + 2.068 * _b_s(M_piplus / T_in_GeV)
            + 4.16 * _b_s(M_1 / T_in_GeV)
            + 30.55 * _b_s(M_2 / T_in_GeV)
            + 90.0 * _b_s(M_3 / T_in_GeV)
            + 6209.0 * _b_s(M_4 / T_in_GeV)
        )
    else:
        return LOW_T_G_S_STAR  # Low temperature limit
