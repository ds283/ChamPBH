from typing import Mapping

import jax.lax as lax
from jax import grad
from jax.numpy import exp, log, pow

from CosmologyModels.GenericEOS.GenericEOS import (
    GenericEOSBase,
    _HIGH_T_GSTAR,
    _LOW_T_G_S_STAR,
    _LOW_T_GSTAR,
)
from CosmologyModels.model_ids import QCD_EOS_IDENTIFIER
from Units.base import UnitsLike

# above _T_HI_GEV (measured in GeV) we assume the asymptotic high temperature degrees of freedom
_T_HI_GEV = 1e16

# boundary value (measured in GeV) from Saikawa & Shirai
_T_120_MEV = 0.12

# below _T_LO_GEV (measured in GeV) we assume the asymptotic low temperature degrees of freedom.
# Saikawa & Shirai say that their parametrization is valid down to 10 keV = 1E-5 GeV, which
# may well be true for G_rho(T) and G_S(T) alone, but we will get the wrong equation of state
# if we do this, because the neutrinos are already out of thermal equilibrium here.
# We cut to the late-time asymptotic values at 600 keV, just before e+e- annihilation reheats
# the photon temperature.
_T_LO_GEV = 1e-5

# Fitting functions for g and g_s from Appendix C of Saikawa & Shirai https://arxiv.org/pdf/1803.01038

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
M_piplus = M_piminus = 0.140
M_1 = 0.5
M_2 = 0.77
M_3 = 1.2
M_4 = 2.0


def polynomial_sum(coeffs: Mapping[int, float], x):
    """Compute sum of polynomial terms"""
    return sum(c * pow(x, i) for i, c in coeffs.items())


# Fitting functions for energy density
def f_rho(x):
    """Low-temperature fitting function for fermion energy density"""
    x_2 = x * x
    x_3 = x_2 * x
    return exp(-1.04855 * x) * (1.0 + 1.03757 * x + 0.508630 * x_2 + 0.0893988 * x_3)


def b_rho(x):
    """Low-temperature fitting function for boson energy density"""
    x_2 = x * x
    x_3 = x_2 * x
    return exp(-1.03149 * x) * (1.0 + 1.03317 * x + 0.398264 * x_2 + 0.0648056 * x_3)


# Fitting functions for entropy density
def f_s(x):
    """Low-temperature fitting function for fermion entropy"""
    x_2 = x * x
    x_3 = x_2 * x
    return exp(-1.04190 * x) * (1.0 + 1.03400 * x + 0.456426 * x_2 + 0.0595249 * x_3)


def b_s(x):
    """Low-temperature fitting function for boson entropy"""
    x_2 = x * x
    x_3 = x_2 * x
    return exp(-1.03365 * x) * (1.0 + 1.03397 * x + 0.342548 * x_2 + 0.0506182 * x_3)


def S_fit(x):
    """Combined entropy fitting function"""
    return 1.0 + (7.0 / 4.0) * f_s(x)


def _raw_G_rho(T_in_GeV):
    log_T_in_GeV = log(T_in_GeV)

    # compatibility with JAX tracing for JIT means that we have to use this rather
    # opaque looking block to handle flow control
    # Note that lax.cond expects its true/false branches to be *functions*
    # presumably the JIT compiler will optimize the lambda evaluations away by inlining
    return lax.cond(
        T_in_GeV > _T_HI_GEV,
        lambda: _HIGH_T_GSTAR,
        lambda: lax.cond(
            T_in_GeV >= _T_120_MEV,
            lambda: polynomial_sum(a_coeffs, log_T_in_GeV)
            / polynomial_sum(b_coeffs, log_T_in_GeV),
            lambda: lax.cond(
                T_in_GeV >= _T_LO_GEV,
                lambda: 2.030
                + 1.353 * pow(S_fit(M_e / T_in_GeV), 4.0 / 3.0)
                + 3.495 * f_rho(M_e / T_in_GeV)
                + 3.446 * f_rho(M_mu / T_in_GeV)
                + 1.05 * b_rho(M_pi0 / T_in_GeV)
                + 2.08 * b_rho(M_piplus / T_in_GeV)
                + 4.165 * b_rho(M_1 / T_in_GeV)
                + 30.55 * b_rho(M_2 / T_in_GeV)
                + 89.4 * b_rho(M_3 / T_in_GeV)
                + 8209.0 * b_rho(M_4 / T_in_GeV),
                lambda: _LOW_T_GSTAR,
            ),
        ),
    )


def _raw_G_s(T_in_GeV):
    log_T_in_GeV = log(T_in_GeV)

    return lax.cond(
        T_in_GeV > _T_HI_GEV,
        lambda: _HIGH_T_GSTAR,
        lambda: lax.cond(
            T_in_GeV >= _T_120_MEV,
            lambda: _raw_G_rho(T_in_GeV)
            / (
                1.0
                + polynomial_sum(c_coeffs, log_T_in_GeV)
                / polynomial_sum(d_coeffs, log_T_in_GeV)
            ),
            lambda: lax.cond(
                T_in_GeV >= _T_LO_GEV,
                lambda: 2.008
                + 1.923 * S_fit(M_e / T_in_GeV)
                + 3.442 * f_s(M_e / T_in_GeV)
                + 3.468 * f_s(M_mu / T_in_GeV)
                + 1.034 * b_s(M_pi0 / T_in_GeV)
                + 2.068 * b_s(M_piplus / T_in_GeV)
                + 4.16 * b_s(M_1 / T_in_GeV)
                + 30.55 * b_s(M_2 / T_in_GeV)
                + 90.0 * b_s(M_3 / T_in_GeV)
                + 6209.0 * b_s(M_4 / T_in_GeV),
                lambda: _LOW_T_G_S_STAR,
            ),
        ),
    )


class QCD_EOS(GenericEOSBase):
    EOS_T_LO_GEV = 2e-3

    def __init__(self, units: UnitsLike):
        GenericEOSBase.__init__(self, units)

        # use JAX automatic differentiation to obtain a result for the temperature derivatives
        self._grad_raw_G_rho = grad(_raw_G_rho)
        self._grad_raw_G_s = grad(_raw_G_s)

        self._EOS_T_LO = QCD_EOS.EOS_T_LO_GEV * self._units.GeV

    @property
    def name(self):
        return "QCD equation of state in Saikawa & Shirai parametrization (arXiv:1803.01038)"

    @property
    def type_id(self) -> int:
        # 0 is the unique ID for the LambdaCDM cosmology type
        return QCD_EOS_IDENTIFIER

    # Complete effective degrees of freedom functions

    def _raw_G_rho(self, T):
        """
        Compute effective number of bosonic degrees of freedom g(T) for the energy, at temperature T.
        T should be regarded as a dimensionful quantity, measured in the given UnitsLike system,
        but supplied here as a float for compatibility with JAX tracing
        :param T: dimensionful temperature T
        :return: dimensionless number representing g(T)
        """
        T_in_GeV = T / self._units.GeV
        return _raw_G_rho(T_in_GeV)

    def _raw_dG_rho_dT(self, T):
        # units of the output will be 1/GeV because we internally evaluate T in GeV
        T_in_GeV = T / self._units.GeV
        return self._grad_raw_G_rho(T_in_GeV) / self._units.GeV

    def _raw_G_s(self, T):
        """
        Compute effective number of bosonic degrees of freedom g_S(T) for the entropy, at temperature T
        T should be regarded as a dimensionful quantity, measured in the given UnitsLike system,
        but supplied here as a float for compatibility with JAX tracing
        :param T: dimensionful temperature T
        :return: dimensionless number representing g_S(T)
        """
        T_in_GeV = T / self._units.GeV
        return _raw_G_s(T_in_GeV)

    def _raw_dG_s_dT(self, T):
        # units of the output will be 1/GeV because we internally evaluate T in GeV
        T_in_GeV = T / self._units.GeV
        return self._grad_raw_G_s(T_in_GeV) / self._units.GeV

    # override equation of state implementation
    def _raw_w(self, T):
        """
        Compute equation of state parameter w(T) as a function of temperature T.
        :return:
        """

        # below _T_LO_GEV we have photons and neutrinos, each with
        #   P(T) = s(T) T - rho(T)
        # so we cannot write a single formula for w(T) = P(T)/rho(T) that is valid both above and below _T_LO_GEV.
        # However, with our choices w(z) will just evaluate to 1/3 for all temperatures in this range.
        # To get a smooth result we evaluate the asymptotic value exactly at _T_LO_GEV

        T_in_GeV = T / self._units.GeV

        T = lax.cond(T_in_GeV <= _T_LO_GEV, lambda: self._EOS_T_LO, lambda: T)

        G = self._raw_G_rho(T)
        Gs = self._raw_G_s(T)
        return (4.0 * Gs) / (3.0 * G) - 1.0
