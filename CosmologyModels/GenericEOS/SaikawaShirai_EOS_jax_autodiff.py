from typing import Mapping

import jax.numpy as jnp
from jax import grad, Array, config

config.update("jax_enable_x64", True)

from CosmologyConcepts import TemperatureLike, GetTemperature
from CosmologyModels.GenericEOS.GenericEOS import (
    GenericEOSBase,
)
from CosmologyModels.GenericEOS.SaikawaShirai_common import (
    a_coeffs,
    b_coeffs,
    c_coeffs,
    d_coeffs,
    M_e,
    M_mu,
    M_pi0,
    M_1,
    M_2,
    M_3,
    M_4,
    M_piplus,
    HIGH_T_GSTAR,
    LOW_T_GSTAR,
    LOW_T_G_S_STAR,
    SAIKAWA_SHIRAI_T_HI,
    SAIKAWA_SHIRAI_T_120_MEV,
    SAIKAWA_SHIRAI_T_LO,
)
from CosmologyModels.model_ids import QCD_EOS_JAX_AUTODIFF_IDENTIFIER
from Units.base import UnitsLike

_EOS_T_LO = 2e-3


def _jax_polynomial_sum(coeffs: Mapping[int, Array], x: Array) -> Array:
    """Compute sum of polynomial terms"""
    terms = jnp.asarray([c * jnp.pow(x, i) for i, c in coeffs.items()])
    return jnp.sum(terms)


# Fitting functions for energy density
def _jax_f_rho(x: Array) -> Array:
    """Low-temperature fitting function for fermion energy density"""
    x_2 = x * x
    x_3 = x_2 * x
    return jnp.exp(-1.04855 * x) * (
        1.0 + 1.03757 * x + 0.508630 * x_2 + 0.0893988 * x_3
    )


def _jax_b_rho(x: Array) -> Array:
    """Low-temperature fitting function for boson energy density"""
    x_2 = x * x
    x_3 = x_2 * x
    return jnp.exp(-1.03149 * x) * (
        1.0 + 1.03317 * x + 0.398264 * x_2 + 0.0648056 * x_3
    )


# Fitting functions for entropy density
def _jax_f_s(x: Array) -> Array:
    """Low-temperature fitting function for fermion entropy"""
    x_2 = x * x
    x_3 = x_2 * x
    return jnp.exp(-1.04190 * x) * (
        1.0 + 1.03400 * x + 0.456426 * x_2 + 0.0595249 * x_3
    )


def _jax_b_s(x: Array) -> Array:
    """Low-temperature fitting function for boson entropy"""
    x_2 = x * x
    x_3 = x_2 * x
    return jnp.exp(-1.03365 * x) * (
        1.0 + 1.03397 * x + 0.342548 * x_2 + 0.0506182 * x_3
    )


def _jax_S_fit(x: Array) -> Array:
    """Combined entropy fitting function"""
    return 1.0 + (7.0 / 4.0) * _jax_f_s(x)


def _jax_raw_G_rho(T_in_GeV: Array) -> Array:
    if T_in_GeV > SAIKAWA_SHIRAI_T_HI:
        return jnp.asarray(HIGH_T_GSTAR)  # Asymptotic high temperature limit
    elif SAIKAWA_SHIRAI_T_120_MEV <= T_in_GeV <= SAIKAWA_SHIRAI_T_HI:
        log_T_in_GeV = jnp.log(T_in_GeV)
        return _jax_polynomial_sum(a_coeffs, log_T_in_GeV) / _jax_polynomial_sum(
            b_coeffs, log_T_in_GeV
        )
    elif SAIKAWA_SHIRAI_T_LO <= T_in_GeV <= SAIKAWA_SHIRAI_T_120_MEV:
        # Eq. (C.3) of 1803.01038 v2
        return (
            2.030
            + 1.353 * pow(_jax_S_fit(M_e / T_in_GeV), 4.0 / 3.0)
            + 3.495 * _jax_f_rho(M_e / T_in_GeV)
            + 3.446 * _jax_f_rho(M_mu / T_in_GeV)
            + 1.05 * _jax_b_rho(M_pi0 / T_in_GeV)
            + 2.08 * _jax_b_rho(M_piplus / T_in_GeV)
            + 4.165 * _jax_b_rho(M_1 / T_in_GeV)
            + 30.55 * _jax_b_rho(M_2 / T_in_GeV)
            + 89.4 * _jax_b_rho(M_3 / T_in_GeV)
            + 8209.0 * _jax_b_rho(M_4 / T_in_GeV)
        )
    else:
        return jnp.asarray(LOW_T_GSTAR)  # Low temperature limit


def _jax_raw_G_s(T_in_GeV: Array) -> Array:
    if T_in_GeV > SAIKAWA_SHIRAI_T_HI:
        return jnp.asarray(HIGH_T_GSTAR)  # Asymptotic high temperature limit
    elif SAIKAWA_SHIRAI_T_120_MEV <= T_in_GeV <= SAIKAWA_SHIRAI_T_HI:
        log_T_in_GeV = jnp.log(T_in_GeV)
        return _jax_raw_G_rho(T_in_GeV) / (
            1.0
            + _jax_polynomial_sum(c_coeffs, log_T_in_GeV)
            / _jax_polynomial_sum(d_coeffs, log_T_in_GeV)
        )
    elif SAIKAWA_SHIRAI_T_LO <= T_in_GeV <= SAIKAWA_SHIRAI_T_120_MEV:
        # Eq. (C.4) of 1803.01038 v2
        return (
            2.008
            + 1.923 * _jax_S_fit(M_e / T_in_GeV)
            + 3.442 * _jax_f_s(M_e / T_in_GeV)
            + 3.468 * _jax_f_s(M_mu / T_in_GeV)
            + 1.034 * _jax_b_s(M_pi0 / T_in_GeV)
            + 2.068 * _jax_b_s(M_piplus / T_in_GeV)
            + 4.16 * _jax_b_s(M_1 / T_in_GeV)
            + 30.55 * _jax_b_s(M_2 / T_in_GeV)
            + 90.0 * _jax_b_s(M_3 / T_in_GeV)
            + 6209.0 * _jax_b_s(M_4 / T_in_GeV)
        )
    else:
        return jnp.asarray(LOW_T_G_S_STAR)  # Low temperature limit


class SaikawaShirai_EOS_jax_autodiff(GenericEOSBase):

    # above SAIKAWA_SHIRAI_SAIKAWA_SHIRAI_T_HI (measured in GeV) we assume the asymptotic high temperature degrees of freedom
    SAIKAWA_SHIRAI_T_HI = 1e16

    # boundary value (measured in GeV) from Saikawa & Shirai
    SAIKAWA_SHIRAI_T_120_MEV = 0.12

    # below SAIKAWA_SHIRAI_SAIKAWA_SHIRAI_T_LO (measured in GeV) we assume the asymptotic low temperature degrees of freedom.
    # Saikawa & Shirai say that their parametrization is valid down to 10 keV = 1E-5 GeV, which
    # may well be true for G_rho(T) and G_S(T) alone, but we will get the wrong equation of state
    # if we do this, because the neutrinos are already out of thermal equilibrium here.
    # We cut to the late-time asymptotic values at 600 keV, just before e+e- annihilation reheats
    # the photon temperature.
    SAIKAWA_SHIRAI_T_LO = 1e-5
    EOS_SAIKAWA_SHIRAI_T_LO = 2e-3

    def __init__(self, units: UnitsLike):
        GenericEOSBase.__init__(self, units)

        # use JAX automatic differentiation to obtain a result for the temperature derivatives
        self._grad_raw_G_rho = grad(_jax_raw_G_rho)
        self._grad_raw_G_s = grad(_jax_raw_G_s)

    @property
    def name(self):
        return "QCD equation of state in Saikawa & Shirai parametrization (arXiv:1803.01038, jax autodiff)"

    @property
    def type_id(self) -> int:
        # 0 is the unique ID for the LambdaCDM cosmology type
        return QCD_EOS_JAX_AUTODIFF_IDENTIFIER

    # Complete effective degrees of freedom functions

    def G_rho(self, T: TemperatureLike) -> Array:
        """
        Compute effective number of bosonic degrees of freedom g(T) for the energy, at temperature T.
        T should be regarded as a dimensionful quantity, measured in the given UnitsLike system
        :param T: dimensionful temperature T
        :return: dimensionless number representing g(T)
        """

        T_in_GeV = GetTemperature(T) / self._units.GeV
        return _jax_raw_G_rho(T_in_GeV)

    def dG_rho_dlogT(self, T: TemperatureLike) -> Array:

        # units of the output will be 1/GeV because we internally evaluate T in GeV
        T_in_GeV = GetTemperature(T) / self._units.GeV
        return T_in_GeV * self._grad_raw_G_rho(T_in_GeV)

    def G_s(self, T: TemperatureLike) -> Array:
        """
        Compute effective number of bosonic degrees of freedom g_S(T) for the entropy, at temperature T
        T should be regarded as a dimensionful quantity, measured in the given UnitsLike system
        :param T: dimensionful temperature T
        :return: dimensionless number representing g_S(T)
        """

        T_in_GeV = GetTemperature(T) / self._units.GeV
        return _jax_raw_G_s(T_in_GeV)

    def dG_s_dlogT(self, T: TemperatureLike) -> Array:

        # units of the output will be 1/GeV because we internally evaluate T in GeV
        T_in_GeV = GetTemperature(T) / self._units.GeV
        return T_in_GeV * self._grad_raw_G_s(T_in_GeV)

    # override equation of state implementation
    def w(self, T: TemperatureLike) -> Array:
        """
        Compute equation of state parameter w(T) as a function of temperature T.
        :return:
        """

        # below SAIKAWA_SHIRAI_SAIKAWA_SHIRAI_T_LO we have photons and neutrinos, each with
        #   P(T) = s(T) T - rho(T)
        # so we cannot write a single formula for w(T) = P(T)/rho(T) that is valid both above and below SAIKAWA_SHIRAI_SAIKAWA_SHIRAI_T_LO.
        # However, with our choices w(z) will just evaluate to 1/3 for all temperatures in this range.
        # To get a smooth result we evaluate the asymptotic value exactly at SAIKAWA_SHIRAI_SAIKAWA_SHIRAI_T_LO

        T_in_GeV: float = GetTemperature(T) / self._units.GeV

        if T_in_GeV <= _EOS_T_LO:
            T = _EOS_T_LO * self._units.GeV

        G = self.G_rho(T)
        Gs = self.G_s(T)
        return (4.0 * Gs) / (3.0 * G) - 1.0
