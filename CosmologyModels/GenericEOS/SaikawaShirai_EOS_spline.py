import numpy as np
from scipy.interpolate import make_interp_spline

from CosmologyConcepts import TemperatureLike, GetTemperature
from CosmologyModels.GenericEOS.GenericEOS import (
    GenericEOSBase,
)
from CosmologyModels.GenericEOS.SaikawaShirai_common import (
    _raw_G_rho,
    _raw_G_s,
    SAIKAWA_SHIRAI_T_LO,
    SAIKAWA_SHIRAI_T_HI,
    HIGH_T_GSTAR,
    LOW_T_GSTAR,
    LOW_T_G_S_STAR,
)
from CosmologyModels.model_ids import (
    QCD_EOS_SPLINE_IDENTIFIER,
)
from Units.base import UnitsLike

_EOS_T_LO = 2e-3

_SAMPLES_PER_LOG10_T = 250

_LOG10_SAIKAWA_SHIRAI_T_HI = np.log10(SAIKAWA_SHIRAI_T_HI)
_LOG10_SAIKAWA_SHIRAI_T_LO = np.log10(SAIKAWA_SHIRAI_T_LO)


class SaikawaShirai_EOS_spline(GenericEOSBase):

    def __init__(self, units: UnitsLike):
        GenericEOSBase.__init__(self, units)

        log10_lo = np.log10(0.8 * SAIKAWA_SHIRAI_T_LO)
        log10_hi = np.log10(1.2 * SAIKAWA_SHIRAI_T_HI)
        range = log10_hi - log10_lo
        samples = int(round(_SAMPLES_PER_LOG10_T * range + 0.5, 0))

        self._log_T_grid = np.linspace(
            log10_lo,
            log10_hi,
            samples,
            endpoint=True,
        )

        self._gstar_rho_grid = np.asarray(
            [_raw_G_rho(np.pow(10.0, T)) for T in self._log_T_grid]
        )
        self._g_star_rho_spline = make_interp_spline(
            self._log_T_grid,
            self._gstar_rho_grid,
        )
        self._dg_star_dlogT_spline = self._g_star_rho_spline.derivative()

        self._gstar_s_grid = np.asarray(
            [_raw_G_s(np.pow(10.0, T)) for T in self._log_T_grid]
        )
        self._g_star_s_spline = make_interp_spline(
            self._log_T_grid,
            self._gstar_s_grid,
        )
        self._dg_star_s_dlogT_spline = self._g_star_s_spline.derivative()

    @property
    def name(self):
        return "QCD equation of state in Saikawa & Shirai parametrization (arXiv:1803.01038, splined)"

    @property
    def type_id(self) -> int:
        # 0 is the unique ID for the LambdaCDM cosmology type
        return QCD_EOS_SPLINE_IDENTIFIER

    # Complete effective degrees of freedom functions

    def G_rho(self, T: TemperatureLike) -> float:
        """
        Compute effective number of bosonic degrees of freedom g(T) for the energy, at temperature T.
        T should be regarded as a dimensionful quantity, measured in the given UnitsLike system
        :param T: dimensionful temperature T
        :return: dimensionless number representing g(T)
        """

        T_in_GeV = GetTemperature(T) / self._units.GeV
        if T_in_GeV <= 0.0:
            raise RuntimeError(
                f"!! SaikawaShirai_EOS_spline.G_rho: Temperature T = {T_in_GeV:.5g} GeV (raw T = {T:.5g}) is negative"
            )

        log10_T_in_GeV = np.log10(T_in_GeV)

        if log10_T_in_GeV >= _LOG10_SAIKAWA_SHIRAI_T_HI:
            return HIGH_T_GSTAR
        elif log10_T_in_GeV <= _LOG10_SAIKAWA_SHIRAI_T_LO:
            return LOW_T_GSTAR

        return self._g_star_rho_spline(log10_T_in_GeV)

    def dG_rho_dlogT(self, T: TemperatureLike) -> float:

        # units of the output will be 1/GeV because we internally evaluate T in GeV
        T_in_GeV = GetTemperature(T) / self._units.GeV
        if T_in_GeV <= 0.0:
            raise RuntimeError(
                f"!! SaikawaShirai_EOS_spline.dG_rho_dlogT: Temperature T = {T_in_GeV:.5g} GeV (raw T = {T:.5g}) is negative"
            )

        log10_T_in_GeV = np.log10(T_in_GeV)

        if log10_T_in_GeV >= _LOG10_SAIKAWA_SHIRAI_T_HI:
            return 0.0
        elif log10_T_in_GeV <= _LOG10_SAIKAWA_SHIRAI_T_LO:
            return 0.0

        return self._dg_star_dlogT_spline(log10_T_in_GeV)

    def G_s(self, T: TemperatureLike) -> float:
        """
        Compute effective number of bosonic degrees of freedom g_S(T) for the entropy, at temperature T
        T should be regarded as a dimensionful quantity, measured in the given UnitsLike system
        :param T: dimensionful temperature T
        :return: dimensionless number representing g_S(T)
        """

        T_in_GeV = GetTemperature(T) / self._units.GeV
        if T_in_GeV <= 0.0:
            raise RuntimeError(
                f"!! SaikawaShirai_EOS_spline.G_s: Temperature T = {T_in_GeV:.5g} GeV (raw T = {T:.5g}) is negative"
            )

        log10_T_in_GeV = np.log10(T_in_GeV)

        if log10_T_in_GeV >= _LOG10_SAIKAWA_SHIRAI_T_HI:
            return HIGH_T_GSTAR
        elif log10_T_in_GeV <= _LOG10_SAIKAWA_SHIRAI_T_LO:
            return LOW_T_G_S_STAR

        return self._g_star_s_spline(log10_T_in_GeV)

    def dG_s_dlogT(self, T: TemperatureLike) -> float:

        # units of the output will be 1/GeV because we internally evaluate T in GeV
        T_in_GeV = GetTemperature(T) / self._units.GeV
        if T_in_GeV <= 0.0:
            raise RuntimeError(
                f"!! SaikawaShirai_EOS_spline.dG_s_dlogT: Temperature T = {T_in_GeV:.5g} GeV (raw T = {T:.5g}) is negative"
            )

        log10_T_in_GeV = np.log10(T_in_GeV)

        if log10_T_in_GeV >= _LOG10_SAIKAWA_SHIRAI_T_HI:
            return 0.0
        elif log10_T_in_GeV <= _LOG10_SAIKAWA_SHIRAI_T_LO:
            return 0.0

        return self._dg_star_s_dlogT_spline(log10_T_in_GeV)

    # override equation of state implementation
    def w(self, T: TemperatureLike) -> float:
        """
        Compute equation of state parameter w(T) as a function of temperature T.
        :return:
        """

        # below SAIKAWA_SHIRAI_T_LO we have photons and neutrinos, each with
        #   P(T) = s(T) T - rho(T)
        # so we cannot write a single formula for w(T) = P(T)/rho(T) that is valid both above and below SAIKAWA_SHIRAI_T_LO.
        # However, with our choices w(z) will just evaluate to 1/3 for all temperatures in this range.
        # To get a smooth result we evaluate the asymptotic value exactly at SAIKAWA_SHIRAI_T_LO

        T_in_GeV: float = GetTemperature(T) / self._units.GeV
        if T_in_GeV <= 0.0:
            raise RuntimeError(
                f"!! SaikawaShirai_EOS_spline.w: Temperature T = {T_in_GeV:.5g} GeV (raw T = {T:.5g}) is negative"
            )

        if T_in_GeV <= _EOS_T_LO:
            T = _EOS_T_LO * self._units.GeV

        G = self.G_rho(T)
        Gs = self.G_s(T)
        return (4.0 * Gs) / (3.0 * G) - 1.0
