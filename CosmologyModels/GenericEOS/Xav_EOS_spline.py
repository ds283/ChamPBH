import numpy as np
import pandas as pd
from scipy.interpolate import make_interp_spline

from CosmologyConcepts import TemperatureLike, GetTemperature
from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import SaikawaShirai_EOS_spline
from CosmologyModels.model_ids import (
    XAV_IMPROVED_EOS_IDENTIFIER,
)
from Units.base import UnitsLike

_EOS_T_LO = 2e-3


class Xav_EOS_spline(SaikawaShirai_EOS_spline):

    def __init__(self, units: UnitsLike):
        SaikawaShirai_EOS_spline.__init__(self, units)

        self._data = pd.read_csv("CosmologyModels/GenericEOS/Xav_EOS_data.csv")
        self._T_series = self._data["T_GeV"]
        self._w_series = self._data["w"]

        self._T_max = self._T_series.max().astype(float)
        self._T_min = self._T_series.min().astype(float)

        self._log_T_series = np.asarray([np.log(T) for T in self._T_series])
        self._spline = make_interp_spline(self._log_T_series, self._w_series)

    @property
    def name(self):
        return "QCD equation of state in Saikawa & Shirai parametrization (arXiv:1803.01038, splined)"

    @property
    def type_id(self) -> int:
        # 0 is the unique ID for the LambdaCDM cosmology type
        return XAV_IMPROVED_EOS_IDENTIFIER

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

        if T_in_GeV >= self._T_max:
            return 1.0 / 3.9
        elif T_in_GeV <= self._T_min:
            return 1.0 / 3.0

        return self._spline(np.log(T_in_GeV))
