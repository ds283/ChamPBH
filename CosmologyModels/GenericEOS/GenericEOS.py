from abc import ABC, abstractmethod

from CosmologyConcepts import TemperatureLike
from Units.base import UnitsLike

# at high temperature, G_rho and G_S usually have the same value
HIGH_T_GSTAR = 106.75

# G_rho and G_S usually only differ at low temperatures after neutrino decoupling, once e+/e- annihilation
# reheats the photons (but *not* the neutrinos)
# LOW_T_GSTAR = 3.36
# LOW_T_G_S_STAR = 3.91

# TODO: check, https://www.astronomy.ohio-state.edu/weinberg.21/A8873/notes7a.pdf quotes instead
LOW_T_GSTAR = 3.38
LOW_T_G_S_STAR = 3.94
# these values look correct to me because e.g.
#   2 + 2 * 3.042 * (7/8) * (4/11)^(4/3) = 3.38172
# so this value of G_rho* includes N_eff from Planck, plus reheating of the photons but not the neutrinos


class GenericEOSBase(ABC):

    def __init__(self, units: UnitsLike):
        self._units = units

    @property
    @abstractmethod
    def name(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def type_id(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def G_rho(self, T: TemperatureLike) -> float:
        """
        Compute effective number of bosonic degrees of freedom g(T) for the energy, at temperature T.
        T should be regarded as a dimensionful quantity, measured in the given UnitsLike system
        :param T: dimensionful temperature T
        :return: dimensionless number representing g(T)
        """
        raise NotImplementedError

    @abstractmethod
    def G_s(self, T: TemperatureLike) -> float:
        """
        Compute effective number of bosonic degrees of freedom g_S(T) for the entropy, at temperature T
        T should be regarded as a dimensionful quantity, measured in the given UnitsLike system
        :param T: dimensionful temperature T
        :return: dimensionless number representing g_S(T)
        """
        raise NotImplementedError

    @abstractmethod
    def dG_s_dT(self, T: TemperatureLike) -> float:
        """
        Compute derivative of G_s(T) with respect to temperature T
        T should be regarded as a dimensionful quantity, measured in the given UnitsLike system
        :param T: dimensionful temperature T
        :return: DIMENSIONFUL number representing d(g_S)/dT at T (units should be inverse to T)
        """
        raise NotImplementedError

    def w(self, T: TemperatureLike) -> float:
        """
        Generic formula for equation of state parameter w(T) as a function of temperature T.
        T should be regarded as a dimensionful quantity, measured in the given UnitsLike system.
        :return:
        """

        # Obtained using s = (rho + P)/T and therefore sT = rho + P
        # Since w = P/rho, we get 1 + w = sT/rho.
        # Now, expressing S in terms of g*_s and rho in terms of g*_rho, gives the required formula.

        # Notice that g*_s and g*_rho can't be independent. They must satisfy quite a nontrivial differential constraint
        # in order to make this formula for w(T) compatible with the continuity equation
        # d ln(rho)/dt = 3 (1 + w), because this naively involves derivatives of both g*_s and g*_rho

        G = self.G_rho(T)
        Gs = self.G_s(T)
        w = (4.0 * Gs) / (3.0 * G) - 1.0

        # print(
        #     f">> evaluate w(T) at T = {T/self._units.GeV:.5g} GeV = {T/self._units.Kelvin:.5g} K | g* = {G_rho:.5g}, g_S* = {G_s:.5g}, w = {w:.5g}"
        # )

        return w
