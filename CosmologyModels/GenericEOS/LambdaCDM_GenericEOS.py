from CosmologyConcepts import TemperatureLike, GetTemperature
from CosmologyModels import BaseCosmology
from CosmologyModels.GenericEOS.GenericEOS import GenericEOSBase
from Units.base import UnitsLike
from constants import RadiationConstant


class LambdaCDM_GenericEOS(BaseCosmology):
    """
    Construct a datastore
    """

    def __init__(
        self,
        store_id: int,
        eos: GenericEOSBase,
        units: UnitsLike,
        params,
    ):
        BaseCosmology.__init__(self, store_id)

        self._params = params
        self._units = units
        self._eos = eos

        # unpack details of the parameter block so we can access them without extensive nesting
        self._name = f"{eos.name} | {params.name}"

        # Omega factors are all measured today
        self.omega_cc = params.omega_cc
        self.omega_m = params.omega_m
        self.f_baryon = params.f_baryon
        self.h = params.h
        self.T_CMB_Kelvin = params.T_CMB_Kelvin

        # Neff not used here because it is baked into the G_rho(T) and G_S(T) parametersa computed by the EOS object
        # self.Neff = params.Neff

        # derived dimensionful quantities, expressed in whatever system of units we require
        self._H0 = 100.0 * params.h * units.Kilometre / (units.Second * units.Mpc)
        self._T_CMB = params.T_CMB_Kelvin * units.Kelvin

        self.H0sq = self._H0 * self._H0
        self.Mpsq = units.PlanckMass * units.PlanckMass

        # POPULATE KEY DATA NOT PROVIDED AS PART OF THE PARAMS BLOCK

        T_CMB_2 = self._T_CMB * self._T_CMB
        T_CMB_4 = T_CMB_2 * T_CMB_2

        Omega_factor = 3.0 * self.H0sq * self.Mpsq

        self.rho_m0 = Omega_factor * self.omega_m

        # note the effective G_rho* reported by the EOS object should have reheating
        # of the thermal bath relative to the neutrinos already included.
        # Therefore, we don't need the extra famous factor (4/11)^(4/3)
        self.rho_r0 = RadiationConstant * self._eos.G_rho(self._T_CMB) * T_CMB_4
        self.rho_cc = Omega_factor * self.omega_cc

        self.omega_r = self.rho_r0 / Omega_factor

        # cache values of G_rho(T_CMB), G_S(T_CMB), [G_rho(T_CMB)]^(4/3) and [G_S(T_CMB)]^(4/3) which we need to use later
        self._G_CMB = eos.G_rho(self._T_CMB)
        self._G_S_CMB = eos.G_s(self._T_CMB)
        self._G_CMB_pow13 = pow(self._G_CMB, 1.0 / 3.0)
        self._G_S_CMB_pow13 = pow(self._G_S_CMB, 1.0 / 3.0)

        print(f'@@ Parametrized equation-of-state LambdaCDM-like model "{self._name}"')

    @property
    def type_id(self) -> int:
        # inherit our unique ID from the underlying choice of equation of state
        return self._eos.type_id

    @property
    def name(self) -> str:
        return self._name

    @property
    def units(self) -> UnitsLike:
        return self._units

    @property
    def H0(self) -> float:
        return self._H0

    def G_rho(self, T: TemperatureLike) -> float:
        return self._eos.G_rho(T)

    def G_s(self, T: TemperatureLike) -> float:
        return self._eos.G_s(T)

    def dG_s_dT(self, T: TemperatureLike) -> float:
        return self._eos.dG_s_dT(T)

    def dG_rho_dT(self, T: TemperatureLike) -> float:
        return self._eos.dG_rho_dT(T)

    def w(self, T: TemperatureLike) -> float:
        return self._eos.w(T)

    def z(self, T: TemperatureLike) -> float:
        """
        Compute z(T), the redshift as a function of temperature
        :param T: temperature (as a dimensionful quantity)
        :return: redshift at this temperature
        """

        # in the absence of entropy effects, T(z) a(z) = T_CMB a0, where a0 is the value of
        # the scale factor today, and T_CMB is the radiation temperature today. Then
        # T and z should solve the equation
        #   T = T_CMB (a0/a) = T_CMB (1 + z)
        #   --> 1 + z(T) = T/T_CMB
        # With entropy effects included, this scaling is no longer exact. Instead, T and z
        # should solve the implicit equation
        #   T [G_S(T)]^(1/3) = T_CMB [G_S(T_CMB)]^(1/3) (1 + z)
        #  --> 1 + z(T) = (T/T_CMB) [ G_S(T) / G_S(T_CMB) ]^(1/3)

        T_float: float = GetTemperature(T)

        one_plus_z: float = (
            (T_float / self._T_CMB)
            * pow(self._eos.G_s(T), 1.0 / 3.0)
            / self._G_S_CMB_pow13
        )
        return one_plus_z - 1.0
