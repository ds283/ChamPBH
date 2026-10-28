from math import isnan, isinf, exp

from ComputeTargets.exceptions import ComputationFailureError
from CosmologyConcepts.Models.AbstractModel import RealizedModel
from CosmologyModels import BaseCosmology
from Units.base import UnitsLike


class PotentialDerivativePolicy:
    """
    V/3H^2 M_P^2 and its derivatives, from the realized model's log V (jordan-normalization
    prompt 04, M4). Until that prompt the policy also offered a plain-V route, for a potential
    that implemented V but not log V; the exponential model is the only one, and RealizedModel
    offers log V only, so the route had no user and was removed.
    """

    def __init__(self, task_label: str, cosmology: BaseCosmology, model: RealizedModel):
        self.task_label: str = task_label
        self.units: UnitsLike = cosmology.units

        self.model: RealizedModel = model

        self.MP = self.units.PlanckMass
        self.CONST_MP_SQ = self.MP * self.MP
        self.CONST_3_MP_SQ = 3.0 * self.CONST_MP_SQ
        self.CONST_6_MP_SQ = 6.0 * self.CONST_MP_SQ

        self.GeV = self.units.GeV
        self.Kelvin = self.units.Kelvin

    def log_V(self, phi_Einstein: float) -> float:
        return self.model.log_V(phi_Einstein)

    def _evaluate_V_over_3H2Mp2_using_log_V(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        fm: float,
        r_chi: float = 0.0,
    ) -> float:
        log_V = self.model.log_V(phi_Einstein)
        # can assume log_V is not nan or inf at this stage

        log_rhorad_over_V: float = log_rhorad_Einstein - log_V

        # T = V/(V + rho_R (1 + f_m) + rho_chi) = 1/(1 + (rho_R/V)(1 + f_m + r_chi)), with
        # r_chi = rho_chi,E/rho_R,E (inelastic-reflection prompt 01). With no dark radiation
        # (r_chi == 0.0) each branch takes exactly the operations it took before, so that K = 0 is
        # the old code bitwise (README §5)
        if log_rhorad_over_V > 2.0:
            V_over_rhorad: float = exp(-log_rhorad_over_V)
            if r_chi == 0.0:
                T = V_over_rhorad / (V_over_rhorad + 1.0 + fm)
            else:
                T = V_over_rhorad / (V_over_rhorad + 1.0 + fm + r_chi)
        else:
            rhorad_over_V: float = exp(log_rhorad_over_V)
            if r_chi == 0.0:
                T = 1.0 / (1.0 + rhorad_over_V * (1.0 + fm))
            else:
                T = 1.0 / (1.0 + rhorad_over_V * (1.0 + fm + r_chi))

        G: float = 1.0 - pi_Einstein * pi_Einstein / self.CONST_6_MP_SQ
        if G < 0.0:
            msg = f"!! ODEPolicy ({self.task_label}): negative value of G = {G:.5g} | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            print(msg)
            raise ComputationFailureError(msg)

        V_over_3H2Mp2: float = G * T

        return V_over_3H2Mp2

    def V_over_3H2Mp2(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        fm: float,
        r_chi: float = 0.0,
    ) -> float:
        # obtain value of scalar potential
        log_V: float = self.model.log_V(phi_Einstein)

        if isnan(log_V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): log_V=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(log_V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): log_V=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        return self._evaluate_V_over_3H2Mp2_using_log_V(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm, r_chi=r_chi
        )

    def _evaluate_Vprime_over_3H2Mp2_using_log_V(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        fm: float,
        r_chi: float = 0.0,
    ) -> float:
        Vprime_over_V: float = self.model.d_logV_dphi(phi_Einstein)

        if isnan(Vprime_over_V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): V'/V=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(Vprime_over_V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): V'/V=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        V_over_3H2Mp2: float = self.V_over_3H2Mp2(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm, r_chi=r_chi
        )

        return Vprime_over_V * V_over_3H2Mp2

    def Vprime_over_3H2Mp2(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        fm: float,
        r_chi: float = 0.0,
    ) -> float:
        # obtain value of scalar potential
        log_V: float = self.model.log_V(phi_Einstein)

        if isnan(log_V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): log_V=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(log_V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): log_V=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        return self._evaluate_Vprime_over_3H2Mp2_using_log_V(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm, r_chi=r_chi
        )

    def _evaluate_Vprimeprime_over_3H2Mp2_using_log_V(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        fm: float,
        r_chi: float = 0.0,
    ) -> float:
        logV_prime: float = self.model.d_logV_dphi(phi_Einstein)
        logV_primeprime: float = self.model.d2_logV_dphi2(phi_Einstein)

        if isnan(logV_primeprime):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): (log V)''=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(logV_primeprime):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): (log V)''=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        V_over_3H2Mp2: float = self.V_over_3H2Mp2(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm, r_chi=r_chi
        )

        Vprimeprime_over_V = logV_primeprime + logV_prime * logV_prime

        return Vprimeprime_over_V * V_over_3H2Mp2

    def Vprimeprime_over_3H2Mp2(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        fm: float,
        r_chi: float = 0.0,
    ) -> float:
        # obtain value of scalar potential
        log_V: float = self.model.log_V(phi_Einstein)

        if isnan(log_V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): log_V=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(log_V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): log_V=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        return self._evaluate_Vprimeprime_over_3H2Mp2_using_log_V(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm, r_chi=r_chi
        )

    @staticmethod
    def matter_and_dark_radiation_fractions(
        G: float, V_over_3H2Mp2: float, fm: float, r_chi: float
    ) -> tuple:
        """
        (E, Y) = (rho_R (1 + f_m), rho_chi)/3H^2 M_P^2, for r_chi = rho_chi,E/rho_R,E != 0
        (inelastic-reflection prompt 01). The constraint 3 M_P^2 H^2 G = V + rho_R (1 + f_m) + rho_chi
        gives G - V/3H^2 M_P^2 = E + Y, and E : Y = (1 + f_m) : r_chi, so

            E = (G - V/3H^2 M_P^2) (1 + f_m)/(1 + f_m + r_chi),
            Y = (G - V/3H^2 M_P^2) r_chi/(1 + f_m + r_chi).

        Each is a product and a quotient of G - V/3H^2 M_P^2, so neither adds a cancellation to the
        one that difference carries when V dominates. E = (G - V/3H^2 M_P^2) - Y would add one, of
        two terms whose ratio tends to 1 as r_chi grows (the derivation is in the prompt's log).
        """
        G_minus_U: float = G - V_over_3H2Mp2
        denominator: float = 1.0 + fm + r_chi
        E: float = G_minus_U * (1.0 + fm) / denominator
        Y: float = G_minus_U * r_chi / denominator
        return E, Y

    def Hdot_over_H2_plus_3(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        Sigma,
        fm: float,
        r_chi: float = 0.0,
    ):
        # G must be positive in order that H_Einstein^2 is also positive
        # this gives a limit pi_Einstein < sqrt(6) Mp
        # equality only happens if the scalar field KE dominates the energy budget of the universe,
        # which we should not encounter
        G: float = 1.0 - pi_Einstein * pi_Einstein / self.CONST_6_MP_SQ
        if G < 0.0:
            msg = f"!! ODEPolicy ({self.task_label}): negative value of G = {G:.5g} | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            print(msg)
            raise ComputationFailureError(msg)

        # compute R = (Sigma + fm)/(1 + fm)
        # try to avoid any problems with overflow
        R: float
        if fm > 10.0:
            R = (1.0 + Sigma / fm) / (1.0 + 1.0 / fm)
        else:
            R = (Sigma + fm) / (1.0 + fm)
        A1: float = 1.0 + R / 2.0
        A2: float = 4.0 - R

        V_over_3H2Mp2 = self.V_over_3H2Mp2(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm, r_chi=r_chi
        )

        C: float = V_over_3H2Mp2 / 2.0

        if r_chi == 0.0:
            # no dark radiation: exactly the old operations (README §5, "alpha = 0 is the old code")
            return G * A1 + C * A2

        # dark radiation, w = 1/3 (inelastic-reflection prompt 01): with Y = rho_chi/3H^2 M_P^2,
        # Hdot/H^2 + 3 = G A1 + C A2 - Y R/2. The term vanishes when R = 0 (a radiation bath), where
        # Hdot/H^2 = -2 whatever rho_chi is
        _, Y = self.matter_and_dark_radiation_fractions(G, V_over_3H2Mp2, fm, r_chi)
        return G * A1 + C * A2 - Y * R / 2.0
