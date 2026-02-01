from math import log, isnan, isinf, exp

from numpy import inf

from ComputeTargets.exceptions import ComputationFailureError
from CosmologyConcepts.Potentials import AbstractPotential
from CosmologyModels import BaseCosmology
from Units.base import UnitsLike


class PotentialDerivativePolicy:
    def __init__(
        self, task_label: str, cosmology: BaseCosmology, potential: AbstractPotential
    ):
        self.task_label: str = task_label
        self.units: UnitsLike = cosmology.units

        self.potential: AbstractPotential = potential

        # check what features this potential implementation offers
        self._has_log_V_functions: bool = False
        if hasattr(self.potential, "log_V") and hasattr(self.potential, "d_logV_dphi"):
            self._has_log_V_functions = True

        self._has_plain_V_functions: bool = False
        if hasattr(self.potential, "V") and hasattr(self.potential, "dV_dphi"):
            self._has_plain_V_functions = True

        self.MP = self.units.PlanckMass
        self.CONST_MP_SQ = self.MP * self.MP
        self.CONST_3_MP_SQ = 2.0 * self.CONST_MP_SQ
        self.CONST_6_MP_SQ = 6.0 * self.CONST_MP_SQ

        self.GeV = self.units.GeV
        self.Kelvin = self.units.Kelvin

        # check what features this potential implementation offers
        self._has_log_V_functions: bool = False
        if (
            hasattr(self.potential, "log_V")
            and hasattr(self.potential, "d_logV_dphi")
            and hasattr(self.potential, "d2_logV_dphi2")
        ):
            self._has_log_V_functions = True

        self._has_plain_V_functions: bool = False
        if (
            hasattr(self.potential, "V")
            and hasattr(self.potential, "dV_dphi")
            and hasattr(self.potential, "d2V_dphi2")
        ):
            self._has_plain_V_functions = True

        if not self._has_log_V_functions and not self._has_plain_V_functions:
            raise RuntimeError(
                f'PotentialDerivativePolicy: supplied potential "{self.potential.name}" must implement either logarithmic or ordinary potential derivatives'
            )

    def log_V(self, phi_Einstein: float) -> float:
        if self._has_log_V_functions:
            return self.potential.log_V(phi_Einstein)

        V: float = self.potential.V(phi_Einstein)

        if V > 0.0:
            return log(V)
        else:
            return -inf

    def _evaluate_V_over_3H2Mp2_using_plain_V(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        fm: float,
    ) -> float:
        V: float = self.potential.V(phi_Einstein)

        if isnan(V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): V=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): V=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        rhorad_Einstein: float = exp(log_rhorad_Einstein)

        G: float = 1.0 - pi_Einstein * pi_Einstein / self.CONST_6_MP_SQ
        if G < 0.0:
            msg = f"!! ODEPolicy ({self.task_label}): negative value of G = {G:.5g} | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            print(msg)
            raise ComputationFailureError(msg)

        ThreeH2Mp2: float = (V + rhorad_Einstein * (1.0 + fm)) / G

        V_over_3H2Mp2: float = V / ThreeH2Mp2

        return V_over_3H2Mp2

    def _evaluate_V_over_3H2Mp2_using_log_V(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        fm: float,
    ) -> float:
        log_V = self.potential.log_V(phi_Einstein)
        # can assume log_V is not nan or inf at this stage

        log_rhorad_over_V: float = log_rhorad_Einstein - log_V

        if log_rhorad_over_V > 2.0:
            V_over_rhorad: float = exp(-log_rhorad_over_V)
            T = V_over_rhorad / (V_over_rhorad + 1.0 + fm)
        else:
            rhorad_over_V: float = exp(log_rhorad_over_V)
            T = 1.0 / (1.0 + rhorad_over_V * (1.0 + fm))

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
    ) -> float:
        if not self._has_log_V_functions:
            # potential does not implement log_V and d_log_V, so we only have the option to use raw V
            return self._evaluate_V_over_3H2Mp2_using_plain_V(
                phi_Einstein,
                pi_Einstein,
                log_rhorad_Einstein,
                fm,
            )

        # obtain value of scalar potential
        log_V: float = self.potential.log_V(phi_Einstein)

        if isnan(log_V):
            if self._has_plain_V_functions:
                # fall back on plain V
                return self._evaluate_V_over_3H2Mp2_using_plain_V(
                    phi_Einstein,
                    pi_Einstein,
                    log_rhorad_Einstein,
                    fm,
                )

            msg = f"!! PotentialDerivativePolicy ({self.task_label}): log_V=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(log_V):
            if self._has_plain_V_functions:
                # fall back on plain V
                return self._evaluate_V_over_3H2Mp2_using_plain_V(
                    phi_Einstein,
                    pi_Einstein,
                    log_rhorad_Einstein,
                    fm,
                )

            msg = f"!! PotentialDerivativePolicy ({self.task_label}): log_V=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        return self._evaluate_V_over_3H2Mp2_using_log_V(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm
        )

    def _evaluate_Vprime_over_3H2Mp2_using_plain_V(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        fm: float,
    ) -> float:
        V: float = self.potential.V(phi_Einstein)
        Vprime: float = self.potential.dV_dphi(phi_Einstein)

        if isnan(V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): V=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): V=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isnan(Vprime):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): Vprime=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(Vprime):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): Vprime=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        G: float = 1.0 - pi_Einstein * pi_Einstein / self.CONST_6_MP_SQ
        if G < 0.0:
            msg = f"!! ODEPolicy ({self.task_label}): negative value of G = {G:.5g} | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            print(msg)
            raise ComputationFailureError(msg)

        rhorad_Einstein: float = exp(log_rhorad_Einstein)

        ThreeH2Mp2: float = (V + rhorad_Einstein * (1.0 + fm)) / G

        Vprime_over_3H2Mp2: float = Vprime / ThreeH2Mp2

        return Vprime_over_3H2Mp2

    def _evaluate_Vprime_over_3H2Mp2_using_log_V(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        fm: float,
    ) -> float:
        Vprime_over_V: float = self.potential.d_logV_dphi(phi_Einstein)

        if isnan(Vprime_over_V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): V'/V=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(Vprime_over_V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): V'/V=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        V_over_3H2Mp2: float = self.V_over_3H2Mp2(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm
        )

        return Vprime_over_V * V_over_3H2Mp2

    def Vprime_over_3H2Mp2(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        fm: float,
    ) -> float:
        if not self._has_log_V_functions:
            # potential does not implement log_V and d_log_V, so we only have the option to use raw V
            return self._evaluate_Vprime_over_3H2Mp2_using_plain_V(
                phi_Einstein,
                pi_Einstein,
                log_rhorad_Einstein,
                fm,
            )

        # obtain value of scalar potential
        log_V: float = self.potential.log_V(phi_Einstein)

        if isnan(log_V):
            if self._has_plain_V_functions:
                # fall back on plain V
                return self._evaluate_Vprime_over_3H2Mp2_using_plain_V(
                    phi_Einstein,
                    pi_Einstein,
                    log_rhorad_Einstein,
                    fm,
                )

            msg = f"!! PotentialDerivativePolicy ({self.task_label}): log_V=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(log_V):
            if self._has_plain_V_functions:
                # fall back on plain V
                return self._evaluate_Vprime_over_3H2Mp2_using_plain_V(
                    phi_Einstein,
                    pi_Einstein,
                    log_rhorad_Einstein,
                    fm,
                )

            msg = f"!! PotentialDerivativePolicy ({self.task_label}): log_V=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        return self._evaluate_Vprime_over_3H2Mp2_using_log_V(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm
        )

    def _evaluate_Vprimeprime_over_3H2Mp2_using_plain_V(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        fm: float,
    ) -> float:
        V: float = self.potential.V(phi_Einstein)
        Vprimeprime: float = self.potential.d2V_dphi2(phi_Einstein)

        if isnan(V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): V=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(V):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): V=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isnan(Vprimeprime):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): Vprime=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(Vprimeprime):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): Vprime=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        G: float = 1.0 - pi_Einstein * pi_Einstein / self.CONST_6_MP_SQ
        if G < 0.0:
            msg = f"!! ODEPolicy ({self.task_label}): negative value of G = {G:.5g} | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            print(msg)
            raise ComputationFailureError(msg)

        rhorad_Einstein: float = exp(log_rhorad_Einstein)

        ThreeH2Mp2: float = (V + rhorad_Einstein * (1.0 + fm)) / G

        Vprime_over_3H2Mp2: float = Vprimeprime / ThreeH2Mp2

        return Vprime_over_3H2Mp2

    def _evaluate_Vprimeprime_over_3H2Mp2_using_log_V(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        fm: float,
    ) -> float:
        logV_prime: float = self.potential.d_logV_dphi(phi_Einstein)
        logV_primeprime: float = self.potential.d2_logV_dphi2(phi_Einstein)

        if isnan(logV_primeprime):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): (log V)''=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(logV_primeprime):
            msg = f"!! PotentialDerivativePolicy ({self.task_label}): (log V)''=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        V_over_3H2Mp2: float = self.V_over_3H2Mp2(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm
        )

        Vprimeprime_over_V = logV_primeprime + logV_prime * logV_prime

        return Vprimeprime_over_V * V_over_3H2Mp2

    def Vprimeprime_over_3H2Mp2(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        fm: float,
    ) -> float:
        if not self._has_log_V_functions:
            # potential does not implement log_V and d_log_V, so we only have the option to use raw V
            return self._evaluate_Vprimeprime_over_3H2Mp2_using_plain_V(
                phi_Einstein,
                pi_Einstein,
                log_rhorad_Einstein,
                fm,
            )

        # obtain value of scalar potential
        log_V: float = self.potential.log_V(phi_Einstein)

        if isnan(log_V):
            if self._has_plain_V_functions:
                # fall back on plain V
                return self._evaluate_Vprimeprime_over_3H2Mp2_using_plain_V(
                    phi_Einstein,
                    pi_Einstein,
                    log_rhorad_Einstein,
                    fm,
                )

            msg = f"!! PotentialDerivativePolicy ({self.task_label}): log_V=nan encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        if isinf(log_V):
            if self._has_plain_V_functions:
                # fall back on plain V
                return self._evaluate_Vprimeprime_over_3H2Mp2_using_plain_V(
                    phi_Einstein,
                    pi_Einstein,
                    log_rhorad_Einstein,
                    fm,
                )

            msg = f"!! PotentialDerivativePolicy ({self.task_label}): log_V=±inf encountered | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            raise ComputationFailureError(msg)

        return self._evaluate_Vprimeprime_over_3H2Mp2_using_log_V(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm
        )

    def Hdot_over_H2_plus_3(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        Sigma,
        fm: float,
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
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm
        )

        C: float = V_over_3H2Mp2 / 2.0

        return G * A1 + C * A2
