from math import asinh, exp, fabs, hypot
from typing import Optional, List, Mapping, Sequence

import ray
from scipy.interpolate import make_interp_spline

from CosmologyConcepts import redshift_array, redshift
from CosmologyConcepts.ConformalCouplings import AbstractCoupling
from CosmologyConcepts.Potentials import AbstractPotential
from CosmologyModels import BaseCosmology
from Datastore import DatastoreObject
from MetadataConcepts import store_tag
from Units.base import UnitsLike
from config.sharding import ShardKeyType
from utilities import WallclockTimer
from .Policies import PotentialDerivativePolicy
from .ScalarModel import (
    ScalarModelProxy,
    ScalarModel,
    ScalarModelValue,
)
from .exceptions import ComputationFailureError


def conformal_mass_over_H2(
    three_MP_sq: float,
    E: float,
    Sigma: float,
    fm: float,
    d_logOmega_dphi: float,
    d2_logOmega_dphi2: float,
    Sigma_T: float,
    x: float,
) -> float:
    """
    The conformal part of M^2_eff/H^2: the phi-derivative of the source term
    (ln Omega)' (Sigma rho_R,E + rho_m,E) in V_eff', at fixed Einstein-frame scale factor
    and fixed comoving entropy, divided by H^2:

        3 M_P^2 E [ (ln Omega)'' R + (ln Omega)'^2 (Sigma^2 - Sigma_T/(1 + x) + f_m)/(1 + f_m) ]

    with R = (Sigma + f_m)/(1 + f_m), Sigma_T = d Sigma / d ln T_J = -3 d w / d ln T_J, and
    x = (1/3) d ln g_s / d ln T_J. The response of the source to delta phi is read off the
    ODE (ComputeTargets/ScalarModel.py, ODERHS): d ln rho_R,E / d ln Omega = Sigma,
    d ln rho_m,E / d ln Omega = 1, and d ln T_J / d ln Omega = -1/(1 + x) from entropy
    conservation T_J Omega a_E g_s^{1/3} = const.

    The first term is the (ln Omega)'' R term the code has always had, evaluated in the same
    way. The second, the source response, was missing before production-readiness prompt 03
    (review H5); for the exponential coupling it is the whole conformal mass, and as
    f_m -> infinity it tends to the standard beta^2 rho_m,E/(M_P^2 H^2).
    Derivation: prompts/production-readiness/logs/03-adiabatic-source-response.md, section 1.

    :param three_MP_sq: 3 M_P^2 (reduced Planck mass) in the cosmology's units
    :param E: E = G - V/(3 H^2 M_P^2), so that 3 M_P^2 H^2 E = rho_R,E (1 + f_m)
    :param Sigma: Sigma = 1 - 3 w(T_J), the kicking function the ODE uses
    :param fm: f_m = rho_m,E / rho_R,E
    :param d_logOmega_dphi: (ln Omega)'
    :param d2_logOmega_dphi2: (ln Omega)''
    :param Sigma_T: d Sigma / d ln T_J
    :param x: (1/3) d ln g_s / d ln T_J
    :return: the conformal contribution to M^2_eff/H^2
    """
    bracket: float = Sigma * Sigma - Sigma_T / (1.0 + x)

    # R = (Sigma + fm)/(1 + fm) and S = (bracket + fm)/(1 + fm), guarded against overflow
    # at large f_m in the same way as R is everywhere else
    R: float
    S: float
    if fm > 10.0:
        R = (1.0 + Sigma / fm) / (1.0 + 1.0 / fm)
        S = (1.0 + bracket / fm) / (1.0 + 1.0 / fm)
    else:
        R = (Sigma + fm) / (1.0 + fm)
        S = (bracket + fm) / (1.0 + fm)

    curvature_term: float = three_MP_sq * E * d2_logOmega_dphi2 * R
    source_response_term: float = (
        three_MP_sq * E * d_logOmega_dphi * d_logOmega_dphi * S
    )

    return curvature_term + source_response_term


def Q_numerator(
    raw_N_grid: Sequence[float],
    M2eff_over_H2_grid: Sequence[float],
    Hdot_over_H2_grid: Sequence[float],
) -> List[float]:
    """
    Q's numerator A*C at each sample, in the form that is smooth through M^2_eff = 0:

        A*C = (M^2/H^2) (1 + (1/2) d ln|M^2|/dN) = m (1 + Hdot/H^2) + (1/2) dm/dN,

    with m = M^2_eff/H^2, since d ln H^2/dN = 2 Hdot/H^2. dm/dN comes from a cubic spline
    of asinh(m) against N, dm/dN = sqrt(1 + m^2) d asinh(m)/dN. asinh is linear through
    m = 0 and logarithmic at large |m|, so the representation is accurate both where M^2_eff
    changes sign and across a bounce's dynamic range.

    Before production-readiness prompt 03, C came from a spline of log|M^2_eff|, which is
    singular where M^2_eff changes sign (and raised at an exact zero) although A*C is not.

    :param raw_N_grid: Einstein-frame e-folds of the samples, strictly increasing
    :param M2eff_over_H2_grid: m at each sample
    :param Hdot_over_H2_grid: Hdot/H^2 (Einstein frame) at each sample
    :return: A*C at each sample
    """
    asinh_m_spline = make_interp_spline(
        raw_N_grid, [asinh(m) for m in M2eff_over_H2_grid]
    )
    d_asinh_m_values = asinh_m_spline.derivative()(raw_N_grid)

    return [
        m * (1.0 + Hdot_over_H2) + 0.5 * hypot(1.0, m) * float(d_asinh_m)
        for m, Hdot_over_H2, d_asinh_m in zip(
            M2eff_over_H2_grid, Hdot_over_H2_grid, d_asinh_m_values
        )
    ]


class AdiabaticComputePolicy:
    def __init__(
        self,
        task_label: str,
        cosmology: BaseCosmology,
        potential: AbstractPotential,
        coupling: AbstractCoupling,
    ):
        self.task_label: str = task_label
        self.units: UnitsLike = cosmology.units

        self.cosmology: BaseCosmology = cosmology
        self.potential: AbstractPotential = potential
        self.coupling: AbstractCoupling = coupling

        self.V_policy: PotentialDerivativePolicy = PotentialDerivativePolicy(
            task_label, cosmology, potential
        )

        self.MP = self.units.PlanckMass
        self.CONST_MP_SQ = self.MP * self.MP
        self.CONST_3_MP_SQ = 3.0 * self.CONST_MP_SQ
        self.CONST_6_MP_SQ = 6.0 * self.CONST_MP_SQ

        self.GeV = self.units.GeV
        self.Kelvin = self.units.Kelvin

    def M2eff_over_H2(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        Sigma: float,
        fm: float,
        T_Jordan: float,
    ):
        # compute the effective mass of the chameleon field normalized to H^2
        #
        # we have M^2_eff = grad_phi V_eff' - H^2(2 + dot(H)/H^2)
        # so M^2_eff/H^2 = (grad_phi V_eff')/H^2 - (2 + dot(H)/H^2)
        #
        # to get grad_phi V'_eff, we proceed as follows.
        # We have V'_eff = V' + (d ln Omega / dphi) rho_R* (Sigma + fm)
        # where * means Einstein frame
        #
        # grad_phi of this is taken at fixed Einstein-frame scale factor and fixed comoving
        # entropy, which is how the ODE responds to delta phi (see conformal_mass_over_H2):
        # grad_phi V'_eff = V'' + (d2 ln Omega / dphi2) rho_R* (Sigma + fm)
        #                   + (d ln Omega / dphi)^2 rho_R* (Sigma^2 - Sigma_T/(1 + x) + fm)
        # Until production-readiness prompt 03 the last line, the response of the source, was
        # omitted (review H5).
        #
        # T_Jordan is exp(log_T_Jordan) of the sample, never derived from z; it supplies
        # Sigma_T = d Sigma / d ln T_J and x = (1/3) d ln g_s / d ln T_J.

        Vpp_over_3H2Mp2: float = self.V_policy.Vprimeprime_over_3H2Mp2(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm
        )
        V_over_3H2Mp2: float = self.V_policy.V_over_3H2Mp2(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm
        )
        d_logOmega_dphi: float = self.coupling.d_logOmega_dphi(phi_Einstein)
        d2_logOmega_dphi2: float = self.coupling.d2_logOmega_dphi2(phi_Einstein)

        # Sigma_T = d Sigma / d ln T_J, from the EOS's analytic derivative of the same w the
        # ODE uses; x = (1/3) d ln g_s / d ln T_J, as in the ODE's temperature law
        Sigma_T: float = -3.0 * float(self.cosmology.dw_dlogT(T_Jordan))
        x: float = (
            float(self.cosmology.dG_s_dlogT(T_Jordan))
            / float(self.cosmology.G_s(T_Jordan))
            / 3.0
        )

        G: float = 1.0 - pi_Einstein * pi_Einstein / self.CONST_6_MP_SQ
        if G < 0.0:
            msg = f"!! AdiabaticComputePolicy ({self.task_label}): negative value of G = {G:.5g} | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            print(msg)
            raise ComputationFailureError(msg)

        self_mass = self.CONST_3_MP_SQ * Vpp_over_3H2Mp2

        E: float = G - V_over_3H2Mp2
        if E < 0.0:
            # unclear whether we should treat this as a genuine computational error, or whether it just means that rho_r is very small
            # (e.g. at the end of the integration) and should harmlessly be treated as zero
            msg = f"!! ODEPolicy ({self.task_label}): negative value of E = {E:.5g} | f_m = {fm:.5g}, phi_Einstein = {phi_Einstein / self.MP:.5g} Mp, pi_Einstein = {pi_Einstein / self.MP:.5g} Mp"
            print(msg)
            # raise ComputationFailureError(msg)
            E = 0.0

        conformal_mass: float = conformal_mass_over_H2(
            self.CONST_3_MP_SQ,
            E,
            Sigma,
            fm,
            d_logOmega_dphi,
            d2_logOmega_dphi2,
            Sigma_T,
            x,
        )

        gravitational_mass: float = 1.0 - self.V_policy.Hdot_over_H2_plus_3(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, Sigma, fm
        )

        return self_mass + conformal_mass + gravitational_mass

    def Hdot_over_H2(
        self,
        phi_Einstein: float,
        pi_Einstein: float,
        log_rhorad_Einstein: float,
        Sigma: float,
        fm: float,
    ) -> float:
        # Einstein-frame Hdot/H^2 = (Hdot/H^2 + 3) - 3, the same quantity the ODE's friction
        # term and the gravitational mass use (production-readiness prompt 03)
        return (
            self.V_policy.Hdot_over_H2_plus_3(
                phi_Einstein, pi_Einstein, log_rhorad_Einstein, Sigma, fm
            )
            - 3.0
        )


@ray.remote
def compute_adiabatic_values(
    model_proxy: ScalarModelProxy, labels: Mapping[str, float], task_label: str
):
    model: ScalarModel = model_proxy.get()
    cosmology: BaseCosmology = model._cosmology
    units: UnitsLike = cosmology.units

    potential: AbstractPotential = model.potential
    coupling: AbstractCoupling = model.coupling

    CONST_MP_SQ = units.PlanckMass * units.PlanckMass
    CONST_3_MP_SQ = 3.0 * CONST_MP_SQ
    CONST_6_MP_SQ = 6.0 * CONST_MP_SQ

    abs_Q_samples: Mapping[str, List[float]] = {label: [] for label in labels}
    max_abs_Q_values: Mapping[str, Optional[float]] = {label: None for label in labels}

    z_grid: List[redshift] = []
    raw_N_grid: List[float] = []
    M2eff_over_H2_grid: List[float] = []
    Hdot_over_H2_grid: List[float] = []

    policy: AdiabaticComputePolicy = AdiabaticComputePolicy(
        task_label, cosmology, potential, coupling
    )

    with WallclockTimer() as timer:
        for value in model.values:
            value: ScalarModelValue

            raw_N_grid.append(value.raw_N)
            z_grid.append(value.z)

            phi_Einstein: float = value.phi_Einstein
            pi_Einstein: float = value.pi_Einstein
            Sigma: float = value.Sigma
            fm: float = exp(value.log_fm)
            log_rhorad_Einstein: float = value.log_rhorad_Einstein

            # the Jordan-frame temperature of the sample, never derived from z
            T_Jordan: float = exp(value.log_T_Jordan)

            M2eff_over_H2: float = policy.M2eff_over_H2(
                phi_Einstein, pi_Einstein, log_rhorad_Einstein, Sigma, fm, T_Jordan
            )
            Hdot_over_H2: float = policy.Hdot_over_H2(
                phi_Einstein, pi_Einstein, log_rhorad_Einstein, Sigma, fm
            )

            M2eff_over_H2_grid.append(M2eff_over_H2)
            Hdot_over_H2_grid.append(Hdot_over_H2)

        # A*C = (M^2/H^2)(1 + (1/2) d ln|M^2|/dN), computed in its form that is smooth where
        # M^2_eff changes sign (production-readiness prompt 03; see Q_numerator)
        AC_grid: List[float] = Q_numerator(
            raw_N_grid, M2eff_over_H2_grid, Hdot_over_H2_grid
        )

        for i, N in enumerate(raw_N_grid):
            for label, kp_over_H in labels.items():
                kp2_over_H2: float = kp_over_H * kp_over_H

                B: float = M2eff_over_H2_grid[i] + kp2_over_H2
                B2: float = pow(fabs(B), 3.0 / 2.0)

                abs_Q: float = fabs(AC_grid[i] / B2)
                abs_Q_samples[label].append(abs_Q)

                if max_abs_Q_values[label] is None or abs_Q > max_abs_Q_values[label]:
                    max_abs_Q_values[label] = abs_Q

    return {
        "z_grid": z_grid,
        "raw_N_grid": raw_N_grid,
        "abs_Q_samples": abs_Q_samples,
        "max_abs_Q_values": max_abs_Q_values,
        "compute_time": timer.elapsed,
    }


class AdiabaticHistory(DatastoreObject):
    Q_labels = {
        "kp_over_H_1E1": 1e1,
        "kp_over_H_1E2": 1e2,
        "kp_over_H_1E3": 1e3,
        "kp_over_H_1E4": 1e4,
    }

    def __init__(
        self,
        payload,
        model_proxy: ScalarModelProxy,
        label: Optional[str] = None,
        tags: Optional[List[store_tag]] = None,
    ):
        self._model_proxy: ScalarModelProxy = model_proxy
        model: ScalarModel = model_proxy.get()
        self._coupling = model.coupling
        self._potential = model.potential

        self._label: str = label
        self._tags: Optional[List[store_tag]] = tags if tags is not None else []

        if payload is None:
            DatastoreObject.__init__(self, None)

            self._values = None
            self._compute_time = None
            self._max_abs_Q_values = None

        else:
            DatastoreObject.__init__(self, payload["store_id"])

            self._values = payload["values"]
            self._compute_time = payload["compute_time"]
            self._max_abs_Q_values = payload["max_abs_Q_values"]

        self._compute_ref: Optional[ray.ObjectRef] = None

    @property
    def shard_key(self) -> ShardKeyType:
        return self._coupling.shard_key

    @property
    def label(self) -> str:
        return self._label

    @property
    def tags(self) -> List[store_tag]:
        return self._tags

    @property
    def potential(self) -> AbstractPotential:
        return self._potential

    @property
    def coupling(self) -> AbstractCoupling:
        return self._coupling

    @property
    def values(self) -> List:
        if self._values is None:
            raise RuntimeError("values has not yet been populated")
        return self._values

    def max_abs_Q(self, label: str) -> Optional[float]:
        if self._values is None:
            raise RuntimeError("values have not yet been populated")

        return self._max_abs_Q_values[label]

    @property
    def compute_time(self) -> float:
        if self._values is None:
            raise RuntimeError("values have not yet been populated")

        return self._compute_time

    def compute(self, label: Optional[str] = None) -> ray.ObjectRef:
        if self._values is not None:
            raise RuntimeError("values have already been populated")

        if label is not None:
            self._label = label

        self._compute_ref = compute_adiabatic_values.remote(
            self._model_proxy,
            AdiabaticHistory.Q_labels,
            task_label=(
                self._label
                if self._label is not None
                else f"{self._potential.name}-{self._coupling.name}"
            ),
        )
        return self._compute_ref

    def store(self) -> Optional[bool]:
        if self._compute_ref is None:
            raise RuntimeError(
                "AdiabaticHistory: store() called, but no compute() is in progress"
            )

        # check whether the computation has actually resolved
        resolved, unresolved = ray.wait([self._compute_ref], timeout=0)

        # if not, return None
        if len(resolved) == 0:
            return None

        # retrieve result and populate ourselves
        data = ray.get(self._compute_ref)
        self._compute_ref = None

        abs_Q_samples: Mapping[str, List[float]] = data["abs_Q_samples"]
        z_grid: redshift_array = data["z_grid"]
        raw_N_grid: redshift_array = data["raw_N_grid"]

        self._values = []
        for i in range(len(z_grid)):
            self._values.append(
                AdiabaticHistoryValue(
                    None,
                    z=z_grid[i],
                    raw_N=raw_N_grid[i],
                    values={
                        label: abs_Q_samples[label][i]
                        for label in AdiabaticHistory.Q_labels
                    },
                )
            )

        self._compute_time = data["compute_time"]
        self._max_abs_Q_values: Mapping[str, float] = data["max_abs_Q_values"]

        return True


class AdiabaticHistoryValue(DatastoreObject):
    def __init__(
        self, store_id: int, z: redshift, raw_N: float, values: Mapping[str, float]
    ):
        DatastoreObject.__init__(self, store_id)

        self._z: redshift = z
        self._raw_N: float = raw_N

        self._values: Mapping[str, float] = values

    @property
    def shard_key(self) -> ShardKeyType:
        return NotImplementedError

    @property
    def z(self) -> redshift:
        return self._z

    @property
    def raw_N(self) -> float:
        return self._raw_N

    @property
    def values(self) -> Mapping[str, float]:
        return self._values

    def value(self, label: str) -> float:
        return self._values[label]
