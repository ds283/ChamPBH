from math import exp, fabs, log
from typing import Optional, List, Mapping

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
        # We interpret grad_phi of this to mean
        # grad_phi V'_eff = V'' + (d2 ln Omega / dphi2) rho_R* (Sigma + fm)

        Vpp_over_3H2Mp2: float = self.V_policy.Vprimeprime_over_3H2Mp2(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm
        )
        V_over_3H2Mp2: float = self.V_policy.V_over_3H2Mp2(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, fm
        )
        d2_logOmega_dphi2: float = self.coupling.d2_logOmega_dphi2(phi_Einstein)

        R: float
        if fm > 10.0:
            R = (1.0 + Sigma / fm) / (1.0 + 1.0 / fm)
        else:
            R = (Sigma + fm) / (1.0 + fm)

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

        conformal_mass: float = self.CONST_3_MP_SQ * E * d2_logOmega_dphi2 * R

        gravitational_mass: float = 1.0 - self.V_policy.Hdot_over_H2_plus_3(
            phi_Einstein, pi_Einstein, log_rhorad_Einstein, Sigma, fm
        )

        return self_mass + conformal_mass + gravitational_mass


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
    log_abs_M2eff_grid: List[float] = []

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

            M2eff_over_H2: float = policy.M2eff_over_H2(
                phi_Einstein, pi_Einstein, log_rhorad_Einstein, Sigma, fm
            )
            H2: float = value.H_Einstein * value.H_Einstein

            M2eff_over_H2_grid.append(M2eff_over_H2)
            log_abs_M2eff_grid.append(log(fabs(H2 * M2eff_over_H2)))

        log_abs_M2eff_spline = make_interp_spline(raw_N_grid, log_abs_M2eff_grid)
        d_log_abs_M2eff_spline = log_abs_M2eff_spline.derivative()

        for i, N in enumerate(raw_N_grid):
            for label, kp_over_H in labels.items():
                kp2_over_H2: float = kp_over_H * kp_over_H

                A: float = M2eff_over_H2_grid[i]
                B: float = M2eff_over_H2_grid[i] + kp2_over_H2
                B2: float = pow(fabs(B), 3.0 / 2.0)
                C: float = 1.0 + d_log_abs_M2eff_spline(N) / 2.0

                abs_Q: float = fabs(A * C / B2)
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
