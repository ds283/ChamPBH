from math import exp, sqrt, fabs
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
from .ScalarModel import ScalarModelProxy, ScalarModel, ScalarModelValue


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
    preQ_samples: List[float] = []
    dotH_over_H2_samples: List[float] = []

    with WallclockTimer() as timer:
        for value in model.values:
            value: ScalarModelValue

            raw_N_grid.append(value.raw_N)
            z_grid.append(value.z)

            phi_Einstein: float = value.phi_Einstein
            pi_Einstein: float = value.pi_Einstein

            T_Jordan: float = exp(value.log_T_Jordan)

            Sigma: float = value.Sigma
            fm: float = exp(value.log_fm)

            G: float = 1.0 - pi_Einstein * pi_Einstein / CONST_6_MP_SQ

            R: float
            if fm > 10.0:
                R = (1.0 + Sigma / fm) / (1.0 + 1.0 / fm)
            else:
                R = (Sigma + fm) / (1.0 + fm)
            A1: float = 1.0 + R / 2.0
            A2: float = 4.0 - R

            log_V: float = potential.log_V(phi_Einstein)
            d_logV_dphi: float = potential.d_logV_dphi(phi_Einstein)
            d2_logV_dphi2: float = potential.d2_logV_dphi2(phi_Einstein)

            # obtain V''/V
            Vpp_over_V: float = d2_logV_dphi2 + d_logV_dphi * d_logV_dphi
            if Vpp_over_V < 0.0:
                print(
                    f"!! compute_adiabatic_values ({task_label}): detected undressed tachyon V''/V = {Vpp_over_V:.5g} at N={value.raw_N:.8g}"
                )

            # obtain d2 ln(Omega) / dphi2
            d2_logOmega_dphi2: float = coupling.d2_logOmega_dphi2(phi_Einstein)

            log_rhorad_over_V: float = value.log_rhorad_Einstein - log_V

            d_Vpeff_dphi_over_V: float = Vpp_over_V + d2_logOmega_dphi2 * exp(
                log_rhorad_over_V
            ) * (value.Sigma + fm)

            T: float
            if log_rhorad_over_V > 2.0:
                V_over_rhorad: float = exp(-log_rhorad_over_V)
                T = V_over_rhorad / (V_over_rhorad + 1.0 + fm)
            else:
                rhorad_over_V: float = exp(log_rhorad_over_V)
                T = 1.0 / (1.0 + rhorad_over_V * (1.0 + fm))

            V_over_3H2Mp2: float = G * T
            V_over_H2: float = CONST_3_MP_SQ * V_over_3H2Mp2
            C: float = V_over_3H2Mp2 / 2.0

            dotH_over_H2: float = -3.0 + (G * A1 + C * A2)
            if dotH_over_H2 > 0.0:
                print(
                    f"!! compute_adiabatic_values ({task_label}): detected positive dotH/H^2 = {dotH_over_H2:.5g} at N={value.raw_N:.8g}"
                )
                raise RuntimeError(
                    f"compute_adiabatic_values ({task_label}): detected positive dotH/H^2 = {dotH_over_H2:.5g}"
                )

            dotH_over_H2_samples.append(dotH_over_H2)

            Q: float = d_Vpeff_dphi_over_V * V_over_H2 - 2.0 - dotH_over_H2
            # if Q < 0.0:
            #     print(
            #         f"!! compute_adiabatic_values ({task_label}): detected negative Q = {Q:.5g} at N={value.raw_N:.8g} | grad_phi(V')/V = {d_Vpeff_dphi_over_V:.5g}, grad_phi(V')/H^2 = {d_Vpeff_dphi_over_V * V_over_H2:.5g}, V''/V = {Vpp_over_V:.5g}, dotH/H^2 = {dotH_over_H2:.5g}, 2+dotH/H^2 = {2.0+dotH_over_H2:.5g}"
            #     )

            preQ_samples.append(Q)

        preQ_spline = make_interp_spline(raw_N_grid, preQ_samples)
        preQ_derivative_spline = preQ_spline.derivative()

        for i, N in enumerate(raw_N_grid):
            for label in labels:
                kp_over_H: float = labels[label]
                kp2_over_H2: float = kp_over_H * kp_over_H

                preQ: float = preQ_samples[i]
                abs_preQ: float = fabs(preQ)
                dotH_over_H2: float = dotH_over_H2_samples[i]

                abs_preQ_12: float = sqrt(fabs(preQ))

                log_preQ_derivative: float = preQ_derivative_spline(N) / preQ

                A: float = 1.0 / abs_preQ_12
                B: float = 1.0 + dotH_over_H2 + log_preQ_derivative / 2.0
                C: float = 1.0 + kp2_over_H2 / abs_preQ
                D: float = pow(C, 3.0 / 2.0)

                abs_Q: float = fabs(B / D / A)
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
