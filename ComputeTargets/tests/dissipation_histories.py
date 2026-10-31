# (c) University of Sussex 2026
# Created by David Seery
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
The histories the store's tests of inelastic-reflection prompt 09 share, each integrated once per
process and cached. Not a test module: it holds no tests.

Every history is `test_injection`'s setup (n = 1, phi* = 5 M_P, pi* = 0, T* = 2e4 GeV, to T_CMB,
the default tolerances, the model's seed realization, untuned), integrated by `integrate_history`
with the IntegrationResult captured by wrapping `integrate_scalar_history`, as
`test_injection.integrate_captured` does. The sampling axis is the golden one with the store ids
k + 1 (as `test_tuned_history_round_trip`), so that no redshift row has serial 0 when a test stores
the history.

`payload(beta, M, alpha)` is the integration's payload with the keys of `TUNING_OUTPUT_KEYS` added
(`STAND_IN_TUNING`, with the realization's own normalization and phi_ref, so that a history read back
from a store rebuilds the realized model it was computed with): what `ScalarHistory.store()` reads
from `compute_scalar_history`'s payload.

Each integration takes 2-6 s unloaded (log 09 §5.6), longer under load. No Ray, no store.
"""

import contextlib
import functools
import importlib
import io
from unittest import mock

from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value, redshift, redshift_array, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from CosmologyConcepts.redshift import log_one_plus_z_grid

SM = importlib.import_module("ComputeTargets.ScalarHistory")

# test_injection's setup
T_INIT_GEV = 2.0e4
PHI_INIT_OVER_MP = 5.0
PI_INIT = 0.0
N_EXPONENT = 1

# the keys tune_history adds, as stand-ins (normalization and phi_ref are the realization's)
STAND_IN_TUNING = dict(
    V_ref=1.0e-120,
    phi_0=0.059,
    pi_0=0.08,
    tracking=0.9,
    residual_H=1.0e-6,
    residual_phi=-1.0e-6,
    tuning_iterations=1,
    plain_steps=0,
    secant_steps=0,
    guard_steps=0,
    seed_kind="tracking",
    seed_normalization=-276.9,
    seed_phi_ref=0.061,
)


@functools.lru_cache(maxsize=None)
def cosmology():
    with contextlib.redirect_stdout(io.StringIO()):
        return gth.make_cosmology()


@functools.lru_cache(maxsize=None)
def z_grid():
    """The golden sampling axis, with store ids k + 1."""
    xs = log_one_plus_z_grid(gth.SAMPLES_PER_LOG10_Z, gth.LOG10_ONE_PLUS_Z_HIGH)
    return redshift_array(z_array=[redshift(k + 1, x) for k, x in enumerate(xs)])


def realized_model(beta: float, M_over_Mp: float):
    units = cosmology().units
    model = ExponentialModel(
        0,
        M_value(0, M_over_Mp * units.PlanckMass),
        N_EXPONENT,
        beta_value(0, beta),
        units,
    )
    seed = model.seed(cosmology())
    return model.realize(seed.normalization, seed.phi_ref)


@functools.lru_cache(maxsize=None)
def history(beta: float, M_over_Mp: float, alpha: float):
    """(data, result, realized model): integrate_history's payload, the captured
    IntegrationResult and the realized model, at test_injection's setup."""
    cos = cosmology()
    units = cos.units
    rm = realized_model(beta, M_over_Mp)

    captured = []
    integrate = SM.integrate_scalar_history

    def capturing(*args, **kwargs):
        result = integrate(*args, **kwargs)
        captured.append(result)
        return result

    with mock.patch.object(
        SM, "integrate_scalar_history", capturing
    ), contextlib.redirect_stdout(io.StringIO()):
        data = SM.integrate_history(
            cos,
            temperature(0, T_INIT_GEV * units.GeV),
            temperature(1, cos.T_CMB_Kelvin * units.Kelvin),
            phi_value(0, PHI_INIT_OVER_MP * units.PlanckMass),
            pi_value(0, PI_INIT),
            z_grid(),
            rm,
            task_label=f"dissipation-b{beta:g}-M{M_over_Mp:g}-a{alpha:g}",
            alpha=alpha,
        )
    if data.get("failure", False):
        raise RuntimeError(
            f"dissipation_histories: beta={beta}, M={M_over_Mp}, alpha={alpha} failed: "
            f"{data['failure_reason']}"
        )
    return data, captured[-1], rm


def payload(beta: float, M_over_Mp: float, alpha: float) -> dict:
    """The history's payload with the tuning's keys added, as compute_scalar_history returns it."""
    data, _, rm = history(beta, M_over_Mp, alpha)
    out = dict(data)
    out.update(STAND_IN_TUNING)
    out["normalization"] = float(rm.normalization)
    out["phi_ref"] = float(rm.phi_ref)
    return out
