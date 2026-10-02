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
The fixed-temperature values of a history (science-readiness prompt 06b, README §2 (n), §6.7b).

`T_Jordan_crossing(result, log_T)` returns the N of the history's first crossing of
ln T_J = log_T on the dense output, or None; `fixed_T_values(result, policy, coupling, units)`
reads phi and rho_NP/rho_R,J there, at T_J = 1 MeV and 70 keV, through `fixed_T_value_at`.

(a) runs one full history through `compute_scalar_model._function`, capturing its
`IntegrationResult`, and ties the dense-output values to the stored samples and to
`compute_BBN_data`'s per-sample ratio. (b) and (c) drive `integrate_scalar_history` from the
integrator audit's P1 state with the helpers of `test_kinematic_cap_loop`. No Ray cluster, no
datastore, no PRyMordial solve. The round trip through a store is in
`Datastore/tests/test_fixed_T_values_round_trip.py`.

On `HEAD~1` there is no `T_Jordan_crossing`, `fixed_T_values` or `fixed_T_value_at`, and
`compute_scalar_model` returns no "fixed_T_values", so every test here fails.
"""

import contextlib
import io
import unittest
from math import exp, log
from unittest import mock

import numpy as np

import ComputeTargets.tests.test_kinematic_cap_loop as kcl
from CosmologyConcepts import (
    Lambda_value,
    M_value,
    beta_value,
    redshift,
    redshift_array,
    temperature,
)
from CosmologyConcepts.ConformalCouplings.ExponentialCoupling import (
    ExponentialCoupling,
)
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Potentials.ExponentialPotential import ExponentialPotential

SM = kcl.SM
_units = kcl._units


def rel(a: float, b: float) -> float:
    return abs(a - b) / abs(b)


def bbn_ratio(sample) -> float:
    """
    compute_BBN_data's per-sample rho_NP/rho_R,J, written out from ComputeTargets/BBNData.py
    lines 455-465 (on 336630a), not imported:

        rhorad_Jordan = exp(value.log_rhorad_Jordan)
        H2_Jordan = value.H_Jordan * value.H_Jordan
        fm = exp(value.log_fm)
        LHS = H2_Jordan * CONST_3_MP_SQ
        density_NP = LHS - rhorad_Jordan * (1.0 + fm)
        density_NP / rhorad_Jordan
    """
    CONST_3_MP_SQ = 3.0 * _units.PlanckMass * _units.PlanckMass
    rhorad_Jordan = exp(sample.log_rhorad_Jordan)
    H2_Jordan = sample.H_Jordan * sample.H_Jordan
    fm = exp(sample.log_fm)
    LHS = H2_Jordan * CONST_3_MP_SQ
    density_NP = LHS - rhorad_Jordan * (1.0 + fm)
    return density_NP / rhorad_Jordan


def full_history(beta: float, M: float):
    """
    compute_scalar_model._function with main.py's initial data (phi* = 5 M_P, pi* = 0,
    T* = 2e4 GeV, T_stop = T_CMB, 250 samples per decade of 1 + z to 1e35), as
    tools/history_and_bbn.py runs it. Returns (data, result, policy, coupling), where result
    is the IntegrationResult, captured by wrapping integrate_scalar_history for the call.
    """
    potential = ExponentialPotential(
        0,
        M_value(0, M * _units.PlanckMass),
        Lambda_value(0, 1e-3 * _units.eV),
        1,
        _units,
    )
    coupling = ExponentialCoupling(0, beta_value(0, beta), _units)

    zs = np.logspace(0.0, 35.0, int(round(250 * 35.0 + 0.5, 0))) - 1.0
    z_grid = redshift_array(z_array=[redshift(i, float(z)) for i, z in enumerate(zs)])

    captured = []
    integrate = SM.integrate_scalar_history

    def capturing(*args, **kwargs):
        result = integrate(*args, **kwargs)
        captured.append(result)
        return result

    with mock.patch.object(
        SM, "integrate_scalar_history", capturing
    ), contextlib.redirect_stdout(io.StringIO()):
        data = SM.compute_scalar_model._function(
            kcl._cosmology,
            kcl._T_init,
            kcl._T_stop,
            phi_value(0, 5.0 * _units.PlanckMass),
            pi_value(0, 0.0),
            z_grid,
            potential,
            coupling,
            task_label="test_fixed_T_values",
        )

    policy = SM.ODEPolicy("test", kcl._cosmology, potential, coupling)
    return data, captured[-1], policy, coupling


class TestFixedTValues(unittest.TestCase):
    def test_a_agrees_with_the_stored_samples(self):
        """
        (a) The full history beta = 2, M = 0.5 (about 2 s): at three stored samples with
        0.07 MeV < T_J < 1 MeV the crossing is the sample's raw_N, phi the sample's, and the
        ratio compute_BBN_data's on the sample.
        """
        data, result, policy, coupling = full_history(2.0, 0.5)
        self.assertFalse(data.get("failure", False))

        MeV = _units.MeV
        window = [
            s for s in data["sample"] if 0.07 * MeV < exp(s.log_T_Jordan) < 1.0 * MeV
        ]
        self.assertGreater(len(window), 10)
        chosen = [window[0], window[len(window) // 2], window[-1]]

        for s in chosen:
            N = SM.T_Jordan_crossing(result, s.log_T_Jordan)
            self.assertIsNotNone(N)
            self.assertLessEqual(abs(N - s.raw_N), 1e-10)

            k, N_k = SM._T_Jordan_crossing_step(result, s.log_T_Jordan)
            self.assertEqual(N_k, N)
            phi, ratio = SM.fixed_T_value_at(result, k, N, policy, coupling, _units)
            self.assertLessEqual(rel(phi, s.phi_Einstein), 1e-9)
            self.assertLessEqual(rel(ratio, bbn_ratio(s)), 1e-8)

        # compute_scalar_model returns fixed_T_values on the same result, all four reached
        fixed_T = data["fixed_T_values"]
        self.assertIsInstance(fixed_T, SM.FixedTValues)
        self.assertEqual(fixed_T, SM.fixed_T_values(result, policy, coupling, _units))
        for value in fixed_T:
            self.assertIsInstance(value, float)

        # and they are fixed_T_value_at at the crossings of the two module constants
        for T_MeV, phi, ratio in (
            (
                SM.FIXED_T_JORDAN_HIGH_MEV,
                fixed_T.phi_Einstein_1MeV,
                fixed_T.density_NP_ratio_1MeV,
            ),
            (
                SM.FIXED_T_JORDAN_LOW_MEV,
                fixed_T.phi_Einstein_70keV,
                fixed_T.density_NP_ratio_70keV,
            ),
        ):
            k, N = SM._T_Jordan_crossing_step(result, log(T_MeV * MeV))
            self.assertLessEqual(
                abs(result.solution.interpolants[k](N)[4] - log(T_MeV * MeV)), 1e-12
            )
            self.assertEqual(
                (phi, ratio),
                SM.fixed_T_value_at(result, k, N, policy, coupling, _units),
            )

    def test_b_not_reached(self):
        """
        (b) The P1 window (beta = 2, M = 0.5) to N = 21 ends near T_J = 0.4 GeV, above both
        temperatures: every field is None. A target above the window's first T_J or below its
        last gives None. About 0.2 s.
        """
        result, _ = kcl.integrate(kcl.P1, 0.5, 21.0)
        policy, _, _ = kcl.build(2.0, 0.5)

        log_T_first = result.solution.interpolants[0](result.solution.ts[0])[4]
        log_T_last = result.solution.interpolants[-1](result.solution.ts[-1])[4]
        self.assertGreater(log_T_last, log(1.0 * _units.MeV))

        self.assertIsNone(SM.T_Jordan_crossing(result, log_T_first + 0.1))
        self.assertIsNone(SM.T_Jordan_crossing(result, log_T_last - 0.1))
        self.assertIsNotNone(
            SM.T_Jordan_crossing(result, 0.5 * (log_T_first + log_T_last))
        )

        fixed_T = SM.fixed_T_values(result, policy, policy.coupling, _units)
        self.assertEqual(fixed_T, SM.FixedTValues(None, None, None, None))

    def test_c_crossing_across_a_reflection(self):
        """
        (c) P1 at M = 1e-10, one floor reflection, to N = 21; about 0.2 s. A target on the step
        that ends at the reflection is found on that step, and ln T_J is continuous across the
        reflection to 1e-12.
        """
        result, _ = kcl.integrate(kcl.P1, 1e-10, 21.0)
        self.assertEqual(len(result.reflections), 1)
        N_r = result.reflections[0].N

        ts = list(result.solution.ts)
        interpolants = result.solution.interpolants
        k = ts.index(N_r) - 1
        self.assertGreaterEqual(k, 0)

        before, after = interpolants[k](N_r), interpolants[k + 1](N_r)
        # the reflection flips pi and leaves ln T_J where it was
        self.assertLess(before[1], 0.0)
        self.assertGreater(after[1], 0.0)
        self.assertLessEqual(abs(before[4] - after[4]), 1e-12)

        # a target inside the step that ends at the reflection is found on that step
        log_T_low_end = interpolants[k](ts[k])[4]
        target = 0.5 * (log_T_low_end + before[4])
        k_found, N = SM._T_Jordan_crossing_step(result, target)
        self.assertEqual(k_found, k)
        self.assertLess(ts[k], N)
        self.assertLess(N, N_r)
        self.assertEqual(SM.T_Jordan_crossing(result, target), N)
        self.assertLessEqual(abs(interpolants[k](N)[4] - target), 1e-12)

        # the reflection's own ln T_J is reached at the reflection, on that step
        k_found, N = SM._T_Jordan_crossing_step(result, before[4])
        self.assertEqual(k_found, k)
        self.assertLessEqual(abs(N - N_r), 1e-12)

        # a target inside the step that starts at the reflection is found on that one
        target = 0.5 * (after[4] + interpolants[k + 1](ts[k + 2])[4])
        k_found, N = SM._T_Jordan_crossing_step(result, target)
        self.assertEqual(k_found, k + 1)
        self.assertLess(N_r, N)

        # and the values there are read from the post-reflection state
        policy, _, _ = kcl.build(2.0, 1e-10)
        phi, ratio = SM.fixed_T_value_at(
            result, k_found, N, policy, policy.coupling, _units
        )
        self.assertEqual(phi, float(interpolants[k + 1](N)[0]))
        self.assertTrue(np.isfinite(ratio))


if __name__ == "__main__":
    unittest.main()
