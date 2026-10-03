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
PRyMordial as a passenger of the scalar field, through the Hubble-only route,
and why a BBN solve failed.

Written for review-remediation prompt 03 (item R2), when the vendored
`PRyM/PRyM_main.py` integrated an unread NP temperature whose equation was
singular wherever d rho_NP / dT = 0; that prompt made the equation inert.
Science-readiness prompt 01 replaced the route: rho_NP now reaches PRyMordial
through the expansion rate alone (`NP_hubble_flag`), `NP_thermo_flag` is off,
and the inert equation was reverted to upstream, since nothing calls it. The
tests keep their purposes on the new route:

- (a) the oscillating family still completes;
- (b) rho_NP = 0 through the route is PRyMordial with no new physics at all,
  now identically (it replaces "the patch is inert", which compared
  NP_thermo_flag on and off with rho_NP = 0);
- (c) the constant family reproduces a pinned reference, now the Hubble-only
  reference of the science-readiness README section 6.1 (it replaces the
  comparison against the review-remediation route's own pinned values);
- (d) a failure carries a reason (unchanged).

**This module runs PRyMordial: four solves, about 30 s.** No Ray cluster, no
datastore. Run it from the repository root, since PRyMordial reads `PRyMrates/`
from the working directory:

    PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .
"""

import time
import unittest
import warnings
from math import isfinite
from types import SimpleNamespace

from ComputeTargets.BBNData import compute_BBN_data, compute_SM_baseline
from ComputeTargets.tests.prym_fixtures import (
    CONSTANT,
    OSCILLATING,
    RES_D_OVER_H_E5,
    RES_HE3_OVER_H_E5,
    RES_LI7_OVER_H_E10,
    RES_YP_BBN,
    SavedPRyMGlobals,
    run_prym,
    run_prym_without_new_physics,
)
from Units import Planck_units
from config.defaults import DEFAULT_STRING_LENGTH
from utilities import energy_formatter

# (a) wall-clock bound for the oscillating family (review-remediation README section 6.2)
OSCILLATING_WALL_BOUND_S = 60.0

# (c) The Hubble-only reference for the constant family rho_NP = 0.08 rho_SM,
# small network: science-readiness README section 6.1, row `const-honly`,
# measured by `prompts/science-readiness/planning-probes/honly_constant_reference.py
# const-honly` on 6aaa706, and re-measured to every digit on 7b518c9 by
# science-readiness prompt 01 (log 01). It is the route before prompt 01 with
# the pressure set to -rho_NP and the density derivative to 0, so that only H
# saw the new physics: what the patched route must reproduce, up to the third
# (inert) LSODA component it no longer carries.
# Re-pinned 2026-10-03 by bbn-tolerance prompt 02, which gives the small low-T
# call rtol 1e-6 (it passed none, so SciPy's 1e-3): the same provenance
# re-derived on 7b518c9 with that call at rtol 1e-6 by
# prompts/bbn-tolerance/logs/01-probes/pinned_reference_7b518c9.py
# const-honly-small --rtol 1e-6 (bbn-tolerance log 01c, row 7; log 02). The
# values before, at the default rtol (pinned on 7b518c9, unchanged through
# 2bc124b): Yp 0.2536690816, D/H x1e5 2.6481673. The bound is unchanged.
CONST_HONLY_SMALL_YP = 0.253669508
CONST_HONLY_SMALL_D_OVER_H_E5 = 2.649288446
CONST_HONLY_RTOL = 1e-6

# The same reference with the full network, for test_network_flag (b) and
# test_bbn_callbacks (h): measured on 7b518c9 by science-readiness prompt 01
# with the planning probe's const-honly case and small_network=False (log 01,
# Verification).
# Re-pinned 2026-10-03 by bbn-tolerance prompt 02, which gives the full low-T
# call rtol 1e-5 (it passed none, so SciPy's 1e-3): the same provenance
# re-derived on 7b518c9 with that call at rtol 1e-5 by
# prompts/bbn-tolerance/logs/01-probes/pinned_reference_7b518c9.py
# const-honly-full --rtol 1e-5 (bbn-tolerance log 01, Verification item 11;
# log 02). The values before, at the default rtol (pinned on 7b518c9,
# unchanged through 2bc124b): Yp 0.2536754614, D/H x1e5 2.648809882. The bound
# is unchanged.
CONST_HONLY_FULL_YP = 0.2536731562
CONST_HONLY_FULL_D_OVER_H_E5 = 2.649990509
REFERENCE_RTOL = 1e-5


def _relative(a: float, b: float) -> float:
    return abs(a - b) / abs(b)


class TestPRyMordialPassenger(unittest.TestCase):
    def test_a_oscillating_case_completes(self):
        """
        (a) The oscillating family finishes within 60 s through the Hubble-only
        route, with finite abundances. Runs one PRyMordial solve (about 9 s).
        """
        start = time.perf_counter()
        res = run_prym(OSCILLATING.rho)
        wall = time.perf_counter() - start

        self.assertLessEqual(wall, OSCILLATING_WALL_BOUND_S)
        for i, value in enumerate(res):
            self.assertTrue(isfinite(value), f"result {i} = {value} is not finite")
        self.assertGreater(res[RES_YP_BBN], 0.0)
        self.assertGreater(res[RES_D_OVER_H_E5], 0.0)

    def test_b_zero_is_plain_prymordial(self):
        """
        (b) compute_SM_baseline(small_network=True), which is rho_NP = 0 through
        the Hubble-only route, gives every abundance identical (==) to
        PRyMordial with every new-physics flag off and no callback, and raises
        no RuntimeWarning. Runs two PRyMordial solves (small network).
        """
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with SavedPRyMGlobals():
                baseline = compute_SM_baseline(small_network=True)

        runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
        self.assertEqual(
            len(runtime_warnings),
            0,
            f"RuntimeWarnings: {[str(w.message) for w in runtime_warnings[:5]]}",
        )

        plain = run_prym_without_new_physics(small_network=True)

        print(
            "\n[test_prym_passenger (b)] baseline "
            + ", ".join(
                f"{k} {baseline[k]!r}"
                for k in ("Yp_BBN", "DOverH", "He3OverH", "Li7OverH")
            )
        )
        for key, index in (
            ("Yp_BBN", RES_YP_BBN),
            ("DOverH", RES_D_OVER_H_E5),
            ("He3OverH", RES_HE3_OVER_H_E5),
            ("Li7OverH", RES_LI7_OVER_H_E10),
        ):
            with self.subTest(key):
                self.assertEqual(baseline[key], plain[index])

    def test_c_constant_family_reproduces_the_hubble_only_reference(self):
        """
        (c) rho_NP = 0.08 rho_SM through the patched route, small network, with
        wall_clock_limit=None, gives the README section 6.1 const-honly Yp and
        D/H to 1e-6 relative. Runs one PRyMordial solve.
        """
        res = run_prym(CONSTANT.rho, small_network=True, wall_clock_limit=None)

        dYp = _relative(res[RES_YP_BBN], CONST_HONLY_SMALL_YP)
        dDoH = _relative(res[RES_D_OVER_H_E5], CONST_HONLY_SMALL_D_OVER_H_E5)
        print(
            f"\n[test_prym_passenger (c)] Yp {res[RES_YP_BBN]:.10g} ({dYp:.2e}), "
            f"D/H x1e5 {res[RES_D_OVER_H_E5]:.10g} ({dDoH:.2e}) against const-honly"
        )
        with self.subTest("Yp"):
            self.assertLessEqual(dYp, CONST_HONLY_RTOL)
        with self.subTest("D/H"):
            self.assertLessEqual(dDoH, CONST_HONLY_RTOL)

    def test_d_failure_carries_a_reason(self):
        """
        (d) compute_BBN_data's pre-check failure returns a failure_reason naming
        both temperatures. The body is reached through `._function` with a
        stand-in model whose T_Jordan_stop (100 eV) is above 0.1 * T_BBN_spline_min
        (20 eV); nothing past the pre-check is touched. No PRyMordial solve.
        Unchanged by science-readiness prompt 01; prompt 06 moved the default
        floor from 1e-4 keV to 0.2 keV, and the stop from 1 eV to 100 eV with it.
        """
        units = Planck_units()
        T_stop = SimpleNamespace(as_float=100.0 * units.eV)
        model = SimpleNamespace(
            _cosmology=SimpleNamespace(units=units),
            potential=None,
            coupling=None,
            T_Jordan_stop=T_stop,
        )
        proxy = SimpleNamespace(get=lambda: model)

        result = compute_BBN_data._function(proxy, task_label="prompt-03-test")

        self.assertTrue(result["failure"])
        reason = result["failure_reason"]
        self.assertIsInstance(reason, str)
        self.assertLessEqual(len(reason), DEFAULT_STRING_LENGTH)

        formatter = energy_formatter(units)
        T_spline_min = 0.2 * units.keV  # compute_BBN_data's default
        self.assertIn(formatter(T_stop), reason)
        self.assertIn(formatter(0.1 * T_spline_min), reason)


if __name__ == "__main__":
    unittest.main()
