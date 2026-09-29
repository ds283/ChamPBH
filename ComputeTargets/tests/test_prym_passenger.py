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
PRyMordial's inert NP-temperature equation, and why a BBN solve failed.

Written for review-remediation prompt 03 (item R2). The vendored
`PRyM/PRyM_main.py` integrated dT_NP/dt = -3H(rho_NP + p_NP)/(drho_NP/dT) for a
variable T_NP that nothing reads. It is singular wherever drho_NP/dT = 0, and an
oscillating rho_NP stalled LSODA for more than 120 s (killed; P0 in log 03).
The patch makes it return 0.

**This module runs PRyMordial: four solves, about 40 s.** No Ray cluster, no
datastore. Run it from the repository root, since PRyMordial reads `PRyMrates/`
from the working directory:

    PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .

With `PRyM/PRyM_main.py` as it was before prompt 03, test (a) does not finish;
run it under an external `timeout 120`.
"""

import time
import unittest
import warnings
from math import isfinite
from types import SimpleNamespace

from ComputeTargets.BBNData import compute_BBN_data
from ComputeTargets.tests.prym_fixtures import (
    CONSTANT,
    OSCILLATING,
    RES_D_OVER_H_E5,
    RES_YP_BBN,
    ZERO,
    run_prym,
)
from Units import Planck_units
from config.defaults import DEFAULT_STRING_LENGTH
from utilities import energy_formatter

# (a) wall-clock bound for the oscillating family (README section 6.2)
OSCILLATING_WALL_BOUND_S = 60.0

# (b) rho_NP = 0 with NP_thermo_flag True against False
INERT_RTOL = 1e-6

# (c) The constant family on the tree before the patch (47c50ae), measured by
# `python -m ComputeTargets.tests.prym_fixtures constant` (P0 in log 03). The
# user's decision of 2026-09-29 (option C): compare against the fixture's own
# unpatched values, not against README section 2 (f)'s five-figure 0.25409 /
# 2.6715. Those agree with these to their rounding. PRyMordial's output moves
# by ~1e-5 in Yp when rho_NP moves by 1e-9 (log 03), so a five-figure reference
# cannot carry a 1e-5 test.
CONSTANT_YP_UNPATCHED = 0.2540937879
CONSTANT_D_OVER_H_E5_UNPATCHED = 2.671500711
REFERENCE_RTOL = 1e-5


def _relative(a: float, b: float) -> float:
    return abs(a - b) / abs(b)


class TestPRyMordialPassenger(unittest.TestCase):
    def test_a_oscillating_case_completes(self):
        """
        (a) The oscillating family finishes within 60 s, with finite abundances.
        Runs one PRyMordial solve (about 9 s patched; over 120 s unpatched).
        """
        start = time.perf_counter()
        res = run_prym(OSCILLATING.rho, OSCILLATING.p, OSCILLATING.drho_dT)
        wall = time.perf_counter() - start

        self.assertLessEqual(wall, OSCILLATING_WALL_BOUND_S)
        for i, value in enumerate(res):
            self.assertTrue(isfinite(value), f"result {i} = {value} is not finite")
        self.assertGreater(res[RES_YP_BBN], 0.0)
        self.assertGreater(res[RES_D_OVER_H_E5], 0.0)

    def test_b_patch_is_inert(self):
        """
        (b) rho_NP = 0 with NP_thermo_flag = True reproduces NP_thermo_flag =
        False to 1e-6 in Yp and D/H, and raises no RuntimeWarning (unpatched, it
        raised 916 from the 0/0 in dTNPdt). Runs two PRyMordial solves.
        """
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with_np = run_prym(ZERO.rho, ZERO.p, ZERO.drho_dT, NP_thermo_flag=True)

        runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
        self.assertEqual(
            len(runtime_warnings),
            0,
            f"RuntimeWarnings: {[str(w.message) for w in runtime_warnings[:5]]}",
        )

        without_np = run_prym(ZERO.rho, ZERO.p, ZERO.drho_dT, NP_thermo_flag=False)

        for index, name in ((RES_YP_BBN, "Yp"), (RES_D_OVER_H_E5, "D/H")):
            with self.subTest(name):
                self.assertLessEqual(
                    _relative(with_np[index], without_np[index]), INERT_RTOL
                )

    def test_c_reference_abundances_unchanged(self):
        """
        (c) rho_NP = 0.08 rho_SM gives the Yp and D/H it gave before the patch,
        to 1e-5 relative. Runs one PRyMordial solve.
        """
        res = run_prym(CONSTANT.rho, CONSTANT.p, CONSTANT.drho_dT)

        with self.subTest("Yp"):
            self.assertLessEqual(
                _relative(res[RES_YP_BBN], CONSTANT_YP_UNPATCHED), REFERENCE_RTOL
            )
        with self.subTest("D/H"):
            self.assertLessEqual(
                _relative(res[RES_D_OVER_H_E5], CONSTANT_D_OVER_H_E5_UNPATCHED),
                REFERENCE_RTOL,
            )

    def test_d_failure_carries_a_reason(self):
        """
        (d) compute_BBN_data's pre-check failure returns a failure_reason naming
        both temperatures. The body is reached through `._function` with a
        stand-in model whose T_Jordan_stop (1 eV) is above 0.1 * T_BBN_spline_min
        (0.01 eV); nothing past the pre-check is touched. No PRyMordial solve.
        """
        units = Planck_units()
        T_stop = SimpleNamespace(as_float=1.0 * units.eV)
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
        T_spline_min = 1e-4 * units.keV  # compute_BBN_data's default
        self.assertIn(formatter(T_stop), reason)
        self.assertIn(formatter(0.1 * T_spline_min), reason)


if __name__ == "__main__":
    unittest.main()
