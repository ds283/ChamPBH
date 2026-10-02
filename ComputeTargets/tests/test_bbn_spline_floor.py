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
The BBN spline floor, science-readiness prompt 06 (README section 6.7).
`compute_BBN_data`'s `T_BBN_keV_spline_min` defaults to 0.2 keV, below
PRyMordial's lowest query of the callback (0.363 keV, measured); the pre-check
`T_Jordan_stop > 0.1 * T_BBN_spline_min` is unchanged, so a history must reach
20 eV. Before the prompt the floor was 1e-4 keV and a history had to reach
0.01 eV.

Test (a) runs no solve: `_run_PRyMordial` is replaced by a recorder. **Test (b)
runs one small-network PRyMordial solve, about 20 s.** Run from the repository
root:

    PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .
"""

import unittest
from math import exp, log, pi, sqrt
from types import SimpleNamespace
from unittest import mock

import sys

from ComputeTargets.BBNData import compute_BBN_data
from ComputeTargets.tests.prym_fixtures import run_prym
from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import (
    SaikawaShirai_EOS_spline,
)
from Units import Planck_units

FLOOR_KEV = 0.2
PRYMORDIAL_LOWEST_QUERY_KEV = 0.363

# (a) history samples from 1 GeV down to 1e-9 GeV = 1 eV in T_Jordan
N_SAMPLES = 90
LOG10_T_MEV_HI = 3.0
LOG10_T_MEV_LO = -6.0

# (a) stops: 1e-8 GeV = 10 eV passes the 20 eV pre-check, 1e-7 GeV = 100 eV fails
T_STOP_PASSES_GEV = 1e-8
T_STOP_FAILS_GEV = 1e-7

SENTINEL = {"failure": True, "failure_reason": "recorded (test_bbn_spline_floor)"}


def _stand_in(t_stop_GeV: float):
    units = Planck_units()
    eos = SaikawaShirai_EOS_spline(units)
    cosmology = SimpleNamespace(units=units, G_rho=eos.G_rho)
    M_P2 = units.PlanckMass * units.PlanckMass

    values = []
    for i in range(N_SAMPLES):
        lt = LOG10_T_MEV_HI + (LOG10_T_MEV_LO - LOG10_T_MEV_HI) * i / (N_SAMPLES - 1)
        T = 10.0**lt * units.MeV
        rho_R = (pi * pi / 30.0) * 10.0 * T**4
        f_m = 1e-3
        H_J = sqrt(rho_R * (1.0 + f_m + 0.05) / (3.0 * M_P2))
        values.append(
            SimpleNamespace(
                z=SimpleNamespace(store_id=i),
                raw_N=0.1 * i,
                log_T_Jordan=log(T),
                log_rhorad_Jordan=log(rho_R),
                log_fm=log(f_m),
                H_Jordan=H_J,
            )
        )
    model = SimpleNamespace(
        _cosmology=cosmology,
        potential=None,
        coupling=None,
        T_Jordan_stop=SimpleNamespace(as_float=t_stop_GeV * 1e3 * units.MeV),
        values=values,
    )
    return SimpleNamespace(get=lambda: model)


class TestBBNSplineFloor(unittest.TestCase):
    def test_a_precheck_arithmetic(self):
        """(a) No solve. The default floor is 0.2 keV. A stand-in model with
        T_Jordan_stop = 1e-8 GeV passes the pre-check (`_run_PRyMordial` is
        reached) and one at 1e-7 GeV returns the pre-check failure payload,
        which names both temperatures. On HEAD~1 the first fails the
        pre-check, since the old floor needed 0.01 eV."""
        import inspect

        # ComputeTargets.BBNData the package attribute is the BBNData class
        BBNData = sys.modules["ComputeTargets.BBNData"]

        with mock.patch.object(
            BBNData, "_run_PRyMordial", return_value=SENTINEL
        ) as run:
            result = compute_BBN_data._function(
                _stand_in(T_STOP_PASSES_GEV), task_label="test-a-pass"
            )
        self.assertEqual(run.call_count, 1, result)
        self.assertEqual(result, SENTINEL)

        with mock.patch.object(
            BBNData, "_run_PRyMordial", return_value=SENTINEL
        ) as run:
            result = compute_BBN_data._function(
                _stand_in(T_STOP_FAILS_GEV), task_label="test-a-fail"
            )
        self.assertEqual(run.call_count, 0)
        self.assertTrue(result["failure"])
        reason = result["failure_reason"]
        self.assertTrue(reason.startswith("pre-check: T_Jordan_stop="), reason)
        self.assertIn("0.1*T_BBN_spline_min=", reason)

        default = (
            inspect.signature(compute_BBN_data._function)
            .parameters["T_BBN_keV_spline_min"]
            .default
        )
        self.assertEqual(default, FLOOR_KEV)

    def test_b_prymordial_never_queries_below_the_floor(self):
        """(b) The lowest positive T at which PRyMordial calls the callback
        (small network, callback identically zero, the flags compute_BBN_data
        sets) is above the 0.2 keV floor. **Runs one PRyMordial solve, about
        20 s.** Re-runs the measurement of planning-probes/prym_callback_domain.py
        (0.3628 keV on 6aaa706)."""
        Ts = []

        def rec(T):
            Ts.append(T)
            return 0.0

        run_prym(rec, small_network=True)
        positive = [T for T in Ts if T > 0]
        self.assertGreater(len(positive), 100)
        lowest_keV = min(positive) * 1e3
        print(
            f"[test_bbn_spline_floor (b)] calls={len(Ts)} lowest positive T = "
            f"{lowest_keV:.4g} keV (floor {FLOOR_KEV} keV)"
        )
        self.assertGreater(lowest_keV, FLOOR_KEV)


if __name__ == "__main__":
    unittest.main()
