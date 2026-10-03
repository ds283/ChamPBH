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
PRyMordial's low-temperature nuclear network runs at the tolerances ruled for
it, and every other stage at the tolerances it had.

Written for bbn-tolerance prompt 02 (README section 6.2, row 1; ruling U4).
Upstream PRyMordial passes the low-T `solve_ivp` calls an `atol` and no `rtol`,
so SciPy's default 1e-3 applied, and the network's output scattered by ~1e-3
in D/H under ulp-level changes to its input (bbn-tolerance logs 01 and 01c).
The vendored copy now passes `rtol` 1e-6 to the small network's call and 1e-5
to the full network's, with `atol` unchanged. The test fails on the tree before
that prompt, where the low-T calls carry no `rtol`.

The calls are recorded, not changed, by `tools/bbn_from_store.py`'s
`stage_tolerance_override` with no settings; it recognises each call by the
stage name PRyMordial hands `_limited` (bbn-tolerance log 01, deviation 3).

**This module runs PRyMordial twice: one small-network and one full-network
SM baseline, about 20 s.** No Ray cluster, no datastore. Run from the
repository root, since PRyMordial reads `PRyMrates/` from the working
directory:

    PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .
"""

import unittest

from ComputeTargets.BBNData import compute_SM_baseline
from ComputeTargets.tests.prym_fixtures import SavedPRyMGlobals

# the low-T calls, as ruled (bbn-tolerance README section 0.2 U4)
LOWT_SMALL_RTOL, LOWT_SMALL_ATOL = 1e-6, 1e-11
LOWT_FULL_RTOL, LOWT_FULL_ATOL = 1e-5, 1e-15

# every other Python-branch call, unchanged by bbn-tolerance prompt 02
OTHER_RTOL, OTHER_ATOL = 1e-6, 1e-9


def _tool():
    import tools.bbn_from_store as tool

    return tool


def _recorded_calls(small_network: bool):
    """Every solve_ivp call of one SM-baseline solve, recorded and passed through
    unchanged."""
    tool = _tool()
    with SavedPRyMGlobals():
        with tool.stage_tolerance_override({}) as calls:
            compute_SM_baseline(small_network)
    return list(calls)


class TestLowTTolerance(unittest.TestCase):
    def _check(self, small_network, low_t_stage, rtol, atol):
        calls = _recorded_calls(small_network)
        stages = [c.stage for c in calls]
        self.assertNotIn(None, stages, "a solve_ivp call announced no stage")
        self.assertEqual(stages.count(low_t_stage), 1, stages)
        for c in calls:
            with self.subTest(network=small_network, stage=c.stage):
                self.assertTrue(c.success, "the solve did not succeed")
                if c.stage == low_t_stage:
                    self.assertIn("rtol", c.kwargs, "the low-T call passes no rtol")
                    self.assertEqual(c.kwargs["rtol"], rtol)
                    self.assertEqual(c.kwargs["atol"], atol)
                else:
                    self.assertEqual(c.kwargs["rtol"], OTHER_RTOL)
                    self.assertEqual(c.kwargs["atol"], OTHER_ATOL)

    def test_a_low_T_calls_receive_the_ruled_tolerances(self):
        """(a) An SM-baseline solve on each network with every solve_ivp call
        recorded: the small network's low-T call carries rtol 1e-6 and atol
        1e-11, the full network's rtol 1e-5 and atol 1e-15, and every other call
        rtol 1e-6 and atol 1e-9. Fails before bbn-tolerance prompt 02, where the
        low-T calls carry no rtol.
        **Runs two PRyMordial solves (small, then full), about 20 s.**"""
        tool = _tool()
        self._check(True, tool.STAGE_LOW_T_SMALL, LOWT_SMALL_RTOL, LOWT_SMALL_ATOL)
        self._check(False, tool.STAGE_LOW_T_FULL, LOWT_FULL_RTOL, LOWT_FULL_ATOL)


if __name__ == "__main__":
    unittest.main()
