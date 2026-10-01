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
The reason a ScalarModel history failed (science-readiness prompt 02, README §2 (f), §6.3).

(b) `compute_scalar_model._function`, from main.py's initial data at beta = 2, M = 0.5, returns
a failure payload that carries the reason. The step budget is reduced to 50 by patching the
`StepControl` that `compute_scalar_model` builds, with `mock`, because the function does not
take a `StepControl`: the history then fails at the 51st accepted step. No Ray, no datastore,
no PRyMordial solve; the history stops after 50 steps, well under a second.

(c) The grouping main.py prints for the stage's failures is the pure function
`pipeline_selection.summarise_failure_reasons`; it is tested on a list of reasons.

On `HEAD~1` the payload is `{"failure": True}` and (b) fails on its `failure_reason`; (c) fails
at import because the function does not exist.
"""

import unittest
from unittest import mock

from config.defaults import DEFAULT_STRING_LENGTH
from pipeline_selection import NO_FAILURE_REASON, summarise_failure_reasons
from ComputeTargets.tests.test_kinematic_cap_loop import SM
from tools import history_and_bbn as driver


def _history(step_budget=None):
    from CosmologyConcepts import Lambda_value, M_value, beta_value, temperature
    from CosmologyConcepts.ConformalCouplings.ExponentialCoupling import (
        ExponentialCoupling,
    )
    from CosmologyConcepts.FieldValues import phi_value, pi_value
    from CosmologyConcepts.Potentials.ExponentialPotential import ExponentialPotential
    from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
    from CosmologyModels.LambdaCDM import Planck2018
    from Units import Planck_units

    units = Planck_units()
    params = Planck2018()
    cosmology = QCD_Cosmology(0, units, params)
    potential = ExponentialPotential(
        0,
        M_value(0, 0.5 * units.PlanckMass),
        Lambda_value(0, driver.LAMBDA_EV * units.eV),
        1,
        units,
    )
    coupling = ExponentialCoupling(0, beta_value(0, 2.0), units)
    args = (
        cosmology,
        temperature(0, driver.T_INIT_GEV * units.GeV),
        temperature(1, params.T_CMB_Kelvin * units.Kelvin),
        phi_value(0, driver.PHI_INIT_MP * units.PlanckMass),
        pi_value(0, driver.PI_INIT),
        driver._z_grid(),
        potential,
        coupling,
    )

    real = SM.StepControl
    with mock.patch.object(
        SM,
        "StepControl",
        lambda **kw: real(
            **kw, **({} if step_budget is None else {"step_budget": step_budget})
        ),
    ):
        return SM.compute_scalar_model._function(*args, task_label="reason-test")


class TestFailurePayload(unittest.TestCase):
    def test_b_step_budget_failure_carries_its_reason(self):
        data = _history(step_budget=50)
        self.assertTrue(data["failure"])
        self.assertTrue(
            data["failure_reason"].startswith("step budget exhausted"),
            data["failure_reason"],
        )
        self.assertLessEqual(len(data["failure_reason"]), DEFAULT_STRING_LENGTH)

    def test_b2_the_payload_truncates_to_the_column_width(self):
        payload = SM._failure_payload("x" * 300)
        self.assertTrue(payload["failure"])
        self.assertEqual(payload["failure_reason"], "x" * DEFAULT_STRING_LENGTH)


class TestSummary(unittest.TestCase):
    def test_c_grouped_by_first_clause_most_frequent_first(self):
        reasons = [
            "step budget exhausted: 2000000 accepted steps",
            "sampling: overflow when assembling sample values: x",
            "step budget exhausted: 50 accepted steps",
            None,
            "",
            "reached N_failsafe: N = 1000",
            "step budget exhausted: 7",
            "sampling: overflow when assembling sample values: y",
        ]
        self.assertEqual(
            summarise_failure_reasons(reasons),
            [
                ("step budget exhausted", 3),
                (NO_FAILURE_REASON, 2),
                ("sampling", 2),
                ("reached N_failsafe", 1),
            ],
        )

    def test_c2_empty(self):
        self.assertEqual(summarise_failure_reasons([]), [])


if __name__ == "__main__":
    unittest.main()
