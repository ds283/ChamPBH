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
The inventory names alpha (inelastic-reflection prompt 10, test 9, P38), on temporary stores built
with the harness of `test_inventory_report` (real rows through the factories' inserts, in the
stand-in cluster, no Ray):

- `alpha_value` is a class of "Redshifts, tolerances and parameter values", after `beta_value`;
- a store whose histories share one alpha prints it once, on the common line (`alpha=0`), and not
  on each history line;
- a store with two alpha prints `alpha=` on each history line, and not on the common line.

Since one-loop-drain prompt 03 an `alpha_value` row is the pair (value, drain_model). One-loop-drain prompt 04 (P39)
labels each row by its model: a `fixed` row reads as before 03 (`values = [ 0, 0.1 ]` and `alpha=0`), which these
tests pin again, and a row of another model reads `A=<A:.5g> (<drain_model>)` in the values list and on a history's
line (`TestTheInventoryNamesTheModel`).

No Ray, no PRyMordial. About 1 s.
"""

import unittest

import CosmologyConcepts.Models.model_ids as model_ids
import CosmologyModels.model_ids as cosmology_ids
from config.datastore import factories
from datastorekit.store_inventory import read_inventory
from tools.inventory_report import (
    HISTORY_EXTRA_FIELDS,
    INVENTORY_CATEGORIES,
    format_inventory_report,
)
from tools.tests.test_inventory_report import (
    LABEL,
    _InventoryStoreCase,
    _report_lines,
)


class _AlphaStoreCase(_InventoryStoreCase):
    def build(self, alphas):
        """A store with one successful ScalarHistory for each alpha of `alphas`."""
        store = self.open(LABEL)

        def ins(cls_name, row):
            return self.insert(store, cls_name, row)

        atol = ins("tolerance", {"log10_tol": -10.0})
        rtol = ins("tolerance", {"log10_tol": -8.0})
        tuning_tol = ins("tolerance", {"log10_tol": -5.0})
        beta = ins("beta_value", {"value": 25.0})
        M = ins("M_value", {"value_eV": 2.436e24})
        T_init = ins("temperature", {"value_GeV": 2.0e4})
        phi = ins("phi_value", {"value_PlanckMass": 5.0})
        pi = ins("pi_value", {"value_PlanckMass": 0.0})
        cosmology = ins(
            "QCD_Cosmology",
            dict(
                name="Planck2018",
                omega_m=0.3111,
                omega_cc=0.6889,
                h=0.6766,
                f_baryon=0.1575,
                T_CMB_Kelvin=2.7255,
                Neff=3.046,
            ),
        )
        solver = ins("IntegrationSolver", {"label": "solve_ivp+RK45", "stepping": 0})
        model = ins("ExponentialModel", dict(M_serial=M, n_value=1, beta_serial=beta))
        for k, value in enumerate(alphas):
            # a bare value is a row of the fixed model; a (value, drain_model) pair is that model's
            value, drain_model = value if isinstance(value, tuple) else (value, "fixed")
            alpha = ins("alpha_value", {"value": value, "drain_model": drain_model})
            ins(
                "ScalarHistory",
                dict(
                    cosmology_type=cosmology_ids.XAV_IMPROVED_EOS_IDENTIFIER,
                    cosmology_serial=cosmology,
                    model_type=model_ids.EXPONENTIAL_MODEL,
                    atol_serial=atol,
                    rtol_serial=rtol,
                    tuning_tol_serial=tuning_tol,
                    alpha_serial=alpha,
                    phi_Einstein_init_serial=phi,
                    pi_Einstein_init_serial=pi,
                    T_Jordan_init_serial=T_init,
                    validated=True,
                    label=f"job-a-ScalarHistory-alpha-{k}",
                    model_serial=model,
                    failure=False,
                    solver_serial=solver,
                    z_samples=0,
                ),
            )
        self.close_actors()
        return self.db

    def report(self, alphas):
        inventory = read_inventory(self.build(alphas), factories)
        self.assertEqual(inventory.problems, ())
        text = format_inventory_report(inventory, "store.db")
        return text, _report_lines(text)


class TestTheInventoryNamesAlpha(_AlphaStoreCase):
    def test_9_alpha_value_is_a_parameter_class_after_beta_value(self):
        names = dict(INVENTORY_CATEGORIES)["Redshifts, tolerances and parameter values"]
        self.assertEqual(names.index("alpha_value"), names.index("beta_value") + 1)
        # and in no other category, so that it is not also "Other classes"
        others = [
            n for title, cs in INVENTORY_CATEGORIES for n in cs if n == "alpha_value"
        ]
        self.assertEqual(others, ["alpha_value"])
        _, lines = self.report([0.0])
        heading = lines.index(
            next(l for l in lines if "Redshifts, tolerances and parameter values" in l)
        )
        beta = next(i for i, l in enumerate(lines) if l.startswith("@@ beta_value"))
        alpha = next(i for i, l in enumerate(lines) if l.startswith("@@ alpha_value"))
        self.assertGreater(alpha, beta)
        self.assertGreater(alpha, heading)
        self.assertFalse(any("Other classes" in l for l in lines))
        self.assertIn("values = [ 0 ]", lines[alpha:])

    def test_9_the_extra_field_follows_the_tuning_tolerance(self):
        fields = [f for f, _ in HISTORY_EXTRA_FIELDS]
        self.assertEqual(fields[fields.index("tuning_tol") + 1], "alpha")
        self.assertIn(("alpha", "alpha"), HISTORY_EXTRA_FIELDS)

    def test_9_one_alpha_is_printed_once_on_the_common_line(self):
        _, lines = self.report([0.0])
        common = [l for l in lines if l.startswith("common to every history:")]
        self.assertEqual(len(common), 1)
        self.assertTrue(common[0].endswith("log10_tuning_tol=-5, alpha=0"), common[0])
        histories = [l for l in lines if l.startswith("- T_init=")]
        self.assertEqual(len(histories), 1)
        self.assertNotIn("alpha=", histories[0])

    def test_9_two_alpha_are_printed_on_each_history_line(self):
        text, lines = self.report([0.0, 0.1])
        common = [l for l in lines if l.startswith("common to every history:")]
        self.assertEqual(len(common), 1)
        self.assertNotIn("alpha=", common[0])
        histories = [l for l in lines if l.startswith("- T_init=")]
        self.assertEqual(len(histories), 2)
        self.assertEqual(
            sorted(l.split("alpha=")[1].split(",")[0].split(" ")[0] for l in histories),
            ["0", "0.1"],
        )
        for l in histories:
            self.assertEqual(l.count("alpha="), 1)
        self.assertIn("values = [ 0, 0.1 ]", lines)


ONE_LOOP = "one-loop-measured-v1"


class TestTheInventoryNamesTheModel(_AlphaStoreCase):
    """One-loop-drain prompt 04, P39: a row of the one-loop model reads `A=<A:.5g> (<drain_model>)`."""

    def test_the_values_list_and_the_history_lines_name_a_one_loop_row(self):
        _, lines = self.report([0.0, 0.5, (0.5, ONE_LOOP), (1.0, ONE_LOOP)])
        # fixed rows by value, then the other model's: 0.5 is two rows and reads two ways
        self.assertIn(
            f"values = [ 0, 0.5, A=0.5 ({ONE_LOOP}), A=1 ({ONE_LOOP}) ]", lines
        )
        histories = [l for l in lines if l.startswith("- T_init=")]
        self.assertEqual(len(histories), 4)
        self.assertEqual(
            sorted(l.split(", ")[-1].split("  |")[0] for l in histories),
            sorted(
                ["alpha=0", "alpha=0.5", f"A=0.5 ({ONE_LOOP})", f"A=1 ({ONE_LOOP})"]
            ),
        )
        common = [l for l in lines if l.startswith("common to every history:")]
        self.assertNotIn("alpha", common[0].split("log10_tuning_tol=-5")[-1])
        self.assertFalse(any("drain_model=" in l for l in lines), "no raw key")

    def test_one_one_loop_row_alone_is_common_to_every_history(self):
        _, lines = self.report([(2.0, ONE_LOOP)])
        common = [l for l in lines if l.startswith("common to every history:")]
        self.assertTrue(
            common[0].endswith(f"log10_tuning_tol=-5, A=2 ({ONE_LOOP})"), common[0]
        )
        self.assertIn(f"values = [ A=2 ({ONE_LOOP}) ]", lines)

    def test_a_store_of_fixed_rows_alone_reports_as_before_one_loop_drain_03(self):
        text, lines = self.report([0.0, 0.1])
        self.assertNotIn("drain_model", text)
        self.assertNotIn("A=", text)
        self.assertIn("values = [ 0, 0.1 ]", lines)


if __name__ == "__main__":
    unittest.main()
