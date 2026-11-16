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
The store tools on a store holding both models (power-law-wall prompt 05 §1.7, tools/tests; README §6.9 P35, P37,
P39, P42).

A temporary store (`_TempStoreCase`, through `test_inverse_power_store`'s harness) holding one history of each model
at the same (M, beta, alpha), whose two model rows take the same serial (1) in their two tables. Both rows are stored
from one payload, `test_dissipation_store`'s TEST_1 (M = 0.5 M_P, beta = 2, alpha = 0.1; one integration of a few
seconds, shared through `dissipation_histories`' cache): the tools read the store and never the payload's model, so
the exponential's payload stands for the power law's, and the two rows differ in `model_type` alone among their
columns, which is the case a join on `model_serial` alone gets wrong.

- `tools/compare_stores.py`: a copy of the store is equal to it, with each history resolved through its own model
  table; one value changed in the power-law history's row is a difference in that row alone, rendered with its model
  as an `InversePowerModel` row. Tests 13 and 13b of `test_compare_stores` check the maps;
- `tools/inventory_report.py`: the report lists both model classes, under "Models", and each history with its own
  model;
- `tools/outward_step_scan.py`: the store read selects the exponential's history only, and counts the power-law
  history in one line on stderr, beside the line of the rows under another drain model, which a store of one model
  never prints.

No Ray, no PRyMordial solve.
"""

import contextlib
import glob
import io
import shutil
import sqlite3
from pathlib import Path

from datastorekit.store_inventory import read_inventory

from ComputeTargets.tests import dissipation_histories as dh
from CosmologyConcepts.Models import EXPONENTIAL_MODEL, INVERSE_POWER_MODEL
from ObjectFactories.tests.test_dissipation_store import TEST_1
from ObjectFactories.tests.test_inverse_power_store import _PowerStoreCase
from config.datastore import factories
from tools import compare_stores as cs
from tools import outward_step_scan as oss
from tools.inventory_report import INVENTORY_CATEGORIES, format_inventory_report

BETA, M, ALPHA = TEST_1


class _MixedStore(_PowerStoreCase):
    """One history of each model, both from TEST_1's payload, at the same (M, beta, alpha); the actors closed."""

    def setUp(self):
        super().setUp()
        payload = dh.payload(*TEST_1)
        self.serials = {}
        for table in ("ExponentialModel", "InversePowerModel"):
            with self.as_model(table):
                _, self.serials[table] = self.write(payload, BETA, M, ALPHA)
        self.store_cosmology_row()
        self.close_actors()
        shards = sorted(glob.glob(str(self._dir / "*shard*.db")))
        self.assertEqual(len(shards), 1)
        self.shard = shards[0]
        self.stem = self.shard.rsplit("-shard", 1)[0]

        con = sqlite3.connect(self.shard)
        rows = con.execute(
            "select serial, model_type, model_serial from ScalarHistory order by serial"
        ).fetchall()
        con.close()
        self.assertEqual(
            rows,
            [
                (self.serials["ExponentialModel"], EXPONENTIAL_MODEL, 1),
                (self.serials["InversePowerModel"], INVERSE_POWER_MODEL, 1),
            ],
        )

    def copy(self, name: str) -> Path:
        """A copy of the store's files in a directory of its own; returns the copy's primary."""
        directory = self._dir / name
        directory.mkdir()
        for path in glob.glob(str(self._dir / "store*.db")):
            shutil.copy(path, directory / Path(path).name)
        return directory / Path(self.db).name


class TestCompareStoresOnAMixedStore(_MixedStore):
    def test_a_copy_is_equal_with_each_history_through_its_own_table(self):
        other = self.copy("b")
        result = cs.compare_stores(self.db, other)
        self.assertEqual(result["problems"], [], cs.format_result(result))
        self.assertTrue(result["equal"], cs.format_result(result))
        tables = result["tables"]
        self.assertEqual(tables["ScalarHistory"]["matched"], 2)
        self.assertEqual(tables["ExponentialModel"]["matched"], 1)
        self.assertEqual(tables["InversePowerModel"]["matched"], 1)
        self.assertEqual(tables["InversePowerModel"]["kind_a"], "sharded")

    def test_a_changed_value_in_the_power_law_row_is_that_rows_difference(self):
        other = self.copy("b")
        shard = glob.glob(str(other.parent / "*shard*.db"))[0]
        con = sqlite3.connect(shard)
        with con:
            con.execute(
                "update ScalarHistory set Delta_N_eff_today = Delta_N_eff_today * 2 where serial = ?",
                (self.serials["InversePowerModel"],),
            )
        con.close()
        result = cs.compare_stores(self.db, other)
        self.assertEqual(result["problems"], [], cs.format_result(result))
        self.assertFalse(result["equal"])
        history = result["tables"]["ScalarHistory"]
        self.assertEqual(history["matched"], 1)
        self.assertEqual((history["a_only"], history["b_only"]), (1, 1))
        (pair,) = history["near_misses"]
        self.assertEqual(
            [d["column"] for d in pair["differing"]], ["Delta_N_eff_today"]
        )
        for side in ("a", "b"):
            self.assertEqual(pair[side]["model_type"], INVERSE_POWER_MODEL)
            self.assertEqual(pair[side]["model_serial"]["table"], "InversePowerModel")
        # the exponential's history and both model rows are unchanged
        self.assertEqual(result["tables"]["ExponentialModel"]["matched"], 1)
        self.assertEqual(result["tables"]["InversePowerModel"]["matched"], 1)


class TestInventoryReportOnAMixedStore(_MixedStore):
    def test_both_model_classes_and_each_history_with_its_own(self):
        inventory = read_inventory(self.db, factories)
        self.assertEqual(inventory.problems, ())
        report = format_inventory_report(inventory, "store.db")
        lines = [line.strip() for line in report.splitlines()]
        self.assertIn("problems: none", lines)
        self.assertEqual(
            dict(INVENTORY_CATEGORIES)["Models"],
            ["ExponentialModel", "InversePowerModel"],
        )
        self.assertIn(
            "@@ ExponentialModel (sharded): 1 record | committed",
            report.replace("\n", " "),
        )
        for table in ("ExponentialModel", "InversePowerModel"):
            with self.subTest(table=table):
                self.assertTrue(
                    any(l.startswith(f"@@ {table} (sharded): 1 record") for l in lines),
                    report,
                )
                self.assertTrue(
                    any(f"model={table}(M=" in l for l in lines if l.startswith("- ")),
                    report,
                )
        # the models' section lists the exponential's before the power law's
        self.assertLess(
            report.index("@@ ExponentialModel"), report.index("@@ InversePowerModel")
        )


class TestOutwardStepScanOnAMixedStore(_MixedStore):
    def select(self, **kwargs):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            specs = oss.select_from_store(self.stem, **kwargs)
        return specs, err.getvalue()

    def test_the_exponentials_rows_only_and_the_other_counted(self):
        specs, err = self.select()
        self.assertEqual(
            [s["store"]["serial"] for s in specs], [self.serials["ExponentialModel"]]
        )
        self.assertEqual((specs[0]["beta"], specs[0]["alpha"]), (BETA, ALPHA))
        lines = err.splitlines()
        self.assertEqual(len(lines), 1, err)
        self.assertIn("1 ScalarHistory row of", lines[0])
        self.assertIn(self.stem, lines[0])
        self.assertIn("of another model than 'ExponentialModel' skipped", lines[0])
        self.assertNotIn("drain model", lines[0])

    def test_the_count_does_not_depend_on_the_filters(self):
        for kwargs in (dict(M=[1e-12]), dict(beta=[99.0]), dict(success_only=True)):
            with self.subTest(kwargs=kwargs):
                _, err = self.select(**kwargs)
                self.assertEqual(len(err.splitlines()), 1, err)
                self.assertIn("1 ScalarHistory row of", err)
