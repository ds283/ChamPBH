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
The inventory report, ``tools/inventory_report.py``, and ``main.py --inventory``
(dk-and-rpk-migration prompt 09).

No Ray cluster, no PRyMordial solve, no persistent datastore. Each store is built in a ``tempfile``
directory through ChamPBH's stand-in cluster (``Datastore/tests/standin_pool.py``), by the harness
of ``test_version_keyed_lookups._TempStoreCase``: a real one-shard ``ShardedPool`` writes the
version row and is closed, then the package's undecorated actor inserts the rows with no explicit
serial (the stand-in broker leases each), stamped with fixed times. The actors are closed before
the store is read, read-only, by ``datastorekit.store_inventory.read_inventory``.

The store's rows are real: every parent a spec names exists, and the type columns carry the values
of the three ``model_ids`` modules. Values are chosen here and asserted as the report prints them
(decimal, ``.5g``, with their unit), never re-derived from a key.

Four tests run ``main.py`` in a subprocess from the repository root, each with an address nothing
listens on, so that a run that started Ray would fail rather than join a cluster. Each takes a few
seconds.
"""

import ast
import contextlib
import io
import os
import subprocess
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import ray
import sqlalchemy as sqla
from datastorekit.store_inventory import read_inventory

import CosmologyConcepts.ConformalCouplings.model_ids as coupling_ids
import CosmologyConcepts.Potentials.model_ids as potential_ids
import CosmologyModels.model_ids as cosmology_ids
from Datastore.tests.test_version_keyed_lookups import _TempStoreCase
from config.datastore import factories
from tools.inventory_report import format_inventory_report

REPO_ROOT = Path(__file__).resolve().parents[2]

LABEL = "2026.6.0"
T0 = datetime(2026, 10, 12, 16, 55, 55)

# 25 redshifts, so that the report truncates them unless verbose
REDSHIFTS = [0.5 * i for i in range(25)]

# an address nothing listens on
NO_RAY = "127.0.0.1:9"


def _filled(table: sqla.Table, row: dict) -> dict:
    """``row`` with a zero for every NOT NULL column it leaves out that has no default."""
    out = dict(row)
    for column in table.columns:
        if (
            column.name in out
            or column.name in ("serial", "version", "timestamp")
            or column.nullable
            or column.primary_key
            or column.default is not None
        ):
            continue
        if isinstance(column.type, sqla.Float):
            out[column.name] = 0.0
        elif isinstance(column.type, sqla.Boolean):
            out[column.name] = False
        else:
            out[column.name] = 0
    return out


class _InventoryStoreCase(_TempStoreCase):
    """The harness's one-shard store, filled with real rows, its actors closed before it is read."""

    def tearDown(self):
        super().tearDown()
        # the harness asserts this too; every test of this module, the driver's included, says so
        self.assertFalse(ray.is_initialized())

    def insert(self, store, cls_name, row, at=None):
        table = store._tables[cls_name]
        with store._engine.begin() as conn:
            return store._schema[cls_name]["insert"](
                conn, _filled(table, row), insert_timestamp=at or T0
            )

    def close_actors(self):
        for store in self._stores:
            with contextlib.redirect_stdout(io.StringIO()):
                store.__exit__(None, None, None)
        self._stores = []

    def build_store(self, problems=False):
        """
        A store with one failed and one successful ScalarModel, an AdiabaticHistory and an
        unvalidated BBNData row on the successful model, each of the last three with value rows.
        With ``problems``, also five ScalarModelValue rows naming ScalarModel rows that are not
        there (with serials long enough to make the problem longer than any line width), and a
        ScalarModel whose cosmology_type (0, LAMBDACDM_IDENTIFIER) names no stored class.
        """
        store = self.open(LABEL)

        def ins(cls_name, row, at=None):
            return self.insert(store, cls_name, row, at)

        tag = ins("store_tag", {"label": "pilot"})
        z = [ins("redshift", {"z": value}) for value in REDSHIFTS]
        atol = ins("tolerance", {"log10_tol": -10.0})
        rtol = ins("tolerance", {"log10_tol": -8.0})
        beta_failed = ins("beta_value", {"value": 1.1})
        beta_ok = ins("beta_value", {"value": 25.0})
        M = ins("M_value", {"value_eV": 2.436e24})
        Lambda = ins("Lambda_value", {"value_eV": 1.0e-3})
        T_init = ins("temperature", {"value_GeV": 2.0e4})
        T_stop = ins("temperature", {"value_GeV": 2.3493e-13})
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
        potential = ins(
            "ExponentialPotential", dict(M_serial=M, Lambda_serial=Lambda, n_value=1)
        )
        coupling_failed = ins("ExponentialCoupling", {"beta_serial": beta_failed})
        coupling_ok = ins("ExponentialCoupling", {"beta_serial": beta_ok})

        model = dict(
            cosmology_type=cosmology_ids.XAV_IMPROVED_EOS_IDENTIFIER,
            cosmology_serial=cosmology,
            potential_type=potential_ids.EXPONENTIAL_POTENTIAL,
            potential_serial=potential,
            coupling_type=coupling_ids.EXPONENTIAL_COUPLING,
            atol_serial=atol,
            rtol_serial=rtol,
            phi_Einstein_init_serial=phi,
            pi_Einstein_init_serial=pi,
            T_Jordan_init_serial=T_init,
            T_Jordan_stop_serial=T_stop,
            validated=True,
        )
        ins(
            "ScalarModel",
            dict(
                model,
                label="job-a-ScalarModel-beta=1.1-2026-10-12T16:59:07",
                coupling_serial=coupling_failed,
                failure=True,
                failure_reason="step budget exhausted",
            ),
            at=T0 + timedelta(minutes=4),
        )
        ok = ins(
            "ScalarModel",
            dict(
                model,
                label="job-a-ScalarModel-beta=25-2026-10-12T16:59:36",
                coupling_serial=coupling_ok,
                failure=False,
                solver_serial=solver,
                z_samples=3,
            ),
            at=T0 + timedelta(minutes=8),
        )
        ins("ScalarModel_tags", {"model_serial": ok, "tag_serial": tag})
        for serial in z[:3]:
            ins("ScalarModelValue", {"model_serial": ok, "z_serial": serial})

        history = ins(
            "AdiabaticHistory",
            dict(model_serial=ok, label="job-b-history", z_samples=2, validated=True),
            at=T0 + timedelta(minutes=10),
        )
        for serial in z[:2]:
            ins(
                "AdiabaticHistoryValue", {"history_serial": history, "z_serial": serial}
            )

        bbn = ins(
            "BBNData",
            dict(
                model_serial=ok,
                label="job-b-BBNData",
                failure=False,
                PRyM_version="bf24c3d",
                z_samples=4,
                validated=False,
            ),
            at=T0 + timedelta(minutes=11),
        )
        for serial in z[:4]:
            ins("BBNDataValue", {"bbn_serial": bbn, "z_serial": serial})

        if problems:
            for k in range(5):
                ins(
                    "ScalarModelValue",
                    {"model_serial": 10**18 + k, "z_serial": z[0]},
                )
            ins(
                "ScalarModel",
                dict(
                    model,
                    cosmology_type=cosmology_ids.LAMBDACDM_IDENTIFIER,
                    label="job-c-ScalarModel",
                    coupling_serial=coupling_ok,
                    failure=True,
                ),
            )

        self.close_actors()
        return self.db


def _report_lines(report: str):
    return [line.strip() for line in report.splitlines()]


class TestReportOnAStore(_InventoryStoreCase):
    """Test 3: a store with one failed and one successful ScalarModel, an AdiabaticHistory and a
    BBNData row, each with value rows, reads back with no problem, and the report shows every field
    of P15 (as decided in prompt 09 §1.2), by value."""

    def setUp(self):
        super().setUp()
        self.inventory = read_inventory(self.build_store(), factories)
        self.report = format_inventory_report(self.inventory, "store.db")
        self.lines = _report_lines(self.report)

    def test_no_problem_and_every_class(self):
        self.assertEqual(self.inventory.problems, ())
        self.assertEqual(len(self.inventory.classes), 21)
        self.assertIn("problems: none", self.lines)
        self.assertEqual(self.lines[0], "@@ DATASTORE INVENTORY: store.db")
        self.assertIn("read-only; 1 shard (#0); 46 records in 21 classes", self.lines)

    def test_the_quantities(self):
        for line in (
            "values = [ 1.1, 25 ]",
            "values = [ 2.3493e-13 GeV, 20000 GeV ]",
            "values = [ 2.436e+24 eV ]",
            "values = [ 0.001 eV ]",
            "values = [ 5 PlanckMass ]",
            "values = [ 0 PlanckMass ]",
            "log10_tol = [ -10, -8 ]",
            "label = [ 2026.6.0 ]",
            "label = [ pilot ]",
        ):
            self.assertIn(line, self.lines)
        # 25 redshifts: the first and last ten
        self.assertIn(
            "values = [ 0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, "
            "... 5 values not shown (--show-all shows every one) ..., "
            "7.5, 8, 8.5, 9, 9.5, 10, 10.5, 11, 11.5, 12 ]",
            self.lines,
        )
        self.assertIn(
            "@@ redshift (replicated): 25 records | committed at Mon 12 Oct 2026 16:55:55",
            self.lines,
        )

    def test_the_scalar_models(self):
        self.assertIn(
            "@@ ScalarModel (sharded): 2 records: 2 validated, 0 unvalidated; "
            "3 ScalarModelValue rows | committed between Mon 12 Oct 2026 16:59:55 and "
            "Mon 12 Oct 2026 17:03:55",
            self.lines,
        )
        self.assertIn(
            "common to every model: cosmology=QCD_Cosmology(omega_m=0.3111, "
            "omega_cc=0.6889, h=0.6766, f_baryon=0.1575, T_CMB_Kelvin=2.7255, Neff=3.046), "
            "log10_atol=-10, log10_rtol=-8",
            self.lines,
        )
        self.assertIn("validated: 2 records; version = 2026.6.0", self.lines)
        self.assertIn("unvalidated: none", self.lines)
        common = (
            "T_init=20000 GeV, T_stop=2.3493e-13 GeV, phiE_init=5 PlanckMass, "
            "piE_init=0 PlanckMass, potential=ExponentialPotential(M=2.436e+24 eV, "
            "Lambda=0.001 eV, n_value=1)"
        )
        self.assertIn(
            f"- {common}, coupling=ExponentialCoupling(beta=1.1), failure=True  | values: 0",
            self.lines,
        )
        self.assertIn(
            f"- {common}, coupling=ExponentialCoupling(beta=25), failure=False  "
            f"tags: pilot  | values: 3",
            self.lines,
        )

    def test_the_history_and_the_bbn_data(self):
        model = (
            "model: {T_init=20000 GeV, T_stop=2.3493e-13 GeV, phiE_init=5 PlanckMass, "
            "piE_init=0 PlanckMass, potential=ExponentialPotential(M=2.436e+24 eV, "
            "Lambda=0.001 eV, n_value=1), coupling=ExponentialCoupling(beta=25), "
            "failure=False, tags=[pilot]}"
        )
        self.assertIn(
            "@@ AdiabaticHistory (sharded): 1 record: 1 validated, 0 unvalidated; "
            "2 AdiabaticHistoryValue rows | committed at Mon 12 Oct 2026 17:05:55",
            self.lines,
        )
        self.assertIn(f"- {model}  | values: 2", self.lines)
        self.assertIn(
            "@@ BBNData (sharded): 1 record: 0 validated, 1 unvalidated; "
            "4 BBNDataValue rows | committed at Mon 12 Oct 2026 17:06:55",
            self.lines,
        )
        self.assertIn("unvalidated: 1 record; version = 2026.6.0", self.lines)
        self.assertIn(
            f"- failure=False, PRyM_version=bf24c3d, {model}  | values: 4", self.lines
        )

    def test_the_other_classes(self):
        for line in (
            "- label=solve_ivp+RK45, stepping=0",
            "- M=2.436e+24 eV, Lambda=0.001 eV, n_value=1",
            "- beta=1.1",
            "- beta=25",
        ):
            self.assertIn(line, self.lines)

    def test_the_stored_labels_are_not_shown(self):
        self.assertNotIn("job-", self.report)

    def test_verbose_lists_every_value(self):
        verbose = _report_lines(
            format_inventory_report(self.inventory, "store.db", verbose=True)
        )
        self.assertIn(
            "values = [ " + ", ".join(f"{z:.5g}" for z in REDSHIFTS) + " ]", verbose
        )
        self.assertNotIn("not shown", "\n".join(verbose))


class TestProblemsAreShownWhole(_InventoryStoreCase):
    """Test 4: every problem ``read_inventory`` names is printed whole, on one line, under its
    class, however long."""

    def test_an_orphan_value_row_and_an_unresolved_cosmology(self):
        inventory = read_inventory(self.build_store(problems=True), factories)
        problems = inventory.problems
        self.assertEqual(len(problems), 2, problems)
        self.assertTrue(problems[0].startswith("orphan-value: ScalarModel: 5 "))
        self.assertIn("1000000000000000004", problems[0])
        self.assertTrue(
            problems[1].startswith(
                "unresolved-parent: ScalarModel: 1 row(s) on shard #0"
            )
        )
        self.assertIn("(field(s) cosmology)", problems[1])
        # longer than any width the renderer could truncate to
        self.assertGreater(len(problems[0]), 200)

        report = format_inventory_report(inventory, "store.db")
        lines = report.splitlines()
        self.assertIn("   problems: 2, each listed in full under its class", lines)
        for problem in problems:
            self.assertIn(f"            !! {problem}", lines)
        # under ScalarModel's section, after its header
        header = next(i for i, l in enumerate(lines) if "@@ ScalarModel " in l)
        listed = [i for i, l in enumerate(lines) if l.strip().startswith("!! ")]
        self.assertTrue(all(i > header for i in listed))
        self.assertIn("         2 problems:", lines)


def _main(*arguments, timeout=300):
    return subprocess.run(
        [sys.executable, "main.py", "--ray-address", NO_RAY, *arguments],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


class TestTheDriver(_InventoryStoreCase):
    """Test 5: ``main.py --inventory`` reads a closed store before Ray starts, refuses ``--drop``,
    fails cleanly on a store that is not there, and prints the report (verbose with
    ``--show-all``)."""

    def test_an_absent_store_exits_1_and_creates_nothing(self):
        absent = self._dir / "absent.db"
        result = _main("--database", str(absent), "--inventory")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("!! --inventory: Cannot read sharded datastore", result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        self.assertFalse(absent.exists())
        self.assertEqual(os.listdir(self._dir), [])

    def test_drop_is_refused_with_exit_2(self):
        absent = self._dir / "absent.db"
        result = _main("--database", str(absent), "--inventory", "--drop", "bbn-data")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("!! --inventory: ", result.stderr)
        self.assertIn("--drop", result.stderr)
        self.assertEqual(os.listdir(self._dir), [])

    def test_a_store_prints_the_report_and_exits_0(self):
        db = self.build_store()
        before = sorted(os.listdir(self._dir))
        result = _main("--database", str(db), "--inventory")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = format_inventory_report(read_inventory(db, factories), str(db))
        lines = result.stdout.splitlines()
        self.assertTrue(lines[0].startswith(">> packages: "))
        self.assertEqual("\n".join(lines[1:]), report)
        self.assertIn("not shown", report)
        self.assertEqual(sorted(os.listdir(self._dir)), before)

    def test_show_all_prints_the_verbose_report(self):
        db = self.build_store()
        result = _main("--database", str(db), "--inventory", "--show-all")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = format_inventory_report(
            read_inventory(db, factories), str(db), verbose=True
        )
        self.assertEqual("\n".join(result.stdout.splitlines()[1:]), report)
        self.assertNotIn("not shown", report)

    def test_the_inventory_branch_returns_before_ray_starts(self):
        tree = ast.parse((REPO_ROOT / "main.py").read_text(), "main.py")

        def calls(node, dotted):
            return any(
                isinstance(n, ast.Call) and ast.unparse(n.func) == dotted
                for n in ast.walk(node)
            )

        branch = [
            i
            for i, stmt in enumerate(tree.body)
            if isinstance(stmt, ast.If)
            and ast.unparse(stmt.test) == "args.inventory"
            and calls(stmt, "read_inventory")
            and calls(stmt, "sys.exit")
        ]
        init = [i for i, stmt in enumerate(tree.body) if calls(stmt, "ray.init")]
        self.assertEqual(len(branch), 1, "main.py: module-level --inventory branches")
        self.assertEqual(len(init), 1, "main.py: module-level ray.init calls")
        self.assertLess(branch[0], init[0])
        # the branch ends in sys.exit(0): nothing after it runs
        self.assertEqual(ast.unparse(tree.body[branch[0]].body[-1]), "sys.exit(0)")


if __name__ == "__main__":
    unittest.main()
