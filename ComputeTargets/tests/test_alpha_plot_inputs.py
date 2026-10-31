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
`plot_inputs.parameters` and the alphas (inelastic-reflection prompt 10, test 3), on a temporary
store built as `test_plot_inputs` builds its stores (main.py's lookups, in the stand-in cluster, no
Ray) with two alpha rows, 0 and 0.1.

- two alpha stored: `parameters` returns both rows, in the order given (not the order stored);
- an alpha not stored: `PlotOptionNotStored` naming `--alpha-values` and that alpha, the first
  missing one in the order given, and after the parameters before it (a missing `--rel-tol` is
  named first);
- the read-only pool creates no `alpha_value` row, and writes no byte of the store;
- the drivers' handler, by `ast`: both plot scripts catch `PlotOptionNotStored`, print it to stderr
  and `sys.exit(2)` (the exit 2 of `test_plot_inputs`, which a driver cannot show without Ray: it
  opens the pool after `ray.init`).

No Ray, no integration, no PRyMordial. About 3 s.
"""

import ast
import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

import ray

import plot_inputs
from ComputeTargets.tests import test_plot_inputs as tpi
from StoreFixtures import standin_pool as sp
from config.datastore import serial_batch_sizes
from config.sharding import read_table_config
from config.version import VERSION_LABEL

ROOT = Path(__file__).resolve().parents[2]
_STORE = {}


def _build_two_alpha_store(directory: Path) -> Path:
    """main.py's lookups at alpha = 0, then the row of alpha = 0.1, in one store."""
    directory.mkdir(parents=True)
    primary = directory / "store.sqlite"
    cluster = sp.StandinCluster()
    with cluster.active(), tpi.quiet():
        pool = cluster.open_pool(
            primary,
            version_label=VERSION_LABEL,
            read_table_config=read_table_config,
            serial_batch_sizes=serial_batch_sizes,
        )
        try:
            tpi._build_run(pool, **tpi.FULL[0])
            ray.get(pool.object_get("alpha_value", value=0.1))
        finally:
            cluster.close_pool(pool)
    return primary


def setUpModule():
    tmp = tempfile.TemporaryDirectory()
    _STORE["tmp"] = tmp
    _STORE["primary"] = _build_two_alpha_store(Path(tmp.name) / "two")
    tpi._MODULE["two_alpha"] = _STORE["primary"]


def tearDownModule():
    tpi._MODULE.pop("two_alpha", None)
    _STORE["tmp"].cleanup()
    if ray.is_initialized():
        raise AssertionError("Ray was initialised by test_alpha_plot_inputs")


def _parameters(pool, *alpha_values, extra=()):
    args = tpi._args(*extra)
    return plot_inputs.parameters(
        pool,
        abs_tol=args.abs_tol,
        rel_tol=args.rel_tol,
        T_init_GeV=args.T_init_GeV,
        phi_init_Mp=args.phi_init_Mp,
        units=tpi.UNITS,
        tuning_tol=args.tuning_tolerance,
        alpha_values=list(alpha_values),
    )


def _alpha_rows(primary: Path) -> int:
    total = 0
    for path in sorted(primary.parent.glob("store-shard*.sqlite")):
        con = sqlite3.connect(path)
        try:
            total += con.execute("select count(*) from alpha_value").fetchone()[0]
        finally:
            con.close()
    return total


def _digests(primary: Path):
    return {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(primary.parent.iterdir())
        if p.is_file()
    }


class TestAlphaParameters(tpi._ReadOnlyCase):
    def test_3_two_alpha_stored_both_rows_in_the_order_given(self):
        primary = self.copy("two_alpha")
        with self.read_only(primary) as pool:
            with tpi.quiet():
                both = _parameters(pool, 0.0, 0.1)
                reverse = _parameters(pool, 0.1, 0.0)
                one = _parameters(pool, 0.1)
        self.assertEqual([a.value for a in both.alphas], [0.0, 0.1])
        self.assertEqual([a.value for a in reverse.alphas], [0.1, 0.0])
        self.assertEqual([a.value for a in one.alphas], [0.1])
        self.assertIsInstance(both.alphas, tuple)
        # each a stored row (a fresh serial would mean an insert), and the rows differ
        for a in (*both.alphas, *reverse.alphas):
            self.assertTrue(getattr(a, "_deserialized", False), a)
        self.assertNotEqual(both.alphas[0].store_id, both.alphas[1].store_id)
        self.assertEqual(both.alphas[0].store_id, reverse.alphas[1].store_id)
        # the other parameters are as before
        self.assertEqual(both.tuning_tol.log10_tol, -5.0)

    def test_3_an_alpha_not_stored_names_the_option_and_the_value(self):
        primary = self.copy("two_alpha")
        with self.read_only(primary) as pool:
            with tpi.quiet(), self.assertRaises(plot_inputs.PlotOptionNotStored) as ctx:
                _parameters(pool, 0.0, 0.3)
        e = ctx.exception
        self.assertEqual(e.class_name, "alpha_value")
        self.assertEqual(e.option, "--alpha-values")
        self.assertEqual(e.value, "0.3")
        self.assertEqual(
            str(e),
            "!! plot: the store holds no alpha_value for --alpha-values 0.3; main.py was not "
            "run with it, so no history in this store can match. Give the plot the options "
            "the build used",
        )
        self.assertEqual(e.__cause__.class_name, "alpha_value")

    def test_3_the_first_missing_alpha_in_the_order_given_is_named(self):
        primary = self.copy("two_alpha")
        with self.read_only(primary) as pool:
            with tpi.quiet(), self.assertRaises(plot_inputs.PlotOptionNotStored) as ctx:
                _parameters(pool, 0.1, 0.2, 0.3)
        self.assertEqual(ctx.exception.value, "0.2")
        # the parameters before it come first: a missing --rel-tol is named before a missing alpha
        with self.read_only(primary) as pool:
            with tpi.quiet(), self.assertRaises(plot_inputs.PlotOptionNotStored) as ctx:
                _parameters(pool, 0.3, extra=("--rel-tol", "1e-6"))
        self.assertEqual(ctx.exception.option, "--rel-tol")

    def test_3_the_read_only_pool_creates_no_alpha_row(self):
        primary = self.copy("two_alpha")
        rows, files = _alpha_rows(primary), _digests(primary)
        self.assertEqual(
            rows, 2 * len(list(primary.parent.glob("store-shard*.sqlite")))
        )
        with self.read_only(primary) as pool:
            with tpi.quiet(), self.assertRaises(plot_inputs.PlotOptionNotStored):
                _parameters(pool, 0.1, 0.45)
            with tpi.quiet():
                _parameters(pool, 0.0, 0.1)
        self.assertEqual(_alpha_rows(primary), rows)
        self.assertEqual(_digests(primary), files)


class TestTheDriversHandler(unittest.TestCase):
    def test_3_both_scripts_stop_with_exit_2_on_an_alpha_not_stored(self):
        for name in ("plot_by_beta.py", "plot_ScalarHistory.py"):
            with self.subTest(script=name):
                tree = ast.parse((ROOT / name).read_text(), name)
                handlers = [
                    n
                    for n in ast.walk(tree)
                    if isinstance(n, ast.ExceptHandler)
                    and n.type is not None
                    and ast.unparse(n.type) == "plot_inputs.PlotOptionNotStored"
                ]
                self.assertEqual(len(handlers), 1)
                body = ast.unparse(ast.Module(body=handlers[0].body, type_ignores=[]))
                self.assertIn("print(str(e), file=sys.stderr)", body)
                self.assertIn("sys.exit(2)", body)
                # and the call that can raise it sits in the try the handler belongs to
                tries = [
                    n
                    for n in ast.walk(tree)
                    if isinstance(n, ast.Try) and handlers[0] in n.handlers
                ]
                self.assertEqual(len(tries), 1)
                self.assertIn("plot_inputs.parameters", ast.unparse(tries[0]))


if __name__ == "__main__":
    unittest.main()
