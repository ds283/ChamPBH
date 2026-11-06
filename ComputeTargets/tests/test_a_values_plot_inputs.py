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
`plot_inputs.parameters` and the A rows (one-loop-drain prompt 04, prompt §1.3, §1.9, §2.3), on a temporary store
built as `test_alpha_plot_inputs` builds its stores (main.py's lookups, in the stand-in cluster, no Ray) with the
rows alpha = 0 and 0.5 under the fixed model and A = 0.5 and 1 under the one-loop model, so that 0.5 is stored
under both:

- both kinds of row are returned in one tuple, the fixed rows first and the one-loop rows after them, each in the
  order given and each with its model;
- an A stored only as an alpha is refused, naming `--A-values`, the value and the model;
- an alpha stored only as an A is refused, naming `--alpha-values`;
- the first missing row in that order is named, after the parameters before it;
- a call with no `A_values` is the call of before;
- the read-only pool writes no byte of the store and creates no row;
- the drivers' handler still exits 2 on `PlotOptionNotStored` (`test_alpha_plot_inputs`' test, which this module
  reruns by `ast` for the A rows' option).

No Ray, no integration, no PRyMordial. About 3 s.
"""

import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

import ray

import plot_inputs
from ComputeTargets.tests import test_plot_inputs as tpi
from CosmologyConcepts.drain_model import DRAIN_MODEL_FIXED, DRAIN_MODEL_ONE_LOOP
from StoreFixtures import standin_pool as sp
from config.datastore import serial_batch_sizes
from config.sharding import read_table_config
from config.version import VERSION_LABEL

_STORE = {}

# the alpha rows stored under the fixed model, and the A rows under the one-loop model; 0.5 is both
FIXED_ROWS = (0.0, 0.5)
ONE_LOOP_ROWS = (0.5, 1.0)


def _build_store(directory: Path, fixed=FIXED_ROWS, one_loop=ONE_LOOP_ROWS) -> Path:
    """main.py's lookups at alpha = 0, then the rows of the alphas `fixed` and of the As `one_loop`, in one store."""
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
            for a in fixed:
                ray.get(pool.object_get("alpha_value", value=a))
            for A in one_loop:
                ray.get(
                    pool.object_get(
                        "alpha_value", value=A, drain_model=DRAIN_MODEL_ONE_LOOP
                    )
                )
        finally:
            cluster.close_pool(pool)
    return primary


def setUpModule():
    tmp = tempfile.TemporaryDirectory()
    _STORE["tmp"] = tmp
    _STORE["primary"] = _build_store(Path(tmp.name) / "both")
    tpi._MODULE["both_models"] = _STORE["primary"]
    # 0.25 is stored as an alpha only, and 0.75 as an A only
    tpi._MODULE["split_models"] = _build_store(
        Path(tmp.name) / "split", fixed=(0.0, 0.25), one_loop=(0.75,)
    )


def tearDownModule():
    tpi._MODULE.pop("both_models", None)
    tpi._MODULE.pop("split_models", None)
    _STORE["tmp"].cleanup()
    if ray.is_initialized():
        raise AssertionError("Ray was initialised by test_a_values_plot_inputs")


def _parameters(pool, alphas, As=None, extra=()):
    args = tpi._args(*extra)
    kwargs = dict(
        abs_tol=args.abs_tol,
        rel_tol=args.rel_tol,
        T_init_GeV=args.T_init_GeV,
        phi_init_Mp=args.phi_init_Mp,
        units=tpi.UNITS,
        tuning_tol=args.tuning_tolerance,
        alpha_values=list(alphas),
    )
    if As is not None:
        kwargs["A_values"] = list(As)
    return plot_inputs.parameters(pool, **kwargs)


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


class TestAParameters(tpi._ReadOnlyCase):
    def test_both_kinds_of_row_in_the_order_given_each_with_its_model(self):
        primary = self.copy("both_models")
        with self.read_only(primary) as pool:
            with tpi.quiet():
                rows = _parameters(pool, [0.5, 0.0], [1.0, 0.5])
                other = _parameters(pool, [0.0], [0.5])
        self.assertIsInstance(rows.alphas, tuple)
        # the fixed rows first, in the order given of --alpha-values, then the one-loop rows in that of --A-values
        self.assertEqual(
            [(a.value, a.drain_model) for a in rows.alphas],
            [
                (0.5, DRAIN_MODEL_FIXED),
                (0.0, DRAIN_MODEL_FIXED),
                (1.0, DRAIN_MODEL_ONE_LOOP),
                (0.5, DRAIN_MODEL_ONE_LOOP),
            ],
        )
        self.assertEqual(
            [(a.value, a.drain_model) for a in other.alphas],
            [(0.0, DRAIN_MODEL_FIXED), (0.5, DRAIN_MODEL_ONE_LOOP)],
        )
        # each a stored row, and 0.5 is two rows: the same value under two models
        for a in rows.alphas:
            self.assertTrue(getattr(a, "_deserialized", False), a)
        serials = [a.store_id for a in rows.alphas]
        self.assertEqual(len(set(serials)), 4)
        self.assertEqual(rows.alphas[0].drain_law, "fixed")
        self.assertEqual(rows.alphas[2].drain_law, "one_loop")
        # the other parameters are as before
        self.assertEqual(rows.tuning_tol.log10_tol, -5.0)

    def test_a_call_with_no_A_values_is_the_call_of_before(self):
        primary = self.copy("both_models")
        with self.read_only(primary) as pool:
            with tpi.quiet():
                rows = _parameters(pool, [0.0, 0.5])
                empty = _parameters(pool, [0.0, 0.5], [])
        self.assertEqual(
            [(a.value, a.drain_model) for a in rows.alphas],
            [(0.0, "fixed"), (0.5, "fixed")],
        )
        self.assertEqual(
            [a.store_id for a in rows.alphas], [a.store_id for a in empty.alphas]
        )

    def test_an_A_stored_only_as_an_alpha_is_refused_naming_the_option(self):
        primary = self.copy("split_models")
        # 0.25 is a fixed row of this store and no one-loop row
        with self.read_only(primary) as pool:
            with tpi.quiet(), self.assertRaises(plot_inputs.PlotOptionNotStored) as ctx:
                _parameters(pool, [0.0, 0.25], [0.75, 0.25])
        e = ctx.exception
        self.assertEqual(e.class_name, "alpha_value")
        self.assertEqual(e.option, "--A-values")
        self.assertEqual(e.value, "0.25 (one-loop-measured-v1)")
        self.assertIn("--A-values 0.25 (one-loop-measured-v1)", str(e))
        self.assertEqual(e.__cause__.class_name, "alpha_value")

    def test_an_alpha_stored_only_as_an_A_is_refused_naming_the_option(self):
        primary = self.copy("split_models")
        # 0.75 is a one-loop row of this store and no fixed row
        with self.read_only(primary) as pool:
            with tpi.quiet(), self.assertRaises(plot_inputs.PlotOptionNotStored) as ctx:
                _parameters(pool, [0.0, 0.75], [0.75])
        e = ctx.exception
        self.assertEqual(e.class_name, "alpha_value")
        self.assertEqual(e.option, "--alpha-values")
        self.assertEqual(e.value, "0.75")
        self.assertEqual(
            str(e),
            "!! plot: the store holds no alpha_value for --alpha-values 0.75; main.py was not "
            "run with it, so no history in this store can match. Give the plot the options "
            "the build used",
        )
        # and each is found under its own model
        with self.read_only(primary) as pool:
            with tpi.quiet():
                rows = _parameters(pool, [0.25], [0.75])
        self.assertEqual(
            [(a.value, a.drain_model) for a in rows.alphas],
            [(0.25, "fixed"), (0.75, DRAIN_MODEL_ONE_LOOP)],
        )

    def test_the_first_missing_row_in_the_order_is_named_after_the_parameters(self):
        primary = self.copy("both_models")
        with self.read_only(primary) as pool:
            # a missing alpha is named before a missing A
            with tpi.quiet(), self.assertRaises(plot_inputs.PlotOptionNotStored) as ctx:
                _parameters(pool, [0.0, 0.3], [0.2])
        self.assertEqual(ctx.exception.option, "--alpha-values")
        with self.read_only(primary) as pool:
            # and a missing --rel-tol before either
            with tpi.quiet(), self.assertRaises(plot_inputs.PlotOptionNotStored) as ctx:
                _parameters(pool, [0.3], [0.2], extra=("--rel-tol", "1e-6"))
        self.assertEqual(ctx.exception.option, "--rel-tol")

    def test_the_read_only_pool_writes_no_byte(self):
        primary = self.copy("both_models")
        rows, files = _alpha_rows(primary), _digests(primary)
        self.assertEqual(
            rows, 4 * len(list(primary.parent.glob("store-shard*.sqlite")))
        )
        with self.read_only(primary) as pool:
            with tpi.quiet(), self.assertRaises(plot_inputs.PlotOptionNotStored):
                _parameters(pool, [0.0], [0.25])
            with tpi.quiet(), self.assertRaises(plot_inputs.PlotOptionNotStored):
                _parameters(pool, [0.25], [1.0])
            with tpi.quiet():
                _parameters(pool, [0.0, 0.5], [0.5, 1.0])
        self.assertEqual(_alpha_rows(primary), rows)
        self.assertEqual(_digests(primary), files)


if __name__ == "__main__":
    unittest.main()
