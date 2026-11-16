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
`plot_inputs` with the potentials (power-law-wall prompt 06 §1.3, §1.5, §1.10; README §6.10 P47, P49), on stores built by
main.py's lookups in the stand-in cluster (`StoreFixtures/standin_pool.py`, as test_plot_inputs builds them) and opened
read-only, as the plot scripts open them:

- **a mixed store** (both potentials at the same M, the exponential at n = 1 and beta 0.5, 1; the power law at n = 1 and
  2 and beta 0.5): each potential's models are found, in its own table and never the other's, though the two tables'
  serials repeat each other; `exponential_models` is the exponential's call of the generic lookup;
- **a potential not stored**: on an exponential-only store `--models inverse-power` is `PlotOptionNotStored` naming
  `--models` and `InversePowerModel`, raised before the per-n check; a stored potential without a row at one n is still
  refused naming `--n-values`;
- **the A rows**: each potential's A rows under its own drain model, A by A in the order given; an A stored only under
  the other potential's drain model refused, naming `--A-values`, the value and the drain model; the fixed rows shared.

No Ray cluster, no persistent store, no integration. About 3 s.
"""

import contextlib
import io
import shutil
import tempfile
import unittest
from pathlib import Path

import ray

import plot_inputs
from CosmologyConcepts.Models import ExponentialModel, InversePowerModel
from StoreFixtures import standin_pool as sp
from Units import Planck_units
from config.datastore import serial_batch_sizes
from config.model_list import build_model_list
from config.sharding import read_table_config
from config.version import VERSION_LABEL

UNITS = Planck_units()
V1 = "one-loop-measured-v1"
PL = "one-loop-measured-pl-v1"
M_EV = 1.0e25

_MODULE = {}


@contextlib.contextmanager
def quiet():
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
        io.StringIO()
    ):
        yield


def _build_run(pool, tables_n_betas, alpha_rows):
    """One build's lookups, read-write, with main.py's payloads: the parameter rows, the alpha rows (value, drain model
    or None), and for each (table, n, betas) the models at M_EV."""
    ray.get(pool.object_get("temperature", value=20000 * UNITS.GeV, units=UNITS))
    ray.get(
        [
            pool.object_get("phi_value", value=5.0 * UNITS.PlanckMass, units=UNITS),
            pool.object_get("pi_value", value=0.0, units=UNITS),
        ]
    )
    ray.get(
        [
            pool.object_get("tolerance", tol=1e-8),
            pool.object_get("tolerance", tol=1e-8),
            pool.object_get("tolerance", tol=1e-5),
            *[
                (
                    pool.object_get("alpha_value", value=v)
                    if m is None
                    else pool.object_get("alpha_value", value=v, drain_model=m)
                )
                for v, m in alpha_rows
            ],
        ]
    )
    all_betas = sorted({b for _, _, betas in tables_n_betas for b in betas})
    beta_array = ray.get(
        pool.object_get("beta_value", payload_data=[{"value": b} for b in all_betas])
    )
    (M,) = ray.get(
        pool.object_get(
            "M_value", payload_data=[{"value": M_EV * UNITS.eV, "units": UNITS}]
        )
    )
    for table, n_values, betas in tables_n_betas:
        stored = [b for b in beta_array if b.value in betas]
        ray.get(
            pool.object_get(
                table,
                payload_data=[
                    {"shard_key": b, "M": M, "n": n, "beta": b, "units": UNITS}
                    for n in n_values
                    for b in stored
                ],
            )
        )
    build_model_list(pool, UNITS)


def _build_store(directory: Path, **run) -> Path:
    directory.mkdir(parents=True)
    primary = directory / "store.sqlite"
    cluster = sp.StandinCluster()
    with cluster.active(), quiet():
        pool = cluster.open_pool(
            primary,
            version_label=VERSION_LABEL,
            read_table_config=read_table_config,
            serial_batch_sizes=serial_batch_sizes,
        )
        try:
            _build_run(pool, **run)
        finally:
            cluster.close_pool(pool)
    return primary


MIXED = dict(
    tables_n_betas=[
        ("ExponentialModel", (1,), (0.5, 1.0)),
        ("InversePowerModel", (1, 2), (0.5,)),
    ],
    alpha_rows=[(0.0, None), (0.1, None), (1.0, V1), (1.0, PL), (0.5, V1)],
)
EXPONENTIAL_ONLY = dict(
    tables_n_betas=[("ExponentialModel", (1,), (0.5, 1.0))],
    alpha_rows=[(0.0, None), (1.0, V1)],
)


def setUpModule():
    tmp = tempfile.TemporaryDirectory()
    _MODULE["tmp"] = tmp
    root = Path(tmp.name)
    _MODULE["mixed"] = _build_store(root / "mixed", **MIXED)
    _MODULE["exponential"] = _build_store(root / "exponential", **EXPONENTIAL_ONLY)


def tearDownModule():
    _MODULE["tmp"].cleanup()
    if ray.is_initialized():
        raise AssertionError("Ray was initialised by test_models_plot_inputs")


class _ReadOnlyCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self._copies = 0
        self.cluster = sp.StandinCluster()
        active = self.cluster.active()
        active.__enter__()
        self.addCleanup(active.__exit__, None, None, None)

    def tearDown(self):
        self.assertFalse(ray.is_initialized())

    @contextlib.contextmanager
    def read_only(self, name: str):
        self._copies += 1
        d = self.root / f"copy-{self._copies}"
        shutil.copytree(_MODULE[name].parent, d)
        with quiet():
            pool = self.cluster.open_pool(
                d / "store.sqlite",
                version_label=VERSION_LABEL,
                read_only=True,
                read_table_config=read_table_config,
                serial_batch_sizes=serial_batch_sizes,
            )
        try:
            yield pool
        finally:
            self.cluster.close_pool(pool)

    def models(self, pool, potential, n_values):
        beta_table, M_table = plot_inputs.stored_grid(pool, UNITS)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            found = plot_inputs.potential_models(
                pool, potential, M_table, beta_table, n_values, UNITS
            )
        return found, out.getvalue()

    def parameters(self, pool, alpha_values, A_values=(), A_drain_models=()):
        with quiet():
            return plot_inputs.parameters(
                pool,
                abs_tol=1e-8,
                rel_tol=1e-8,
                T_init_GeV=20000,
                phi_init_Mp=5.0,
                units=UNITS,
                tuning_tol=1e-5,
                alpha_values=alpha_values,
                A_values=A_values,
                A_drain_models=A_drain_models,
            )


class TestTheModels(_ReadOnlyCase):
    def test_each_potential_finds_its_own_and_never_the_others(self):
        with self.read_only("mixed") as pool:
            exp, printed = self.models(pool, "exponential", [1])
            self.assertEqual(len(exp), 2)
            for m in exp:
                self.assertIsInstance(m, ExponentialModel)
            self.assertEqual(sorted(m._beta.value for m in exp), [0.5, 1.0])
            # beta = 1 has no power-law model at n = 1: skipped, with one warning naming the class
            ip, printed = self.models(pool, "inverse-power", [1, 2])
            self.assertEqual(len(ip), 2)
            for m in ip:
                self.assertIsInstance(m, InversePowerModel)
            self.assertEqual([(m._n, m._beta.value) for m in ip], [(1, 0.5), (2, 0.5)])
            self.assertIn("no InversePowerModel stored for", printed)
            self.assertIn("2 of 4 InversePowerModel combinations", printed)
            # the two tables' serials repeat each other: each table's first row is serial 1
            self.assertIn(1, {m.store_id for m in exp} & {m.store_id for m in ip})

    def test_exponential_models_is_the_exponentials_call(self):
        with self.read_only("mixed") as pool:
            beta_table, M_table = plot_inputs.stored_grid(pool, UNITS)
            with quiet():
                old = plot_inputs.exponential_models(
                    pool, M_table, beta_table, [1], UNITS
                )
            new, _ = self.models(pool, "exponential", [1])
            self.assertEqual(
                [(type(m), m.store_id) for m in old],
                [(type(m), m.store_id) for m in new],
            )

    def test_a_potential_not_stored_names_models(self):
        with self.read_only("exponential") as pool:
            with self.assertRaises(plot_inputs.PlotOptionNotStored) as ctx:
                self.models(pool, "inverse-power", [1])
            e = ctx.exception
            self.assertEqual(
                (e.class_name, e.option, e.value),
                ("InversePowerModel", "--models", "inverse-power"),
            )
            self.assertIn("--models inverse-power", str(e))
            # the exponential itself is found
            self.assertEqual(len(self.models(pool, "exponential", [1])[0]), 2)

    def test_a_stored_potential_without_an_n_names_n_values(self):
        with self.read_only("mixed") as pool:
            with self.assertRaises(plot_inputs.PlotOptionNotStored) as ctx:
                self.models(pool, "exponential", [1, 2])
            self.assertEqual(
                (ctx.exception.class_name, ctx.exception.option, ctx.exception.value),
                ("ExponentialModel", "--n-values", "2"),
            )
            with self.assertRaises(plot_inputs.PlotOptionNotStored) as ctx:
                self.models(pool, "inverse-power", [1, 3])
            self.assertEqual(
                (ctx.exception.option, ctx.exception.value), ("--n-values", "3")
            )


class TestTheARows(_ReadOnlyCase):
    def test_each_potentials_rows_under_its_own_model(self):
        with self.read_only("mixed") as pool:
            p = self.parameters(pool, [0.0, 0.1], [1.0], [V1, PL])
            self.assertEqual(
                [(a.value, a.drain_model) for a in p.alphas],
                [(0.0, "fixed"), (0.1, "fixed"), (1.0, V1), (1.0, PL)],
            )
            p = self.parameters(pool, [0.0], [1.0], [PL])
            self.assertEqual(
                [(a.value, a.drain_model) for a in p.alphas],
                [(0.0, "fixed"), (1.0, PL)],
            )
            # A by A, each under every model in the order given
            p = self.parameters(pool, [], [1.0], [PL, V1])
            self.assertEqual(
                [(a.value, a.drain_model) for a in p.alphas], [(1.0, PL), (1.0, V1)]
            )

    def test_an_A_stored_only_under_the_other_potentials_model_is_refused(self):
        with self.read_only("mixed") as pool:
            # 0.5 is stored under one-loop-measured-v1 only
            self.assertEqual(
                [
                    (a.value, a.drain_model)
                    for a in self.parameters(pool, [0.0], [0.5], [V1]).alphas
                ],
                [(0.0, "fixed"), (0.5, V1)],
            )
            with self.assertRaises(plot_inputs.PlotOptionNotStored) as ctx:
                self.parameters(pool, [0.0], [0.5], [PL])
            e = ctx.exception
            self.assertEqual((e.class_name, e.option), ("alpha_value", "--A-values"))
            self.assertEqual(e.value, f"0.5 ({PL})")
        with self.read_only("exponential") as pool:
            with self.assertRaises(plot_inputs.PlotOptionNotStored) as ctx:
                self.parameters(pool, [0.0], [1.0], [PL])
            self.assertEqual(ctx.exception.value, f"1 ({PL})")

    def test_A_values_need_a_drain_model(self):
        with self.read_only("mixed") as pool:
            with self.assertRaises(ValueError):
                self.parameters(pool, [0.0], [1.0], [])


if __name__ == "__main__":
    unittest.main()
