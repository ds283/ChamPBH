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
What the plot scripts read before the histories, on read-only stores (read-only-plots prompt 02;
README P1, P2, P3, U3, U4).

`plot_inputs.py` holds the setup `plot_by_beta.py` and `plot_ScalarHistory.py` share. These tests
drive it on stores built through ChamPBH's copy of the package's stand-in cluster
(`StoreFixtures/standin_pool.py`): a real `ShardedPool`, the package's actor code, ChamPBH's
factories and SQLite, in `tempfile` directories, with no Ray. Each store is built read-write with
the lookups `main.py` makes (its `execute()`: the same classes and payloads), closed, copied for
each test, and reopened `read_only=True` with
`read_table_config=`, as the package's `datastorekit/tests/test_read_only_pool.py` does at
`v0.3.0`. The options are the shared parser's, as a plot run is given them.

1. A store `main.py`'s lookups build (two beta, two M, one potential type): every function
   returns what the store holds, in the scripts' order, and prints no warning.
2. An incomplete M x beta product (two runs' grids): the two models never stored are skipped,
   each with its warning, then the count; the rest are returned in order (U3).
3. A beta without a model (an interrupted run): skipped with its warning (U3).
4. A parameter not stored (`--rel-tol`, the constant tuning tolerance, and the constant
   cosmology): `PlotOptionNotStored`, naming the option or constant and the value (U4).
5. Nothing written: the SHA-256 of every file of each store is unchanged after tests 1-4's calls,
   and no `-journal`, `-wal` or `-shm` file appears.
6. A missing store: the read-only open is refused, and nothing is created at the path.
7. By `ast`, the two scripts: `ShardedPool(read_only=True)` and no `drop_tables`; they call
   `plot_inputs` and define none of `convert_to_potential`, `convert_to_coupling` and
   `convert_to_model`; each refuses `--drop` before `ray.init`.

Beside them: each lookup's class and payload, key for key, are main.py's (on a recording pool,
for every `--potential-type`); a refusal of another class or operation propagates unchanged (on a
refusing pool).

Since jordan-normalization prompt 07 a history is keyed by its model (an `ExponentialModel` of M,
n and beta), its initial data and the tuning tolerance, and ends at T_CMB, so the store holds no
Lambda, potential, coupling or final temperature: `plot_inputs.exponential_models` replaced
`potentials` and `couplings`, and `T_stop` went. The tests of `couplings` and of `T_stop` went with
them (the log of that prompt names each).

No test runs a PRyMordial solve, and none starts Ray. About 5 s.
"""

import ast
import contextlib
import io
import shutil
import tempfile
import types
import unittest
from pathlib import Path

import ray
from datastorekit.replication import ReadOnlyMiss, ReadOnlyWrite

import plot_inputs
from StoreFixtures import standin_pool as sp
from Units import Planck_units
from ComputeTargets.ScalarHistory import DEFAULT_TUNING_TOLERANCE
from config.argument_parser import create_argument_parser
from config.datastore import serial_batch_sizes
from config.model_list import build_model_list
from config.sharding import read_table_config
from config.version import VERSION_LABEL

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ("plot_by_beta.py", "plot_ScalarHistory.py")

UNITS = Planck_units()

_MODULE = {}


def _args(*extra):
    """The shared parser's options, as a plot run is given them (prompt 01's switch included)."""
    return create_argument_parser().parse_args(
        ["--database", "unused.db", "--ray-address", "127.0.0.1:6380", *extra]
    )


@contextlib.contextmanager
def quiet():
    with (
        contextlib.redirect_stdout(io.StringIO()),
        contextlib.redirect_stderr(io.StringIO()),
    ):
        yield


@contextlib.contextmanager
def captured():
    """The `!! plot:` lines printed inside the block (the cosmology's own output is dropped)."""
    out = io.StringIO()
    lines = []
    with contextlib.redirect_stdout(out):
        yield lines
    lines.extend(l for l in out.getvalue().splitlines() if l.startswith("!! plot:"))


def _build_run(pool, betas, Ms_eV, models_for=None):
    """
    One `main.py` run's lookups, read-write: the same classes and payloads as `main.py`. A model is
    stored only for the betas in `models_for` (all of them if None), as a run interrupted between
    storing a beta and its models leaves it.
    """
    args = _args("--potential-type", "Exponential")

    # main.py, execute(): T_init, phi_init and pi_init
    ray.get(
        pool.object_get("temperature", value=args.T_init_GeV * UNITS.GeV, units=UNITS)
    )
    ray.get(
        [
            pool.object_get(
                "phi_value", value=args.phi_init_Mp * UNITS.PlanckMass, units=UNITS
            ),
            pool.object_get("pi_value", value=0.0, units=UNITS),
        ]
    )
    # the tolerances, and the tuning tolerance
    ray.get(
        [
            pool.object_get("tolerance", tol=args.abs_tol),
            pool.object_get("tolerance", tol=args.rel_tol),
            pool.object_get("tolerance", tol=DEFAULT_TUNING_TOLERANCE),
        ]
    )
    # the beta and M helpers
    beta_array = ray.get(
        pool.object_get("beta_value", payload_data=[{"value": b} for b in betas])
    )
    M_array = ray.get(
        pool.object_get(
            "M_value",
            payload_data=[{"value": M * UNITS.eV, "units": UNITS} for M in Ms_eV],
        )
    )
    # convert_to_model, over this run's M x beta, M outer
    stored_betas = [
        b
        for b in beta_array
        if models_for is None or any(abs(b.value - c) < 1e-12 for c in models_for)
    ]
    if len(stored_betas) > 0:
        ray.get(
            pool.object_get(
                "ExponentialModel",
                payload_data=[
                    {"shard_key": b, "M": M, "n": 1, "beta": b, "units": UNITS}
                    for M in M_array
                    for b in stored_betas
                ],
            )
        )
    # the cosmology (build_model_list)
    build_model_list(pool, UNITS)


def _build_store(directory: Path, runs) -> Path:
    """A store built by one `main.py` run per entry of `runs`, closed; its primary."""
    directory.mkdir(parents=True)
    primary = directory / "store.sqlite"
    cluster = sp.StandinCluster()
    with cluster.active(), quiet():
        for run in runs:
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


# two beta, two M
FULL = [dict(betas=[0.5, 1.0], Ms_eV=[1.0e25, 1.0e26])]

# two runs on different grids: {M1, M2} x {b1, b2}, then {M3} x {b1, b2, b3}. The stored
# product {M1, M2, M3} x {b1, b2, b3} lacks (M1, b3) and (M2, b3)
INCOMPLETE = [
    dict(betas=[0.5, 1.0], Ms_eV=[1.0e25, 1.0e26]),
    dict(betas=[0.5, 1.0, 2.0], Ms_eV=[1.0e27]),
]

# beta = 1.0 stored, its model not
NO_MODEL = [dict(betas=[0.5, 1.0, 2.0], Ms_eV=[1.0e25], models_for=[0.5, 2.0])]


def setUpModule():
    tmp = tempfile.TemporaryDirectory()
    _MODULE["tmp"] = tmp
    root = Path(tmp.name)
    _MODULE["full"] = _build_store(root / "full", FULL)
    _MODULE["incomplete"] = _build_store(root / "incomplete", INCOMPLETE)
    _MODULE["no_model"] = _build_store(root / "no_model", NO_MODEL)


def tearDownModule():
    _MODULE["tmp"].cleanup()
    if ray.is_initialized():
        raise AssertionError("Ray was initialised by test_plot_inputs")


class _ReadOnlyCase(unittest.TestCase):
    """A fresh copy of a module store per open, and the stand-in cluster active."""

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

    def copy(self, name: str) -> Path:
        """A copy of the module store `name`, alone in its own directory; its primary."""
        self._copies += 1
        d = self.root / f"copy-{self._copies}"
        shutil.copytree(_MODULE[name].parent, d)
        return d / "store.sqlite"

    @contextlib.contextmanager
    def read_only(self, primary: Path):
        """`primary` opened as the plot scripts open it: read-only, with `read_table_config=`."""
        with quiet():
            pool = self.cluster.open_pool(
                primary,
                version_label=VERSION_LABEL,
                read_only=True,
                read_table_config=read_table_config,
                serial_batch_sizes=serial_batch_sizes,
            )
        try:
            yield pool
        finally:
            self.cluster.close_pool(pool)


# each test's calls, shared with test 5


def _parameters(pool, args, tuning_tol=DEFAULT_TUNING_TOLERANCE):
    return plot_inputs.parameters(
        pool,
        abs_tol=args.abs_tol,
        rel_tol=args.rel_tol,
        T_init_GeV=args.T_init_GeV,
        phi_init_Mp=args.phi_init_Mp,
        units=UNITS,
        tuning_tol=tuning_tol,
    )


def _read_full(pool):
    args = _args("--potential-type", "Exponential")
    with captured() as warnings:
        params = _parameters(pool, args)
        grid = plot_inputs.stored_grid(pool, UNITS)
        models = plot_inputs.exponential_models(
            pool, args.potential_type, grid.M, grid.beta, UNITS
        )
        cosmologies = plot_inputs.models(pool, UNITS)
    return params, grid, models, cosmologies, warnings


def _read_grid_and_lookups(pool):
    with captured() as warnings:
        grid = plot_inputs.stored_grid(pool, UNITS)
        models = plot_inputs.exponential_models(
            pool, "Exponential", grid.M, grid.beta, UNITS
        )
    return grid, models, warnings


def _miss_rel_tol(pool):
    return _parameters(pool, _args("--rel-tol", "1e-6"))


def _miss_tuning_tol(pool):
    return _parameters(pool, _args(), tuning_tol=1.0e-6)


def _eV(x):
    return x.value / UNITS.eV


def _M_n_beta(models):
    return [(round(_eV(m._M)), m._n, m._beta.value) for m in models]


# ------------------------------------------------------------------------------------------------
# 1. a store main.py's lookups build
# ------------------------------------------------------------------------------------------------


class TestAStoreMainBuilt(_ReadOnlyCase):
    def test_every_function_returns_what_the_store_holds(self):
        primary = self.copy("full")
        with self.read_only(primary) as pool:
            params, grid, models, cosmologies, warnings = _read_full(pool)

        self.assertEqual(warnings, [])

        args = _args()
        # the parameters: each the stored row (a fresh serial would mean an insert)
        self.assertAlmostEqual(params.atol.log10_tol, -8.0)
        self.assertAlmostEqual(params.rtol.log10_tol, -8.0)
        self.assertAlmostEqual(params.T_init.value, args.T_init_GeV * UNITS.GeV)
        self.assertAlmostEqual(
            params.phi_init.value, args.phi_init_Mp * UNITS.PlanckMass
        )
        self.assertEqual(params.pi_init.value, 0.0)
        self.assertAlmostEqual(params.tuning_tol.log10_tol, -5.0)
        for obj in params:
            self.assertTrue(getattr(obj, "_deserialized", False), obj)

        # the stored grid, in ascending order of value
        self.assertEqual([b.value for b in grid.beta], [0.5, 1.0])
        self.assertEqual([round(_eV(M)) for M in grid.M], [1.0e25, 1.0e26])

        # the models, M outer and beta inner, n = 1: each a stored row
        self.assertEqual(
            _M_n_beta(models),
            [(1.0e25, 1, 0.5), (1.0e25, 1, 1.0), (1.0e26, 1, 0.5), (1.0e26, 1, 1.0)],
        )
        self.assertEqual([type(m).__name__ for m in models], ["ExponentialModel"] * 4)
        for obj in models:
            self.assertTrue(getattr(obj, "_deserialized", False), obj)

        # the cosmology
        self.assertEqual([m["label"] for m in cosmologies], ["QCD_Cosmology"])
        self.assertTrue(getattr(cosmologies[0]["cosmology"], "_deserialized", False))


class _RecordingPool:
    """A pool that records every lookup's class and keyword payload, and finds a row for each."""

    def __init__(self):
        self.lookups = []

    def object_get(self, cls, **kwargs):
        self.lookups.append((cls if isinstance(cls, str) else cls.__name__, kwargs))
        return sp.StandinRef(result=len(self.lookups))


class TestThePayloads(_ReadOnlyCase):
    """Each lookup's class and payload, key for key, are main.py's: one item of each vectorized
    payload per lookup."""

    def test_the_parameters(self):
        pool = _RecordingPool()
        plot_inputs.parameters(
            pool,
            abs_tol=1e-8,
            rel_tol=1e-6,
            T_init_GeV=2e4,
            phi_init_Mp=5.0,
            units=UNITS,
            tuning_tol=1e-5,
        )
        self.assertEqual(
            pool.lookups,
            [
                ("tolerance", {"tol": 1e-8}),
                ("tolerance", {"tol": 1e-6}),
                ("temperature", {"value": 2e4 * UNITS.GeV, "units": UNITS}),
                ("phi_value", {"value": 5.0 * UNITS.PlanckMass, "units": UNITS}),
                ("pi_value", {"value": 0.0, "units": UNITS}),
                ("tolerance", {"tol": 1e-5}),
            ],
        )

    def test_the_models(self):
        Ms, betas = ["M1", "M2"], ["b1", "b2"]
        payload = lambda M, b: {
            "shard_key": b,
            "M": M,
            "n": 1,
            "beta": b,
            "units": UNITS,
        }
        for potential_type, cls_name in (("Exponential", "ExponentialModel"),):
            with self.subTest(potential_type=potential_type):
                pool = _RecordingPool()
                found = plot_inputs.exponential_models(
                    pool, potential_type, Ms, betas, UNITS
                )
                self.assertEqual(
                    pool.lookups,
                    [(cls_name, payload(M, b)) for M in Ms for b in betas],
                )
                self.assertEqual(found, [1, 2, 3, 4])

        # jordan-normalization prompt 01: the other three types were removed (R11) and are refused
        for potential_type in ("InversePower", "Starobinsky", "Recliner"):
            with self.subTest(removed=potential_type):
                pool = _RecordingPool()
                with self.assertRaises(ValueError) as caught:
                    plot_inputs.exponential_models(
                        pool, potential_type, Ms, betas, UNITS
                    )
                self.assertEqual(
                    str(caught.exception), f"Unknown potential type: {potential_type}"
                )
                self.assertEqual(pool.lookups, [])


# ------------------------------------------------------------------------------------------------
# 2. an incomplete M x beta product
# ------------------------------------------------------------------------------------------------


class TestAnIncompleteProduct(_ReadOnlyCase):
    def test_the_two_missing_combinations_are_skipped_and_the_rest_returned(self):
        primary = self.copy("incomplete")
        with self.read_only(primary) as pool:
            grid, models, warnings = _read_grid_and_lookups(pool)

        self.assertEqual([round(_eV(M)) for M in grid.M], [1.0e25, 1.0e26, 1.0e27])
        self.assertEqual([b.value for b in grid.beta], [0.5, 1.0, 2.0])
        self.assertEqual(
            _M_n_beta(models),
            [
                (1.0e25, 1, 0.5),
                (1.0e25, 1, 1.0),
                (1.0e26, 1, 0.5),
                (1.0e26, 1, 1.0),
                (1.0e27, 1, 0.5),
                (1.0e27, 1, 1.0),
                (1.0e27, 1, 2.0),
            ],
        )
        self.assertEqual(
            warnings,
            [
                "!! plot: no ExponentialModel stored for M=1e+25 eV, n=1, β=2; no history "
                "can use it; skipped",
                "!! plot: no ExponentialModel stored for M=1e+26 eV, n=1, β=2; no history "
                "can use it; skipped",
                "!! plot: 2 of 9 ExponentialModel combinations of the stored M and β are not "
                "stored, and were skipped",
            ],
        )


# ------------------------------------------------------------------------------------------------
# 3. a beta without a model
# ------------------------------------------------------------------------------------------------


class TestABetaWithoutAModel(_ReadOnlyCase):
    def test_it_is_skipped_with_its_warning(self):
        primary = self.copy("no_model")
        with self.read_only(primary) as pool:
            grid, models, warnings = _read_grid_and_lookups(pool)

        self.assertEqual([b.value for b in grid.beta], [0.5, 1.0, 2.0])
        self.assertEqual([m._beta.value for m in models], [0.5, 2.0])
        self.assertEqual(
            warnings,
            [
                "!! plot: no ExponentialModel stored for M=1e+25 eV, n=1, β=1; no history "
                "can use it; skipped",
                "!! plot: 1 of 3 ExponentialModel combinations of the stored M and β are not "
                "stored, and were skipped",
            ],
        )


# ------------------------------------------------------------------------------------------------
# 4. a parameter not stored
# ------------------------------------------------------------------------------------------------


def _delete_everywhere(primary: Path, sql: str):
    """Run `sql` on every shard of the store `primary` (a test's own copy)."""
    import sqlite3

    for path in sorted(primary.parent.glob("store-shard*.sqlite")):
        conn = sqlite3.connect(path)
        try:
            conn.execute(sql)
            conn.commit()
        finally:
            conn.close()


def _delete_cosmology(case, primary: Path):
    """Delete the QCD_Cosmology row from every shard of `primary` (a test's own copy)."""
    _delete_everywhere(primary, "DELETE FROM QCD_Cosmology")


def _miss_cosmology(pool):
    return plot_inputs.models(pool, UNITS)


class TestAParameterNotStored(_ReadOnlyCase):
    def assert_stops(self, primary, run, class_name, message):
        with self.read_only(primary) as pool:
            with quiet(), self.assertRaises(plot_inputs.PlotOptionNotStored) as ctx:
                run(pool)
        e = ctx.exception
        self.assertEqual(str(e), message)
        self.assertEqual(e.class_name, class_name)
        self.assertIsInstance(e.__cause__, ReadOnlyMiss)
        self.assertEqual(e.__cause__.class_name, class_name)
        return e

    def test_a_rel_tol_the_build_did_not_use(self):
        self.assert_stops(
            self.copy("full"),
            _miss_rel_tol,
            "tolerance",
            "!! plot: the store holds no tolerance for --rel-tol 1e-06; main.py was not run "
            "with it, so no history in this store can match. Give the plot the options the "
            "build used",
        )

    def test_a_tuning_tolerance_the_build_did_not_use_names_the_constant(self):
        self.assert_stops(
            self.copy("full"),
            _miss_tuning_tol,
            "tolerance",
            "!! plot: the store holds no tolerance for the constant tuning tolerance 1e-06; "
            "main.py was not run with it, so no history in this store can match. Give the "
            "plot the options the build used",
        )

    def test_the_cosmology_not_stored_names_the_constant(self):
        primary = self.copy("full")
        _delete_cosmology(self, primary)
        self.assert_stops(
            primary,
            _miss_cosmology,
            "QCD_Cosmology",
            "!! plot: the store holds no QCD_Cosmology for the constant cosmology Planck2018 "
            "of config/model_list.py; main.py was not run with it, so no history in this store "
            "can match. Give the plot the options the build used",
        )


class _RefusingPool:
    """A pool whose every lookup is refused with `error`, as the stand-in's ref would raise it."""

    def __init__(self, error):
        self.error = error

    def object_get(self, *args, **kwargs):
        return sp.StandinRef(error=self.error)


class TestOtherRefusalsPropagate(_ReadOnlyCase):
    """Only the refusal each lookup expects is handled; any other propagates unchanged."""

    def test_a_parameter_lookup_refused_for_another_class(self):
        error = ReadOnlyMiss("version", {"label": "x"})
        with self.assertRaises(ReadOnlyMiss) as ctx:
            plot_inputs.parameters(
                _RefusingPool(error), 1e-8, 1e-8, 2e4, 5.0, UNITS, 1e-5
            )
        self.assertNotIsInstance(ctx.exception, plot_inputs.PlotOptionNotStored)
        self.assertEqual(ctx.exception.class_name, "version")

    def test_a_model_lookup_refused_for_another_class(self):
        primary = self.copy("full")
        with self.read_only(primary) as pool:
            grid = plot_inputs.stored_grid(pool, UNITS)
        error = ReadOnlyMiss("M_value", {"value": 1.0})
        with quiet(), self.assertRaises(ReadOnlyMiss) as ctx:
            plot_inputs.exponential_models(
                _RefusingPool(error), "Exponential", grid.M, grid.beta, UNITS
            )
        self.assertEqual(ctx.exception.class_name, "M_value")

    def test_a_model_lookup_refused_as_another_write(self):
        primary = self.copy("full")
        with self.read_only(primary) as pool:
            grid = plot_inputs.stored_grid(pool, UNITS)
        for error in (
            ReadOnlyWrite("shard0000", "object_get", class_name="ExponentialModel"),
            ReadOnlyWrite("shard0000", "an insert", class_name="ScalarHistory"),
        ):
            with self.subTest(operation=error.operation, class_name=error.class_name):
                with quiet(), self.assertRaises(ReadOnlyWrite) as ctx:
                    plot_inputs.exponential_models(
                        _RefusingPool(error), "Exponential", grid.M, grid.beta, UNITS
                    )
                self.assertEqual(
                    (
                        type(ctx.exception),
                        ctx.exception.operation,
                        ctx.exception.class_name,
                    ),
                    (ReadOnlyWrite, error.operation, error.class_name),
                )

    def test_an_unknown_potential_type(self):
        with self.assertRaises(ValueError) as ctx:
            plot_inputs.exponential_models(None, None, [], [], UNITS)
        self.assertEqual(str(ctx.exception), "Unknown potential type: None")


# ------------------------------------------------------------------------------------------------
# 5. nothing written
# ------------------------------------------------------------------------------------------------


class TestNothingWritten(_ReadOnlyCase):
    def test_every_file_unchanged_after_tests_1_to_4s_calls(self):
        def nothing(case, primary):
            pass

        cases = [
            ("full", nothing, _read_full),
            ("incomplete", nothing, _read_grid_and_lookups),
            ("no_model", nothing, _read_grid_and_lookups),
            ("full", nothing, _miss_rel_tol),
            ("full", nothing, _miss_tuning_tol),
            ("full", _delete_cosmology, _miss_cosmology),
        ]
        for name, prepare, run in cases:
            with self.subTest(store=name, run=run.__name__):
                primary = self.copy(name)
                prepare(self, primary)
                before = sp.store_checksums(primary)
                self.assertEqual(
                    sorted(before),
                    ["store-shard0000.sqlite", "store-shard0001.sqlite"]
                    + ["store-shard0002.sqlite", "store.sqlite"],
                )
                with self.read_only(primary) as pool:
                    with quiet():
                        try:
                            run(pool)
                        except plot_inputs.PlotOptionNotStored:
                            pass
                    # while the pool is open, too: no journal beside any file
                    self.assertEqual(sp.store_checksums(primary), before)
                after = sp.store_checksums(primary)
                self.assertEqual(after, before)
                self.assertEqual(
                    [
                        p.name
                        for p in primary.parent.iterdir()
                        if p.name.endswith(("-journal", "-wal", "-shm"))
                    ],
                    [],
                )


# ------------------------------------------------------------------------------------------------
# 6. a missing store
# ------------------------------------------------------------------------------------------------


class TestAMissingStore(_ReadOnlyCase):
    def test_the_open_is_refused_and_nothing_is_created(self):
        missing = self.root / "nowhere" / "deeper" / "store.sqlite"
        with self.assertRaises(RuntimeError) as ctx:
            with self.read_only(missing):
                pass
        self.assertIn("does not exist", str(ctx.exception))
        self.assertFalse((self.root / "nowhere").exists())
        self.assertEqual(self.cluster.calls, [])


# ------------------------------------------------------------------------------------------------
# 7. the two scripts, by ast (each parses sys.argv and starts Ray at import)
# ------------------------------------------------------------------------------------------------


def _tree(script):
    return ast.parse((REPO_ROOT / script).read_text(), script)


def _is_call(node, dotted):
    return (
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and ast.unparse(node.value.func) == dotted
    )


class TestTheScripts(unittest.TestCase):
    def test_the_pool_is_read_only_with_no_drop_tables(self):
        for script in SCRIPTS:
            with self.subTest(script=script):
                calls = [
                    n
                    for n in ast.walk(_tree(script))
                    if isinstance(n, ast.Call) and ast.unparse(n.func) == "ShardedPool"
                ]
                self.assertEqual(len(calls), 1)
                keywords = {kw.arg: kw.value for kw in calls[0].keywords}
                self.assertIn("read_only", keywords)
                self.assertIs(keywords["read_only"].value, True)
                self.assertIs(keywords["prune_unvalidated"].value, False)
                self.assertNotIn("drop_tables", keywords)
                self.assertNotIn("shards", keywords)
                self.assertEqual(keywords["job_name"].value, Path(script).stem)

    def test_the_setup_is_plot_inputs(self):
        for script in SCRIPTS:
            with self.subTest(script=script):
                tree = _tree(script)
                called = {
                    ast.unparse(n.func)
                    for n in ast.walk(tree)
                    if isinstance(n, ast.Call)
                }
                for name in (
                    "parameters",
                    "stored_grid",
                    "exponential_models",
                    "models",
                ):
                    self.assertIn(f"plot_inputs.{name}", called)
                self.assertNotIn("build_model_list", called)
                defined = {
                    n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                }
                self.assertNotIn("convert_to_potential", defined)
                self.assertNotIn("convert_to_coupling", defined)
                self.assertNotIn("convert_to_model", defined)
                handlers = [
                    ast.unparse(h.type)
                    for n in ast.walk(tree)
                    if isinstance(n, ast.Try)
                    for h in n.handlers
                ]
                self.assertIn("plot_inputs.PlotOptionNotStored", handlers)

    def test_drop_is_refused_before_ray_init(self):
        for script in SCRIPTS:
            with self.subTest(script=script):
                body = _tree(script).body
                inits = [i for i, n in enumerate(body) if _is_call(n, "ray.init")]
                self.assertEqual(len(inits), 1)
                refusals = [
                    i
                    for i, n in enumerate(body)
                    if isinstance(n, ast.If) and "args.drop" in ast.unparse(n.test)
                ]
                self.assertEqual(len(refusals), 1)
                self.assertLess(refusals[0], inits[0])
                guard = body[refusals[0]]
                self.assertEqual(ast.unparse(guard.body[-1]), "sys.exit(2)")
                self.assertIn("--drop", ast.unparse(guard))
                # before prompt 01's check, which stays immediately before its guard
                checks = [
                    i
                    for i, n in enumerate(body)
                    if isinstance(n, ast.Assign)
                    and "ray_address_problem" in ast.unparse(n.value)
                ]
                self.assertEqual(len(checks), 1)
                self.assertLess(refusals[0], checks[0])


if __name__ == "__main__":
    unittest.main()
