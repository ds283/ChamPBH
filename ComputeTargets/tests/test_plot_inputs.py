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

`plot_inputs.py` holds the setup `plot_by_beta.py` and `plot_ScalarModel.py` share. These tests
drive it on stores built through ChamPBH's copy of the package's stand-in cluster
(`StoreFixtures/standin_pool.py`): a real `ShardedPool`, the package's actor code, ChamPBH's
factories and SQLite, in `tempfile` directories, with no Ray. Each store is built read-write with
the lookups `main.py` makes (`main.py:930-1016`, `:1021-1026`, `:1194`, `:1199-1207`, the same
classes and payloads), closed, copied for each test, and reopened `read_only=True` with
`read_table_config=`, as the package's `datastorekit/tests/test_read_only_pool.py` does at
`v0.3.0`. The options are the shared parser's, as a plot run is given them.

1. A store `main.py`'s lookups build (two beta, two M, one Lambda, one potential type): every
   function returns what the store holds, in the scripts' order, and prints no warning.
2. An incomplete M x Lambda product (two runs' grids): the two combinations never stored are
   skipped, each with its warning, then the count; the rest are returned in order (U3).
3. A beta without a coupling (an interrupted run): skipped with its warning (U3).
4. A parameter not stored (`--T-stop-GeV`, `--rel-tol`, and the two constants, `T_CMB` and the
   cosmology): `PlotOptionNotStored`, naming the option or constant and the value (U4).
5. Nothing written: the SHA-256 of every file of each store is unchanged after tests 1-4's calls,
   and no `-journal`, `-wal` or `-shm` file appears.
6. A missing store: the read-only open is refused, and nothing is created at the path.
7. By `ast`, the two scripts: `ShardedPool(read_only=True)` and no `drop_tables`; they call
   `plot_inputs` and define neither `convert_to_potential` nor `convert_to_coupling`; each refuses
   `--drop` before `ray.init`.

Beside them: each lookup's class and payload, key for key, are the scripts' before this prompt
(on a recording pool, for every `--potential-type`); a refusal of another class or operation
propagates unchanged (on a refusing pool).

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
from config.argument_parser import create_argument_parser
from config.datastore import serial_batch_sizes
from config.model_list import build_model_list
from config.sharding import read_table_config
from config.version import VERSION_LABEL

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ("plot_by_beta.py", "plot_ScalarModel.py")

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


def _build_run(
    pool,
    betas,
    Ms_eV,
    Lambdas_eV,
    couplings_for=None,
    T_stop_GeV=None,
):
    """
    One `main.py` run's lookups, read-write: the same classes and payloads as `main.py`. A coupling
    is stored only for the betas in `couplings_for` (all of them if None), as a run interrupted
    between storing a beta and its coupling leaves it.
    """
    args = _args("--potential-type", "Exponential")

    # main.py:930-944
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
    # main.py:1021-1026
    ray.get(
        [
            pool.object_get("tolerance", tol=args.abs_tol),
            pool.object_get("tolerance", tol=args.rel_tol),
        ]
    )
    # main.py:952-970, the beta, M and Lambda helpers
    beta_array = ray.get(
        pool.object_get("beta_value", payload_data=[{"value": b} for b in betas])
    )
    M_array = ray.get(
        pool.object_get(
            "M_value",
            payload_data=[{"value": M * UNITS.eV, "units": UNITS} for M in Ms_eV],
        )
    )
    Lambda_array = ray.get(
        pool.object_get(
            "Lambda_value",
            payload_data=[{"value": L * UNITS.eV, "units": UNITS} for L in Lambdas_eV],
        )
    )
    # main.py:972-1006, over this run's M x Lambda
    ray.get(
        pool.object_get(
            "ExponentialPotential",
            payload_data=[
                {"M": M, "Lambda": L, "n": 1, "units": UNITS}
                for M in M_array
                for L in Lambda_array
            ],
        )
    )
    # main.py:1008-1016
    stored_couplings = [
        b
        for b in beta_array
        if couplings_for is None or any(abs(b.value - c) < 1e-12 for c in couplings_for)
    ]
    if len(stored_couplings) > 0:
        ray.get(
            pool.object_get(
                "ExponentialCoupling",
                payload_data=[
                    {"shard_key": b, "beta": b, "units": UNITS}
                    for b in stored_couplings
                ],
            )
        )
    # main.py:1194, 1199-1207
    for model_data in build_model_list(pool, UNITS):
        if T_stop_GeV is None:
            T_CMB = model_data["cosmology"]._params.T_CMB_Kelvin * UNITS.Kelvin
            ray.get(pool.object_get("temperature", value=T_CMB, units=UNITS))
        else:
            ray.get(
                pool.object_get(
                    "temperature", value=T_stop_GeV * UNITS.GeV, units=UNITS
                )
            )


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


# two beta, two M, one Lambda
FULL = [dict(betas=[0.5, 1.0], Ms_eV=[1.0e25, 1.0e26], Lambdas_eV=[1.0e-2])]

# two runs on different grids: {M1, M2} x {L1}, then {M3} x {L1, L2}. The stored product
# {M1, M2, M3} x {L1, L2} lacks (M1, L2) and (M2, L2)
INCOMPLETE = [
    dict(betas=[0.5, 1.0], Ms_eV=[1.0e25, 1.0e26], Lambdas_eV=[1.0e-2]),
    dict(betas=[0.5, 1.0], Ms_eV=[1.0e27], Lambdas_eV=[1.0e-2, 1.0]),
]

# beta = 1.0 stored, its coupling not
NO_COUPLING = [
    dict(
        betas=[0.5, 1.0, 2.0],
        Ms_eV=[1.0e25],
        Lambdas_eV=[1.0e-2],
        couplings_for=[0.5, 2.0],
    )
]


def setUpModule():
    tmp = tempfile.TemporaryDirectory()
    _MODULE["tmp"] = tmp
    root = Path(tmp.name)
    _MODULE["full"] = _build_store(root / "full", FULL)
    _MODULE["incomplete"] = _build_store(root / "incomplete", INCOMPLETE)
    _MODULE["no_coupling"] = _build_store(root / "no_coupling", NO_COUPLING)


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


def _read_full(pool):
    args = _args("--potential-type", "Exponential")
    with captured() as warnings:
        params = plot_inputs.parameters(
            pool,
            abs_tol=args.abs_tol,
            rel_tol=args.rel_tol,
            T_init_GeV=args.T_init_GeV,
            phi_init_Mp=args.phi_init_Mp,
            units=UNITS,
        )
        grid = plot_inputs.stored_grid(pool, UNITS)
        potentials = plot_inputs.potentials(
            pool, args.potential_type, grid.M, grid.Lambda, UNITS
        )
        couplings = plot_inputs.couplings(pool, grid.beta, UNITS)
        models = plot_inputs.models(pool, UNITS)
        T_stops = [
            plot_inputs.T_stop(pool, args.T_stop_GeV, m["cosmology"], UNITS)
            for m in models
        ]
    return params, grid, potentials, couplings, models, T_stops, warnings


def _read_grid_and_lookups(pool):
    with captured() as warnings:
        grid = plot_inputs.stored_grid(pool, UNITS)
        potentials = plot_inputs.potentials(
            pool, "Exponential", grid.M, grid.Lambda, UNITS
        )
        couplings = plot_inputs.couplings(pool, grid.beta, UNITS)
    return grid, potentials, couplings, warnings


def _miss_T_stop(pool):
    models = plot_inputs.models(pool, UNITS)
    return plot_inputs.T_stop(pool, 1.0e-7, models[0]["cosmology"], UNITS)


def _miss_rel_tol(pool):
    args = _args("--rel-tol", "1e-6")
    return plot_inputs.parameters(
        pool,
        abs_tol=args.abs_tol,
        rel_tol=args.rel_tol,
        T_init_GeV=args.T_init_GeV,
        phi_init_Mp=args.phi_init_Mp,
        units=UNITS,
    )


def _eV(x):
    return x.value / UNITS.eV


# ------------------------------------------------------------------------------------------------
# 1. a store main.py's lookups build
# ------------------------------------------------------------------------------------------------


class TestAStoreMainBuilt(_ReadOnlyCase):
    def test_every_function_returns_what_the_store_holds(self):
        primary = self.copy("full")
        with self.read_only(primary) as pool:
            params, grid, potentials, couplings, models, T_stops, warnings = _read_full(
                pool
            )

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
        for obj in params:
            self.assertTrue(getattr(obj, "_deserialized", False), obj)

        # the stored grid, in ascending order of value
        self.assertEqual([b.value for b in grid.beta], [0.5, 1.0])
        self.assertEqual([round(_eV(M)) for M in grid.M], [1.0e25, 1.0e26])
        self.assertEqual([_eV(L) for L in grid.Lambda], [1.0e-2])

        # the potentials, M outer, and the couplings in beta order: each a stored row
        self.assertEqual(
            [(round(_eV(p._M)), _eV(p._Lambda)) for p in potentials],
            [(1.0e25, 1.0e-2), (1.0e26, 1.0e-2)],
        )
        self.assertEqual(
            [type(p).__name__ for p in potentials], ["ExponentialPotential"] * 2
        )
        self.assertEqual([c._beta.value for c in couplings], [0.5, 1.0])
        for obj in potentials + couplings:
            self.assertTrue(getattr(obj, "_deserialized", False), obj)

        # the cosmology, and T_stop = T_CMB (no --T-stop-GeV)
        self.assertEqual([m["label"] for m in models], ["QCD_Cosmology"])
        self.assertTrue(getattr(models[0]["cosmology"], "_deserialized", False))
        self.assertEqual(len(T_stops), 1)
        self.assertAlmostEqual(
            T_stops[0].value,
            models[0]["cosmology"]._params.T_CMB_Kelvin * UNITS.Kelvin,
        )
        self.assertTrue(getattr(T_stops[0], "_deserialized", False))

    def test_a_given_T_stop_the_build_used_is_found(self):
        directory = self.root / "built-with-T-stop"
        primary = _build_store(directory, [dict(FULL[0], T_stop_GeV=1.0e-7)])
        with self.read_only(primary) as pool:
            models = plot_inputs.models(pool, UNITS)
            T_stop = plot_inputs.T_stop(pool, 1.0e-7, models[0]["cosmology"], UNITS)
        self.assertAlmostEqual(T_stop.value, 1.0e-7 * UNITS.GeV)
        self.assertTrue(getattr(T_stop, "_deserialized", False))


class _RecordingPool:
    """A pool that records every lookup's class and keyword payload, and finds a row for each."""

    def __init__(self):
        self.lookups = []

    def object_get(self, cls, **kwargs):
        self.lookups.append((cls if isinstance(cls, str) else cls.__name__, kwargs))
        return sp.StandinRef(result=len(self.lookups))


class TestThePayloads(_ReadOnlyCase):
    """Each lookup's class and payload, key for key, are the scripts' before this prompt
    (`plot_by_beta.py:1031-1104` at `000be0c`, and `plot_ScalarModel.py`'s identical copy): one
    item of each vectorized payload per lookup."""

    def test_the_parameters(self):
        pool = _RecordingPool()
        plot_inputs.parameters(
            pool,
            abs_tol=1e-8,
            rel_tol=1e-6,
            T_init_GeV=2e4,
            phi_init_Mp=5.0,
            units=UNITS,
        )
        self.assertEqual(
            pool.lookups,
            [
                ("tolerance", {"tol": 1e-8}),
                ("tolerance", {"tol": 1e-6}),
                ("temperature", {"value": 2e4 * UNITS.GeV, "units": UNITS}),
                ("phi_value", {"value": 5.0 * UNITS.PlanckMass, "units": UNITS}),
                ("pi_value", {"value": 0.0, "units": UNITS}),
            ],
        )

    def test_the_potentials(self):
        Ms, Lambdas = ["M1", "M2"], ["L1"]
        with_n = lambda M, L: {"M": M, "Lambda": L, "n": 1, "units": UNITS}
        without_n = lambda M, L: {"M": M, "Lambda": L, "units": UNITS}
        for potential_type, cls_name, payload in (
            ("Exponential", "ExponentialPotential", with_n),
            ("InversePower", "InversePowerPotential", with_n),
            ("Starobinsky", "StarobinskyPotential", without_n),
            ("Recliner", "ReclinerPotential", without_n),
        ):
            with self.subTest(potential_type=potential_type):
                pool = _RecordingPool()
                found = plot_inputs.potentials(pool, potential_type, Ms, Lambdas, UNITS)
                self.assertEqual(
                    pool.lookups,
                    [(cls_name, payload(M, L)) for M in Ms for L in Lambdas],
                )
                self.assertEqual(found, [1, 2])

    def test_the_couplings(self):
        pool = _RecordingPool()
        plot_inputs.couplings(pool, ["b1", "b2"], UNITS)
        self.assertEqual(
            pool.lookups,
            [
                ("ExponentialCoupling", {"shard_key": b, "beta": b, "units": UNITS})
                for b in ["b1", "b2"]
            ],
        )

    def test_T_stop(self):
        cosmology = types.SimpleNamespace(
            _params=types.SimpleNamespace(T_CMB_Kelvin=2.7255)
        )
        pool = _RecordingPool()
        plot_inputs.T_stop(pool, None, cosmology, UNITS)
        plot_inputs.T_stop(pool, 1e-7, cosmology, UNITS)
        self.assertEqual(
            pool.lookups,
            [
                ("temperature", {"value": 2.7255 * UNITS.Kelvin, "units": UNITS}),
                ("temperature", {"value": 1e-7 * UNITS.GeV, "units": UNITS}),
            ],
        )


# ------------------------------------------------------------------------------------------------
# 2. an incomplete M x Lambda product
# ------------------------------------------------------------------------------------------------


class TestAnIncompleteProduct(_ReadOnlyCase):
    def test_the_two_missing_combinations_are_skipped_and_the_rest_returned(self):
        primary = self.copy("incomplete")
        with self.read_only(primary) as pool:
            grid, potentials, couplings, warnings = _read_grid_and_lookups(pool)

        self.assertEqual([round(_eV(M)) for M in grid.M], [1.0e25, 1.0e26, 1.0e27])
        self.assertEqual([_eV(L) for L in grid.Lambda], [1.0e-2, 1.0])
        self.assertEqual(
            [(round(_eV(p._M)), _eV(p._Lambda)) for p in potentials],
            [(1.0e25, 1.0e-2), (1.0e26, 1.0e-2), (1.0e27, 1.0e-2), (1.0e27, 1.0)],
        )
        self.assertEqual([c._beta.value for c in couplings], [0.5, 1.0])
        self.assertEqual(
            warnings,
            [
                "!! plot: no ExponentialPotential stored for M=1e+25 eV, Λ=1 eV; no history "
                "can use it; skipped",
                "!! plot: no ExponentialPotential stored for M=1e+26 eV, Λ=1 eV; no history "
                "can use it; skipped",
                "!! plot: 2 of 6 ExponentialPotential combinations of the stored M and Λ are "
                "not stored, and were skipped",
            ],
        )


# ------------------------------------------------------------------------------------------------
# 3. a beta without a coupling
# ------------------------------------------------------------------------------------------------


class TestABetaWithoutACoupling(_ReadOnlyCase):
    def test_it_is_skipped_with_its_warning(self):
        primary = self.copy("no_coupling")
        with self.read_only(primary) as pool:
            grid, potentials, couplings, warnings = _read_grid_and_lookups(pool)

        self.assertEqual([b.value for b in grid.beta], [0.5, 1.0, 2.0])
        self.assertEqual([c._beta.value for c in couplings], [0.5, 2.0])
        self.assertEqual(len(potentials), 1)
        self.assertEqual(
            warnings,
            [
                "!! plot: no ExponentialCoupling stored for β=1; no history can use it; "
                "skipped",
                "!! plot: 1 of 3 stored β have no ExponentialCoupling, and were skipped",
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


def _delete_T_CMB(case, primary: Path):
    """Delete the T_CMB temperature row from every shard of `primary` (a test's own copy)."""
    with case.read_only(primary) as pool:
        with quiet():
            cosmology = plot_inputs.models(pool, UNITS)[0]["cosmology"]
            T_CMB = plot_inputs.T_stop(pool, None, cosmology, UNITS)
    _delete_everywhere(
        primary, f"DELETE FROM temperature WHERE serial = {T_CMB.store_id}"
    )


def _delete_cosmology(case, primary: Path):
    """Delete the QCD_Cosmology row from every shard of `primary` (a test's own copy)."""
    _delete_everywhere(primary, "DELETE FROM QCD_Cosmology")


def _miss_T_CMB(pool):
    models = plot_inputs.models(pool, UNITS)
    return plot_inputs.T_stop(pool, None, models[0]["cosmology"], UNITS)


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

    def test_a_T_stop_the_build_did_not_use(self):
        self.assert_stops(
            self.copy("full"),
            _miss_T_stop,
            "temperature",
            "!! plot: the store holds no temperature for --T-stop-GeV 1e-07; main.py was not "
            "run with it, so no history in this store can match. Give the plot the options the "
            "build used",
        )

    def test_a_rel_tol_the_build_did_not_use(self):
        self.assert_stops(
            self.copy("full"),
            _miss_rel_tol,
            "tolerance",
            "!! plot: the store holds no tolerance for --rel-tol 1e-06; main.py was not run "
            "with it, so no history in this store can match. Give the plot the options the "
            "build used",
        )

    def test_T_CMB_not_stored_names_the_constant(self):
        primary = self.copy("full")
        _delete_T_CMB(self, primary)
        self.assert_stops(
            primary,
            _miss_T_CMB,
            "temperature",
            "!! plot: the store holds no temperature for the cosmology's constant T_CMB "
            "2.7255 K (the T_stop used when --T-stop-GeV is not given); main.py was not run "
            "with it, so no history in this store can match. Give the plot the options the "
            "build used",
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
            plot_inputs.parameters(_RefusingPool(error), 1e-8, 1e-8, 2e4, 5.0, UNITS)
        self.assertNotIsInstance(ctx.exception, plot_inputs.PlotOptionNotStored)
        self.assertEqual(ctx.exception.class_name, "version")

    def test_a_potential_lookup_refused_for_another_class(self):
        primary = self.copy("full")
        with self.read_only(primary) as pool:
            grid = plot_inputs.stored_grid(pool, UNITS)
        error = ReadOnlyMiss("M_value", {"value": 1.0})
        with quiet(), self.assertRaises(ReadOnlyMiss) as ctx:
            plot_inputs.potentials(
                _RefusingPool(error), "Exponential", grid.M, grid.Lambda, UNITS
            )
        self.assertEqual(ctx.exception.class_name, "M_value")

    def test_a_coupling_lookup_refused_as_another_write(self):
        primary = self.copy("full")
        with self.read_only(primary) as pool:
            grid = plot_inputs.stored_grid(pool, UNITS)
        for error in (
            ReadOnlyWrite("shard0000", "object_get", class_name="ExponentialCoupling"),
            ReadOnlyWrite("shard0000", "an insert", class_name="ScalarModel"),
        ):
            with self.subTest(operation=error.operation, class_name=error.class_name):
                with quiet(), self.assertRaises(ReadOnlyWrite) as ctx:
                    plot_inputs.couplings(_RefusingPool(error), grid.beta, UNITS)
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
            plot_inputs.potentials(None, None, [], [], UNITS)
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
            ("no_coupling", nothing, _read_grid_and_lookups),
            ("full", nothing, _miss_T_stop),
            ("full", nothing, _miss_rel_tol),
            ("full", _delete_T_CMB, _miss_T_CMB),
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
                    "potentials",
                    "couplings",
                    "models",
                    "T_stop",
                ):
                    self.assertIn(f"plot_inputs.{name}", called)
                self.assertNotIn("build_model_list", called)
                defined = {
                    n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                }
                self.assertNotIn("convert_to_potential", defined)
                self.assertNotIn("convert_to_coupling", defined)
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
