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
The tool `tools/bbn_from_store.py` (bbn-tolerance prompt 01, README section
2 (b)): its ratio grid is bitwise the one production builds, its tolerance
override changes the low-T solve_ivp call and no other, and its store lookup
is read-only.

Written for bbn-tolerance prompt 01 (README section 6.1). The tool is new, so
none of these can fail on the tree before it except by ImportError; the
stand-in for a breakage test is the prompt's reproduction of the stored
outcomes (log 01, Verification).

Test (a) runs no solve (`build_rho_NP_callback` is intercepted). **Test (b)
runs two small-network PRyMordial solves, about 10 s.** Test (c) builds a
temporary SQLite file imitating the store's tables; no test reads a real
store. Run from the repository root, since PRyMordial reads `PRyMrates/` from
the working directory:

    PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .
"""

import hashlib
import os
import sqlite3
import sys
import tempfile
import unittest
from math import log, pi, sin, sqrt
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import numpy as np

import PRyM.PRyM_main as PRyMmain
from ComputeTargets.BBNData import compute_BBN_data, compute_SM_baseline
from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests.prym_fixtures import SavedPRyMGlobals
from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import (
    SaikawaShirai_EOS_spline,
)
from Units import Planck_units

# (a) the synthetic history: samples from 10^2.5 MeV down to 10^-4.5 MeV in
# T_Jordan, jittered, of which those in [0.2 keV, 100 MeV] are in the window
A_N_SAMPLES = 80
A_LOG10_T_MEV_HI = 2.5
A_LOG10_T_MEV_LO = -4.5
A_MIN_IN_WINDOW = 50

# (b) the override's rtol, and the low-T call's arguments as PRyMordial passes
# them on this tree (4ae25b4 through 95da274): no rtol, so SciPy's 1e-3, and
# atol 1e-11 for the small network. A prompt that patches the call re-pins
# LOWT_SMALL_RTOL_AS_PASSED (README section 0.2 P7); None means "not passed".
OVERRIDE_RTOL = 1e-6
LOWT_SMALL_RTOL_AS_PASSED = None
LOWT_SMALL_ATOL_AS_PASSED = 1e-11

# (b) the stages PRyMordial solves, in order, under production's flags with the
# small network (test_bbn_solver_failures: N_CALLS = 5)
SMALL_NETWORK_STAGES = (
    "thermodynamics (no NP)",
    "a(T)",
    "high-T n <-> p",
    "mid-T nuclear network (small)",
    "low-T nuclear network (small)",
)

_RECORDED = "recorded (test_bbn_from_store)"


def _tool():
    """The tool module, imported inside the tests so that on a tree without it
    each test fails on its own rather than the module failing to import."""
    import tools.bbn_from_store as tool

    return tool


def _synthetic_samples(units):
    """
    (store rows, stand-in values): each sample's store row in the store's units,
    formed as sqla_ScalarModelValue_factory.build writes it, and the
    ScalarModelValue that sqla_ScalarModel_factory.build reads back from that
    row (its read path), which is what compute_BBN_data iterates over.
    """
    log_Mp = log(units.PlanckMass)
    log_GeV = log(units.GeV)
    M_P2 = units.PlanckMass * units.PlanckMass

    rows, values = [], []
    step = (A_LOG10_T_MEV_LO - A_LOG10_T_MEV_HI) / (A_N_SAMPLES - 1)
    for i in range(A_N_SAMPLES):
        lt = A_LOG10_T_MEV_HI + step * i + 0.01 * sin(1.3 * i)
        T = 10.0**lt * units.MeV
        g = 10.0 + sin(0.37 * i)
        rho_R = (pi * pi / 30.0) * g * T**4
        f_m = 1e-3 * (units.MeV / T) ** 0.5
        r = 0.05 + 0.01 * sin(0.7 * i)
        H_J = sqrt(rho_R * (1.0 + f_m + r) / (3.0 * M_P2))

        # the factory's write path
        row = (
            0.1 * i,
            log(T) - log_GeV,
            H_J / units.PlanckMass,
            log(rho_R) - 4.0 * log_Mp,
            log(f_m),
        )
        rows.append(row)

        # the factory's read path
        values.append(
            SimpleNamespace(
                z=SimpleNamespace(store_id=i),
                raw_N=row[0],
                log_T_Jordan=row[1] + log_GeV,
                H_Jordan=row[2] * units.PlanckMass,
                log_rhorad_Jordan=row[3] + 4.0 * log_Mp,
                log_fm=row[4],
            )
        )
    return rows, values


class _RecordingSolveIVP:
    """Stands in for PRyM_main.solve_ivp: records each call and passes it on."""

    def __init__(self):
        self._solve_ivp = PRyMmain.solve_ivp
        self.calls = []

    def __call__(self, fun, t_span, y0, **kwargs):
        self.calls.append((tuple(t_span), list(y0), dict(kwargs)))
        return self._solve_ivp(fun, t_span, y0, **kwargs)


def _plain(kwargs: dict) -> dict:
    """The keyword arguments that are not callables (fun and jac are rebuilt
    by every PRyMclass, so their identities differ between solves)."""
    return {k: v for k, v in kwargs.items() if not callable(v)}


class TestBBNFromStore(unittest.TestCase):
    def test_a_ratio_grid_is_the_production_grid(self):
        """(a) No solve. compute_BBN_data._function on a stand-in history,
        with build_rho_NP_callback intercepted to record its arguments: the
        tool's ratio_grid, on the same samples in the store's units, returns
        arrays bitwise equal (==) to the ones recorded, on at least 50 samples
        in [0.2 keV, 100 MeV]; callback_domain_MeV equals the recorded T_min and
        T_max bitwise."""
        tool = _tool()
        units = Planck_units()
        eos = SaikawaShirai_EOS_spline(units)
        cosmology = SimpleNamespace(units=units, G_rho=eos.G_rho)
        rows, values = _synthetic_samples(units)

        model = SimpleNamespace(
            _cosmology=cosmology,
            potential=None,
            coupling=None,
            T_Jordan_stop=SimpleNamespace(as_float=1e-3 * units.eV),
            values=values,
        )
        proxy = SimpleNamespace(get=lambda: model)

        recorded = []

        def recorder(
            log_T_MeV, density_ratio, rho_SM, T_min_MeV, T_max_MeV, task_label
        ):
            recorded.append(
                (list(log_T_MeV), list(density_ratio), T_min_MeV, T_max_MeV)
            )
            raise ComputationFailureError(_RECORDED)

        BBNData = sys.modules["ComputeTargets.BBNData"]
        with mock.patch.object(BBNData, "build_rho_NP_callback", recorder):
            result = compute_BBN_data._function(proxy, task_label="test-a")

        self.assertTrue(result["failure"], result)
        self.assertIn(_RECORDED, result["failure_reason"])
        self.assertEqual(len(recorded), 1)
        log_T_prod, r_prod, T_min_prod, T_max_prod = recorded[0]

        log_T, r = tool.ratio_grid(np.array(rows), units)
        self.assertGreaterEqual(len(log_T), A_MIN_IN_WINDOW)
        self.assertLess(len(log_T), A_N_SAMPLES)
        self.assertEqual(len(log_T), len(log_T_prod))
        self.assertTrue(np.array_equal(log_T, np.array(log_T_prod)), "log T differs")
        self.assertTrue(np.array_equal(r, np.array(r_prod)), "ratio differs")
        self.assertTrue(all(a == b for a, b in zip(r, r_prod)))

        T_min, T_max = tool.callback_domain_MeV(units)
        self.assertEqual(T_min, T_min_prod)
        self.assertEqual(T_max, T_max_prod)

    def test_b_override_touches_the_low_T_call_only(self):
        """(b) compute_SM_baseline(small_network=True) twice with every
        solve_ivp call recorded as SciPy receives it: once with no override,
        once under lowT_tolerance_override(rtol=1e-6). The low-T call's rtol is
        absent (as PRyMordial passes it on this tree), then 1e-6, and its atol
        is 1e-11 both times; every other call's t_span, y0 and non-callable
        keyword arguments are identical between the two solves, and the
        override recognises the five stages in order.
        **Runs two small-network PRyMordial solves, about 10 s.**"""
        tool = _tool()

        plain = _RecordingSolveIVP()
        with SavedPRyMGlobals(), mock.patch.object(PRyMmain, "solve_ivp", plain):
            compute_SM_baseline(small_network=True)

        over = _RecordingSolveIVP()
        with SavedPRyMGlobals(), mock.patch.object(PRyMmain, "solve_ivp", over):
            with tool.lowT_tolerance_override(rtol=OVERRIDE_RTOL) as stage_calls:
                compute_SM_baseline(small_network=True)

        # the override restored both names
        self.assertIs(PRyMmain.solve_ivp, plain._solve_ivp)
        self.assertEqual(PRyMmain._limited.__name__, "_limited")

        self.assertEqual(tuple(c.stage for c in stage_calls), SMALL_NETWORK_STAGES)
        self.assertEqual(len(plain.calls), len(SMALL_NETWORK_STAGES))
        self.assertEqual(len(over.calls), len(SMALL_NETWORK_STAGES))

        for i, stage in enumerate(SMALL_NETWORK_STAGES):
            (span_a, y0_a, kw_a), (span_b, y0_b, kw_b) = plain.calls[i], over.calls[i]
            with self.subTest(stage=stage):
                self.assertEqual(span_a, span_b)
                self.assertEqual(y0_a, y0_b)
                self.assertEqual(
                    sorted(k for k, v in kw_a.items() if callable(v)),
                    sorted(k for k, v in kw_b.items() if callable(v)),
                )
                a, b = _plain(kw_a), _plain(kw_b)
                if stage == "low-T nuclear network (small)":
                    self.assertEqual(a.get("rtol"), LOWT_SMALL_RTOL_AS_PASSED)
                    if LOWT_SMALL_RTOL_AS_PASSED is None:
                        self.assertNotIn("rtol", a)
                    self.assertEqual(b["rtol"], OVERRIDE_RTOL)
                    self.assertEqual(a["atol"], LOWT_SMALL_ATOL_AS_PASSED)
                    self.assertEqual(b["atol"], LOWT_SMALL_ATOL_AS_PASSED)
                    a.pop("rtol", None)
                    b.pop("rtol", None)
                self.assertEqual(sorted(a), sorted(b))
                for key in a:
                    va, vb = a[key], b[key]
                    if isinstance(va, np.ndarray) or isinstance(vb, np.ndarray):
                        self.assertTrue(np.array_equal(va, vb), key)
                    else:
                        self.assertEqual(va, vb, key)

    def test_c_find_model_reads_the_store_read_only(self):
        """(c) No solve. find_model on a temporary two-shard store imitating
        the science store's tables (a handful of rows): it finds the one
        ScalarModel with the given beta, M and phi among decoys that share two
        of the three, returns its samples in production's order (redshift
        descending, not insertion order) and its BBNData row; every connection
        it opens is a `mode=ro` URI; a write through connect_ro raises
        sqlite3.OperationalError; and the shard files are byte-for-byte
        unchanged."""
        tool = _tool()
        units = Planck_units()
        Mp_eV = units.PlanckMass / units.eV

        with tempfile.TemporaryDirectory() as tmp:
            stem = os.path.join(tmp, "imitation")
            shards = [f"{stem}-shard{n:04d}.db" for n in range(2)]
            # shard 0: same beta and phi, other M; shard 1: the target (serial
            # 7), a decoy with the target's M and beta but phi 2, and one with
            # the target's M and phi but another beta
            models = {
                0: [(3, 1.6, 1e-5, 5.0)],
                1: [(5, 1.6, 1e-3, 2.0), (7, 1.6, 1e-3, 5.0), (9, 2.0, 1e-3, 5.0)],
            }
            for n, path in enumerate(shards):
                con = sqlite3.connect(path)
                con.executescript("""
                    create table beta_value (serial integer primary key, value real);
                    create table M_value (serial integer primary key, value_eV real);
                    create table phi_value (serial integer primary key, value_PlanckMass real);
                    create table ExponentialCoupling (serial integer primary key, beta_serial integer);
                    create table ExponentialPotential (serial integer primary key, M_serial integer);
                    create table redshift (serial integer primary key, z real);
                    create table ScalarModel (serial integer primary key, coupling_serial integer,
                        potential_serial integer, phi_Einstein_init_serial integer, failure integer,
                        RHS_evaluations integer, first_bounce_log_T_Jordan real);
                    create table ScalarModelValue (serial integer primary key, model_serial integer,
                        z_serial integer, raw_N real, log_T_Jordan_GeV real, H_Jordan_Mp real,
                        log_rhorad_Jordan_Mp4 real, log_fm real);
                    create table BBNData (serial integer primary key, model_serial integer,
                        failure integer, failure_reason text, Yp_BBN real, DOverH real,
                        He3OverH real, Li7OverH real, small_network integer, PRyM_version text);
                    """)
                for serial, beta, M, phi in models[n]:
                    con.execute("insert into beta_value values (?, ?)", (serial, beta))
                    con.execute(
                        "insert into M_value values (?, ?)", (serial, M * Mp_eV)
                    )
                    con.execute("insert into phi_value values (?, ?)", (serial, phi))
                    con.execute(
                        "insert into ExponentialCoupling values (?, ?)",
                        (serial, serial),
                    )
                    con.execute(
                        "insert into ExponentialPotential values (?, ?)",
                        (serial, serial),
                    )
                    con.execute(
                        "insert into ScalarModel values (?, ?, ?, ?, 0, ?, ?)",
                        (serial, serial, serial, serial, 1000 + serial, -0.5),
                    )
                    con.execute(
                        "insert into BBNData values (?, ?, 0, NULL, ?, ?, 1.0, 5.0, 0, 'v')",
                        (serial, serial, 0.24 + serial * 1e-3, 2.4 + serial * 1e-2),
                    )
                # three samples per model, inserted out of redshift order
                for k, z in enumerate((10.0, 1000.0, 100.0)):
                    con.execute("insert into redshift values (?, ?)", (k + 1, z))
                v = 1
                for serial, *_ in models[n]:
                    for k, z in enumerate((10.0, 1000.0, 100.0)):
                        con.execute(
                            "insert into ScalarModelValue values (?, ?, ?, ?, ?, ?, ?, ?)",
                            (
                                v,
                                serial,
                                k + 1,
                                1.0 / z,
                                log(z),
                                1e-30 * z,
                                -150.0 + k,
                                -3.0,
                            ),
                        )
                        v += 1
                con.commit()
                con.close()

            digests = [hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in shards]

            uris = []
            connect = sqlite3.connect

            def spy(database, *args, **kwargs):
                uris.append((database, kwargs.get("uri", False)))
                return connect(database, *args, **kwargs)

            with mock.patch.object(tool.sqlite3, "connect", spy):
                h = tool.find_model(stem, 1.6, 1e-3, 5.0, units)

            self.assertEqual(h.serial, 7)
            self.assertEqual(Path(h.shard).name, "imitation-shard0001.db")
            self.assertEqual(h.RHS_evaluations, 1007)
            # redshift descending: 1000, 100, 10
            self.assertEqual(
                list(h.rows[:, 0]), [1.0 / 1000.0, 1.0 / 100.0, 1.0 / 10.0]
            )
            self.assertEqual(h.bbn["Yp_BBN"], 0.24 + 7 * 1e-3)
            self.assertEqual(h.bbn["PRyM_version"], "v")

            self.assertEqual(len(uris), len(shards))
            for database, uri in uris:
                self.assertTrue(uri, database)
                self.assertTrue(database.startswith("file:"), database)
                self.assertTrue(database.endswith("?mode=ro"), database)

            with self.assertRaises(LookupError):
                tool.find_model(stem, 1.6, 0.5, 5.0, units)

            con = tool.connect_ro(shards[1])
            try:
                with self.assertRaises(sqlite3.OperationalError):
                    con.execute("insert into beta_value values (99, 9.9)")
                    con.commit()
                with self.assertRaises(sqlite3.OperationalError):
                    con.execute("create table t (x integer)")
            finally:
                con.close()

            after = [hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in shards]
            self.assertEqual(digests, after)


if __name__ == "__main__":
    unittest.main()
