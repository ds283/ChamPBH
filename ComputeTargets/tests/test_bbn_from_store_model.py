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
`tools/bbn_from_store.py` on a store holding both models (power-law-wall prompt 05 §1.5, §1.7; README §6.9 P37,
P38).

A temporary SQLite store, written as `test_bbn_from_store_alpha` writes its own (one shard, the tables and columns the
find query reads, with the model tables' `n_value` and the history's `model_type`): one beta, M and phi_init, an
`ExponentialModel` row (n = 1) and two `InversePowerModel` rows (n = 1, 2), the first of each table at serial 1, and
the histories (serial: model, n, drain model, value)

    10: exponential, 1, fixed, 0          20: inverse-power, 1, fixed, 0      30: inverse-power, 2, fixed, 0
    40: exponential, 1, one-loop-exponential-v2, 1                           50: inverse-power, 1, one-loop-inverse-power-v1, 1
    60: inverse-power, 1, one-loop-exponential-v2, 1 (a row no ScalarHistory may now be constructed with; P36)

- the find query matches the model's table, `model_type` and `n_value`: each (model, n) finds its own history and
  never the other model's of the same serial; an (model, n) with no history is a `LookupError` naming the model and
  n; the defaults find the exponential's at n = 1, as before;
- under the one-loop law the drain model matched is the model's own `one_loop_drain_model` at n (50, never 60), and
  `main` passes it to `find_model`;
- `--model inverse-power --n 3 --drain-law one_loop` is refused at parse, exit 2, naming `--n`; under the fixed law
  n = 3 is accepted;
- the header line names the model and n away from the defaults, and only there.

No solve (the variants are replaced where `main` runs whole), no Ray. Instant.
"""

import contextlib
import io
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import numpy as np

from CosmologyConcepts.drain_model import (
    DRAIN_MODEL_FIXED,
    DRAIN_MODEL_ONE_LOOP_EXPONENTIAL,
    DRAIN_MODEL_ONE_LOOP_INVERSE_POWER,
)
from CosmologyConcepts.Models.model_ids import EXPONENTIAL_MODEL, INVERSE_POWER_MODEL
from Units import Planck_units

BETA, M_MP, PHI = 2.0, 4.0e-31, 5.0

# (history serial, model type, model serial, alpha serial); RHS evaluations = 1000 + serial
HISTORIES = [
    (10, EXPONENTIAL_MODEL, 1, 1),
    (20, INVERSE_POWER_MODEL, 1, 1),
    (30, INVERSE_POWER_MODEL, 2, 1),
    (40, EXPONENTIAL_MODEL, 1, 2),
    (50, INVERSE_POWER_MODEL, 1, 3),
    (60, INVERSE_POWER_MODEL, 1, 2),
]
# (alpha serial, value, drain model)
ALPHAS = [
    (1, 0.0, DRAIN_MODEL_FIXED),
    (2, 1.0, DRAIN_MODEL_ONE_LOOP_EXPONENTIAL),
    (3, 1.0, DRAIN_MODEL_ONE_LOOP_INVERSE_POWER),
]


def _tool():
    import tools.bbn_from_store as tool

    return tool


def _write_store(tmp, Mp_eV):
    """One shard, `stem-shard0000.db`, holding HISTORIES; returns the stem."""
    stem = os.path.join(tmp, "modelstore")
    con = sqlite3.connect(f"{stem}-shard0000.db")
    con.executescript("""
        create table beta_value (serial integer primary key, value real);
        create table M_value (serial integer primary key, value_eV real);
        create table phi_value (serial integer primary key, value_PlanckMass real);
        create table alpha_value (serial integer primary key, value real, drain_model text);
        create table ExponentialModel (serial integer primary key, M_serial integer,
            n_value integer, beta_serial integer);
        create table InversePowerModel (serial integer primary key, M_serial integer,
            n_value integer, beta_serial integer);
        create table redshift (serial integer primary key, log_one_plus_z real);
        create table ScalarHistory (serial integer primary key, model_type integer,
            model_serial integer, phi_Einstein_init_serial integer, failure integer,
            RHS_evaluations integer, first_bounce_log_T_Jordan real, alpha_serial integer);
        create table ScalarHistoryValue (serial integer primary key, history_serial integer,
            z_serial integer, raw_N real, log_T_Jordan_GeV real, H_Jordan_Mp real,
            log_rhorad_Jordan_Mp4 real, log_fm real);
        create table BBNData (serial integer primary key, scalar_history_serial integer,
            failure integer, failure_reason text, Yp_BBN real, DOverH real,
            He3OverH real, Li7OverH real, small_network integer, PRyM_version text);
        """)
    con.execute("insert into beta_value values (1, ?)", (BETA,))
    con.execute("insert into M_value values (1, ?)", (M_MP * Mp_eV,))
    con.execute("insert into phi_value values (1, ?)", (PHI,))
    for serial, value, drain_model in ALPHAS:
        con.execute(
            "insert into alpha_value values (?, ?, ?)", (serial, value, drain_model)
        )
    con.execute("insert into ExponentialModel values (1, 1, 1, 1)")
    con.execute("insert into InversePowerModel values (1, 1, 1, 1)")
    con.execute("insert into InversePowerModel values (2, 1, 2, 1)")
    for k, z in enumerate((10.0, 1000.0, 100.0)):
        con.execute("insert into redshift values (?, ?)", (k + 1, np.log1p(z)))
    v = 1
    for serial, model_type, model_serial, alpha_serial in HISTORIES:
        con.execute(
            "insert into ScalarHistory values (?, ?, ?, 1, 0, ?, -0.5, ?)",
            (serial, model_type, model_serial, 1000 + serial, alpha_serial),
        )
        for k, z in enumerate((10.0, 1000.0, 100.0)):
            con.execute(
                "insert into ScalarHistoryValue values (?, ?, ?, ?, ?, ?, ?, ?)",
                (v, serial, k + 1, 1.0 / z, np.log(z), 1e-30 * z, -150.0 + k, -3.0),
            )
            v += 1
    con.commit()
    con.close()
    return stem


class _ModelStoreCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.tool = _tool()
        self.units = Planck_units()
        self.stem = _write_store(self.tmp, self.units.PlanckMass / self.units.eV)

    def find(self, **kwargs):
        return self.tool.find_model(self.stem, BETA, M_MP, PHI, self.units, **kwargs)


class TestFindModelByModelAndN(_ModelStoreCase):
    def test_each_model_and_n_finds_its_own_history(self):
        for kwargs, serial in (
            (dict(), 10),
            (dict(model="exponential", n=1), 10),
            (dict(model="inverse-power", n=1), 20),
            (dict(model="inverse-power", n=2), 30),
        ):
            with self.subTest(kwargs=kwargs):
                h = self.find(**kwargs)
                self.assertEqual(h.serial, serial)
                self.assertEqual(h.RHS_evaluations, 1000 + serial)

    def test_a_model_and_n_with_no_history_names_them(self):
        for kwargs, text in (
            (dict(model="exponential", n=2), " model=exponential n=2"),
            (dict(model="inverse-power", n=3), " model=inverse-power n=3"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(LookupError) as ctx:
                    self.find(**kwargs)
                self.assertIn(f"M={M_MP:g}{text} phi=", str(ctx.exception))

    def test_under_the_one_loop_law_each_model_matches_its_own_drain_model(self):
        self.assertEqual(
            self.find(drain_model=DRAIN_MODEL_ONE_LOOP_EXPONENTIAL, alpha=1.0).serial,
            40,
        )
        self.assertEqual(
            self.find(
                model="inverse-power",
                n=1,
                drain_model=DRAIN_MODEL_ONE_LOOP_INVERSE_POWER,
                alpha=1.0,
            ).serial,
            50,
        )

    def test_a_shard_without_the_models_table_holds_none_of_its_histories(self):
        con = sqlite3.connect(f"{self.stem}-shard0000.db")
        con.execute("drop table InversePowerModel")
        con.commit()
        con.close()
        self.assertEqual(self.find().serial, 10)
        with self.assertRaises(LookupError):
            self.find(model="inverse-power", n=1)


class TestTheOptions(_ModelStoreCase):
    def run_main(self, *extra, stub_find=True):
        argv = [self.stem, "--beta", f"{BETA:g}", "--M-Mp", f"{M_MP:g}", *extra]
        out, err = io.StringIO(), io.StringIO()
        patches = [
            mock.patch.object(
                self.tool, "solve_history_variant", return_value=({}, None)
            ),
            mock.patch.object(self.tool, "_print_outcome"),
        ]
        find = None
        if stub_find:
            find = mock.Mock(side_effect=SystemExit("reached find_model"))
            patches.append(mock.patch.object(self.tool, "find_model", find))
        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            stack.enter_context(contextlib.redirect_stdout(out))
            stack.enter_context(contextlib.redirect_stderr(err))
            try:
                code = self.tool.main(argv)
            except SystemExit as e:
                code = e.code
        return code, out.getvalue(), err.getvalue(), find

    def test_main_passes_the_models_own_drain_model(self):
        for extra, model, n, drain_model in (
            ((), "exponential", 1, DRAIN_MODEL_FIXED),
            (
                ("--drain-law", "one_loop", "--alpha", "1"),
                "exponential",
                1,
                DRAIN_MODEL_ONE_LOOP_EXPONENTIAL,
            ),
            (
                ("--model", "inverse-power", "--n", "2"),
                "inverse-power",
                2,
                DRAIN_MODEL_FIXED,
            ),
            (
                ("--model", "inverse-power", "--drain-law", "one_loop", "--alpha", "1"),
                "inverse-power",
                1,
                DRAIN_MODEL_ONE_LOOP_INVERSE_POWER,
            ),
            (
                (
                    "--model",
                    "inverse-power",
                    "--n",
                    "2",
                    "--drain-law",
                    "one_loop",
                    "--alpha",
                    "1",
                ),
                "inverse-power",
                2,
                DRAIN_MODEL_ONE_LOOP_INVERSE_POWER,
            ),
        ):
            with self.subTest(extra=extra):
                code, _, err, find = self.run_main(*extra)
                self.assertEqual(code, "reached find_model", err)
                kwargs = find.call_args.kwargs
                self.assertEqual(
                    (kwargs["model"], kwargs["n"], kwargs["drain_model"]),
                    (model, n, drain_model),
                )

    def test_an_n_with_no_one_loop_law_is_refused_at_parse(self):
        code, _, err, find = self.run_main(
            "--model",
            "inverse-power",
            "--n",
            "3",
            "--drain-law",
            "one_loop",
            "--alpha",
            "1",
        )
        self.assertEqual(code, 2)
        self.assertIn("argument --n:", err)
        self.assertIn("n = 1 and 2 only", err)
        find.assert_not_called()
        # under the fixed law n = 3 is a key like any other
        code, _, err, find = self.run_main("--model", "inverse-power", "--n", "3")
        self.assertEqual(code, "reached find_model", err)
        self.assertEqual(find.call_args.kwargs["n"], 3)
        # and n must be a positive integer, as tools/history_and_bbn.py's
        code, _, err, _ = self.run_main("--n", "0")
        self.assertEqual(code, 2)
        self.assertIn("argument --n:", err)

    def test_the_header_names_the_model_and_n_away_from_the_defaults_only(self):
        for extra, serial, text in (
            ((), 10, ""),
            (("--model", "exponential", "--n", "1"), 10, ""),
            (("--model", "inverse-power"), 20, " model=inverse-power n=1"),
            (("--model", "inverse-power", "--n", "2"), 30, " model=inverse-power n=2"),
            (
                ("--model", "inverse-power", "--drain-law", "one_loop", "--alpha", "1"),
                50,
                " model=inverse-power n=1",
            ),
        ):
            with self.subTest(extra=extra):
                code, out, err, _ = self.run_main(*extra, stub_find=False)
                self.assertEqual(code, 0, err)
                (header,) = [l for l in out.splitlines() if l.startswith("store ")]
                self.assertTrue(
                    header.startswith(
                        f"store beta={BETA:g} M={M_MP:g}{text} phi={PHI:g}"
                    ),
                    header,
                )
                self.assertIn(f" serial={serial} ", header)


if __name__ == "__main__":
    unittest.main()
