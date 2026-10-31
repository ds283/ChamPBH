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
`tools/bbn_from_store.py --alpha` (inelastic-reflection prompt 10, test 11, P39, ruling 8), on a
temporary SQLite file imitating the store's tables as `test_bbn_from_store`'s test (c) does, with
the `alpha_value` table and the history's `alpha_serial` column (as the store's schema since
prompt 09). One model, one beta, M and phi_init, three histories under it, at alpha = 0, 0.1 and
0.2:

- the default (and alpha = 0 given) finds the alpha = 0 history; alpha = 0.1 finds its own, and
  0.2 its own;
- alpha is matched as the dimensionless factory matches it: absolutely at 0 (a stored 1e-9 is 0, a
  stored 1e-6 is not), relatively otherwise (a stored 0.1 * (1 + 1e-7) is 0.1, a stored
  0.1 * (1 + 1e-3) is not);
- an alpha with no history is a `LookupError` that names it (`alpha=0.3`); at alpha = 0 the message
  is the one the tool had;
- a shard with no `alpha_value` table (a store of before alpha was a key) holds alpha = 0 histories
  only;
- the option parses through `alpha_fraction` (1.0 is refused, exit 2).

No solve, no Ray. Instant.
"""

import contextlib
import io
import os
import sqlite3
import tempfile
import unittest

import numpy as np

from Units import Planck_units

BETA, M_MP, PHI = 2.0, 0.5, 5.0
# (history serial, alpha value): serial = 10 * (1 + index of the alpha), RHS evaluations = 1000 + serial
HISTORIES = [(10, 0.0), (20, 0.1), (30, 0.2)]


def _tool():
    import tools.bbn_from_store as tool

    return tool


def _write_store(tmp, Mp_eV, alpha_rows=HISTORIES, with_alpha_table=True):
    """One shard, `stem-shard0000.db`: the model, and a history for each (serial, alpha) of
    `alpha_rows`; returns the stem."""
    stem = os.path.join(tmp, "alphastore")
    con = sqlite3.connect(f"{stem}-shard0000.db")
    alpha_column = ", alpha_serial integer" if with_alpha_table else ""
    con.executescript(f"""
        create table beta_value (serial integer primary key, value real);
        create table M_value (serial integer primary key, value_eV real);
        create table phi_value (serial integer primary key, value_PlanckMass real);
        create table alpha_value (serial integer primary key, value real);
        create table ExponentialModel (serial integer primary key, M_serial integer,
            beta_serial integer);
        create table redshift (serial integer primary key, log_one_plus_z real);
        create table ScalarHistory (serial integer primary key, model_serial integer,
            phi_Einstein_init_serial integer, failure integer,
            RHS_evaluations integer, first_bounce_log_T_Jordan real{alpha_column});
        create table ScalarHistoryValue (serial integer primary key, history_serial integer,
            z_serial integer, raw_N real, log_T_Jordan_GeV real, H_Jordan_Mp real,
            log_rhorad_Jordan_Mp4 real, log_fm real);
        create table BBNData (serial integer primary key, scalar_history_serial integer,
            failure integer, failure_reason text, Yp_BBN real, DOverH real,
            He3OverH real, Li7OverH real, small_network integer, PRyM_version text);
        """)
    if not with_alpha_table:
        con.execute("drop table alpha_value")
    con.execute("insert into beta_value values (1, ?)", (BETA,))
    con.execute("insert into M_value values (1, ?)", (M_MP * Mp_eV,))
    con.execute("insert into phi_value values (1, ?)", (PHI,))
    con.execute("insert into ExponentialModel values (1, 1, 1)")
    for k, z in enumerate((10.0, 1000.0, 100.0)):
        con.execute("insert into redshift values (?, ?)", (k + 1, np.log1p(z)))
    v = 1
    for serial, alpha in alpha_rows:
        if with_alpha_table:
            con.execute("insert into alpha_value values (?, ?)", (serial, alpha))
            con.execute(
                "insert into ScalarHistory values (?, 1, 1, 0, ?, -0.5, ?)",
                (serial, 1000 + serial, serial),
            )
        else:
            con.execute(
                "insert into ScalarHistory values (?, 1, 1, 0, ?, -0.5)",
                (serial, 1000 + serial),
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


class TestFindModelByAlpha(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.tool = _tool()
        self.units = Planck_units()
        self.Mp_eV = self.units.PlanckMass / self.units.eV

    def find(self, stem, **kwargs):
        return self.tool.find_model(stem, BETA, M_MP, PHI, self.units, **kwargs)

    def test_11_each_alpha_finds_its_own_history_and_the_default_the_zero_one(self):
        stem = _write_store(self.tmp, self.Mp_eV)
        self.assertEqual(self.find(stem).serial, 10)
        self.assertEqual(self.find(stem, alpha=0.0).serial, 10)
        self.assertEqual(self.find(stem, alpha=0.1).serial, 20)
        self.assertEqual(self.find(stem, alpha=0.2).serial, 30)
        self.assertEqual(self.find(stem, alpha=0.1).RHS_evaluations, 1020)
        # without the keyword the three histories would be "3 ScalarHistories match": the
        # alpha is what tells them apart

    def test_11_alpha_is_matched_as_the_dimensionless_factory_matches_it(self):
        # absolute at 0: 1e-9 is 0, 1e-6 is not
        stem = _write_store(
            self.tmp, self.Mp_eV, [(10, 1.0e-9), (20, 0.1 * (1 + 1e-7))]
        )
        self.assertEqual(self.find(stem).serial, 10)
        self.assertEqual(self.find(stem, alpha=0.1).serial, 20)
        stem = os.path.join(self.tmp, "second")
        os.mkdir(stem)
        stem = _write_store(stem, self.Mp_eV, [(10, 1.0e-6), (20, 0.1 * (1 + 1e-3))])
        with self.assertRaises(LookupError):
            self.find(stem)
        with self.assertRaises(LookupError):
            self.find(stem, alpha=0.1)

    def test_11_an_alpha_with_no_history_names_it_and_alpha_0_is_as_before(self):
        stem = _write_store(self.tmp, self.Mp_eV, [(20, 0.1)])
        with self.assertRaises(LookupError) as ctx:
            self.find(stem)
        # the message the tool had at alpha = 0
        self.assertEqual(
            str(ctx.exception),
            f"no ScalarHistory with beta=2 M=0.5 phi=5 in {stem}",
        )
        with self.assertRaises(LookupError) as ctx:
            self.find(stem, alpha=0.3)
        self.assertEqual(
            str(ctx.exception),
            f"no ScalarHistory with beta=2 M=0.5 phi=5 alpha=0.3 in {stem}",
        )
        # two histories at one alpha: the message at alpha > 0 names it, and the shards
        stem2 = os.path.join(self.tmp, "dup")
        os.mkdir(stem2)
        stem2 = _write_store(stem2, self.Mp_eV, [(20, 0.1), (21, 0.1)])
        with self.assertRaises(LookupError) as ctx:
            self.find(stem2, alpha=0.1)
        self.assertIn(
            "2 ScalarHistories match beta=2 M=0.5 phi=5 alpha=0.1: ", str(ctx.exception)
        )

    def test_11_a_shard_without_the_alpha_table_holds_alpha_0_only(self):
        stem = _write_store(self.tmp, self.Mp_eV, [(10, 0.0)], with_alpha_table=False)
        self.assertEqual(self.find(stem).serial, 10)
        with self.assertRaises(LookupError):
            self.find(stem, alpha=0.1)

    def test_11_the_option_parses_through_alpha_fraction(self):
        tool = self.tool
        for value in ("1.0", "-0.1", "nan"):
            with self.subTest(value=value):
                err = io.StringIO()
                with contextlib.redirect_stderr(err), self.assertRaises(
                    SystemExit
                ) as ctx:
                    tool.main(
                        ["stem", "--beta", "2", "--M-Mp", "0.5", "--alpha", value]
                    )
                self.assertEqual(ctx.exception.code, 2)
                self.assertIn("argument --alpha: must be a fraction", err.getvalue())

    def test_11_the_cli_finds_each_alpha_without_a_solve(self):
        """`main` with the solve replaced: the header line names the alpha only above 0."""
        from unittest import mock

        stem = _write_store(self.tmp, self.Mp_eV)
        for argv, serial, alpha_text in (
            ([], 10, ""),
            (["--alpha", "0"], 10, ""),
            (["--alpha", "0.1"], 20, " alpha=0.1"),
        ):
            with self.subTest(argv=argv):
                out = io.StringIO()
                with mock.patch.object(
                    self.tool, "solve_history_variant", return_value=({}, None)
                ), mock.patch.object(
                    self.tool, "_print_outcome"
                ), contextlib.redirect_stdout(
                    out
                ):
                    status = self.tool.main(
                        [
                            stem,
                            "--beta",
                            "2",
                            "--M-Mp",
                            "0.5",
                            "--variant",
                            "prod",
                            *argv,
                        ]
                    )
                self.assertEqual(status, 0)
                header = [
                    l for l in out.getvalue().splitlines() if l.startswith("store ")
                ]
                self.assertEqual(len(header), 1)
                self.assertTrue(
                    header[0].startswith(
                        f"store beta=2 M=0.5 phi=5{alpha_text}: shard=0000 serial={serial} "
                    ),
                    header[0],
                )


if __name__ == "__main__":
    unittest.main()
