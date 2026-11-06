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
The tools that read `alpha_value` by raw SQL, and the registered run's scope (one-loop-drain prompt 04, prompt §1.8,
§1.9; orchestrator note 04, rulings 4 and 7).

On a temporary store holding the value 0.1 under both models, for the same (beta, M) of P-2197 (one integration
under the fixed law and one under the one-loop law at A = 0.1, a few seconds each, shared with the other tests of
the process through `dissipation_histories` and `test_drain_store`'s caches):

- `tools/bbn_from_store.py`'s find query: each law finds its own history; a store with no `drain_model` column (the
  format-6 imitation: the one-loop rows removed and the column dropped) finds the fixed one and, under `one_loop`,
  nothing; a shard with no `alpha_value` table finds the alpha = 0 history only and under `fixed` only;
- its `--drain-law` and `--alpha` (ruling 4): `--alpha 10` is accepted under `one_loop` and refused under `fixed`
  with today's message; `--alpha` is required under `one_loop`; the header text names A and the model;
- `tools/outward_step_scan.py`'s selection reads fixed rows only, counts the rows of another model it skips in one
  line on stderr (ruling 7), counts nothing on a store with no `drain_model` column, and prints nothing when there is
  no other row;
- `describe_scope` is tested in `RunDrivers/tests/test_scope_a_values.py`, and the inventory report in
  `test_inventory_alpha.py`.

No Ray, no PRyMordial solve.
"""

import contextlib
import glob
import io
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ComputeTargets.tests import dissipation_histories as dh
from CosmologyConcepts.drain_model import (
    DRAIN_MODEL_FIXED,
    DRAIN_MODEL_ONE_LOOP,
)
from ObjectFactories.tests.test_drain_store import (
    ONE_LOOP,
    P2197,
    _DrainStoreCase,
    one_loop_payload,
)
from tools import bbn_from_store as bfs
from tools import outward_step_scan as oss

BETA, M, _ = P2197
VALUE = 0.1


class _TwoModelStore(_DrainStoreCase):
    """The value 0.1 under both models, for the same (beta, M): serials of the fixed and of the one-loop history."""

    def setUp(self):
        super().setUp()
        fixed = dh.payload(BETA, M, VALUE)
        _, self.fixed_serial = self.write(fixed, BETA, M, VALUE)
        key = (BETA, M, VALUE)
        _, self.one_loop_serial = self.write_one_loop(
            key, one_loop_payload(BETA, M, VALUE)
        )
        shards = sorted(glob.glob(str(self._dir / "*shard*.db")))
        self.assertEqual(len(shards), 1)
        self.shard = shards[0]
        self.stem = self.shard.rsplit("-shard", 1)[0]

    def format_6_imitation(self) -> str:
        """A copy of the store as a format-6 store holds it: fixed rows only and no `drain_model` column."""
        directory = Path(tempfile.mkdtemp(prefix="champbh-format6-"))
        self.addCleanup(shutil.rmtree, directory, True)
        copy = directory / Path(self.shard).name
        shutil.copy(self.shard, copy)
        con = sqlite3.connect(copy)
        con.execute(
            "delete from ScalarHistoryValue where history_serial = ?",
            (self.one_loop_serial,),
        )
        con.execute("delete from ScalarHistoryBounce")
        con.execute(
            "delete from ScalarHistory where serial = ?", (self.one_loop_serial,)
        )
        con.execute("delete from alpha_value where drain_model != 'fixed'")
        # drop the column by rebuilding the table, whatever its constraints
        columns = [
            r[1]
            for r in con.execute("pragma table_info(alpha_value)").fetchall()
            if r[1] != "drain_model"
        ]
        listed = ", ".join(columns)
        con.execute(f"create table alpha_value_6 as select {listed} from alpha_value")
        con.execute("drop table alpha_value")
        con.execute("alter table alpha_value_6 rename to alpha_value")
        con.commit()
        self.assertNotIn(
            "drain_model",
            [r[1] for r in con.execute("pragma table_info(alpha_value)").fetchall()],
        )
        con.close()
        return str(directory / Path(self.shard).name.rsplit("-shard", 1)[0])


class TestBbnFromStoreFindsEachLaw(_TwoModelStore):
    def find(self, stem, **kwargs):
        return bfs.find_model(stem, BETA, M, 5.0, self.units, alpha=VALUE, **kwargs)

    def test_each_law_finds_its_own_history(self):
        fixed = self.find(self.stem, drain_model=DRAIN_MODEL_FIXED)
        one_loop = self.find(self.stem, drain_model=DRAIN_MODEL_ONE_LOOP)
        self.assertEqual(fixed.serial, self.fixed_serial)
        self.assertEqual(one_loop.serial, self.one_loop_serial)
        self.assertNotEqual(fixed.serial, one_loop.serial)
        # the default is the fixed model: the call of before
        self.assertEqual(self.find(self.stem).serial, self.fixed_serial)

    def test_a_value_of_one_model_is_not_found_under_the_other(self):
        for kwargs, text in (
            (dict(alpha=0.25, drain_model=DRAIN_MODEL_FIXED), " alpha=0.25"),
            (
                dict(alpha=0.25, drain_model=DRAIN_MODEL_ONE_LOOP),
                f" A=0.25 ({ONE_LOOP})",
            ),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(LookupError) as ctx:
                    bfs.find_model(self.stem, BETA, M, 5.0, self.units, **kwargs)
                self.assertIn(f"phi=5{text} in {self.stem}", str(ctx.exception))
        # alpha = 0 is no row of the one-loop model
        with self.assertRaises(LookupError):
            bfs.find_model(
                self.stem,
                BETA,
                M,
                5.0,
                self.units,
                alpha=0.0,
                drain_model=DRAIN_MODEL_ONE_LOOP,
            )

    def test_a_format_6_imitation_finds_the_fixed_history_and_under_one_loop_nothing(
        self,
    ):
        stem = self.format_6_imitation()
        fixed = self.find(stem, drain_model=DRAIN_MODEL_FIXED)
        self.assertEqual(fixed.serial, self.fixed_serial)
        self.assertEqual(self.find(stem).serial, self.fixed_serial)
        with self.assertRaises(LookupError) as ctx:
            self.find(stem, drain_model=DRAIN_MODEL_ONE_LOOP)
        self.assertIn(f"A=0.1 ({ONE_LOOP})", str(ctx.exception))

    def test_a_shard_with_no_alpha_table_holds_alpha_zero_under_the_fixed_model_only(
        self,
    ):
        con = sqlite3.connect(":memory:")
        sql, parameters = bfs._find_query(con, BETA, 5.0, 0.0, DRAIN_MODEL_FIXED)
        self.assertIs(sql, bfs._FIND_QUERY_WITHOUT_ALPHA)
        self.assertEqual(parameters, (BETA, 5.0))
        self.assertIsNone(bfs._find_query(con, BETA, 5.0, 0.1, DRAIN_MODEL_FIXED))
        self.assertIsNone(bfs._find_query(con, BETA, 5.0, 1.0, DRAIN_MODEL_ONE_LOOP))
        # the model is bound, never formatted into the SQL
        with contextlib.closing(sqlite3.connect(self.shard)) as shard:
            sql, parameters = bfs._find_query(
                shard, BETA, 5.0, 0.1, DRAIN_MODEL_ONE_LOOP
            )
        self.assertNotIn(ONE_LOOP, sql)
        self.assertEqual(parameters[-1], DRAIN_MODEL_ONE_LOOP)

    def test_the_header_text_names_A_and_the_model_only_under_the_one_loop_model(self):
        self.assertEqual(bfs._alpha_text(0.0), "")
        self.assertEqual(bfs._alpha_text(0.1), " alpha=0.1")
        self.assertEqual(bfs._alpha_text(0.1, DRAIN_MODEL_FIXED), " alpha=0.1")
        self.assertEqual(
            bfs._alpha_text(1.0, DRAIN_MODEL_ONE_LOOP), f" A=1 ({ONE_LOOP})"
        )
        self.assertEqual(
            bfs._alpha_text(10.0, DRAIN_MODEL_ONE_LOOP), f" A=10 ({ONE_LOOP})"
        )


class TestBbnFromStoreOptions(unittest.TestCase):
    """`--drain-law` and `--alpha` through main(), with find_model stubbed to stop the tool after the parse."""

    def run_main(self, *extra):
        argv = ["store", "--beta", "1.2", "--M-Mp", "1e-3", *extra]
        err = io.StringIO()
        with mock.patch.object(
            bfs, "find_model", side_effect=LookupError("stub")
        ) as find, contextlib.redirect_stderr(err), contextlib.redirect_stdout(
            io.StringIO()
        ):
            try:
                bfs.main(argv)
                code = None
            except LookupError:
                code = "reached find_model"
            except SystemExit as e:
                code = e.code
        return code, err.getvalue(), find

    def test_alpha_10_is_accepted_under_one_loop_and_refused_under_fixed(self):
        code, err, find = self.run_main("--drain-law", "one_loop", "--alpha", "10")
        self.assertEqual(code, "reached find_model", err)
        self.assertEqual(find.call_args.kwargs["alpha"], 10.0)
        self.assertEqual(find.call_args.kwargs["drain_model"], DRAIN_MODEL_ONE_LOOP)
        for extra in (("--alpha", "10"), ("--drain-law", "fixed", "--alpha", "10")):
            with self.subTest(extra=extra):
                code, err, find = self.run_main(*extra)
                self.assertEqual(code, 2)
                find.assert_not_called()
                # today's message, as argparse gave it when it was the option's type
                self.assertIn("argument --alpha: must be a fraction", err)
                self.assertIn("0 <= alpha < 1", err)

    def test_the_default_is_the_fixed_law_at_alpha_zero(self):
        code, err, find = self.run_main()
        self.assertEqual(code, "reached find_model", err)
        self.assertEqual(find.call_args.kwargs["alpha"], 0.0)
        self.assertEqual(find.call_args.kwargs["drain_model"], DRAIN_MODEL_FIXED)
        code, err, find = self.run_main("--alpha", "0.3")
        self.assertEqual(find.call_args.kwargs["alpha"], 0.3)
        self.assertEqual(find.call_args.kwargs["drain_model"], DRAIN_MODEL_FIXED)

    def test_alpha_is_required_under_one_loop_and_checked_by_the_law(self):
        code, err, find = self.run_main("--drain-law", "one_loop")
        self.assertEqual(code, 2)
        self.assertIn("argument --alpha: --drain-law one_loop needs --alpha A", err)
        for value in ("0", "-1", "nan", "inf", "x"):
            with self.subTest(value=value):
                code, err, find = self.run_main(
                    "--drain-law", "one_loop", "--alpha", value
                )
                self.assertEqual(code, 2)
                self.assertIn("argument --alpha:", err)
                find.assert_not_called()
        code, err, _ = self.run_main("--drain-law", "one_loop", "--alpha", "0")
        # the message is check_drain_model's, with the option for its context
        self.assertIn("under the one-loop drain law alpha carries A", err)

    def test_an_unknown_law_is_refused_by_argparse(self):
        code, err, _ = self.run_main("--drain-law", "no_such_law")
        self.assertEqual(code, 2)
        self.assertIn("--drain-law", err)


class TestOutwardStepScanReadsFixedRowsOnly(_TwoModelStore):
    def select(self, base, **kwargs):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            specs = oss.select_from_store(base, **kwargs)
        return specs, err.getvalue()

    def test_a_one_loop_row_is_skipped_with_its_count(self):
        specs, err = self.select(self.stem)
        self.assertEqual([s["store"]["serial"] for s in specs], [self.fixed_serial])
        self.assertEqual(specs[0]["alpha"], VALUE)
        lines = err.splitlines()
        self.assertEqual(len(lines), 1, err)
        self.assertIn("1 ScalarHistory row of", lines[0])
        self.assertIn(self.stem, lines[0])
        self.assertIn("under another drain model than 'fixed' skipped", lines[0])

    def test_the_count_does_not_depend_on_the_filters(self):
        for kwargs in (
            dict(M=[1e-12]),
            dict(beta=[99.0]),
            dict(alpha=[0.5]),
            dict(blocks=["D"]),
            dict(success_only=True),
        ):
            with self.subTest(kwargs=kwargs):
                _, err = self.select(self.stem, **kwargs)
                self.assertIn("1 ScalarHistory row of", err)
                self.assertEqual(len(err.splitlines()), 1)

    def test_a_format_6_imitation_is_read_as_before_with_nothing_counted(self):
        base = self.format_6_imitation()
        specs, err = self.select(base)
        self.assertEqual([s["store"]["serial"] for s in specs], [self.fixed_serial])
        self.assertEqual(err, "")

    def test_a_store_with_no_other_row_prints_nothing(self):
        base = self.format_6_imitation()
        # and the same on a store of this format whose one-loop history is removed
        directory = Path(tempfile.mkdtemp(prefix="champbh-fixedonly-"))
        self.addCleanup(shutil.rmtree, directory, True)
        copy = directory / Path(self.shard).name
        shutil.copy(self.shard, copy)
        con = sqlite3.connect(copy)
        con.execute("delete from ScalarHistoryBounce")
        con.execute(
            "delete from ScalarHistoryValue where history_serial = ?",
            (self.one_loop_serial,),
        )
        con.execute(
            "delete from ScalarHistory where serial = ?", (self.one_loop_serial,)
        )
        con.commit()
        con.close()
        specs, err = self.select(
            str(directory / Path(self.shard).name.rsplit("-shard", 1)[0])
        )
        self.assertEqual(len(specs), 1)
        self.assertEqual(err, "")

    def test_the_model_is_bound_never_formatted_into_the_query(self):
        text = Path(oss.__file__).read_text()
        self.assertNotIn(ONE_LOOP, text)
        self.assertIn("DRAIN_MODEL_FIXED", text)


if __name__ == "__main__":
    unittest.main()
