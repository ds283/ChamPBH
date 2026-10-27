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
Which work main.py's three stages schedule (run-integrity prompt 03, README §6.3;
jordan-normalization prompt 07).

`TestScalarStageFirstPass`: the scalar stage's first pass, `missing_models`
(jordan-normalization prompt 07). Before that prompt the stage's "which are missing?"
comprehension kept every model whatever its lookup found, so a fully stored batch was
resubmitted (`[02-scalar-stage-missing-filter-keeps-every-model]`,
`[03-step-1-first-pass-lookup-filters-nothing]`); a fully stored batch now gives [].

Before prompt 03 both stages built their downstream query from the ScalarHistories
that did not fail, then zipped the results against the unfiltered shard bin. One
failed model shifted every later result by one, and `zip` dropped the last. The
decision now lives in `pipeline_selection`, which main.py's two stages call; it is
pure, so these tests reach it without main.py's argument parsing or Ray.

The lookup results are stand-ins carrying `available` and `failure`, as in
`prompts/run-integrity/planning-probes/pairing_probe.py`, which reproduces the old
logic and is the breakage record for (d)-(f): on the tree before prompt 03 it
schedules {V1, V3}. Test (g) reads the argument parser, and no config file. Since
jordan-normalization prompt 07 an entry is (model, history), the model "V<i>" a
stand-in string where it was the pair ("V<i>", "beta<i>").

`TestRetryFailedScalar` and `TestRetryFailedScalarFlag` (tuning-followups prompt 01):
`missing_models(…, retry_failed=True)` counts a stored failure as missing, the default is
unchanged, `count_stored_failures` counts the stored failures for the stage's summary
line, and the parser and `main.py --help` carry `--retry-failed-scalar`. Before prompt 01
neither the keyword, the function nor the flag existed.

No Ray cluster, no datastore, no PRyMordial solve. Instant, but for
`test_i_main_py_help_lists_it`, which runs `main.py --help` in a subprocess (about 1.3 s;
argparse exits before `ray.init`).
"""

import os
import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace as NS

N_MODELS = 5
FAILED_MODEL = 1
HAS_BBN = {0, 3}


def _models():
    return [f"V{i}" for i in range(N_MODELS)]


def _scalar_histories():
    return [
        NS(i=i, available=True, failure=(i == FAILED_MODEL)) for i in range(N_MODELS)
    ]


def _bbn_results(entries, failed_bbn=()):
    """One stand-in BBN lookup result per entry, keyed on the entry's history."""
    results = []
    for entry in entries:
        i = entry.history.i
        if i in failed_bbn:
            results.append(NS(model=i, available=True, failure=True))
        elif i in HAS_BBN:
            results.append(NS(model=i, available=True, failure=False))
        else:
            results.append(NS(model=i, available=False, failure=None))
    return results


def _names(entries):
    return [entry.model for entry in entries]


class TestPipelineSelection(unittest.TestCase):
    def test_d_the_probe_bin_schedules_V2_and_V4(self):
        """(d) Five models, model 1's ScalarHistory failed, models 0 and 3 have BBN rows: missing is {V2, V4}."""
        from pipeline_selection import build_query_entries, select_missing

        models = _models()
        built = build_query_entries(models, _scalar_histories())

        self.assertEqual(built.skipped_failed_models, 1)
        self.assertEqual(built.unavailable, [])
        self.assertEqual(_names(built.entries), ["V0", "V2", "V3", "V4"])

        # each entry carries the model, and the history, it was built from
        for entry in built.entries:
            i = entry.history.i
            self.assertEqual(entry.model, models[i])

        selection = select_missing(built.entries, _bbn_results(built.entries))
        self.assertEqual(_names(selection.missing), ["V2", "V4"])
        self.assertEqual(selection.stored_failures, 0)
        for entry in selection.missing:
            self.assertEqual(entry.model, models[entry.history.i])

    def test_e_a_stored_failure_is_retried_only_under_the_flag(self):
        """(e) The same bin with V3's stored BBN row a failure: {V2, V4} without the flag, {V2, V3, V4} with it."""
        from pipeline_selection import build_query_entries, select_missing

        built = build_query_entries(_models(), _scalar_histories())
        results = _bbn_results(built.entries, failed_bbn={3})

        without = select_missing(built.entries, results)
        self.assertEqual(_names(without.missing), ["V2", "V4"])
        self.assertEqual(without.stored_failures, 1)

        with_flag = select_missing(built.entries, results, retry_failed=True)
        self.assertEqual(_names(with_flag.missing), ["V2", "V3", "V4"])
        self.assertEqual(with_flag.stored_failures, 1)

    def test_e2_a_result_without_a_failure_attribute_is_never_a_stored_failure(self):
        """AdiabaticHistory results carry no `failure`: an available one is done, an unavailable one missing."""
        from pipeline_selection import build_query_entries, select_missing

        built = build_query_entries(_models(), _scalar_histories())
        results = [
            NS(available=(entry.history.i in HAS_BBN)) for entry in built.entries
        ]

        selection = select_missing(built.entries, results, retry_failed=False)
        self.assertEqual(_names(selection.missing), ["V2", "V4"])
        self.assertEqual(selection.stored_failures, 0)

    def test_e3_an_unavailable_scalar_history_is_reported_not_queried(self):
        """A ScalarHistory lookup that found no row is reported in `unavailable`, and its `failure` is not read."""
        from pipeline_selection import build_query_entries

        histories = _scalar_histories()
        histories[4] = NS(i=4, available=False)  # no `failure` attribute at all

        built = build_query_entries(_models(), histories)
        self.assertEqual(built.unavailable, ["V4"])
        self.assertEqual(built.skipped_failed_models, 1)
        self.assertEqual(_names(built.entries), ["V0", "V2", "V3"])

    def test_f_results_of_the_wrong_length_raise(self):
        """(f) Results one short raise ValueError, at either step; they are not truncated."""
        from pipeline_selection import build_query_entries, select_missing

        with self.assertRaises(ValueError):
            build_query_entries(_models(), _scalar_histories()[:-1])

        built = build_query_entries(_models(), _scalar_histories())
        results = _bbn_results(built.entries)
        with self.assertRaises(ValueError):
            select_missing(built.entries, results[:-1])
        with self.assertRaises(ValueError):
            select_missing(built.entries, results[:-1], retry_failed=True)

        # and one too many is refused as well
        with self.assertRaises(ValueError):
            select_missing(built.entries, results + [results[0]])


class TestScalarStageFirstPass(unittest.TestCase):
    """main.py's scalar stage's first pass reads its lookup results (jordan-normalization
    prompt 07; README §3.1's two issues)."""

    def test_a_fully_stored_batch_gives_no_work(self):
        from pipeline_selection import missing_models

        stored = [NS(available=True, failure=False) for _ in range(N_MODELS)]
        self.assertEqual(missing_models(_models(), stored), [])

    def test_a_batch_with_one_unavailable_gives_that_one(self):
        from pipeline_selection import missing_models

        lookups = [NS(available=True, failure=False) for _ in range(N_MODELS)]
        lookups[2] = NS(available=False)  # no `failure` attribute at all
        self.assertEqual(missing_models(_models(), lookups), ["V2"])

    def test_a_stored_failure_is_not_missing(self):
        from pipeline_selection import missing_models

        # the stage does not recompute a stored failure: its second lookup would find it
        lookups = [NS(available=True, failure=False) for _ in range(N_MODELS)]
        lookups[1] = NS(available=True, failure=True)
        self.assertEqual(missing_models(_models(), lookups), [])

    def test_the_order_is_kept(self):
        from pipeline_selection import missing_models

        available = {0, 2}
        lookups = [
            NS(available=(i in available), failure=False) for i in range(N_MODELS)
        ]
        self.assertEqual(missing_models(_models(), lookups), ["V1", "V3", "V4"])

    def test_lookups_of_the_wrong_length_raise(self):
        from pipeline_selection import missing_models

        lookups = [NS(available=False) for _ in range(N_MODELS)]
        with self.assertRaises(ValueError):
            missing_models(_models(), lookups[:-1])
        with self.assertRaises(ValueError):
            missing_models(_models(), lookups + [lookups[0]])


class TestRetryFailedScalar(unittest.TestCase):
    """The scalar stage's first pass under --retry-failed-scalar (tuning-followups prompt 01)."""

    def _lookups(self):
        # V0 stored, V1 a stored failure, V2 not stored (no `failure` attribute at all),
        # V3 stored, V4 a stored failure
        lookups = [NS(available=True, failure=False) for _ in range(N_MODELS)]
        lookups[1] = NS(available=True, failure=True)
        lookups[2] = NS(available=False)
        lookups[4] = NS(available=True, failure=True)
        return lookups

    def test_h_a_stored_failure_is_missing_under_the_flag(self):
        """(h) With retry_failed, the stored failures are missing, beside the model with no row, in order."""
        from pipeline_selection import missing_models

        self.assertEqual(
            missing_models(_models(), self._lookups(), retry_failed=True),
            ["V1", "V2", "V4"],
        )

    def test_h_a_stored_success_is_not_missing_under_the_flag(self):
        """(h) With retry_failed, a fully stored batch of successes still gives no work."""
        from pipeline_selection import missing_models

        stored = [NS(available=True, failure=False) for _ in range(N_MODELS)]
        self.assertEqual(missing_models(_models(), stored, retry_failed=True), [])

    def test_h_the_default_is_unchanged(self):
        """(h) Without retry_failed, or with it False, only the model with no row is missing."""
        from pipeline_selection import missing_models

        self.assertEqual(missing_models(_models(), self._lookups()), ["V2"])
        self.assertEqual(
            missing_models(_models(), self._lookups(), retry_failed=False), ["V2"]
        )

    def test_h_count_stored_failures(self):
        """(h) count_stored_failures counts the lookups that found a stored failure, and nothing else."""
        from pipeline_selection import count_stored_failures

        self.assertEqual(count_stored_failures(self._lookups()), 2)
        self.assertEqual(
            count_stored_failures(
                [NS(available=True, failure=False), NS(available=False)]
            ),
            0,
        )
        self.assertEqual(count_stored_failures([]), 0)


class TestRetryFailedScalarFlag(unittest.TestCase):
    def test_i_the_flag_parses(self):
        """(i) create_argument_parser() parses --retry-failed-scalar to True, and its absence to False."""
        from config.argument_parser import create_argument_parser

        base = ["--database", "unused.db"]

        args = create_argument_parser().parse_args(base)
        self.assertIs(args.retry_failed_scalar, False)

        args = create_argument_parser().parse_args(base + ["--retry-failed-scalar"])
        self.assertIs(args.retry_failed_scalar, True)
        # the two retry flags are independent
        self.assertIs(args.retry_failed_bbn, False)

    def test_i_main_py_help_lists_it(self):
        """(i) main.py --help lists --retry-failed-scalar. A subprocess, from the repository root; argparse exits before ray.init."""
        root = Path(__file__).resolve().parents[2]
        env = dict(os.environ, PYTHONPATH=str(root))
        result = subprocess.run(
            [sys.executable, "main.py", "--help"],
            cwd=root,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--retry-failed-scalar", result.stdout)
        help_text = " ".join(result.stdout.split())
        self.assertIn("--tuning-max-integrations", help_text)
        self.assertIn("a stored scalar failure counts as done", help_text)


class TestRetryFailedBBNFlag(unittest.TestCase):
    def test_g_the_flag_parses(self):
        """(g) create_argument_parser() parses --retry-failed-bbn to True, and its absence to False."""
        from config.argument_parser import create_argument_parser

        # --database is the one required argument; no config file is named, and the
        # parser declares no default config files
        base = ["--database", "unused.db"]

        args = create_argument_parser().parse_args(base)
        self.assertIs(args.retry_failed_bbn, False)

        args = create_argument_parser().parse_args(base + ["--retry-failed-bbn"])
        self.assertIs(args.retry_failed_bbn, True)


if __name__ == "__main__":
    unittest.main()
