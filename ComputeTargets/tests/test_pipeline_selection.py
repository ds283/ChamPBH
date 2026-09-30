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
Which work main.py's adiabatic and BBN stages schedule (run-integrity prompt 03,
README §6.3).

Before prompt 03 both stages built their downstream query from the ScalarModels
that did not fail, then zipped the results against the unfiltered shard bin. One
failed model shifted every later result by one, and `zip` dropped the last. The
decision now lives in `pipeline_selection`, which main.py's two stages call; it is
pure, so these tests reach it without main.py's argument parsing or Ray.

The lookup results are stand-ins carrying `available` and `failure`, as in
`prompts/run-integrity/planning-probes/pairing_probe.py`, which reproduces the old
logic and is the breakage record for (d)-(f): on the tree before prompt 03 it
schedules {V1, V3}. Test (g) reads the argument parser, and no config file.

No Ray cluster, no datastore, no PRyMordial solve. Instant.
"""

import unittest
from types import SimpleNamespace as NS

N_MODELS = 5
FAILED_MODEL = 1
HAS_BBN = {0, 3}


def _pairs():
    return [(f"V{i}", f"beta{i}") for i in range(N_MODELS)]


def _scalar_models():
    return [
        NS(i=i, available=True, failure=(i == FAILED_MODEL)) for i in range(N_MODELS)
    ]


def _bbn_results(entries, failed_bbn=()):
    """One stand-in BBN lookup result per entry, keyed on the entry's model."""
    results = []
    for entry in entries:
        i = entry.model.i
        if i in failed_bbn:
            results.append(NS(model=i, available=True, failure=True))
        elif i in HAS_BBN:
            results.append(NS(model=i, available=True, failure=False))
        else:
            results.append(NS(model=i, available=False, failure=None))
    return results


def _names(entries):
    return [entry.potential for entry in entries]


class TestPipelineSelection(unittest.TestCase):
    def test_d_the_probe_bin_schedules_V2_and_V4(self):
        """(d) Five pairs, model 1's ScalarModel failed, models 0 and 3 have BBN rows: missing is {V2, V4}."""
        from pipeline_selection import build_query_entries, select_missing

        pairs = _pairs()
        built = build_query_entries(pairs, _scalar_models())

        self.assertEqual(built.skipped_failed_models, 1)
        self.assertEqual(built.unavailable, [])
        self.assertEqual(_names(built.entries), ["V0", "V2", "V3", "V4"])

        # each entry carries the pair, and the model, it was built from
        for entry in built.entries:
            i = entry.model.i
            self.assertEqual((entry.potential, entry.coupling), pairs[i])

        selection = select_missing(built.entries, _bbn_results(built.entries))
        self.assertEqual(_names(selection.missing), ["V2", "V4"])
        self.assertEqual(selection.stored_failures, 0)
        for entry in selection.missing:
            self.assertEqual((entry.potential, entry.coupling), pairs[entry.model.i])

    def test_e_a_stored_failure_is_retried_only_under_the_flag(self):
        """(e) The same bin with V3's stored BBN row a failure: {V2, V4} without the flag, {V2, V3, V4} with it."""
        from pipeline_selection import build_query_entries, select_missing

        built = build_query_entries(_pairs(), _scalar_models())
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

        built = build_query_entries(_pairs(), _scalar_models())
        results = [NS(available=(entry.model.i in HAS_BBN)) for entry in built.entries]

        selection = select_missing(built.entries, results, retry_failed=False)
        self.assertEqual(_names(selection.missing), ["V2", "V4"])
        self.assertEqual(selection.stored_failures, 0)

    def test_e3_an_unavailable_scalar_model_is_reported_not_queried(self):
        """A ScalarModel lookup that found no row is reported in `unavailable`, and its `failure` is not read."""
        from pipeline_selection import build_query_entries

        models = _scalar_models()
        models[4] = NS(i=4, available=False)  # no `failure` attribute at all

        built = build_query_entries(_pairs(), models)
        self.assertEqual(built.unavailable, [("V4", "beta4")])
        self.assertEqual(built.skipped_failed_models, 1)
        self.assertEqual(_names(built.entries), ["V0", "V2", "V3"])

    def test_f_results_of_the_wrong_length_raise(self):
        """(f) Results one short raise ValueError, at either step; they are not truncated."""
        from pipeline_selection import build_query_entries, select_missing

        with self.assertRaises(ValueError):
            build_query_entries(_pairs(), _scalar_models()[:-1])

        built = build_query_entries(_pairs(), _scalar_models())
        results = _bbn_results(built.entries)
        with self.assertRaises(ValueError):
            select_missing(built.entries, results[:-1])
        with self.assertRaises(ValueError):
            select_missing(built.entries, results[:-1], retry_failed=True)

        # and one too many is refused as well
        with self.assertRaises(ValueError):
            select_missing(built.entries, results + [results[0]])


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
