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
tools/history_and_bbn.py --drain-law (one-loop-drain prompt 01 §1.5, P25; prompt 02c §1.4), through
`main(argv)` with stdout captured and `compute_BBN_data` replaced by a stub, as test_history_and_bbn_alpha
does, so that no PRyMordial solve runs.

- `--drain-law one_loop` takes `--alpha` as A: finite and > 0, with no upper bound; A missing, 0, < 0,
  nan or inf is refused at parse, exit 2, naming the option, before any integration. Under the fixed law
  --alpha is checked as before (test_history_and_bbn_alpha pins its message);
- `--gap-prescription`, removed by one-loop-drain prompt 02c with the gap prescriptions (README §8.10), is
  refused as an unknown argument, exit 2;
- the tuned path hands the law and A to tune_history;
- `drain_values` on a constructed payload: the counts by regime, the shares of K(N_final) by regime and
  the parking leftover's, ell_min and the unresolvable assessments;
- one untuned integration under one_loop at M = 0.5, beta = 2 (test_history_and_bbn_alpha's point, 4-5 s
  unloaded, about 10 s at a load of 8): the history line names `drain_law=one_loop A=1
  model=one-loop-measured-v1`, and a
  `drain` line follows the dissipation line and parses back to drain_values of the payload.

Under the fixed law every line is as before: test_history_and_bbn and test_history_and_bbn_alpha pin
them, unedited. No Ray, no store, no PRyMordial.
"""

import contextlib
import importlib
import io
import unittest
from types import SimpleNamespace
from unittest import mock

import tools.history_and_bbn as hb
from tools.tests.test_history_and_bbn_alpha import FIXED, _line, _run

SM = importlib.import_module("ComputeTargets.ScalarHistory")


class TestTheOptions(unittest.TestCase):
    def _refused(self, argv, needle):
        err = io.StringIO()
        with contextlib.redirect_stderr(err), mock.patch.object(
            hb, "integrate_history"
        ) as integrate, mock.patch.object(hb, "tune_history") as tune:
            with self.assertRaises(SystemExit) as ctx:
                hb.main(argv)
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(needle, err.getvalue())
        integrate.assert_not_called()
        tune.assert_not_called()

    def test_5_A_is_refused_unless_finite_and_positive(self):
        for value in ("0", "-0", "-1", "nan", "inf", "-inf", "x"):
            with self.subTest(value=value):
                self._refused(
                    ["2", "0.5", "--drain-law", "one_loop", "--alpha", value],
                    "argument --alpha: ",
                )

    def test_5_one_loop_needs_A(self):
        self._refused(["2", "0.5", "--drain-law", "one_loop"], "argument --alpha: ")

    def test_5_an_unknown_law_is_refused(self):
        self._refused(["2", "0.5", "--drain-law", "elastic"], "--drain-law")

    def test_5_the_gap_prescription_is_an_unknown_argument(self):
        """Removed by prompt 02c (README §8.10, decision 2): argparse refuses it as unrecognized."""
        for extra in (
            [],
            ["--drain-law", "fixed"],
            ["--drain-law", "one_loop", "--alpha", "1"],
        ):
            for value in ("zero", "edge"):
                with self.subTest(extra=extra, value=value):
                    self._refused(
                        ["2", "0.5", "--gap-prescription", value] + extra,
                        "unrecognized arguments: --gap-prescription",
                    )

    def test_5_the_fixed_laws_alpha_is_checked_as_before(self):
        self._refused(
            ["2", "0.5", "--alpha", "1.5"], "argument --alpha: must be a fraction"
        )
        self._refused(["2", "0.5", "--alpha", "x"], "argument --alpha: invalid")

    def test_5_the_tuned_path_hands_the_law_and_A_on(self):
        failure = {"failure": True, "failure_reason": "stand-in"}
        seen = []

        def tune(*args, **kwargs):
            seen.append(kwargs)
            return failure

        for argv, law, A in (
            (["2", "0.5"], "fixed", 0.0),
            (["2", "0.5", "--alpha", "0.1"], "fixed", 0.1),
            (
                ["2", "0.5", "--drain-law", "one_loop", "--alpha", "2.5"],
                "one_loop",
                2.5,
            ),
            (
                ["2", "0.5", "--drain-law", "one_loop", "--alpha", "0.1"],
                "one_loop",
                0.1,
            ),
        ):
            with self.subTest(argv=argv):
                seen.clear()
                with mock.patch.object(hb, "tune_history", tune):
                    status, stdout, _ = _run(argv)
                self.assertEqual(status, 1)
                kwargs = seen[0]
                self.assertEqual(kwargs["drain_law"], law)
                self.assertEqual(kwargs["alpha"], A)
                self.assertIn("FAILURE", stdout)


def _injection(delta_K):
    return SimpleNamespace(delta_K=delta_K)


class _Ledger:
    def __init__(self, K):
        self._K = K

    def K(self, N):
        return self._K


class TestTheDrainValues(unittest.TestCase):
    def test_5_counts_shares_ell_min_and_the_count(self):
        candidates = (
            SM.DrainCandidate("one_loop", 12.0, 0.2, 1.0, -300.0, -80.0, 30.0, 40.0),
            SM.DrainCandidate("one_loop", 9.5, 0.28, 1.0, -301.0, -81.0, 26.0, 35.0),
            SM.DrainCandidate("gap", None, 0.0, 1.0, -302.0, -82.0, 3.0, 1.0),
            SM.DrainCandidate("classical", None, 0.0, 0.5, -303.0, -83.0, 2.0, -1.0),
            SM.DrainCandidate("unassessed", None, 0.0, 1.0, None, -84.0, None, None),
        )
        data = {
            "drain": SM.DrainRecord(
                "one_loop", 1.0, "one-loop-measured-v1", 3, candidates, 2
            ),
            "injections": [_injection(k) for k in (2.0, 5.0, 0.0, 0.0, 0.0)],
            "parking": SimpleNamespace(delta_K=1.0),
            "dark_radiation": _Ledger(8.0),
        }
        values = hb.drain_values(data, SimpleNamespace(N_final=40.0))
        self.assertEqual(
            values,
            {
                "candidates": 5,
                "one_loop": 2,
                "gap": 1,
                "classical": 1,
                "unassessed": 1,
                "K_share_one_loop": 7.0 / 8.0,
                "K_share_gap": 0.0,
                "K_share_classical": 0.0,
                "K_share_unassessed": 0.0,
                "K_share_parking": 1.0 / 8.0,
                "ell_min": 9.5,
                "unresolvable_assessments": 3,
                "unresolved_declined": 2,
            },
        )

    def test_5_nothing_drained(self):
        candidates = (
            SM.DrainCandidate("classical", None, 0.0, 1.0, -303.0, -83.0, 2.0, -50.0),
        )
        data = {
            "drain": SM.DrainRecord(
                "one_loop", 1.0, "one-loop-measured-v1", 0, candidates, 0
            ),
            "injections": [_injection(0.0)],
            "parking": None,
            "dark_radiation": _Ledger(0.0),
        }
        values = hb.drain_values(data, SimpleNamespace(N_final=40.0))
        self.assertIsNone(values["ell_min"])
        for regime in SM.DRAIN_REGIMES:
            self.assertIsNone(values[f"K_share_{regime}"])
        self.assertIsNone(values["K_share_parking"])
        self.assertEqual(values["classical"], 1)


class TestAOneLoopRun(unittest.TestCase):
    def test_5_the_history_line_and_the_drain_line(self):
        calls = []
        original = hb.integrate_history

        def recording(*args, **kwargs):
            data = original(*args, **kwargs)
            calls.append((args, kwargs, data))
            return data

        status, stdout, _ = _run(
            FIXED + ["--drain-law", "one_loop", "--alpha", "1"], recording
        )
        self.assertEqual(status, 0, stdout)
        args, kwargs, data = calls[0]
        self.assertEqual(kwargs["alpha"], 1.0)
        self.assertEqual(kwargs["drain_law"], "one_loop")
        self.assertEqual(data["drain"].law, "one_loop")
        self.assertEqual(data["drain"].model, "one-loop-measured-v1")

        history = _line(stdout, "history beta=2 M=0.5: ")
        self.assertRegex(
            history,
            r"integrations=1 drain_law=one_loop A=1 model=one-loop-measured-v1 "
            r"wall=[0-9.]+ s$",
        )
        self.assertNotIn("alpha=", history)
        lines = stdout.splitlines()
        dissipation = next(
            i for i, l in enumerate(lines) if l.startswith("dissipation ")
        )
        self.assertTrue(lines[dissipation + 1].startswith("drain beta=2 M=0.5: "))
        printed = dict(
            item.split("=", 1)
            for item in _line(stdout, "drain beta=2 M=0.5: ")
            .split(": ", 1)[1]
            .split(" ")
        )
        self.assertEqual(
            list(printed),
            [
                "candidates",
                "one_loop",
                "gap",
                "classical",
                "unassessed",
                "K_share_one_loop",
                "K_share_gap",
                "K_share_classical",
                "K_share_unassessed",
                "K_share_parking",
                "ell_min",
                "unresolvable_assessments",
                "unresolved_declined",
            ],
        )
        self.assertEqual(int(printed["candidates"]), len(data["injections"]))


if __name__ == "__main__":
    unittest.main()
