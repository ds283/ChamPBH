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
--model {exponential, inverse-power} and --n on tools/history_and_bbn.py and tools/surfing_scan.py
(power-law-wall prompt 01 §1.4, README §6.3 P5 as amended).

history_and_bbn, through `main(argv)` with stdout captured and `compute_BBN_data` replaced by a stub (no
PRyMordial solve), as test_history_and_bbn_alpha does:
- (1) the defaults given explicitly (`--model exponential --n 1`) print test_history_and_bbn_alpha's
  PINNED_ALPHA_ZERO, byte for byte (masked as there): one untuned integration at M = 0.5, beta = 2, 4-5 s
  unloaded. With neither option given, that test and test_history_and_bbn pin the lines unedited;
- (2) `--model inverse-power --n 2`, untuned at the model's seed, M = 4e-31, beta = 2, to T_J = 100 MeV:
  the model integrated is InversePowerModel with n = 2, and every line's label is
  `beta=2 M=4e-31 model=inverse-power n=2`; a few seconds;
- (3) `--model exponential --n 2` and `--model inverse-power` (n = 1): the tuned path hands the model of
  that class and n to tune_history, and the label names both (a stand-in tuning that fails, no
  integration);
- (4) `--drain-law one_loop` with `--model inverse-power --n 3`: exit 2 with check_drain_law_model's message on
  stderr, before any tuning, integration or BBN; under the exponential the same options reach the tuning (since
  power-law-wall prompt 03 the one-loop law is defined on the power wall at n = 1 and 2, and refused at any other n);
- (5) `--n 0`, `--n x` and an unknown model are refused at parse, exit 2;
- (6) build_model, model_label and model_task_suffix.

surfing_scan, through `main(argv)` with `run_history` replaced by a stub, in a `tempfile` directory, as
test_surfing_scan's TestResume does, and `run_history` itself with `tune_history` replaced by a stand-in:
- (7) the defaults given explicitly write the same console lines, CSV and meta.json options as no option;
  no line names a model, and the options hold no "model" or "n";
- (8) `--model inverse-power --n 2`: the setup handed to run_history carries the model and n; the first
  console line and every beta's name `model=inverse-power n=2`; the CSV's header is COLUMNS (no column);
  the meta.json options record the model and n;
- (9) a CSV is resumed only by its own model and n: another is refused, exit 2, the file unchanged;
- (10) run_history builds the model of setup.model at setup.n and hands it to tune_history.

No Ray, no store, no PRyMordial.
"""

import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import tools.history_and_bbn as hb
import tools.surfing_scan as ss
from CosmologyConcepts.Models import ExponentialModel, InversePowerModel
from tools.tests.test_history_and_bbn_alpha import (
    FIXED,
    PINNED_ALPHA_ZERO,
    _mask,
    _run,
)
from tools.tests.test_surfing_scan import _StubRuns
from Units import Planck_units

_units = Planck_units()

# (2)'s point: the inverse-power-law model at M = 4e-31 M_P, beta = 2, n = 2, and the label it prints
IP_M = "4e-31"
IP_LABEL = "beta=2 M=4e-31 model=inverse-power n=2"


class TestHistoryAndBBNDefaults(unittest.TestCase):
    def test_1_the_defaults_given_explicitly_print_the_pinned_lines(self):
        """(1) One untuned integration at M = 0.5, beta = 2 (4-5 s unloaded)."""
        status, stdout, stderr = _run(FIXED + ["--model", "exponential", "--n", "1"])
        self.assertEqual(status, 0, stdout)
        self.assertEqual(stderr, "")
        self.assertEqual(_mask(stdout), PINNED_ALPHA_ZERO)
        self.assertNotIn("model=", stdout)


class TestHistoryAndBBNInversePower(unittest.TestCase):
    def test_2_every_label_names_the_model_and_n(self):
        """
        (2) Untuned at the seed of InversePowerModel(M = 4e-31 M_P, n = 2, beta = 2), to T_J = 100 MeV;
        a few seconds. Every line after the cosmology's own names the model and n in its label.
        """
        from ComputeTargets.tests.test_model_interface import cosmology

        seed = hb.build_model("inverse-power", 4.0e-31, 2, 2.0, _units).seed(
            cosmology()
        )
        calls = []
        original = hb.integrate_history

        def recording(*args, **kwargs):
            data = original(*args, **kwargs)
            calls.append(args)
            return data

        status, stdout, stderr = _run(
            [
                "2",
                IP_M,
                "--model",
                "inverse-power",
                "--n",
                "2",
                "--fixed-normalization",
                repr(seed.normalization),
                repr(seed.phi_ref / _units.PlanckMass),
                "--T-stop-GeV",
                "0.1",
            ],
            recording,
        )
        self.assertEqual(status, 0, stdout)
        self.assertEqual(stderr, "")
        realized = calls[0][6]
        self.assertIsInstance(realized.model, InversePowerModel)
        self.assertEqual(realized.model._n, 2)
        self.assertEqual(realized.normalization, seed.normalization)

        lines = [l for l in stdout.splitlines() if not l.startswith("@@ ")]
        self.assertGreater(len(lines), 5)
        for line in lines:
            with self.subTest(line=line):
                # the label follows the line's first word, before ": " (or a ratio line's window)
                rest = line.split(" ", 1)[1]
                self.assertTrue(
                    rest.startswith(IP_LABEL + ": ")
                    or rest.startswith(IP_LABEL + " ["),
                    line,
                )
        heads = {l.split(" ", 1)[0] for l in lines}
        self.assertEqual(
            heads, {"history", "bounce", "fixed_T", "ratio", "bbn"}, stdout
        )

    def test_3_the_tuned_path_hands_on_the_model_of_that_class_and_n(self):
        """(3) A stand-in tuning that fails: no integration."""
        failure = {"failure": True, "failure_reason": "stand-in"}
        for argv, cls, n, label in (
            (
                ["2", "0.5", "--model", "exponential", "--n", "2"],
                ExponentialModel,
                2,
                "history beta=2 M=0.5 model=exponential n=2: FAILURE",
            ),
            (
                ["2", "0.5", "--model", "inverse-power"],
                InversePowerModel,
                1,
                "history beta=2 M=0.5 model=inverse-power n=1: FAILURE",
            ),
            (["2", "0.5"], ExponentialModel, 1, "history beta=2 M=0.5: FAILURE"),
        ):
            with self.subTest(argv=argv):
                with mock.patch.object(
                    hb, "tune_history", return_value=failure
                ) as tune:
                    status, stdout, _ = _run(argv)
                self.assertEqual(status, 1)
                model = tune.call_args.args[1]
                self.assertIs(type(model), cls)
                self.assertEqual(model._n, n)
                self.assertEqual(model._beta_float, 2.0)
                self.assertTrue(stdout.splitlines()[-1].startswith(label), stdout)

    def test_4_the_one_loop_law_refuses_the_inverse_power_model_before_anything_runs(
        self,
    ):
        stub = mock.MagicMock()
        err, out = io.StringIO(), io.StringIO()
        with mock.patch.object(hb, "tune_history") as tune, mock.patch.object(
            hb, "integrate_history"
        ) as integrate, mock.patch.object(hb, "compute_BBN_data", stub):
            with contextlib.redirect_stderr(err), contextlib.redirect_stdout(out):
                status = hb.main(
                    [
                        "2",
                        IP_M,
                        "--model",
                        "inverse-power",
                        "--n",
                        "3",
                        "--drain-law",
                        "one_loop",
                        "--alpha",
                        "1",
                    ]
                )
        self.assertEqual(status, 2)
        # power-law-wall prompt 03: at n = 1 and 2 the model has its own one-loop law; at n = 3 it is undefined
        message = err.getvalue().strip()
        self.assertTrue(
            message.startswith(
                "!! history_and_bbn: history_and_bbn: the one-loop drain law: InversePowerModel (n=3) has no "
                "one-loop drain law: one_loop_drain_model is defined at n = 1 and 2 only"
            ),
            message,
        )
        self.assertEqual(len(message.splitlines()), 1)
        self.assertEqual(out.getvalue(), "")
        tune.assert_not_called()
        integrate.assert_not_called()
        self.assertEqual(stub.mock_calls, [])

        # the exponential under the same law reaches the tuning
        failure = {"failure": True, "failure_reason": "stand-in"}
        with mock.patch.object(hb, "tune_history", return_value=failure) as tune:
            status, _, _ = _run(
                ["2", IP_M, "--n", "2", "--drain-law", "one_loop", "--alpha", "1"]
            )
        self.assertEqual(status, 1)
        tune.assert_called_once()

    def test_5_bad_options_are_refused_at_parse(self):
        for argv, needle in (
            (["2", "0.5", "--n", "0"], "argument --n"),
            (["2", "0.5", "--n", "x"], "argument --n"),
            (["2", "0.5", "--model", "power"], "argument --model"),
        ):
            with self.subTest(argv=argv):
                err = io.StringIO()
                with contextlib.redirect_stderr(err), mock.patch.object(
                    hb, "tune_history"
                ) as tune:
                    with self.assertRaises(SystemExit) as ctx:
                        hb.main(argv)
                self.assertEqual(ctx.exception.code, 2)
                self.assertIn(needle, err.getvalue())
                tune.assert_not_called()

    def test_6_the_helpers(self):
        for name, cls in (
            ("exponential", ExponentialModel),
            ("inverse-power", InversePowerModel),
        ):
            m = hb.build_model(name, 1.0e-3, 2, 1.5, _units)
            self.assertIs(type(m), cls)
            self.assertEqual(m._n, 2)
            self.assertEqual(m._M_float, 1.0e-3 * _units.PlanckMass)
            self.assertEqual(m._beta_float, 1.5)
        self.assertEqual(hb.model_label("exponential", 1), "")
        self.assertEqual(hb.model_label("exponential", 2), " model=exponential n=2")
        self.assertEqual(hb.model_label("inverse-power", 1), " model=inverse-power n=1")
        self.assertEqual(hb.model_task_suffix("exponential", 1), "")
        self.assertEqual(hb.model_task_suffix("inverse-power", 2), "-inverse-power-n2")


class _RecordingStub(_StubRuns):
    """test_surfing_scan's stub, which also records the setup it is handed."""

    def __init__(self):
        super().__init__()
        self.setups = []

    def __call__(self, setup, *args, **kwargs):
        self.setups.append(setup)
        return super().__call__(setup, *args, **kwargs)


class TestSurfingScan(unittest.TestCase):
    def setUp(self):
        self.assertTrue((Path(os.getcwd()) / ss.EOS_TABLE).is_file())
        self._tmp = tempfile.TemporaryDirectory()
        self.out = Path(self._tmp.name) / "scan.csv"
        self.meta = Path(str(self.out) + ".meta.json")
        self.stub = _RecordingStub()
        self._saved = ss.run_history
        ss.run_history = self.stub

    def tearDown(self):
        ss.run_history = self._saved
        self._tmp.cleanup()

    def run_main(self, *argv) -> int:
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = ss.main(["--out", str(self.out), *argv])
        self.stdout = buffer.getvalue()
        return code

    def clear(self):
        self.out.unlink()
        self.meta.unlink()

    def options(self):
        return json.loads(self.meta.read_text())["runs"][-1]["options"]

    def test_7_the_defaults_given_explicitly_are_no_option(self):
        self.assertEqual(self.run_main("--beta-values", "1.2", "1.3", "1.4"), 1)
        plain = (self.stdout, self.out.read_bytes(), self.options())
        self.clear()
        self.assertEqual(
            self.run_main(
                "--beta-values",
                "1.2",
                "1.3",
                "1.4",
                "--model",
                "exponential",
                "--n",
                "1",
            ),
            1,
        )
        explicit = (self.stdout, self.out.read_bytes(), self.options())
        self.assertEqual(plain, explicit)
        self.assertNotIn("model=", plain[0])
        self.assertNotIn("model", plain[2])
        self.assertNotIn("n", plain[2])
        for setup in self.stub.setups:
            self.assertEqual((setup.model, setup.n), ("exponential", 1))

    def test_8_the_inverse_power_model_is_named(self):
        self.assertEqual(
            self.run_main(
                "--beta-values",
                "1.2",
                "1.3",
                "1.4",
                "--model",
                "inverse-power",
                "--n",
                "2",
            ),
            1,
        )
        for setup in self.stub.setups:
            self.assertEqual((setup.model, setup.n), ("inverse-power", 2))
        lines = self.stdout.splitlines()
        self.assertTrue(lines[0].startswith("-- surfing_scan: "))
        self.assertTrue(lines[0].endswith(" model=inverse-power n=2"), lines[0])
        betas = [l for l in lines if l.startswith("beta=")]
        self.assertEqual(len(betas), 3)
        for line, beta in zip(betas, ("1.2", "1.3", "1.4")):
            self.assertTrue(
                line.startswith(f"beta={beta} model=inverse-power n=2 status="), line
            )
        with open(self.out) as f:
            self.assertEqual(f.readline().rstrip("\n").split(","), ss.COLUMNS)
        options = self.options()
        self.assertEqual(options["model"], "inverse-power")
        self.assertEqual(options["n"], 2)

    def test_9_a_csv_is_resumed_only_by_its_own_model_and_n(self):
        self.assertEqual(self.run_main("--beta-values", "1.2"), 0)
        before = self.out.read_bytes()
        for extra in (["--model", "inverse-power"], ["--n", "2"]):
            with self.subTest(extra=extra):
                self.assertEqual(self.run_main("--beta-values", "1.5", *extra), 2)
                self.assertIn("was written with model=exponential n=1", self.stdout)
                self.assertEqual(self.out.read_bytes(), before)
        self.assertEqual(self.stub.calls, [1.2])

        self.clear()
        argv = ("--model", "inverse-power", "--n", "2")
        self.assertEqual(self.run_main("--beta-values", "1.2", *argv), 0)
        self.assertEqual(self.run_main("--beta-values", "1.5"), 2)
        self.assertIn("was written with model=inverse-power n=2", self.stdout)
        # its own model and n resume it
        self.assertEqual(self.run_main("--beta-values", "1.2", "1.5", *argv), 0)
        self.assertEqual(self.stub.calls, [1.2, 1.2, 1.5])


class TestSurfingRunHistory(unittest.TestCase):
    def test_10_run_history_builds_the_setups_model(self):
        failure = {"failure": True, "failure_reason": "stand-in"}
        units = _units
        for model, n, cls in (
            ("inverse-power", 2, InversePowerModel),
            ("exponential", 1, ExponentialModel),
        ):
            with self.subTest(model=model):
                setup = SimpleNamespace(units=units, cosmology=None, model=model, n=n)
                with mock.patch.object(
                    ss, "tune_history", return_value=failure
                ) as tune:
                    outcome = ss.run_history(setup, 2.5, 5.0, 4.0e-31, 1.0e-3)
                self.assertEqual(outcome.failure_reason, "stand-in")
                built = tune.call_args.args[1]
                self.assertIs(type(built), cls)
                self.assertEqual(built._n, n)
                self.assertEqual(built._beta_float, 2.5)
                suffix = "" if model == "exponential" else f"-{model}-n{n}"
                self.assertEqual(
                    tune.call_args.kwargs["task_label"],
                    f"surfing_scan-beta2.5-M4e-31{suffix}",
                )
        # a setup without the attributes (made before this prompt) is the exponential at n = 1
        setup = SimpleNamespace(units=units, cosmology=None)
        with mock.patch.object(ss, "tune_history", return_value=failure) as tune:
            ss.run_history(setup, 2.5, 5.0, 4.0e-31, 1.0e-3)
        self.assertIs(type(tune.call_args.args[1]), ExponentialModel)
        self.assertEqual(tune.call_args.args[1]._n, 1)


if __name__ == "__main__":
    unittest.main()
