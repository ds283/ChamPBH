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
tools/history_and_bbn.py --alpha (inelastic-reflection prompt 10, test 10, P10, rulings 7 and 9),
through `main(argv)` with stdout captured and `compute_BBN_data` replaced by a stub, as
`test_history_and_bbn` does, so that no PRyMordial solve runs.

- `--alpha` parses, and 1.0 (also -0.1 and nan) is refused at parse, exit 2, naming the option;
- at alpha = 0 every line is byte-identical to the dispatch commit's (`d097745`, the commit before
  this prompt's code): `PINNED_ALPHA_ZERO` is the tool's whole stdout there for
  `2 0.5 --fixed-normalization -277.3027772506518 0.7439937496556057` (M = 0.5, beta = 2,
  test_injection's setup, untuned, the model's seed realization), the numbers of `wall=` and
  `PRyM_time=` replaced by `#`, and nothing else masked. It was taken by running
  `PYTHONPATH=. ./venv/bin/python pins.py` (the same call, through `main` with the stub) from a
  temporary `git worktree` of `d097745`, and the worktree removed. The normalization and phi_ref
  are the seed's (`ExponentialModel.seed`), given as their repr. No `alpha=` and no `dissipation`
  line appears at alpha = 0;
- at alpha = 0.1 the history line gains `alpha=0.1` just before `wall=`, a `dissipation` line
  follows the bounce line with every field of `dissipation_values(data)` in order, and each printed
  value equals `float(f"{x:.6g}")` of the field (the two ln T_J fields minus ln GeV, under their
  `_GeV` names), None as `None`; the tuned path hands alpha to `tune_history`.

One untuned integration at M = 0.5, beta = 2 at alpha = 0 and one at alpha = 0.1 (4-5 s each
unloaded, about 10 s at a load of 8). No Ray, no store, no PRyMordial.
"""

import contextlib
import io
import re
import unittest
from math import log
from unittest import mock

import tools.history_and_bbn as hb
from ComputeTargets.ScalarHistory import DissipationValues, dissipation_values

NORMALIZATION = "-277.3027772506518"
PHI_REF_MP = "0.7439937496556057"
FIXED = ["2", "0.5", "--fixed-normalization", NORMALIZATION, PHI_REF_MP]

_BBN = {
    "Yp_BBN": 0.247,
    "DOverH": 2.5,
    "He3OverH": 1.0,
    "Li7OverH": 5.0,
    "BBN_compute_time": 0.0,
    "PRyM_version": "stub",
}

PINNED_ALPHA_ZERO = """\
@@ Parametrized equation-of-state LambdaCDM-like model "QCD equation of state based on Saikawa & Shirai parametrization (arXiv:1803.01038), with adjustments (splined) | Planck2018 68% central values TT+TE+EE+lowP+lensing+BAO"
history beta=2 M=0.5: RHS=33838 accepted_steps=3725 reflections=0 samples=5287 integrations=1 wall=# s
bounce beta=2 M=0.5: N=20.342271137 T_J=746.622784 MeV phi=4.960119e-03 reflected=False
fixed_T beta=2 M=0.5: 1MeV crossings=1 phi=1.243342069e-02 ratio=-9.512453022e-01 stand_in_phi=1.243304736e-02 stand_in_ratio=-9.512454771e-01 | 70keV crossings=1 phi=1.011759728e-02 ratio=-9.471102694e-01 stand_in_phi=1.011743502e-02 stand_in_ratio=-9.471100909e-01
ratio beta=2 M=0.5 [0.3,1) keV: n=131 min=-0.9476 median=-0.9467 max=-0.9463 rms_step=1.13e-05
ratio beta=2 M=0.5 [1,3) keV: n=119 min=-0.9463 median=-0.9461 max=-0.946 rms_step=3.486e-06
ratio beta=2 M=0.5 [3,10) keV: n=130 min=-0.9464 median=-0.9463 max=-0.9461 rms_step=2.994e-06
ratio beta=2 M=0.5 [10,100) keV: n=257 min=-0.9503 median=-0.9463 max=-0.9435 rms_step=0.0003283
bbn beta=2 M=0.5: Yp=0.247 DoH=2.5 He3oH=1 Li7oH=5 network=small PRyM_time=# s wall=# s PRyM_version=stub
"""


class _StubBBN:
    def _function(self, proxy, **kwargs):
        return dict(_BBN)


def _mask(text):
    return re.sub(r"(wall|PRyM_time)=[0-9.]+ s", r"\1=# s", text)


def _run(argv, integrate_spy=None):
    out, err = io.StringIO(), io.StringIO()
    with mock.patch.object(hb, "compute_BBN_data", _StubBBN()):
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            if integrate_spy is None:
                status = hb.main(argv)
            else:
                with mock.patch.object(hb, "integrate_history", integrate_spy):
                    status = hb.main(argv)
    return status, out.getvalue(), err.getvalue()


def _line(stdout, head):
    lines = [l for l in stdout.splitlines() if l.startswith(head)]
    if len(lines) != 1:
        raise AssertionError(f"{len(lines)} lines starting {head!r} in:\n{stdout}")
    return lines[0]


class TestTheOption(unittest.TestCase):
    def test_10_alpha_out_of_range_is_refused_at_parse(self):
        for value in ("1.0", "-0.1", "nan"):
            with self.subTest(value=value):
                err = io.StringIO()
                with contextlib.redirect_stderr(err), mock.patch.object(
                    hb, "integrate_history"
                ) as integrate, mock.patch.object(hb, "tune_history") as tune:
                    with self.assertRaises(SystemExit) as ctx:
                        hb.main(["2", "0.5", "--alpha", value])
                self.assertEqual(ctx.exception.code, 2)
                self.assertIn("argument --alpha: must be a fraction", err.getvalue())
                integrate.assert_not_called()
                tune.assert_not_called()

    def test_10_the_tuned_path_hands_alpha_to_the_tuning(self):
        failure = {"failure": True, "failure_reason": "stand-in"}
        for argv, alpha in ((["2", "0.5"], 0.0), (["2", "0.5", "--alpha", "0.1"], 0.1)):
            with self.subTest(alpha=alpha):
                with mock.patch.object(
                    hb, "tune_history", return_value=failure
                ) as tune:
                    status, stdout, _ = _run(argv)
                self.assertEqual(status, 1)
                self.assertEqual(tune.call_args.kwargs["alpha"], alpha)
                self.assertIn("FAILURE", stdout)


class TestAlphaZeroIsUnchanged(unittest.TestCase):
    def test_10_every_line_is_the_dispatch_commits(self):
        status, stdout, stderr = _run(FIXED)
        self.assertEqual(status, 0, stdout)
        self.assertEqual(stderr, "")
        self.assertEqual(_mask(stdout), PINNED_ALPHA_ZERO)
        self.assertNotIn("alpha=", stdout)
        self.assertNotIn("dissipation", stdout)
        # giving the default is giving nothing
        status, again, _ = _run(FIXED + ["--alpha", "0"])
        self.assertEqual(_mask(again), PINNED_ALPHA_ZERO)


class TestAlphaPositive(unittest.TestCase):
    def test_10_the_history_line_names_alpha_and_the_dissipation_line_parses_back(self):
        calls = []
        original = hb.integrate_history

        def recording(*args, **kwargs):
            data = original(*args, **kwargs)
            calls.append((args, kwargs, data))
            return data

        status, stdout, _ = _run(FIXED + ["--alpha", "0.1"], recording)
        self.assertEqual(status, 0, stdout)
        self.assertEqual(len(calls), 1)
        args, kwargs, data = calls[0]
        self.assertEqual(kwargs["alpha"], 0.1)

        history = _line(stdout, "history beta=2 M=0.5: ")
        self.assertRegex(history, r"integrations=1 alpha=0\.1 wall=[0-9.]+ s$")
        lines = stdout.splitlines()
        # the dissipation line follows the bounce line
        bounce = next(i for i, l in enumerate(lines) if l.startswith("bounce "))
        self.assertTrue(lines[bounce + 1].startswith("dissipation beta=2 M=0.5: "))

        printed = dict(
            item.split("=", 1)
            for item in _line(stdout, "dissipation beta=2 M=0.5: ")
            .split(": ", 1)[1]
            .split(" ")
        )
        values = dissipation_values(data)
        units = args[0].units
        expected = {}
        for name in DissipationValues._fields:
            value = getattr(values, name)
            if name in ("parking_log_T_Jordan", "release_log_T_Jordan"):
                name = name + "_GeV"
                if value is not None:
                    value = value - log(units.GeV)
            expected[name] = value
        # every field, in order
        self.assertEqual(list(printed), list(expected))
        none_seen = False
        for name, value in expected.items():
            with self.subTest(field=name):
                if value is None:
                    none_seen = True
                    self.assertEqual(printed[name], "None")
                else:
                    self.assertEqual(float(printed[name]), float(f"{value:.6g}"))
        # this history neither parks nor is released (log 09 test 1's point): the None path is
        # exercised, and the counts are not None
        self.assertTrue(none_seen)
        self.assertIsNone(values.parking_N)
        self.assertIsNone(values.release_N)
        self.assertGreater(values.injections, 0)


if __name__ == "__main__":
    unittest.main()
