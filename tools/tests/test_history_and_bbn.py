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
tools/history_and_bbn.py in both of its modes (jordan-normalization prompt 08, D4, D7), through
`main(argv)` with stdout captured and `compute_BBN_data` replaced by a stub, so that no
PRyMordial solve runs (one would cost about 10 s).

(a) tuned at beta = 3, M = 0.01 (the tuned golden history's point and setup): the tuning line's
    values equal golden_history_06_tuned.json's `tuning` record bitwise, key for key, and the
    history line's counts its `history` record's. Five integrations to T_CMB: 22 s at load 4.
(b) --fixed-normalization at that record's normalization and phi_ref, given as their repr: one
    integrate_history call, whose realized model holds exactly those values, to T_CMB; the
    history line's samples, steps and RHS evaluations equal the record's (the tuning's final
    integration, the same integration). 4.6 s at load 4.
(c) --T-stop-GeV without --fixed-normalization: exit 2 with the message, no integration.

No Ray, no store. Every test runs from the repository root, as the suites do.
"""

import contextlib
import io
import json
import unittest
from pathlib import Path
from unittest import mock

import tools.history_and_bbn as hb
from ComputeTargets.ScalarHistory import TUNING_OUTPUT_KEYS

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "ComputeTargets/tests/data/golden_history_06_tuned.json"

# the stub's abundances: what compute_BBN_data._function returns, with no solve
_BBN = {
    "Yp_BBN": 0.247,
    "DOverH": 2.5,
    "He3OverH": 1.0,
    "Li7OverH": 5.0,
    "BBN_compute_time": 0.0,
    "PRyM_version": "stub",
}


def _golden():
    return json.loads(GOLDEN.read_text())


class _StubBBN:
    """A stand-in for compute_BBN_data: its `_function` records the stand-in model and returns
    fixed abundances, with no PRyMordial solve."""

    def __init__(self):
        self.calls = []

    def _function(self, proxy, **kwargs):
        self.calls.append((proxy.get(), kwargs))
        return dict(_BBN)


def _run(argv):
    """main(argv) with the BBN stub; (exit status, stdout, stderr, the stub)."""
    stub = _StubBBN()
    out, err = io.StringIO(), io.StringIO()
    with mock.patch.object(hb, "compute_BBN_data", stub):
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            status = hb.main(argv)
    return status, out.getvalue(), err.getvalue(), stub


def _line(stdout, head):
    lines = [l for l in stdout.splitlines() if l.startswith(head)]
    if len(lines) != 1:
        raise AssertionError(f"{len(lines)} lines starting {head!r} in:\n{stdout}")
    return lines[0]


def _fields(line):
    """key=value pairs after the line's ': ' (a unit such as the "s" after wall=... is not one)."""
    return dict(
        item.split("=", 1) for item in line.split(": ", 1)[1].split() if "=" in item
    )


class TestTuned(unittest.TestCase):
    def test_a_the_tuning_line_is_the_tuned_goldens_record_bitwise(self):
        """
        (a) tuned at beta = 3, M = 0.01: five integrations to T_CMB, 22 s at load 4 (no
        PRyMordial; the BBN stage is a stub). The tuning line's every value equals the golden
        record's exactly: floats through float.fromhex, ints and strings as they are.
        """
        golden = _golden()
        status, stdout, _, stub = _run(["3", "0.01"])
        self.assertEqual(status, 0, stdout)

        tunes = [l for l in stdout.splitlines() if l.startswith("-- tune ")]
        self.assertEqual(len(tunes), golden["tuning"]["tuning_iterations"])

        fields = _fields(_line(stdout, "tuning beta=3 M=0.01: "))
        self.assertEqual(list(fields), list(TUNING_OUTPUT_KEYS))
        for key, recorded in golden["tuning"].items():
            with self.subTest(key=key):
                if isinstance(recorded, str) and recorded.startswith(("0x", "-0x")):
                    self.assertEqual(float(fields[key]), float.fromhex(recorded))
                elif isinstance(recorded, int):
                    self.assertEqual(int(fields[key]), recorded)
                else:
                    self.assertEqual(fields[key], recorded)

        history = _fields(_line(stdout, "history beta=3 M=0.01: "))
        self.assertEqual(int(history["samples"]), golden["history"]["n_samples"])
        self.assertEqual(
            int(history["accepted_steps"]), golden["history"]["accepted_steps"]
        )
        self.assertEqual(int(history["integrations"]), 5)

        # the BBN stand-in ends where the history ends, at T_CMB
        self.assertEqual(len(stub.calls), 1)
        stand_in, _ = stub.calls[0]
        cosmology = stand_in._cosmology
        self.assertEqual(
            stand_in.T_Jordan_stop.as_float,
            cosmology.T_CMB_Kelvin * cosmology.units.Kelvin,
        )
        self.assertTrue(
            _line(stdout, "bbn beta=3 M=0.01: ").endswith("PRyM_version=stub")
        )


class TestFixedNormalization(unittest.TestCase):
    def test_b_one_integration_at_the_given_normalization_and_phi_ref(self):
        """
        (b) --fixed-normalization at the golden record's normalization and phi_ref (their repr):
        one integrate_history call to T_CMB, 4.6 s at load 4 (no PRyMordial; the BBN stage is a
        stub). Its realized model holds exactly those values, and the history line's counts are
        the golden record's: it is the tuning's final integration.
        """
        golden = _golden()
        normalization = float.fromhex(golden["tuning"]["normalization"])
        phi_ref = float.fromhex(golden["tuning"]["phi_ref"])

        calls = []
        original = hb.integrate_history

        def recording(*args, **kwargs):
            calls.append((args, kwargs))
            return original(*args, **kwargs)

        with mock.patch.object(hb, "integrate_history", recording):
            status, stdout, _, _ = _run(
                [
                    "3",
                    "0.01",
                    "--fixed-normalization",
                    repr(normalization),
                    repr(phi_ref),
                ]
            )
        self.assertEqual(status, 0, stdout)
        self.assertEqual(len(calls), 1)
        args, _ = calls[0]
        realized = args[6]
        self.assertEqual(realized.normalization, normalization)
        self.assertEqual(realized.phi_ref, phi_ref * realized.model.units.PlanckMass)
        cosmology = args[0]
        self.assertEqual(
            args[2].as_float, cosmology.T_CMB_Kelvin * cosmology.units.Kelvin
        )

        self.assertNotIn("-- tune ", stdout)
        self.assertFalse(any(l.startswith("tuning ") for l in stdout.splitlines()))
        history = _fields(_line(stdout, "history beta=3 M=0.01: "))
        self.assertEqual(int(history["samples"]), golden["history"]["n_samples"])
        self.assertEqual(
            int(history["accepted_steps"]), golden["history"]["accepted_steps"]
        )
        self.assertEqual(int(history["RHS"]), golden["history"]["RHS_evaluations"])
        self.assertEqual(int(history["integrations"]), 1)


class TestRefusals(unittest.TestCase):
    def test_c_T_stop_without_fixed_normalization_is_refused(self):
        """(c) exit 2, the message on stderr, and nothing integrated."""
        with mock.patch.object(hb, "tune_history") as tune, mock.patch.object(
            hb, "integrate_history"
        ) as integrate:
            status, stdout, stderr, stub = _run(["3", "0.01", "--T-stop-GeV", "1e-3"])
        self.assertEqual(status, 2)
        self.assertEqual(
            stderr.strip(),
            "!! history_and_bbn: --T-stop-GeV is accepted only with "
            "--fixed-normalization; a tuned history ends at T_CMB",
        )
        self.assertEqual(stdout, "")
        tune.assert_not_called()
        integrate.assert_not_called()
        self.assertEqual(stub.calls, [])


if __name__ == "__main__":
    unittest.main()
