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
The outward-step scanner, tools/outward_step_scan.py (reflection-restart prompt 01 §1.1; README §6.2
P1, P2, P3; made a tool on the production code by prompt 03).

- **The flag** on synthetic step quantities: each condition alone, neither, a sign change, an inward
  step; and the scanner's flag against ScalarHistory's (`_outward_step_flagged`, the guard's
  predicate since prompt 03) on synthetic interpolants.
- **The observer** on a stub integration: a stand-in solver class observed as the scanner observes
  Radau, built from functions named as integrate_scalar_history's are, so that what started each
  instance is read from the frames as in a real history; the counts and the summary, the ruled
  signature's among them (kept, and discarded by a retake); the patch is gone after `observe`; a task
  that names one of prompt 01's probe variants is refused.
- **The batch** on stub tasks (mode "stub", a function of this module run in the worker): one line
  per task, the resume (a task whose line exists is not run again), a task that raises is recorded
  with its traceback and the batch goes on, numpy values in a record (a payload's 0-d arrays) are written, a task past its wall-clock limit is recorded as a
  timeout, the pid file and the last log line.
- **The store comparison** on a temporary store built by `_TempStoreCase`
  (ObjectFactories/tests/test_dissipation_store.py's `_DissipationStoreCase`): a dissipation history
  (M = 0.5 M_P, beta = 2, alpha = 0.1, untuned) written by the factory compares bitwise with its own
  payload; a sample value, a history column and an extra_data key changed in the file are each
  reported; the store selection finds the history with its stored normalization, and the scanner's
  store mode re-integrates it bitwise; a path that cannot be opened read-only is refused.

No Ray, no production store. Cost: the flag and observer tests well under a second; the batch tests
about 10-20 s (one spawned worker per stub task, a 2 s timeout); the store tests two integrations of
2-6 s each unloaded (one shared with ObjectFactories' tests' cache when run in one process), longer
under load.
"""

import contextlib
import glob
import importlib
import io
import json
import os
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

import numpy as np

from tools import outward_step_scan as oss

SM = importlib.import_module("ComputeTargets.ScalarHistory")

F = 0.1


# ---- stub tasks for the batch, run in a spawned worker ---------------------------------------------


def stub_ok(spec):
    marker = spec.get("marker")
    if marker:
        with open(marker, "a") as f:
            f.write(spec["key"] + "\n")
    return {"outcome": "success", "value": spec.get("value")}


def stub_numpy(spec):
    # a payload's sample fields can be 0-d numpy arrays (gstar_rho, gstar_s)
    return {"outcome": "success", "value": np.array(1.5), "values": np.arange(3.0)}


def stub_raise(spec):
    raise RuntimeError("a stub that raises")


def stub_sleep(spec):
    time.sleep(60.0)
    return {"outcome": "success"}


# ---- the flag ---------------------------------------------------------------------------------------


class TestTheFlag(unittest.TestCase):
    def test_the_ratio_condition_alone(self):
        c = oss.classify_step(1.0, 1.0, 0.1, False, 0.95, F)
        self.assertEqual(c, {"ratio": True, "phi": False, "flagged": True})

    def test_the_phi_condition_alone(self):
        c = oss.classify_step(1.0, 1.0, 0.99, False, 0.5, F)
        self.assertEqual(c, {"ratio": False, "phi": True, "flagged": True})

    def test_neither(self):
        c = oss.classify_step(1.0, 1.0, 0.99, False, 1.0, F)
        self.assertEqual(c, {"ratio": False, "phi": False, "flagged": False})

    def test_the_thresholds_are_strict(self):
        self.assertFalse(oss.classify_step(1.0, 1.0, 0.5, False, 0.9, F)["flagged"])
        self.assertTrue(oss.classify_step(1.0, 1.0, 0.4999, False, 0.9, F)["ratio"])
        self.assertTrue(oss.classify_step(1.0, 1.0, 1.0, False, 0.8999, F)["phi"])

    def test_a_sign_change_is_not_the_ratio_condition(self):
        # a turning point inside the step: pi_end/pi_start is small or negative, but pi changed sign
        self.assertFalse(oss.classify_step(1.0, 1.0, -0.5, True, 0.95, F)["flagged"])
        self.assertFalse(oss.classify_step(1.0, 1.0, 0.1, True, 0.95, F)["flagged"])
        # ... though it may still carry the phi condition
        self.assertEqual(
            oss.classify_step(1.0, 1.0, -0.5, True, 0.5, F),
            {"ratio": False, "phi": True, "flagged": True},
        )

    def test_an_inward_step_is_never_flagged(self):
        self.assertFalse(oss.classify_step(1.0, -1.0, -0.1, False, 0.5, F)["flagged"])


def _interpolant(phi, pi):
    """A stand-in interpolant on [0, 1]: phi(t) and pi(t) as functions of numpy arrays."""

    def call(t):
        t = np.asarray(t, dtype=float)
        return np.array([phi(t), pi(t), 0.0 * t, 0.0 * t, 0.0 * t])

    return call


class TestTheFlagAgreesWithTheGuard(unittest.TestCase):
    """The scanner's flag and ScalarHistory's, which the guard applies (prompt 03), agree."""

    CASES = {
        "neither": (lambda t: 1.0 + t, lambda t: 1.0 - 0.01 * t),
        "ratio": (lambda t: 1.0 + t, lambda t: 1.0 - 0.9 * t),
        "phi": (lambda t: 1.0 - 0.8 * np.sin(np.pi * t), lambda t: 1.0 - 0.01 * t),
        "sign change": (lambda t: 1.0 + t, lambda t: np.cos(2.5 * np.pi * t)),
        "swing below zero": (
            lambda t: 1.0
            + 3.0 * np.sin(2.0 * np.pi * t) * (1.0 - t)
            - 5.0 * t * (1 - t),
            lambda t: 1.0 - 0.9999 * t,
        ),
    }

    def test_agreement(self):
        for name, (phi, pi) in self.CASES.items():
            with self.subTest(name):
                interp = _interpolant(phi, pi)
                phi_s, pi_s = oss.step_samples(interp, 0.0, 1.0)
                scanner = oss.classify_step(
                    float(phi(0.0)),
                    float(pi(0.0)),
                    float(pi(1.0)),
                    bool(np.any(pi_s <= 0.0)),
                    float(np.min(phi_s)),
                    F,
                )["flagged"]
                guard = SM._outward_step_flagged(
                    interp, 0.0, 1.0, float(phi(0.0)), float(pi(0.0)), float(pi(1.0)), F
                )
                self.assertEqual(scanner, guard)

    def test_the_cases_cover_both_outcomes(self):
        outcomes = set()
        for phi, pi in self.CASES.values():
            outcomes.add(
                SM._outward_step_flagged(
                    _interpolant(phi, pi), 0.0, 1.0, phi(0.0), pi(0.0), pi(1.0), F
                )
            )
        self.assertEqual(outcomes, {True, False})


# ---- the observer -----------------------------------------------------------------------------------


class _StubSolver:
    """A stand-in for Radau: each step() takes the next (h, y_end, interpolant) of its script."""

    def __init__(
        self, fun, t0, y0, t_bound, max_step=np.inf, first_step=None, **kwargs
    ):
        self.t = float(t0)
        self.y = np.array(y0, dtype=float)
        self.max_step = max_step
        self.h_abs = 1e-3 if first_step is None else first_step
        self.J = np.array([[0.0, 1.0], [-4.0e6, 0.0]])
        self.f = np.array([self.y[1], -1.0])
        self.script = list(fun) if fun is not None else []
        self.t_old = None
        self._dense = None

    def step(self):
        h, y_end, interp = self.script.pop(0)
        t0 = self.t
        self.t_old = t0
        self.t = t0 + h
        y_start = self.y.copy()
        self.y = np.array(y_end, dtype=float)
        self._dense = lambda N: interp((np.asarray(N) - t0) / h, y_start)
        return None

    def dense_output(self):
        return self._dense


def _linear(x, y0, y1=None):
    return np.array([y0[0] + 0.0 * x, y0[1] + 0.0 * x])


def _straight_out(x, y0):
    return np.array([y0[0] * (1.0 + x), y0[1] * (1.0 - 0.001 * x)])


def _bad(x, y0):
    # the diagnosis's bad step in shape: phi swings below zero and pi ends near zero
    return np.array(
        [y0[0] * (1.0 + 30.0 * np.sin(2.0 * np.pi * x)), y0[1] * (1.0 - 0.9999 * x)]
    )


def integrate_scalar_history(cls, scripts):
    """A stub of the loop's shape: the names the scanner reads the start reason from, and
    new_solver building the solver directly, as the loop's does."""

    def new_solver(N, y, first_step=None):
        return cls(scripts[N], N, y, 10.0, first_step=first_step)

    def resume_after_release(N, y):
        return new_solver(N, y)

    solver = new_solver(0.0, np.array([1.0, 0.0]))
    solver.step()  # pi = 0 at the start: not outward
    y_reflected = np.array([1.0, 2.0])
    solver = new_solver(1.0, y_reflected)
    solver.step()  # outward, good
    solver.step()  # outward, bad: a later step
    y_drained = np.array([2.0, 1.0])
    solver = new_solver(2.0, y_drained)
    solver.step()  # outward, bad: a first step
    solver = resume_after_release(3.0, np.array([3.0, 0.5]))
    solver.step()  # outward, good
    y_retake = np.array([4.0, 1.0])
    solver = new_solver(4.0, y_retake)
    solver.step()  # outward, good
    return solver


def integrate_with_a_retake(cls, scripts):
    """A stub of the loop's shape (its function named integrate_scalar_history, as the scanner reads
    the frames) in which the guard discards a reflection's first step and retakes it from the same
    start (N = 1.0), as the loop does: new_solver(N, y_retake, ...)."""

    def integrate_scalar_history():
        def new_solver(N, y, first_step=None):
            return cls(scripts.pop(0), N, y, 10.0, first_step=first_step)

        solver = new_solver(0.0, np.array([1.0, 0.0]))
        solver.step()  # pi = 0 at the start: not outward
        y_reflected = np.array([1.0, 2.0])
        solver = new_solver(1.0, y_reflected)
        solver.step()  # outward, bad: a first step, discarded
        y_retake = y_reflected.copy()
        solver = new_solver(1.0, y_retake, 5e-4)
        solver.step()  # outward, good: the retake's first step
        return solver

    return integrate_scalar_history()


class TestTheObserver(unittest.TestCase):
    def run_stub(self, rows_mode="flagged"):
        observer = oss.Observer(F, 0.5, rows_mode)
        cls = oss._observed_radau_class(_StubSolver, observer)
        scripts = {
            0.0: [(1e-6, [1.0, 0.0], _linear)],
            1.0: [(1e-6, [1.0, 1.999], _straight_out), (1e-3, [1.0, 1e-4], _bad)],
            2.0: [(1e-3, [2.0, 1e-4], _bad)],
            3.0: [(1e-6, [3.0, 0.4999], _straight_out)],
            4.0: [(1e-6, [4.0, 0.999], _straight_out)],
        }

        observer.integration += 1
        integrate_scalar_history(cls, scripts)
        return observer

    def test_the_start_reasons(self):
        observer = self.run_stub()
        self.assertEqual(
            [i["reason"] for i in observer.instances],
            ["start", "reflection", "drain", "release", "retake"],
        )

    def test_the_counts(self):
        s = self.run_stub().summary()
        self.assertEqual(s["radau_accepted_steps"], 6)
        self.assertEqual(s["outward_steps"], 5)
        self.assertEqual(s["first_outward_steps"], 4)
        self.assertEqual(s["flagged_first"], 1)
        self.assertEqual(s["flagged_later"], 1)
        self.assertEqual(s["flagged"], 2)
        self.assertEqual(s["flag_both"], 2)
        # the ruled signature: the drain's bad first step; the reflection's bad step is a later one
        self.assertEqual(s["signature_kept"], 1)
        self.assertEqual(s["signature_discarded"], 0)
        self.assertEqual(s["instances"], 5)
        self.assertAlmostEqual(s["max_h_omega"], 1e-3 * 2000.0)

    def test_the_rows(self):
        flagged = self.run_stub("flagged").rows
        self.assertEqual(
            [(r["reason"], r["first"]) for r in flagged],
            [("reflection", False), ("drain", True)],
        )
        self.assertTrue(all(r["flagged"] for r in flagged))
        first = self.run_stub("first").rows
        self.assertEqual(len(first), 5)
        self.assertEqual(len(self.run_stub("all").rows), 5)

    def test_a_discarded_first_step_is_a_discarded_signature_step(self):
        observer = oss.Observer(F, 0.5, "flagged")
        cls = oss._observed_radau_class(_StubSolver, observer)
        scripts = [
            [(1e-6, [1.0, 0.0], _linear)],
            [(1e-3, [1.0, 1e-4], _bad)],
            [(1e-6, [1.0, 1.999], _straight_out)],
        ]
        observer.integration += 1
        integrate_with_a_retake(cls, scripts)
        self.assertEqual(
            [i["reason"] for i in observer.instances], ["start", "reflection", "retake"]
        )
        self.assertEqual(observer.instances[2]["first_step"], 5e-4)
        s = observer.summary()
        self.assertEqual(s["signature_kept"], 0)
        self.assertEqual(s["signature_discarded"], 1)
        self.assertEqual(s["discarded_steps"], 1)
        self.assertEqual(s["discarded_flagged_steps"], 1)
        self.assertEqual(s["flagged"], 0)
        self.assertEqual(
            [(r["reason"], r["signature"], r["discarded"]) for r in observer.rows],
            [("reflection", True, True)],
        )

    def test_observe_restores_the_module(self):
        radau, integrate = SM.Radau, SM.integrate_scalar_history
        with oss.observe(1.0):
            self.assertIsNot(SM.Radau, radau)
            self.assertIsNot(SM.integrate_scalar_history, integrate)
        self.assertIs(SM.Radau, radau)
        self.assertIs(SM.integrate_scalar_history, integrate)
        # SciPy's own class is never touched
        from scipy.integrate import Radau

        self.assertIs(Radau, radau)

    def test_a_task_that_names_a_probe_variant_is_refused(self):
        for name, value in (("variant", "F3"), ("stiffness_c", 1.0)):
            with self.subTest(name=name):
                spec = {
                    "key": "k",
                    "mode": "stub",
                    "stub": f"{__name__}:stub_ok",
                    name: value,
                }
                with self.assertRaises(ValueError):
                    oss.scan_one(spec)
        # None, as 01's None tasks carry it, is the production code
        record = oss.scan_one(
            {"key": "k", "mode": "stub", "stub": f"{__name__}:stub_ok", "variant": None}
        )
        self.assertEqual(record["outcome"], "success")


# ---- the batch --------------------------------------------------------------------------------------


class TestTheBatch(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="champbh-outward-scan-")
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        self.output = self.dir / "out.jsonl"
        self.marker = self.dir / "marker.txt"

    def task(self, key, fn="stub_ok", **extra):
        return dict(
            key=key,
            mode="stub",
            stub=f"tools.tests.test_outward_step_scan:{fn}",
            beta=1.0,
            M=1.0,
            marker=str(self.marker),
            **extra,
        )

    def lines(self):
        return [
            json.loads(l) for l in self.output.read_text().splitlines() if l.strip()
        ]

    def test_one_line_per_task_and_the_resume(self):
        oss.run_batch(
            [self.task("a", value=1), self.task("b", value=2)], str(self.output), jobs=2
        )
        self.assertEqual(sorted(r["key"] for r in self.lines()), ["a", "b"])
        self.assertEqual({r["key"]: r["value"] for r in self.lines()}, {"a": 1, "b": 2})
        oss.run_batch(
            [self.task("a"), self.task("b"), self.task("c", value=3)],
            str(self.output),
            jobs=2,
        )
        self.assertEqual(sorted(r["key"] for r in self.lines()), ["a", "b", "c"])
        # a and b ran once each, c once
        self.assertEqual(sorted(self.marker.read_text().split()), ["a", "b", "c"])
        log = (self.dir / "out.jsonl.log").read_text().splitlines()
        self.assertTrue(log[-1].endswith("** scan done"))
        self.assertTrue((self.dir / "out.jsonl.pid").read_text().strip().isdigit())

    def test_a_task_that_raises_is_recorded_and_the_batch_goes_on(self):
        oss.run_batch(
            [self.task("x", "stub_raise"), self.task("y")], str(self.output), jobs=1
        )
        lines = {r["key"]: r for r in self.lines()}
        self.assertEqual(lines["x"]["status"], "error")
        self.assertIn("RuntimeError: a stub that raises", lines["x"]["traceback"])
        self.assertEqual(lines["y"]["status"], "done")

    def test_numpy_values_in_a_record_are_written(self):
        oss.run_batch([self.task("np", "stub_numpy")], str(self.output), jobs=1)
        (line,) = self.lines()
        self.assertEqual(line["status"], "done")
        self.assertEqual((line["value"], line["values"]), (1.5, [0.0, 1.0, 2.0]))

    def test_a_task_past_its_limit_is_a_timeout(self):
        start = time.monotonic()
        oss.run_batch(
            [self.task("slow", "stub_sleep"), self.task("quick")],
            str(self.output),
            jobs=2,
            timeout=3.0,
        )
        self.assertLess(time.monotonic() - start, 30.0)
        lines = {r["key"]: r for r in self.lines()}
        self.assertEqual(lines["slow"]["status"], "timeout")
        self.assertGreaterEqual(lines["slow"]["wall"], 3.0)
        self.assertEqual(lines["quick"]["status"], "done")


# ---- the store comparison ---------------------------------------------------------------------------

from ObjectFactories.tests.test_dissipation_store import (  # noqa: E402
    TEST_1,
    _DissipationStoreCase,
    dh,
)


class TestTheStoreComparison(_DissipationStoreCase):
    def setUp(self):
        super().setUp()
        beta, M, alpha = TEST_1
        self.payload = dh.payload(beta, M, alpha)
        _, self.serial = self.write(self.payload, beta, M, alpha)
        shards = sorted(glob.glob(str(self._dir / "*shard*.db")))
        self.assertEqual(len(shards), 1)
        self.shard = shards[0]
        self.base = self.shard.rsplit("-shard", 1)[0]
        self.where = {"shard": self.shard, "serial": self.serial}

    def compare(self):
        return oss.compare_with_store(self.payload, self.units, self.where)

    def poke(self, sql, *args):
        conn = sqlite3.connect(self.shard)
        conn.execute(sql, args)
        conn.commit()
        conn.close()

    def test_a_history_compares_bitwise_with_its_own_payload(self):
        result = self.compare()
        self.assertTrue(result["bitwise"], result)
        self.assertEqual(result["stored_samples"], len(self.payload["sample"]))
        self.assertEqual(result["unmatched_stored"], 0)

    def test_a_changed_sample_is_reported(self):
        raw_N = (
            sqlite3.connect(self.shard)
            .execute(
                "select raw_N from ScalarHistoryValue where history_serial = ? order by raw_N "
                "limit 1 offset 100",
                (self.serial,),
            )
            .fetchone()[0]
        )
        self.poke(
            "update ScalarHistoryValue set phi_Einstein_Mp = phi_Einstein_Mp * (1 + 1e-12) "
            "where history_serial = ? and raw_N = ?",
            self.serial,
            raw_N,
        )
        result = self.compare()
        self.assertFalse(result["bitwise"])
        self.assertEqual(list(result["value_columns"]), ["phi_Einstein_Mp"])
        entry = result["value_columns"]["phi_Einstein_Mp"]
        self.assertEqual(entry["count"], 1)
        self.assertEqual(entry["first"]["raw_N"], raw_N)
        self.assertAlmostEqual(entry["max_rel"], 1e-12, delta=1e-15)

    def test_a_changed_history_column_is_reported(self):
        self.poke(
            "update ScalarHistory set Delta_N_eff_today = Delta_N_eff_today * 2 "
            "where serial = ?",
            self.serial,
        )
        result = self.compare()
        self.assertFalse(result["bitwise"])
        self.assertEqual(list(result["history_columns"]), ["Delta_N_eff_today"])

    def test_a_changed_extra_data_key_is_reported(self):
        stored = json.loads(
            sqlite3.connect(self.shard)
            .execute(
                "select extra_data from ScalarHistory where serial = ?", (self.serial,)
            )
            .fetchone()[0]
        )
        stored["accepted_steps"] += 1
        self.poke(
            "update ScalarHistory set extra_data = ? where serial = ?",
            json.dumps(stored),
            self.serial,
        )
        result = self.compare()
        self.assertFalse(result["bitwise"])
        self.assertEqual(list(result["extra_data"]), ["accepted_steps"])

    def test_the_selection_and_the_store_mode(self):
        specs = oss.select_from_store(self.base, success_only=True)
        self.assertEqual(len(specs), 1)
        spec = specs[0]
        beta, M, alpha = TEST_1
        self.assertEqual((spec["beta"], spec["M"], spec["alpha"]), (beta, M, alpha))
        self.assertEqual(spec["normalization"], self.payload["normalization"])
        self.assertEqual(spec["phi_ref_Mp"], self.payload["phi_ref"])
        self.assertEqual(oss.select_from_store(self.base, M=[1e-12]), [])
        self.assertEqual(len(oss.select_from_store(self.base, blocks=["D"])), 1)
        spec["key"] = oss.task_key(spec)
        record = oss.scan_one(spec)
        self.assertEqual(record["outcome"], "success")
        self.assertTrue(record["comparison"]["bitwise"], record["comparison"])

    def test_a_path_that_cannot_be_opened_read_only_is_refused(self):
        with self.assertRaises(SystemExit):
            oss.connect_ro(str(self._dir / "absent.db"))
        with self.assertRaises(SystemExit):
            oss.store_shards(str(self._dir / "absent"))


if __name__ == "__main__":
    unittest.main()
