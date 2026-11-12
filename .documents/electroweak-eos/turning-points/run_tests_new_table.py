"""
Probe (electroweak-eos prompt 01a): run the 22 tests that prompt 01's new w table breaks (note 01 §0;
the list is tests_broken_by_the_table.txt beside this file) with the class's CSV read redirected to a
given table, and record, for each test, whether it passes and its first failing assertion, and for
each integration it makes, whether it holds the early pair.

It is prompt 01's run_failing.py (its handoff §3), extended:
  - test_release keeps its own reference to integrate_scalar_history (_INTEGRATE, bound at import),
    so the wrapper is installed there as well; 01's run missed those two histories;
  - each integration records unresolved_turning_point_pairs where the field exists (01a);
  - each failing test records its exception's last line and the last test-file frame (its assertion).

"The early pair" is 01's criterion: a TurningPoint with T_J > 300 GeV and |phi - phi*| < 1e-6 M_P,
phi* the history's initial field.

Run from the repository root:
  PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/turning-points/run_tests_new_table.py \
      TABLE.csv OUT.json [IDS.txt]
No store, no Ray. The tests' own runtimes apply (several minutes in all).
"""

import contextlib
import importlib
import json
import sys
import traceback
import unittest
from math import exp
from pathlib import Path
from unittest import mock

import pandas as pd

import CosmologyModels.GenericEOS.Xav_EOS_spline as xav_module
from Units import Planck_units

SM = importlib.import_module("ComputeTargets.ScalarHistory")

HERE = Path(__file__).resolve().parent


def main():
    path = sys.argv[1]
    out = sys.argv[2]
    ids_file = (
        Path(sys.argv[3])
        if len(sys.argv) > 3
        else HERE / "tests_broken_by_the_table.txt"
    )
    ids = [
        x.strip()
        for x in ids_file.read_text().splitlines()
        if x.strip() and not x.startswith("#")
    ]

    real_read = pd.read_csv
    units = Planck_units()
    GeV, Mp = units.GeV, units.PlanckMass
    current = {"test": None}
    seen = []
    real_isc = SM.integrate_scalar_history

    def wrapped(RHS, supervisor, initial_state, *a, **k):
        res = real_isc(RHS, supervisor, initial_state, *a, **k)
        try:
            phi0 = initial_state.phi_Einstein
            early = [
                (
                    t.N,
                    t.kind,
                    exp(t.log_T_Jordan) / GeV,
                    (t.phi_Einstein - phi0) / Mp,
                )
                for t in res.turning_points
                if exp(t.log_T_Jordan) / GeV > 300
                and abs(t.phi_Einstein - phi0) < 1e-6 * Mp
            ]
            model = getattr(getattr(RHS, "policy", None), "model", None)
            # the test-file frames that made this integration: a setUpClass runs before its class's
            # first test starts, so "test" (the last test started) can name the previous class
            frames = [
                f"{Path(fs.filename).stem}:{fs.name}"
                for fs in traceback.extract_stack()
                if "/tests/" in fs.filename
                and Path(fs.filename).stem.startswith("test_")
            ]
            seen.append(
                {
                    "test": current["test"],
                    "frames": frames[:3],
                    "model": getattr(model, "name", None),
                    "alpha": k.get("alpha", 0.0),
                    "drain_law": k.get("drain_law", "fixed"),
                    "phi0_MP": phi0 / Mp,
                    "n_turning_points": len(res.turning_points),
                    "early": early,
                    "dropped_drains": res.dropped_drains,
                    "unresolved_turning_point_pairs": getattr(
                        res, "unresolved_turning_point_pairs", None
                    ),
                    "n_injections": len(res.injections),
                }
            )
        except (
            Exception
        ) as e:  # recorded, never raised: the probe must not change a result
            seen.append({"test": current["test"], "error": repr(e)})
        return res

    class Result(unittest.TextTestResult):
        def startTest(self, test):
            current["test"] = test.id()
            super().startTest(test)

    with contextlib.ExitStack() as stack:
        stack.enter_context(
            mock.patch.object(
                xav_module.pd,
                "read_csv",
                lambda a, *k, **kw: real_read(path, *k, **kw),
            )
        )
        stack.enter_context(mock.patch.object(SM, "integrate_scalar_history", wrapped))
        tr = importlib.import_module("ComputeTargets.tests.test_release")
        stack.enter_context(mock.patch.object(tr, "_INTEGRATE", wrapped))

        suite = unittest.TestSuite()
        for i in ids:
            suite.addTests(unittest.defaultTestLoader.loadTestsFromName(i))
        runner = unittest.TextTestRunner(
            stream=sys.stderr, verbosity=2, resultclass=Result
        )
        r = runner.run(suite)

    outcome = {}
    for i in ids:
        outcome[i] = {"status": "pass"}

    # a failing subTest is reported against its parent test, with the subtest's id in "subtests"
    def parent(t):
        return getattr(t, "test_case", t).id()

    for t, _ in r.failures:
        outcome.setdefault(parent(t), {}).update(status="fail")
    for t, _ in r.errors:
        outcome.setdefault(parent(t), {}).update(status="error")
    for t, text in r.failures + r.errors:
        if parent(t) != t.id():
            outcome[parent(t)].setdefault("subtests", []).append(t.id())
            if "exception" in outcome[parent(t)]:
                continue
        lines = [x for x in text.strip().splitlines() if x.strip()]
        frames = [
            j
            for j, x in enumerate(lines)
            if x.strip().startswith('File "') and "/tests/" in x
        ]
        if frames:
            j = frames[-1]
            where = lines[j].strip().split("/")[-1]
            source = lines[j + 1].strip() if j + 1 < len(lines) else ""
            outcome[parent(t)]["frame"] = f"{where} :: {source}"
        else:
            outcome[parent(t)]["frame"] = None
        idx = None
        for j, x in enumerate(lines):
            if x.startswith("AssertionError") or (
                not x.startswith(" ") and "Error" in x.split(":")[0]
            ):
                idx = j
        outcome[parent(t)]["exception"] = (
            lines[idx][:400] if idx is not None else lines[-1][:400]
        )
    json.dump(
        {
            "table": path,
            "has_resolved_sign_changes": hasattr(SM, "resolved_sign_changes"),
            "run": r.testsRun,
            "outcome": outcome,
            "integrations": seen,
        },
        open(out, "w"),
        indent=1,
        default=float,
    )
    n_pass = sum(1 for v in outcome.values() if v["status"] == "pass")
    print(
        f"{r.testsRun} run, {n_pass} pass, {len(r.failures)} fail, {len(r.errors)} error"
    )


if __name__ == "__main__":
    main()
