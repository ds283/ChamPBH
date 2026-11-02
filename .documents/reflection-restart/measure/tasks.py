"""
The task lists of reflection-restart prompt 01's measurement (prompt §1.3; orchestrator note 01, rulings
1 and 5), for tools/outward_step_scan.py's batch.

    PYTHONPATH=. ./venv/bin/python .documents/reflection-restart/measure/tasks.py signature
    PYTHONPATH=. ./venv/bin/python .documents/reflection-restart/measure/tasks.py main

`signature` is the diagnosis's nine histories, tuned, under the production code (variant None): the
Signature gate's run (§1.3, "Signature first"), scanned with --rows first. `main` is §1.3's items 1-5
under every variant:
  1. the three failures, tuned: (beta, M, alpha) = (1.5, 1e-12, 0.05), (1.14, 1e-15, 0.01),
     (1.09, 1e-15, 0.03);
  2. the six clean histories of the diagnosis's scan, tuned;
  3. the golden histories as ComputeTargets/tests/golden_history.py (beta 1.5 and 5.5 at M = 1e-3,
     untuned at 4 ln(1 meV), phi_ref 0, to T_J = 1 MeV, 85 decades of 1 + z) and
     golden_tuned_history.py (beta 3, M = 0.01, tuned) run them, each compared with its record by
     golden_post.py in the worker;
  4. the reflection store's 42 histories (var/datastores/science-2026.8.0-reflections), at their stored
     normalization, each compared with its stored rows;
  5. block B: at M = 1e-33 and 1e-30, from var/datastores/science-2026.8.0, the three beta of the grid
     nearest 1.2, 2.0 and 2.9 at alpha = 0.01 and 0.08, at their stored normalization, compared.
Variants None, F1, F2 (c = 1), F3, F4, F3+F4 everywhere, and F2 at c = 0.3 and 3 on items 1 and 2. The
order is ruling 5's: every variant but F1 and F2 first, then F1 and F2 away from 4e-31, then F1 and F2
at 4e-31.

Writes var/reflection-restart/01/tasks-<set>.json and nothing else. Opens the stores read-only (sqlite
mode=ro on each shard, through outward_step_scan.select_from_store).
"""

import json
import sys
from math import log
from pathlib import Path

sys.path.insert(0, ".")
from tools import outward_step_scan as oss  # noqa: E402
from Units import Planck_units  # noqa: E402

OUT = Path("var/reflection-restart/01")
POST = ".documents/reflection-restart/measure/golden_post.py:record"

FAILURES = [(1.5, 1e-12, 0.05), (1.14, 1e-15, 0.01), (1.09, 1e-15, 0.03)]
CLEAN = [
    (1.5, 1e-12, 0.02),
    (1.5, 1e-12, 0.15),
    (1.09, 1e-12, 0.08),
    (1.2, 1e-15, 0.03),
    (1.5, 4e-31, 0.08),
    (1.2, 4e-31, 0.08),
]
VARIANTS = [None, "F1", "F2", "F3", "F4", "F3+F4"]


def tuned(beta, M, alpha, item):
    return {"mode": "tuned", "beta": beta, "M": M, "alpha": alpha, "item": item}


def goldens():
    units = Planck_units()
    out = []
    for beta in (1.5, 5.5):
        out.append(
            {
                "mode": "fixed",
                "beta": beta,
                "M": 1.0e-3,
                "alpha": 0.0,
                "normalization": 4.0 * log(1.0e-3 * units.eV),
                "phi_ref_Mp": 0.0,
                "T_stop_GeV": 1.0e-3,
                "log10_z_high": 85,
                "item": 3,
                "golden": f"beta={beta:g}",
                "post": POST,
            }
        )
    out.append(
        {
            "mode": "tuned",
            "beta": 3.0,
            "M": 1.0e-2,
            "alpha": 0.0,
            "item": 3,
            "golden": "tuned",
            "post": POST,
        }
    )
    return out


def block_B():
    store = "var/datastores/science-2026.8.0"
    out = []
    for M in (1e-33, 1e-30):
        rows = oss.select_from_store(
            store, M=[M], alpha=[0.01, 0.08], success_only=False
        )
        betas = sorted({r["beta"] for r in rows})
        chosen = [min(betas, key=lambda b: abs(b - t)) for t in (1.2, 2.0, 2.9)]
        for r in rows:
            if r["beta"] in chosen:
                out.append(dict(r, item=5))
    return out


def reflections():
    rows = oss.select_from_store(
        "var/datastores/science-2026.8.0-reflections", success_only=False
    )
    return [dict(r, item=4) for r in rows]


def with_variant(spec, variant, c=None):
    task = dict(spec, variant=variant)
    if c is not None:
        task["stiffness_c"] = c
    return task


def main():
    which = sys.argv[1]
    OUT.mkdir(parents=True, exist_ok=True)
    if which == "signature":
        tasks = [
            dict(tuned(b, M, a, 1 if (b, M, a) in FAILURES else 2), variant=None)
            for b, M, a in FAILURES + CLEAN
        ]
    elif which == "main":
        nine = [tuned(b, M, a, 1) for b, M, a in FAILURES] + [
            tuned(b, M, a, 2) for b, M, a in CLEAN
        ]
        histories = nine + goldens() + reflections() + block_B()
        first, middle, last = [], [], []
        for variant in VARIANTS:
            for h in histories:
                task = with_variant(h, variant)
                if variant in ("F1", "F2"):
                    (last if h["M"] == 4e-31 else middle).append(task)
                else:
                    first.append(task)
        for c in (0.3, 3.0):
            for h in nine:
                task = with_variant(h, "F2", c)
                (last if h["M"] == 4e-31 else middle).append(task)
        tasks = first + middle + last
    else:
        raise SystemExit(f"unknown set {which!r}")
    for t in tasks:
        t["key"] = oss.task_key(t) + (f" golden={t['golden']}" if "golden" in t else "")
    path = OUT / f"tasks-{which}.json"
    path.write_text(json.dumps(tasks, indent=1))
    counts = {}
    for t in tasks:
        counts[t["item"]] = counts.get(t["item"], 0) + 1
    print(f"{path}: {len(tasks)} tasks; by item {dict(sorted(counts.items()))}")


if __name__ == "__main__":
    main()
