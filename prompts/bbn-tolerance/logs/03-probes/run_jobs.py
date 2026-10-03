"""
bbn-tolerance prompt 03, README section 6.3 (re-measure): the final tree, no
override, small network, `prod` only, on the 17-input roster (the 16 histories
and the SM baseline): 17 solves, through tools/bbn_from_store.py, several at a
time. Adapted from logs/02-probes/run_jobs.py (copied, not edited in place),
with the variants cut to `prod`.

Run from the repository root:

    ./venv/bin/python prompts/bbn-tolerance/logs/03-probes/run_jobs.py [--jobs N]

All rows go to remeasure.csv with tag R03. No --lowT-rtol is passed, so the
tool's override changes nothing and only records the calls. Each invocation's
stdout is kept in out/<job>.txt. The store is opened read-only by the tool.
"""

import argparse
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
STORE = os.path.expanduser("~/ChamPBH-stores/science-2026.6.0")
TOOL = "tools/bbn_from_store.py"
PY = "./venv/bin/python"
TAG = "R03"

# README section 6.0: (beta, M/M_P)
FAILURES = [
    (1.6, 1e-5),
    (2.09, 1e-5),
    (2.12, 1e-5),
    (2.4, 1e-5),
    (1.345, 1e-3),
    (2.89, 1e-3),
    (1.05, 1e-2),
    (1.1, 0.03),
    (1.7, 0.03),
    (2.1, 0.1),
    (1.05, 0.5),
]
CONTROLS = [
    (1.6, 1e-3),
    (2.0, 1e-5),
    (2.0, 0.5),
    (1.2, 1e-3),
    (1.05, 1e-5),
]
HISTORIES = FAILURES + CONTROLS


def jobs():
    out = [("SM", [PY, TOOL, "--sm-baseline", "--small-network"])]
    for beta, M in HISTORIES:
        out.append(
            (
                f"b{beta:g}_M{M:g}",
                [PY, TOOL, STORE, "--beta", repr(beta), "--M-Mp", repr(M)]
                + ["--variant", "prod", "--small-network"],
            )
        )
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--jobs", type=int, default=8)
    p.add_argument("--csv", default=str(HERE / "remeasure.csv"))
    args = p.parse_args()

    out_dir = HERE / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    todo = jobs()
    print(f"{len(todo)} invocations, {args.jobs} at a time -> {args.csv}", flush=True)

    def run(job):
        name, cmd = job
        cmd = cmd + ["--csv", args.csv, "--tag", TAG]
        t0 = time.time()
        with open(out_dir / f"{name}.txt", "w") as fh:
            rc = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT).returncode
        print(f"  done rc={rc} {time.time() - t0:6.1f} s {name}", flush=True)
        return rc

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.jobs) as ex:
        rcs = list(ex.map(run, todo))
    print(
        f"finished {len(todo)} in {time.time() - t0:.0f} s; nonzero rc: {sum(1 for r in rcs if r)}"
    )


if __name__ == "__main__":
    main()
