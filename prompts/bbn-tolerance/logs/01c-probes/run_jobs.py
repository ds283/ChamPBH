"""
bbn-tolerance prompt 01c: run tools/bbn_from_store.py, small network, over
README section 2 (c') T1 and T2, several invocations at a time (README section
0.2 U1, P10), each appending to one CSV. Adapted from
logs/01-probes/run_jobs.py (copied, not edited in place).

Run from the repository root:

    ./venv/bin/python prompts/bbn-tolerance/logs/01c-probes/run_jobs.py BLOCK [--jobs N] [--t2-rtol X]

BLOCK is one of
    T1-repro  T1's cells on log 01's S3 inputs (SM; beta = 1.6, M = 1e-3;
              beta = 1.6, M = 1e-5; beta = 2.4, M = 1e-5) at the five rtol, the
              histories with prod, pert12 and pert9 (prod first). Run first: its
              prod rows are compared with log 01's S3 (compare_s3.py).
    T1-rest   the rest of T1: the other 13 histories at the five rtol.
    T2        the breadth sample of breadth.csv, prod, at --t2-rtol.
All rows go to scan.csv with the block, T1 or T2, in the tool's `tag` column.
Each invocation's stdout is kept in out/<BLOCK>/<job>.txt. The store is opened
read-only by the tool.
"""

import argparse
import csv
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
STORE = os.path.expanduser("~/ChamPBH-stores/science-2026.6.0")
TOOL = "tools/bbn_from_store.py"
PY = "./venv/bin/python"

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
VARIANTS = ["prod", "pert12", "pert9"]

# None is the default (no rtol passed, SciPy's 1e-3)
T1_RTOL = [None, 1e-4, 1e-5, 1e-6, 1e-8]
S3_HISTORIES = [(1.6, 1e-3), (1.6, 1e-5), (2.4, 1e-5)]


def history_job(beta, M, variants, rtol=None, phi=None, tag=""):
    cmd = [PY, TOOL, STORE, "--beta", repr(beta), "--M-Mp", repr(M)]
    if phi is not None:
        cmd += ["--phi-init-Mp", repr(phi)]
    cmd += ["--variant", *variants, "--small-network"]
    if rtol is not None:
        cmd += ["--lowT-rtol", repr(rtol)]
    name = f"{tag}_b{beta:g}_M{M:g}_phi{phi}_r{rtol}".replace(" ", "")
    return name, cmd, tag


def sm_job(rtol=None, tag=""):
    cmd = [PY, TOOL, "--sm-baseline", "--small-network"]
    if rtol is not None:
        cmd += ["--lowT-rtol", repr(rtol)]
    return f"{tag}_SM_r{rtol}", cmd, tag


def jobs_for(block, t2_rtol):
    jobs = []
    if block == "T1-repro":
        for rtol in T1_RTOL:
            jobs.append(sm_job(rtol, tag="T1"))
            for b, M in S3_HISTORIES:
                jobs.append(history_job(b, M, VARIANTS, rtol, tag="T1"))
    elif block == "T1-rest":
        # the slowest settings first, so the pool drains evenly
        for rtol in reversed(T1_RTOL):
            for b, M in HISTORIES:
                if (b, M) not in S3_HISTORIES:
                    jobs.append(history_job(b, M, VARIANTS, rtol, tag="T1"))
    elif block == "T2":
        if t2_rtol is None:
            raise SystemExit("T2 needs --t2-rtol")
        rtol = None if t2_rtol == "default" else float(t2_rtol)
        with open(HERE / "breadth.csv") as fh:
            for row in csv.DictReader(fh):
                jobs.append(
                    history_job(
                        float(row["beta"]),
                        float(row["M_Mp"]),
                        ["prod"],
                        rtol,
                        phi=float(row["phi_init_Mp"]),
                        tag="T2",
                    )
                )
    else:
        raise SystemExit(f"unknown block {block}")
    return jobs


def main():
    p = argparse.ArgumentParser()
    p.add_argument("block")
    p.add_argument("--jobs", type=int, default=9)
    p.add_argument("--csv", default=None)
    p.add_argument("--t2-rtol", default=None)
    args = p.parse_args()

    csv_path = args.csv or str(HERE / "scan.csv")
    out_dir = HERE / "out" / args.block
    out_dir.mkdir(parents=True, exist_ok=True)
    jobs = jobs_for(args.block, args.t2_rtol)
    print(f"{len(jobs)} invocations, {args.jobs} at a time -> {csv_path}", flush=True)

    def run(job):
        name, cmd, tag = job
        cmd = cmd + ["--csv", csv_path, "--tag", tag]
        t0 = time.time()
        with open(out_dir / f"{name}.txt", "w") as fh:
            rc = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT).returncode
        print(f"  done rc={rc} {time.time() - t0:6.1f} s {name}", flush=True)
        return rc

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.jobs) as ex:
        rcs = list(ex.map(run, jobs))
    print(
        f"finished {len(jobs)} in {time.time() - t0:.0f} s; nonzero rc: {sum(1 for r in rcs if r)}"
    )


if __name__ == "__main__":
    main()
