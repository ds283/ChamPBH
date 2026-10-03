"""
bbn-tolerance prompt 01: run tools/bbn_from_store.py over the roster, several
invocations at a time (README section 0.2 U1), each appending to one CSV.

Run from the repository root:

    ./venv/bin/python prompts/bbn-tolerance/logs/01-probes/run_jobs.py BLOCK [--jobs N]

BLOCK is one of
    reproduce  the 16 histories, prod, default tolerance        -> reproduce.csv
    S1         README section 2 (c) S1                           -> scan.csv
    S2         S2, at the rtol values given by --s2-rtol (two)   -> scan.csv
    S3         S3                                                -> scan.csv
Each invocation's stdout is kept in out/<BLOCK>/<job>.txt. The store is opened
read-only by the tool.
"""

import argparse
import os
import subprocess
import sys
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

# S1: None is the default (no rtol passed, SciPy's 1e-3)
S1_RTOL = [None, 1e-4, 1e-5, 1e-6, 1e-8]
S3_INPUTS = ["SM", (1.6, 1e-3), (1.6, 1e-5), (2.4, 1e-5)]


def history_job(beta, M, variants, rtol=None, atol=None, small=False, tag=""):
    cmd = [PY, TOOL, STORE, "--beta", repr(beta), "--M-Mp", repr(M)]
    cmd += ["--variant", *variants]
    if rtol is not None:
        cmd += ["--lowT-rtol", repr(rtol)]
    if atol is not None:
        cmd += ["--lowT-atol", str(atol)]
    if small:
        cmd += ["--small-network"]
    name = f"{tag}_b{beta:g}_M{M:g}_r{rtol}_a{atol}".replace(" ", "")
    return name, cmd, tag


def sm_job(rtol=None, atol=None, small=False, tag=""):
    cmd = [PY, TOOL, "--sm-baseline"]
    if rtol is not None:
        cmd += ["--lowT-rtol", repr(rtol)]
    if atol is not None:
        cmd += ["--lowT-atol", str(atol)]
    if small:
        cmd += ["--small-network"]
    name = f"{tag}_SM_r{rtol}_a{atol}"
    return name, cmd, tag


def jobs_for(block, s2_rtol, s2_atol):
    jobs = []
    if block == "reproduce":
        for b, M in HISTORIES:
            jobs.append(history_job(b, M, ["prod"], tag="reproduce"))
    elif block == "S1":
        # the slowest settings first, so the pool drains evenly
        for rtol in reversed(S1_RTOL):
            jobs.append(sm_job(rtol, tag="S1"))
            for b, M in HISTORIES:
                jobs.append(history_job(b, M, VARIANTS, rtol, tag="S1"))
    elif block == "S2":
        for rtol in s2_rtol:
            jobs.append(sm_job(rtol, s2_atol, tag="S2"))
            for b, M in HISTORIES:
                jobs.append(history_job(b, M, VARIANTS, rtol, s2_atol, tag="S2"))
    elif block == "S3":
        for rtol in S1_RTOL:
            for inp in S3_INPUTS:
                if inp == "SM":
                    jobs.append(sm_job(rtol, small=True, tag="S3"))
                else:
                    jobs.append(history_job(*inp, ["prod"], rtol, small=True, tag="S3"))
    else:
        raise SystemExit(f"unknown block {block}")
    return jobs


def main():
    p = argparse.ArgumentParser()
    p.add_argument("block")
    p.add_argument("--jobs", type=int, default=9)
    p.add_argument("--csv", default=None)
    p.add_argument("--s2-rtol", type=float, nargs="*", default=[])
    p.add_argument("--s2-atol", default=None)
    args = p.parse_args()

    csv_path = args.csv or str(
        HERE / ("reproduce.csv" if args.block == "reproduce" else "scan.csv")
    )
    out_dir = HERE / "out" / args.block
    out_dir.mkdir(parents=True, exist_ok=True)
    jobs = jobs_for(args.block, args.s2_rtol, args.s2_atol)
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
