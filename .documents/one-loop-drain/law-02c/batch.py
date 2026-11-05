"""
one-loop-drain prompt 02c §2.3, the batch (orchestrator note 02c, rulings 4 and 5): crossover's four adopted-law
maps by `cascade_map.py` unpatched on this tree, the control (`measured_law_map.py --law=adopted` on P-2197 at
A = 0.05), and, for the re-pinned TestTheOneLoopLawOn2197, 2197 at A = 1 both ways. At most five at once, each a
fresh `./venv/bin/python`, stdout to var/one-loop-drain/02c/maps/<name>.txt, stderr to <name>.err, its wall time to
batch.log. batch.end is written last.

    nohup ./venv/bin/python .documents/one-loop-drain/law-02c/batch.py > var/one-loop-drain/02c/batches/maps/batch.log 2>&1 &
"""

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CROSSOVER = "prompts/one-loop-drain/source/crossover"
TASKS = "var/one-loop-drain/02b/tasks"
INPUTS = "var/one-loop-drain/02c/inputs"
MAPS = ROOT / "var/one-loop-drain/02c/maps"
BATCH = ROOT / "var/one-loop-drain/02c/batches/maps"
WORKERS = 5

RUNS = (
    (
        "P-2197-A0.05",
        [f"{CROSSOVER}/cascade_map.py", f"{TASKS}/P-2197-A0.05-zero.result.json"],
    ),
    (
        "W-2187-A0.05",
        [f"{CROSSOVER}/cascade_map.py", f"{TASKS}/W-2187-A0.05-zero.result.json"],
    ),
    (
        "R-632-A0.5",
        [f"{CROSSOVER}/cascade_map.py", f"{TASKS}/R-632-A0.5-zero.result.json"],
    ),
    (
        "L-M5e-20-A0.2",
        [f"{CROSSOVER}/cascade_map.py", f"{INPUTS}/L-M5e-20-A0.2.result.json"],
    ),
    (
        "control-P-2197-A0.05",
        [
            f"{CROSSOVER}/measured_law_map.py",
            f"{TASKS}/P-2197-A0.05-zero.result.json",
            "--law=adopted",
        ],
    ),
    ("P-2197-A1", [f"{CROSSOVER}/cascade_map.py", f"{INPUTS}/P-2197-A1.result.json"]),
    (
        "reference-P-2197-A1",
        [
            f"{CROSSOVER}/measured_law_map.py",
            f"{INPUTS}/P-2197-A1.result.json",
            "--law=adopted",
        ],
    ),
)


def log(text: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {text} | load {os.getloadavg()}", flush=True)


def main() -> int:
    MAPS.mkdir(parents=True, exist_ok=True)
    BATCH.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, PYTHONPATH=".")
    pending = list(RUNS)
    running = {}
    start = time.perf_counter()
    log(f"batch start: {len(RUNS)} runs, at most {WORKERS} at once, pid {os.getpid()}")
    while pending or running:
        while pending and len(running) < WORKERS:
            name, args = pending.pop(0)
            out = open(MAPS / f"{name}.txt", "w")
            err = open(MAPS / f"{name}.err", "w")
            proc = subprocess.Popen(
                [str(ROOT / "venv/bin/python")] + args,
                cwd=ROOT,
                env=env,
                stdout=out,
                stderr=err,
            )
            running[proc.pid] = (name, proc, time.perf_counter(), out, err)
            log(f"start {name} pid {proc.pid}")
        pid, status, usage = os.wait4(-1, 0)
        if pid not in running:
            continue
        name, proc, t0, out, err = running.pop(pid)
        out.close()
        err.close()
        rc = os.waitstatus_to_exitcode(status)
        log(
            f"end {name} rc={rc} wall={time.perf_counter() - t0:.1f} s maxrss={usage.ru_maxrss}"
        )
    log(f"batch end: wall={time.perf_counter() - start:.1f} s")
    (BATCH / "batch.end").write_text(time.strftime("%Y-%m-%dT%H:%M:%S") + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
