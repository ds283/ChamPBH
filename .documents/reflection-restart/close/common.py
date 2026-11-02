"""
reflection-restart prompt 04 (the close): what the in-process scripts of close/ share (orchestrator note 04,
rulings 3-5). Not a script.

- check_tree() prints ComputeTargets.ScalarHistory.__file__ and tools.outward_step_scan.__file__ and refuses
  to go on unless both lie under the working directory (ruling 3), so that a script run from the 89a0760
  worktree runs that tree's code, and one run from the repository root runs this branch's;
- load_task() reads one task of a task list by its serial;
- run_with_timeout() calls the scanner's scan_one on the task in this process, with a wall-clock limit
  (ruling 2: 3600 s) by SIGALRM; a task past it gives a "timeout" record;
- write_record() appends the record as one JSON line (the scanner's _json_default).
"""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

TIMEOUT = 3600.0


class _Timeout(BaseException):
    pass


def check_tree():
    import importlib

    # by importlib: the package ComputeTargets exports a class ScalarHistory that shadows the module's name
    SM = importlib.import_module("ComputeTargets.ScalarHistory")
    scan = importlib.import_module("tools.outward_step_scan")

    cwd = Path(os.getcwd()).resolve()
    for name, module in (
        ("ComputeTargets.ScalarHistory", SM),
        ("tools.outward_step_scan", scan),
    ):
        path = Path(module.__file__).resolve()
        print(f"{name}.__file__ = {path}")
        if cwd not in path.parents:
            raise SystemExit(
                f"!! {name} is not under the working directory {cwd}; refusing to run"
            )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=cwd
    ).stdout.strip()
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        capture_output=True,
        text=True,
        cwd=cwd,
    ).stdout.strip()
    print(f"tree: {cwd} at {head}; tracked changes: {'none' if not status else status}")
    sys.stdout.flush()
    return SM, scan, head


def load_task(tasks_path: str, serial: int) -> dict:
    tasks = json.loads(Path(tasks_path).read_text())
    hits = [
        t
        for t in tasks
        if t.get("store", {}).get("serial") == serial
        or f"#{serial} " in t.get("key", "")
    ]
    if len(hits) != 1:
        raise SystemExit(f"!! {len(hits)} tasks for serial {serial} in {tasks_path}")
    return hits[0]


def run_with_timeout(scan, spec: dict, timeout: float = TIMEOUT) -> dict:
    spec = dict(spec)
    spec["key"] = scan.task_key(spec)

    def alarm(signum, frame):
        raise _Timeout()

    old = signal.signal(signal.SIGALRM, alarm)
    signal.setitimer(signal.ITIMER_REAL, timeout)
    start = time.monotonic()
    try:
        record = scan.scan_one(spec)
        record["status"] = "done"
    except _Timeout:
        record = {
            "key": spec["key"],
            "spec": spec,
            "status": "timeout",
            "outcome": "timeout",
            "wall": time.monotonic() - start,
        }
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, old)
    return record


def write_record(scan, record: dict, output: str):
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    with open(output, "a") as f:
        f.write(json.dumps(record, default=scan._json_default) + "\n")
        f.flush()
