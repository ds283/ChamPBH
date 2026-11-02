"""
reflection-restart prompt 04 §1.2 (b): which of 03's changes moves the trajectory. On this branch's tree, replace
ComputeTargets.ScalarHistory._restart_first_step with a function that returns None (SciPy's own choice of the
first step at every restart, as before 03), leave the guard live, and run one history of tasks-pair.json at its
stored normalization through the scanner's scan_one, in this process (orchestrator note 04, ruling 4: the
batch spawns its workers, which a patch here would not reach). integrate_scalar_history looks the function up at
call time (ScalarHistory.py, new_solver), so the replacement reaches every restart; with first_step None the
clamp to the bound is skipped and the guard is still armed.

The patch is confined to this process. Nothing is written but one JSON line, appended to OUTPUT. Run from the
repository root, detached, one process per history:

    PYTHONPATH=. ./venv/bin/python .documents/reflection-restart/close/f3_off.py SERIAL OUTPUT

The record is compared with the history's 89a0760 record by compare.py.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import check_tree, load_task, run_with_timeout, write_record  # noqa: E402

TASKS = "var/reflection-restart/04/tasks-pair.json"


def main():
    serial, output = int(sys.argv[1]), sys.argv[2]
    SM, scan, head = check_tree()

    original = SM._restart_first_step

    def scipy_choice(y, global_max_step, h_floor, cap_fraction):
        return None

    SM._restart_first_step = scipy_choice
    print(
        f"_restart_first_step replaced: {original.__qualname__} -> scipy_choice (returns None); "
        f"_outward_step_flagged left as {SM._outward_step_flagged.__qualname__}"
    )
    spec = load_task(TASKS, serial)
    spec["key"] = scan.task_key(spec) + " f3_off"
    print(f"task: {spec['key']}")
    sys.stdout.flush()
    start = time.strftime("%Y-%m-%dT%H:%M:%S")
    record = run_with_timeout(scan, spec)
    record["tree"] = head
    record["patch"] = "_restart_first_step -> None (SciPy's choice); guard live"
    record["started"] = start
    write_record(scan, record, output)
    print(
        f"{record.get('status')} {record.get('outcome')} wall {record.get('wall', 0):.1f} s; "
        f"restart_retakes {record.get('restart_retakes')}; scan {record.get('scan', {}).get('signature_kept')} kept / "
        f"{record.get('scan', {}).get('signature_discarded')} discarded"
    )
    if record.get("outcome") == "failure":
        print(f"failure_reason: {record['failure_reason']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
