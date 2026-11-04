"""
one-loop-drain prompt 02b §1.3, "The tool check" (orchestrator note 02b ruling 8, which is note 02's ruling 9
unchanged): `tools/history_and_bbn.py` itself, run in a subprocess on P-2197 at A = 10, gap zero, with the driver
task's argv (the spot batch's `tool:P-2197-A10-zero` line; stdout in var/one-loop-drain/02b/tasks/spot-tool-<id>.log).
Its stdout is compared with the driver task's stdout line by line, every line byte-equal once `wall=` and
`PRyM_time=` are masked, and its history, drain and bbn lines with the driver's JSON line. A difference is a stop
(exit 3).

It is 02's `measure/tool_check.py`, imported, with four overrides of its module globals, each read by its main() at
call time (ruling 3): CHECKS -> (P-2197-A10-zero,); TASK_DIR -> var/one-loop-drain/02b/tasks; SPOT ->
var/one-loop-drain/02b/spot.jsonl; run_lines -> 02b's (02's own default argument is bound to 02's sample).

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure-02b/tool_wide.py \\
        > .documents/one-loop-drain/measure-02b/out/tool_check.txt
"""

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import wide  # noqa: E402

import tool_check as tc02  # noqa: E402  (02's tool_check.py, by import)

tc02.CHECKS = ("P-2197-A10-zero",)
tc02.TASK_DIR = wide.TASK_DIR
tc02.SPOT = wide.SPOT
tc02.run_lines = wide.run_lines_02b

if __name__ == "__main__":
    tc02.main()
