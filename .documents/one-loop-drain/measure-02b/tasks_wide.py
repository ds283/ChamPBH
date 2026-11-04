"""
one-loop-drain prompt 02b §1.1: the task list. It is 02's `measure/tasks.py`, imported, with one override (orchestrator
note 02b, ruling 3): its module global A_VALUES, read by its main() at call time, set to the nine A of 02 and 02b
(10, 5, 2, 1.5, 1, 0.5, 0.1, 0.05, 0.01). Its table, its store check (the 2026.8.0 store's main shards, mode=ro) and
its task format are 02's, so that every task id and argv at 02's three A is 02's own (the reproduction check runs two
of them), and 02b's six A add 360 tasks. 540 tasks in all.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure-02b/tasks_wide.py var/one-loop-drain/02b/tasks.json \\
        > .documents/one-loop-drain/measure-02b/out/tasks.txt
"""

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import wide  # noqa: E402

import tasks as tasks02  # noqa: E402  (02's tasks.py, by import)

tasks02.A_VALUES = wide.A_ALL

if __name__ == "__main__":
    print(
        f"# 02's tasks.py with A_VALUES = {wide.A_ALL} (02b's six: {wide.A_02B}; 02's three: {wide.A_02})"
    )
    tasks02.main()
