"""
one-loop-drain prompt 02c §2.3, the inputs (orchestrator note 02c, ruling 4).

P-2197 and W-2187 at A = 0.05 and R-632 at A = 0.5 use 02b's task records unchanged, read in place. The ladder
L-M5e-20 at A = 0.2 is 02b's `L-M5e-20-A0.5-zero.result.json` with argv's `--alpha` value set to "0.2" and nothing
else changed, as crossover rebuilt it ("A = 0.2, tuning of A0.5"). One more copy, for the cross-check of the
re-pinned TestTheOneLoopLawOn2197 (prompt §1.6): `P-2197-A0.05-zero.result.json` with `--alpha` set to "1", the
test's history (2197 at its stored normalization, which is that task's tuning) at the test's A.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law-02c/make_inputs.py
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TASKS = ROOT / "var/one-loop-drain/02b/tasks"
INPUTS = ROOT / "var/one-loop-drain/02c/inputs"

COPIES = (
    ("L-M5e-20-A0.5-zero.result.json", "L-M5e-20-A0.2.result.json", "0.2"),
    ("P-2197-A0.05-zero.result.json", "P-2197-A1.result.json", "1"),
)


def main() -> int:
    INPUTS.mkdir(parents=True, exist_ok=True)
    for source, target, alpha in COPIES:
        record = json.loads((TASKS / source).read_text())
        argv = record["argv"]
        i = argv.index("--alpha")
        before = argv[i + 1]
        argv[i + 1] = alpha
        (INPUTS / target).write_text(json.dumps(record))
        print(f"{target}: from {source}, argv --alpha {before} -> {alpha}; argv={argv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
