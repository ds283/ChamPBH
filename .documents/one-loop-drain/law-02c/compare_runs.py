"""
one-loop-drain prompt 02c, the re-verification after the user's ruling B of 2026-11-05 ("I suggest we move `L` into
the model class, with no default"): L moved from ScalarHistory.py into ExponentialModel._drain_L, with the same
expression. The batch (batch.py) and §2.4's tool run were run again on the moved tree; the first run's outputs were
kept aside under var/one-loop-drain/02c/maps-run1/ and tool-P-2197-A0.05-run1.txt. This compares the two, byte for
byte for every map and the control, and the tool's lines but each line's wall time (the `drain` line verbatim).

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law-02c/compare_runs.py > .documents/one-loop-drain/law-02c/out/compare_runs.txt
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "var/one-loop-drain/02c"
NAMES = (
    "P-2197-A0.05",
    "W-2187-A0.05",
    "R-632-A0.5",
    "L-M5e-20-A0.2",
    "control-P-2197-A0.05",
    "P-2197-A1",
    "reference-P-2197-A1",
)
WALL = re.compile(r"(wall|PRyM_time)=[0-9.]+ s")


def main() -> int:
    ok = True
    for name in NAMES:
        first = (OUT / "maps-run1" / f"{name}.txt").read_bytes()
        second = (OUT / "maps" / f"{name}.txt").read_bytes()
        same = first == second
        ok &= same
        print(
            f"{name}: {'byte-identical' if same else 'DIFFERENT'} ({len(first)} and {len(second)} bytes)"
        )
    first = (OUT / "tool-P-2197-A0.05-run1.txt").read_text().splitlines()
    second = (OUT / "tool-P-2197-A0.05.txt").read_text().splitlines()
    drain = [l for l in first if l.startswith("drain ")] == [
        l for l in second if l.startswith("drain ")
    ]
    rest = [WALL.sub(r"\1=<t> s", l) for l in first] == [
        WALL.sub(r"\1=<t> s", l) for l in second
    ]
    ok &= drain and rest
    print(
        f"tool: drain line {'identical' if drain else 'DIFFERENT'}; every line but the wall times {'identical' if rest else 'DIFFERENT'}"
    )
    print(f"all equal: {ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
