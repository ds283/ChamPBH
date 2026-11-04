"""
one-loop-drain prompt 02b: what the 02b scripts share. Its paths and A values, 02's `common` module imported (not
copied), and the comparisons of the reproduction check and the spot checks across 02's and 02b's directories.

02's scripts are imported from `.documents/one-loop-drain/measure/`, put on sys.path after this directory, so that
each 02b script runs 02's functions unchanged (orchestrator note 02b, ruling 3). 02b's own module names (wide,
run_wide, tasks_wide, decide_wide, reproduce, spot_wide, tool_wide, analyse_wide) differ from 02's, so that neither
shadows the other. Nothing of 02's `common` is overridden here: its functions that read a path take it as an argument
(run_lines, all_lines), and those are called with 02b's paths explicitly (its defaults are bound to 02's SAMPLE when
it is imported). Its comparison (compare_lines, note 02's ruling 6, which note 02b's ruling 7 keeps) is used as it
stands, with the fields that 02b's lines add and 02's lack left out where a 02b line is compared with a 02 line
(note 02b, ruling 4).

The A values: 02's three (1, 0.1, 0.01) are 02's lines, reused once the reproduction check holds; 02b's six
(10, 5, 2, 1.5, 0.5, 0.05) are run here. A_ALL is the nine, in descending order.
"""

import sys
from pathlib import Path

ROOT = Path("/Users/ds283/Documents/Code/ChamPBH")
MEASURE_02 = ROOT / ".documents/one-loop-drain/measure"
# nothing is written under 02's measure/, not even its bytecode
sys.dont_write_bytecode = True
if str(MEASURE_02) not in sys.path:
    sys.path.insert(1, str(MEASURE_02))

import common as c02  # noqa: E402  (02's common, by import)

OUT = ROOT / "var/one-loop-drain/02b"
TASK_DIR = OUT / "tasks"
BATCH_DIR = OUT / "batches"
SAMPLE = OUT / "sample.jsonl"
SPOT = OUT / "spot.jsonl"
REPRO = OUT / "repro.jsonl"
TASKS_JSON = OUT / "tasks.json"

OUT_02 = c02.OUT
TASK_DIR_02 = c02.TASK_DIR
SAMPLE_02 = c02.SAMPLE

A_02 = c02.A_VALUES  # (1.0, 0.1, 0.01)
A_02B = (10.0, 5.0, 2.0, 1.5, 0.5, 0.05)
A_ALL = tuple(sorted(A_02 + A_02B, reverse=True))
GAPS = c02.GAPS
GROUP_ORDER = c02.GROUP_ORDER

# the fields 02b's lines and partial lines add, which 02's lack (ruling 4: not compared between a 02 and a 02b line)
ADDED_LINE_KEYS = ()
ADDED_ACCEPTED_KEYS = ("first_impact",)
ADDED_INTEGRATION_KEYS = ("cap_one_loop_by_ell", "cap_gap_edge")
ADDED_PARTIAL_KEYS = ("ells",) + ADDED_INTEGRATION_KEYS

task_id = c02.task_id
a_label = c02.a_label
histories = c02.histories
read_jsonl = c02.read_jsonl
candidates_known = c02.candidates_known
met = c02.met
s1_holds = c02.s1_holds
s2_holds = c02.s2_holds
g = c02.g


def load_tasks():
    import json

    return json.loads(TASKS_JSON.read_text())


def run_lines_02b(path=None):
    return c02.run_lines(SAMPLE if path is None else path)


def all_lines_02b(path=None):
    return c02.all_lines(SAMPLE if path is None else path)


def run_lines_02():
    return c02.run_lines(SAMPLE_02)


def all_lines_02():
    return c02.all_lines(SAMPLE_02)


def all_lines_merged():
    """02's 180 lines and 02b's 360, by task id. The ids do not overlap (02b runs no A of 02's in its sample)."""
    a, b = all_lines_02(), all_lines_02b()
    both = set(a) & set(b)
    if both:
        raise SystemExit(f"task ids in both samples: {sorted(both)[:5]}")
    out = dict(a)
    out.update(b)
    return out


def strip_added(line):
    """A copy of a 02b line without the fields that 02b adds (ruling 4), for a comparison with a 02 line."""
    out = {k: v for k, v in line.items() if k not in ADDED_LINE_KEYS}
    if isinstance(out.get("accepted"), dict):
        out["accepted"] = {
            k: v for k, v in out["accepted"].items() if k not in ADDED_ACCEPTED_KEYS
        }
    if out.get("integrations") is not None:
        out["integrations"] = [
            {k: v for k, v in i.items() if k not in ADDED_INTEGRATION_KEYS}
            for i in out["integrations"]
        ]
    return out


def compare_lines_across(a_02b, b, shortcut):
    """
    compare_lines (02's, ruling 6) between a 02b line and a line b, which is 02's or 02b's. Where b is 02's the fields
    that 02b adds are left out of a first (ruling 4).
    """
    b_is_02 = "first_impact" not in (b.get("accepted") or {}) and not any(
        "cap_one_loop_by_ell" in i for i in (b.get("integrations") or [])
    )
    a = strip_added(a_02b) if b_is_02 else a_02b
    return c02.compare_lines(a, b, shortcut)


def compare_partial_files(path_a, path_b, shortcut=None):
    """
    Two partial files, per integration (ruling 4): every value of each integration's partial line by repr, the full
    regime sequence and injection triples among them, but its wall time and maxrss (ruling 6, as note 02's
    INTEGRATION_NOT_COMPARED) and the fields 02b adds where only one of the two has them. Under a shortcut ("S1" or
    "S2") the drain record's gap_prescription or A is not compared, since it differs by construction (ruling 7).
    Returns (differences, the regime and injection entries compared).
    """
    pa, pb = read_jsonl(path_a), read_jsonl(path_b)
    if len(pa) != len(pb):
        return [f"partial: {len(pa)} and {len(pb)} integrations"], 0
    allowed = {None: set(), "S1": {"gap_prescription"}, "S2": {"A"}}[shortcut]
    out = []
    n = 0
    for j, (x, y) in enumerate(zip(pa, pb)):
        skip = set(c02.INTEGRATION_NOT_COMPARED)
        for k in ADDED_PARTIAL_KEYS:
            if (k in x) != (k in y):
                skip.add(k)
        x = {k: v for k, v in x.items() if k not in skip}
        y = {k: v for k, v in y.items() if k not in skip}
        dx, dy = x.pop("drain", None), y.pop("drain", None)
        c02._diff(x, y, f"partial[{j}]", out)
        if dx is None or dy is None:
            if dx is not dy:
                out.append(f"partial[{j}].drain: None in one only")
        else:
            c02._diff(dx, dy, f"partial[{j}].drain", out, skip=allowed)
        n += len(x.get("regimes") or []) + len(x.get("injections") or [])
    return out, n
