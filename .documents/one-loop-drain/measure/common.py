"""
one-loop-drain prompt 02: what the measure scripts share. Reading the sample, the S1 and S2 premises (prompt §1.2),
and the comparison of two task lines "bitwise" in the sense of orchestrator note 02, ruling 6.

Ruling 6: equal by repr are, per integration, N_final, accepted_steps, reflections, nfev, the regime sequence and
each Injection's (N, alpha_eff, delta_K) (the line carries the SHA-256 of their reprs; the full sequences are in the
partial files, which compare_partials reads); the tuning record; the two SHA-256 digests of the samples; Y_p, D/H,
He3/H and Li7/H; and every other value of the line, except the wall times, the peak RSS, PRyMordial's time, the
task's own fields and how it was obtained, and the record's gap_prescription (S1) or A (S2), which differ by
construction.
"""

import json
from pathlib import Path

ROOT = Path("/Users/ds283/Documents/Code/ChamPBH")
OUT = ROOT / "var/one-loop-drain/02"
TASK_DIR = OUT / "tasks"
SAMPLE = OUT / "sample.jsonl"
SPOT = OUT / "spot.jsonl"
TASKS_JSON = OUT / "tasks.json"

GROUP_ORDER = ("P", "W", "R", "B", "L", "C")
A_VALUES = (1.0, 0.1, 0.01)
GAPS = ("zero", "edge")

# what a line carries that is not compared (ruling 6)
NOT_COMPARED = {
    "id",
    "history",
    "group",
    "M",
    "beta",
    "A",
    "gap",
    "shard",
    "serial",
    "stored_alpha",
    "obtained",
    "equals",
    "values_from",
    "tag",
    "started",
    "ended",
    "wall",
    "peak_rss",
    "maxrss",
    "tuning_wall",
}
INTEGRATION_NOT_COMPARED = {"wall", "maxrss"}
BBN_NOT_COMPARED = {"PRyM_time", "wall"}


def a_label(A):
    return f"A{A:g}"


def task_id(hid, A, gap):
    return f"{hid}-{a_label(A)}-{gap}"


def load_tasks():
    return json.loads(TASKS_JSON.read_text())


def histories(tasks):
    out = []
    for t in tasks:
        if t["history"] not in [h["history"] for h in out]:
            out.append(
                {
                    k: t[k]
                    for k in (
                        "history",
                        "group",
                        "M",
                        "beta",
                        "shard",
                        "serial",
                        "stored_alpha",
                    )
                }
            )
    return out


def read_jsonl(path):
    path = Path(path)
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_text().splitlines() if x.strip()]


def run_lines(path=SAMPLE):
    """The `run` lines of a sample file, by task id (the last, if a task appears twice)."""
    out = {}
    for line in read_jsonl(path):
        if "tool_check" in line:
            continue
        if line.get("obtained") == "run":
            out[line["id"]] = line
    return out


def all_lines(path=SAMPLE):
    out = {}
    for line in read_jsonl(path):
        if "tool_check" in line:
            continue
        out[line["id"]] = line
    return out


def candidates_known(line) -> bool:
    """Whether every integration of a line has its candidate list (needed to decide S1 or S2)."""
    if line["outcome"] in ("timeout", "crashed"):
        return False
    ints = line.get("integrations") or []
    if not ints:
        return False
    return all(i.get("counts") is not None for i in ints)


def met(line, regime) -> int:
    """The candidates of a regime over every integration of a line."""
    return sum(i["counts"][regime] for i in line["integrations"])


def s1_holds(zero_line) -> bool:
    """S1 (prompt §1.2): the zero tuning at this A met no gap candidate in any of its integrations."""
    return candidates_known(zero_line) and met(zero_line, "gap") == 0


def s2_holds(zero_A1_line, gap: str) -> bool:
    """
    S2 (prompt §1.2) for a (history, prescription), decided from the zero tuning at A = 1: no one-loop candidate in any
    of its integrations, and under edge no gap candidate either.
    """
    if not candidates_known(zero_A1_line):
        return False
    if met(zero_A1_line, "one_loop") > 0:
        return False
    if gap == "edge" and met(zero_A1_line, "gap") > 0:
        return False
    return True


def _diff(a, b, path, out, skip=()):
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k in skip:
                continue
            if k not in a or k not in b:
                out.append(f"{path}.{k}: present in one only")
                continue
            _diff(a[k], b[k], f"{path}.{k}", out)
        return
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            out.append(f"{path}: lengths {len(a)} and {len(b)}")
            return
        for j, (x, y) in enumerate(zip(a, b)):
            _diff(x, y, f"{path}[{j}]", out)
        return
    if repr(a) != repr(b):
        out.append(f"{path}: {a!r} != {b!r}")


def compare_lines(a, b, shortcut: str):
    """
    The differences between two lines under ruling 6, as a list of strings (empty: equal). shortcut is "S1" (the
    record's gap_prescription may differ) or "S2" (its A may differ).
    """
    allowed = {"S1": "gap_prescription", "S2": "A"}[shortcut]
    out = []
    keys = (set(a) | set(b)) - NOT_COMPARED
    for k in sorted(keys):
        if k not in a or k not in b:
            out.append(f"{k}: present in one only")
            continue
        if k == "integrations":
            ia, ib = a[k], b[k]
            if len(ia) != len(ib):
                out.append(f"integrations: {len(ia)} and {len(ib)}")
                continue
            for j, (x, y) in enumerate(zip(ia, ib)):
                xd, yd = dict(x), dict(y)
                dx, dy = xd.pop("drain", None), yd.pop("drain", None)
                _diff(xd, yd, f"integrations[{j}]", out, skip=INTEGRATION_NOT_COMPARED)
                if dx is None or dy is None:
                    if dx is not dy:
                        out.append(f"integrations[{j}].drain: None in one only")
                else:
                    _diff(dx, dy, f"integrations[{j}].drain", out, skip={allowed})
        elif k == "bbn":
            _diff(a[k], b[k], "bbn", out, skip=BBN_NOT_COMPARED)
        else:
            _diff(a[k], b[k], k, out)
    return out


def compare_partials(id_a, prefix_a, id_b, prefix_b):
    """The full regime sequences and injection triples of two tasks' partial files, compared by repr."""
    pa = read_jsonl(TASK_DIR / f"{prefix_a}{id_a}.partial.jsonl")
    pb = read_jsonl(TASK_DIR / f"{prefix_b}{id_b}.partial.jsonl")
    out = []
    if len(pa) != len(pb):
        return [f"partial: {len(pa)} and {len(pb)} integrations"], 0
    n = 0
    for j, (x, y) in enumerate(zip(pa, pb)):
        for k in ("regimes", "injections"):
            if repr(x.get(k)) != repr(y.get(k)):
                out.append(f"partial[{j}].{k} differ")
            n += len(x.get(k) or [])
    return out, n


def g(x, fmt=".4g"):
    if x is None:
        return "None"
    if isinstance(x, bool):
        return str(x)
    if isinstance(x, int):
        return str(x)
    return format(x, fmt)
