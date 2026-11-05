"""
one-loop-drain prompt 02d §1.4 step 1: the driver's checks, from var/one-loop-drain/02d/checks.jsonl (the tagged lines
of the batches check-1, tag `check1-`, and check-2, tag `check2-`; orchestrator note 02d, ruling 3) and the tasks'
files under var/one-loop-drain/02d/tasks/. A difference in either is a stop (exit 3).

    checks_02d.py tool         the tool check: `tools/history_and_bbn.py` itself on P-2197 at A = 10 (check-1's
                               `tool:` line, stdout in tasks/check1-tool-P-2197-A10.log) against the driver task
                               (tasks/check1-P-2197-A10.log): every stdout line byte-equal once `wall=...` and
                               `PRyM_time=...` are masked (note 02's ruling 9); and the tool's history, drain and bbn
                               lines against the driver's JSON line.
    checks_02d.py determinism  P-2197 and L-M1e-16 at A = 1, run in check-1 and again in check-2: the two lines equal
                               under note 02's ruling 6 with the record's `model` in place of its gap prescription
                               (nothing of the record is excused: A and model are the same), and the partial files'
                               regimes, injections and candidates equal by repr, per integration (note 02d ruling 8).
                               Where the A1 pass has run, its untagged line and partial are compared with check-1's
                               too (not required by the prompt; reported apart).

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure-02d/checks_02d.py tool \\
        > .documents/one-loop-drain/measure-02d/out/tool_check.txt

Copies, each changed as marked (prompt 02d §1.2):
  - from `.documents/one-loop-drain/measure/common.py`: NOT_COMPARED, INTEGRATION_NOT_COMPARED, BBN_NOT_COMPARED,
    read_jsonl, _diff, compare_lines, compare_partials;
  - from `.documents/one-loop-drain/measure/tool_check.py`: MASK, masked, fields, and main as `tool`.
"""

import json
import re
import sys
from pathlib import Path

ROOT = Path("/Users/ds283/Documents/Code/ChamPBH")
OUT = ROOT / "var/one-loop-drain/02d"
TASK_DIR = OUT / "tasks"
CHECKS = OUT / "checks.jsonl"
SAMPLE = OUT / "sample.jsonl"

TOOL_TASK = "P-2197-A10"
TOOL_TAG = "check1-"
DETERMINISM = ("P-2197-A1", "L-M1e-16-A1")
TAGS = ("check1-", "check2-")

# source: measure/common.py, NOT_COMPARED; changed: no "gap" (02d's tasks have none)
NOT_COMPARED = {
    "id",
    "history",
    "group",
    "M",
    "beta",
    "A",
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
# source: measure/common.py (unchanged)
INTEGRATION_NOT_COMPARED = {"wall", "maxrss"}
BBN_NOT_COMPARED = {"PRyM_time", "wall"}


# source: measure/common.py, read_jsonl (unchanged)
def read_jsonl(path):
    path = Path(path)
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_text().splitlines() if x.strip()]


# source: measure/common.py, _diff (unchanged)
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


# source: measure/common.py, compare_lines; changed: no shortcut, so no field of the record is excused (ruling 8)
def compare_lines(a, b):
    """The differences between two lines under note 02's ruling 6 (empty: equal), nothing of the record excused."""
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
                _diff(x, y, f"integrations[{j}]", out, skip=INTEGRATION_NOT_COMPARED)
        elif k == "bbn":
            _diff(a[k], b[k], "bbn", out, skip=BBN_NOT_COMPARED)
        else:
            _diff(a[k], b[k], k, out)
    return out


# source: measure/common.py, compare_partials; changed: "candidates" compared too (ruling 8), and the paths are 02d's
def compare_partials(id_a, prefix_a, id_b, prefix_b):
    """The full regime sequences, injection triples and DrainCandidate lists of two partial files, by repr."""
    pa = read_jsonl(TASK_DIR / f"{prefix_a}{id_a}.partial.jsonl")
    pb = read_jsonl(TASK_DIR / f"{prefix_b}{id_b}.partial.jsonl")
    out = []
    if len(pa) != len(pb):
        return [f"partial: {len(pa)} and {len(pb)} integrations"], 0, 0
    n, nc = 0, 0
    for j, (x, y) in enumerate(zip(pa, pb)):
        for k in ("regimes", "injections", "candidates"):
            if repr(x.get(k)) != repr(y.get(k)):
                out.append(f"partial[{j}].{k} differ")
        n += len(x.get("injections") or [])
        nc += len(x.get("candidates") or [])
    return out, n, nc


# source: measure/tool_check.py, MASK, masked, fields (unchanged)
MASK = re.compile(r"\b(wall|PRyM_time)=[^ ]+")


def masked(text):
    return [MASK.sub(r"\1=*", x) for x in text.splitlines()]


def fields(line):
    return dict(kv.split("=", 1) for kv in line.split(": ", 1)[1].split() if "=" in kv)


def tagged_lines(tag):
    return {
        l["id"]: l
        for l in read_jsonl(CHECKS)
        if "tool_check" not in l and l.get("tag") == tag
    }


# source: measure/tool_check.py, main; changed: one task (P-2197 at A = 10), its driver line from checks.jsonl, the
# record's model checked in the history line
def tool():
    runs = tagged_lines(TOOL_TAG)
    tools = {
        l["tool_check"]: l
        for l in read_jsonl(CHECKS)
        if "tool_check" in l and l.get("tag") == TOOL_TAG
    }
    stop = False
    tid = TOOL_TASK
    drv = (TASK_DIR / f"{TOOL_TAG}{tid}.log").read_text()
    tool_out = (TASK_DIR / f"{TOOL_TAG}tool-{tid}.log").read_text()
    a, b = masked(drv), masked(tool_out)
    diffs = [(j, x, y) for j, (x, y) in enumerate(zip(a, b)) if x != y]
    print(
        f"{tid}: driver stdout {len(a)} lines, tool stdout {len(b)} lines, tool rc {tools[tid]['rc']}; "
        f"{len(a) - len(diffs) if len(a) == len(b) else 'n/a'} byte-equal after masking `wall=` and `PRyM_time=`"
    )
    if len(a) != len(b) or diffs:
        stop = True
        print(f"   !! line counts {len(a)} and {len(b)}; {len(diffs)} lines differ")
        for j, x, y in diffs[:10]:
            print(f"   [{j}] driver: {x}\n   [{j}]   tool: {y}")
    line = runs[tid]
    acc = line["accepted"]
    bbn = line["bbn"]
    tl = {
        x.split(" ", 1)[0]: x
        for x in tool_out.splitlines()
        if x.split(" ", 1)[0] in ("history", "drain", "bbn")
    }
    h, d, bb = fields(tl["history"]), fields(tl["drain"]), fields(tl["bbn"])
    want = {
        ("history", "accepted_steps"): str(acc["accepted_steps"]),
        ("history", "reflections"): str(acc["reflections"]),
        ("history", "samples"): str(acc["n_samples"]),
        ("history", "RHS"): str(acc["RHS_evaluations"]),
        ("history", "integrations"): str(line["tuning"]["tuning_iterations"]),
        ("history", "model"): acc["model"],
        ("bbn", "Yp"): format(bbn["Yp"], ".10g"),
        ("bbn", "DoH"): format(bbn["DoH"], ".10g"),
        ("bbn", "He3oH"): format(bbn["He3oH"], ".10g"),
        ("bbn", "Li7oH"): format(bbn["Li7oH"], ".10g"),
    }
    for k, v in acc["drain_values"].items():
        want[("drain", k)] = "None" if v is None else format(v, ".6g")
    got = {"history": h, "drain": d, "bbn": bb}
    bad = [(s, k, v, got[s].get(k)) for (s, k), v in want.items() if got[s].get(k) != v]
    print(
        f"   history, drain and bbn lines against the driver's JSON line: {len(want) - len(bad)} of {len(want)} "
        f"values equal"
    )
    for s, k, v, gv in bad:
        print(f"   !! {s}.{k}: driver {v} tool {gv}")
    if bad:
        stop = True
    if stop:
        print("!! STOP: the driver disagrees with the tool")
        sys.exit(3)
    print("the driver agrees with the tool")


def _one(tid, la, pa, lb, pb, label):
    d = compare_lines(la, lb)
    p, n, nc = compare_partials(tid, pa, tid, pb)
    so = masked((TASK_DIR / f"{pa}{tid}.log").read_text())
    sb = masked((TASK_DIR / f"{pb}{tid}.log").read_text())
    s_eq = so == sb
    ints = la.get("integrations") or []
    print(
        f"{tid} ({label}): outcome {la['outcome']} / {lb['outcome']}; {len(ints)} integration(s); "
        f"{n} injections and {nc} candidates in the partial files; line differences {len(d)}; partial "
        f"differences {len(p)}; stdout {'equal' if s_eq else 'DIFFERENT'} after masking ({len(so)} lines)"
    )
    for x in (d + p)[:20]:
        print(f"   !! {x}")
    if la.get("accepted"):
        acc = la["accepted"]
        print(
            f"   samples_sha256 {acc['samples_sha256']}; Delta_N_eff_today "
            f"{acc['dissipation']['Delta_N_eff_today']!r}; Yp {la['bbn']['Yp']!r}; model {acc['model']}"
        )
    return not d and not p


def determinism():
    a, b = tagged_lines(TAGS[0]), tagged_lines(TAGS[1])
    print(
        "# the determinism check (prompt 02d §1.4 step 1; note 02d ruling 8): check-1's run against check-2's, under"
    )
    print(
        "# note 02's ruling 6 with nothing of the record excused, and the partial files' regimes, injections and"
    )
    print("# candidates by repr per integration")
    stop = False
    for tid in DETERMINISM:
        if not _one(tid, a[tid], TAGS[0], b[tid], TAGS[1], "check-1 against check-2"):
            stop = True
    sample = {
        l["id"]: l
        for l in read_jsonl(SAMPLE)
        if "tool_check" not in l and l.get("tag") == ""
    }
    extra = [t for t in DETERMINISM if t in sample]
    if extra:
        print(
            "\n# not required: check-1's run against the A1 pass's untagged run of the same task"
        )
        for tid in extra:
            if not _one(tid, a[tid], TAGS[0], sample[tid], "", "check-1 against A1"):
                stop = True
    if stop:
        print("!! STOP: a run is not bitwise equal to its repeat")
        sys.exit(3)
    print("\nevery pair is bitwise equal")


def main():
    {"tool": tool, "determinism": determinism}[sys.argv[1]]()


if __name__ == "__main__":
    main()
