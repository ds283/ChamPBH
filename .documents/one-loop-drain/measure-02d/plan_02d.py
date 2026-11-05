"""
one-loop-drain prompt 02d §1.4: the batches' task lists, the workers and the projection (orchestrator note 02d,
rulings 3-5). It writes no task line; run_02d.py runs the lists.

    plan_02d.py ids NAME       writes var/one-loop-drain/02d/batches/NAME/ids.txt for the batch NAME of ruling 3, and
                               prints it:
                                 check-1  tool:P-2197-A10, P-2197-A10, P-2197-A1, L-M1e-16-A1 (run with --tag check1-)
                                 check-2  P-2197-A1, L-M1e-16-A1 (run with --tag check2-)
                                 A1       the thirty at A = 1
                                 wide     the 150 at A = 10, 5, 2, 1.5 and 0.5
                                 P005     P-2197 and P-2188 at A = 0.05
                                 small    the rest of A = 0.1 and 0.05, 58 tasks
                               Within a batch C-3873's tasks come first, then the rest in the task list's order
                               (ruling 3); every id of A1, wide, P005 and small is checked not to be in sample.jsonl yet.
    plan_02d.py workers        min(6, floor(8 GiB / the largest peak RSS of any process of 02d so far)) (ruling 5),
                               from checks.jsonl and sample.jsonl
    plan_02d.py project        ruling 4's projection for `small`, after P005; exit 3 (stop and ask) if it exceeds three
                               hours or the workers come to fewer than three

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure-02d/plan_02d.py project \\
        > .documents/one-loop-drain/measure-02d/out/projection.txt

Ruling 4: each remaining task is costed at its own history's 02d A = 0.5 wall times r, where r is 02b's A = 0.05 wall
over 02b's A = 0.5 wall (both `zero` lines, both run), or, for the five histories whose 02b lines are S2's, 02's A = 0.1
wall over 02's A = 1 wall (`zero`, both run); P's measured ratio (the larger of P-2197's and P-2188's 02d A = 0.05 /
A = 0.5) is used where it is larger. The same r serves A = 0.1, an upper estimate. Each cost is capped at 3630 s; the
projected wall is the larger of the summed cost over the workers and the longest single task.
"""

import json
import sys
from pathlib import Path

ROOT = Path("/Users/ds283/Documents/Code/ChamPBH")
OUT = ROOT / "var/one-loop-drain/02d"
TASKS_JSON = OUT / "tasks.json"
SAMPLE = OUT / "sample.jsonl"
CHECKS = OUT / "checks.jsonl"
BATCH_DIR = OUT / "batches"
SAMPLE_02 = ROOT / "var/one-loop-drain/02/sample.jsonl"
SAMPLE_02B = ROOT / "var/one-loop-drain/02b/sample.jsonl"

GIB = 2**30
MEMORY_BUDGET = 8 * GIB
MAX_WORKERS = 6
COST_CAP_S = 3630.0
LIMIT_S = 3 * 3600.0
FIRST = "C-3873"


def read_jsonl(path):
    path = Path(path)
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_text().splitlines() if x.strip()]


def tasks():
    return json.loads(TASKS_JSON.read_text())


def ordered(ids):
    """Ruling 3: C-3873's tasks first, then the rest in the task list's order."""
    order = [t["id"] for t in tasks()]
    ids = sorted(ids, key=order.index)
    return [i for i in ids if i.startswith(FIRST + "-")] + [
        i for i in ids if not i.startswith(FIRST + "-")
    ]


def batch_ids(name):
    ts = tasks()
    if name == "check-1":
        return [
            "tool:P-2197-A10",
            "P-2197-A10",
            "P-2197-A1",
            "L-M1e-16-A1",
        ]
    if name == "check-2":
        return ["P-2197-A1", "L-M1e-16-A1"]
    if name == "A1":
        sel = [t["id"] for t in ts if t["A"] == 1.0]
    elif name == "wide":
        sel = [t["id"] for t in ts if t["A"] in (10.0, 5.0, 2.0, 1.5, 0.5)]
    elif name == "P005":
        sel = ["P-2197-A0.05", "P-2188-A0.05"]
    elif name == "small":
        sel = [
            t["id"]
            for t in ts
            if t["A"] in (0.1, 0.05) and t["id"] not in ("P-2197-A0.05", "P-2188-A0.05")
        ]
    else:
        raise SystemExit(f"unknown batch {name}")
    done = {l["id"] for l in read_jsonl(SAMPLE) if "tool_check" not in l}
    again = [i for i in sel if i in done]
    if again:
        raise SystemExit(f"already in sample.jsonl: {again}")
    return ordered(sel)


def ids(name):
    lst = batch_ids(name)
    d = BATCH_DIR / name
    d.mkdir(parents=True, exist_ok=True)
    (d / "ids.txt").write_text("\n".join(lst) + "\n")
    print(f"{name}: {len(lst)} items, written to {d / 'ids.txt'}")
    for x in lst:
        print(f"   {x}")


def peak_rss():
    lines = read_jsonl(CHECKS) + read_jsonl(SAMPLE)
    best = None
    for l in lines:
        r = l.get("peak_rss")
        if r is not None and (best is None or r > best[0]):
            best = (r, l.get("id") or f"tool:{l.get('tool_check')}", l.get("tag", ""))
    return best


def n_workers():
    best = peak_rss()
    if best is None:
        return MAX_WORKERS, None
    return min(MAX_WORKERS, MEMORY_BUDGET // best[0]), best


def workers():
    n, best = n_workers()
    if best is None:
        print(f"no 02d line yet: {MAX_WORKERS} workers")
        return
    print(
        f"largest peak RSS so far: {best[0]} bytes = {best[0] / GIB:.3f} GiB ({best[2]}{best[1]}); "
        f"workers = min({MAX_WORKERS}, floor(8 GiB / that)) = {n}"
    )


def project():
    lines = {
        l["id"]: l
        for l in read_jsonl(SAMPLE)
        if "tool_check" not in l and l.get("tag") == ""
    }
    old02 = {l["id"]: l for l in read_jsonl(SAMPLE_02) if "tool_check" not in l}
    old02b = {l["id"]: l for l in read_jsonl(SAMPLE_02B) if "tool_check" not in l}
    hs = []
    for t in tasks():
        if t["history"] not in hs:
            hs.append(t["history"])
    # P's measured ratio
    pr = {}
    for h in ("P-2197", "P-2188"):
        a, b = lines[f"{h}-A0.05"], lines[f"{h}-A0.5"]
        pr[h] = a["wall"] / b["wall"]
    p_ratio = max(pr.values())
    n, best = n_workers()
    print(
        "# ruling 4's projection for the batch `small` (the rest of A = 0.1 and 0.05)"
    )
    print(
        f"# P's measured ratio, 02d A=0.05 wall / A=0.5 wall: "
        + ", ".join(
            f"{h} {lines[f'{h}-A0.05']['wall']:.1f}/{lines[f'{h}-A0.5']['wall']:.1f} = {r:.3f}"
            for h, r in pr.items()
        )
        + f"; the larger is {p_ratio:.3f}"
    )
    print(
        f"# workers: largest peak RSS so far {best[0] / GIB:.3f} GiB ({best[2]}{best[1]}), "
        f"min(6, floor(8 GiB / that)) = {n}"
    )
    print(
        "# r: 02b A=0.05 / A=0.5 (zero, run), or for S2's five 02 A=0.1 / A=1 (zero, run); P's ratio where larger"
    )
    print(
        "history       02d A=0.5 wall (outcome)   r source                     r_hist   r used   cost per task  tasks"
    )
    total = 0.0
    longest = 0.0
    rows = []
    for h in hs:
        rest = [
            f"{h}-A0.1",
            f"{h}-A0.05",
        ]
        rest = [x for x in rest if x not in ("P-2197-A0.05", "P-2188-A0.05")]
        base = lines[f"{h}-A0.5"]
        b05 = old02b.get(f"{h}-A0.05-zero")
        b5 = old02b.get(f"{h}-A0.5-zero")
        if (
            b05 is not None
            and b5 is not None
            and b05["obtained"] == "run"
            and b5["obtained"] == "run"
        ):
            r_h = b05["wall"] / b5["wall"]
            src = f"02b {b05['wall']:.1f}/{b5['wall']:.1f}"
        else:
            o1, o01 = old02[f"{h}-A1-zero"], old02[f"{h}-A0.1-zero"]
            assert o1["obtained"] == "run" and o01["obtained"] == "run"
            r_h = o01["wall"] / o1["wall"]
            src = f"02 (S2 in 02b) {o01['wall']:.1f}/{o1['wall']:.1f}"
        r = max(r_h, p_ratio)
        cost = min(COST_CAP_S, base["wall"] * r)
        total += cost * len(rest)
        longest = max(longest, cost)
        rows.append((h, base, src, r_h, r, cost, rest))
        print(
            f"{h:12s}  {base['wall']:9.1f} ({base['outcome']:7s})      {src:28s} {r_h:7.3f}  {r:7.3f}  "
            f"{cost:12.1f}   {len(rest)}"
        )
    ntask = sum(len(x[6]) for x in rows)
    wall = max(total / n, longest)
    print(
        f"\n{ntask} tasks; summed cost {total:.0f} s = {total / 3600:.2f} h of CPU; over {n} workers "
        f"{total / n:.0f} s; longest single task {longest:.0f} s"
    )
    print(
        f"projected wall = max({total / n:.0f}, {longest:.0f}) s = {wall:.0f} s = {wall / 3600:.2f} h "
        f"(limit 3 h; workers {n}, floor 3)"
    )
    if wall > LIMIT_S or n < 3:
        print(
            "!! STOP: the projection exceeds three hours, or the workers are fewer than three"
        )
        sys.exit(3)
    print("within the limit: proceed")


def main():
    sub = sys.argv[1]
    if sub == "ids":
        ids(sys.argv[2])
    elif sub == "workers":
        workers()
    elif sub == "project":
        project()
    else:
        raise SystemExit(f"unknown subcommand {sub}")


if __name__ == "__main__":
    main()
