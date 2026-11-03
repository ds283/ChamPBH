"""
one-loop-drain prompt 02 §1.2, §1.4: the order of the batches and the shortcuts' decisions, under orchestrator note
02, rulings 1 and 5. Each subcommand reads var/one-loop-drain/02/sample.jsonl and tasks.json, prints what it decided,
and writes the next batch's id file where there is one. Exit 3 is a stop (ruling 1's free S2 check, or ruling 5's
projection or worker floor).

    decide.py step1a   IDS          the zero passes at A = 1, then 0.1, every history (60 tasks)
    decide.py step1b   IDS [ONLY]   after step1a: S2 for each history (zero, edge), ruling 1's free check of S2 at
                                    A = 0.1, S1 at A = 1 and 0.1; IDS gets the edge runs S1 does not cover
    decide.py step2a   IDS          the zero pass at A = 0.01 for P's two
    decide.py project  IDS          after step2a: ruling 5's workers and projection; IDS gets the rest of A = 0.01's
                                    zero pass, in the order R, B, W, L (C only where S2 fails), less S2's
    decide.py step3    IDS          after step2b: the edge runs at A = 0.01 that S1 does not cover
    decide.py derive                after every run: appends a line for every task not run, obtained "S1" or "S2", with
                                    the task it equals ("equals") and a copy of that task's values (so the record's A or
                                    gap_prescription in it is the source's, which is the one value that differs by
                                    construction: ruling 6)

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure/decide.py SUBCOMMAND [IDS] | tee -a \\
        .documents/one-loop-drain/measure/out/decisions.txt
"""

import json
import sys
from math import floor

from common import (
    A_VALUES,
    GROUP_ORDER,
    SAMPLE,
    all_lines,
    candidates_known,
    compare_lines,
    histories,
    load_tasks,
    met,
    run_lines,
    s1_holds,
    s2_holds,
    task_id,
)

GiB = 2**30
MEMORY_BUDGET = 8 * GiB
MAX_WORKERS = 6
PROJECTION_LIMIT_S = 4 * 3600.0
WORKER_FLOOR = 3


def write_ids(path, ids, header):
    with open(path, "w") as f:
        for i in ids:
            f.write(i + "\n")
    print(f"{header}: {len(ids)} tasks -> {path}")
    for i in ids:
        print(f"   {i}")


def line_summary(line):
    ints = line.get("integrations") or []
    tot = {
        r: sum((i.get("counts") or {}).get(r, 0) for i in ints)
        for r in ("one_loop", "gap", "classical", "unassessed")
    }
    return (
        f"{line['outcome']:8s} integrations={len(ints)} one_loop={tot['one_loop']} gap={tot['gap']} "
        f"classical={tot['classical']} unassessed={tot['unassessed']} wall={line['wall']:.1f}"
    )


def need(runs, ids):
    missing = [i for i in ids if i not in runs]
    if missing:
        raise SystemExit(f"missing run lines: {missing}")


def step1a(ids_path):
    tasks = load_tasks()
    hs = histories(tasks)
    ids = [task_id(h["history"], A, "zero") for A in (1.0, 0.1) for h in hs]
    write_ids(ids_path, ids, "step 1a (zero, A = 1 then 0.1)")


def s2_table(runs, hs):
    out = {}
    for h in hs:
        z1 = runs[task_id(h["history"], 1.0, "zero")]
        out[h["history"]] = {
            "zero": s2_holds(z1, "zero"),
            "edge": s2_holds(z1, "edge"),
            "known": candidates_known(z1),
        }
    return out


def step1b(ids_path, only=None):
    tasks = load_tasks()
    hs = histories(tasks)
    if only is not None:
        # a subset of the histories, decided once both their zero runs have ended (a comma-separated list, or
        # "!h1,h2" for every history but those)
        if only.startswith("!"):
            hs = [h for h in hs if h["history"] not in only[1:].split(",")]
        else:
            hs = [h for h in hs if h["history"] in only.split(",")]
        print(f"step 1b for {len(hs)} histories: {only}")
    runs = run_lines()
    need(runs, [task_id(h["history"], A, "zero") for A in (1.0, 0.1) for h in hs])
    s2 = s2_table(runs, hs)
    print("step 1b: the zero passes at A = 1 and 0.1")
    for A in (1.0, 0.1):
        for h in hs:
            tid = task_id(h["history"], A, "zero")
            print(f"   {tid:28s} {line_summary(runs[tid])}")
    print(
        "S2 from the zero tuning at A = 1 (zero: no one-loop candidate; edge: and no gap candidate)"
    )
    for h in hs:
        s = s2[h["history"]]
        print(
            f"   {h['history']:12s} known={s['known']} S2(zero)={s['zero']} S2(edge)={s['edge']}"
        )
    print(
        "ruling 1's free check of S2: the zero run at A = 0.1 against A = 1, where S2(zero) holds"
    )
    stop = False
    checked = 0
    for h in hs:
        if not s2[h["history"]]["zero"]:
            continue
        a = runs[task_id(h["history"], 1.0, "zero")]
        b = runs[task_id(h["history"], 0.1, "zero")]
        diffs = compare_lines(a, b, "S2")
        checked += 1
        if diffs:
            stop = True
            print(f"   !! STOP {h['history']}: {len(diffs)} differences")
            for d in diffs[:20]:
                print(f"      {d}")
        else:
            print(f"   {h['history']:12s} A=0.1 equals A=1 under ruling 6")
    print(f"   {checked} checked")
    ids = []
    print("S1 at A = 1 and 0.1 (the zero tuning at that A met no gap candidate)")
    for A in (1.0, 0.1):
        for h in hs:
            z = runs[task_id(h["history"], A, "zero")]
            s1 = s1_holds(z)
            print(f"   {task_id(h['history'], A, 'edge'):28s} S1={s1}")
            if not s1:
                ids.append(task_id(h["history"], A, "edge"))
    write_ids(
        ids_path, ids, "step 1b (edge runs at A = 1 and 0.1 that S1 does not cover)"
    )
    if stop:
        print("!! STOP: ruling 1's free check of S2 found a difference")
        sys.exit(3)


def step2a(ids_path):
    ids = [task_id(h, 0.01, "zero") for h in ("P-2197", "P-2188")]
    write_ids(ids_path, ids, "step 2a (zero, A = 0.01, P's two)")


def remaining_zero(runs, hs):
    s2 = s2_table(runs, hs)
    ids, derived = [], []
    for group in ("R", "B", "W", "L", "C"):
        for h in hs:
            if h["group"] != group:
                continue
            tid = task_id(h["history"], 0.01, "zero")
            if s2[h["history"]]["zero"]:
                derived.append(tid)
            else:
                ids.append(tid)
    return ids, derived


def project(ids_path):
    tasks = load_tasks()
    hs = histories(tasks)
    runs = run_lines()
    p_ids = [task_id(h, 0.01, "zero") for h in ("P-2197", "P-2188")]
    need(runs, p_ids)
    print("ruling 5: the projection after P's two at A = 0.01")
    walls, rss = [], []
    for tid in p_ids:
        line = runs[tid]
        walls.append(line["wall"])
        rss.append(line["peak_rss"])
        print(
            f"   {tid:28s} {line_summary(line)} peak_rss={line['peak_rss'] / GiB:.3f} GiB"
        )
    workers = min(MAX_WORKERS, floor(MEMORY_BUDGET / max(rss)))
    mean = sum(walls) / len(walls)
    n_zero = sum(1 for h in hs if h["group"] in ("W", "R", "B", "L"))
    n_edge = sum(1 for h in hs if h["group"] in ("P", "W", "R", "B", "L"))
    n = n_zero + n_edge
    projection = n * mean / workers if workers > 0 else float("inf")
    print(
        f"   workers = min({MAX_WORKERS}, floor(8 GiB / {max(rss) / GiB:.3f} GiB)) = {workers}"
    )
    print(
        f"   tasks counted: {n_zero} zero (W, R, B, L) + {n_edge} edge (P, W, R, B, L) = {n}, each at the mean "
        f"wall of P's two, {mean:.1f} s"
    )
    print(
        f"   projection = {n} x {mean:.1f} s / {workers} = {projection:.0f} s = {projection / 3600:.2f} h "
        f"(limit {PROJECTION_LIMIT_S / 3600:g} h, worker floor {WORKER_FLOOR})"
    )
    ids, derived = remaining_zero(runs, hs)
    write_ids(
        ids_path,
        ids,
        f"step 2b (zero, A = 0.01, R B W L, less S2's) at {workers} workers",
    )
    print(f"   derived by S2 (equal to A = 1), not run: {derived}")
    with open(str(ids_path) + ".workers", "w") as f:
        f.write(f"{workers}\n")
    if projection > PROJECTION_LIMIT_S or workers < WORKER_FLOOR:
        print("!! STOP: ruling 5's projection or worker floor")
        sys.exit(3)


def step3(ids_path):
    tasks = load_tasks()
    hs = histories(tasks)
    runs = run_lines()
    ids = []
    print(
        "S1 at A = 0.01 (the zero tuning at A = 0.01, run or S2's, met no gap candidate)"
    )
    s2 = s2_table(runs, hs)
    for h in hs:
        tid = task_id(h["history"], 0.01, "zero")
        src = (
            tid if not s2[h["history"]]["zero"] else task_id(h["history"], 1.0, "zero")
        )
        need(runs, [src])
        s1 = s1_holds(runs[src])
        print(f"   {task_id(h['history'], 0.01, 'edge'):28s} from {src:28s} S1={s1}")
        if not s1:
            ids.append(task_id(h["history"], 0.01, "edge"))
    write_ids(ids_path, ids, "step 3 (edge runs at A = 0.01 that S1 does not cover)")


def derive():
    tasks = load_tasks()
    hs = histories(tasks)
    runs = run_lines()
    lines = all_lines()
    s2 = s2_table(runs, hs)
    new = []

    def source_of(hid, A, gap):
        tid = task_id(hid, A, gap)
        if tid in runs:
            return tid, "run"
        if gap == "zero":
            if A == 0.01 and s2[hid]["zero"]:
                return task_id(hid, 1.0, "zero"), "S2"
            raise SystemExit(f"no run and no shortcut for {tid}")
        z, _ = source_of(hid, A, "zero")
        if s1_holds(runs[z]):
            return z, "S1"
        raise SystemExit(f"no run and no shortcut for {tid}")

    def immediate(hid, A, gap, how):
        # the task a derived task equals by its own shortcut: S2, the zero task at A = 1; S1, the zero task at its A
        return task_id(hid, 1.0, "zero") if how == "S2" else task_id(hid, A, "zero")

    by_id = {t["id"]: t for t in tasks}
    for h in hs:
        for A in A_VALUES:
            for gap in ("zero", "edge"):
                tid = task_id(h["history"], A, gap)
                src, how = source_of(h["history"], A, gap)
                if how == "run" or tid in lines:
                    continue
                if src not in runs:
                    raise SystemExit(f"{tid}: its source {src} has no run line")
                line = dict(runs[src])
                for k in (
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
                ):
                    line[k] = by_id[tid][k]
                line["obtained"] = how
                line["equals"] = immediate(h["history"], A, gap, how)
                line["values_from"] = src
                for k in (
                    "tag",
                    "started",
                    "ended",
                    "wall",
                    "peak_rss",
                    "exit_status",
                    "maxrss",
                    "tuning_wall",
                ):
                    line.pop(k, None)
                new.append(line)
    with open(SAMPLE, "a") as f:
        for line in new:
            f.write(json.dumps(line) + "\n")
    print(f"derive: {len(new)} lines appended to {SAMPLE}")
    for line in new:
        print(f"   {line['id']:28s} {line['obtained']} = {line['equals']}")
    total = all_lines()
    print(f"   sample.jsonl now holds {len(total)} task lines of {len(tasks)}")


def main():
    cmd = sys.argv[1]
    if cmd == "derive":
        derive()
        return
    fn = {
        "step1a": step1a,
        "step1b": step1b,
        "step2a": step2a,
        "project": project,
        "step3": step3,
    }[cmd]
    fn(*sys.argv[2:])


if __name__ == "__main__":
    main()
