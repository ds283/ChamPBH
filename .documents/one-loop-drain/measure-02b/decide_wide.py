"""
one-loop-drain prompt 02b §1.2, §1.4: the order of the batches and the shortcuts' decisions, under orchestrator note
02b, rulings 1, 4 and 5. Each subcommand reads 02b's var/one-loop-drain/02b/sample.jsonl (and, for S2, 02's
var/one-loop-drain/02/sample.jsonl), prints what it decided, and writes the next batch's id file where there is one.
Exit 3 is a stop (ruling 5's projection or worker floor).

    decide_wide.py repro    IDS   ruling 4: P-2197-A1-zero and R-865-A0.1-zero, re-run by 02b's driver
    decide_wide.py step2a   IDS   S2 for each history (from 02's zero tuning at A = 1); the zero passes at A = 10, 5,
                                  2, 1.5 and 0.5, in that order, every history less S2's (ruling 1)
    decide_wide.py step2b   IDS   after step2a: S1 at those A (from the zero task, run or S2's); IDS gets the edge runs
                                  S1 does not cover, and IDS.workers ruling 5's workers
    decide_wide.py step3a   IDS   the zero pass at A = 0.05 for P's two (2197, 2188), at two workers
    decide_wide.py project  IDS   after step3a: ruling 5's workers and projection; IDS gets the rest of A = 0.05's zero
                                  pass, in the order W, R, B, L (C only where S2 fails), less S2's
    decide_wide.py step3c   IDS   after step3b: the edge runs at A = 0.05 that S1 does not cover
    decide_wide.py derive         after every run: appends to 02b's sample.jsonl a line for every 02b task not run,
                                  obtained "S1" or "S2", with "equals" (the task its own shortcut equates it with) and
                                  "values_from" (the run line whose values it carries, 02's line for S2), as 02's
                                  decide.py derive does (02's log, D4)

S2, generalised to every A (prompt 02b §1.2): if 02's A = 1 zero tuning of a history met no one-loop candidate in any
integration, the zero tuning at any A of 02b makes the same integrations bit for bit, with only the record's A
differing; under edge, the same holds if it met no gap candidate either. 02's s2_holds decides it, unchanged. S1 is
02's s1_holds, unchanged, on the zero task at the same A (run, or S2's, in which case 02's A = 1 zero line).

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure-02b/decide_wide.py SUB [IDS] | tee -a \\
        .documents/one-loop-drain/measure-02b/out/decisions.txt
"""

import json
import sys
from math import floor
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import wide  # noqa: E402
from wide import (  # noqa: E402
    A_02B,
    REPRO,
    SAMPLE,
    SPOT,
    candidates_known,
    histories,
    load_tasks,
    run_lines_02,
    run_lines_02b,
    s1_holds,
    s2_holds,
    task_id,
)

GiB = 2**30
MEMORY_BUDGET = 8 * GiB
MAX_WORKERS = 6
PROJECTION_LIMIT_S = 3 * 3600.0
WORKER_FLOOR = 3
STEP2_A = (10.0, 5.0, 2.0, 1.5, 0.5)
A_SMALL = 0.05


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
        f"classical={tot['classical']} unassessed={tot['unassessed']} wall={line.get('wall', 0.0):.1f}"
    )


def need(runs, ids):
    missing = [i for i in ids if i not in runs]
    if missing:
        raise SystemExit(f"missing run lines: {missing}")


def s2_table(hs):
    """S2 for each history, from 02's zero run at A = 1 (02's s2_holds)."""
    runs02 = run_lines_02()
    out = {}
    for h in hs:
        z1 = runs02[task_id(h["history"], 1.0, "zero")]
        out[h["history"]] = {
            "zero": s2_holds(z1, "zero"),
            "edge": s2_holds(z1, "edge"),
            "known": candidates_known(z1),
            "source": z1,
        }
    return out


def zero_source(hid, A, runs, s2):
    """The run line a zero task at A of 02b carries: its own run, or, under S2, 02's zero run at A = 1."""
    if s2[hid]["zero"]:
        return s2[hid]["source"]
    tid = task_id(hid, A, "zero")
    need(runs, [tid])
    return runs[tid]


def peak_rss_so_far():
    """The largest peak RSS of any task run in 02b so far (sample, reproduction and spot runs), in bytes."""
    rss = []
    for path in (SAMPLE, REPRO, SPOT):
        for line in wide.read_jsonl(path):
            if "peak_rss" in line and line.get("obtained", "run") == "run":
                rss.append(line["peak_rss"])
    return max(rss) if rss else None


def workers_now():
    rss = peak_rss_so_far()
    w = min(MAX_WORKERS, floor(MEMORY_BUDGET / rss)) if rss else MAX_WORKERS
    return w, rss


def repro(ids_path):
    write_ids(
        ids_path,
        ["P-2197-A1-zero", "R-865-A0.1-zero"],
        "the reproduction check (ruling 4), at two workers",
    )


def step2a(ids_path):
    hs = histories(load_tasks())
    s2 = s2_table(hs)
    print(
        "S2 from 02's zero tuning at A = 1 (zero: no one-loop candidate; edge: and no gap candidate)"
    )
    for h in hs:
        s = s2[h["history"]]
        print(
            f"   {h['history']:12s} known={s['known']} S2(zero)={s['zero']} S2(edge)={s['edge']}   "
            f"02's {task_id(h['history'], 1.0, 'zero')}: {line_summary(s['source'])}"
        )
    ids, derived = [], []
    for A in STEP2_A:
        for h in hs:
            tid = task_id(h["history"], A, "zero")
            (derived if s2[h["history"]]["zero"] else ids).append(tid)
    write_ids(
        ids_path, ids, "step 2a (zero, A = 10, 5, 2, 1.5, 0.5, every history less S2's)"
    )
    print(f"   derived by S2 (equal to 02's A = 1), not run: {len(derived)}: {derived}")


def step2b(ids_path):
    hs = histories(load_tasks())
    runs = run_lines_02b()
    s2 = s2_table(hs)
    print("step 2b: the zero passes at A = 10, 5, 2, 1.5, 0.5")
    for A in STEP2_A:
        for h in hs:
            tid = task_id(h["history"], A, "zero")
            if s2[h["history"]]["zero"]:
                print(f"   {tid:28s} S2 = 02's {task_id(h['history'], 1.0, 'zero')}")
            else:
                need(runs, [tid])
                print(f"   {tid:28s} {line_summary(runs[tid])}")
    ids = []
    print(
        "S1 at A = 10, 5, 2, 1.5, 0.5 (the zero tuning at that A, run or S2's, met no gap candidate)"
    )
    for A in STEP2_A:
        for h in hs:
            z = zero_source(h["history"], A, runs, s2)
            s1 = s1_holds(z)
            print(f"   {task_id(h['history'], A, 'edge'):28s} S1={s1}")
            if not s1:
                ids.append(task_id(h["history"], A, "edge"))
    workers, rss = workers_now()
    write_ids(
        ids_path,
        ids,
        f"step 2b (edge runs at A = 10, 5, 2, 1.5, 0.5 that S1 does not cover) at {workers} workers "
        f"(ruling 5: largest peak RSS so far {rss / GiB:.3f} GiB)",
    )
    with open(str(ids_path) + ".workers", "w") as f:
        f.write(f"{workers}\n")


def step3a(ids_path):
    ids = [task_id(h, A_SMALL, "zero") for h in ("P-2197", "P-2188")]
    write_ids(ids_path, ids, "step 3a (zero, A = 0.05, P's two), at two workers")


def remaining_zero(hs, s2):
    ids, derived = [], []
    for group in ("W", "R", "B", "L", "C"):
        for h in hs:
            if h["group"] != group:
                continue
            tid = task_id(h["history"], A_SMALL, "zero")
            (derived if s2[h["history"]]["zero"] else ids).append(tid)
    return ids, derived


def project(ids_path):
    hs = histories(load_tasks())
    runs = run_lines_02b()
    s2 = s2_table(hs)
    p_ids = [task_id(h, A_SMALL, "zero") for h in ("P-2197", "P-2188")]
    need(runs, p_ids)
    print("ruling 5: the projection after P's two at A = 0.05")
    walls = []
    for tid in p_ids:
        line = runs[tid]
        walls.append(line["wall"])
        print(
            f"   {tid:28s} {line_summary(line)} peak_rss={line['peak_rss'] / GiB:.3f} GiB"
        )
    workers, rss = workers_now()
    mean = sum(walls) / len(walls)
    zero_ids, zero_derived = remaining_zero(hs, s2)
    edge_hist = [h["history"] for h in hs if not s2[h["history"]]["edge"]]
    n_zero, n_edge = len(zero_ids), len(edge_hist)
    n = n_zero + n_edge
    projection = n * mean / workers if workers > 0 else float("inf")
    print(
        f"   workers = min({MAX_WORKERS}, floor(8 GiB / {rss / GiB:.3f} GiB)), the largest peak RSS seen so far in "
        f"02b, = {workers}"
    )
    print(
        f"   tasks counted: {n_zero} zero (W, R, B, L less S2's) + {n_edge} edge (every history S2 does not spare "
        f"under edge) = {n}, each at the mean wall of P's two, {mean:.1f} s"
    )
    print(f"   edge histories counted: {edge_hist}")
    print(
        f"   projection = {n} x {mean:.1f} s / {workers} = {projection:.0f} s = {projection / 3600:.2f} h "
        f"(limit {PROJECTION_LIMIT_S / 3600:g} h, worker floor {WORKER_FLOOR})"
    )
    write_ids(
        ids_path,
        zero_ids,
        f"step 3b (zero, A = 0.05, W R B L, less S2's) at {workers} workers",
    )
    print(f"   derived by S2 (equal to 02's A = 1), not run: {zero_derived}")
    with open(str(ids_path) + ".workers", "w") as f:
        f.write(f"{workers}\n")
    if projection > PROJECTION_LIMIT_S or workers < WORKER_FLOOR:
        print("!! STOP: ruling 5's projection or worker floor")
        sys.exit(3)


def step3c(ids_path):
    hs = histories(load_tasks())
    runs = run_lines_02b()
    s2 = s2_table(hs)
    ids = []
    print(
        "S1 at A = 0.05 (the zero tuning at A = 0.05, run or S2's, met no gap candidate)"
    )
    for h in hs:
        z = zero_source(h["history"], A_SMALL, runs, s2)
        s1 = s1_holds(z)
        print(
            f"   {task_id(h['history'], A_SMALL, 'edge'):28s} from {z['id']:28s} S1={s1}"
        )
        if not s1:
            ids.append(task_id(h["history"], A_SMALL, "edge"))
    workers, rss = workers_now()
    write_ids(
        ids_path,
        ids,
        f"step 3c (edge runs at A = 0.05 that S1 does not cover) at {workers} workers "
        f"(ruling 5: largest peak RSS so far {rss / GiB:.3f} GiB)",
    )
    with open(str(ids_path) + ".workers", "w") as f:
        f.write(f"{workers}\n")


def derive():
    tasks = load_tasks()
    hs = histories(tasks)
    runs = run_lines_02b()
    runs02 = run_lines_02()
    lines = wide.all_lines_02b()
    s2 = s2_table(hs)
    by_id = {t["id"]: t for t in tasks}
    new = []

    def source_of(hid, A, gap):
        """(the run line whose values the task carries, how, the task its own shortcut equates it with)."""
        tid = task_id(hid, A, gap)
        if tid in runs:
            return runs[tid], "run", None
        if gap == "zero":
            if s2[hid]["zero"]:
                src = task_id(hid, 1.0, "zero")
                return runs02[src], "S2", src
            raise SystemExit(f"no run and no shortcut for {tid}")
        z, _, _ = source_of(hid, A, "zero")
        if s1_holds(z):
            return z, "S1", task_id(hid, A, "zero")
        raise SystemExit(f"no run and no shortcut for {tid}")

    for h in hs:
        for A in A_02B:
            for gap in ("zero", "edge"):
                tid = task_id(h["history"], A, gap)
                src, how, equals = source_of(h["history"], A, gap)
                if how == "run" or tid in lines:
                    continue
                line = dict(src)
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
                line["equals"] = equals
                line["values_from"] = src["id"]
                line["values_from_sample"] = (
                    "02" if src["id"] in runs02 and src["id"] not in runs else "02b"
                )
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
        print(
            f"   {line['id']:28s} {line['obtained']} = {line['equals']} (values from {line['values_from_sample']}'s "
            f"{line['values_from']})"
        )
    total = wide.all_lines_02b()
    n_02b = sum(1 for t in tasks if t["A"] in A_02B)
    print(f"   02b's sample.jsonl now holds {len(total)} task lines of {n_02b}")


def main():
    cmd = sys.argv[1]
    if cmd == "derive":
        derive()
        return
    fn = {
        "repro": repro,
        "step2a": step2a,
        "step2b": step2b,
        "step3a": step3a,
        "project": project,
        "step3c": step3c,
    }[cmd]
    fn(*sys.argv[2:])


if __name__ == "__main__":
    main()
