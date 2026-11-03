"""
one-loop-drain prompt 02 §1.2, "Spot checks": each shortcut run anyway on two histories, and compared bitwise with what
it replaces (orchestrator note 02, ruling 6), in the tuning's integrations (the line's per-integration values and
digests, and the full regime sequences and injection triples of the partial files), the samples' SHA-256 and the BBN
values. A difference is a stop (exit 3).

  - S1 on P-2197 at A = 1 and on R-865 at A = 0.1: the edge task, run, against the zero task's run. Where S1 does not
    apply (the zero tuning met a gap candidate), the next history of groups P, R and B, in the table's order, to which
    it applies at that A.
  - S2 on C-3016 at A = 0.01 and on C-3233 at A = 0.1: the zero task, run, against the zero task at A = 1. Under
    ruling 1 the zero pass at A = 0.1 is run for every history, so C-3233's S2 check is the step-1 run itself (the
    free check); it is repeated here from those lines.

    decide plan:  spot_check.py plan IDS    writes the spot runs' ids (run with --tag spot-, into spot.jsonl)
    check:        spot_check.py check       compares, and prints each check's result
"""

import sys

from common import (
    SPOT,
    compare_lines,
    compare_partials,
    load_tasks,
    histories,
    run_lines,
    s1_holds,
    s2_holds,
    task_id,
)

S1_WANTED = (("P-2197", 1.0), ("R-865", 0.1))
S2_WANTED = (("C-3016", 0.01), ("C-3233", 0.1))


def s1_choices(runs, hs):
    order = [h["history"] for h in hs if h["group"] in ("P", "R", "B")]
    chosen = []
    for want, A in S1_WANTED:
        cands = [want] + [x for x in order[order.index(want) + 1 :]] + order
        for hid in cands:
            if (hid, A) in chosen:
                continue
            z = runs.get(task_id(hid, A, "zero"))
            if z is not None and s1_holds(z):
                chosen.append((hid, A))
                break
        else:
            raise SystemExit(f"no history of P, R, B to which S1 applies at A = {A}")
    return chosen


def plan(ids_path):
    tasks = load_tasks()
    hs = histories(tasks)
    runs = run_lines()
    ids = []
    for hid, A in s1_choices(runs, hs):
        ids.append(task_id(hid, A, "edge"))
    for hid, A in S2_WANTED:
        z1 = runs[task_id(hid, 1.0, "zero")]
        if not s2_holds(z1, "zero"):
            print(f"!! S2 does not apply to {hid}")
            sys.exit(3)
        tid = task_id(hid, A, "zero")
        if tid not in runs:
            ids.append(tid)
    with open(ids_path, "w") as f:
        for i in ids:
            f.write(i + "\n")
    print(f"spot runs: {ids}")


def check():
    tasks = load_tasks()
    hs = histories(tasks)
    runs = run_lines()
    spots = run_lines(SPOT)
    stop = False
    pairs = []
    for hid, A in s1_choices(runs, hs):
        pairs.append(
            ("S1", task_id(hid, A, "edge"), "spot-", task_id(hid, A, "zero"), "")
        )
    for hid, A in S2_WANTED:
        tid = task_id(hid, A, "zero")
        if tid in spots:
            pairs.append(("S2", tid, "spot-", task_id(hid, 1.0, "zero"), ""))
        else:
            pairs.append(("S2", tid, "", task_id(hid, 1.0, "zero"), ""))
    for how, a_id, a_prefix, b_id, b_prefix in pairs:
        a = (spots if a_prefix else runs)[a_id]
        b = runs[b_id]
        diffs = compare_lines(a, b, how)
        pdiffs, n = compare_partials(a_id, a_prefix, b_id, b_prefix)
        ints = a.get("integrations") or []
        cands = sum(sum((i.get("counts") or {}).values()) for i in ints)
        where = "spot run" if a_prefix else "sample run"
        verdict = "BITWISE EQUAL" if not diffs and not pdiffs else "DIFFERS"
        print(
            f"{how} {a_id} ({where}, outcome {a['outcome']}) against {b_id}: {verdict}; "
            f"{len(ints)} integrations, {cands} candidates, {n} regime and injection entries compared in full; "
            f"samples {a.get('accepted', {}).get('samples_sha256', 'None')[:16]}..., "
            f"Yp {a.get('bbn', {}).get('Yp')!r}, DoH {a.get('bbn', {}).get('DoH')!r}; "
            f"record {how and ('gap_prescription' if how == 'S1' else 'A')}: "
            f"{[i['drain'][('gap_prescription' if how == 'S1' else 'A')] for i in ints if i.get('drain')][:1]} against "
            f"{[i['drain'][('gap_prescription' if how == 'S1' else 'A')] for i in b.get('integrations') or [] if i.get('drain')][:1]}"
        )
        for d in (diffs + pdiffs)[:20]:
            print(f"   {d}")
        if diffs or pdiffs:
            stop = True
    if stop:
        print("!! STOP: a spot check is not bitwise")
        sys.exit(3)
    print("every spot check bitwise equal")


def main():
    if sys.argv[1] == "plan":
        plan(sys.argv[2])
    elif sys.argv[1] == "check":
        check()
    else:
        raise SystemExit("plan IDS | check")


if __name__ == "__main__":
    main()
