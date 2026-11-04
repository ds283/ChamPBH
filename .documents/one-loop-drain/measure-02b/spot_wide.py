"""
one-loop-drain prompt 02b §1.2, "Spot checks": each shortcut run anyway on two cases and compared bitwise with what it
replaces (orchestrator note 02 ruling 6, kept by note 02b ruling 7): in the line (02's compare_lines, with the record's
gap_prescription (S1) or A (S2) allowed to differ) and in the partial files, per integration, every value but the wall
and maxrss (the full regime sequences and injection triples among them). A difference is a stop (exit 3).

  - S1: the first history of groups P, R and B, in prompt 02's table order, to which S1 applies at A = 10, and the first
    to which it applies at A = 0.05; if none applies at one of these A, the next A in the order 5, 2, 0.5. The edge task
    is run and compared with the zero task at that A (02b's run, or under S2 02's zero run at A = 1).
  - S2: C-3016 at A = 10 and C-3233 at A = 0.05: the zero task is run and compared with 02's zero run at A = 1 (the
    fields 02b adds are left out of that comparison, ruling 4).
  The tool check (note 02b ruling 8; tool_wide.py) is planned in the same batch, as `tool:P-2197-A10-zero`.

    spot_wide.py plan IDS     writes the spot batch's ids (run with --tag spot-, into var/one-loop-drain/02b/spot.jsonl)
    spot_wide.py check        compares, and prints each check's result
"""

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import wide  # noqa: E402
from wide import histories, load_tasks, s1_holds, s2_holds, task_id  # noqa: E402

S1_A = ((10.0, (5.0, 2.0, 0.5)), (0.05, (5.0, 2.0, 0.5)))
S2_WANTED = (("C-3016", 10.0), ("C-3233", 0.05))
TOOL_CHECK = "P-2197-A10-zero"
PREFIX = "spot-"


def zero_source(hid, A, runs, runs02):
    """The run line a zero task carries: its own run, or 02's zero run at A = 1 where S2 holds."""
    z1 = runs02[task_id(hid, 1.0, "zero")]
    if s2_holds(z1, "zero"):
        return z1, "02"
    return runs[task_id(hid, A, "zero")], "02b"


def s1_choices(runs, runs02, hs):
    order = [h["history"] for h in hs if h["group"] in ("P", "R", "B")]
    chosen = []
    for A0, fallback in S1_A:
        for A in (A0,) + fallback:
            hit = None
            for hid in order:
                if (hid, A) in chosen:
                    continue
                z, _ = zero_source(hid, A, runs, runs02)
                if s1_holds(z):
                    hit = (hid, A)
                    break
            if hit is not None:
                chosen.append(hit)
                break
        else:
            raise SystemExit(
                f"no history of P, R, B to which S1 applies at A = {A0} or its fallbacks"
            )
    return chosen


def plan(ids_path):
    hs = histories(load_tasks())
    runs, runs02 = wide.run_lines_02b(), wide.run_lines_02()
    ids = [task_id(hid, A, "edge") for hid, A in s1_choices(runs, runs02, hs)]
    for hid, A in S2_WANTED:
        z1 = runs02[task_id(hid, 1.0, "zero")]
        if not s2_holds(z1, "zero"):
            print(f"!! S2 does not apply to {hid}")
            sys.exit(3)
        tid = task_id(hid, A, "zero")
        if tid in runs:
            print(f"!! {tid} was run in the sample; S2 was not used for it")
            sys.exit(3)
        ids.append(tid)
    ids.append(f"tool:{TOOL_CHECK}")
    with open(ids_path, "w") as f:
        for i in ids:
            f.write(i + "\n")
    print(f"spot runs and the tool check: {ids}")


def check():
    hs = histories(load_tasks())
    runs, runs02 = wide.run_lines_02b(), wide.run_lines_02()
    spots = wide.run_lines_02b(wide.SPOT)
    stop = False
    pairs = []
    for hid, A in s1_choices(runs, runs02, hs):
        z, where = zero_source(hid, A, runs, runs02)
        pairs.append(("S1", task_id(hid, A, "edge"), z, where))
    for hid, A in S2_WANTED:
        pairs.append(
            ("S2", task_id(hid, A, "zero"), runs02[task_id(hid, 1.0, "zero")], "02")
        )
    for how, a_id, b, where in pairs:
        a = spots[a_id]
        diffs = wide.compare_lines_across(a, b, how)
        b_dir = wide.TASK_DIR_02 if where == "02" else wide.TASK_DIR
        pdiffs, n = wide.compare_partial_files(
            wide.TASK_DIR / f"{PREFIX}{a_id}.partial.jsonl",
            b_dir / f"{b['id']}.partial.jsonl",
            shortcut=how,
        )
        ints = a.get("integrations") or []
        cands = sum(sum((i.get("counts") or {}).values()) for i in ints)
        field = "gap_prescription" if how == "S1" else "A"
        verdict = "BITWISE EQUAL" if not diffs and not pdiffs else "DIFFERS"
        print(
            f"{how} {a_id} (spot run, outcome {a['outcome']}) against {where}'s {b['id']}: {verdict}; "
            f"{len(ints)} integrations, {cands} candidates, {n} regime and injection entries compared in full; "
            f"samples {str((a.get('accepted') or {}).get('samples_sha256'))[:16]}..., "
            f"Yp {(a.get('bbn') or {}).get('Yp')!r}, DoH {(a.get('bbn') or {}).get('DoH')!r}; record {field}: "
            f"{[i['drain'][field] for i in ints if i.get('drain')][:1]} against "
            f"{[i['drain'][field] for i in b.get('integrations') or [] if i.get('drain')][:1]}"
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
