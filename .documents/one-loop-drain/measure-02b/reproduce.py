"""
one-loop-drain prompt 02b §1.2, the reproduction check (orchestrator note 02b, ruling 4). 02's P-2197-A1-zero and
R-865-A0.1-zero were re-run by 02b's driver (run_wide.py, one batch at two workers, --tag repro-, lines in
var/one-loop-drain/02b/repro.jsonl). Each is compared with 02's:
  - the line, under note 02's ruling 6 (02's compare_lines), with nothing allowed to differ: the comparison is made
    under both of its shortcut modes and the differences of either are reported, so that neither the record's
    gap_prescription nor its A is excused. Not compared, as ruling 4 says: the wall times, started/ended,
    peak_rss/maxrss, the tag and the task file names (02's NOT_COMPARED and INTEGRATION_NOT_COMPARED, and
    BBN_NOT_COMPARED for PRyMordial's time and the stage's wall), and the fields 02b adds that 02's lines lack
    (accepted.first_impact; each integration's cap_one_loop_by_ell and cap_gap_edge);
  - the partial file, per integration: every value of the partial line by repr, the full regime sequence and the
    injection triples (N, alpha_eff, delta_K) among them, but its wall and maxrss, and 02b's "ells" and cap counts;
  - in addition, for information, the task's stdout against 02's, line by line with `wall=` and `PRyM_time=` masked
    (note 02's ruling 9's mask).
A difference in the line or the partial file is a stop (exit 3).

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure-02b/reproduce.py \\
        > .documents/one-loop-drain/measure-02b/out/reproduce.txt
"""

import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import wide  # noqa: E402

CHECKS = ("P-2197-A1-zero", "R-865-A0.1-zero")
PREFIX = "repro-"
MASK = re.compile(r"\b(wall|PRyM_time)=[^ ]+")


def masked(text):
    return [MASK.sub(r"\1=*", x) for x in text.splitlines()]


def main():
    mine = wide.run_lines_02b(wide.REPRO)
    theirs = wide.run_lines_02()
    stop = False
    for tid in CHECKS:
        a, b = mine[tid], theirs[tid]
        diffs = sorted(
            set(wide.compare_lines_across(a, b, "S1"))
            | set(wide.compare_lines_across(a, b, "S2"))
        )
        pdiffs, n = wide.compare_partial_files(
            wide.TASK_DIR / f"{PREFIX}{tid}.partial.jsonl",
            wide.TASK_DIR_02 / f"{tid}.partial.jsonl",
        )
        ints = a.get("integrations") or []
        cands = sum(sum((i.get("counts") or {}).values()) for i in ints)
        verdict = "BITWISE EQUAL" if not diffs and not pdiffs else "DIFFERS"
        acc = a.get("accepted") or {}
        print(
            f"{tid}: 02b's run (outcome {a['outcome']}, wall {a['wall']:.1f} s) against 02's (outcome {b['outcome']}, "
            f"wall {b['wall']:.1f} s): {verdict}"
        )
        print(
            f"   {len(ints)} integration(s), {cands} candidates; {n} regime and injection entries of the partial files "
            f"compared in full; samples SHA-256 {acc.get('samples_sha256')} (02: "
            f"{(b.get('accepted') or {}).get('samples_sha256')}); Yp {a.get('bbn', {}).get('Yp')!r} (02: "
            f"{b.get('bbn', {}).get('Yp')!r}); DoH {a.get('bbn', {}).get('DoH')!r} (02: {b.get('bbn', {}).get('DoH')!r})"
        )
        for d in (diffs + pdiffs)[:30]:
            print(f"   !! {d}")
        if diffs or pdiffs:
            stop = True
        out_a = masked((wide.TASK_DIR / f"{PREFIX}{tid}.log").read_text())
        out_b = masked((wide.TASK_DIR_02 / f"{tid}.log").read_text())
        sd = [j for j, (x, y) in enumerate(zip(out_a, out_b)) if x != y]
        print(
            f"   stdout (for information): {len(out_a)} and {len(out_b)} lines, "
            f"{len(out_a) - len(sd) if len(out_a) == len(out_b) else 'n/a'} byte-equal after masking"
        )
        for j in sd[:5]:
            print(f"   [{j}] 02b: {out_a[j]}\n   [{j}]  02: {out_b[j]}")
    if stop:
        print("!! STOP: the reproduction check is not bitwise")
        sys.exit(3)
    print(
        "the reproduction check holds: both tasks bitwise equal to 02's, line and partial file"
    )


if __name__ == "__main__":
    main()
