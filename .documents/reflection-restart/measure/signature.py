"""
The Signature gate of reflection-restart prompt 01 (§1.3, "Signature first"; orchestrator note 01, ruling 1):
the scanner's run of the diagnosis's nine histories under the production code
(var/reflection-restart/01/signature.jsonl, from tasks.py signature, scanned with --rows first) against
the diagnosis's .documents/post-reflection-step-2026-11-02/out/scan_first_steps.txt.

For each history, the diagnosis's lines are re-made from the scanner's rows in the diagnosis's own
accounting (every SciPy Radau instance of the process; a "first step" is the step made by the instance's
first call of step(), counted if it begins with pi > 0; flagged if pi_end/pi_start < 0.5) and printed
beside the diagnosis's, with the printed digits compared as text: the outcome line (the reason, or RHS,
accepted steps, reflections, samples, integrations), the counts, the good steps' statistics and each
flagged step's numbers. The scanner's own count (the first accepted step of each instance) is printed
beside it, and any difference is listed step by step.

    PYTHONPATH=. ./venv/bin/python .documents/reflection-restart/measure/signature.py \\
        > .documents/reflection-restart/out/signature.txt

Reads only; needs no Ray and no store.
"""

import json
import re
from pathlib import Path

import numpy as np

DIAG = Path(".documents/post-reflection-step-2026-11-02/out/scan_first_steps.txt")
SCAN = Path("var/reflection-restart/01/signature.jsonl")


def diagnosis():
    blocks, current = {}, None
    for line in DIAG.read_text().splitlines():
        m = re.match(r"beta=(\S+) M=(\S+) alpha=(\S+): (.*)$", line)
        if m:
            current = (float(m.group(1)), float(m.group(2)), float(m.group(3)))
            blocks[current] = {"outcome": m.group(4), "lines": []}
        elif current is not None:
            blocks[current]["lines"].append(line.strip())
    return blocks


def remake(rec):
    """The diagnosis's lines, from the scanner's record."""
    beta, M = rec["spec"]["beta"], rec["spec"]["M"]
    rows = [r for r in rec["rows"] if r["first_call"]]
    if rec["outcome"] == "failure":
        outcome = f"history beta={beta:g} M={M:g}: FAILURE"
        reason = rec["failure_reason"]
    else:
        n_int = len(rec["integrations"])
        outcome = (
            f"history beta={beta:g} M={M:g}: RHS={rec['RHS_evaluations']} "
            f"accepted_steps={rec['accepted_steps']} reflections={rec['reflections']} "
            f"samples={rec['n_samples']} integrations={n_int} alpha={rec['spec']['alpha']:g}"
        )
        reason = None
    R = np.array(
        [
            (
                r["N0"],
                r["h"],
                r["h_omega"],
                r["pi_ratio"],
                r["phi_start_over_M"],
                r["phi_min_over_M"],
                r["cap"],
                r["h_abs_in"],
                r["a_in"],
            )
            for r in rows
        ]
    )
    bad = R[R[:, 3] < 0.5]
    good = R[R[:, 3] >= 0.5]
    lines = [
        f"first outward steps: {len(R)}; flagged (pi_end/pi_start < 0.5): {len(bad)}; "
        f"dense phi < 0: {int((R[:, 5] < 0).sum())}",
        f"good steps: h max {good[:, 1].max():.3g}, h*omega median {np.median(good[:, 2]):.3g} "
        f"max {good[:, 2].max():.3g}, least pi_end/pi_start {good[:, 3].min():.6g}",
        f"good steps: cap median {np.median(good[:, 6]):.3g} max {good[:, 6].max():.3g}; "
        f"first h_abs median {np.median(good[:, 7]):.3g} max {good[:, 7].max():.3g}; "
        f"inward acceleration > 0 at {int((good[:, 8] > 0).sum())} of {len(good)}",
    ]
    for r in bad:
        lines.append(
            f"flagged: N0={r[0]:.10g} h={r[1]:.3g} h*omega={r[2]:.3g} pi_end/pi_start={r[3]:.3g} "
            f"phi_start/M={r[4]:.3g} least dense phi/M={r[5]:.3g} cap={r[6]:.3g} "
            f"first h_abs={r[7]:.3g} inward acceleration={r[8]:.3g}"
        )
    return outcome, reason, lines, rows


def main():
    diag = diagnosis()
    records = {}
    for line in SCAN.read_text().splitlines():
        rec = json.loads(line)
        s = rec["spec"]
        records[(s["beta"], s["M"], s["alpha"])] = rec

    total_diag = total_call = total_first = 0
    all_match = True
    for key, d in diag.items():
        rec = records[key]
        outcome, reason, lines, rows = remake(rec)
        n_diag = int(re.search(r"first outward steps: (\d+)", d["lines"][0]).group(1))
        n_call = len(rows)
        n_first = rec["scan"]["first_outward_steps"]
        total_diag += n_diag
        total_call += n_call
        total_first += n_first
        print(f"== beta={key[0]:g} M={key[1]:g} alpha={key[2]:g}")
        d_out = d["outcome"]
        if reason is not None:
            m = re.search(r"reason=(.*)$", d_out)
            ok_out = m is not None and reason.startswith(m.group(1))
            print(f"   outcome   diagnosis: {d_out}")
            print(f"             scanner:   FAILURE reason={reason[:110]}")
        else:
            ok_out = d_out.split(" wall=")[0].startswith(
                outcome.split(" alpha=")[0]
            ) and (outcome.split(" alpha=")[0] in d_out)
            print(f"   outcome   diagnosis: {d_out}")
            print(f"             scanner:   {outcome}")
        print(f"   outcome matches: {ok_out}")
        for dl, sl in zip(d["lines"], lines):
            same = dl == sl
            all_match = all_match and same
            print(f"   diagnosis: {dl}")
            print(f"   scanner:   {sl}   [{'same' if same else 'DIFFERS'}]")
        if len(d["lines"]) != len(lines):
            all_match = False
            print(f"   line counts differ: {len(d['lines'])} against {len(lines)}")
        all_match = all_match and ok_out
        print(
            f"   counts: diagnosis {n_diag}; scanner, diagnosis's accounting (first call) {n_call}; "
            f"scanner's own (first accepted step of an instance) {n_first}; integrations "
            f"{len(rec['integrations'])}; Radau instances {rec['scan']['instances']} "
            f"{rec['scan']['instance_reasons']}"
        )
        if n_first != n_call:
            extra = [r for r in rec["rows"] if r["first"] != r["first_call"]]
            for r in extra:
                print(
                    f"     difference: instance {r['instance']} ({r['reason']}) N0={r['N0']!r}: "
                    f"first accepted {r['first']}, first call {r['first_call']}"
                )
    print()
    print(
        f"total first outward steps: diagnosis {total_diag} (its README says 845); scanner in the "
        f"diagnosis's accounting {total_call}; scanner's own {total_first}"
    )
    print(f"every printed line and outcome the same: {all_match}")


if __name__ == "__main__":
    main()
