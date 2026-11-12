"""
Probe (electroweak-eos prompt 01a §2): what surfing_turning_points records on the scans' histories, from
scan_turning_points.py's records. surfing_turning_points samples g = 1 + beta pi/M_P on each step as
observe_turning_points samples pi, and keeps every sign change, with no tolerance. Printed per run: the
successful histories, the crossings, those above 300 GeV, and for each history with six or more crossings,
how many lie within 1e-9 (relative) of its last crossing's T_J, and the smallest spacing in N between
consecutive crossings.

  PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/turning-points/surfing_survey.py DIR [DIR ...]
"""

import glob
import json
import os
import sys


def main():
    for d in sys.argv[1:]:
        hists = n = above = 0
        rows = []
        for f in sorted(glob.glob(os.path.join(d, "*.json"))):
            h = json.load(open(f))["histories"][0]
            if h.get("failure"):
                continue
            cs = h.get("surfing_turning_points", [])
            hists += 1
            n += len(cs)
            above += sum(1 for c in cs if c["T_J_GeV_dec"] > 300.0)
            if len(cs) >= 6:
                T = [c["T_J_GeV_dec"] for c in cs]
                N = [float.fromhex(c["N"]) for c in cs]
                close = sum(1 for t in T if abs(t / T[-1] - 1.0) < 1e-9)
                dN = min(b - a for a, b in zip(N, N[1:]))
                rows.append(
                    f"  beta={h['beta']:<5} M={h['M']:<6g} crossings {len(cs):>4}, "
                    f"{close:>3} within 1e-9 of the last T_J = {T[-1]:.10g} GeV, "
                    f"N {N[0]:.4f} to {N[-1]:.4f}, smallest spacing {dN:.3g}"
                )
        print(
            f"{d}: {hists} successful histories, {n} crossings, {above} above 300 GeV"
        )
        print("\n".join(rows))


if __name__ == "__main__":
    main()
