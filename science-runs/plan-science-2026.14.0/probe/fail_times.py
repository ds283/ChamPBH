"""The probe's per-history times by beta, failed and succeeded, from histories.py's JSON lines.

Reads the JSON lines of .documents/power-law-wall/rehearse-07d/histories.py's listing (out/p2-histories-pl.txt) and
writes stdout. The time of a history is histories.py's tuning wall: from its submission to its storage, so it
includes queueing. Run from the repository root:

    ./venv/bin/python science-runs/plan-science-2026.14.0/probe/fail_times.py <p2-histories-pl.txt>
"""

import json
import re
import statistics as st
import sys
from collections import defaultdict


def val(v):
    return float(v["repr"]) if isinstance(v, dict) and "repr" in v else v


H = []
for line in open(sys.argv[1]):
    if line.startswith("JSON "):
        H.append({k: val(v) for k, v in json.loads(line[5:]).items()})

g = defaultdict(list)
for h in H:
    g[(h["beta"], bool(h["failure"]))].append(h["tuning_wall_s"])
print(
    f"{'beta':>6} {'state':>5} {'n':>4} {'median s':>9} {'mean s':>8} {'max s':>8} {'sum h':>6}"
)
for (b, f), v in sorted(g.items()):
    v = [x for x in v if x is not None]
    if v:
        print(
            f"{b:6g} {'FAIL' if f else 'ok':>5} {len(v):4} {st.median(v):9.1f} {st.mean(v):8.1f} {max(v):8.1f} {sum(v) / 3600:6.2f}"
        )

print("\nbeta = 20 failures, one line each:")
for h in sorted(
    (h for h in H if h["beta"] == 20.0 and h["failure"]),
    key=lambda h: (h["M_Mp"], h["n"], h["drain_model"], h["alpha"]),
):
    r = re.sub(r"\(.*?\): ", "", h["failure_reason"] or "", count=1)[:70]
    print(
        f"  M {h['M_Mp']:8.3g} n {h['n']} {h['drain_model'][:9]:9} {h['alpha']:5g}  {h['tuning_wall_s']:7.1f} s  {r}"
    )
ok20 = [h for h in H if h["beta"] == 20.0 and not h["failure"]]
print(
    f"\nbeta = 20 successes: {len(ok20)}: "
    + ", ".join(
        f"M {h['M_Mp']:.3g} n {h['n']} {h['drain_model'][:9]} {h['alpha']:g}"
        for h in ok20
    )
)
