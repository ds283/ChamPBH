"""
bbn-tolerance prompt 01: the brief's runs with a patched PRyM/ (rtol 1e-6 on
the full low-T call; source/lt_failure_diagnostics.csv) against the scan's
S1 rows at rtol 1e-6 made by interception (scan.csv), Yp and D/H to the
brief's printed digits.

    ./venv/bin/python prompts/bbn-tolerance/logs/01-probes/compare_brief_rtol1e-6.py
"""

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent
brief = [
    r
    for r in csv.DictReader(
        open(HERE.parents[1] / "source" / "lt_failure_diagnostics.csv")
    )
    if "1e-6" in r["setting"]
]
scan = [
    r
    for r in csv.DictReader(open(HERE / "scan.csv"))
    if r["tag"] == "S1" and r["lowT_rtol"] == "1e-06" and r["input"] != "SM"
]
idx = {(float(r["beta"]), float(r["M_Mp"]), r["variant"]): r for r in scan}
n = 0
for b in brief:
    s = idx[(float(b["beta"]), float(b["M_Mp"]), b["variant"])]
    ok = (
        f"{float(s['Yp']):.10g}" == b["Yp"]
        and f"{float(s['DOverH']):.10g}" == b["DH_x1e5"]
    )
    n += ok
    if not ok:
        print("DIFFER", dict(b), s["Yp"], s["DOverH"])
print(
    f"{n} of {len(brief)} of the brief's rtol-1e-6 rows match the override to every printed digit"
)
