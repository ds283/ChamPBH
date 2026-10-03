"""
bbn-tolerance prompt 01, section 4: compare reproduce.csv (the tool, prod,
default tolerance) with the stored BBNData rows, read read-only through the
tool's find_model.

    ./venv/bin/python prompts/bbn-tolerance/logs/01-probes/compare_reproduction.py [CSV]

For a stored success: Yp and D/H as printed (.10g) and bitwise (repr).
For a stored failure: the stage and the 't reached X of target Y' (%.6g) of the
stored reason against the tool's.
"""

import csv
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from tools.bbn_from_store import find_model  # noqa: E402

STORE = os.path.expanduser("~/ChamPBH-stores/science-2026.6.0")
HERE = Path(__file__).resolve().parent
REASON = re.compile(r"stage '([^']*)'.*t reached ([0-9.e+-]+) of target ([0-9.e+-]+)")


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else HERE / "reproduce.csv"
    rows = [r for r in csv.DictReader(open(path)) if r["variant"] == "prod"]
    n_ok = 0
    for r in rows:
        beta, M = float(r["beta"]), float(r["M_Mp"])
        h = find_model(STORE, beta, M)
        s = h.bbn
        if not s["failure"]:
            pY, pD = f"{s['Yp_BBN']:.10g}", f"{s['DOverH']:.10g}"
            tY, tD = f"{float(r['Yp']):.10g}", f"{float(r['DOverH']):.10g}"
            bit = float(r["Yp"]) == s["Yp_BBN"] and float(r["DOverH"]) == s["DOverH"]
            ok = r["status"] == "ok" and pY == tY and pD == tD
            print(
                f"control beta={beta:g} M={M:g}: stored Yp={pY} DoH={pD}; tool Yp={tY} DoH={tD}; "
                f"printed {'MATCH' if ok else 'DIFFER'}, bitwise {'yes' if bit else 'no'}"
            )
        else:
            m = REASON.search(s["failure_reason"])
            st_stage, st_t, st_T = m.group(1), m.group(2), m.group(3)
            if r["status"] == "FAILURE":
                tt = f"{float(r['t_reached']):.6g}"
                tT = f"{float(r['t_target']):.6g}"
                m2 = REASON.search(r["failure_reason"])
                ok = (
                    r["failure_stage"] == st_stage
                    and m2.group(2) == st_t
                    and m2.group(3) == st_T
                    and tt == st_t
                )
                print(
                    f"failure beta={beta:g} M={M:g}: stored {st_stage!r} t={st_t}/{st_T}; "
                    f"tool {r['failure_stage']!r} t={tt}/{tT} (full {r['t_reached']}); "
                    f"{'MATCH' if ok else 'DIFFER'}; reason identical: "
                    f"{r['failure_reason'] == s['failure_reason']}"
                )
            else:
                ok = False
                print(f"failure beta={beta:g} M={M:g}: stored failure, tool ok: DIFFER")
        n_ok += ok
    print(f"{n_ok} of {len(rows)} reproduce")


if __name__ == "__main__":
    main()
