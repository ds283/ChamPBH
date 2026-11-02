"""
reflection-restart prompt 04 §1.2 (d): the nine of prompt §0.2 recomputed tuned on this branch's tree
(var/reflection-restart/04/the-nine.jsonl, by the scanner's batch). For each failure, the failing call's phi, ln E
and normalization from the record's printed message (stdout_tail; the payload's failure_reason is cut at 256
characters, as the store's is), the call made alone on a model built as the scanner builds it, whether it raises
with the same message, and the root's margin in quanta (failing_call.py's definition), set beside the stored
failure's phi, ln E and margin. Reads only. Run from the repository root:

    PYTHONPATH=. ./venv/bin/python .documents/reflection-restart/close/nine_margins.py
"""

import json
import math
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import check_tree  # noqa: E402

D = Path("var/reflection-restart/04")
PATTERN = re.compile(
    r"normalization = (?P<norm>[-0-9.e+]+), phi = (?P<phi>[-0-9.e+]+), log_E = (?P<logE>[-0-9.e+]+)\)"
)


def margin(log_E, log_V_floor):
    d = math.log1p(math.exp(log_E - log_V_floor))
    return d / math.ulp(max(abs(log_V_floor + d), abs(log_V_floor)))


def main():
    SM, scan, head = check_tree()
    import importlib

    AM = importlib.import_module("CosmologyConcepts.Models.AbstractModel")
    from CosmologyConcepts import M_value, beta_value
    from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
    from Units import Planck_units

    units = Planck_units()
    K = AM.WALL_ROUNDING_QUANTA
    tasks = json.loads((D / "tasks-nine.json").read_text())
    recs = {}
    for line in (D / "the-nine.jsonl").read_text().splitlines():
        if line.strip():
            r = json.loads(line)
            recs[r["key"]] = r

    stored_reasons = {}
    for t in tasks:
        shard, serial = t["key"].split()[0].split("#")
        conn = scan.connect_ro(
            f"/Users/ds283/Documents/Code/ChamPBH/var/datastores/science-2026.8.0-{shard}.db"
        )
        stored_reasons[t["key"]] = conn.execute(
            "select failure_reason from ScalarHistory where serial = ?", (int(serial),)
        ).fetchone()["failure_reason"]
        conn.close()

    print(f"WALL_ROUNDING_QUANTA = {K}")
    print()
    print(
        "| store | beta | M/M_P | alpha | outcome on this tree | phi/M_P (this tree) | ln E (this tree) | margin (quanta) | "
        "alone: raises, same message | stored phi/M_P | stored ln E | stored margin |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    worst = 0.0
    for t in tasks:
        r = recs[t["key"]]
        s = PATTERN.search(stored_reasons[t["key"]])
        s_phi, s_logE, s_norm = float(s["phi"]), float(s["logE"]), float(s["norm"])
        s_m = margin(s_logE, s_norm)
        store = t["key"].split()[0]
        if r.get("outcome") != "failure":
            tu = r.get("tuning", {})
            print(
                f"| {store} | {t['beta']:g} | {t['M']:g} | {t['alpha']:g} | {r.get('outcome')} "
                f"({len(r.get('integrations', []))} integrations; normalization {tu.get('normalization')!r}) | — | — | — | — | "
                f"{s_phi:.6g} | {s_logE!r} | {s_m:.4f} |"
            )
            continue
        full = [
            x.strip()
            for x in r["stdout_tail"]
            if x.strip().startswith("classical_turning_point")
        ]
        message = full[0] if full else r["failure_reason"]
        m = PATTERN.search(message)
        norm, phi, log_E = float(m["norm"]), float(m["phi"]), float(m["logE"])
        model = ExponentialModel(
            0, M_value(0, t["M"] * units.PlanckMass), 1, beta_value(0, t["beta"]), units
        )
        try:
            model.classical_turning_point(norm, phi, log_E)
            alone, same = False, False
        except Exception as e:
            alone, same = True, getattr(e, "message", str(e)) == message
        q = margin(log_E, model._log_V_floor(norm))
        worst = max(worst, q)
        print(
            f"| {store} | {t['beta']:g} | {t['M']:g} | {t['alpha']:g} | failure (seed integration; normalization "
            f"{norm!r}) | {phi / units.PlanckMass:.6g} | {log_E!r} | {q:.4f} | {alone}, {same} | {s_phi:.6g} | "
            f"{s_logE!r} | {s_m:.4f} |"
        )
    print()
    print(
        f"largest margin of a failure on this tree: {worst:.4f} quanta; within K + 1 = {K + 1}: {worst <= K + 1}"
    )
    print()
    print("the failures' messages, whole (from each record's stdout_tail):")
    for t in tasks:
        r = recs[t["key"]]
        if r.get("outcome") == "failure":
            full = [
                x.strip()
                for x in r["stdout_tail"]
                if x.strip().startswith("classical_turning_point")
            ]
            print(f"- {t['key']}: {full[0] if full else r['failure_reason']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
