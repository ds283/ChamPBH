"""
one-loop-drain prompt 01, after the user's ruling (b) (2026-11-03): §2.1's batches and §2.2's runs on the final
tree (var/one-loop-drain/01/final/) against the first run, made on the tree before the ruling
(var/one-loop-drain/01/). Each task or run that succeeded in the first run must be equal in the second: for the
scanner's records, the outcome, both SHA-256 digests, n_samples, accepted_steps, RHS_evaluations, reflections
and restart_retakes (and a failure's reason); for the one-loop runs, every candidate's (N, alpha_eff, delta_K),
the counts, the parking N, r_chi and Delta N_eff today. A run that failed in the first run is reported beside
its second.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law/compare_final.py > .documents/one-loop-drain/law/out/compare_final.txt
"""

import json
import sys
from pathlib import Path

FIRST = Path("/Users/ds283/Documents/Code/ChamPBH/var/one-loop-drain/01")
FINAL = FIRST / "final"
FIELDS = (
    "outcome",
    "samples_sha256",
    "log_one_plus_z_sha256",
    "n_samples",
    "accepted_steps",
    "RHS_evaluations",
    "reflections",
    "restart_retakes",
    "failure_reason",
)

stops = []


def jsonl(path):
    return {json.loads(l)["key"]: json.loads(l) for l in path.read_text().splitlines()}


def main():
    for name in ("the-42.jsonl", "pair.jsonl", "the-nine.jsonl"):
        a, b = jsonl(FIRST / name), jsonl(FINAL / name)
        differing = []
        for key in a:
            if a[key].get("outcome") != "success":
                print(
                    f"{name}: {key} failed in the first run; now {b.get(key, {}).get('outcome')}"
                )
                continue
            d = [f for f in FIELDS if a[key].get(f) != b.get(key, {}).get(f)]
            if d:
                differing.append((key, d))
        print(
            f"{name}: {len(a)} tasks first, {len(b)} now; same keys {set(a) == set(b)}; of those that "
            f"succeeded first, differing: {len(differing)}"
            + ("" if not differing else " " + repr(differing))
        )
        if differing or set(a) != set(b):
            stops.append(name)
    print()
    runs = sorted(p.name for p in (FIRST / "oneloop").glob("*-A*-*-*.json"))
    for name in runs:
        a = json.loads((FIRST / "oneloop" / name).read_text())
        p = FINAL / "oneloop" / name
        if not p.exists():
            print(f"oneloop/{name}: missing in the final run")
            stops.append(name)
            continue
        b = json.loads(p.read_text())
        if a["failure"]:
            print(
                f"oneloop/{name}: failed first ({a['failure_reason'][:90]}...); now "
                f"{'failure: ' + b['failure_reason'][:90] if b['failure'] else 'success'}, unresolved_declined "
                f"{b['drain'].get('unresolved_declined')}"
            )
            continue
        same = [(c["N"], c["alpha_eff"], c["delta_K"]) for c in a["cands"]] == [
            (c["N"], c["alpha_eff"], c["delta_K"]) for c in b.get("cands", [])
        ] and all(
            a[k] == b.get(k)
            for k in (
                "candidates",
                "drains",
                "reflections",
                "accepted_steps",
                "parking_N",
                "N_final",
                "dark_radiation_values",
            )
        )
        print(
            f"oneloop/{name}: equal to the first run: {same}; unresolved_declined {b['drain'].get('unresolved_declined')}"
        )
        if not same:
            stops.append(name)
    print()
    print(f"stops: {stops if stops else 'none'}")
    return 1 if stops else 0


if __name__ == "__main__":
    sys.exit(main())
