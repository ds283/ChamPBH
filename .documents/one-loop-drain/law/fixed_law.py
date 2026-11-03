"""
one-loop-drain prompt 01 §2.1.4-§2.1.6, the fixed law (orchestrator note 01, ruling 2), from the scanner's
records and predicate_cost.py's:

  §2.1.4  2197 and 2188 at their stored normalization, alpha = 10^-3 (var/one-loop-drain/01/pair.jsonl): the
          outcome, wall time, reflections, the parking N, r_chi and Delta N_eff today, beside the store's
          (read-only), as a measurement; unresolvable_assessments from predicate_cost.py's run of the same task
          (var/one-loop-drain/01/cost/*.json), whose samples' digest must equal the scanner's;
  §2.1.5  the nine of reflection-restart 04 §0.2, tuned (var/one-loop-drain/01/the-nine.jsonl): each outcome and,
          for a failure, its reason, beside 04's on the fixed tree (var/reflection-restart/04/the-nine.jsonl);
  §2.1.6  the predicate's cost: its share of 2197's (and 2188's) wall time (predicate_cost.py).

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law/fixed_law.py > .documents/one-loop-drain/law/out/fixed_law.txt
"""

import json
import sqlite3
import os
import sys
from pathlib import Path

HERE = Path(
    os.environ.get(
        "ONE_LOOP_DRAIN_01_HERE",
        "/Users/ds283/Documents/Code/ChamPBH/var/one-loop-drain/01",
    )
)
# predicate_cost.py's runs: measured once, on the tree before the user's ruling (b), whose change does not touch
# the predicate (var/one-loop-drain/01/cost/)
COST = Path(os.environ.get("ONE_LOOP_DRAIN_01_COST", str(HERE / "cost")))
RR04 = Path("/Users/ds283/Documents/Code/ChamPBH/var/reflection-restart/04")
WALL_SEARCH = ("classical_turning_point", "not monotone", "outer end is still -inf")

stops = []


def jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def stored(shard, serial):
    c = sqlite3.connect(f"file:{shard}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    r = c.execute(
        """select compute_time, parking_N, rho_chi_ratio_today, Delta_N_eff_today, injections, extra_data
           from ScalarHistory where serial = ?""",
        (serial,),
    ).fetchone()
    c.close()
    extra = json.loads(r["extra_data"]) if r["extra_data"] else {}
    return {
        "compute_time": r["compute_time"],
        "reflections": extra.get("number_reflections", 0),
        "accepted_steps": extra.get("accepted_steps"),
        "injections": r["injections"],
        "parking_N": r["parking_N"],
        "rho_chi_ratio_today": r["rho_chi_ratio_today"],
        "Delta_N_eff_today": r["Delta_N_eff_today"],
    }


def main():
    print(
        "§2.1.4 2197 and 2188 at their stored normalization, alpha = 10^-3, the fixed law"
    )
    print()
    print(
        "| history | source | outcome | wall (s) | unresolvable_assessments | reflections | accepted_steps | candidates | parking N | r_chi today | Delta N_eff today |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for rec in sorted(
        jsonl(HERE / "pair.jsonl"),
        key=lambda d: d["spec"]["store"]["serial"],
        reverse=True,
    ):
        spec = rec["spec"]
        serial = spec["store"]["serial"]
        cost_path = COST / f"{serial}.json"
        cost = json.loads(cost_path.read_text()) if cost_path.exists() else None
        if rec["outcome"] != "success":
            print(
                f"| {serial} | this tree | {rec['outcome']} | {rec['wall']:.1f} | | | | | | | |"
            )
            print(f"   reason: {rec.get('failure_reason')}")
            if any(w in (rec.get("failure_reason") or "") for w in WALL_SEARCH):
                stops.append(f"{serial}: a wall-search failure")
            continue
        d = rec["dissipation"]
        ua = (
            None
            if cost is None or cost["drain"] is None
            else cost["drain"]["unresolvable_assessments"]
        )
        print(
            f"| {serial} | this tree (scanner) | success | {rec['wall']:.1f} | {ua} | {rec['reflections']} | "
            f"{rec['accepted_steps']} | {d['injections']} | {d['parking_N']!r} | {d['rho_chi_ratio_today']!r} | "
            f"{d['Delta_N_eff_today']!r} |"
        )
        s = stored(spec["store"]["shard"], serial)
        print(
            f"| {serial} | the 2026.8.0 store | success | {s['compute_time']:.1f} (compute_time) | | {s['reflections']} | "
            f"{s['accepted_steps']} | {s['injections']} | {s['parking_N']!r} | {s['rho_chi_ratio_today']!r} | "
            f"{s['Delta_N_eff_today']!r} |"
        )
        if cost is not None:
            same = cost.get("samples_sha256") == rec["samples_sha256"]
            print(
                f"   predicate_cost.py's run of the same task: wall {cost['wall_s']:.1f} s, samples digest equal to "
                f"the scanner's: {same}; drain record {cost['drain']}"
            )
            if not same:
                stops.append(f"{serial}: the cost run is not the scanner's run")
        comparison = rec.get("comparison", {})
        print(
            f"   against the store: bitwise {comparison.get('bitwise')} (F3 moved these histories; not expected)"
        )
    print()

    print(
        "§2.1.5 the nine of reflection-restart 04 §0.2, tuned, the fixed law, --timeout 3600"
    )
    print()
    old = {d["key"]: d for d in jsonl(RR04 / "the-nine.jsonl")}
    path = HERE / "the-nine.jsonl"
    new = {d["key"]: d for d in jsonl(path)} if path.exists() else {}
    print(
        "| task | here | integrations | wall (s) | reason, or parking N and r_chi today | reflection-restart 04 (fixed tree) |"
    )
    print("|---|---|---|---|---|---|")
    for key in old:
        d = new.get(key)
        o = old[key]
        if d is None:
            print(f"| {key} | (missing) | | | | {o['outcome']} |")
            continue
        integrations = len(d.get("integrations", [])) or d.get("integration_reached")
        if d["outcome"] == "timeout":
            what = (
                f"timeout: integration {d.get('integration_reached')} reached N = {d.get('N_reached')!r} after "
                f"{d.get('radau_accepted_steps_reached')} accepted steps"
            )
        elif d["outcome"] == "success":
            what = f"parking N {d['dissipation']['parking_N']!r}, r_chi {d['dissipation']['rho_chi_ratio_today']!r}"
        else:
            what = (d.get("failure_reason") or d.get("progress") or "")[:160]
            if any(w in (d.get("failure_reason") or "") for w in WALL_SEARCH):
                stops.append(f"{key}: a wall-search failure")
        print(
            f"| {key} | {d['outcome']} | {integrations} | {d.get('wall', float('nan')):.1f} | {what} | "
            f"{o['outcome']}{'' if o['outcome'] == 'success' else ': ' + (o.get('failure_reason') or '')[:60]} |"
        )
    print()

    print("§2.1.6 the predicate's cost")
    for serial in (2197, 2188):
        p = COST / f"{serial}.json"
        if not p.exists():
            print(f"   {serial}: (missing)")
            continue
        c = json.loads(p.read_text())
        total = c["predicate_seconds"]["model"] + c["predicate_seconds"]["loop"]
        print(
            f"   {serial}: {c['predicate_calls']['model']} calls in classical_turning_point and "
            f"{c['predicate_calls']['loop']} in the step loop, {total:.4f} s in all, of a wall time of "
            f"{c['wall_s']:.1f} s: a share of {c['predicate_share_of_wall']:.3e} (the stop is 1e-2)"
        )
        if c["predicate_share_of_wall"] > 0.01:
            stops.append(f"{serial}: the predicate costs over 1%")
    print()
    print(f"stops: {stops if stops else 'none'}")
    return 1 if stops else 0


if __name__ == "__main__":
    sys.exit(main())
