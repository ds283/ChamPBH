"""
one-loop-drain prompt 01 §2.2 (orchestrator note 01, ruling 7): the one-loop runs of one_loop_runs.sh
(var/one-loop-drain/01/oneloop/*.json) against the drain probe's JSON summaries
(prompts/one-loop-drain/source/probe/drain/results/), and against each other.

  §2.2.1  2197 and 2188 at A = 1 and 0.1, gap zero. With the gate patched to 1.0 (the probe's law), each
          candidate's (N, alpha_eff, delta_K) must equal the probe's `cands` bitwise and in order, and the
          candidate and drain counts, by_regime's counts and its ell and alpha_eff extremes, the parking N,
          r_chi today and Delta N_eff today must equal the probe's JSON bitwise; a difference is a stop,
          reported with the first differing candidate. Then the production code (the gate kept, U9): each
          candidate whose alpha_eff differs from the probe's, with its resolved and w_gate, and the summary.
  §2.2.2  2197 at A = 1, gap edge, against gap zero (production): every candidate's (N, alpha_eff, delta_K)
          and the summary; the record names the prescription.
  §2.2.3  3016 and 3233 at A = 1: every alpha_eff 0; their reflections, parking and r_chi today beside the
          probe's.
  §2.2.4  3873 at A = 1: the outcome.
  §2.2.5  each run's "drain" key: one record per Injection, in order; the regime counts; the regime against
          peak_estimate's for every candidate (but a one-loop solution with ell <= 0, ruling 4), and the
          loop's PeakEstimate against a fresh one.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law/probe_table.py > .documents/one-loop-drain/law/out/probe_table.txt
"""

import json
import re
import os
import sys
from pathlib import Path

RUNS = Path(
    os.environ.get(
        "ONE_LOOP_DRAIN_01_RUNS",
        "/Users/ds283/Documents/Code/ChamPBH/var/one-loop-drain/01/oneloop",
    )
)
PROBE = Path(
    "/Users/ds283/Documents/Code/ChamPBH/prompts/one-loop-drain/source/probe/drain/results"
)
PARKING_N = re.compile(r"Parking\(N=(?P<N>[-0-9.e+]+),")

stops = []


def load(name):
    path = RUNS / f"{name}.json"
    return json.loads(path.read_text()) if path.exists() else None


def probe(serial, A):
    return json.loads((PROBE / f"{serial}-A{A}.json").read_text())


def probe_parking_N(p):
    if p.get("parking") in (None, "None"):
        return None
    return float(PARKING_N.match(p["parking"])["N"])


def bits(x):
    return None if x is None else float(x).hex()


def summary(run):
    if run["failure"]:
        return {"failure": run["failure_reason"]}
    dv = run["dark_radiation_values"]
    return {
        "candidates": run["candidates"],
        "drains": run["drains"],
        "reflections": run["reflections"],
        "parking_N": run["parking_N"],
        "rho_chi_ratio_today": dv["rho_chi_ratio_today"],
        "Delta_N_eff_today": dv["Delta_N_eff_today"],
        "wall_s": round(run["wall_s"], 1),
    }


def compare_with_probe(run, p, label, bitwise_cands=True):
    """The ruling-7 comparison; returns the list of differences."""
    diffs = []
    if run["failure"] or p["failure"]:
        if run["failure"] != p["failure"]:
            diffs.append(
                f"outcome: here failure={run['failure']}, probe failure={p['failure']}"
            )
        return diffs
    for key in ("candidates", "drains", "reflections"):
        if run[key] != p[key]:
            diffs.append(f"{key}: here {run[key]}, probe {p[key]}")
    here = [(c["N"], c["alpha_eff"], c["delta_K"]) for c in run["cands"]]
    there = [(c[0], c[10], c[11]) for c in p["cands"]]
    if bitwise_cands:
        for k, (a, b) in enumerate(zip(here, there)):
            if tuple(map(bits, a)) != tuple(map(bits, b)):
                diffs.append(
                    f"first differing candidate {k}: here (N, alpha_eff, delta_K) = {a}, probe {b}"
                )
                break
        if len(here) != len(there):
            diffs.append(
                f"candidate lists differ in length: here {len(here)}, probe {len(there)}"
            )
    by_here, by_probe = run["by_regime"], p["by_regime"]
    if set(by_here) != set(by_probe):
        diffs.append(
            f"by_regime's regimes: here {sorted(by_here)}, probe {sorted(by_probe)}"
        )
    for regime in sorted(set(by_here) & set(by_probe)):
        for key in ("n", "alpha_min", "alpha_max", "ell_min", "ell_max"):
            if bits(by_here[regime][key]) != bits(by_probe[regime][key]):
                diffs.append(
                    f"by_regime[{regime}][{key}]: here {by_here[regime][key]!r}, probe {by_probe[regime][key]!r}"
                )
    if bits(run["parking_N"]) != bits(probe_parking_N(p)):
        diffs.append(
            f"parking N: here {run['parking_N']!r}, probe {probe_parking_N(p)!r}"
        )
    for key in ("rho_chi_ratio_today", "Delta_N_eff_today"):
        a, b = run["dark_radiation_values"][key], p["dark_radiation_values"][key]
        if bits(a) != bits(b):
            diffs.append(f"{key}: here {a!r}, probe {b!r}")
    return diffs


def drain_key_check(run, label):
    d = run["drain"]
    if d is None:
        print(f"   {label}: no drain record")
        return
    if run["failure"]:
        print(
            f"   {label}: failure; drain record as far as the loop reached: {d['n_candidates']} candidates, "
            f"regimes {d['regime_counts']}, unresolvable_assessments {d['unresolvable_assessments']}, "
            f"gap prescription {d['gap_prescription']!r}"
        )
        return
    one_per = d["n_candidates"] == run["candidates"]
    in_order = all(
        rec[2] == c["alpha_eff"] and rec[0] == c["regime"]
        for rec, c in zip(d["candidates"], run["cands"])
    )
    agree = [c for c in run["cands"] if not c["ell_nonpositive"]]
    disagree = [
        k
        for k, c in enumerate(run["cands"])
        if not c["ell_nonpositive"] and not c["regime_agrees"]
    ]
    reuse_bad = [k for k, c in enumerate(run["cands"]) if not c["reuse_equals_fresh"]]
    nonpos = [(c["N"], c["ell"]) for c in run["cands"] if c["ell_nonpositive"]]
    print(
        f"   {label}: law {d['law']}, A {d['A']}, gap {d['gap_prescription']!r}; {d['n_candidates']} records for "
        f"{run['candidates']} Injections (one per Injection, in order: {one_per and in_order}); regimes "
        f"{d['regime_counts']}; unresolvable_assessments {d['unresolvable_assessments']}; unresolved_declined "
        f"{d.get('unresolved_declined')}; regime agrees with "
        f"peak_estimate's at {len(agree) - len(disagree)} of {len(agree)} (disagreeing: {disagree}); ell <= 0 "
        f"candidates: {nonpos}; reused PeakEstimate equal to a fresh one at "
        f"{len(run['cands']) - len(reuse_bad)} of {len(run['cands'])}"
    )
    if not (one_per and in_order) or disagree or reuse_bad:
        stops.append(f"{label}: the drain key check failed")


def main():
    print("§2.2.1 the probe's table (2197, 2188; A = 1, 0.1; gap zero)")
    print()
    print(
        "| run | law | candidates | drains | reflections | parking N | r_chi today | Delta N_eff today | wall (s) |"
    )
    print("|---|---|---|---|---|---|---|---|---|")
    for serial in (2197, 2188):
        for A in ("1", "0.1"):
            p = probe(serial, A)
            print(
                f"| {serial} A = {A} | probe | {p['candidates']} | {p['drains']} | {p['reflections']} | "
                f"{probe_parking_N(p)!r} | {p['dark_radiation_values']['rho_chi_ratio_today']!r} | "
                f"{p['dark_radiation_values']['Delta_N_eff_today']!r} | {p['wall_s']:.1f} |"
            )
            for gate in ("probe", "production"):
                run = load(f"{serial}-A{A}-zero-{gate}")
                if run is None:
                    print(f"| {serial} A = {A} | {gate} | (missing) |")
                    stops.append(f"{serial} A={A} {gate}: missing")
                    continue
                s = summary(run)
                print(
                    f"| {serial} A = {A} | here, gate {gate} | {s.get('candidates')} | {s.get('drains')} | "
                    f"{s.get('reflections')} | {s.get('parking_N')!r} | {s.get('rho_chi_ratio_today')!r} | "
                    f"{s.get('Delta_N_eff_today')!r} | {s.get('wall_s')} |"
                )
    print()
    for serial in (2197, 2188):
        for A in ("1", "0.1"):
            p = probe(serial, A)
            run = load(f"{serial}-A{A}-zero-probe")
            diffs = compare_with_probe(run, p, f"{serial} A={A}")
            print(
                f"{serial} A = {A}, gate patched to 1.0, against the probe, bitwise: {'equal' if not diffs else 'DIFFERENT'}"
            )
            for d in diffs:
                print(f"   {d}")
            if diffs:
                stops.append(f"{serial} A={A} probe-gate differs from the probe")
            prod = load(f"{serial}-A{A}-zero-production")
            moved = [
                (
                    k,
                    c["N"],
                    c["alpha_eff"],
                    pc[10],
                    c["resolved"],
                    c["w_gate"],
                    c["kind"],
                )
                for k, (c, pc) in enumerate(zip(prod["cands"], p["cands"]))
                if bits(c["alpha_eff"]) != bits(pc[10])
            ]
            print(
                f"{serial} A = {A}, production (gate kept): {len(moved)} of {len(prod['cands'])} candidates' alpha_eff "
                f"differ from the probe's; resolved candidates {sum(1 for c in prod['cands'] if c['resolved'])}, "
                f"w_gate < 1 at {sum(1 for c in prod['cands'] if c['w_gate'] < 1.0)}; kinds "
                f"{ {k: sum(1 for c in prod['cands'] if c['kind'] == k) for k in sorted({c['kind'] for c in prod['cands']})} }"
            )
            for k, N, a, b, resolved, w, kind in moved[:20]:
                print(
                    f"   candidate {k} ({kind}) at N = {N!r}: alpha_eff {a!r} here, {b!r} probe; resolved {resolved}, w_gate {w!r}"
                )
            diffs = compare_with_probe(
                prod, p, f"{serial} A={A} production", bitwise_cands=False
            )
            print(
                f"   summary against the probe: {'equal' if not diffs else '; '.join(diffs)}"
            )
    print()

    print(
        "§2.2.2 both prescriptions: 2197 at A = 1, gap edge against gap zero (production)"
    )
    zero, edge = load("2197-A1-zero-production"), load("2197-A1-edge-production")
    same = [(c["N"], c["alpha_eff"], c["delta_K"]) for c in zero["cands"]] == [
        (c["N"], c["alpha_eff"], c["delta_K"]) for c in edge["cands"]
    ] and summary(zero) | {"wall_s": 0} == summary(edge) | {"wall_s": 0}
    print(
        f"   every candidate's (N, alpha_eff, delta_K) and the summary equal: {same}; gap candidates "
        f"{edge['drain']['regime_counts']['gap']}; the records name {zero['drain']['gap_prescription']!r} and "
        f"{edge['drain']['gap_prescription']!r}"
    )
    if not same:
        stops.append("2197 edge differs from zero")
    print()

    print("§2.2.3 large M at A = 1: 3016 and 3233; §2.2.4: 3873")
    print()
    print(
        "| run | gate | outcome | candidates | every alpha_eff 0 | regimes | reflections | parking N | r_chi today | wall (s) |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|")
    for serial in (3016, 3233, 3873):
        p = probe(serial, "1")
        print(
            f"| {serial} | probe | {'failure' if p['failure'] else 'success'} | {p.get('candidates')} | "
            f"{None if p['failure'] else all(c[10] == 0.0 for c in p['cands'])} | "
            f"{None if p['failure'] else sorted(p['by_regime'])} | {p.get('reflections')} | "
            f"{None if p['failure'] else probe_parking_N(p)!r} | "
            f"{None if p['failure'] else p['dark_radiation_values']['rho_chi_ratio_today']!r} | {p['wall_s']:.1f} |"
        )
        for gate in ("probe", "production"):
            run = load(f"{serial}-A1-zero-{gate}")
            if run["failure"]:
                print(
                    f"| {serial} | {gate} | failure | | | {run['drain']['regime_counts']} | | | | {run['wall_s']:.1f} |"
                )
                continue
            zero_all = all(c["alpha_eff"] == 0.0 for c in run["cands"])
            print(
                f"| {serial} | here, {gate} | success | {run['candidates']} | {zero_all} | "
                f"{run['drain']['regime_counts']} | {run['reflections']} | {run['parking_N']!r} | "
                f"{run['dark_radiation_values']['rho_chi_ratio_today']!r} | {run['wall_s']:.1f} |"
            )
            if not zero_all and serial in (3016, 3233):
                stops.append(f"{serial} {gate}: an alpha_eff is not 0")
        for gate in ("probe",):
            run = load(f"{serial}-A1-zero-{gate}")
            diffs = compare_with_probe(run, p, f"{serial}")
            print(
                f"   {serial}, gate patched, against the probe bitwise: {'equal' if not diffs else '; '.join(diffs)}"
            )
            # 3873 stopped in the probe; after the user's ruling (b) it may complete, which is not a stop
            if diffs and serial != 3873:
                stops.append(f"{serial} differs from the probe")
    for gate in ("probe", "production"):
        run = load(f"3873-A1-zero-{gate}")
        print(
            f"   3873 ({gate}): failure {run['failure']}; reason {run['failure_reason']!r}; unresolved_declined "
            f"{run['drain'].get('unresolved_declined')}"
            + (
                ""
                if run["failure"]
                else f"; {run['candidates']} candidates, {run['reflections']} reflections, "
                f"parking N {run['parking_N']!r}, r_chi today {run['dark_radiation_values']['rho_chi_ratio_today']!r}"
            )
        )
    print()

    print('§2.2.5 the "drain" key of each run')
    for path in sorted(RUNS.glob("*-A*-*-*.json")):
        run = json.loads(path.read_text())
        drain_key_check(run, path.stem)
    print()
    print(f"stops: {stops if stops else 'none'}")
    return 1 if stops else 0


if __name__ == "__main__":
    sys.exit(main())
