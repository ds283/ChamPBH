"""
Summarise the probe of PLAN.md §2 against inelastic-reflection prompt 11's points at the same
(M, beta), and against the Standard Model through the same PRyMordial path.

    PYTHONPATH=. ./venv/bin/python science-runs/plan-science-2026.8.0/summarise_probe.py

From the repository root: compute_SM_baseline runs one PRyMordial solve (about 10 s), which reads
PRyMrates/ from the working directory. Reads var/science-2026.8.0/probe/*.txt and
var/inelastic-reflection-11/grid/dis-M4e-31-*.txt; writes nothing but its stdout.
"""

import math
import re
from pathlib import Path

from ComputeTargets.BBNData import compute_SM_baseline

PROBE = Path("var/science-2026.8.0/probe")
REF = Path("var/inelastic-reflection-11/grid")


def _fields(line):
    return dict(re.findall(r"(\w+)=([^\s]+)", line))


def parse(path):
    out = {"file": path.name}
    for line in path.read_text().splitlines():
        for kind in ("history", "dissipation", "bbn"):
            if line.startswith(kind + " "):
                out[kind] = _fields(line)
        if line.startswith("## exit status:"):
            out["exit"] = line.split(":", 1)[1].strip()
        if line.startswith("## wall:"):
            out["wall"] = float(line.split(":", 1)[1].split()[0])
    return out


def _f(d, key):
    try:
        return float(d[key])
    except (KeyError, TypeError, ValueError):
        return math.nan


def row(label, r, SM):
    h, d, b = r.get("history", {}), r.get("dissipation", {}), r.get("bbn", {})
    Yp, DH = _f(b, "Yp"), _f(b, "DoH")
    park = _f(d, "parking_log_T_Jordan_GeV")
    park_keV = math.exp(park) * 1e6 if not math.isnan(park) else math.nan
    rel = d.get("release_log_T_Jordan_GeV", "None")
    margins = ",".join(
        d.get(k, "-") for k in ("birth_failing_share", "reflection_failing_share", "tracker_failing_share")
    )
    return (
        f"{label:<24} exit={r.get('exit', '?'):<16} wall={r.get('wall', math.nan):7.1f}s "
        f"steps={h.get('accepted_steps', '-'):>7} refl={h.get('reflections', '-'):>6} "
        f"inj={d.get('injections', '-'):>6} inj_frac={d.get('injected_fraction', '-'):>10} "
        f"park={park_keV:10.4g}keV release={'no' if rel == 'None' else rel} "
        f"dNeff_today={d.get('Delta_N_eff_today', '-'):>10} dNeff_1MeV={d.get('Delta_N_eff_1MeV', '-'):>10} "
        f"fail_shares={margins} "
        f"dYp={100 * (Yp / SM['Yp_BBN'] - 1):+8.4f}% dDH={100 * (DH / SM['DOverH'] - 1):+8.4f}%"
    )


def main():
    SM = compute_SM_baseline(small_network=True)
    print(
        f"SM baseline (rho_NP = 0, {SM['PRyM_version']}, small network): "
        f"Yp={SM['Yp_BBN']:.8g} D/H x1e5={SM['DOverH']:.8g}"
    )

    probe = {p.stem: parse(p) for p in sorted(PROBE.glob("*.txt"))}

    print("\n== S: small alpha at M = 4e-31, with prompt 11's alpha = 0.1 and 0.03 (ref)")
    for beta in ("1.2", "2", "3"):
        for alpha in ("0.1", "0.03"):
            ref = REF / f"dis-M4e-31-b{beta}-a{alpha}.txt"
            if ref.exists():
                print(row(f"ref b={beta} a={alpha}", parse(ref), SM))
        for alpha in ("0.01", "0.003", "0.001"):
            slug = f"S-M4e-31-b{beta}-a{alpha}"
            if slug in probe:
                print(row(f"b={beta} a={alpha}", probe[slug], SM))
        print()

    print("== L: lower M at alpha = 0.1, with prompt 11's M = 4e-31 (ref)")
    for beta in ("1.2", "2", "3"):
        ref = REF / f"dis-M4e-31-b{beta}-a0.1.txt"
        if ref.exists():
            print(row(f"ref b={beta} M=4e-31", parse(ref), SM))
        for M in ("1e-32", "1e-33"):
            slug = f"L-M{M}-b{beta}-a0.1"
            if slug in probe:
                print(row(f"b={beta} M={M}", probe[slug], SM))
        print()

    print("== N: non-surfing at M = 4e-31")
    for beta in ("0.6", "1.0"):
        for alpha in ("0.1", "0.01"):
            slug = f"N-M4e-31-b{beta}-a{alpha}"
            if slug in probe:
                print(row(f"b={beta} a={alpha}", probe[slug], SM))
    print()

    print("== T: the tail at M = 4e-31, with prompt 11's beta = 5 (ref)")
    for alpha in ("0.1", "0.03"):
        ref = REF / f"dis-M4e-31-b5-a{alpha}.txt"
        if ref.exists():
            print(row(f"ref b=5 a={alpha}", parse(ref), SM))
    for beta in ("10", "20"):
        for alpha in ("0.1", "0.01"):
            slug = f"T-M4e-31-b{beta}-a{alpha}"
            if slug in probe:
                print(row(f"b={beta} a={alpha}", probe[slug], SM))


if __name__ == "__main__":
    main()
