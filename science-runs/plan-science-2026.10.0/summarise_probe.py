"""
Summarise the probe of PLAN.md §2 beside its comparison points: one-loop-drain 02d's observables at
1e-33, 4e-31 and 1e-30 M_P (set L), the 2026.8.0 store's export at 1e-33 M_P (set F), and 02d's
L-M1e-16 (set G).

    PYTHONPATH=. ./venv/bin/python science-runs/plan-science-2026.10.0/summarise_probe.py

From the repository root. Reads var/science-2026.10.0/probe/*.txt,
.documents/one-loop-drain/measure-02d/out/observables.txt and
var/science-2026.8.0/out/main/QCD_Cosmology/histories.csv; runs no solve and writes nothing but
its stdout.
"""

import csv
import math
import re
from pathlib import Path

PROBE = Path("var/science-2026.10.0/probe")
OBS_02D = Path(".documents/one-loop-drain/measure-02d/out/observables.txt")
HIST_8 = Path("var/science-2026.8.0/out/main/QCD_Cosmology/histories.csv")

# 02d's histories at the probe's beta, by (beta, M)
REF_02D = {
    ("1.2", "1e-33"): "B-2819",
    ("1.2", "1e-30"): "B-2821",
    ("2", "1e-33"): "B-2496",
    ("2", "4e-31"): "R-865",
    ("2", "1e-30"): "B-2498",
    ("10", "1e-33"): "B-2772",
    ("10", "4e-31"): "R-611",
    ("10", "1e-30"): "B-2774",
    ("2", "1e-16"): "L-M1e-16",
}


def _fields(line):
    return dict(re.findall(r"(\w+)=([^\s]+)", line))


def parse(path):
    out = {"tunings": 0}
    for line in path.read_text().splitlines():
        for kind in ("history", "tuning", "dissipation", "drain", "bbn"):
            if line.startswith(kind + " "):
                out[kind] = _fields(line)
        if line.startswith("-- tune "):
            out["tunings"] += 1
        if line.startswith("## exit status:"):
            out["exit"] = line.split(":", 1)[1].strip()
        if line.startswith("## wall:"):
            out["wall"] = float(line.split(":", 1)[1].split()[0])
    return out


def parse_02d():
    """{history: {A: (dNeff_1MeV, dNeff_today, r_chi_today, Yp, DoH)}}"""
    refs, current = {}, None
    for line in OBS_02D.read_text().splitlines():
        m = re.match(r"^(\S+)\s+M=\S+\s+beta=\S+", line)
        if m:
            current = refs.setdefault(m.group(1), {})
            continue
        m = re.match(r"^\s+A=(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)", line)
        if m and current is not None:
            current[m.group(1)] = tuple(m.group(i) for i in range(2, 7))
    return refs


def parse_8():
    """{(beta, M, alpha): row} from the 2026.8.0 export"""
    out = {}
    if not HIST_8.exists():
        return out
    with open(HIST_8) as f:
        for r in csv.DictReader(f):
            out[(float(r["beta"]), float(r["M_Mp"]), float(r["alpha"]))] = r
    return out


def _g(d, key, fmt="{:>11}"):
    return fmt.format(d.get(key, "-"))


def line(label, r):
    h, t, d, dr, b = (
        r.get(k, {}) for k in ("history", "tuning", "dissipation", "drain", "bbn")
    )
    regimes = (
        f"1L={dr.get('one_loop', '-')} gap={dr.get('gap', '-')} cl={dr.get('classical', '-')}"
        if dr
        else ""
    )
    return (
        f"{label:<22} exit={r.get('exit', '?'):<16} wall={r.get('wall', math.nan):7.1f}s "
        f"tunings={r['tunings']:>2} steps={h.get('accepted_steps', '-'):>8} refl={h.get('reflections', '-'):>6} "
        f"dNeff_today={_g(d, 'Delta_N_eff_today')} dNeff_1MeV={_g(d, 'Delta_N_eff_1MeV')} "
        f"Yp={_g(b, 'Yp', '{:>12}')} DoH={_g(b, 'DoH', '{:>11}')} "
        f"parkN={_g(d, 'parking_N', '{:>8}')} lnR1={_g(d, 'log_R_first_impact', '{:>8}')} {regimes}"
    )


def ref_line(label, vals):
    if vals is None:
        return f"{label:<22} (no 02d line)"
    d1, dt, _, Yp, DoH = vals
    return f"{label:<22} 02d            dNeff_today={dt:>11} dNeff_1MeV={d1:>11} Yp={Yp:>12} DoH={DoH:>11}"


def main():
    probe = {p.stem: parse(p) for p in sorted(PROBE.glob("*.txt"))}
    refs = parse_02d()
    h8 = parse_8()

    print("== C: the one-loop law at M_crit, A = 1")
    for slug in ("C-M1e-12-b1.1-A1", "C-M1e-15-b1.1-A1", "C-M1e-15-b2-A1"):
        if slug in probe:
            print(line(slug, probe[slug]))
    print()

    print("== G: the fixed law at 1e-16, beta = 2, beside 02d's one-loop L-M1e-16")
    for a in ("0.01", "0.1"):
        slug = f"G-M1e-16-b2-a{a}"
        if slug in probe:
            print(line(f"alpha={a}", probe[slug]))
    for A in ("0.05", "0.1", "1", "10"):
        print(ref_line(f"one-loop A={A}", refs.get("L-M1e-16", {}).get(A)))
    print()

    print(
        "== L: the one-loop law against M, by beta and A (02d at 1e-33, 4e-31, 1e-30)"
    )
    for beta in ("1.2", "2", "10"):
        for A in ("0.05", "1", "10"):
            for M in ("1e-40", "1e-36"):
                slug = f"L-M{M}-b{beta}-A{A}"
                if slug in probe:
                    print(line(f"b={beta} A={A} M={M}", probe[slug]))
            for M in ("1e-33", "4e-31", "1e-30"):
                h = REF_02D.get((beta, M))
                if h:
                    print(ref_line(f"b={beta} A={A} M={M}", refs.get(h, {}).get(A)))
            print()

    print("== F: the fixed law against M (2026.8.0's export at 1e-33 and 4e-31)")
    for beta in ("1.2", "2"):
        for a in ("0.01", "0.1"):
            for M in ("1e-40", "1e-36"):
                slug = f"F-M{M}-b{beta}-a{a}"
                if slug in probe:
                    print(line(f"b={beta} a={a} M={M}", probe[slug]))
            for M in (1e-33, 4e-31):
                r = h8.get((float(beta), M, float(a)))
                if r:
                    print(
                        f"{'b=%s a=%s M=%g' % (beta, a, M):<22} 2026.8.0       "
                        f"dNeff_today={r['Delta_N_eff_today']:>11} dNeff_1MeV={r['Delta_N_eff_1MeV']:>11} "
                        f"Yp={r['Yp_BBN']:>12} DoH={r['D_over_H']:>11}"
                    )
            print()


if __name__ == "__main__":
    main()
