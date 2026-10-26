"""
jordan-normalization prompt 06, ruling 13: the comparison's tables, from compare.py's logs.

    PYTHONPATH=. ./venv/bin/python prompts/jordan-normalization/logs/06-probes/table.py LOGDIR

Reads LOGDIR/b<beta>_M<M>_phi<phi*>_<method>.log (run_all.sh's names), and prints two Markdown
tables: integrations, steps by kind and the converged values for each method; then the agreement
between the methods, the plain update's last |phi_ref/phi_min - 1|, the secant against the
planning probes (README §0.3 item 1's table, transcribed below), and the wall times.
"""

import re
import sys
from math import exp, log
from pathlib import Path

from Units import Planck_units

# (beta, M/M_P, phi*) in note 06's order, with README §0.3 item 1's tuned Lambda (meV) and
# phi_ref/M_P (the planning probes, on the units before prompt 05, from the plain start)
POINTS = [
    ("1.5", "1e-3", "5", 2.125913, 0.0349624),
    ("25", "1e-3", "5", 1.843465, 0.0068090),
    ("5.5", "1e-3", "5", 2.021177, 0.0167195),
    ("3", "1e-3", "5", 2.074576, 0.0236629),
    ("1.1", "1e-3", "2", 2.145591, 0.0415339),
    ("0.5", "1e-3", "1", 2.185554, 0.0638212),
    ("3", "1e-5", "5", 2.227091, 0.0026705),
    ("1", "1e-5", "5", 2.237209, 0.0046642),
    ("3", "0.01", "5", 1.831913, 0.0622282),
    ("1", "0.03", "5", 1.881702, 0.2028458),
    ("3", "0.1", "5", 1.389305, 0.1477194),
    ("1", "0.5", "5", 1.332621, 0.6276160),
    ("3", "0.5", "5", 0.937734, 0.2708441),
]

RESULT = re.compile(
    r" RESULT integrations=(\d+) plain=(\d+) secant=(\d+) guard=(\d+) seed=(\w+) "
    r"normalization=(\S+) phi_ref=(\S+) residual_H=(\S+) residual_phi=(\S+) .* total=([\d.]+)"
)
SEED = re.compile(r" seed=(\w+) norm=")


def read(path: Path) -> dict:
    text = path.read_text()
    m = RESULT.search(text)
    if m is None:
        raise SystemExit(f"{path}: no RESULT line")
    return {
        "integrations": int(m.group(1)),
        "plain": int(m.group(2)),
        "secant": int(m.group(3)),
        "guard": int(m.group(4)),
        "seed": m.group(5),
        "norm": float.fromhex(m.group(6)),
        "phi_ref": float.fromhex(m.group(7)),
        "residual_H": float(m.group(8)),
        "residual_phi": float(m.group(9)),
        "total": float(m.group(10)),
    }


def steps(r: dict) -> str:
    parts = [f"{k} {r[k]}" for k in ("guard", "plain", "secant") if r[k] > 0]
    return f"{r['integrations']} ({', '.join(parts)})"


def main() -> int:
    logdir = Path(sys.argv[1])
    units = Planck_units()
    Mp = units.PlanckMass

    def Lambda_meV(norm):
        return exp(norm / 4) / units.eV * 1e3

    rows = []
    for beta, M, phi, L_plan, phi_plan in POINTS:
        p = read(logdir / f"b{beta}_M{M}_phi{phi}_plain.log")
        s = read(logdir / f"b{beta}_M{M}_phi{phi}_secant.log")
        if p["seed"] != s["seed"]:
            raise SystemExit(f"seed kinds differ at {beta} {M} {phi}")
        rows.append((beta, M, phi, L_plan, phi_plan, p, s))

    print(
        "| β | M/M_P | φ* | seed | plain: integrations (steps) | secant: integrations (steps) "
        "| Λ plain (meV) | Λ secant (meV) | φ_ref/M_P plain | φ_ref/M_P secant |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|")
    for beta, M, phi, _, _, p, s in rows:
        print(
            f"| {beta} | {M} | {phi} | {s['seed']} | {steps(p)} | {steps(s)} "
            f"| {Lambda_meV(p['norm']):.6f} | {Lambda_meV(s['norm']):.6f} "
            f"| {p['phi_ref'] / Mp:.7e} | {s['phi_ref'] / Mp:.7e} |"
        )
    print()

    print(
        "| β | M/M_P | φ* | \\|Δ ln Λ\\| | \\|Δφ_ref\\|/φ_ref | plain's last \\|φ_ref/φ_min − 1\\| "
        "| Λ_secant/Λ_planning − 1 | φ_ref,secant/φ_ref,planning − 1 | wall plain, secant (s) |"
    )
    print("|---|---|---|---|---|---|---|---|---|")
    total_p = total_s = 0
    for beta, M, phi, L_plan, phi_plan, p, s in rows:
        dlnL = abs(p["norm"] - s["norm"]) / 4.0
        dphi = abs(p["phi_ref"] - s["phi_ref"]) / s["phi_ref"]
        rL = Lambda_meV(s["norm"]) / L_plan - 1.0
        rphi = (s["phi_ref"] / Mp) / phi_plan - 1.0
        total_p += p["integrations"]
        total_s += s["integrations"]
        print(
            f"| {beta} | {M} | {phi} | {dlnL:.1e} | {dphi:.1e} | {abs(p['residual_phi']):.1e} "
            f"| {rL:+.3e} | {rphi:+.1e} | {p['total']:.0f}, {s['total']:.0f} |"
        )
    print()
    print(f"integrations in all: plain {total_p}, secant {total_s}")
    worse = [
        (r[0], r[1], r[2]) for r in rows if r[6]["integrations"] > r[5]["integrations"]
    ]
    print(f"points where the secant needs more integrations: {worse or 'none'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
