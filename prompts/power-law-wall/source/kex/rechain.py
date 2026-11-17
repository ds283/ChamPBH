"""
power-law-wall prompt 07b §1.3 (P77): every stage-2 point re-chained in both forms of k_ex.

The rows, each in its own (L or x, ln R, n):
  - crossover's out/stage2_scan.txt and out/stage2_mscan.txt (n = 1, Wall(L)) and out/stage2_n2.txt (n = 2,
    WallN(L, 2)), read from prompts/one-loop-drain/source/crossover/out/ (a record; read only);
  - 02's .documents/power-law-wall/wall-02/stage2_scan.txt (PowerWall(x, n) in units of phi_t, x = e^{ln x}). It is 02's
    only scan file: wall-02/validation.txt holds validation runs and stage2_curves.md the same rows as tables.
The inputs are the ones the stage-2 run used: for crossover's cascade rows (2197, R-865, L-M*) L and ln R are read from
the cascade map the scan read (out/cascade-*.txt, matched by index), at the digits that scan read them; for its curve
rows ln R is stage2_mscan.lnR_on(L, ln(V0/M^4)) at the curve's ln(V0/M^4), as the scan computed it; the grids' are
exact. 02's are its printed ln x and ln R (4 decimals), whose effect on ell is carried into the tolerance below.

For each row and each form ("lambda", the code's -d ln V/dphi; "v3", |V'''/V''|): ell, m_ta, phi_ta/phi_t and k_ex (in
omega_t), or the reason there is no chain; the regime (classical: ln R <= 0; one-loop: a chain with ell > 0; gap:
otherwise). Then:
  - **bitwise**: kexchain.chain(w, R, "lambda") against the copied born.chain (crossover/born.py for crossover's rows,
    wall/born.py for 02's), every returned float compared with ==, at every row;
  - **the record**: ell("lambda") against the recorded ell, to the printed digits: |diff| <= half a unit of ell's last
    printed digit, plus, for 02's rows, |d ell/d ln R| and |d ell/d ln x| (centred differences, step 1e-4) times half a
    unit of the inputs' printed digit (4 decimals);
  - the rows whose regime moves, tabled, and ell_new - ell_old against L (exponential) and x (power wall).

    cd prompts/power-law-wall/source/kex && ../../../../venv/bin/python -B rechain.py \\
        > ../../../../.documents/power-law-wall/kex-07b/rechain.txt
"""

import os
import sys
import time
from math import exp, log, pi

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kexchain as kc  # noqa: E402

ROOT = os.path.abspath(os.path.join(HERE, "../../../.."))
XO_OUT = os.path.join(ROOT, "prompts/one-loop-drain/source/crossover/out")
W02 = os.path.join(ROOT, ".documents/power-law-wall/wall-02")

XO = kc.load_aside("crossover", ("walls", "born"), "xo")
WW = kc.load_aside("wall", ("walls", "born"), "w02")


def lnR_on(L, lv):
    """stage2_mscan.lnR_on (crossover), restated: ln R on the n = 1 curve of ln(V0/M^4) = lv."""
    return lv + 2 * L + 2 * log(L**4 + 2 * L**3) - log(exp(L) - 1) - log(8 * pi**2)


def cascade_rows(name):
    """{idx: (L, ln R)} of a crossover cascade map, as stage2_scan.rows reads it (the printed digits)."""
    out = {}
    for line in open(os.path.join(XO_OUT, name)):
        if line.startswith("#") or line.startswith("@@") or not line.strip():
            continue
        f = line.split()
        if not f[0].isdigit():
            continue
        if f[2] == "one_loop":
            out[int(f[0])] = (float(f[7]), float(f[6]))
        else:
            out[int(f[0])] = (float(f[6]), float(f[5]))
    return out


CASCADES = {
    "2197 A0.05": cascade_rows("cascade-P-2197-A0.05.txt"),
    "2197 A0.01": cascade_rows("cascade-P-2197-A0.01.txt"),
    "R-865 A0.05": cascade_rows("cascade-R-865-A0.05.txt"),
    "L-M8e-18": cascade_rows("cascade-L-M8e-18-A1.txt"),
    "L-M1e-16": cascade_rows("cascade-L-M1e-16-A1.txt"),
    "L-M5e-16": cascade_rows("cascade-L-M5e-16-A1.txt"),
}
CURVE_LV = {"curve-119": -119.16, "curve-129": -129.27, "M1e-3": -249.0}


def exponential_rows():
    """crossover's rows: dict(file, set, idx, n, L, lnR, ell_rec (str), frac, back, digits)."""
    rows = []
    for line in open(os.path.join(XO_OUT, "stage2_scan.txt")):
        f = line.split()
        if not f or f[0].startswith("#"):
            continue
        o = 2 if f[0] == "grid" else 3
        label = f[0] if f[0] == "grid" else f"{f[0]} {f[1]}"
        idx = int(f[o - 1])
        L, lnR = float(f[o]), float(f[o + 1])
        if label in CASCADES:
            L, lnR = CASCADES[label][idx]
        rows.append(
            dict(
                file="stage2_scan",
                set=label,
                idx=idx,
                n=1,
                L=L,
                lnR=lnR,
                ell_rec=f[o + 2],
                frac=float(f[o + 4]),
                back=f[o + 9] == "True",
            )
        )
    for line in open(os.path.join(XO_OUT, "stage2_mscan.txt")):
        f = line.split()
        if not f or f[0].startswith("#"):
            continue
        idx = int(f[1])
        L, lnR = float(f[3]), float(f[4])
        if f[0] in CASCADES:
            L, lnR = CASCADES[f[0]][idx]
        elif f[0] in CURVE_LV:
            lnR = lnR_on(L, CURVE_LV[f[0]])
        rows.append(
            dict(
                file="stage2_mscan",
                set=f[0],
                idx=idx,
                n=1,
                L=L,
                lnR=lnR,
                ell_rec=f[6],
                frac=float(f[8]),
                back=f[13] == "True",
            )
        )
    for line in open(os.path.join(XO_OUT, "stage2_n2.txt")):
        f = line.split()
        if not f or f[0] != "scan":
            continue
        rows.append(
            dict(
                file="stage2_n2",
                set="n2-grid",
                idx=-1,
                n=2,
                L=float(f[1]),
                lnR=float(f[2]),
                ell_rec=f[4],
                frac=float(f[5]),
                back=f[10] == "True",
            )
        )
    return rows


def power_rows():
    rows = []
    for line in open(os.path.join(W02, "stage2_scan.txt")):
        f = line.split()
        if not f or f[0].startswith("#"):
            continue
        if not (f[0].startswith("curve-") or f[0].startswith("grid-")):
            continue
        failed = "FAILED" in line
        rows.append(
            dict(
                file="wall-02/stage2_scan",
                set=f[0],
                idx=-1,
                n=int(f[1]),
                lnR=float(f[2]),
                lnx=float(f[3]),
                ell_rec=f[6],
                frac=None if failed else float(f[9]),
                back=None if failed else f[12] == "True",
            )
        )
    return rows


def wall_of(r):
    if "lnx" in r:
        return WW["walls"].PowerWall(exp(r["lnx"]), r["n"])
    if r["n"] == 1:
        return XO["walls"].Wall(r["L"])
    return XO["walls"].WallN(r["L"], r["n"])


def born_of(r):
    return (WW if "lnx" in r else XO)["born"].chain


def ell_at(r, dlnR=0.0, dlnx=0.0):
    rr = dict(r)
    rr["lnR"] = r["lnR"] + dlnR
    if "lnx" in r:
        rr["lnx"] = r["lnx"] + dlnx
    ch = kc.chain(wall_of(rr), exp(rr["lnR"]), "lambda")
    return None if ch is None else ch[3]


def tolerance(r, ell):
    """half a unit of the recorded ell's last digit, plus (02's rows) the inputs' printed digits propagated."""
    s = r["ell_rec"]
    tol = 0.5 * 10.0 ** (-len(s.split(".")[1]))
    if "lnx" in r:
        h = 1e-4
        for d in (dict(dlnR=h), dict(dlnx=h)):
            up = ell_at(r, **d)
            dn = ell_at(r, **{k: -v for k, v in d.items()})
            if up is not None and dn is not None:
                tol += abs(up - dn) / (2 * h) * 0.5e-4
    return tol


def fmt_chain(w, ch, why):
    if ch is None:
        return f"{'none(' + why + ')':>16s} {'':>9s} {'':>12s} {'':>11s}"
    p, m, kex, ell = ch
    return f"{ell:16.6f} {m:9.4e} {p / w.phi_t:12.6e} {kex:11.5e}"


def main():
    t0 = time.time()
    print(f"# rechain.py; start: {os.popen('uptime').read().strip()}")
    print(
        "# per row: the inputs; the recorded ell; then for each form (lambda, v3): ell, m_ta, phi_ta/phi_t, k_ex "
        "(omega_t = 1), or none(reason), and the fixed point's iterations; the regime under each; bitwise = lambda's chain == the copied born.chain; "
        "rec = |ell(lambda) - recorded| <= tol"
    )
    all_rows = [("exponential", r) for r in exponential_rows()] + [
        ("power", r) for r in power_rows()
    ]
    bitwise_bad, rec_bad, rec_n, rec_worst = 0, 0, 0, 0.0
    reasons = {}
    moves = []
    dells = []
    slow = []
    hdr = (
        f"# {'file':20s} {'set':15s} {'idx':>5s} n {'L or ln x':>10s} {'ln R':>10s} {'ell rec':>8s} | "
        f"{'ell(lambda)':>16s} {'m_ta':>9s} {'phi_ta/phi_t':>12s} {'k_ex':>11s} | {'ell(v3)':>16s} {'m_ta':>9s} "
        f"{'phi_ta/phi_t':>12s} {'k_ex':>11s} | reg(lambda) reg(v3)  bitwise rec(|diff| tol)   frac  back"
    )
    print(hdr)
    for wallname, r in all_rows:
        w = wall_of(r)
        R = exp(r["lnR"])
        old, why_old, it_old = kc.chain_reason(w, R, "lambda")
        new, why_new, it_new = kc.chain_reason(w, R, "v3")
        for form, ch, it in (("lambda", old, it_old), ("v3", new, it_new)):
            if ch is not None and it > 100:
                slow.append((form, r, it, ch[3]))
        ref = born_of(r)(w, R)
        same = (old is None and ref is None) or (
            old is not None
            and ref is not None
            and all(a == b for a, b in zip(old, ref))
        )
        bitwise_bad += not same
        reg_old, reg_new = kc.regime(r["lnR"], old), kc.regime(r["lnR"], new)
        for reg, why, ch in ((reg_old, why_old, old), (reg_new, why_new, new)):
            if ch is None and r["lnR"] > 0:
                reasons[why] = reasons.get(why, 0) + 1
        rec = ""
        rec_rec = r["ell_rec"]
        if rec_rec not in ("none", "None"):
            if old is None:
                rec = "MISSING"
                rec_bad += 1
            else:
                d = abs(old[3] - float(rec_rec))
                tol = tolerance(r, old[3])
                ok = d <= tol
                rec_bad += not ok
                rec_n += 1
                rec_worst = max(rec_worst, d)
                rec = f"{'ok' if ok else 'BAD'}({d:.1e} {tol:.1e})"
        elif old is not None:
            rec = "EXTRA"
            rec_bad += 1
        else:
            rec = "none=none"
        coord = r["L"] if "L" in r else r["lnx"]
        frac = "" if r["frac"] is None else f"{r['frac']:.4f}"
        print(
            f"  {r['file']:20s} {r['set']:15s} {r['idx']:5d} {r['n']} {coord:10.4f} {r['lnR']:10.4f} "
            f"{rec_rec:>8s} | {fmt_chain(w, old, why_old)} {it_old:3d} | {fmt_chain(w, new, why_new)} {it_new:3d} | "
            f"{reg_old:>11s} "
            f"{reg_new:>8s} {str(same):>7s} {rec:18s} {frac:>7s} {str(r['back']):>5s}"
        )
        if reg_old != reg_new:
            moves.append((wallname, r, reg_old, reg_new, new))
        if old is not None and new is not None:
            dells.append((wallname, r, coord, new[3] - old[3]))
    n_rows = len(all_rows)
    print(
        f"\n# bitwise: kexchain.chain(lambda) == the copied born.chain at {n_rows - bitwise_bad} of {n_rows} rows"
    )
    print(
        f"# the record: {rec_n} rows with a recorded ell; {rec_bad} outside the printed digits (or a chain on one side "
        f"only); largest |ell(lambda) - recorded| {rec_worst:.2e}"
    )
    print(
        f"# no chain at ln R > 0, by reason (both forms counted): {dict(sorted(reasons.items()))}"
    )
    print(
        f"# chains that needed more than 100 iterations (production's ONE_LOOP_MAX_ITERATIONS; born.chain allows 200): "
        f"{len(slow)}"
    )
    for form, r, it, ell in slow:
        coord = r["L"] if "L" in r else r["lnx"]
        print(
            f"#   {form:6s} {r['file']:20s} {r['set']:15s} {coord:10.4f} {r['lnR']:10.4f} ell {ell:.4f}: {it} iterations"
        )
    for wallname in ("exponential", "power"):
        ms = [m for m in moves if m[0] == wallname]
        print(
            f"\n## {wallname}: {len(ms)} rows change regime (of {sum(1 for a, _ in all_rows if a == wallname)})"
        )
        print(
            "#   file                 set             idx n  L or ln x       ln R  old -> new        ell(v3)    "
            "frac  back"
        )
        for _, r, a, b, new in ms:
            coord = r["L"] if "L" in r else r["lnx"]
            frac = "" if r["frac"] is None else f"{r['frac']:.4f}"
            print(
                f"    {r['file']:20s} {r['set']:15s} {r['idx']:5d} {r['n']} {coord:10.4f} {r['lnR']:10.4f}  "
                f"{a:>8s} -> {b:<8s} {'' if new is None else format(new[3], '9.4f'):>9s} {frac:>7s} "
                f"{str(r['back']):>5s}"
            )
    print(
        "\n## ell(v3) - ell(lambda) where both chains exist, against L (exponential, by n) and against x (power wall, by n)"
    )
    for wallname, key, edges in (
        ("exponential", "L", (0, 2, 4, 6, 8, 10, 15, 30, 60, 100, 140)),
        ("power", "ln x", (-10, 0, 2, 5, 10, 20, 40, 80, 120, 240)),
    ):
        for n in (1, 2):
            pts = [(c, d) for a, r, c, d in dells if a == wallname and r["n"] == n]
            if not pts:
                continue
            c = np.array([p[0] for p in pts])
            d = np.array([p[1] for p in pts])
            print(
                f"# {wallname} n = {n}: {len(pts)} rows; {key} from {c.min():.4g} to {c.max():.4g}; ell_new - ell_old "
                f"from {d.min():.4f} to {d.max():.4f}"
            )
            for lo, hi in zip(edges[:-1], edges[1:]):
                m = (c >= lo) & (c < hi)
                if m.any():
                    print(
                        f"    {key} in [{lo:g}, {hi:g}): {int(m.sum()):3d} rows, ell_new - ell_old min {d[m].min():.4f} "
                        f"median {np.median(d[m]):.4f} max {d[m].max():.4f}"
                    )
    print(
        f"\n# rechain.py end ({time.time() - t0:.0f} s): {os.popen('uptime').read().strip()}"
    )


if __name__ == "__main__":
    main()
