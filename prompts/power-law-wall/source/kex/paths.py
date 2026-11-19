"""
power-law-wall prompt 07b §1.8 (P83): the mapped paths re-chained in both forms of k_ex. Measured, not judged.

The candidates:
  - crossover's cascade maps (prompts/one-loop-drain/source/crossover/out/cascade-*.txt; the exponential, n = 1, under
    the one-loop law of their day at the A of each file), with L = ln(1 + E/V0) from the printed ln(E/V0) (4 decimals;
    the printed L has 4 significant figures) and the printed ln R;
  - 02's maps (.documents/power-law-wall/wall-02/map/: 22 power-wall files, 1148 candidates, and three exponential ones;
    the fixed law), with x = E/V0 = e^{ln(E/V0)};
  - 04's sample (.documents/power-law-wall/sample-04/runs/*.txt, the `cand` rows; the power wall, both laws).
At each: in both forms, ell (the one-loop chain's, or none), ell_t = ln(k_ex/omega_t) at phi_t, the regime (classical at
ln R <= 0, one-loop with a chain, gap otherwise; unassessed candidates are skipped), and ln(k_ex/M_P) of the candidate's
peak estimate (the turnaround's k_ex where there is a chain, else the classical one), from the chain's k_ex in omega_t
and ln omega_t^2 = (ln R + ln 8 pi^2 + ln E)/2, ln E = ln V0 + ln(E/V0). The old form's ell is checked against the
recorded one (the largest difference is printed; the inputs' printed digits bound it).

**The drained share** (a first-order measure; 07d measures the histories). ΔK_c = alpha_eff,c E_c e^{4 N_c} (the ledger
of CLAUDE.md, rho_chi,E = K e^{-4N}); with the history's trajectory held fixed, eps_c = (E_c/V0) e^{4 N_c} is known
from the recorded ln(E/V0) and N at every candidate, and a law X gives K_X = sum_c alpha_X,c eps_c (V0 a constant of the
history). On 04's sample, whose rows record ΔK_c/K, alpha_rec,c eps_c, scaled to the recorded sum of ΔK/K (below 1 by the
parking's share of K), reproduces each candidate's ΔK_c/K, which checks the proxy. Reported per history: K_X/K_v1 on the exponential (v1 is production's law, drain_fraction, in the old form; the
candidates of §1.5 in the new form, with fits.json's parameters, both of fit_law's selection and of note §0.3's), and
K_new/K_old on the power wall (its law f_n(ln R) in both forms). alpha_X = min(0.99, A f_X) w_gate, with the history's A
(1 for a fixed-law map), and w_gate = 1: every candidate here is an unresolved reflection, and 04's 1632 candidates all
record w_gate = 1. The classical regime's f is R S(L) under every law (production's drain_S_of_L).

    PYTHONPATH=. ./venv/bin/python -B prompts/power-law-wall/source/kex/paths.py \\
        > .documents/power-law-wall/kex-07b/paths.txt
It writes every non-classical candidate's row to .documents/power-law-wall/kex-07b/paths_candidates.txt.
"""

import glob
import json
import os
import sys
import time
from math import exp, log, log1p, pi

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kexchain as kc  # noqa: E402
import rechain  # noqa: E402

from ComputeTargets.ScalarHistory import drain_fraction, drain_S_of_L  # noqa: E402

FF = kc.load_aside("wall", ("fit_fn",), "w02p")["fit_fn"]
ROOT = rechain.ROOT
OUT = os.path.join(ROOT, ".documents/power-law-wall/kex-07b")
LOG_8PI2 = log(8.0 * pi * pi)
FITS = json.load(open(os.path.join(OUT, "fits.json")))
SETS = (("fit_law", FITS["n1"]), ("note", FITS["n1_planner_selection"]))
CANDS = ("i", "ii", "iii", "iv")


def law(name, prm, reg, ell, L, lnR):
    """f under a §1.5 candidate in the new form (uncapped as fitted; a non-positive one-loop denominator gives y = inf)."""
    if reg == "classical":
        return exp(lnR) * drain_S_of_L(L)

    def gap():
        if lnR <= prm["R0"]:
            return 0.0
        s = 1.0 + prm["Q"] * log(L)
        return (
            0.0
            if s <= 0.0
            else 1.0 - exp(-prm["G"] * (lnR - prm["R0"]) ** prm["P"] * s)
        )

    def fall():
        d = ell + prm["B0"] - prm["C"] * log(L)
        return 1.0 if d <= 0.0 else 1.0 - exp(-prm["A"] / d)

    if name in ("i", "ii"):
        if reg == "one_loop":
            return prm["plateau"] if ell < 1.0 else fall()
        return gap()
    if name == "iii":
        return fall() if reg == "one_loop" and ell >= prm["ell_g"] else gap()
    # iv: min of the two y
    g = gap()
    if reg != "one_loop":
        return g
    return min(g, fall())


def read_cascade(path):
    head, rows = None, []
    for line in open(path):
        if line.startswith("# ") and "normalization(ln V0)=" in line:
            head = line
            continue
        f = line.split()
        if not f or not f[0].isdigit():
            continue
        if f[2] == "one_loop":
            rec_ell, alpha, lnEV0, lnR = f[3], float(f[4]), float(f[5]), float(f[6])
        else:
            rec_ell, alpha, lnEV0, lnR = None, float(f[3]), float(f[4]), float(f[5])
        rows.append(
            dict(
                idx=int(f[0]),
                N=float(f[1]),
                reg_rec=f[2],
                ell_rec=rec_ell,
                alpha=alpha,
                lnEV0=lnEV0,
                lnR=lnR,
            )
        )
    A = float(head.split(" A=")[1].split()[0])
    norm = float(head.split("normalization(ln V0)=")[1].split()[0])
    return dict(
        name=os.path.basename(path)[:-4],
        wall="exponential",
        n=1,
        A=A,
        norm=norm,
        rows=rows,
    )


def read_map02(path):
    base = os.path.basename(path)[:-4]
    n = int(base.split("-")[1][1:])
    alpha = float(base.split("-a")[-1])
    norm, rows = None, []
    for line in open(path):
        if "tuned normalization ln V0 = " in line:
            norm = float(line.split("ln V0 = ")[1].split(",")[0])
        elif line[:1] == " " and len(line.split()) >= 15:
            f = line.split()
            ell = f[7]
            rows.append(
                dict(
                    idx=int(f[0]),
                    N=float(f[1]),
                    reg_rec=f[8],
                    ell_rec=None if f[8] != "one_loop" else ell,
                    alpha=alpha,
                    lnEV0=float(f[3]),
                    lnR=float(f[4]),
                )
            )
    wall = "exponential" if base.startswith("exp") else "power"
    return dict(name=f"wall-02/{base}", wall=wall, n=n, A=1.0, norm=norm, rows=rows)


def read_run04(path):
    base = os.path.basename(path)[:-4]
    n = int(base.split("-")[1][1:])
    norm, rows = None, []
    A = float(base.split("oneloop")[1]) if "oneloop" in base else None
    for line in open(path):
        if line.startswith("sample tuning:") and "normalization=" in line:
            norm = float(line.split("normalization=")[1].split()[0])
        if not line.startswith("cand ") or line.startswith("cand idx"):
            continue
        f = line.split()
        if f[8] in ("unassessed", "None"):
            continue
        rows.append(
            dict(
                idx=int(f[1]),
                N=float(f[2]),
                reg_rec=f[8],
                ell_rec=None if f[7] == "None" else f[7],
                f_rec=None if f[9] == "None" else float(f[9]),
                alpha=float(f[10]),
                w_gate=float(f[11]),
                dK=float(f[12]),
                lnEV0=float(f[4]),
                lnR=float(f[6]),
            )
        )
    return dict(name=f"sample-04/{base}", wall="power", n=n, A=A, norm=norm, rows=rows)


def chains(h, r):
    """Both forms at one candidate: {form: (regime, ell, ell_t, ln k_ex/M_P)}, and L (exponential) or x."""
    if h["wall"] == "exponential":
        L = log1p(exp(r["lnEV0"]))
        w = (
            rechain.XO["walls"].Wall(L)
            if h["n"] == 1
            else rechain.XO["walls"].WallN(L, h["n"])
        )
    else:
        L = None
        w = rechain.WW["walls"].PowerWall(exp(r["lnEV0"]), h["n"])
    lnR = r["lnR"]
    log_E = h["norm"] + r["lnEV0"]
    log_omega_t = (
        lnR + LOG_8PI2 + log_E
    ) / 4.0  # ln omega_t^2 = (ln R + ln 8 pi^2 + ln E)/2
    out = {}
    for form in kc.FORMS:
        ch = None if lnR <= 0.0 else kc.chain(w, exp(lnR), form)
        k_t = kc.classical_kex(w, form)
        k = ch[2] if ch is not None else k_t
        out[form] = (
            kc.regime(lnR, ch),
            None if ch is None else ch[3],
            log(k_t),
            log(k) + log_omega_t,
        )
    return out, L


def main():
    t0 = time.time()
    print(f"# paths.py; start: {os.popen('uptime').read().strip()}")
    hist = [
        read_cascade(p)
        for p in sorted(
            glob.glob(
                os.path.join(
                    ROOT, "prompts/one-loop-drain/source/crossover/out/cascade-*.txt"
                )
            )
        )
    ]
    hist += [
        read_map02(p)
        for p in sorted(
            glob.glob(os.path.join(ROOT, ".documents/power-law-wall/wall-02/map/*.txt"))
        )
    ]
    hist += [
        read_run04(p)
        for p in sorted(
            glob.glob(
                os.path.join(ROOT, ".documents/power-law-wall/sample-04/runs/*.txt")
            )
        )
        if "natural" not in p
    ]
    hist = [h for h in hist if h["rows"]]
    cand_lines = [
        "# paths_candidates.txt (paths.py): every non-classical candidate, both forms. Columns: history, idx, N, ln(E/V0), "
        "ln R, recorded regime and ell; then per form (lambda | v3): regime, ell, ell_t, ln(k_ex/M_P); on the exponential "
        "f under v1 (old form) and the four candidates (fit_law's selection)"
    ]
    print(
        "# per history: candidates (non-classical); regime moves old -> new by kind; the largest |ell_new - ell_old| and "
        "the range of ell_t's change; the old form's ell against the record (largest |diff|); then the drained share: "
        "exponential K_X/K_v1 for the candidates (i)-(iv) under fit_law's selection | note §0.3's; power K_new/K_old"
    )
    totals = dict(cands=0, moves=0)
    for h in hist:
        moves, dell, dellt, rec = {}, 0.0, [], 0.0
        eps_sum, K = [], {}
        proxy = []
        ncls = 0
        for r in h["rows"]:
            c, L = chains(h, r)
            ro, rn = c["lambda"], c["v3"]
            totals["cands"] += 1
            if ro[0] != rn[0]:
                moves[f"{ro[0]}->{rn[0]}"] = moves.get(f"{ro[0]}->{rn[0]}", 0) + 1
                totals["moves"] += 1
            if ro[1] is not None and rn[1] is not None:
                dell = max(dell, abs(rn[1] - ro[1]))
            dellt.append(rn[2] - ro[2])
            if (
                r["ell_rec"] not in (None, "None", "nochain", "None(inside)")
                and ro[1] is not None
            ):
                rec = max(rec, abs(ro[1] - float(r["ell_rec"])))
            eps = r["lnEV0"] + 4.0 * r["N"]
            if h["wall"] == "exponential":
                A = h["A"]
                fs = {
                    "v1": (
                        drain_fraction(ro[0], ro[1], L, r["lnR"])
                        if ro[0] != "classical"
                        else exp(r["lnR"]) * drain_S_of_L(L)
                    )
                }
                for sname, prms in SETS:
                    for cn in CANDS:
                        fs[f"{sname}:{cn}"] = law(
                            cn, prms[cn], rn[0], rn[1], L, r["lnR"]
                        )
            else:
                A = h["A"] if h["A"] is not None else 1.0
                fs = {
                    "old": FF.fraction_power(h["n"], r["lnR"]),
                    "new": FF.fraction_power(h["n"], r["lnR"]),
                }
                # the recorded f (6 significant figures) against f_n(ln R) at the printed ln R (5 decimals)
                if (
                    "f_rec" in r
                    and r["f_rec"] is not None
                    and abs(r["f_rec"] / fs["old"] - 1.0) > 1e-4
                ):
                    fs["mismatch"] = 1.0
            for k_, f_ in fs.items():
                K.setdefault(k_, []).append((min(0.99, A * f_), eps))
            eps_sum.append(eps)
            if "dK" in r:
                proxy.append((r["alpha"], eps, r["dK"]))
            if ro[0] == "classical" and rn[0] == "classical":
                ncls += 1
                continue
            row = (
                f"{h['name']:36s} {r['idx']:5d} {r['N']:9.4f} {r['lnEV0']:10.4f} {r['lnR']:9.3f} {r['reg_rec']:>9s} "
                f"{str(r['ell_rec']):>8s} | "
                + " | ".join(
                    f"{x[0]:>9s} {'none' if x[1] is None else format(x[1], '8.4f'):>8s} {x[2]:8.4f} {x[3]:9.3f}"
                    for x in (ro, rn)
                )
            )
            if h["wall"] == "exponential":
                row += " | " + " ".join(
                    f"{fs[k]:.3f}"
                    for k in ("v1",) + tuple(f"fit_law:{c}" for c in CANDS)
                )
            cand_lines.append(row)
        # the drained shares, with eps_c normalized by the largest
        e0 = max(eps_sum)

        def Ksum(key):
            return sum(a * exp(e - e0) for a, e in K[key])

        if h["wall"] == "exponential":
            base = Ksum("v1")
            shares = (
                " ".join(f"{Ksum(f'fit_law:{c}') / base:.3f}" for c in CANDS)
                + " | "
                + " ".join(f"{Ksum(f'note:{c}') / base:.3f}" for c in CANDS)
            )
        else:
            shares = f"{Ksum('new') / Ksum('old'):.6f}" + (
                " (f_rec mismatch!)" if "mismatch" in K else ""
            )
        pr = ""
        if proxy:
            # sum dK/K over the candidates is below 1 by the parking's share of K, so compare shapes: the proxy scaled to
            # the recorded sum, against each candidate's recorded dK/K above 1e-6, relative
            tot = sum(a * exp(e - e0) for a, e, _ in proxy)
            sd = sum(d for _, _, d in proxy)
            worst = max(
                abs(a * exp(e - e0) / tot * sd / d - 1.0)
                for a, e, d in proxy
                if d > 1e-6
            )
            pr = f"; proxy vs recorded dK/K: largest relative diff {worst:.1e} (sum dK/K {sd:.6f})"
        print(
            f"{h['name']:36s} A={h['A']} n={h['n']}: {len(h['rows'])} candidates ({len(h['rows']) - ncls} non-classical); "
            f"moves {moves if moves else 'none'}; max|d ell| {dell:.4f}; d ell_t {min(dellt):.4f}..{max(dellt):.4f}; "
            f"old ell vs record {rec:.1e}; drained share {shares}{pr}"
        )
    with open(os.path.join(OUT, "paths_candidates.txt"), "w") as fh:
        fh.write("\n".join(cand_lines) + "\n")
    print(
        f"\n# {len(hist)} histories, {totals['cands']} candidates, {totals['moves']} change regime; per-candidate rows of "
        f"the non-classical in paths_candidates.txt"
    )
    print(
        f"# paths.py end ({time.time() - t0:.0f} s): {os.popen('uptime').read().strip()}"
    )


if __name__ == "__main__":
    main()
