# Side-by-side comparison of transition.py's histories across M, per beta. No thresholds.
# Usage: compare.py DIR BETA [BETA ...]
import json
import sys
import numpy as np
from math import exp, log

D = sys.argv[1]
Ms = ["1e-5", "1e-6", "1e-7"]
for beta in sys.argv[2:]:
    runs = {m: json.load(open(f"{D}/beta{beta}_M{m}.json")) for m in Ms}
    print(f"\n######## beta = {beta}")
    for m, d in runs.items():
        if "failure" in d:
            print(f"M={m}: FAILED {d['failure']}"); continue
        print(f"M={m}: steps {d['accepted_steps']}, turning points {d['turning_points']}, reflections {d['reflections']}, "
              f"N_final {d['N_final']:.5f}, wall {d['wall_time_s']:.0f} s, rss {d['max_rss_MB']:.0f} MB")
    # (a) bouncing: per e-fold, half-oscillations, median amplitude (in M_P and in units of M), median phi_min/M
    print("\n(a) per e-fold of N: half-oscillations | median amplitude dphi/M_P | median dphi/phi_min  [M = " + ", ".join(Ms) + "]")
    stats = {}
    Ns_settle = {}
    for m, d in runs.items():
        M = float(m)
        tp = d["turning_points_list"]
        rows = []
        for i in range(1, len(tp)):
            N, kind, phiM, lT, pmM = tp[i]
            amp = abs(tp[i][2] - tp[i - 1][2])  # in units of M
            rows.append((N, amp * M, (amp / pmM) if pmM else np.nan, lT))
        stats[m] = rows
        # N_s: the first turning point after which every half-oscillation has dphi < phi_min
        ratio = np.array([r[2] for r in rows])
        above = np.nonzero(~(ratio < 1.0))[0]
        Ns_settle[m] = (rows[above[-1] + 1][0], rows[above[-1] + 1][3]) if len(above) and above[-1] + 1 < len(rows) else None
    nmax = int(max(max(r[0] for r in s) for s in stats.values())) + 1
    for nb in range(20, nmax + 1):
        cells = []
        for m in Ms:
            sel = [r for r in stats[m] if nb <= r[0] < nb + 1]
            if sel:
                cells.append(f"{len(sel):6d} {np.median([r[1] for r in sel]):9.2e} {np.nanmedian([r[2] for r in sel]):9.2e}")
            else:
                cells.append(f"{'-':>6s} {'':9s} {'':9s}")
        lT = [r[3] for s in stats.values() for r in s if nb <= r[0] < nb + 1]
        T = f"{exp(np.median(lT)):9.3e}" if lT else " " * 9
        print(f"N {nb:3d} T_J~{T} MeV | " + " | ".join(cells))
    for m in Ms:
        s = Ns_settle[m]
        print(f"N_s (after which dphi < phi_min for every half-oscillation), M={m}: "
              + (f"N = {s[0]:.4f}, T_J = {exp(s[1]):.4e} MeV" if s else "never"))
    # (c) per e-fold averages
    keys = ["HJ_over_HE", "HJ_over_HE_rms", "pi2_over_6Mp2", "ln_A", "rho_kin_over_rho_R", "r"]
    print("\n(c) per e-fold averages over the trajectory, M = " + ", ".join(Ms))
    bins = {m: dict(zip(runs[m]["efold_bins"]["N"], range(len(runs[m]["efold_bins"]["N"])))) for m in Ms}
    for key in keys:
        print(f"  {key}:")
        for nb in range(14, nmax + 1, 2):
            vals = []
            for m in Ms:
                e = runs[m]["efold_bins"]; j = bins[m].get(float(nb))
                vals.append(e[key][j] if j is not None else np.nan)
            e0 = runs[Ms[0]]["efold_bins"]; j0 = bins[Ms[0]].get(float(nb))
            T = exp(e0["ln_TJ_MeV"][j0]) if j0 is not None else np.nan
            print(f"    N {nb:3d} T_J {T:9.3e} MeV: " + "  ".join(f"{v:+.6e}" for v in vals))
    # BBN window: r at the stored samples (what BBN reads) and cell means, 10 MeV - 1 keV
    print("\n(b) BBN window, 10 MeV to 1 keV, against M = 1e-7, on the stored samples' T_J")
    ref = runs["1e-7"]["stored_samples"]
    for m in Ms:
        s = runs[m]["stored_samples"]; c = runs[m]["cells"]
        lT = np.array(s["ln_TJ_MeV"]); r = np.array(s["r"])
        w = (lT <= log(10.0)) & (lT >= log(1e-3))
        lTr = np.array(ref["ln_TJ_MeV"]); rr = np.array(ref["r"])
        # interpolate the reference at this run's T_J (both are on main.py's axis in z_E, not T_J)
        o = np.argsort(lTr)
        rref = np.interp(lT[w], lTr[o], rr[o])
        cl = np.array(c["ln_TJ_MeV_upper"]); cm = np.array(c["r_mean"]); cw = (cl <= log(10.0)) & (cl >= log(1e-3))
        print(f"  M={m}: {w.sum()} samples; r: median {np.median(r[w]):+.4e}, min {r[w].min():+.4e}, max {r[w].max():+.4e}; "
              f"|r - r(1e-7)| median {np.median(np.abs(r[w]-rref)):.3e}, max {np.abs(r[w]-rref).max():.3e}; "
              f"cell means: median {np.median(cm[cw]):+.4e}, min {cm[cw].min():+.4e}, max {cm[cw].max():+.4e}")
    print("\n(d) today")
    for m in Ms:
        t = runs[m]["today"]
        print(f"  M={m}: phi_0/M {t['phi_0_over_M']:.6f}, phi_min/M {t['phi_min_over_M']:.6f}, pi_0 {t['pi_0']:+.6e}, "
              f"tracking {t['tracking']:.6f}, residual_H {t['residual_H']:+.6e}; seed normalization {runs[m]['normalization']:.6f}")
