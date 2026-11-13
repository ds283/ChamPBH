"""
electroweak-eos prompt 02 §1.4, "The comparison" and "Beside the figures" (README §9.2 P18, P19): the sample's
histories, old tree (ec6bca8, 2026.10.0) against new (2026.11.0), from the two sides' exports. Measurements, not
bounds: nothing here is tested against a threshold.

    ./venv/bin/python .documents/electroweak-eos/effect/compare_sample.py \\
        [--old var/electroweak-eos/02/export/old/QCD_Cosmology] \\
        [--new var/electroweak-eos/02/export/new/QCD_Cosmology] [--out DIR]

It reads each side's histories.csv (plot_by_beta.py --no-plots) and, for each one-loop history, its bounces.csv
(plot_ScalarHistory.py --no-plots). The export's marker rows ("no ScalarHistory row in the store") are set aside. Rows
are matched old to new on beta, M, n, phi*, alpha and drain_model, as written (both sides built them from the same
options). Each history is put in the run of the sample that built it (README §9.2 P14): r at M = 1e-16 M_P; c at
1e-15 and 1e-12; d at 1e-3, 0.1 and 0.5; b at 1e-30 to 1e-42; at 4e-31, a-edge for the 13 beta of 2.980-3.040 and
a-spread otherwise.

It writes into --out (default out/ beside this script):
  - effect.csv, one row per history: for each observable, old, new, new - old and (new - old)/|old|;
  - effect.txt:
    - the histories whose T_deliver lies on opposite sides of 10 GeV on the two trees (a label between the QCD
      deliveries, about 1-2 GeV, and the EW ones, about 30-60 GeV; not a test), with beta, M and row;
    - failures on either side, with both reasons;
    - per one-loop history, the two bounces.csv aligned by ordinal: the counts, the first ordinal whose kind or
      source differs, and over the rows before it the largest relative difference in N, log_T_Jordan_GeV and
      alpha_eff (P19), and the largest absolute difference in log_T_Jordan_GeV beside its relative one (ln T_J is
      near 0 at about 1 GeV, where a relative difference of the logarithm says nothing; the absolute one is the
      relative change of T_J);
    - by run, the largest and median |new - old| and |new - old|/|old| of each observable, over the histories
      with a value on both sides;
    - the quantities each figure of 2026.10.0 plots, for the sampled histories that lie on it, in the figure's own
      units (fig6, fig6b, m-trend, relativistic).
"""

import argparse
import csv
import math
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]))
from Units.base import REDUCED_PLANCK_MASS_EV  # noqa: E402

MARKER = "no ScalarHistory row in the store"
EDGE = [repr(i / 1000) for i in range(2980, 3041, 5)]
EDGE_F = {float(b) for b in EDGE}
RUNS = ("a-edge", "a-spread", "b", "c", "d", "r")
LABEL_T_GEV = 10.0

OBSERVABLES = [
    "Yp_BBN",
    "D_over_H",
    "delta_Yp",
    "delta_D_over_H",
    "Delta_N_eff_today",
    "Delta_N_eff_1MeV",
    "T_deliver_GeV",
    "reflections",
    "injections",
    "bounce_rows",
    "candidates_one_loop",
    "candidates_gap",
    "candidates_classical",
    "candidates_unassessed",
    "drained_share_one_loop",
    "drained_share_gap",
    "drained_share_classical",
    "drained_share_unassessed",
    "parking_T_GeV",
    "release_T_GeV",
    "tuning_iterations",
    # beside the figures (fig6b's ln R; relativistic's margins and shares)
    "log_R_first_impact",
    "ell_min",
    "all_worst_birth_margin",
    "all_worst_reflection_margin",
    "all_reflection_failing_share",
    "parking_share",
]
TEXT = ["failure_reasons"]
BOUNCE_FIELDS = ("N", "log_T_Jordan_GeV", "alpha_eff")


def read(path: Path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def num(s):
    if s is None or s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def key(r):
    return (
        float(r["beta"]),
        float(r["M_Mp"]),
        float(r["n"]),
        float(r["phi_init_Mp"]),
        float(r["alpha"]),
        r["drain_model"],
    )


def row_label(r) -> str:
    if r["drain_model"] == "fixed":
        return f"alpha={r['alpha']}"
    return f"A={r['alpha']} ({r['drain_model']})"


def run_of(r) -> str:
    M = float(r["M_Mp"])
    close = lambda x: math.isclose(M, x, rel_tol=1e-12)  # noqa: E731
    if close(1e-16):
        return "r"
    if close(1e-15) or close(1e-12):
        return "c"
    if close(1e-3) or close(0.1) or close(0.5):
        return "d"
    if close(4e-31):
        return "a-edge" if float(r["beta"]) in EDGE_F else "a-spread"
    return "b"


def history_dir(root: Path, r):
    """The per-history folder of plot_ScalarHistory.py's export (<row folder>/csv/beta=<beta>/M=<M>eV_n=<n>/), found by
    value: the row folder whose alpha or A equals the row's, the beta folder whose beta equals it, and the M folder
    whose M in eV (written to five significant figures) is the row's to 1e-4. None if there is no fields.csv.
    """
    if r["drain_model"] == "fixed":
        rows = list(root.glob("alpha=*"))
    else:
        rows = list((root / r["drain_model"]).glob("A=*"))
    base = next(
        (
            d / "csv"
            for d in rows
            if float(d.name.split("=", 1)[1]) == float(r["alpha"])
        ),
        None,
    )
    if base is None:
        return None
    beta = float(r["beta"])
    M_eV = float(r["M_Mp"]) * REDUCED_PLANCK_MASS_EV
    for bdir in base.glob("beta=*"):
        if float(bdir.name.split("=", 1)[1]) != beta:
            continue
        for mdir in bdir.glob("M=*eV_n=*"):
            m = float(mdir.name[2:].split("eV_n=")[0])
            if math.isclose(m, M_eV, rel_tol=1e-4) and (mdir / "fields.csv").is_file():
                return mdir
    return None


def compare_bounces(old_dir: Path, new_dir: Path):
    fo, fn = (
        (old_dir / "bounces.csv", new_dir / "bounces.csv")
        if old_dir and new_dir
        else (None, None)
    )
    if fo is None or not fo.is_file() or not fn.is_file():
        return None
    bo, bn = read(fo), read(fn)
    first = None
    for i, (a, b) in enumerate(zip(bo, bn)):
        if a["kind"] != b["kind"] or a["source"] != b["source"]:
            first = i
            break
    upto = first if first is not None else min(len(bo), len(bn))
    worst = {}
    for f in BOUNCE_FIELDS + ("abs:log_T_Jordan_GeV",):
        absolute = f.startswith("abs:")
        col = f[4:] if absolute else f
        w, at = 0.0, None
        for i in range(upto):
            x, y = num(bo[i][col]), num(bn[i][col])
            if x is None or y is None:
                continue
            if absolute:
                d = abs(y - x)
            else:
                d = abs(y - x) / abs(x) if x != 0 else (0.0 if y == 0 else math.inf)
            if d > w:
                w, at = d, i
        worst[f] = (w, at)
    first_impact = lambda rows: next(  # noqa: E731
        (
            num(r["ell"])
            for r in rows
            if r["source"] == "candidate" and num(r["ell"]) is not None
        ),
        None,
    )
    return dict(
        n_old=len(bo),
        n_new=len(bn),
        first_diff=first,
        first_diff_old=(
            (bo[first]["source"], bo[first]["kind"], bo[first]["N"])
            if first is not None
            else None
        ),
        first_diff_new=(
            (bn[first]["source"], bn[first]["kind"], bn[first]["N"])
            if first is not None
            else None
        ),
        worst=worst,
        ell_first_old=first_impact(bo),
        ell_first_new=first_impact(bn),
    )


def fmt(x, spec=".6g"):
    if x is None:
        return "-"
    if isinstance(x, float) and math.isinf(x):
        return "inf"
    return format(x, spec)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument(
        "--old",
        type=Path,
        default=Path("var/electroweak-eos/02/export/old/QCD_Cosmology"),
    )
    p.add_argument(
        "--new",
        type=Path,
        default=Path("var/electroweak-eos/02/export/new/QCD_Cosmology"),
    )
    p.add_argument("--out", type=Path, default=HERE / "out")
    args = p.parse_args(argv)

    lines = []

    def say(s=""):
        print(s)
        lines.append(s)

    old = [
        r
        for r in read(args.old / "histories.csv")
        if MARKER not in r["failure_reasons"]
    ]
    new = [
        r
        for r in read(args.new / "histories.csv")
        if MARKER not in r["failure_reasons"]
    ]
    O = {key(r): r for r in old}
    Nw = {key(r): r for r in new}
    say("compare_sample.py (electroweak-eos prompt 02 §1.4, P18-P19)")
    say(
        f"old: {args.old} ({len(old)} histories); new: {args.new} ({len(new)} histories)"
    )
    only_old = sorted(set(O) - set(Nw))
    only_new = sorted(set(Nw) - set(O))
    if only_old or only_new:
        say(
            f"!! unmatched: {len(only_old)} only on the old side, {len(only_new)} only on the new"
        )
    keys = sorted(
        set(O) & set(Nw),
        key=lambda k: (RUNS.index(run_of(O[k])), k[1], k[5], k[4], k[0]),
    )
    say(f"matched: {len(keys)}")

    out_rows = []
    per = {}
    for k in keys:
        a, b = O[k], Nw[k]
        run = run_of(a)
        rec = dict(
            run=run,
            beta=a["beta"],
            M_Mp=a["M_Mp"],
            row=row_label(a),
            alpha=a["alpha"],
            drain_model=a["drain_model"],
        )
        vals = {}
        for c in OBSERVABLES:
            x, y = num(a[c]), num(b[c])
            rec[f"{c}_old"] = a[c]
            rec[f"{c}_new"] = b[c]
            d = (y - x) if (x is not None and y is not None) else None
            rel = (
                (d / abs(x))
                if (d is not None and x != 0)
                else (0.0 if d == 0 else None)
            )
            rec[f"{c}_diff"] = "" if d is None else repr(d)
            rec[f"{c}_rel"] = "" if rel is None else repr(rel)
            vals[c] = (x, y, d, rel)
        for c in TEXT:
            rec[f"{c}_old"] = a[c]
            rec[f"{c}_new"] = b[c]
        od = history_dir(args.old, a)
        nd = history_dir(args.new, b)
        bc = compare_bounces(od, nd) if a["drain_model"] != "fixed" else None
        if bc:
            rec["bounces_old"] = bc["n_old"]
            rec["bounces_new"] = bc["n_new"]
            rec["bounces_first_diff"] = (
                "" if bc["first_diff"] is None else bc["first_diff"]
            )
            for f in BOUNCE_FIELDS:
                rec[f"bounces_max_rel_{f}"] = repr(bc["worst"][f][0])
            rec["bounces_max_abs_log_T_Jordan_GeV"] = repr(
                bc["worst"]["abs:log_T_Jordan_GeV"][0]
            )
            rec["ell_first_impact_old"] = (
                "" if bc["ell_first_old"] is None else repr(bc["ell_first_old"])
            )
            rec["ell_first_impact_new"] = (
                "" if bc["ell_first_new"] is None else repr(bc["ell_first_new"])
            )
        out_rows.append(rec)
        per[k] = dict(a=a, b=b, run=run, vals=vals, bc=bc, od=od, nd=nd)

    # ---- the 10 GeV label
    say()
    say(
        f"T_deliver on opposite sides of {LABEL_T_GEV:g} GeV on the two trees (a label between the QCD deliveries, "
        "about 1-2 GeV, and the EW ones, about 30-60 GeV; not a test):"
    )
    n_cross = 0
    for k in keys:
        x, y, _, _ = per[k]["vals"]["T_deliver_GeV"]
        if x is None or y is None:
            continue
        if (x < LABEL_T_GEV) != (y < LABEL_T_GEV):
            n_cross += 1
            a = per[k]["a"]
            say(
                f"  {per[k]['run']}: beta={a['beta']} M={a['M_Mp']} {row_label(a)}: "
                f"T_deliver {x:.6g} -> {y:.6g} GeV"
            )
    say(f"  {n_cross} histories")

    # ---- failures
    say()
    say("failures (failure_reasons non-empty) on either side:")
    n_fail = 0
    for k in keys:
        a, b = per[k]["a"], per[k]["b"]
        if a["failure_reasons"] or b["failure_reasons"]:
            n_fail += 1
            say(f"  {per[k]['run']}: beta={a['beta']} M={a['M_Mp']} {row_label(a)}")
            say(f"     old: {a['failure_reasons'] or '(none)'}")
            say(f"     new: {b['failure_reasons'] or '(none)'}")
    say(f"  {n_fail} histories")

    # ---- the bounce rows
    say()
    say(
        "bounce rows (one-loop histories, bounces.csv by ordinal): counts old/new, the first ordinal whose kind or "
        "source differs, and before it the largest relative |new - old| in N, log_T_Jordan_GeV and alpha_eff"
    )
    for k in keys:
        bc = per[k]["bc"]
        if per[k]["a"]["drain_model"] == "fixed":
            continue
        a = per[k]["a"]
        tag = f"  {per[k]['run']}: beta={a['beta']} M={a['M_Mp']} {row_label(a)}"
        if bc is None:
            say(
                f"{tag}: no bounces.csv on {'both sides' if not per[k]['od'] and not per[k]['nd'] else 'one side'}"
            )
            continue
        fd = (
            f"none in the first {min(bc['n_old'], bc['n_new'])} (the shorter's length)"
            if bc["first_diff"] is None
            else (
                f"{bc['first_diff']} (old {bc['first_diff_old'][0]}/{bc['first_diff_old'][1] or '-'} at N {float(bc['first_diff_old'][2]):.6g}; "
                f"new {bc['first_diff_new'][0]}/{bc['first_diff_new'][1] or '-'} at N {float(bc['first_diff_new'][2]):.6g})"
            )
        )
        w = bc["worst"]
        at = lambda k: "" if w[k][1] is None else f" (ordinal {w[k][1]})"  # noqa: E731
        say(
            f"{tag}: {bc['n_old']}/{bc['n_new']} rows; first ordinal whose kind or source differs: {fd}; before it, "
            f"max rel N {fmt(w['N'][0], '.3g')}{at('N')}, max |d ln T_J| {fmt(w['abs:log_T_Jordan_GeV'][0], '.3g')}"
            f"{at('abs:log_T_Jordan_GeV')} (rel {fmt(w['log_T_Jordan_GeV'][0], '.3g')}), max rel alpha_eff "
            f"{fmt(w['alpha_eff'][0], '.3g')}{at('alpha_eff')}"
        )

    # ---- by run
    say()
    say(
        "by run: over the histories with a value on both sides, |new - old| and |new - old|/|old|, largest and median"
    )
    for run in RUNS:
        ks = [k for k in keys if per[k]["run"] == run]
        say(f"[{run}] {len(ks)} histories")
        for c in OBSERVABLES:
            ds = [
                abs(per[k]["vals"][c][2])
                for k in ks
                if per[k]["vals"][c][2] is not None
            ]
            rs = [
                abs(per[k]["vals"][c][3])
                for k in ks
                if per[k]["vals"][c][3] is not None
                and math.isfinite(per[k]["vals"][c][3])
            ]
            if not ds:
                continue
            say(
                f"  {c:30s} n={len(ds):2d}  |d| max {max(ds):.4g} median {statistics.median(ds):.4g}"
                + (
                    f"  |rel| max {max(rs):.4g} median {statistics.median(rs):.4g}"
                    if rs
                    else ""
                )
            )

    # ---- beside the figures
    YP0, DH0 = 0.24688, 2.4583e-5
    say()
    say(
        "beside the figures (each figure's own units; the sampled histories that lie on it)"
    )
    say(
        "[fig6] M = 4e-31 M_P: dYp = 100 delta_Yp [%] (axis -1.5..12.5), dDH = 100 delta_D_over_H [%] (-3..23), "
        "Delta_N_eff today (log 1e-4..1), T_J at parking [GeV] (log 1e-13..1e3); x = beta"
    )
    for k in keys:
        if per[k]["run"] not in ("a-edge", "a-spread"):
            continue
        a, v = per[k]["a"], per[k]["vals"]
        dy = v["delta_Yp"]
        dd = v["delta_D_over_H"]
        nf = v["Delta_N_eff_today"]
        pk = v["parking_T_GeV"]
        say(
            f"  beta={a['beta']:6s} {row_label(a):32s} dYp {fmt(None if dy[0] is None else 100*dy[0], '.4f')} -> "
            f"{fmt(None if dy[1] is None else 100*dy[1], '.4f')} ({fmt(None if dy[2] is None else 100*dy[2], '+.4f')} pp); "
            f"dDH {fmt(None if dd[0] is None else 100*dd[0], '.4f')} -> {fmt(None if dd[1] is None else 100*dd[1], '.4f')} "
            f"({fmt(None if dd[2] is None else 100*dd[2], '+.4f')} pp); dNeff {fmt(nf[0], '.5g')} -> {fmt(nf[1], '.5g')} "
            f"(x{fmt(None if nf[0] in (None, 0) or nf[1] is None else nf[1]/nf[0], '.5g')}); "
            f"T_park {fmt(pk[0], '.5g')} -> {fmt(pk[1], '.5g')} GeV"
        )
    say(
        "[fig6b] M = 1e-15, 1e-12 M_P: dYp [%], dDH [%], Delta_N_eff today (log 1e-9..1), ln R at the first impact "
        "(-40..32); one-loop A = 1 and the fixed law"
    )
    for k in keys:
        if per[k]["run"] != "c":
            continue
        a, v = per[k]["a"], per[k]["vals"]
        dy, dd, nf, lr = (
            v["delta_Yp"],
            v["delta_D_over_H"],
            v["Delta_N_eff_today"],
            v["log_R_first_impact"],
        )
        say(
            f"  M={a['M_Mp']:6s} beta={a['beta']} {row_label(a):32s} dYp {fmt(None if dy[2] is None else 100*dy[2], '+.4f')} pp; "
            f"dDH {fmt(None if dd[2] is None else 100*dd[2], '+.4f')} pp; dNeff {fmt(nf[0], '.5g')} -> {fmt(nf[1], '.5g')}; "
            f"ln R {fmt(lr[0], '.6g')} -> {fmt(lr[1], '.6g')}"
        )
    say(
        "[m-trend] A = 1: (a) Delta_N_eff today relative to 4e-31 (axis 0.38..1.08), here at beta = 2 (and 3), "
        "(b) ell at the first impact (28..74), first candidate row of bounces.csv"
    )
    lab = {}
    for k in keys:
        a = per[k]["a"]
        if (
            a["drain_model"] != "fixed"
            and float(a["alpha"]) == 1.0
            and math.isclose(float(a["M_Mp"]), 4e-31, rel_tol=1e-12)
        ):
            lab[float(a["beta"])] = per[k]["vals"]["Delta_N_eff_today"]
    for k in keys:
        a = per[k]["a"]
        if (
            per[k]["run"] not in ("b", "a-spread", "a-edge")
            or a["drain_model"] == "fixed"
            or float(a["alpha"]) != 1.0
        ):
            continue
        beta = float(a["beta"])
        if beta not in (2.0, 3.0):
            continue
        v = per[k]["vals"]["Delta_N_eff_today"]
        ref = lab.get(beta)
        ro = v[0] / ref[0] if ref and v[0] is not None and ref[0] else None
        rn = v[1] / ref[1] if ref and v[1] is not None and ref[1] else None
        bc = per[k]["bc"]
        say(
            f"  M={a['M_Mp']:6s} beta={a['beta']}: ratio {fmt(ro, '.6g')} -> {fmt(rn, '.6g')}; "
            f"ell(first impact) {fmt(bc['ell_first_old'] if bc else None, '.6g')} -> {fmt(bc['ell_first_new'] if bc else None, '.6g')}"
        )
    say(
        "[relativistic] all_worst_birth_margin, all_worst_reflection_margin, all_reflection_failing_share, "
        "parking_share (one-loop rows; E, from the quanta measures, not computed here)"
    )
    for k in keys:
        a, v = per[k]["a"], per[k]["vals"]
        if a["drain_model"] == "fixed":
            continue
        if per[k]["run"] == "a-edge":
            continue
        cols = (
            "all_worst_birth_margin",
            "all_worst_reflection_margin",
            "all_reflection_failing_share",
            "parking_share",
        )
        say(
            f"  {per[k]['run']:8s} M={a['M_Mp']:6s} beta={a['beta']:4s} {row_label(a):28s} "
            + "; ".join(
                f"{c.replace('all_', '')} {fmt(v[c][0], '.5g')} -> {fmt(v[c][1], '.5g')}"
                for c in cols
            )
        )

    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "effect.txt").write_text("\n".join(lines) + "\n")
    fields = []
    for r in out_rows:
        for c in r:
            if c not in fields:
                fields.append(c)
    with open(args.out / "effect.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(out_rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
