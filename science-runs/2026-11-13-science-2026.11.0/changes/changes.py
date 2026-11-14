"""
What moved between the 2026.10.0 and 2026.11.0 science runs (PLAN.md P5): the main store's histories, joined on
(M, beta, row), where a row is the drain law and its strength (alpha for the fixed law, A for the one-loop law).
Measurements, not bounds: nothing here is tested against a threshold.

    ./venv/bin/python science-runs/2026-11-13-science-2026.11.0/changes/changes.py

Reads:
  - ../fig6/inputs/histories.csv, this run's plot_by_beta.py export of the main store;
  - ../../2026-11-08-science-2026.10.0/fig6/inputs/histories.csv, the same export of 2026.10.0's;
  - ../../../.documents/electroweak-eos/effect/out/effect.csv, electroweak-eos prompt 02's 48 histories built on the
    old and the new table (its verification section 2.4.5), to set this run beside them.
The export's marker rows ("no ScalarHistory row in the store") are set aside. Needs no Ray and no store.

Writes beside this file:
  - changes.csv: one row per (M, beta, row) in either run, with each run's status and delivery class and, for each
    observable, old, new, new - old and (new - old)/|old|;
  - changes.txt: the histories whose status or delivery class changed; by block, the largest and median |new - old| of
    each observable over the histories with a value in both runs, at the electroweak edge and away from it; the same
    in Figure 6's units; and this run against electroweak-eos's new side for its 48 histories;
  - changes.pdf, changes.png: |new - old| of T_deliver (relative), Delta Y_p and Delta(D/H) (percentage points of the
    baseline) and Delta N_eff today (relative) against beta, each block in its own colour;
  - changes.provenance.json: the script's and the inputs' SHA-256, the git commit and whether the tree was dirty, and
    the counts.

The delivery class is a label, not a test: "not at the wall" below 10 MeV (Figure 6's beige region), "QCD" from
10 MeV to 10 GeV (deliveries of about 1-2 GeV), "EW" above 10 GeV (about 20-65 GeV), the 10 GeV split being
electroweak-eos verification section 2.4.5's.
"""

import hashlib
import json
import subprocess
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
NEW = HERE.parent / "fig6" / "inputs" / "histories.csv"
OLD = (
    HERE.parent.parent
    / "2026-11-08-science-2026.10.0"
    / "fig6"
    / "inputs"
    / "histories.csv"
)
EFFECT = (
    HERE.parent.parent.parent
    / ".documents"
    / "electroweak-eos"
    / "effect"
    / "out"
    / "effect.csv"
)

KEY = ["M_Mp", "beta", "drain_model", "alpha"]
OBSERVABLES = [
    "T_deliver_GeV",
    "Yp_BBN",
    "D_over_H",
    "delta_Yp",
    "delta_D_over_H",
    "Delta_N_eff_today",
    "Delta_N_eff_1MeV",
    "parking_T_GeV",
    "log_R_first_impact",
    "reflections",
    "tuning_iterations",
]
T_WALL_GEV = 1e-2
T_EW_GEV = 10.0
EDGE = (
    2.95,
    3.04,
)  # the dense grid's fine segment, where the electroweak capture moved
BLOCK_COLOUR = {"A": "black", "B": "#2a9d8f", "C": "#d1495b", "D": "#e9a23b"}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args) -> str:
    return subprocess.run(
        ["git", *args], cwd=HERE, capture_output=True, text=True, check=True
    ).stdout.strip()


def block(M: float) -> str:
    if np.isclose(M, 4e-31, rtol=1e-9, atol=0.0):
        return "A"
    if M in (1e-15, 1e-12):
        return "C"
    if M in (1e-3, 0.1, 0.5):
        return "D"
    return "B"


def row_name(dm: str, a: float) -> str:
    return f"alpha={a:g}" if dm == "fixed" else f"A={a:g}"


def load(path: Path) -> pd.DataFrame:
    H = pd.read_csv(path, low_memory=False)
    H = H[~H.failure_reasons.fillna("").str.startswith("no ScalarHistory")].copy()
    ok = H.failure_reasons.isna() & H.Yp_BBN.notna()
    H["status"] = np.where(
        ok,
        "ok",
        "failed: "
        + H.failure_reasons.fillna("no BBN result")
        .str.split()
        .str[:3]
        .str.join(" ")
        .str.replace(r"=.*$", "", regex=True),
    )
    T = H.T_deliver_GeV
    H["delivery"] = np.select(
        [T.isna(), T < T_WALL_GEV, T < T_EW_GEV],
        ["none", "not at the wall", "QCD"],
        default="EW",
    )
    return H.set_index(KEY)[["status", "delivery", "failure_reasons", *OBSERVABLES]]


def edge(beta) -> np.ndarray:
    b = np.asarray(beta)
    return (b >= EDGE[0] - 1e-9) & (b <= EDGE[1] + 1e-9)


def main():
    # the tree's state before this script writes anything beside itself
    head = git("rev-parse", "HEAD")
    dirty = bool(git("status", "--porcelain"))
    old, new = load(OLD), load(NEW)
    J = old.join(new, how="outer", lsuffix="_old", rsuffix="_new")
    J = J.reset_index()
    J["block"] = J.M_Mp.map(block)
    J["row"] = [row_name(d, a) for d, a in zip(J.drain_model, J.alpha)]
    J["status_old"] = J.status_old.fillna("not in the run")
    J["status_new"] = J.status_new.fillna("not in the run")
    for c in OBSERVABLES:
        J[f"{c}_diff"] = J[f"{c}_new"] - J[f"{c}_old"]
        J[f"{c}_rel"] = J[f"{c}_diff"] / J[f"{c}_old"].abs().replace(0.0, np.nan)
    cols = ["block", "M_Mp", "beta", "row", "drain_model", "alpha"]
    cols += ["status_old", "status_new", "delivery_old", "delivery_new"]
    for c in OBSERVABLES:
        cols += [f"{c}_old", f"{c}_new", f"{c}_diff", f"{c}_rel"]
    J = J.sort_values(["block", "M_Mp", "drain_model", "alpha", "beta"])
    J[cols].to_csv(HERE / "changes.csv", index=False)

    out = []
    p = out.append
    both = J[(J.status_old == "ok") & (J.status_new == "ok")]
    p(
        "What moved between 2026.10.0 and 2026.11.0, the main store, joined on (M, beta, row)"
    )
    p(
        f"  histories: 2026.10.0 {int((J.status_old != 'not in the run').sum())}, "
        f"2026.11.0 {int((J.status_new != 'not in the run').sum())}, "
        f"ok in both {len(both)}"
    )
    p("")

    p("Status changed (either run failed, and not the same way):")
    st = J[J.status_old != J.status_new]
    counts = {}
    for _, r in st.iterrows():
        k = (r.status_old, r.status_new)
        if r.status_old == "not in the run":
            counts.setdefault(("new history", r.status_new), []).append(r)
            continue
        p(
            f"  {r.block} M={r.M_Mp:g} beta={r.beta:g} {r.row}: {r.status_old} -> {r.status_new}"
        )
    for (a, b), rs in counts.items():
        betas = sorted({r.beta for r in rs})
        p(
            f"  {len(rs)} histories new in 2026.11.0 ({b}): beta {', '.join(f'{x:g}' for x in betas)}"
        )
    p("")

    p(
        f"Delivery class changed (T_deliver across {T_WALL_GEV:g} GeV or {T_EW_GEV:g} GeV):"
    )
    dc = J[
        (J.delivery_old.notna())
        & (J.delivery_new.notna())
        & (J.delivery_old != J.delivery_new)
    ]
    grp = dc.groupby(["block", "beta", "delivery_old", "delivery_new"])
    for (b, beta, d0, d1), g in grp:
        Ms = sorted(set(g.M_Mp), reverse=True)
        p(
            f"  {b} beta={beta:g}: {d0} -> {d1}, {len(g)} histories (M {', '.join(f'{m:g}' for m in Ms)}; "
            f"rows {', '.join(sorted(set(g.row)))}); T_deliver {g.T_deliver_GeV_old.min():.4g}-"
            f"{g.T_deliver_GeV_old.max():.4g} -> {g.T_deliver_GeV_new.min():.4g}-{g.T_deliver_GeV_new.max():.4g} GeV"
        )
    p("")

    def table(S, title):
        p(title)
        p(
            "  observable: largest |new - old| / median, by block (relative where marked)"
        )
        for c, kind in [
            ("T_deliver_GeV", "rel"),
            ("delta_Yp", "pp"),
            ("delta_D_over_H", "pp"),
            ("Delta_N_eff_today", "abs"),
            ("Delta_N_eff_today", "rel"),
            ("Delta_N_eff_1MeV", "rel"),
            ("parking_T_GeV", "rel"),
            ("log_R_first_impact", "abs"),
            ("reflections", "abs"),
            ("tuning_iterations", "abs"),
        ]:
            cells = []
            for b in "ABCD":
                g = S[S.block == b]
                v = g[f"{c}_rel"] if kind == "rel" else g[f"{c}_diff"]
                v = (100 * v if kind == "pp" else v).abs().dropna()
                if len(v):
                    w = g.loc[v.idxmax()]
                    cells.append(
                        f"{b} {v.max():.3g}/{v.median():.3g} (n={len(v)}; largest at "
                        f"M={w.M_Mp:g} beta={w.beta:g} {w.row})"
                    )
                else:
                    cells.append(f"{b} -")
            p(f"  {c} ({kind}): " + "; ".join(cells))
        p("")

    table(
        both[edge(both.beta)],
        f"At the edge (beta {EDGE[0]}-{EDGE[1]}), ok in both runs:",
    )
    table(both[~edge(both.beta)], "Away from the edge, ok in both runs:")

    p(
        "Figure 6's tests (|dY_p| > 2.449 pp, |d(D/H)| > 2.374 pp, Delta N_eff today > 0.30), ok in both runs:"
    )
    tol = {"delta_Yp": 2 * 0.003 / 0.245, "delta_D_over_H": 2 * 0.030 / 2.527}

    def excluded(s):
        return (
            (both[f"delta_Yp_{s}"].abs() > tol["delta_Yp"])
            | (both[f"delta_D_over_H_{s}"].abs() > tol["delta_D_over_H"])
            | (both[f"Delta_N_eff_today_{s}"] > 0.30)
        )

    eo, en = excluded("old"), excluded("new")
    flips = both[eo != en]
    for b in "ABCD":
        f = flips[flips.block == b]
        p(
            f"  {b}: {int(eo[both.block == b].sum())} excluded in 2026.10.0, {int(en[both.block == b].sum())} in "
            f"2026.11.0; {int((~eo & en)[both.block == b].sum())} become excluded, "
            f"{int((eo & ~en)[both.block == b].sum())} cease to be"
        )
        for _, r in f.iterrows():
            p(
                f"      M={r.M_Mp:g} beta={r.beta:g} {r.row}: dY_p {100 * r.delta_Yp_old:.3f} -> {100 * r.delta_Yp_new:.3f}, "
                f"d(D/H) {100 * r.delta_D_over_H_old:.3f} -> {100 * r.delta_D_over_H_new:.3f}, "
                f"DNeff {r.Delta_N_eff_today_old:.3g} -> {r.Delta_N_eff_today_new:.3g}"
            )
    p("")

    # beside electroweak-eos verification section 2.4.5
    E = pd.read_csv(EFFECT)
    E["drain_model"] = E.drain_model.fillna("fixed")
    Ei = E.set_index(KEY)
    Ni = new.copy()
    common = Ei.index.intersection(Ni.index)
    p(
        f"This run against electroweak-eos 02's new side, its {len(E)} histories ({len(common)} matched; "
        "the rest are not at an M of the main store):"
    )
    agree = {}
    for c in OBSERVABLES:
        if f"{c}_new" not in Ei.columns:
            continue
        a = Ei.loc[common, f"{c}_new"].astype(float)
        b = Ni.loc[common, c].astype(float)
        d = (b - a).abs() / a.abs().replace(0.0, np.nan)
        same = int(((a == b) | (a.isna() & b.isna())).sum())
        agree[c] = {
            "identical": same,
            "max_rel": None if d.dropna().empty else float(d.max()),
        }
        p(
            f"  {c}: identical in {same} of {len(common)}; largest relative difference "
            f"{'-' if d.dropna().empty else f'{d.max():.3g}'}"
        )
    p("")
    (HERE / "changes.txt").write_text("\n".join(out) + "\n")

    # the figure
    rows = [
        ("T_deliver_GeV_rel", r"$|\Delta T_{\rm deliver}| / T_{\rm deliver}$", 1.0),
        ("delta_Yp_diff", r"$|\Delta(\Delta Y_p)|$ [pp]", 100.0),
        ("delta_D_over_H_diff", r"$|\Delta(\Delta\,\mathrm{D/H})|$ [pp]", 100.0),
        (
            "Delta_N_eff_today_rel",
            r"$|\Delta(\Delta N_{\rm eff})| / \Delta N_{\rm eff}$",
            1.0,
        ),
    ]
    floor = 1e-12
    fig, axes = plt.subplots(len(rows), 1, figsize=(9.0, 10.0), sharex=True)
    for A, (c, label, scale) in zip(axes, rows):
        for b, colour in BLOCK_COLOUR.items():
            g = both[both.block == b]
            y = (scale * g[c]).abs().clip(lower=floor)
            A.scatter(g.beta, y, s=6, color=colour, alpha=0.6, lw=0, label=f"block {b}")
        A.set_yscale("log")
        A.set_ylim(floor / 3, None)
        A.axvspan(*EDGE, color="0.9", zorder=0, lw=0)
        A.set_ylabel(label)
        A.grid(alpha=0.3)
    axes[0].legend(loc="upper left", fontsize=8, markerscale=3, ncol=4)
    axes[-1].set_xscale("log")
    axes[-1].set_xlabel(r"$\beta$")
    axes[-1].set_xticks([0.5, 1, 1.5, 2, 3, 5, 10, 20])
    axes[-1].set_xticklabels(["0.5", "1", "1.5", "2", "3", "5", "10", "20"])
    fig.suptitle(
        "2026.11.0 against 2026.10.0: every history ok in both runs "
        f"(zero drawn at {floor:g}; grey: beta {EDGE[0]}-{EDGE[1]})",
        fontsize=10,
    )
    fig.tight_layout()
    meta = {
        "Title": "What moved, 2026.11.0 against 2026.10.0",
        "Subject": f"git {head}{' (dirty)' if dirty else ''}",
    }
    fig.savefig(HERE / "changes.pdf", metadata=meta)
    fig.savefig(HERE / "changes.png", dpi=150)

    provenance = {
        "script": "changes.py",
        "script_sha256": sha256(Path(__file__)),
        "git_head": head,
        "git_dirty": dirty,
        "inputs": {
            "new": {"path": "../fig6/inputs/histories.csv", "sha256": sha256(NEW)},
            "old": {
                "path": "../../2026-11-08-science-2026.10.0/fig6/inputs/histories.csv",
                "sha256": sha256(OLD),
            },
            "effect": {
                "path": "../../../.documents/electroweak-eos/effect/out/effect.csv",
                "sha256": sha256(EFFECT),
            },
        },
        "histories_ok_in_both": int(len(both)),
        "status_changed": int((st.status_old != "not in the run").sum()),
        "delivery_class_changed": int(len(dc)),
        "figure6_tests_flips": {
            b: {
                "become_excluded": int((~eo & en)[both.block == b].sum()),
                "cease_to_be": int((eo & ~en)[both.block == b].sum()),
            }
            for b in "ABCD"
        },
        "beside_electroweak_eos": agree,
    }
    (HERE / "changes.provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
