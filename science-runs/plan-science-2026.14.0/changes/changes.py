"""
What moved between the 2026.11.0 and 2026.14.0 science runs (PLAN.md P14): the exponential's main-store
histories, joined on (M, beta, law class, strength), where the law class is "fixed" or "one-loop" and the
strength is alpha for the fixed law and A for the one-loop law. The one-loop law is one-loop-measured-v1 in
2026.11.0 and one-loop-exponential-v2 here, so the law class, not the drain model's name, is the key.
Measurements, not bounds: nothing here is tested against a threshold. The power law has no predecessor run
and is not compared.

    ./venv/bin/python science-runs/<folder>/changes/changes.py

Reads:
  - ../fig6/inputs/exp/histories.csv, this run's plot_by_beta.py export of the exponential wall, its rows
    filtered by the export's model (exponential) and n (1) columns;
  - ../../2026-11-13-science-2026.11.0/fig6/inputs/histories.csv, the same export of 2026.11.0's main store
    (which has no model column: every row is the exponential at n = 1);
  - the exponential store's sidecar in ../registry/stores/, for its digest.
--inputs, --old, --sidecar and --out point it elsewhere, to test it. The export's marker rows ("no
ScalarHistory row in the store") are set aside. Needs no Ray and no store.

Writes beside this file:
  - changes.csv: one row per (M, beta, law class, strength) in either run, with each run's drain model,
    status and delivery class and, for each observable, old, new, new - old and (new - old)/|old|;
  - changes.txt: what changed between the trees; the histories whose status or delivery class changed; by
    block, the largest and median |new - old| of each observable over the histories with a value in both
    runs, at the electroweak edge and away from it; and Figure 6's tests in both runs;
  - changes.pdf, changes.png: |new - old| of T_deliver (relative), Delta Y_p and Delta(D/H) (percentage
    points of the baseline) and Delta N_eff today (relative) against beta, each block in its own colour;
  - changes.provenance.json: the script's and the inputs' SHA-256, the store's digest, the git commit and
    whether the tree was dirty, and the counts.

The delivery class is a label, not a test: "not at the wall" below 10 MeV (Figure 6's beige region), "QCD"
from 10 MeV to 10 GeV (deliveries of about 1-2 GeV), "EW" above 10 GeV (about 20-65 GeV), the 10 GeV split
being electroweak-eos verification section 2.4.5's.

Ported on 2026-11-20 from the 2026.11.0 comparison (../../2026-11-13-science-2026.11.0/changes/changes.py)
for the 2026.14.0 run (PLAN.md section 5, P14). Changed: the runs (2026.11.0 old, 2026.14.0 new); the join
key's drain model replaced by the law class; the new side filtered by model and n; the comparison with
electroweak-eos 02's 48 histories dropped, since this run did not change the w table; changes.txt opens
with what changed between the trees; the store's sidecar in the provenance; --inputs, --old, --sidecar and
--out.
"""

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
RUN = "science-2026.14.0"
OLD_RUN, NEW_RUN = "2026.11.0", "2026.14.0"
NEW = HERE.parent / "fig6" / "inputs" / "exp" / "histories.csv"
OLD = (
    HERE.parent.parent
    / "2026-11-13-science-2026.11.0"
    / "fig6"
    / "inputs"
    / "histories.csv"
)
SIDECAR = HERE.parent / "registry" / "stores" / f"{RUN}-exponential.manifest.json"
MODEL, N = "exponential", 1

# P14: the law class ("fixed" or "one-loop") and its strength, so that v1 joins v2
KEY = ["M_Mp", "beta", "law", "alpha"]
TREE_CHANGES = [
    "the exponential's one-loop law: one-loop-measured-v1 (2026.11.0) -> one-loop-exponential-v2, fitted on "
    "ln(V0/M^4) <= 10 and used beyond it below 4e-31 M_P (PLAN.md P4, by the user's ruling)",
    "one k_ex = (1/2) E^(1/2) |V'''/V''| for both potentials (power-law-wall 07b-07d, the k_ex ruling of "
    "2026-11-16)",
    "the tabulated spectrum tables (power-law-wall R8), the exponential's as well",
    "for context only, not a test: 07d measured Delta N_eff today moving by -0.36% to -6.70% on eleven "
    "histories between the trees (review-remediation verification section 11.5)",
]
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


def row_name(law: str, a: float) -> str:
    return f"alpha={a:g}" if law == "fixed" else f"A={a:g}"


def load(path: Path) -> pd.DataFrame:
    H = pd.read_csv(path, low_memory=False)
    if (
        "model" in H.columns
    ):  # 2026.11.0's export has none: every row is the exponential at n = 1
        H = H[(H.model == MODEL) & (H.n == N)]
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
    H["law"] = np.where(H.drain_model == "fixed", "fixed", "one-loop")
    return H.set_index(KEY)[
        ["drain_model", "status", "delivery", "failure_reasons", *OBSERVABLES]
    ]


def edge(beta) -> np.ndarray:
    b = np.asarray(beta)
    return (b >= EDGE[0] - 1e-9) & (b <= EDGE[1] + 1e-9)


def main():
    global NEW, OLD, SIDECAR
    parser = argparse.ArgumentParser(
        description="What moved between the 2026.11.0 and 2026.14.0 science runs (the exponential)"
    )
    parser.add_argument(
        "--inputs",
        type=Path,
        help="this run's histories.csv (default ../fig6/inputs/exp/histories.csv)",
    )
    parser.add_argument(
        "--old",
        type=Path,
        help="2026.11.0's histories.csv (default its committed copy)",
    )
    parser.add_argument(
        "--sidecar",
        type=Path,
        help="the store's sidecar (default ../registry/stores/science-2026.14.0-exponential.manifest.json)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=HERE,
        help="where to write (default beside this file)",
    )
    args = parser.parse_args()
    NEW, OLD, SIDECAR = args.inputs or NEW, args.old or OLD, args.sidecar or SIDECAR
    out_dir = args.out
    out_dir.mkdir(parents=True, exist_ok=True)

    # the tree's state before this script writes anything beside itself
    head = git("rev-parse", "HEAD")
    dirty = bool(git("status", "--porcelain"))
    old, new = load(OLD), load(NEW)
    J = old.join(new, how="outer", lsuffix="_old", rsuffix="_new")
    J = J.reset_index()
    J["block"] = J.M_Mp.map(block)
    J["row"] = [row_name(d, a) for d, a in zip(J.law, J.alpha)]
    J["status_old"] = J.status_old.fillna("not in the run")
    J["status_new"] = J.status_new.fillna("not in the run")
    for c in OBSERVABLES:
        J[f"{c}_diff"] = J[f"{c}_new"] - J[f"{c}_old"]
        J[f"{c}_rel"] = J[f"{c}_diff"] / J[f"{c}_old"].abs().replace(0.0, np.nan)
    cols = ["block", "M_Mp", "beta", "row", "law", "alpha"]
    cols += ["drain_model_old", "drain_model_new"]
    cols += ["status_old", "status_new", "delivery_old", "delivery_new"]
    for c in OBSERVABLES:
        cols += [f"{c}_old", f"{c}_new", f"{c}_diff", f"{c}_rel"]
    J = J.sort_values(["block", "M_Mp", "law", "alpha", "beta"])
    J[cols].to_csv(out_dir / "changes.csv", index=False)

    out = []
    p = out.append
    both = J[(J.status_old == "ok") & (J.status_new == "ok")]
    p(
        f"What moved between {OLD_RUN} and {NEW_RUN}, the exponential's main store (n = 1), joined on "
        "(M, beta, law class, strength)"
    )
    p(
        f"  histories: {OLD_RUN} {int((J.status_old != 'not in the run').sum())}, "
        f"{NEW_RUN} {int((J.status_new != 'not in the run').sum())}, "
        f"ok in both {len(both)}"
    )
    p("")
    p("What changed between the trees (beside these numbers, not tested by them):")
    for t in TREE_CHANGES:
        p(f"  - {t}")
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
            f"  {len(rs)} histories new in {NEW_RUN} ({b}): beta {', '.join(f'{x:g}' for x in betas)}"
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
            f"  {b}: {int(eo[both.block == b].sum())} excluded in {OLD_RUN}, {int(en[both.block == b].sum())} in "
            f"{NEW_RUN}; {int((~eo & en)[both.block == b].sum())} become excluded, "
            f"{int((eo & ~en)[both.block == b].sum())} cease to be"
        )
        for _, r in f.iterrows():
            p(
                f"      M={r.M_Mp:g} beta={r.beta:g} {r.row}: dY_p {100 * r.delta_Yp_old:.3f} -> {100 * r.delta_Yp_new:.3f}, "
                f"d(D/H) {100 * r.delta_D_over_H_old:.3f} -> {100 * r.delta_D_over_H_new:.3f}, "
                f"DNeff {r.Delta_N_eff_today_old:.3g} -> {r.Delta_N_eff_today_new:.3g}"
            )
    p("")

    (out_dir / "changes.txt").write_text("\n".join(out) + "\n")

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
        f"{NEW_RUN} against {OLD_RUN}, the exponential: every history ok in both runs "
        f"(zero drawn at {floor:g}; grey: beta {EDGE[0]}-{EDGE[1]})",
        fontsize=10,
    )
    fig.tight_layout()
    meta = {
        "Title": f"What moved, {NEW_RUN} against {OLD_RUN}",
        "Subject": f"git {head}{' (dirty)' if dirty else ''}",
    }
    fig.savefig(out_dir / "changes.pdf", metadata=meta)
    fig.savefig(out_dir / "changes.png", dpi=150)

    if SIDECAR.exists():
        sidecar = json.loads(SIDECAR.read_text())
    else:  # testing on an export whose store has no sidecar here
        print(f"!! no sidecar at {SIDECAR}; the provenance records none")
        sidecar = {"name": None, "fingerprint": {"digest": None}}

    provenance = {
        "script": "changes.py",
        "script_sha256": sha256(Path(__file__)),
        "git_head": head,
        "git_dirty": dirty,
        "inputs": {
            "new": {"path": os.path.relpath(NEW, HERE), "sha256": sha256(NEW)},
            "old": {"path": os.path.relpath(OLD, HERE), "sha256": sha256(OLD)},
        },
        "store": {
            "name": sidecar["name"],
            "store_id": sidecar.get("store_id"),
            "digest": sidecar["fingerprint"]["digest"],
        },
        "key": KEY,
        "model": MODEL,
        "n": N,
        "joined": int(len(J)),
        "in_old_only": int((J.status_new == "not in the run").sum()),
        "in_new_only": int((J.status_old == "not in the run").sum()),
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
    }
    (out_dir / "changes.provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
