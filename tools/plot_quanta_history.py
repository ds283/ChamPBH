# (c) University of Sussex 2026
# Created by David Seery
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
The per-history figure (one-loop-drain prompt 07 §1.3; README §8.15 P58, P60; orchestrator note 07): six panels sharing the
e-fold axis N, for one one-loop history, from a plot run's exports alone. No store is opened and no Ray is started.

    PYTHONPATH=. ./venv/bin/python tools/plot_quanta_history.py ROOT [--A-values A ...] [--beta-values B ...] \\
        [--M-Mp-values M ...] [--assoc-scale C] [--output DIR]

ROOT is a plot run's `<output>/<label>/`. One figure per drawn one-loop history (quanta_figures.classify) that has its quanta
files at the scale. With no selector, every one; with selectors, those whose `alpha`, `beta` and `M_Mp` cells match one value
of each given list (`math.isclose(rel_tol=1e-9)`); a value that matches no drawn one-loop row is a usage error (exit 2).

**The files** of a history, from its directory (quanta_measures.history_dir): fields.csv, bounces.csv, quanta_bounces<suffix>.csv
and quanta_samples<suffix>.csv, and the history's row of quanta_histories<suffix>.csv, joined on the five key cells as
strings. If quanta_histories<suffix>.csv fails the model check (or cannot be read), every history is refused. A history
with neither quanta file is skipped and counted (`no quanta files`; 05 refused it or it failed 05's K check); with one only,
with a file failing the model check, with no quanta_histories row, with quanta_bounces.csv not holding bounces.csv's `ordinal`s
(and `source`s) in order, with quanta_samples.csv not holding fields.csv's `raw_N` row by row, with `raw_N` not ascending, or
whose generations are not quanta_histories.csv's `generations`, it is refused (exit 1) and nothing is written for it.

**Panels**, top to bottom, sharing N (one shared x-axis over the six axes, panel 3's twin included):
1. ln(m a / M_P) for the field's and the tracker's mass (`log_m_*_Mp` + `raw_N`) as lines; each generation's `log_q_Mp` at its
   `N_turn` as a point coloured by regime (a generation is a candidate row with `delta_K_Mp4` > 0 and a `log_q_Mp`); the
   `plateau` and `wall` events as ticks along the bottom;
2. each candidate's `alpha_eff` at `N` (filled) and the drained fraction dK/(K_before + dK) (open), coloured by regime, log10;
3. K(N)/K_final and `parking_fraction` (quanta_samples.csv), and r_chi on a twin axis, log10;
4. E_field, E_tracker, r_chi E_field, r_chi E_tracker, log10;
5. n_g = exp(`ln_n_g`), `n_bar` and `backreaction_ratio` at the bounce's `N`, log10;
6. `phase_slip` and `k_ex_over_H` per assessed candidate, log10, with a line at 1 rad (the reference 05's
   `phase_slips_below_1` counts against).
A zero or blank is not drawn on a log10 axis; the zeros are counted (footer and printed line), and on panel 2 the candidates
that cannot be drawn are counted by cause: dK = 0 (the fraction), `alpha_eff` zero or blank, `K_before_Mp4` blank. If K_final is
0 the K curve is not drawn and the footer says so. A negative value on a log10 panel, a regime other than the four, or an
`ln_n_g` above 709 or below -745 is a defect of the input and refuses the history. Panels 1, 2 and 5 are rasterised inside
the PDF.

**Written** to `<row>/plots/<history_directory>/quanta_history<suffix>.pdf` and `.png` (150 dpi), below --output if given.
**Exit status:** 0 if every history asked for is written (or skipped as having no quanta files); 1 if a history was refused,
the rest written; 2 on a usage error or an unreadable root.
"""

import argparse
import csv
import sys
import textwrap
import time
from math import exp, isfinite
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Sequence, Tuple

import numpy as np
from matplotlib.figure import Figure
from matplotlib.lines import Line2D

from ComputeTargets import quanta_spectrum as qs
from Units import Planck_units
from tools import quanta_figures as qf
from tools import quanta_measures as qm

LN_N_G_MAX = 709.0
LN_N_G_MIN = -745.0

LINE_COLOURS = {
    "field": "#0072B2",
    "tracker": "#D55E00",
    "K": "#000000",
    "parking": "#56B4E9",
    "r_chi": "#999999",
}
EVENT_COLOURS = {"plateau": "#888888", "wall": "#222222"}


class HistoryData(NamedTuple):
    """Everything one history's figure draws, parsed and checked."""

    history: dict  # the histories.csv row
    quanta: dict  # the quanta_histories row
    fields: Sequence[dict]
    bounces: Sequence[dict]
    quanta_bounces: Sequence[dict]
    quanta_samples: Sequence[dict]
    scale: float
    model: str


def column(rows, name):
    return qf.column(rows, name)


def positive_log(values: np.ndarray, label: str) -> Tuple[np.ndarray, int]:
    """`values` with the non-positive entries replaced by nan, and the number of zeros removed; a negative entry is a defect
    of the input (ruling 4)."""
    v = np.array(values, dtype=float)
    finite = np.isfinite(v)
    if np.any(v[finite] < 0.0):
        raise qf.InputDefect(f"{label}: a negative value on a log10 panel")
    zeros = int(np.count_nonzero(v[finite] == 0.0))
    v[~(v > 0.0)] = np.nan
    return v, zeros


def candidates_of(bounces: Sequence[dict]) -> List[dict]:
    out = []
    for b in bounces:
        if b["source"] == "candidate":
            if b["regime"] not in qf.REGIMES:
                raise qf.FigureRefused(
                    f"candidate row {b['ordinal']} has regime {b['regime']!r}, none of {', '.join(qf.REGIMES)}"
                )
            out.append(b)
    return out


def is_generation(b: dict) -> bool:
    """05's definition: delta_K_Mp4 > 0 and a log_q_Mp."""
    return (
        b["delta_K_Mp4"] != "" and float(b["delta_K_Mp4"]) > 0.0 and b["log_q_Mp"] != ""
    )


def check_history(data: HistoryData) -> None:
    """The joins and orders of §1.3; FigureRefused (nothing is drawn) if one does not hold."""
    b, qb = data.bounces, data.quanta_bounces
    if len(b) != len(qb):
        raise qf.FigureRefused(
            f"quanta_bounces has {len(qb)} rows, bounces.csv {len(b)}"
        )
    for k, (x, y) in enumerate(zip(b, qb)):
        if int(x["ordinal"]) != int(y["ordinal"]) or x["source"] != y["source"]:
            raise qf.FigureRefused(
                f"quanta_bounces row {k + 2} is ordinal {y['ordinal']} ({y['source']}), bounces.csv's is {x['ordinal']} ({x['source']})"
            )
    f, qsmp = data.fields, data.quanta_samples
    if len(f) != len(qsmp):
        raise qf.FigureRefused(
            f"quanta_samples has {len(qsmp)} rows, fields.csv {len(f)}"
        )
    raw = column(f, "raw_N")
    raw_q = column(qsmp, "raw_N")
    if not np.array_equal(raw, raw_q):
        k = int(np.nonzero(raw != raw_q)[0][0])
        raise qf.FigureRefused(
            f"quanta_samples row {k + 2} has raw_N {qsmp[k]['raw_N']}, fields.csv's is {f[k]['raw_N']}"
        )
    if len(raw) > 1 and not np.all(np.diff(raw) > 0.0):
        raise qf.FigureRefused("raw_N is not ascending in quanta_samples.csv")
    candidates_of(b)
    gens = sum(1 for x in b if x["source"] == "candidate" and is_generation(x))
    stated = data.quanta["generations"]
    if stated == "" or int(stated) != gens:
        raise qf.FigureRefused(
            f"bounces.csv has {gens} generations, quanta_histories.csv states {stated!r}"
        )


def build_figure(data: HistoryData) -> Figure:
    """
    The six-panel figure. The returned Figure carries `quanta_counts`: generations, bounces, and the counts of panel 2's
    drawn and undrawn candidates and of the zeros not drawn, by name (see the code).
    """
    check_history(data)
    fig = Figure(figsize=(11, 17))
    axes = fig.subplots(6, 1, sharex=True)
    ax1, ax2, ax3, ax4, ax5, ax6 = axes
    for k, ax in enumerate(axes):
        ax.set_gid(f"panel-{k + 1}")
    f, qsmp = data.fields, data.quanta_samples
    N = column(f, "raw_N")
    cand = candidates_of(data.bounces)
    events = [b for b in data.bounces if b["source"] != "candidate"]
    qb_by_ordinal = {int(r["ordinal"]): r for r in data.quanta_bounces}
    counts: Dict[str, int] = {}

    # --- 1: m a against the generations' q -----------------------------------------------------------------------------
    ax1.plot(
        N,
        column(f, "log_m_eff_Mp") + N,
        color=LINE_COLOURS["field"],
        lw=1.0,
        label="field: ln(m_eff a / M_P)",
        gid="m-field",
    )
    ax1.plot(
        N,
        column(f, "log_m_tracker_Mp") + N,
        color=LINE_COLOURS["tracker"],
        lw=1.0,
        label="tracker: ln(m_tracker a / M_P)",
        gid="m-tracker",
    )
    gens = [c for c in cand if is_generation(c)]
    for regime in qf.REGIMES:
        g = [c for c in gens if c["regime"] == regime]
        sc = ax1.scatter(
            column(g, "N_turn"),
            column(g, "log_q_Mp"),
            s=14,
            color=qf.REGIME_COLOURS[regime],
            rasterized=True,
            zorder=3,
        )
        sc.set_gid(f"generation-{regime}")
    for source, colour in EVENT_COLOURS.items():
        ev = column([e for e in events if e["source"] == source], "N")
        if len(ev):
            lc = ax1.vlines(
                ev,
                0.0,
                0.04,
                transform=ax1.get_xaxis_transform(),
                color=colour,
                lw=1.0,
                label=f"{source} event",
            )
            lc.set_gid(f"event-{source}")
    ax1.set_ylabel("ln(m a), ln(q)  [M_P]", fontsize=8)
    ax1.legend(fontsize=7, loc="best")

    # --- 2: the drain ---------------------------------------------------------------------------------------------------
    alpha = column(cand, "alpha_eff")
    dK = np.nan_to_num(column(cand, "delta_K_Mp4"), nan=0.0)
    Kb = column(cand, "K_before_Mp4")
    Nc = column(cand, "N")
    regimes = np.array([c["regime"] for c in cand])
    alpha_ok = np.isfinite(alpha) & (alpha > 0.0)
    if np.any(np.isfinite(alpha) & (alpha < 0.0)):
        raise qf.InputDefect("alpha_eff: a negative value on a log10 panel")
    counts["alpha_drawn"] = int(np.count_nonzero(alpha_ok))
    counts["alpha_undrawn"] = len(cand) - counts["alpha_drawn"]
    frac_dK_zero = dK <= 0.0
    frac_K_blank = ~frac_dK_zero & ~np.isfinite(Kb)
    with np.errstate(divide="ignore", invalid="ignore"):
        frac = dK / (Kb + dK)
    frac_ok = ~frac_dK_zero & ~frac_K_blank & (frac > 0.0)
    if np.any(dK < 0.0):
        raise qf.InputDefect("delta_K_Mp4: a negative value on a log10 panel")
    counts["fraction_drawn"] = int(np.count_nonzero(frac_ok))
    counts["undrawn_dK_zero"] = int(np.count_nonzero(frac_dK_zero))
    counts["undrawn_alpha_zero_or_blank"] = counts["alpha_undrawn"]
    counts["undrawn_K_before_blank"] = int(np.count_nonzero(frac_K_blank))
    for regime in qf.REGIMES:
        sel = regimes == regime
        colour = qf.REGIME_COLOURS[regime]
        sc = ax2.scatter(
            Nc[sel & alpha_ok],
            alpha[sel & alpha_ok],
            s=14,
            color=colour,
            rasterized=True,
            zorder=3,
        )
        sc.set_gid(f"alpha-{regime}")
        sf = ax2.scatter(
            Nc[sel & frac_ok],
            frac[sel & frac_ok],
            s=22,
            facecolors="none",
            edgecolors=colour,
            linewidths=0.8,
            rasterized=True,
            zorder=3,
        )
        sf.set_gid(f"fraction-{regime}")
    ax2.set_yscale("log")
    ax2.set_ylabel("alpha_eff, dK/K_after  [dimensionless]", fontsize=8)
    ax2.legend(
        [
            Line2D([], [], marker="o", linestyle="none", color="0.3"),
            Line2D([], [], marker="o", linestyle="none", mfc="none", color="0.3"),
        ],
        ["alpha_eff at N", "dK / (K_before + dK)"],
        fontsize=7,
        loc="best",
    )

    # --- 3: the dark radiation ----------------------------------------------------------------------------------------
    K = column(qsmp, "K_Mp4")
    K_final = float(K[-1]) if len(K) else 0.0
    counts["K_final_zero"] = int(K_final == 0.0)
    zero_counts: Dict[str, int] = {}
    if K_final > 0.0:
        Kr, zero_counts["K/K_final"] = positive_log(K / K_final, "K/K_final")
        ax3.plot(
            N, Kr, color=LINE_COLOURS["K"], lw=1.0, label="K(N) / K_final", gid="K"
        )
    pf, zero_counts["parking_fraction"] = positive_log(
        column(qsmp, "parking_fraction"), "parking_fraction"
    )
    ax3.plot(
        N,
        pf,
        color=LINE_COLOURS["parking"],
        lw=1.0,
        label="parking fraction",
        gid="parking",
    )
    ax3.set_yscale("log")
    ax3.set_ylabel("K/K_final, parking frac.  [dimensionless]", fontsize=8)
    ax3t = ax3.twinx()
    ax3t.set_gid("panel-3-twin")
    r, zero_counts["r_chi"] = positive_log(column(f, "rho_chi_over_rho_rad"), "r_chi")
    ax3t.plot(
        N,
        r,
        color=LINE_COLOURS["r_chi"],
        lw=1.0,
        ls="--",
        label="r_chi (right)",
        gid="r_chi",
    )
    ax3t.set_yscale("log")
    ax3t.set_ylabel("r_chi  [dimensionless]", fontsize=8)
    h1, l1 = ax3.get_legend_handles_labels()
    h2, l2 = ax3t.get_legend_handles_labels()
    ax3.legend(h1 + h2, l1 + l2, fontsize=7, loc="best")

    # --- 4: the excess --------------------------------------------------------------------------------------------------
    for name, label, colour, ls in (
        ("E_field", "E_field", LINE_COLOURS["field"], "-"),
        ("E_tracker", "E_tracker", LINE_COLOURS["tracker"], "-"),
        ("r_chi_E_field", "r_chi E_field", LINE_COLOURS["field"], ":"),
        ("r_chi_E_tracker", "r_chi E_tracker", LINE_COLOURS["tracker"], ":"),
    ):
        y, zero_counts[name] = positive_log(column(qsmp, name), name)
        ax4.plot(N, y, color=colour, ls=ls, lw=1.0, label=label, gid=name)
    ax4.set_yscale("log")
    ax4.set_ylabel("E, r_chi E  [dimensionless]", fontsize=8)
    ax4.legend(fontsize=7, loc="best", ncol=2)

    # --- 5: the occupations ----------------------------------------------------------------------------------------------
    qb = [qb_by_ordinal[int(c["ordinal"])] for c in cand]
    Nq = column(qb, "N")
    ln_n = column(qb, "ln_n_g")
    ok = np.isfinite(ln_n)
    if np.any((ln_n[ok] > LN_N_G_MAX) | (ln_n[ok] < LN_N_G_MIN)):
        raise qf.InputDefect(
            f"ln_n_g runs from {float(np.min(ln_n[ok])):.4g} to {float(np.max(ln_n[ok])):.4g}, outside [{LN_N_G_MIN:g}, {LN_N_G_MAX:g}]"
        )
    n_g = np.full_like(ln_n, np.nan)
    n_g[ok] = np.exp(ln_n[ok])
    n_bar, zero_counts["n_bar"] = positive_log(column(qb, "n_bar"), "n_bar")
    back, zero_counts["backreaction_ratio"] = positive_log(
        column(qb, "backreaction_ratio"), "backreaction_ratio"
    )
    for name, y, colour, marker in (
        ("n_g", n_g, "#222222", "o"),
        ("n_bar", n_bar, "#a50f15", "s"),
        ("backreaction_ratio", back, "#6a3d9a", "^"),
    ):
        sc = ax5.scatter(
            Nq,
            y,
            s=14,
            color=colour,
            marker=marker,
            label=name,
            rasterized=True,
        )
        sc.set_gid(name)
    ax5.set_yscale("log")
    ax5.set_ylabel("n_g, n_bar, backreaction  [dimensionless]", fontsize=8)
    ax5.legend(fontsize=7, loc="best")

    # --- 6: the phase slip ------------------------------------------------------------------------------------------------
    assessed = [(c, q) for c, q in zip(cand, qb) if c["log_k_ex_Mp"] != ""]
    Na = column([q for _, q in assessed], "N")
    slip, zero_counts["phase_slip"] = positive_log(
        column([q for _, q in assessed], "phase_slip"), "phase_slip"
    )
    kH, zero_counts["k_ex_over_H"] = positive_log(
        column([q for _, q in assessed], "k_ex_over_H"), "k_ex_over_H"
    )
    s1 = ax6.scatter(Na, slip, s=14, color="#222222", label="phase slip [rad]")
    s1.set_gid("phase_slip")
    s2 = ax6.scatter(Na, kH, s=14, color="#6a3d9a", marker="s", label="k_ex / H")
    s2.set_gid("k_ex_over_H")
    ref = ax6.axhline(
        1.0,
        color="k",
        ls="--",
        lw=1.0,
        label="1 rad (the reference of phase_slips_below_1)",
    )
    ref.set_gid("one-rad")
    ax6.set_yscale("log")
    shown = np.concatenate([slip[np.isfinite(slip)], kH[np.isfinite(kH)], [1.0]])
    ax6.set_ylim(float(shown.min()) / 3.0, float(shown.max()) * 3.0)
    ax6.set_ylabel("phase slip [rad], k_ex/H [-]", fontsize=8)
    ax6.set_xlabel("N [e-folds]")
    ax6.legend(fontsize=7, loc="best")

    # --- the legend of the regimes, once -----------------------------------------------------------------------------------
    handles = [
        Line2D([], [], marker="o", linestyle="none", color=qf.REGIME_COLOURS[r])
        for r in qf.REGIMES
    ]
    leg = fig.legend(
        handles,
        [qf.REGIME_LABELS[r] for r in qf.REGIMES],
        loc="upper right",
        ncol=4,
        fontsize=8,
        frameon=False,
        title="regime",
        title_fontsize=8,
    )
    leg.set_gid("regimes")

    counts["generations"] = len(gens)
    counts["bounces"] = len(data.bounces)
    counts["zeros"] = zero_counts
    h = data.history
    q = data.quanta
    footer = (
        f"{qm.row_label(h)}   spectrum model: {data.model}, c_a = {data.scale:g}\n"
        f"generations {len(gens)}, bounce rows {len(data.bounces)}; delta_stim {q['delta_stim']}; "
        f"drained shares: one-loop {h['drained_share_one_loop']}, gap {h['drained_share_gap']}, classical {h['drained_share_classical']}, "
        f"unassessed {h['drained_share_unassessed']}; parking share {h['parking_share']}\n"
        f"panel 2: alpha_eff drawn {counts['alpha_drawn']}, undrawn (zero or blank) {counts['undrawn_alpha_zero_or_blank']}; "
        f"fraction drawn {counts['fraction_drawn']}, undrawn: dK = 0 {counts['undrawn_dK_zero']}, K_before blank {counts['undrawn_K_before_blank']}"
        + ("; K_final = 0, so the K curve is not drawn" if K_final == 0.0 else "")
        + "\nzeros not drawn: "
        + ", ".join(f"{k} {v}" for k, v in zero_counts.items())
    )
    fig.suptitle(qm.row_label(h), fontsize=10, x=0.02, ha="left")
    footer = "\n".join(
        textwrap.fill(line, width=170, subsequent_indent="    ")
        for line in footer.split("\n")
    )
    fig.text(0.02, 0.003, footer, fontsize=7, va="bottom", ha="left")
    fig.tight_layout(rect=(0.0, 0.085, 1.0, 0.975))
    fig.quanta_counts = counts
    return fig


def figure_line(h: dict, fig: Figure, written: Sequence[Path], wall: float) -> str:
    c = fig.quanta_counts
    z = "; ".join(f"{k} {v}" for k, v in c["zeros"].items() if v)
    return (
        f"{qm.row_label(h)}: generations {c['generations']}; bounce rows {c['bounces']}; "
        f"alpha_eff drawn {c['alpha_drawn']}, undrawn {c['undrawn_alpha_zero_or_blank']}; "
        f"fraction drawn {c['fraction_drawn']}, undrawn dK=0 {c['undrawn_dK_zero']}, K_before blank {c['undrawn_K_before_blank']}; "
        f"zeros not drawn [{z or 'none'}]; wrote {' '.join(str(p) for p in written)}; wall {wall:.2f} s"
    )


def select(
    drawn: Sequence[dict],
    A_values: Optional[Sequence[float]],
    beta_values: Optional[Sequence[float]],
    M_values: Optional[Sequence[float]],
) -> Tuple[List[dict], Optional[str]]:
    """The drawn one-loop rows that match one of each given list, and a message for the first value matching nothing."""
    for name, values, col in (
        ("--A-values", A_values, "alpha"),
        ("--beta-values", beta_values, "beta"),
        ("--M-Mp-values", M_values, "M_Mp"),
    ):
        for v in values or ():
            if not any(qf.is_close(v, r[col]) for r in drawn):
                return [], f"{name} {v:g} matches no drawn one-loop row"
    out = []
    for r in drawn:
        if all(
            not values or any(qf.is_close(v, r[col]) for v in values)
            for values, col in (
                (A_values, "alpha"),
                (beta_values, "beta"),
                (M_values, "M_Mp"),
            )
        ):
            out.append(r)
    return out, None


def run(
    root: Path,
    A_values: Optional[Sequence[float]] = None,
    beta_values: Optional[Sequence[float]] = None,
    M_values: Optional[Sequence[float]] = None,
    assoc_scale: float = qs.DEFAULT_ASSOC_SCALE,
    output: Optional[Path] = None,
    units=None,
    out=None,
) -> int:
    """Draw every selected history; print the lines. Returns the exit status."""
    out = sys.stdout if out is None else out
    units = Planck_units() if units is None else units
    root = Path(root)
    try:
        rows = qf.read_histories(root)
    except (OSError, csv.Error, UnicodeDecodeError) as e:
        print(
            f"!! plot_quanta_history: cannot read {root / 'histories.csv'}: {e}",
            file=sys.stderr,
        )
        return 2
    by_class = qf.walk(rows)
    drawn = by_class[qf.DRAWN]
    selected, problem = select(drawn, A_values, beta_values, M_values)
    if problem:
        print(f"!! plot_quanta_history: {problem}", file=sys.stderr)
        return 2
    tag = qm.suffix(assoc_scale)
    t_start = time.perf_counter()
    counts = dict(written=0, refused=0, no_quanta_files=0)
    print(
        f"plot_quanta_history: histories.csv rows: fixed {len(by_class[qf.FIXED])}, marker {len(by_class[qf.MARKER])}, "
        f"failed {len(by_class[qf.FAILED])}, drawn {len(drawn)}; selected {len(selected)}",
        file=out,
    )
    quanta_path = root / f"quanta_histories{tag}.csv"
    qkeys: Dict[Tuple[str, ...], dict] = {}
    qproblem = None
    try:
        for q in qf.read_checked(quanta_path, assoc_scale):
            k = qf.key_of(q)
            if k in qkeys:
                raise qf.FigureRefused(f"{quanta_path.name}: two rows with the key {k}")
            qkeys[k] = q
    except qf.FigureRefused as e:
        qproblem = str(e)
    for h in selected:
        label = qm.row_label(h)
        if qproblem is not None:
            counts["refused"] += 1
            print(f"REFUSED {label}: {qproblem}", file=out)
            continue
        try:
            d = qm.history_dir(root, h, units)
        except qm.Refused as e:
            counts["refused"] += 1
            print(f"REFUSED {label}: {e}", file=out)
            continue
        names = (f"quanta_bounces{tag}.csv", f"quanta_samples{tag}.csv")
        present = [(d / n).exists() for n in names]
        if not any(present):
            counts["no_quanta_files"] += 1
            print(f"no quanta files {label}", file=out)
            continue
        t0 = time.perf_counter()
        try:
            if not all(present):
                raise qf.FigureRefused(
                    f"only one of {names[0]} and {names[1]} exists in {d}"
                )
            q = qkeys.get(qf.key_of(h))
            if q is None:
                raise qf.FigureRefused(f"no row in {quanta_path.name}")
            try:
                _, fields = qm.read_csv(d / "fields.csv")
                _, bounces = qm.read_csv(d / "bounces.csv")
            except (OSError, csv.Error, UnicodeDecodeError) as e:
                raise qf.FigureRefused(f"cannot read the exports in {d}: {e}")
            qb = qf.read_checked(d / names[0], assoc_scale)
            qsm = qf.read_checked(d / names[1], assoc_scale)
            data = HistoryData(
                h, q, fields, bounces, qb, qsm, assoc_scale, qs.SPECTRUM_MODEL
            )
            fig = build_figure(data)
            stem = qf.to_output(
                root, qf.plots_dir(root, d) / f"quanta_history{tag}", output
            )
            written = qf.save_figure(fig, stem)
        except (qf.FigureRefused, KeyError, ValueError) as e:
            counts["refused"] += 1
            reason = (
                str(e)
                if isinstance(e, qf.FigureRefused)
                else f"{type(e).__name__}: {e}"
            )
            print(f"REFUSED {label}: {reason}", file=out)
            continue
        counts["written"] += 1
        print(figure_line(h, fig, written, time.perf_counter() - t0), file=out)
    print(
        f"plot_quanta_history ({qs.SPECTRUM_MODEL}, assoc scale {assoc_scale:g}): figures written {counts['written']}, "
        f"refused {counts['refused']}, no quanta files {counts['no_quanta_files']}; skipped as fixed-law "
        f"{len(by_class[qf.FIXED])}, marker rows {len(by_class[qf.MARKER])}, failed histories {len(by_class[qf.FAILED])}; "
        f"wall {time.perf_counter() - t_start:.1f} s",
        file=out,
    )
    return 1 if counts["refused"] else 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="The per-history figure of the derived measures of a plot run's one-loop exports (one-loop-drain prompt 07)"
    )
    parser.add_argument("root", help="a plot run's <output>/<label>/")
    parser.add_argument("--A-values", type=float, nargs="+", default=None)
    parser.add_argument("--beta-values", type=float, nargs="+", default=None)
    parser.add_argument("--M-Mp-values", type=float, nargs="+", default=None)
    parser.add_argument(
        "--assoc-scale",
        type=qm._scale,
        default=qs.DEFAULT_ASSOC_SCALE,
        help="the associated piece's scale c_a (default 1)",
    )
    parser.add_argument(
        "--output", type=Path, default=None, help="write below this root, not the input"
    )
    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        return 2 if e.code else 0
    root = Path(args.root)
    if not root.is_dir():
        print(f"!! plot_quanta_history: {root} is not a directory", file=sys.stderr)
        return 2
    return run(
        root,
        args.A_values,
        args.beta_values,
        args.M_Mp_values,
        args.assoc_scale,
        args.output,
    )


if __name__ == "__main__":
    sys.exit(main())
