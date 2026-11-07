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
The grid figure (one-loop-drain prompt 07 §1.2; README §8.15 P58, P59; orchestrator note 07): ten heat maps of the
stored and derived measures over the (beta, M) grid of one one-loop A, from a plot run's exports alone. No store is
opened and no Ray is started.

    PYTHONPATH=. ./venv/bin/python tools/plot_quanta_grid.py ROOT [--A-values A [A ...]] [--assoc-scale C] [--output DIR]

ROOT is a plot run's `<output>/<label>/`, holding histories.csv and quanta_histories<suffix>.csv (tools/quanta_measures.py).
One figure per (drain model, A) of the one-loop rows of histories.csv, or per A given (a value that matches no one-loop
row is a usage error, exit 2; values are matched with `math.isclose(rel_tol=1e-9)` against `float()` of the `alpha` cells).
The rows of histories.csv are counted in the first of fixed, marker, failed, drawn that each meets (quanta_figures.classify);
the figure draws the drawn and the failed rows of its A.

**The join.** histories.csv to quanta_histories<suffix>.csv on the five key cells, compared as strings (05 copies them
verbatim). quanta_histories<suffix>.csv must exist and name SPECTRUM_MODEL and the scale asked for in every row, or no
figure is written (exit 1). A drawn history with no quanta_histories row is drawn from histories.csv alone and crossed in
panels (h)-(j).

**Axes.** beta across, left to right, and M up, the least M at the bottom: the sorted distinct values (by `float()`) of the
A's drawn and failed rows, as categories. Tick labels give beta as `%.5g` and M/M_P as `%.3g`. A cell with no row is the
background. Two rows of one A in one (beta, M) cell refuse that A's figure (exit 1), naming both.

**Panels**, in this order, each with its own colour bar and a title naming its column:
(a) drained_share_one_loop (linear, Normalize(0, 1)); (b) drained_share_gap + drained_share_classical +
drained_share_unassessed (log10, LogNorm); (c) parking_share (log10); (d) ell_min (linear); (e) all_worst_birth_margin,
(f) all_worst_reflection_margin, (g) all_worst_tracker_margin (diverging colour map, Normalize(-m, m), m the largest |value|
of the panel); (h) delta_stim, (i) E_field_max, (j) backreaction_max (quanta_histories; log10). A log10 panel's range is the
finite positive values' own; a panel with no positive value says so in its title and has a colour bar without ticks, and a
panel with one value only is widened (to [v/sqrt(10), v sqrt(10)] on log10, [v - 0.5, v + 0.5] on linear; m = 0 becomes 1).
A negative or non-finite value on a log10 panel is a defect of the input and refuses the figure. Panel (b) is the sum of the
three shares, blank where any is blank.

**Special cells**, the same in every panel: a blank value is a hatched grey; a zero on a log10 panel is a solid grey; a failed
history is crossed in every panel; a drawn history with no quanta_histories row is crossed in panels (h)-(j). Each kind is
counted in the footer and on the printed line.

**Written** to `alpha_path(root, A, drain_model) / "plots" / "quanta_grid<suffix>.pdf"` and `.png` (150 dpi), the suffix
being quanta_measures.suffix ('' at c_a = 1, '-assoc<c:g>' otherwise), below --output if given. **Exit status:** 0 if every
figure asked for is written; 1 if a figure was refused or the quanta_histories file fails its check, the rest written;
2 on a usage error or an unreadable root.
"""

import argparse
import csv
import sys
import time
from math import isfinite, sqrt
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Sequence, Tuple

import numpy as np
from matplotlib.colors import LogNorm, Normalize
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle

from ComputeTargets import quanta_spectrum as qs
from extract_common import alpha_path
from tools import quanta_figures as qf
from tools import quanta_measures as qm

HATCH_FILL = "#d9d9d9"
HATCH_EDGE = "#7f7f7f"
ZERO_FILL = "#6e6e6e"
BACKGROUND = "#f4ecd8"  # a cell with no history; not the white that a diverging map passes through
CROSS_COLOUR = "black"

LINEAR01, LOG10, LINEAR, DIVERGING = "linear01", "log10", "linear", "diverging"


class Panel(NamedTuple):
    letter: str
    columns: Tuple[str, ...]
    source: str  # "histories" or "quanta"
    scale: str
    cmap: str


PANELS = (
    Panel("a", ("drained_share_one_loop",), "histories", LINEAR01, "viridis"),
    Panel(
        "b",
        ("drained_share_gap", "drained_share_classical", "drained_share_unassessed"),
        "histories",
        LOG10,
        "viridis",
    ),
    Panel("c", ("parking_share",), "histories", LOG10, "viridis"),
    Panel("d", ("ell_min",), "histories", LINEAR, "cividis"),
    Panel("e", ("all_worst_birth_margin",), "histories", DIVERGING, "RdBu"),
    Panel("f", ("all_worst_reflection_margin",), "histories", DIVERGING, "RdBu"),
    Panel("g", ("all_worst_tracker_margin",), "histories", DIVERGING, "RdBu"),
    Panel("h", ("delta_stim",), "quanta", LOG10, "magma"),
    Panel("i", ("E_field_max",), "quanta", LOG10, "magma"),
    Panel("j", ("backreaction_max",), "quanta", LOG10, "magma"),
)

# the kinds of cell, as the gid of the artist that draws it
VALUE, BLANK, ZERO, CROSS_FAILED, CROSS_NOQUANTA = (
    "value",
    "blank",
    "zero",
    "cross-failed",
    "cross-noquanta",
)


class Cell(NamedTuple):
    """One histories.csv row of the A: failed (blank normalization) or drawn, and its quanta_histories row if any."""

    row: dict
    quanta: Optional[dict]
    failed: bool


def title_of(panel: Panel) -> str:
    return (
        f"({panel.letter}) " + " + ".join(panel.columns)
        if len(panel.columns) > 1
        else f"({panel.letter}) {panel.columns[0]}"
    )


def cell_value(panel: Panel, cell: Cell) -> Tuple[str, Optional[float]]:
    """(kind, value) of one cell in one panel: kind is one of the module's cell kinds, VALUE or ZERO or BLANK with the value
    (None unless VALUE or ZERO), or a cross."""
    if cell.failed:
        return CROSS_FAILED, None
    if panel.source == "quanta":
        if cell.quanta is None:
            return CROSS_NOQUANTA, None
        source = cell.quanta
    else:
        source = cell.row
    total = 0.0
    for name in panel.columns:
        try:
            text = source[name]
        except KeyError:
            raise qf.FigureRefused(f"a column {name!r} is missing")
        if text == "":
            return BLANK, None
        v = float(text)
        if not isfinite(v):
            raise qf.InputDefect(
                f"panel ({panel.letter}): {name} = {text!r} is not finite"
            )
        total += v
    if panel.scale == LOG10:
        if total < 0.0:
            raise qf.InputDefect(
                f"panel ({panel.letter}): a negative value {total!r} of {' + '.join(panel.columns)} on a log10 panel"
            )
        if total == 0.0:
            return ZERO, 0.0
    return VALUE, total


def norm_of(panel: Panel, values: Sequence[float]) -> Tuple[Normalize, Optional[str]]:
    """The panel's norm (ruling 4) and, if its range was degenerate, `"none"` or `"one"`."""
    vals = [v for v in values if isfinite(v)]
    if panel.scale == LINEAR01:
        return Normalize(0.0, 1.0), ("none" if not vals else None)
    if panel.scale == LOG10:
        if not vals:
            return LogNorm(1.0, 10.0), "none"
        lo, hi = min(vals), max(vals)
        if lo == hi:
            return LogNorm(lo / sqrt(10.0), hi * sqrt(10.0)), "one"
        return LogNorm(lo, hi), None
    if panel.scale == LINEAR:
        if not vals:
            return Normalize(0.0, 1.0), "none"
        lo, hi = min(vals), max(vals)
        if lo == hi:
            return Normalize(lo - 0.5, hi + 0.5), "one"
        return Normalize(lo, hi), None
    # DIVERGING: symmetric about zero, not TwoSlopeNorm (which is not symmetric)
    if not vals:
        return Normalize(-1.0, 1.0), "none"
    m = max(abs(v) for v in vals)
    if m == 0.0:
        return Normalize(-1.0, 1.0), "one"
    return Normalize(-m, m), None


def categories(cells: Sequence[Cell]) -> Tuple[List[float], List[float]]:
    """The sorted distinct beta and M_Mp of the cells, by float()."""
    betas = sorted({float(c.row["beta"]) for c in cells})
    Ms = sorted({float(c.row["M_Mp"]) for c in cells})
    return betas, Ms


def check_cells(cells: Sequence[Cell]) -> None:
    """Two rows of one (beta, M) cell refuse the A's figure, naming both (ruling 2)."""
    seen: Dict[Tuple[float, float], dict] = {}
    for c in cells:
        k = (float(c.row["beta"]), float(c.row["M_Mp"]))
        if k in seen:
            raise qf.FigureRefused(
                f"two rows in one cell: {qm.row_label(seen[k])} and {qm.row_label(c.row)}"
            )
        seen[k] = c.row


def build_figure(
    cells: Sequence[Cell],
    root: Path,
    drain_model: str,
    A: float,
    scale: float,
    model: str = qs.SPECTRUM_MODEL,
) -> Figure:
    """
    The ten-panel figure of one A. The returned Figure carries `quanta_counts`, a dict by panel letter of the counts of
    each kind of cell (`value`, `blank`, `zero`, `cross-failed`, `cross-noquanta`), `quanta_degenerate` (a dict by letter
    of `"none"` or `"one"` for the panels whose range was degenerate), and `quanta_totals` (drawn, failed, noquanta).
    """
    check_cells(cells)
    betas, Ms = categories(cells)
    ib = {b: k for k, b in enumerate(betas)}
    im_ = {m: k for k, m in enumerate(Ms)}
    fig = Figure(figsize=(24, 10.5))
    axes = fig.subplots(2, 5)
    counts: Dict[str, Dict[str, int]] = {}
    degenerate: Dict[str, str] = {}
    drawn = sum(1 for c in cells if not c.failed)
    failed = sum(1 for c in cells if c.failed)
    noquanta = sum(1 for c in cells if not c.failed and c.quanta is None)
    for ax, panel in zip(axes.flat, PANELS):
        kinds = []
        values = []
        for c in cells:
            kind, v = cell_value(panel, c)
            kinds.append((c, kind, v))
            if kind == VALUE:
                values.append(v)
        norm, degen = norm_of(panel, values)
        Z = np.ma.masked_all((len(Ms), len(betas)))
        for c, kind, v in kinds:
            if kind == VALUE:
                Z[im_[float(c.row["M_Mp"])], ib[float(c.row["beta"])]] = v
        image = ax.imshow(
            Z,
            origin="lower",
            aspect="auto",
            interpolation="nearest",
            cmap=panel.cmap,
            norm=norm,
        )
        image.set_gid(f"panel-{panel.letter}-image")
        n = dict.fromkeys((VALUE, BLANK, ZERO, CROSS_FAILED, CROSS_NOQUANTA), 0)
        crosses: Dict[str, Tuple[List[int], List[int]]] = {
            CROSS_FAILED: ([], []),
            CROSS_NOQUANTA: ([], []),
        }
        for c, kind, v in kinds:
            n[kind] += 1
            x, y = ib[float(c.row["beta"])], im_[float(c.row["M_Mp"])]
            if kind == BLANK:
                p = Rectangle(
                    (x - 0.5, y - 0.5),
                    1.0,
                    1.0,
                    facecolor=HATCH_FILL,
                    edgecolor=HATCH_EDGE,
                    hatch="////",
                    linewidth=0.0,
                )
                p.set_gid(BLANK)
                ax.add_patch(p)
            elif kind == ZERO:
                p = Rectangle(
                    (x - 0.5, y - 0.5),
                    1.0,
                    1.0,
                    facecolor=ZERO_FILL,
                    edgecolor="none",
                    linewidth=0.0,
                )
                p.set_gid(ZERO)
                ax.add_patch(p)
            elif kind in crosses:
                crosses[kind][0].append(x)
                crosses[kind][1].append(y)
        for kind, (xs, ys) in crosses.items():
            (line,) = ax.plot(
                xs,
                ys,
                linestyle="none",
                marker="x",
                color=CROSS_COLOUR,
                markersize=7,
                markeredgewidth=1.2,
            )
            line.set_gid(kind)
        cb = fig.colorbar(image, ax=ax, pad=0.02)
        title = title_of(panel)
        if degen == "none":
            title += " (no positive value)" if panel.scale == LOG10 else " (no value)"
            cb.set_ticks([])
        elif degen == "one":
            title += " (one value)"
        ax.set_title(title, fontsize=9)
        ax.set_facecolor(BACKGROUND)
        ax.set_gid(f"panel-{panel.letter}")
        cb.ax.set_gid(f"panel-{panel.letter}-colourbar")
        ax.set_xticks(range(len(betas)))
        ax.set_xticklabels([f"{b:.5g}" for b in betas], rotation=90, fontsize=7)
        ax.set_yticks(range(len(Ms)))
        ax.set_yticklabels([f"{m:.3g}" for m in Ms], fontsize=7)
        ax.set_xlim(-0.5, len(betas) - 0.5)
        ax.set_ylim(-0.5, len(Ms) - 0.5)
        ax.set_xlabel("beta (dimensionless)", fontsize=8)
        ax.set_ylabel("M / M_P", fontsize=8)
        counts[panel.letter] = n
        if degen:
            degenerate[panel.letter] = degen
    handles = [
        Rectangle(
            (0, 0), 1, 1, facecolor=HATCH_FILL, edgecolor=HATCH_EDGE, hatch="////"
        ),
        Rectangle((0, 0), 1, 1, facecolor=ZERO_FILL),
        Line2D([], [], linestyle="none", marker="x", color=CROSS_COLOUR),
        Rectangle((0, 0), 1, 1, facecolor=BACKGROUND, edgecolor=HATCH_EDGE),
    ]
    fig.legend(
        handles,
        [
            "blank (NULL)",
            "zero on a log10 panel",
            "failed history (all panels); no quanta_histories row (h)-(j)",
            "no history in the cell",
        ],
        loc="upper right",
        ncol=4,
        fontsize=8,
        frameon=False,
    )
    footer = f"Root: {root}   drain model: {drain_model}   A = {A:.5g}   spectrum model: {model}, c_a = {scale:g}\n" f"drawn {drawn}, failed {failed}, no quanta_histories row {noquanta}; blank cells per panel: " + ", ".join(
        f"{p.letter} {counts[p.letter][BLANK]}" for p in PANELS
    ) + "; zero cells (log10 panels): " + ", ".join(
        f"{p.letter} {counts[p.letter][ZERO]}" for p in PANELS if p.scale == LOG10
    )
    fig.suptitle(f"{drain_model}, A = {A:.5g}", fontsize=12, x=0.02, ha="left")
    fig.text(0.02, 0.005, footer, fontsize=8, va="bottom", ha="left")
    fig.tight_layout(rect=(0.0, 0.05, 1.0, 0.95))
    fig.quanta_counts = counts
    fig.quanta_degenerate = degenerate
    fig.quanta_totals = dict(drawn=drawn, failed=failed, noquanta=noquanta)
    return fig


def figure_line(
    drain_model: str, A: float, fig: Figure, written: Sequence[Path], wall: float
) -> str:
    """The printed line of one figure: the counts of the footer and the files written."""
    t = fig.quanta_totals
    c = fig.quanta_counts
    blank = ", ".join(f"{p.letter} {c[p.letter][BLANK]}" for p in PANELS)
    zero = ", ".join(
        f"{p.letter} {c[p.letter][ZERO]}" for p in PANELS if p.scale == LOG10
    )
    degen = "; ".join(f"({k}) {v}" for k, v in sorted(fig.quanta_degenerate.items()))
    return (
        f"{drain_model} A={A:.5g}: drawn {t['drawn']}, failed {t['failed']}, no quanta row {t['noquanta']}; "
        f"blank cells [{blank}]; zero cells [{zero}]; degenerate colour range [{degen or 'none'}]; "
        f"wrote {' '.join(str(p) for p in written)}; wall {wall:.2f} s"
    )


def run(
    root: Path,
    A_values: Optional[Sequence[float]] = None,
    assoc_scale: float = qs.DEFAULT_ASSOC_SCALE,
    output: Optional[Path] = None,
    out=None,
) -> int:
    """Draw the figures of every (drain model, A) asked for; print the lines. Returns the exit status."""
    out = sys.stdout if out is None else out
    root = Path(root)
    try:
        rows = qf.read_histories(root)
    except (OSError, csv.Error, UnicodeDecodeError) as e:
        print(
            f"!! plot_quanta_grid: cannot read {root / 'histories.csv'}: {e}",
            file=sys.stderr,
        )
        return 2
    by_class = qf.walk(rows)
    one_loop = [r for r in rows if r["drain_model"] != qf.DRAIN_MODEL_FIXED]
    if A_values:
        for a in A_values:
            if not any(qf.is_close(a, r["alpha"]) for r in one_loop):
                print(
                    f"!! plot_quanta_grid: --A-values {a:g} matches no one-loop row of {root / 'histories.csv'}",
                    file=sys.stderr,
                )
                return 2
    groups: Dict[Tuple[str, float], List[dict]] = {}
    for r in one_loop:
        a = float(r["alpha"])
        if A_values and not any(qf.is_close(v, r["alpha"]) for v in A_values):
            continue
        groups.setdefault((r["drain_model"], a), []).append(r)
    tag = qm.suffix(assoc_scale)
    t_start = time.perf_counter()
    counts = dict(written=0, refused=0, empty=0)
    print(
        f"plot_quanta_grid: histories.csv rows: fixed {len(by_class[qf.FIXED])}, marker {len(by_class[qf.MARKER])}, "
        f"failed {len(by_class[qf.FAILED])}, drawn {len(by_class[qf.DRAWN])}",
        file=out,
    )
    quanta_path = root / f"quanta_histories{tag}.csv"
    try:
        qrows = qf.read_checked(quanta_path, assoc_scale)
        qkeys: Dict[Tuple[str, ...], dict] = {}
        for q in qrows:
            k = qf.key_of(q)
            if k in qkeys:
                raise qf.FigureRefused(f"{quanta_path.name}: two rows with the key {k}")
            qkeys[k] = q
    except qf.FigureRefused as e:
        print(f"REFUSED {quanta_path}: {e}; no figure is written", file=out)
        return 1
    for (model, A), group in sorted(groups.items()):
        cells = [
            Cell(r, qkeys.get(qf.key_of(r)), qf.classify(r) == qf.FAILED)
            for r in group
            if qf.classify(r) in (qf.DRAWN, qf.FAILED)
        ]
        if not cells:
            counts["empty"] += 1
            print(
                f"{model} A={A:.5g}: no drawn or failed row ({len(group)} marker rows); no figure",
                file=out,
            )
            continue
        t0 = time.perf_counter()
        try:
            fig = build_figure(cells, root, model, A, assoc_scale)
            stem = qf.to_output(
                root,
                alpha_path(root, A, model) / "plots" / f"quanta_grid{tag}",
                output,
            )
            written = qf.save_figure(fig, stem)
        except qf.FigureRefused as e:
            counts["refused"] += 1
            print(f"REFUSED {model} A={A:.5g}: {e}", file=out)
            continue
        counts["written"] += 1
        print(figure_line(model, A, fig, written, time.perf_counter() - t0), file=out)
    print(
        f"plot_quanta_grid ({qs.SPECTRUM_MODEL}, assoc scale {assoc_scale:g}): figures written {counts['written']}, "
        f"refused {counts['refused']}, A with no drawn or failed row {counts['empty']}; skipped as fixed-law "
        f"{len(by_class[qf.FIXED])}, marker rows {len(by_class[qf.MARKER])}; wall {time.perf_counter() - t_start:.1f} s",
        file=out,
    )
    return 1 if counts["refused"] else 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="The grid figure of the derived measures of a plot run's one-loop exports (one-loop-drain prompt 07)"
    )
    parser.add_argument("root", help="a plot run's <output>/<label>/")
    parser.add_argument(
        "--A-values",
        type=float,
        nargs="+",
        default=None,
        help="the A to draw (default: every one-loop A)",
    )
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
        print(f"!! plot_quanta_grid: {root} is not a directory", file=sys.stderr)
        return 2
    return run(root, args.A_values, args.assoc_scale, args.output)


if __name__ == "__main__":
    sys.exit(main())
