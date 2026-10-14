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
Plot the Jordan-frame temperatures a surfing trajectory reaches, against beta, with the hot and
cold roots of the reachability condition, from the CSV written by tools/surfing_scan.py.

Written for surfing-temperature prompt 03 (F1-F3; README §4). Run from the repository root, since
the EOS reads its table from the working directory:

    ./venv/bin/python tools/plot_surfing_temperature.py --csv PATH.csv --out PREFIX

It reads PATH.csv and PATH.csv.meta.json (the sidecar), builds the production cosmology, and from
it `find_kicks` (once) and `reachability_roots` on a fine beta grid, Delta beta = 0.005, over the
CSV's beta range rounded outwards to 0.05. It writes PREFIX.pdf and PREFIX.png. It starts no Ray,
opens no store and integrates no history. `main` takes one to three minutes on the full range,
nearly all of it in `reachability_roots`.

Two panels share the beta axis. The upper panel shows T_J in GeV (log axis): each kick's hot root
(solid) and cold root (dashed) with the reachable band between them filled; an interval that
spans a trough between kicks (`merged`) is one band, filled once, with its hot edge in the hotter
kick's colour and its cold edge in the colder kick's, and every line is broken where `merged`
changes; a dotted vertical line at each kick's threshold beta; and, for captured rows only, the
four measured temperatures (capture, largest, second dip, delivery). A blank is a gap, never a
zero. A band no row captures in is labelled "reachable, not surfed". Rows that are not captured,
and failure rows, are marks on the lower axis edge. The lower panel shows each measured
temperature divided by `T_hot_GeV` of its row, with a line at 1.

A row is classified by `status` first: a failure row has every other field blank, `captured`
included, and a blank `captured` is read as false only where it has no meaning to read.

Exit status: 0 if both files were written, 1 if saving failed, 2 if the run was refused.
"""

import argparse
import csv
import json
import os
import sys
import time
from collections import namedtuple
from math import ceil, floor, sqrt
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import numpy as np
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

# make the repository root importable when run as `python tools/plot_surfing_temperature.py`
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from extract_common import (  # noqa: E402
    _save,
    find_kicks,
    reachability_roots,
)
from tools.surfing_scan import COLUMNS, EOS_TABLE, beta_grid, make_setup  # noqa: E402

# P4: the fine grid of the roots, and the rounding of the CSV's beta range
ROOT_BETA_STEP = 0.005
ROOT_BETA_ROUND = 0.05
_ROUND_TOL = 1e-9

# the kicks, hot to cold, as find_kicks labels them in production, and how to show them
KICK_NAMES = {
    "EW": "EW",
    "QCD": "QCD",
    "mu-pi": r"$\mu/\pi$",
    "ee": r"$e^+e^-$",
}
# muted, and not the markers' colours, so that a band is never read as a measurement
KICK_COLOURS = {
    "EW": "#117733",
    "QCD": "#882255",
    "mu-pi": "#44AA99",
    "ee": "#999933",
}
_FALLBACK_COLOURS = ("#332288", "#AA4499", "#DDCC77", "#88CCEE")

# the four measured temperatures: CSV column, ratio-panel key, legend text, marker, colour,
# face (None = filled), size, z-order. Delivery is drawn under the largest, so that the two stay
# visible where they coincide (the wall-type maximum, board §5.3): a large black disc with a
# smaller orange up-triangle on it.
SERIES = [
    ("T_capture_GeV", "capture", "capture (first dip)", "v", "#E69F00", True, 5.0, 4),
    ("T_max_GeV", "largest", "largest", "^", "#D55E00", True, 4.0, 6),
    ("T_second_dip_GeV", "second dip", "second dip", "D", "#0072B2", False, 4.5, 5),
    (
        "T_deliver_GeV",
        "delivery",
        "delivery (first bounce)",
        "o",
        "#000000",
        True,
        6.5,
        3,
    ),
]
FLOAT_FIELDS = [
    "beta",
    "phi_init_Mp",
    "M_Mp",
    "Lambda_eV",
    "T_stop_GeV",
    "T_capture_GeV",
    "T_max_GeV",
    "T_second_dip_GeV",
    "T_deliver_GeV",
    "T_hot_GeV",
    "T_cold_GeV",
]

# kinds of row
CAPTURED = "captured"
UNCAPTURED = "not captured"
FAILURE = "failure"

# roots, as build_figure takes them: `betas` (the fine grid), `records` (one list per beta, one
# dict per kick, hot to cold, with the keys kick, T_hot, T_cold in GeV or None, and merged) and
# `thresholds` (kick label -> beta_th). The kick order is the order of each beta's records.
RootGrid = namedtuple("RootGrid", ["betas", "records", "thresholds"])


# ---------------------------------------------------------------------------------------------
# reading


def _float_or_none(text: str) -> Optional[float]:
    return None if text == "" else float(text)


def parse_rows(path: Path) -> List[dict]:
    """
    Read the CSV. Each row becomes a dict with the float fields as floats or None (a blank), the
    `kind` (CAPTURED, UNCAPTURED or FAILURE, from `status` first and `captured` only for an ok
    row), `capture_kick` (None for a blank) and the raw `status`. Rows are returned in beta
    order. ValueError if the header is not surfing_scan's, or a field cannot be read.
    """
    with open(path, newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header != COLUMNS:
            raise ValueError(f"{path}: the header is not tools.surfing_scan.COLUMNS")
        rows = []
        for line_no, fields in enumerate(reader, start=2):
            if len(fields) != len(COLUMNS):
                raise ValueError(f"{path}:{line_no}: {len(fields)} fields")
            raw = dict(zip(COLUMNS, fields))
            row = {name: _float_or_none(raw[name]) for name in FLOAT_FIELDS}
            row["status"] = raw["status"]
            row["capture_kick"] = raw["capture_kick"] or None
            if raw["status"] == "failure":
                row["kind"] = FAILURE
            elif raw["status"] == "ok":
                if raw["captured"] == "true":
                    row["kind"] = CAPTURED
                elif raw["captured"] == "false":
                    row["kind"] = UNCAPTURED
                else:
                    raise ValueError(
                        f"{path}:{line_no}: an ok row has captured={raw['captured']!r}"
                    )
            else:
                raise ValueError(f"{path}:{line_no}: status={raw['status']!r}")
            rows.append(row)
    rows.sort(key=lambda r: r["beta"])
    return rows


def read_sidecar(csv_path: Path) -> Optional[dict]:
    """The sidecar `<csv>.meta.json`, or None if there is none. ValueError if it is not JSON."""
    path = Path(str(csv_path) + ".meta.json")
    if not path.is_file():
        return None
    try:
        with open(path) as handle:
            return json.load(handle)
    except json.JSONDecodeError as err:
        raise ValueError(f"{path}: {err}")


def fine_grid(beta_min: float, beta_max: float) -> List[float]:
    """
    The beta grid of the roots: the range rounded outwards to 0.05 (with a tolerance, so that a
    beta already on the 0.05 grid is not moved a cell: 1.15/0.05 is 22.999999999999996), then
    steps of 0.005, built by index as tools.surfing_scan.beta_grid builds it.
    """
    lo = floor(beta_min / ROOT_BETA_ROUND + _ROUND_TOL) * ROOT_BETA_ROUND
    hi = ceil(beta_max / ROOT_BETA_ROUND - _ROUND_TOL) * ROOT_BETA_ROUND
    return beta_grid(round(lo, 10), round(hi, 10), ROOT_BETA_STEP)


def compute_roots(betas: Sequence[float]) -> RootGrid:
    """
    Build the production cosmology, find the kicks once, and the roots at each beta, in GeV.
    Takes minutes on the full range (reachability_roots is about 0.1-0.25 s a beta).
    """
    setup = make_setup()
    cosmology, GeV = setup.cosmology, setup.units.GeV
    kicks = find_kicks(
        cosmology, cosmology._eos._T_min * GeV, cosmology._eos._T_max * GeV
    )
    thresholds = {
        k["label"]: sqrt((2.0 + k["Sigma_peak"]) / (6.0 * k["Sigma_peak"]))
        for k in kicks
    }

    def to_GeV(T):
        return None if T is None else T / GeV

    records = []
    start = time.time()
    for i, beta in enumerate(betas):
        records.append(
            [
                {
                    "kick": r["kick"],
                    "T_hot": to_GeV(r["T_hot"]),
                    "T_cold": to_GeV(r["T_cold"]),
                    "merged": r["merged"],
                }
                for r in reachability_roots(cosmology, beta, kicks)
            ]
        )
        if (i + 1) % 100 == 0 or i + 1 == len(betas):
            print(
                f"roots: {i + 1}/{len(betas)} beta, {time.time() - start:.0f} s",
                flush=True,
            )
    return RootGrid(list(betas), records, thresholds)


# ---------------------------------------------------------------------------------------------
# the figure


def _group_labels(own: str, merged, order: Sequence[str]) -> tuple:
    others = [merged] if isinstance(merged, str) else list(merged)
    return tuple(sorted({own, *others}, key=order.index))


def _title(rows: Sequence[dict], sidecar: Optional[dict]) -> str:
    def values(key):
        seen = []
        for r in rows:
            if r[key] is not None and r[key] not in seen:
                seen.append(r[key])
        return ", ".join(f"{v:g}" for v in seen)

    heads = []
    if sidecar is None or not sidecar.get("runs"):
        heads.append("HEAD unknown")
    else:
        for run in sidecar["runs"]:
            head = str(run.get("git_head", "unknown"))[:7]
            if run.get("git_tracked_changes"):
                head += " + tracked changes"
            if head not in heads:
                heads.append(head)
    return (
        rf"$\phi_*$ = {values('phi_init_Mp')} $M_P$,  $M$ = {values('M_Mp')} $M_P$,  "
        rf"$\Lambda$ = {values('Lambda_eV')} eV,  $T_{{stop}}$ = {values('T_stop_GeV')} GeV"
        f"\ngit HEAD {', '.join(heads)}"
    )


def build_figure(
    rows: Sequence[dict], roots: RootGrid, sidecar: Optional[dict]
) -> Figure:
    """
    The figure of the module docstring, from parsed rows (parse_rows), roots (RootGrid, in GeV)
    and the sidecar (None is allowed). Needs no cosmology. Artists carry a gid (`root-hot-QCD`,
    `merged-cold-QCD+mu-pi`, `marker-capture`, `ratio-delivery`, `edge-failure`, `band-label-ee`,
    `threshold-QCD`, ...) so that a test can find them.
    """
    betas = np.asarray(roots.betas, dtype=float)
    n = len(betas)
    order = [r["kick"] for r in roots.records[0]] if n else []
    colour = {
        k: KICK_COLOURS.get(k, _FALLBACK_COLOURS[i % len(_FALLBACK_COLOURS)])
        for i, k in enumerate(order)
    }

    fig = Figure(figsize=(8.27, 9.0), layout="constrained")
    ax, rx = fig.subplots(2, 1, sharex=True, height_ratios=[2.0, 1.0])
    ax.set_yscale("log")

    # ---- the roots, kick by kick
    hot = {k: np.full(n, np.nan) for k in order}
    cold = {k: np.full(n, np.nan) for k in order}
    group = {k: [None] * n for k in order}
    for i, records in enumerate(roots.records):
        for rec in records:
            k = rec["kick"]
            if rec["T_hot"] is None or rec["T_cold"] is None:
                continue
            hot[k][i], cold[k][i] = rec["T_hot"], rec["T_cold"]
            if rec["merged"] is not None:
                group[k][i] = _group_labels(k, rec["merged"], order)

    def draw_band(ident, mask, upper, lower, c_hot, c_cold):
        if not mask.any():
            return
        ax.fill_between(
            betas,
            np.where(mask, lower, 1.0),
            np.where(mask, upper, 1.0),
            where=mask,
            color=c_hot,
            alpha=0.18,
            linewidth=0.0,
        )
        for suffix, y, style, c in (
            ("hot", upper, "-", c_hot),
            ("cold", lower, "--", c_cold),
        ):
            (line,) = ax.plot(
                betas,
                np.where(mask, y, np.nan),
                linestyle=style,
                color=c,
                linewidth=1.3,
            )
            line.set_gid(f"{ident[0]}-{suffix}-{ident[1]}")

    for k in order:
        alone = np.array([g is None for g in group[k]]) & np.isfinite(hot[k])
        draw_band(("root", k), alone, hot[k], cold[k], colour[k], colour[k])
    merged_groups = sorted(
        {g for k in order for g in group[k] if g is not None},
        key=lambda g: [order.index(x) for x in g],
    )
    for g in merged_groups:
        owner = g[
            0
        ]  # both records carry the same roots; the hotter kick's is drawn once
        mask = np.array([x == g for x in group[owner]])
        draw_band(
            ("merged", "+".join(g)),
            mask,
            hot[owner],
            cold[owner],
            colour[g[0]],
            colour[g[-1]],
        )

    # ---- measured rows
    captured = [r for r in rows if r["kind"] == CAPTURED]
    surfed = {r["capture_kick"] for r in captured if r["capture_kick"] is not None}

    # ---- thresholds, labelled in the headroom
    for k, beta_th in roots.thresholds.items():
        if n == 0 or not betas[0] <= beta_th <= betas[-1]:
            continue  # off the axis; a label there would stretch the layout
        for panel in (ax, rx):
            panel.axvline(beta_th, color="0.35", linestyle=":", linewidth=0.8, zorder=1)
        label = ax.text(
            beta_th,
            0.995,
            f" {KICK_NAMES.get(k, k)} {beta_th:.3f}",
            transform=ax.get_xaxis_transform(),
            rotation=90,
            ha="left",
            va="top",
            fontsize=7,
            color="0.2",
        )
        label.set_gid(f"threshold-{k}")

    # ---- band labels, hanging just below the cold edge, at the middle of the widest run
    def label_band(gid, text, c, mask, upper, lower):
        idx = np.flatnonzero(mask)
        if len(idx) == 0:
            return
        runs = np.split(idx, np.flatnonzero(np.diff(idx) > 1) + 1)
        run = max(runs, key=len)
        j = run[len(run) // 2]
        lab = ax.text(
            betas[j],
            lower[j] / 1.4,
            text,
            ha="center",
            va="top",
            fontsize=7,
            color=c,
            linespacing=1.1,
        )
        lab.set_gid(gid)

    for k in order:
        alone = np.array([g is None for g in group[k]]) & np.isfinite(hot[k])
        text = KICK_NAMES.get(k, k)
        if k not in surfed:
            text += "\nreachable, not surfed"
        label_band(f"band-label-{k}", text, colour[k], alone, hot[k], cold[k])
    for g in merged_groups:
        mask = np.array([x == g for x in group[g[0]]])
        text = " + ".join(KICK_NAMES.get(k, k) for k in g)
        label_band(
            f"band-label-merged-{'+'.join(g)}",
            text,
            colour[g[0]],
            mask,
            hot[g[0]],
            cold[g[0]],
        )

    # ---- the four series, in both panels
    handles: List = []
    for column, key, text, marker, c, filled, size, z in SERIES:
        pts = [
            (r["beta"], r[column], r["T_hot_GeV"])
            for r in captured
            if r[column] is not None
        ]
        style = dict(
            linestyle="none",
            marker=marker,
            markersize=size,
            markerfacecolor=c if filled else "none",
            markeredgecolor=(
                "white"
                if (filled and key == "largest")
                else (c if not filled else "black")
            ),
            markeredgewidth=0.5 if filled else 1.1,
            zorder=z + 5,
        )
        (upper,) = ax.plot([p[0] for p in pts], [p[1] for p in pts], **style)
        upper.set_gid(f"marker-{key}")
        ratios = [(b, T / h) for b, T, h in pts if h is not None]
        (lower,) = rx.plot([p[0] for p in ratios], [p[1] for p in ratios], **style)
        lower.set_gid(f"ratio-{key}")
        handles.append(Line2D([], [], label=text, **{**style, "zorder": 1}))

    # ---- rows not drawn as measurements: marks on the lower axis edge
    for kind, key, text, marker, c in (
        (UNCAPTURED, "uncaptured", "not captured", "|", "0.25"),
        (FAILURE, "failure", "failure", "x", "#CC3311"),
    ):
        xs = [r["beta"] for r in rows if r["kind"] == kind]
        if not xs:
            continue
        style = dict(
            linestyle="none",
            marker=marker,
            markersize=14,
            markeredgewidth=2.2,
            color=c,
            clip_on=False,
            zorder=20,
        )
        for panel in (ax, rx):
            (mark,) = panel.plot(
                xs, [0.0] * len(xs), transform=panel.get_xaxis_transform(), **style
            )
            mark.set_gid(f"edge-{key}-{'upper' if panel is ax else 'lower'}")
        handles.append(Line2D([], [], label=text, **{**style, "markersize": 9}))

    # ---- axes
    ax.set_ylabel(r"$T_J$ [GeV]")
    ax.grid(True, which="major", color="0.88", linewidth=0.6)
    ax.set_axisbelow(True)
    ys = [
        v
        for arr in list(hot.values()) + list(cold.values())
        for v in arr
        if np.isfinite(v)
    ]
    ys += [r[c[0]] for r in captured for c in SERIES if r[c[0]] is not None]
    if ys:
        ax.set_ylim(min(ys) / 3.0, max(ys) * 25.0)  # headroom for the threshold labels
    rx.axhline(1.0, color="k", linewidth=0.8)
    rx.set_xlabel(r"coupling $\beta$")
    rx.set_ylabel(r"$T_J\,/\,T_{\mathrm{hot}}$")
    rx.grid(True, color="0.88", linewidth=0.6)
    rx.set_axisbelow(True)
    ratio_values = [
        r[c[0]] / r["T_hot_GeV"]
        for r in captured
        for c in SERIES
        if r[c[0]] is not None and r["T_hot_GeV"] is not None
    ]
    if ratio_values:
        lo, hi = min(ratio_values + [1.0]), max(ratio_values + [1.0])
        pad = 0.06 * (hi - lo)
        rx.set_ylim(lo - pad, hi + pad)
    if n:
        pad = 0.01 * (betas[-1] - betas[0])  # so that a marker on the end is not cut
        rx.set_xlim(betas[0] - pad, betas[-1] + pad)

    # ---- one legend, outside the data
    handles += [
        Line2D(
            [], [], color="0.3", linestyle="-", linewidth=1.3, label="hot root (solid)"
        ),
        Line2D(
            [],
            [],
            color="0.3",
            linestyle="--",
            linewidth=1.3,
            label="cold root (dashed)",
        ),
        Line2D(
            [],
            [],
            color="0.35",
            linestyle=":",
            linewidth=0.8,
            label=r"kick threshold $\beta$",
        ),
    ]
    handles += [
        Patch(color=colour[k], alpha=0.5, label=KICK_NAMES.get(k, k)) for k in order
    ]
    fig.legend(
        handles=handles, loc="outside lower center", ncol=4, fontsize=8, frameon=False
    )
    fig.suptitle(_title(rows, sidecar), fontsize="small")
    return fig


# ---------------------------------------------------------------------------------------------
# main


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Plot the surfing temperatures and reachability roots against beta."
    )
    parser.add_argument(
        "--csv", required=True, type=Path, help="tools/surfing_scan.py's CSV"
    )
    parser.add_argument(
        "--out", required=True, help="path prefix; writes PREFIX.pdf and PREFIX.png"
    )
    args = parser.parse_args(argv)

    if not Path(os.getcwd(), EOS_TABLE).is_file():
        print(
            f"!! plot_surfing_temperature: run from the repository root; the EOS reads "
            f"{EOS_TABLE} from the working directory",
            file=sys.stderr,
        )
        return 2
    try:
        rows = parse_rows(args.csv)
        sidecar = read_sidecar(args.csv)
    except (OSError, ValueError) as err:
        print(f"!! plot_surfing_temperature: {err}", file=sys.stderr)
        return 2
    if len(rows) == 0:
        print(f"!! plot_surfing_temperature: {args.csv} has no rows", file=sys.stderr)
        return 2
    if sidecar is None:
        print(
            f"!! plot_surfing_temperature: no sidecar beside {args.csv}; the title will say "
            f"HEAD unknown",
            file=sys.stderr,
        )

    start = time.time()
    betas = fine_grid(rows[0]["beta"], rows[-1]["beta"])
    print(f"roots on {len(betas)} beta, {betas[0]:g} to {betas[-1]:g}", flush=True)
    roots = compute_roots(betas)
    fig = build_figure(rows, roots, sidecar)
    if not _save(fig, Path(args.out + ".pdf")):
        return 1
    print(f"wrote {args.out}.pdf and {args.out}.png in {time.time() - start:.0f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
