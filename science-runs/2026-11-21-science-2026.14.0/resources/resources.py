"""
The resource requirements of the 2026.14.0 science run: wall-clock time, CPU time, the histories computed, and the
storage of the datastores and of the derived products.

    ./venv/bin/python science-runs/2026-11-21-science-2026.14.0/resources/resources.py

Reads, all beside this file or in the run's folder, nothing from a store:
- inputs/histories.csv: one row per stored ScalarHistory of the three stores, with its model, alpha row, the
  integrator's statistics and the timings its BBN and adiabatic rows record (extract_remote.sh "histories");
- inputs/tables.csv: bytes per table and index of every shard and primary, from sqlite's dbstat ("tables");
- inputs/files.csv: bytes of every file of the three stores ("files");
- inputs/outputs.csv: the exports under var/science-2026.14.0/out/, by root and kind, file counts and bytes ("outputs");
- inputs/steps.txt: the run script's own ">> <time> <step>" lines from run.out on the Mac Studio;
- ../logs/<slot>.log: each pipeline stage's "ALL WORK ITEMS COMPLETE in time ..." line.

Writes resources-time, resources-histories and resources-storage (.pdf, .png), resources.txt (the numbers the figures
draw and NOTES.md quotes) and resources.provenance.json.

What the timings are (NOTES.md says more):
- a slot's wall-clock is the run script's, from steps.txt; a stage's is its log line;
- "reserved" CPU is 8 x a stage's wall-clock, the Ray cluster's 8 CPUs;
- "recorded" CPU is what the store holds: ScalarHistory.compute_time is the integrator's time for the history's final
  integration, so compute_time x tuning_iterations estimates the tuning's integrations; BBNData records the PRyMordial
  and the new-physics times; AdiabaticHistory its compute_time. A failed history records none, and nothing outside
  the integrator is timed, so "recorded" is a lower bound on the CPU used;
- a history's label ends with the time the driver built its compute task, and its row's timestamp is when the row was
  stored, so their difference includes the time the task waited in Ray's queue (up to 24 tasks in flight on 8 CPUs).
  It is used only for the end times: within each pipeline row (one alpha or A row of one slot), the time from the row's
  first task to its 95th-percentile end is "bulk" and from there to its last end is "tail".

Written 2026-11-23 at the user's request ("can you generate some figures for the resource requirements of the
2026.14.0 run?").
"""

import hashlib
import json
import re
import subprocess
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
RUN_DIR = HERE.parent
REPO = RUN_DIR.parent.parent
INPUTS = HERE / "inputs"
LOGS = RUN_DIR / "logs"
RAY_CPUS = 8
GIB = 1024**3

# the reference palette's categorical slots, in fixed order (dataviz references/palette.md, light mode)
SLOT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
WALL_ORDER = ["exp", "pl2", "pl1"]
WALL_COLOR = {"exp": SLOT[0], "pl2": SLOT[1], "pl1": SLOT[2]}
WALL_NAME = {
    "exp": "exponential (n = 1)",
    "pl2": "inverse power, n = 2",
    "pl1": "inverse power, n = 1",
}
# the pipeline's stages, in their order, as neutral steps (they are not walls)
STAGES = [
    ("SCALAR FIELD HISTORIES FOR SAMPLE GRID", "scalar histories", "#4a4a48"),
    ("ADIABATIC TRANSGRESSION PARAMETERS", "adiabatic", "#c9c8c0"),
    ("BBN DATA", "BBN (PRyMordial)", "#8f8f8a"),
]
INK = "#2b2b29"
MUTED = "#6f6e69"
GRID = "#e4e3dd"

plt.rcParams.update(
    {
        "font.size": 9,
        "axes.edgecolor": MUTED,
        "axes.labelcolor": INK,
        "axes.titlesize": 10,
        "axes.titleweight": "bold",
        "axes.titlelocation": "left",
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "text.color": INK,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "legend.frameon": False,
        "axes.grid": True,
        "grid.color": GRID,
        "grid.linewidth": 0.6,
        "axes.axisbelow": True,
    }
)

OUT_LINES = []


def say(line=""):
    print(line)
    OUT_LINES.append(line)


def secs(text):
    return sum(
        float(v) * {"h": 3600, "m": 60, "s": 1}[u]
        for v, u in re.findall(r"([\d.]+)([hms])", text)
    )


# ---------------------------------------------------------------- inputs


def load_histories():
    d = pd.read_csv(INPUTS / "histories.csv")
    # model_type 1 is ExponentialModel, 2 InversePowerModel (one per store; both in the reflection store, where the
    # two tables' serials overlap, so the columns are chosen by the type and never by which join matched)
    exp_row = d.model_type == 1
    d["n"] = np.where(exp_row, d.em_n, d.ip_n)
    d["beta"] = np.where(exp_row, d.em_beta, d.ip_beta)
    d["M_eV"] = np.where(exp_row, d.em_M_eV, d.ip_M_eV)
    d["wall"] = np.where(d.model_type == 1, "exp", np.where(d.n == 1, "pl1", "pl2"))
    d["slot"] = d.label.str.extract(r"^(.*?)-ScalarHistory")[0]
    d["block"] = d.slot.str[0]
    d["law"] = np.where(d.drain_model == "fixed", "fixed", "one-loop")
    d["submitted"] = pd.to_datetime(
        d.label.str.extract(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)$")[0]
    )
    d["stored"] = pd.to_datetime(d.timestamp)
    d["integration_s"] = d.compute_time * d.tuning_iterations
    d["bbn_s"] = d.bbn_PRyM_time.fillna(0) + d.bbn_NP_time.fillna(0)
    return d


def load_steps():
    rows = []
    for line in (INPUTS / "steps.txt").read_text().splitlines():
        m = re.match(r">> (\S+ \S+) (.*)", line)
        if m:
            rows.append((pd.Timestamp(m.group(1)), m.group(2)))
    return pd.DataFrame(rows, columns=["t", "step"])


def load_stage_times():
    rows = []
    for f in sorted(LOGS.glob("[ABCDR]-*.log")):
        stage = None
        for line in f.read_text(errors="replace").splitlines():
            m = re.match(r"\*\* CALCULATE (.*)", line)
            if m:
                stage = m.group(1).strip()
                continue
            m = re.search(r"ALL WORK ITEMS COMPLETE in time (.*)", line)
            if m and stage:
                rows.append((f.stem, stage, secs(m.group(1))))
                stage = None
    return pd.DataFrame(rows, columns=["slot", "stage", "sec"])


# ---------------------------------------------------------------- the run's phases


def phases(steps):
    """[(name, kind, start, end)]: each build slot, the inventories, and each wall's exports, from the script's lines."""
    out = []
    t = steps.reset_index(drop=True)
    slot_rows = t[~t.step.str.startswith("disk") & ~t.step.str.startswith("science")]
    first_export = t[t.step.str.startswith("plot_by_beta")].t.min()
    builds = slot_rows[slot_rows.t < first_export]
    times = list(builds.t)
    names = list(builds.step)
    # the build ends at the inventories; the script prints no line there, so the last slot ends at its registry
    # record's last heartbeat (README "The registered runs": 00:05:45), and the inventories run to the first export
    build_end = pd.Timestamp("2026-11-22 00:05:45")
    for i, name in enumerate(names):
        end = times[i + 1] if i + 1 < len(times) else build_end
        kind = "pl" if "-pl-" in name else "exp"
        out.append((name, kind, times[i], end))
    out.append(("inventories", "other", build_end, first_export))
    for wall in WALL_ORDER[0:1] + ["pl1", "pl2"]:
        start = t[t.step == f"plot_by_beta ({wall})"].t.iloc[0]
        end = t[t.step.str.startswith(f"wall {wall} done")].t.iloc[0]
        out.append((f"exports, {wall}", "export", start, end))
    return out


# ---------------------------------------------------------------- figure 1: time


def figure_time(d, steps, stages):
    ph = phases(steps)
    t0 = ph[0][2]
    fig, axes = plt.subplots(2, 2, figsize=(12.5, 8.6), constrained_layout=True)

    # (a) timeline
    ax = axes[0, 0]
    colour = {
        "exp": WALL_COLOR["exp"],
        "pl": WALL_COLOR["pl2"],
        "other": "#c9c8c0",
        "export": "#8f8f8a",
    }
    labels = [p[0] for p in ph]
    for i, (name, kind, a, b) in enumerate(ph):
        x0 = (a - t0).total_seconds() / 3600
        w = (b - a).total_seconds() / 3600
        ax.barh(
            i,
            w,
            left=x0,
            height=0.62,
            color=colour[kind],
            edgecolor="white",
            linewidth=1,
        )
        hours = w
        txt = f"{hours:.1f} h" if hours >= 0.95 else f"{hours * 60:.0f} min"
        ax.text(x0 + w + 0.25, i, txt, va="center", fontsize=7.5, color=MUTED)
    ax.set_yticks(range(len(labels)), labels, fontsize=7.5)
    ax.invert_yaxis()
    ax.set_xlabel("hours since launch (2026-11-21 01:17:43 UTC)")
    ax.set_title("(a) The run, slot by slot: 25 h 45 min end to end")
    ax.grid(axis="y", visible=False)
    ax.set_xlim(0, 28.5)
    handles = [
        plt.Rectangle((0, 0), 1, 1, color=colour[k])
        for k in ("exp", "pl", "other", "export")
    ]
    ax.legend(
        handles,
        [
            "exponential build",
            "inverse-power build (n = 1 and 2)",
            "inventories",
            "exports, measures, figures",
        ],
        loc="upper right",
        fontsize=7.5,
    )

    # (b) build wall-clock by stage, per slot
    ax = axes[0, 1]
    slots = [p[0] for p in ph if p[1] in ("exp", "pl")]
    piv = (
        stages.pivot_table(index="slot", columns="stage", values="sec", aggfunc="sum")
        .reindex(slots)
        .fillna(0)
        / 3600
    )
    nh = d.groupby("slot").size().reindex(slots)
    left = np.zeros(len(slots))
    for key, name, col in STAGES:
        v = piv[key].values
        ax.barh(
            range(len(slots)),
            v,
            left=left,
            height=0.62,
            color=col,
            edgecolor="white",
            linewidth=1,
            label=name,
        )
        left += v
    for i, s in enumerate(slots):
        ax.text(
            left[i] + 0.06,
            i,
            f"{nh[s]:,} histories",
            va="center",
            fontsize=7.5,
            color=MUTED,
        )
    ax.set_yticks(range(len(slots)), slots, fontsize=7.5)
    ax.invert_yaxis()
    ax.set_xlabel("hours of wall-clock, by pipeline stage")
    ax.set_title("(b) Build time per slot, by stage")
    ax.grid(axis="y", visible=False)
    ax.set_xlim(0, max(left) * 1.32)
    ax.legend(loc="lower right", fontsize=7.5)

    # (c) CPU: reserved against recorded, by stage
    ax = axes[1, 0]
    stage_wall = stages.groupby("stage").sec.sum() / 3600
    reserved = [stage_wall[k] * RAY_CPUS for k, _, _ in STAGES]
    rec = [
        d.integration_s.sum() / 3600,
        d.adiabatic_time.sum() / 3600,
        d.bbn_s.sum() / 3600,
    ]
    y = np.arange(len(STAGES))
    ax.barh(
        y - 0.19,
        reserved,
        height=0.36,
        color="#c9c8c0",
        edgecolor="white",
        label=f"reserved: {RAY_CPUS} CPUs x stage wall-clock",
    )
    ax.barh(
        y + 0.19,
        rec,
        height=0.36,
        color="#4a4a48",
        edgecolor="white",
        label="recorded in the store (lower bound)",
    )
    for i in range(len(STAGES)):
        ax.text(
            reserved[i] + 1.5,
            y[i] - 0.19,
            f"{reserved[i]:.0f} CPU-h",
            va="center",
            fontsize=7.5,
            color=MUTED,
        )
        ax.text(
            rec[i] + 1.5,
            y[i] + 0.19,
            f"{rec[i]:.1f} CPU-h ({rec[i] / reserved[i]:.0%})",
            va="center",
            fontsize=7.5,
            color=MUTED,
        )
    ax.set_yticks(y, [n for _, n, _ in STAGES], fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel("CPU-hours")
    ax.set_xlim(0, max(reserved) * 1.3)
    ax.set_title(
        f"(c) CPU time: {sum(reserved):.0f} CPU-h reserved, at least {sum(rec):.0f} recorded"
    )
    ax.grid(axis="y", visible=False)
    ax.legend(loc="lower right", fontsize=7.5)

    # (d) the straggler tail in the scalar-history stage
    ax = axes[1, 1]
    tail = tails(d)
    # the reflection slots' rows hold 1 to 3 histories each, where a 95th percentile is the last; only the main blocks
    slots = [s for s in slots if not s.startswith("R-")]
    bt = tail.groupby("slot")[["bulk_h", "tail_h"]].sum().reindex(slots).fillna(0)
    ax.barh(
        range(len(slots)),
        bt.bulk_h,
        height=0.62,
        color="#4a4a48",
        edgecolor="white",
        label="first task to 95% of the row stored",
    )
    ax.barh(
        range(len(slots)),
        bt.tail_h,
        left=bt.bulk_h,
        height=0.62,
        color="#eda100",
        edgecolor="white",
        label="the last 5% of the row",
    )
    for i, s in enumerate(slots):
        tot = bt.bulk_h[s] + bt.tail_h[s]
        if tot > 0:
            ax.text(
                tot + 0.06,
                i,
                f"tail {bt.tail_h[s] / tot:.0%}",
                va="center",
                fontsize=7.5,
                color=MUTED,
            )
    ax.set_yticks(range(len(slots)), slots, fontsize=7.5)
    ax.invert_yaxis()
    ax.set_xlabel(
        "hours of the scalar-history stage, summed over the slot's rows (blocks A-D)"
    )
    ax.set_title(
        f"(d) Waiting on stragglers: {bt.tail_h.sum() / (bt.bulk_h.sum() + bt.tail_h.sum()):.0%} of the stage"
    )
    ax.grid(axis="y", visible=False)
    ax.set_xlim(0, (bt.bulk_h + bt.tail_h).max() * 1.25)
    ax.legend(loc="upper right", fontsize=7.5)

    fig.suptitle(
        "2026.14.0: time on the Mac Studio (8 Ray CPUs)",
        x=0.01,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    save(fig, "resources-time")
    return reserved, rec, tail


def tails(d):
    rows = []
    for (slot, law, alpha), g in d.groupby(["slot", "drain_model", "alpha"]):
        first = g.submitted.min()
        ends = np.sort((g.stored - first).dt.total_seconds().values)
        last = ends[-1]
        p95 = np.quantile(ends, 0.95, method="inverted_cdf")
        rows.append((slot, law, alpha, len(g), p95 / 3600, (last - p95) / 3600))
    return pd.DataFrame(
        rows, columns=["slot", "drain_model", "alpha", "n", "bulk_h", "tail_h"]
    )


# ---------------------------------------------------------------- figure 2: histories


def figure_histories(d):
    fig, axes = plt.subplots(1, 3, figsize=(14.5, 4.9), constrained_layout=True)

    # (a) histories per wall and block, ok and failed
    ax = axes[0]
    blocks = ["A", "B", "C", "D", "R"]
    rows = []
    for w in WALL_ORDER:
        for b in blocks:
            g = d[(d.wall == w) & (d.block == b)]
            if len(g):
                sh = int((g.failure == 1).sum())
                bbn = int(((g.failure == 0) & (g.bbn_failure == 1)).sum())
                rows.append((w, b, len(g) - sh - bbn, sh, bbn))
    t = pd.DataFrame(
        rows, columns=["wall", "block", "ok", "failed_history", "failed_bbn"]
    )
    t["failed"] = t.failed_history + t.failed_bbn
    ylab = [f"{r.wall}  {r.block}" for r in t.itertuples()]
    y = np.arange(len(t))
    for i, r in enumerate(t.itertuples()):
        c = WALL_COLOR[r.wall]
        ax.barh(i, r.ok, height=0.62, color=c, edgecolor="white")
        ax.barh(
            i,
            r.failed_history,
            left=r.ok,
            height=0.62,
            color="white",
            edgecolor=c,
            hatch="////",
            linewidth=0.8,
        )
        ax.barh(
            i,
            r.failed_bbn,
            left=r.ok + r.failed_history,
            height=0.62,
            color="white",
            edgecolor=c,
            hatch="....",
            linewidth=0.8,
        )
        ax.text(
            r.ok + r.failed + 30,
            i,
            f"{r.ok + r.failed:,} ({r.failed} failed)",
            va="center",
            fontsize=7.5,
            color=MUTED,
        )
    ax.set_yticks(y, ylab, fontsize=8)
    ax.invert_yaxis()
    ax.set_xlim(0, t[["ok", "failed"]].sum(axis=1).max() * 1.38)
    ax.set_xlabel("histories")
    ax.legend(
        [
            plt.Rectangle((0, 0), 1, 1, facecolor=MUTED, edgecolor="white"),
            plt.Rectangle(
                (0, 0), 1, 1, facecolor="white", edgecolor=MUTED, hatch="////"
            ),
            plt.Rectangle(
                (0, 0), 1, 1, facecolor="white", edgecolor=MUTED, hatch="...."
            ),
        ],
        ["ok", "history failed", "BBN failed"],
        loc="lower right",
        fontsize=7.5,
    )
    tot = d.groupby("wall").size()
    ax.set_title(
        f"(a) {len(d):,} histories: exp {tot['exp']:,}, pl2 {tot['pl2']:,}, pl1 {tot['pl1']:,}"
    )
    ax.grid(axis="y", visible=False)

    # (b) per-history recorded integration time, CDF per wall (laboratory region, successes)
    ax = axes[1]
    lab = d[(d.failure == 0) & (d.block.isin(["A", "B"]))]
    for w in WALL_ORDER:
        v = np.sort(lab[lab.wall == w].integration_s.dropna().values)
        ax.step(
            v,
            np.arange(1, len(v) + 1) / len(v),
            where="post",
            color=WALL_COLOR[w],
            linewidth=2,
            label=f"{w}: median {np.median(v):.0f} s, 99% {np.quantile(v, 0.99):.0f} s",
        )
    ax.set_xscale("log")
    ax.set_xlabel(
        "integration time per history (s), final integration x tuning iterations"
    )
    ax.set_ylabel("fraction of histories")
    ax.set_title("(b) Cost per history, blocks A and B, successes")
    ax.legend(loc="lower right", fontsize=7.5)

    # (c) median cost against beta at laboratory M, one-loop law
    ax = axes[2]
    a = d[(d.block == "A") & (d.law == "one-loop") & (d.failure == 0)]
    for w in WALL_ORDER:
        m = a[a.wall == w].groupby("beta").integration_s.median()
        ax.plot(m.index, m.values, color=WALL_COLOR[w], linewidth=2, label=w)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks(
        [0.5, 0.75, 1, 1.5, 2, 3, 5, 10, 20],
        ["0.5", "0.75", "1", "1.5", "2", "3", "5", "10", "20"],
    )
    ax.minorticks_off()
    ax.set_xlabel("β")
    ax.set_ylabel("median integration time over A (s)")
    ax.set_title("(c) Where the cost is: laboratory M, one-loop law")
    ax.legend(loc="upper right", fontsize=7.5)

    fig.suptitle(
        "2026.14.0: the histories and what each cost",
        x=0.01,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    save(fig, "resources-histories")
    return t


# ---------------------------------------------------------------- figure 3: storage


def table_kind(name):
    if name in ("ScalarHistoryValue",) or name.startswith("ix_ScalarHistoryValue"):
        return "field samples"
    if name in ("AdiabaticHistoryValue",) or name.startswith(
        "ix_AdiabaticHistoryValue"
    ):
        return "adiabatic samples"
    if name in ("BBNDataValue",) or name.startswith("ix_BBNDataValue"):
        return "BBN samples"
    if name in ("ScalarHistoryBounce",) or name.startswith("ix_ScalarHistoryBounce"):
        return "bounce rows"
    if name == "redshift" or name.startswith("ix_redshift"):
        return "redshift grid"
    return "other"


STORE_KINDS = [
    "field samples",
    "adiabatic samples",
    "BBN samples",
    "bounce rows",
    "redshift grid",
    "other",
]

EXPORT_KIND = {
    "quanta_samples.csv": "quanta_samples.csv",
    "fields.csv.gz": "fields.csv(.gz)",
    "fields.csv": "fields.csv(.gz)",
    "bounces.csv": "bounces + quanta_bounces",
    "quanta_bounces.csv": "bounces + quanta_bounces",
    "pdf": "figures (pdf + png)",
    "png": "figures (pdf + png)",
}
EXPORT_KINDS = [
    "quanta_samples.csv",
    "fields.csv(.gz)",
    "bounces + quanta_bounces",
    "figures (pdf + png)",
    "other",
]


def figure_storage(d, tables, files, outputs):
    fig, axes = plt.subplots(1, 3, figsize=(14.5, 5.0), constrained_layout=True)

    # (a) the stores by content
    ax = axes[0]
    tables["kind"] = tables.name.map(table_kind)
    st = (
        tables.groupby(["store", "kind"])
        .bytes.sum()
        .unstack()
        .reindex(columns=STORE_KINDS)
        .fillna(0)
        / GIB
    )
    stores = ["inverse-power", "exponential", "reflections"]
    st = st.reindex(stores)
    left = np.zeros(len(stores))
    for i, k in enumerate(STORE_KINDS):
        ax.barh(
            range(len(stores)),
            st[k],
            left=left,
            height=0.62,
            color=SLOT[i],
            edgecolor="white",
            linewidth=1,
            label=k,
        )
        left += st[k].values
    fsize = files.groupby("store").bytes.sum().reindex(stores) / GIB
    for i, s in enumerate(stores):
        ax.text(
            fsize[s] + 0.25,
            i,
            f"{fsize[s]:.1f} GiB on disk",
            va="center",
            fontsize=7.5,
            color=MUTED,
        )
    ax.set_yticks(range(len(stores)), stores)
    ax.invert_yaxis()
    ax.set_xlim(0, fsize.max() * 1.35)
    ax.set_xlabel("GiB")
    ax.set_title(f"(a) Datastores: {files.bytes.sum() / GIB:.1f} GiB in 51 files")
    ax.grid(axis="y", visible=False)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=3, fontsize=7.5)

    # (b) the exports by kind, per wall (main and reflections roots together)
    ax = axes[1]
    o = outputs.copy()
    o["wall"] = o.root.str.replace("-reflections", "", regex=False)
    o["k"] = o.kind.map(EXPORT_KIND).fillna("other")
    ex = (
        o.groupby(["wall", "k"])
        .bytes.sum()
        .unstack()
        .reindex(columns=EXPORT_KINDS)
        .fillna(0)
        / GIB
    )
    ex = ex.reindex(WALL_ORDER)
    left = np.zeros(len(WALL_ORDER))
    for i, k in enumerate(EXPORT_KINDS):
        ax.barh(
            range(len(WALL_ORDER)),
            ex[k],
            left=left,
            height=0.62,
            color=SLOT[i],
            edgecolor="white",
            linewidth=1,
            label=k,
        )
        left += ex[k].values
    nfiles = o.groupby("wall").files.sum().reindex(WALL_ORDER)
    for i, w in enumerate(WALL_ORDER):
        ax.text(
            left[i] + 0.2,
            i,
            f"{left[i]:.1f} GiB, {nfiles[w]:,} files",
            va="center",
            fontsize=7.5,
            color=MUTED,
        )
    ax.set_yticks(range(len(WALL_ORDER)), WALL_ORDER)
    ax.invert_yaxis()
    ax.set_xlim(0, left.max() * 1.45)
    ax.set_xlabel("GiB")
    ax.set_title(
        f"(b) Derived products: {o.bytes.sum() / GIB:.1f} GiB in {o.files.sum():,} files"
    )
    ax.grid(axis="y", visible=False)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=3, fontsize=7.5)

    # (c) per history, by wall
    ax = axes[2]
    # store bytes per wall: the exponential store's and the reflection store's exp histories to exp, the power store
    # split between n = 1 and 2 by their sample rows (the samples are 3/4 of it)
    per = {}
    sr = d.groupby(["store", "wall"]).sample_rows.sum()
    store_b = files.groupby("store").bytes.sum()
    for w in WALL_ORDER:
        b = 0.0
        for s in ("exponential", "inverse-power", "reflections"):
            if (s, w) in sr.index:
                b += store_b[s] * sr[(s, w)] / sr[s].sum()
        per[w] = b
    nw = d.groupby("wall").size().reindex(WALL_ORDER)
    store_mb = np.array([per[w] / nw[w] / 1e6 for w in WALL_ORDER])
    export_mb = np.array([o[o.wall == w].bytes.sum() / nw[w] / 1e6 for w in WALL_ORDER])
    y = np.arange(len(WALL_ORDER))
    ax.barh(
        y - 0.19,
        store_mb,
        height=0.36,
        color="#4a4a48",
        edgecolor="white",
        label="datastore",
    )
    ax.barh(
        y + 0.19,
        export_mb,
        height=0.36,
        color="#c9c8c0",
        edgecolor="white",
        label="derived products",
    )
    for i in range(len(WALL_ORDER)):
        ax.text(
            store_mb[i] + 0.05,
            y[i] - 0.19,
            f"{store_mb[i]:.2f} MB",
            va="center",
            fontsize=7.5,
            color=MUTED,
        )
        ax.text(
            export_mb[i] + 0.05,
            y[i] + 0.19,
            f"{export_mb[i]:.2f} MB",
            va="center",
            fontsize=7.5,
            color=MUTED,
        )
    ax.set_yticks(y, WALL_ORDER)
    ax.invert_yaxis()
    ax.set_xlim(0, max(export_mb.max(), store_mb.max()) * 1.3)
    ax.set_xlabel("MB per history (10⁶ bytes)")
    ax.set_title("(c) Per history")
    ax.grid(axis="y", visible=False)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=3, fontsize=7.5)

    fig.suptitle(
        "2026.14.0: storage on the Mac Studio",
        x=0.01,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    save(fig, "resources-storage")
    return st, ex, store_mb, export_mb


def save(fig, stem):
    fig.savefig(
        HERE / f"{stem}.pdf", metadata={"Title": f"{stem}, science run 2026.14.0"}
    )
    fig.savefig(HERE / f"{stem}.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------- the summary


def summary(
    d,
    steps,
    stages,
    reserved,
    rec,
    tail,
    hist_table,
    st,
    ex,
    store_mb,
    export_mb,
    files,
    outputs,
):
    say("2026.14.0 resources (resources.py)")
    say()
    ph = phases(steps)
    t0, t1 = ph[0][2], steps.t.max()
    build = sum((b - a).total_seconds() for n, k, a, b in ph if k in ("exp", "pl"))
    exports = sum((b - a).total_seconds() for n, k, a, b in ph if k == "export")
    say(
        f"end to end: {(t1 - t0).total_seconds() / 3600:.2f} h; build {build / 3600:.2f} h; inventories "
        f"{[(b - a).total_seconds() / 60 for n, k, a, b in ph if k == 'other'][0]:.1f} min; exports {exports / 3600:.2f} h"
    )
    for n, k, a, b in ph:
        say(f"  {n:28s} {(b - a).total_seconds() / 3600:6.2f} h")
    say()
    say("stage wall-clock (h), from the slot logs:")
    sw = stages.groupby("stage").sec.sum() / 3600
    for key, name, _ in STAGES:
        say(f"  {name:20s} {sw[key]:6.2f} h  reserved {sw[key] * RAY_CPUS:6.1f} CPU-h")
    say(
        f"  total               {sw.sum():6.2f} h  reserved {sw.sum() * RAY_CPUS:6.1f} CPU-h"
    )
    say()
    say("recorded CPU (h), lower bounds:")
    for (key, name, _), r, v in zip(STAGES, reserved, rec):
        say(f"  {name:20s} {v:6.2f}  ({v / r:.0%} of reserved)")
    say(
        f"  total               {sum(rec):6.2f}  ({sum(rec) / sum(reserved):.0%} of reserved)"
    )
    by_w = d.groupby("wall")[["integration_s", "bbn_s", "adiabatic_time"]].sum() / 3600
    say("  by wall (integration, BBN, adiabatic):")
    for w in WALL_ORDER:
        r = by_w.loc[w]
        say(f"    {w}: {r.integration_s:.2f}, {r.bbn_s:.2f}, {r.adiabatic_time:.2f}")
    say(
        f"  PRyMordial's share of the BBN time: {d.bbn_PRyM_time.sum() / d.bbn_s.sum():.4f}"
    )
    say(
        f"  final integration alone: {d.compute_time.sum() / 3600:.2f} h; tuning iterations > 1 in "
        f"{(d.tuning_iterations > 1).sum()} histories"
    )
    say()
    tail = tail[
        ~tail.slot.str.startswith("R-")
    ]  # as figure (d): rows of 1 to 3 histories have no tail to measure
    tt = tail.groupby("slot")[["bulk_h", "tail_h"]].sum()
    say(
        f"scalar-history stage, blocks A-D: bulk {tt.bulk_h.sum():.2f} h, tail (last 5% of each row) {tt.tail_h.sum():.2f} h, "
        f"{tt.tail_h.sum() / (tt.bulk_h.sum() + tt.tail_h.sum()):.1%}; rows {len(tail)}"
    )
    say(f"  rows whose tail is over half the row: {(tail.tail_h > tail.bulk_h).sum()}")
    for s, r in tt.iterrows():
        say(f"  {s:24s} bulk {r.bulk_h:5.2f} h  tail {r.tail_h:5.2f} h")
    say()
    say("histories (ok; failed: the history, or its BBN):")
    for r in hist_table.itertuples():
        say(
            f"  {r.wall:4s} {r.block}: {r.ok:5d} ok, {r.failed_history:4d} history failed, {r.failed_bbn:4d} BBN failed"
        )
    for w in WALL_ORDER:
        g = hist_table[hist_table.wall == w]
        say(
            f"  {w}: {g[['ok', 'failed']].sum().sum()} histories, {g.failed_history.sum()} + {g.failed_bbn.sum()} failed"
        )
    say(
        f"  all: {len(d)} histories, {hist_table.failed_history.sum()} + {hist_table.failed_bbn.sum()} = {hist_table.failed.sum()} failed"
    )
    say()
    lab = d[(d.failure == 0) & (d.block.isin(["A", "B"]))]
    say("per history, blocks A and B, successes:")
    for w in WALL_ORDER:
        g = lab[lab.wall == w]
        say(
            f"  {w}: integration median {g.integration_s.median():.1f} s, mean {g.integration_s.mean():.1f} s, "
            f"99% {g.integration_s.quantile(0.99):.0f} s, max {g.integration_s.max():.0f} s; RHS evaluations median "
            f"{g.RHS_evaluations.median():.0f}, mean RHS time {g.mean_RHS_time.median() * 1e6:.1f} us; "
            f"bounce rows median {g.bounce_rows.median():.0f}; sample rows median {g.sample_rows.median():.0f}; "
            f"BBN median {g.bbn_s.median():.1f} s"
        )
    say(
        f"  share of the integration time in the top 1% of histories: "
        f"{lab.integration_s.nlargest(int(len(lab) * 0.01)).sum() / lab.integration_s.sum():.1%}"
    )
    a = d[(d.block == "A") & (d.law == "one-loop") & (d.failure == 0)]
    for w in WALL_ORDER:
        m = a[a.wall == w].groupby("beta").integration_s.median()
        say(
            f"  {w}, block A one-loop: median over A by beta, max {m.max():.0f} s at beta = {m.idxmax()}, "
            f"min {m.min():.1f} s at beta = {m.idxmin()}"
        )
    say()
    say("datastores (GiB, dbstat by content):")
    say(st.round(3).to_string())
    say(
        f"  files on disk (GiB): {(files.groupby('store').bytes.sum() / GIB).round(3).to_dict()}; total {files.bytes.sum() / GIB:.2f}"
    )
    say()
    say("derived products (GiB, by wall, main and reflections roots):")
    say(ex.round(3).to_string())
    say(f"  total {outputs.bytes.sum() / GIB:.2f} GiB in {outputs.files.sum()} files")
    say()
    say(
        "per history (MB = 1e6 bytes): "
        + ", ".join(
            f"{w} store {s:.2f}, products {e:.2f}"
            for w, s, e in zip(WALL_ORDER, store_mb, export_mb)
        )
    )
    say(
        f"throughput: {len(d) / (build / 3600):.0f} histories per hour of build, "
        f"{len(d) / (build / 3600) / RAY_CPUS:.1f} per CPU-hour reserved"
    )


# ---------------------------------------------------------------- provenance


def git(*args):
    return subprocess.run(
        ["git", *args], cwd=REPO, capture_output=True, text=True
    ).stdout.strip()


def sha256(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def main():
    d = load_histories()
    steps = load_steps()
    stages = load_stage_times()
    tables = pd.read_csv(INPUTS / "tables.csv")
    files = pd.read_csv(INPUTS / "files.csv")
    outputs = pd.read_csv(INPUTS / "outputs.csv")

    reserved, rec, tail = figure_time(d, steps, stages)
    hist_table = figure_histories(d)
    st, ex, store_mb, export_mb = figure_storage(d, tables, files, outputs)
    summary(
        d,
        steps,
        stages,
        reserved,
        rec,
        tail,
        hist_table,
        st,
        ex,
        store_mb,
        export_mb,
        files,
        outputs,
    )

    (HERE / "resources.txt").write_text("\n".join(OUT_LINES) + "\n")
    prov = {
        "script": "resources.py",
        "script_sha256": sha256(Path(__file__)),
        "git_sha": git("rev-parse", "HEAD"),
        "git_dirty": bool(git("status", "--porcelain")),
        "inputs": {
            str(p.relative_to(RUN_DIR)): sha256(p)
            for p in sorted(list(INPUTS.glob("*")) + sorted(LOGS.glob("[ABCDR]-*.log")))
        },
        "ray_cpus": RAY_CPUS,
    }
    (HERE / "resources.provenance.json").write_text(json.dumps(prov, indent=2) + "\n")


if __name__ == "__main__":
    main()
