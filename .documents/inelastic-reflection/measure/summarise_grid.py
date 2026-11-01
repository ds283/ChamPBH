"""
Summarise the grid of inelastic-reflection prompt 11 (§1.1, §2.1-§2.5 and §2.9; README §8.2 P41-P46).

    ./venv/bin/python .documents/inelastic-reflection/measure/summarise_grid.py [--grid DIR] [--sm FILE] [--csv FILE] [--md FILE]

Defaults: --grid var/inelastic-reflection-11/grid (the points' <slug>.txt, written by run_grid.py),
--sm var/inelastic-reflection-11/sm-baseline.txt (`tools/bbn_from_store.py --sm-baseline
--small-network`'s output, P43's Standard Model reference), --csv var/inelastic-reflection-11/grid.csv
and --md var/inelastic-reflection-11/grid-tables.md. Stdlib only. It reads the points' outputs and
writes only those two files, under var/inelastic-reflection-11/.

`grid.csv` has one row per point of `points.POINTS`, in that order, with every field of the point's
`history`, `tuning`, `bounce`, `dissipation`, `fixed_T`, `ratio` and `bbn` lines, the header's and
footer's (the start, the end, the load, the exit status, the wall), the tune lines' count and summed
wall, and the warnings (`!!` lines) counted by kind. A point with no `.txt` is a row saying so.

`grid-tables.md` holds the tables of prompt 11 §2.1-§2.5 and §2.9. It reads; it computes nothing but
ratios, differences, orders and unit conversions (ln T_J in GeV to T_J in MeV).
"""

import argparse
import csv
import math
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from points import (
    ALPHAS,
    BETAS,
    DISSIPATION_M,
    EPSILONS,
    NATURAL_M,
    POINTS,
)  # noqa: E402

WINDOWS = ("0.3,1", "1,3", "3,10", "10,100")
REF_NATURAL_M = 1.0e-7

WARNING_KINDS = (
    ("nearly_elastic", "nearly elastic"),
    ("regime_unassessed", "regime could not be assessed"),
    ("natural_turning_point", "natural mode: the classical turning point lies below"),
    ("natural_wall_unassessed", "natural mode: the wall could not be assessed"),
    ("birth_margin", "not relativistic at its birth"),
    ("reflection_margin", "not relativistic at a later bounce"),
    ("tracker_margin", "not relativistic on the tracker"),
    ("margins_failed", "margins could not be computed"),
)


def _num(s):
    if s is None:
        return None
    if s == "None":
        return None
    try:
        if re.fullmatch(r"[+-]?\d+", s):
            return int(s)
        return float(s)
    except ValueError:
        return s


def _kv(text):
    out = {}
    for part in text.split(" "):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k] = v
    return out


def parse(path: Path) -> dict:
    row = {}
    text = path.read_text(errors="replace")
    lines = text.splitlines()
    ups = []
    tune_n, tune_wall = 0, 0.0
    warnings = []
    for line in lines:
        if line.startswith("## command: "):
            row["command"] = line[len("## command: ") :]
        elif line.startswith("## git rev-parse HEAD: "):
            row["git_head"] = line.split(": ", 1)[1]
        elif line.startswith("## start (UTC): "):
            row["start_utc"] = line.split(": ", 1)[1]
        elif line.startswith("## end (UTC): "):
            row["end_utc"] = line.split(": ", 1)[1]
        elif line.startswith("## uptime: "):
            m = re.search(r"load averages?: ([\d.]+),? ([\d.]+),? ([\d.]+)", line)
            ups.append(m.group(1) if m else "")
        elif line.startswith("## exit status: "):
            row["exit_status"] = int(line.split(": ", 1)[1])
        elif line.startswith("## wall: "):
            row["wall_s"] = float(line.split(": ", 1)[1].split()[0])
        elif line.startswith("## timed out"):
            row["timed_out"] = 1
        elif line.startswith("epsilon="):
            row["epsilon_printed"] = line.split("=", 1)[1]
        elif line.startswith("-- tune "):
            tune_n += 1
            w = re.search(r" wall=([\d.]+)", line)
            if w:
                tune_wall += float(w.group(1))
        elif line.startswith("!!"):
            warnings.append(line)
        elif line.startswith("history "):
            body = line.split(": ", 1)[1]
            if body.startswith("FAILURE"):
                row["history_failure"] = 1
                m = re.search(r"wall=([\d.]+) s reason=(.*)$", body)
                row["history_wall_s"] = m.group(1) if m else ""
                row["failure_reason"] = m.group(2) if m else body
            else:
                kv = _kv(body.replace(" s", ""))
                for k, v in kv.items():
                    row[f"history_{k}"] = v
        elif line.startswith("tuning "):
            for k, v in _kv(line.split(": ", 1)[1]).items():
                row[f"tuning_{k}"] = v
        elif line.startswith("bounce "):
            body = line.split(": ", 1)[1]
            if body.strip() == "none":
                row["bounce_none"] = 1
            else:
                for k, v in _kv(body.replace(" MeV", "")).items():
                    row[f"bounce_{k}"] = v
        elif line.startswith("dissipation "):
            for k, v in _kv(line.split(": ", 1)[1]).items():
                row[f"dissipation_{k}"] = v
        elif line.startswith("fixed_T "):
            body = line.split(": ", 1)[1]
            for part in body.split(" | "):
                name, rest = part.split(" ", 1)
                for k, v in _kv(rest).items():
                    row[f"fixed_T_{name}_{k}"] = v
        elif line.startswith("ratio "):
            m = re.match(r"ratio [^\[]*\[([\d.]+),([\d.]+)\) keV: (.*)$", line)
            if m:
                w = f"{m.group(1)},{m.group(2)}"
                rest = m.group(3)
                if "=" in rest:
                    for k, v in _kv(rest).items():
                        row[f"ratio_{w}_{k}"] = v
                else:
                    row[f"ratio_{w}_n"] = rest.split()[0]
        elif line.startswith("bbn "):
            body = line.split(": ", 1)[1]
            if body.startswith("FAILURE"):
                row["bbn_failure"] = 1
                m = re.search(r"reason=(.*)$", body)
                row["bbn_failure_reason"] = m.group(1) if m else body
                for k, v in _kv(body.split(" reason=")[0]).items():
                    row[f"bbn_{k}"] = v
            else:
                for k, v in _kv(body.replace(" s", "")).items():
                    row[f"bbn_{k}"] = v
    row["load_start"] = ups[0] if ups else ""
    row["load_end"] = ups[1] if len(ups) > 1 else ""
    row["tune_lines"] = tune_n
    row["tune_wall_sum_s"] = f"{tune_wall:.1f}"
    row["warnings"] = len(warnings)
    other = 0
    for line in warnings:
        for key, needle in WARNING_KINDS:
            if needle in line:
                row[f"warn_{key}"] = row.get(f"warn_{key}", 0) + 1
                break
        else:
            other += 1
    row["warn_other"] = other
    row["warning_lines"] = " || ".join(w[:200] for w in warnings)
    return row


def sm_baseline(path: Path):
    if not path.exists():
        return None, None
    m = re.search(r"SM\s+Yp=(\S+) DoH=(\S+)", path.read_text())
    if not m:
        return None, None
    return float(m.group(1)), float(m.group(2))


def g(x, spec=".4g"):
    if x is None or x == "":
        return "—"
    if isinstance(x, str):
        x = _num(x)
        if x is None:
            return "None"
        if isinstance(x, str):
            return x
    return format(x, spec)


def f(row, key):
    v = row.get(key)
    return _num(v) if isinstance(v, str) else v


def T_MeV(row, key):
    v = f(row, key)
    return None if v is None else math.exp(v) * 1.0e3


def converged(row):
    return row.get("exit_status") == 0 or (
        "tuning_residual_H" in row and "history_failure" not in row
    )


def status(row):
    if not row.get("_present"):
        return "no output"
    if row.get("timed_out"):
        return "timed out"
    if row.get("history_failure"):
        return "history FAILED"
    if row.get("bbn_failure"):
        return "BBN failed"
    if row.get("exit_status") != 0:
        return f"exit {row.get('exit_status')}"
    return "ok"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument(
        "--grid", type=Path, default=Path("var/inelastic-reflection-11/grid")
    )
    p.add_argument(
        "--sm", type=Path, default=Path("var/inelastic-reflection-11/sm-baseline.txt")
    )
    p.add_argument(
        "--csv", type=Path, default=Path("var/inelastic-reflection-11/grid.csv")
    )
    p.add_argument(
        "--md", type=Path, default=Path("var/inelastic-reflection-11/grid-tables.md")
    )
    args = p.parse_args(argv)

    rows = {}
    for pt in POINTS:
        path = args.grid / f"{pt.slug}.txt"
        r = {
            "slug": pt.slug,
            "kind": pt.kind,
            "M": pt.M,
            "beta": pt.beta,
            "alpha": pt.alpha,
        }
        r["epsilon"] = (
            pt.epsilon
            if pt.epsilon is not None
            else 1.0e-6 if pt.kind != "natural" else ""
        )
        if path.exists():
            r.update(parse(path))
            r["_present"] = 1
        r["status"] = status(r)
        rows[pt.slug] = r

    fields = []
    for r in rows.values():
        for k in r:
            if k not in fields and not k.startswith("_"):
                fields.append(k)
    with open(args.csv, "w", newline="") as fh:
        w = csv.DictWriter(
            fh, fieldnames=fields, extrasaction="ignore", lineterminator="\n"
        )
        w.writeheader()
        for r in rows.values():
            w.writerow({k: r.get(k, "") for k in fields})

    sm_Yp, sm_DH = sm_baseline(args.sm)

    def slug_dis(M, b, a):
        return f"dis-M{M:g}-b{b:g}-a{a:g}"

    def slug_nat(M, b):
        return f"nat-M{M:g}-b{b:g}"

    def slug_eps(e, b):
        return f"eps{e:g}-M4e-31-b{b:g}-a0.1"

    md = []
    out = md.append
    out("# inelastic-reflection 11 — the grid's tables")
    out("")
    out(
        f"Points: {len(rows)}; with output {sum(1 for r in rows.values() if r.get('_present'))}; "
        f"status ok {sum(1 for r in rows.values() if r['status'] == 'ok')}; "
        f"other: {', '.join(f'{r['slug']} ({r['status']})' for r in rows.values() if r['status'] != 'ok') or 'none'}."
    )
    out(
        f"Standard Model reference (P43, small network): Yp = {g(sm_Yp, '.10g')}, D/H = {g(sm_DH, '.10g')}."
    )
    out("")

    # 2.1
    out("## 2.1 Dissipation mode")
    out("")
    out("### Cost and convergence")
    out("")
    out(
        "| M/M_P | β | α | status | integrations | residual_H | residual_φ | wall (s) | tune wall (s) | steps | RHS | load at start | warnings |"
    )
    out("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for M in DISSIPATION_M:
        for b in BETAS:
            for a in ALPHAS:
                r = rows[slug_dis(M, b, a)]
                wk = ", ".join(
                    f"{k[5:]} {r[k]}"
                    for k in r
                    if k.startswith("warn_") and r[k] and k != "warn_other"
                )
                if r.get("warn_other"):
                    wk += (", " if wk else "") + f"other {r['warn_other']}"
                out(
                    f"| {M:g} | {b:g} | {a:g} | {r['status']} | {g(r.get('history_integrations'), 'd') if r.get('history_integrations') else '—'} | "
                    f"{g(r.get('tuning_residual_H'), '+.3e')} | {g(r.get('tuning_residual_phi'), '+.3e')} | "
                    f"{g(r.get('wall_s'), '.1f')} | {g(r.get('tune_wall_sum_s'), '.1f')} | {g(r.get('history_accepted_steps'), 'd') if r.get('history_accepted_steps') else '—'} | "
                    f"{g(r.get('history_RHS'), 'd') if r.get('history_RHS') else '—'} | {r.get('load_start', '')} | {r.get('warnings', '—')}{' (' + wk + ')' if wk else ''} |"
                )
    out("")
    out("### The `dissipation` line: counts, the drain, ΔN_eff, 𝓡, parking and release")
    out("")
    out(
        "| M/M_P | β | α | inj. | res. | unres. | injected_fraction | ρ_χ/ρ_R 1 MeV | ρ_χ/ρ_R 70 keV | ρ_χ/ρ_R today | ΔN_eff 1 MeV | ΔN_eff today | ln 𝓡 first | park N | park T_J (MeV) | release N | release T_J (MeV) |"
    )
    out("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for M in DISSIPATION_M:
        for b in BETAS:
            for a in ALPHAS:
                r = rows[slug_dis(M, b, a)]
                d = lambda k: r.get(f"dissipation_{k}")  # noqa: E731
                out(
                    f"| {M:g} | {b:g} | {a:g} | {g(d('injections'))} | {g(d('resolved_candidates'))} | {g(d('unresolved_candidates'))} | "
                    f"{g(d('injected_fraction'), '.6g')} | {g(d('rho_chi_ratio_1MeV'))} | {g(d('rho_chi_ratio_70keV'))} | {g(d('rho_chi_ratio_today'))} | "
                    f"{g(d('Delta_N_eff_1MeV'), '.5g')} | {g(d('Delta_N_eff_today'), '.5g')} | {g(d('log_R_first_impact'))} | "
                    f"{g(d('parking_N'), '.5g')} | {g(T_MeV(r, 'dissipation_parking_log_T_Jordan_GeV'), '.4g')} | "
                    f"{g(d('release_N'), '.5g')} | {g(T_MeV(r, 'dissipation_release_log_T_Jordan_GeV'), '.4g')} |"
                )
    out("")
    out(
        "### The margins (ln p/m; the worst of each kind, its N, the failing share) and the unassessed share"
    )
    out("")
    out(
        "| M/M_P | β | α | birth | N | share | reflection | N | share | tracker | N | share | unassessed |"
    )
    out("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    present_all = True
    for M in DISSIPATION_M:
        for b in BETAS:
            for a in ALPHAS:
                r = rows[slug_dis(M, b, a)]
                keys = (
                    "worst_birth_margin",
                    "worst_birth_margin_N",
                    "birth_failing_share",
                    "worst_reflection_margin",
                    "worst_reflection_margin_N",
                    "reflection_failing_share",
                    "worst_tracker_margin",
                    "worst_tracker_margin_N",
                    "tracker_failing_share",
                    "unassessed_share",
                )
                if r["status"] == "ok" or r.get("dissipation_injections") is not None:
                    present_all &= all(f"dissipation_{k}" in r for k in keys)
                out(
                    f"| {M:g} | {b:g} | {a:g} | "
                    + " | ".join(g(r.get(f"dissipation_{k}"), ".4g") for k in keys)
                    + " |"
                )
    out("")
    out(
        f"Every success has the ten margin fields present (a value or `None`): **{'yes' if present_all else 'NO'}**."
    )
    out("")
    out("### BBN, against P43's two references")
    out("")
    out(
        "| M/M_P | β | α | Yp | D/H (×10⁵) | Yp − Yp_SM | D/H − D/H_SM | Yp − Yp_nat(10⁻⁷) | D/H − D/H_nat(10⁻⁷) |"
    )
    out("|---|---|---|---|---|---|---|---|---|")
    for M in DISSIPATION_M:
        for b in BETAS:
            nat = rows[slug_nat(REF_NATURAL_M, b)]
            nYp, nDH = f(nat, "bbn_Yp"), f(nat, "bbn_DoH")
            for a in ALPHAS:
                r = rows[slug_dis(M, b, a)]
                Yp, DH = f(r, "bbn_Yp"), f(r, "bbn_DoH")
                dif = lambda x, y: (
                    None if (x is None or y is None) else x - y
                )  # noqa: E731
                out(
                    f"| {M:g} | {b:g} | {a:g} | {g(Yp, '.7g')} | {g(DH, '.7g')} | {g(dif(Yp, sm_Yp), '+.3e')} | {g(dif(DH, sm_DH), '+.3e')} | "
                    f"{g(dif(Yp, nYp), '+.3e')} | {g(dif(DH, nDH), '+.3e')} |"
                    + (
                        f" {r.get('bbn_failure_reason', '')[:120]}"
                        if r.get("bbn_failure")
                        else ""
                    )
                )
    out("")

    # 2.2
    out("## 2.2 The drain's independence of M (gate Measure)")
    out("")
    out(
        "| β | α | injected_fraction 10⁻¹² | injected_fraction 4×10⁻³¹ | rel. diff. | ΔN_eff today 10⁻¹² | ΔN_eff today 4×10⁻³¹ | rel. diff. |"
    )
    out("|---|---|---|---|---|---|---|---|")
    worst_if, worst_dn = 0.0, 0.0
    n_pairs = 0
    for b in BETAS:
        for a in ALPHAS:
            r1, r2 = rows[slug_dis(1e-12, b, a)], rows[slug_dis(4e-31, b, a)]
            i1, i2 = f(r1, "dissipation_injected_fraction"), f(
                r2, "dissipation_injected_fraction"
            )
            d1, d2 = f(r1, "dissipation_Delta_N_eff_today"), f(
                r2, "dissipation_Delta_N_eff_today"
            )
            rel = lambda x, y: (
                None if (x is None or y is None or y == 0) else abs(x - y) / abs(y)
            )  # noqa: E731
            ri, rd = rel(i1, i2), rel(d1, d2)
            if ri is not None:
                worst_if = max(worst_if, ri)
                n_pairs += 1
            if rd is not None:
                worst_dn = max(worst_dn, rd)
            out(
                f"| {b:g} | {a:g} | {g(i1, '.6g')} | {g(i2, '.6g')} | {g(ri, '.2e')} | {g(d1, '.6g')} | {g(d2, '.6g')} | {g(rd, '.2e')} |"
            )
    out("")
    out(
        f"Largest relative difference over {n_pairs} pairs: injected_fraction **{worst_if:.2e}**, ΔN_eff today {worst_dn:.2e}; "
        f"limit 10⁻³: **{'holds' if n_pairs == len(BETAS) * len(ALPHAS) and worst_if <= 1e-3 else 'FAILS or incomplete'}**. "
        "The dissipation line prints `:.6g`, so a relative difference below about 5×10⁻⁶ is not resolved."
    )
    out("")

    # 2.3
    out("## 2.3 ΔN_eff against α (gate Measure)")
    out("")
    out(
        "| M/M_P | β | ΔN_eff today at α = "
        + ", ".join(f"{a:g}" for a in ALPHAS)
        + " | ΔN_eff 1 MeV at the same α | rises strictly | ΔN_eff today/α | (max − min)/mean of ΔN_eff today/α |"
    )
    out("|---|---|---|---|---|---|---|")
    all_rise = True
    for M in DISSIPATION_M:
        for b in BETAS:
            today, mev, okpts = [], [], []
            for a in ALPHAS:
                r = rows[slug_dis(M, b, a)]
                t = f(r, "dissipation_Delta_N_eff_today")
                today.append(t)
                mev.append(f(r, "dissipation_Delta_N_eff_1MeV"))
                if t is not None:
                    okpts.append((a, t))
            rise = all(y2 > y1 for (_, y1), (_, y2) in zip(okpts, okpts[1:]))
            all_rise &= rise
            per = [t / a for a, t in okpts]
            spread = (max(per) - min(per)) / (sum(per) / len(per)) if per else None
            out(
                f"| {M:g} | {b:g} | {', '.join(g(x, '.5g') for x in today)} | {', '.join(g(x, '.5g') for x in mev)} | "
                f"{'yes' if rise else 'NO'} ({len(okpts)} of {len(ALPHAS)}) | {', '.join(g(x, '.4g') for x in per)} | {g(spread, '.3g')} |"
            )
    out("")
    out(
        f"ΔN_eff today rises strictly with α at every (M, β): **{'yes' if all_rise else 'NO'}**."
    )
    out("")

    # 2.4
    out("## 2.4 Natural mode")
    out("")
    out(
        "| M/M_P | β | status | integrations | residual_H | residual_φ | wall (s) | steps | RHS | warnings | P23 warning | Yp | D/H (×10⁵) |"
    )
    out("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for M in NATURAL_M:
        for b in BETAS:
            r = rows[slug_nat(M, b)]
            out(
                f"| {M:g} | {b:g} | {r['status']} | {r.get('history_integrations', '—')} | {g(r.get('tuning_residual_H'), '+.3e')} | "
                f"{g(r.get('tuning_residual_phi'), '+.3e')} | {g(r.get('wall_s'), '.1f')} | {r.get('history_accepted_steps', '—')} | {r.get('history_RHS', '—')} | "
                f"{r.get('warnings', '—')} | {r.get('warn_natural_turning_point', 0)} | {g(f(r, 'bbn_Yp'), '.10g')} | {g(f(r, 'bbn_DoH'), '.10g')} |"
            )
    out("")
    p23 = all(rows[slug_nat(1e-7, b)].get("warn_natural_turning_point") for b in BETAS)
    tcmb = all(
        rows[slug_nat(1e-7, b)].get("tuning_residual_H") is not None for b in BETAS
    )
    out(
        f"At M = 10⁻⁷: P23's warning printed at every β: **{'yes' if p23 else 'no'}**; every history tuned at T_CMB (a `tuning` line, so reached T_CMB): **{'yes' if tcmb else 'no'}**."
    )
    out("")

    # 2.5
    out("## 2.5 The parking ε (M = 4×10⁻³¹, α = 0.1)")
    out("")
    out(
        "| β | ε | status | park N | park T_J (MeV) | release N | release T_J (MeV) | injected_fraction | ΔN_eff today | ΔN_eff 1 MeV | Yp | D/H (×10⁵) | wall (s) | steps |"
    )
    out("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    eps_rows = {}
    for b in BETAS:
        for e in (EPSILONS[0], 1.0e-6, EPSILONS[1]):
            r = rows[slug_dis(4e-31, b, 0.1)] if e == 1.0e-6 else rows[slug_eps(e, b)]
            eps_rows[(b, e)] = r
            out(
                f"| {b:g} | {e:g} | {r['status']} | {g(f(r, 'dissipation_parking_N'), '.6g')} | {g(T_MeV(r, 'dissipation_parking_log_T_Jordan_GeV'), '.5g')} | "
                f"{g(f(r, 'dissipation_release_N'), '.6g')} | {g(T_MeV(r, 'dissipation_release_log_T_Jordan_GeV'), '.5g')} | "
                f"{g(f(r, 'dissipation_injected_fraction'), '.6g')} | {g(f(r, 'dissipation_Delta_N_eff_today'), '.6g')} | {g(f(r, 'dissipation_Delta_N_eff_1MeV'), '.6g')} | "
                f"{g(f(r, 'bbn_Yp'), '.10g')} | {g(f(r, 'bbn_DoH'), '.10g')} | {g(r.get('wall_s'), '.1f')} | {r.get('history_accepted_steps', '—')} |"
            )
    out("")
    out("Differences from ε = 10⁻⁶ (the grid's own point):")
    out("")
    out(
        "| β | ε | Δ park N | Δ injected_fraction (rel.) | Δ ΔN_eff today (rel.) | Δ ΔN_eff 1 MeV (rel.) | Δ Yp | Δ D/H |"
    )
    out("|---|---|---|---|---|---|---|---|")
    for b in BETAS:
        r0 = eps_rows[(b, 1.0e-6)]
        for e in EPSILONS:
            r = eps_rows[(b, e)]

            def dd(k, rel=False):
                x, y = f(r, k), f(r0, k)
                if x is None or y is None:
                    return None
                return (x - y) / y if rel and y != 0 else x - y

            out(
                f"| {b:g} | {e:g} | {g(dd('dissipation_parking_N'), '+.4g')} | {g(dd('dissipation_injected_fraction', True), '+.3e')} | "
                f"{g(dd('dissipation_Delta_N_eff_today', True), '+.3e')} | {g(dd('dissipation_Delta_N_eff_1MeV', True), '+.3e')} | "
                f"{g(dd('bbn_Yp'), '+.3e')} | {g(dd('bbn_DoH'), '+.3e')} |"
            )
    out("")

    # 2.9
    out("## 2.9 P14: the BBN spline across injections (the `ratio` lines)")
    out("")
    out(
        "Each dissipation point's `ratio` lines beside natural mode's at M = 10⁻⁷, the same β: n, median and rms_step per window [lo, hi) keV, and rms_step / rms_step_nat."
    )
    out("")
    out(
        "| M/M_P | β | α | "
        + " | ".join(f"[{w}) n, median, rms_step, ×nat" for w in WINDOWS)
        + " |"
    )
    out("|---|---|---|" + "---|" * len(WINDOWS))
    for b in BETAS:
        nat = rows[slug_nat(REF_NATURAL_M, b)]
        cells = []
        for w in WINDOWS:
            cells.append(
                f"{g(f(nat, f'ratio_{w}_n'))}, {g(f(nat, f'ratio_{w}_median'))}, {g(f(nat, f'ratio_{w}_rms_step'))}, 1"
            )
        out(f"| nat 1e-07 | {b:g} | 0 | " + " | ".join(cells) + " |")
        for M in DISSIPATION_M:
            for a in ALPHAS:
                r = rows[slug_dis(M, b, a)]
                cells = []
                for w in WINDOWS:
                    rs, rn = f(r, f"ratio_{w}_rms_step"), f(nat, f"ratio_{w}_rms_step")
                    x = None if (rs is None or not rn) else rs / rn
                    cells.append(
                        f"{g(f(r, f'ratio_{w}_n'))}, {g(f(r, f'ratio_{w}_median'))}, {g(rs)}, {g(x, '.3g')}"
                    )
                out(f"| {M:g} | {b:g} | {a:g} | " + " | ".join(cells) + " |")
    out("")

    args.md.write_text("\n".join(md) + "\n")
    print(f"wrote {args.csv} ({len(rows)} rows, {len(fields)} fields) and {args.md}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
