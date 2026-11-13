"""
The bath drift of [09-the-bath-drifts-from-g-rho-T4-by-about-1e-3], re-measured on the new w table
(electroweak-eos prompt 02, §1.3; README §9.2 P20). Measurements, not bounds.

Two parts.

`tables` -- the issue's tables-only integral, with no history:

    Delta(T) = int_{ln T}^{ln T*} [4 + d ln g_rho/d ln T - 3(1 + w)(1 + x)] d ln T,
    x = (1/3) d ln g_s/d ln T,

the drift of ln rho_R from ln((pi^2/30) g_rho T^4) that the history's ODE would accumulate from T*
if it followed the tables alone. Every function is QCD_Cosmology's own (G_rho, G_s, dG_rho_dlogT,
dG_s_dlogT, w), in GeV units, as thermo_compare.py
(.documents/peakewsurf-field-equations/scripts/) evaluated them. It is evaluated on two tables:
  - "new", the installed CosmologyModels/GenericEOS/Xav_EOS_data.csv (2026.11.0);
  - "old", prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv, the table before this campaign
    byte for byte, read through a redirect of the one pandas.read_csv call Xav_EOS_spline makes
    while QCD_Cosmology is built (as electroweak-eos 01's table/sigma_change.py does). g_rho and g_s
    are the Saikawa-Shirai fits on both (U4).
The grid is thermo_compare.py's, rebuilt from the definition in nb_thermo.wls (the same folder):
T = 10^i for i = -5, -5 + 1/2000, ... while i <= log10(2e4) - 1e-6, so 2000 points per decade from
1e-5 GeV to 19999.6 GeV (18603 points); T* is its top point, and Delta is the cumulative trapezoid
from it, as thermo_compare.py integrates. The values are read at the grid point nearest each of
100, 1, 0.1, 1e-2, 1e-3 and 1e-4 GeV and at the grid's (and the w table's) low end, 1e-5 GeV.
The issue also quotes a value "today", which this grid does not reach. To set one beside it, the
integral is continued below 1e-5 GeV, at the same 2000 points per decade, down to the cosmology's
T_CMB (below the table, the class's w is 1/3 and g_rho, g_s are the fits); that continuation is
this script's, not thermo_compare.py's, and is reported as such. Each value is printed as Delta,
as 100 Delta (the issue's per cent, which are 100 Delta: -1.168e-2 is its "-1.17 %") and as
100 (e^Delta - 1), the relative offset of rho_R itself.

`histories` -- per history, from a plot_ScalarHistory.py export's fields.csv:

    d(T_J) = log_rhorad_Jordan_GeV4 - ln((pi^2/30) gstar_rho T_Jordan_GeV^4)

at T_J = 1 GeV and 1 MeV and at the last sample. At each target, the samples are walked in the
file's order and the first adjacent pair whose T_J bracket the target is taken; d is interpolated
linearly in ln T_J between them. (T_J is not monotonic through a bounce, so "first" matters; the
pair's index is reported.) d at the first sample is reported as well, as the check that the ODE
starts on g_rho T^4.

Run from the repository root:

    PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/bath/bath_drift.py tables
    PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/bath/bath_drift.py histories \\
        --history LABEL=PATH/fields.csv [--history LABEL=PATH/fields.csv ...]

`tables` writes out/bath_tables.txt and out/bath_tables.csv beside this script (about a minute);
`histories` writes out/bath_histories.txt and out/bath_histories.csv.
"""

import argparse
import csv
import math
import sys
from pathlib import Path
from unittest import mock

import numpy as np
from scipy.integrate import cumulative_trapezoid

import CosmologyModels.GenericEOS.Xav_EOS_spline as xav_module
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import GeV_units

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
OLD = ROOT / "prompts" / "electroweak-eos" / "data" / "pipeline_w_table_ec6bca8.csv"
NEW = ROOT / "CosmologyModels" / "GenericEOS" / "Xav_EOS_data.csv"

PER_DECADE = 2000
T_QUOTE_GEV = [100.0, 1.0, 0.1, 1e-2, 1e-3, 1e-4, 1e-5]
# the issue's table (inelastic-reflection board, [09-...], "cumulative drift"), in per cent
ISSUE_PERCENT = {
    100.0: "+0.19",
    1.0: "-1.17",
    0.1: "-0.08",
    1e-2: "-0.21",
    1e-3: "-0.211",
    1e-4: "-0.098",
    "today": "-0.078",
}
HISTORY_TARGETS_GEV = [1.0, 1e-3]


def cosmology_on(path: Path, units) -> QCD_Cosmology:
    real = xav_module.pd.read_csv

    def redirected(arg, *args, **kwargs):
        return real(path, *args, **kwargs)

    with mock.patch.object(xav_module.pd, "read_csv", redirected):
        return QCD_Cosmology(None, units, Planck2018())


def nb_grid() -> np.ndarray:
    # nb_thermo.wls: Table[10.^i, {i, -5., Log10[2.*10^4] - 10^-6, 1/2000.}]
    i_max = math.log10(2.0e4) - 1e-6
    k_max = int(math.floor((i_max + 5.0) * PER_DECADE + 1e-9))
    return 10.0 ** (-5.0 + np.arange(k_max + 1) / PER_DECADE)


def integrand(c, T: np.ndarray) -> np.ndarray:
    gr = np.array([float(c.G_rho(t)) for t in T])
    gs = np.array([float(c.G_s(t)) for t in T])
    dgr = np.array([float(c.dG_rho_dlogT(t)) for t in T])
    dgs = np.array([float(c.dG_s_dlogT(t)) for t in T])
    w = np.array([float(c.w(t)) for t in T])
    x = dgs / gs / 3.0
    return (4.0 + dgr / gr) - 3.0 * (1.0 + w) * (1.0 + x)


def tables(args) -> int:
    units = GeV_units()
    T = nb_grid()
    lnT = np.log(T)
    lines = []

    def say(s=""):
        print(s)
        lines.append(s)

    say("bath_drift.py tables (electroweak-eos prompt 02, §1.3)")
    say(
        f"grid: nb_thermo.wls's, {len(T)} points, {PER_DECADE} per decade, "
        f"T from {float(T[0])!r} to {float(T[-1])!r} GeV (T* = the top point)"
    )
    rows = []
    results = {}
    for label, path in (("old", args.old), ("new", args.new)):
        c = cosmology_on(path, units)
        f = integrand(c, T)
        cum = cumulative_trapezoid(f, lnT, initial=0.0)
        Delta = cum[-1] - cum
        # the continuation below the grid, to T_CMB, at the same density
        T_cmb = float(c._T_CMB) / units.GeV
        n_ext = int(math.ceil(math.log10(T[0] / T_cmb) * PER_DECADE))
        T_ext = np.logspace(math.log10(T_cmb), math.log10(T[0]), n_ext + 1)
        f_ext = integrand(c, T_ext)
        cum_ext = cumulative_trapezoid(f_ext, np.log(T_ext), initial=0.0)
        # Delta at T_ext[j] = Delta(T[0]) + int_{ln T_ext[j]}^{ln T[0]}
        Delta_ext = Delta[0] + (cum_ext[-1] - cum_ext)
        results[label] = dict(T_cmb=T_cmb, Delta=Delta, Delta_today=Delta_ext[0])
        say()
        say(
            f"[{label}] table {path.relative_to(ROOT) if path.is_relative_to(ROOT) else path}"
        )
        say(
            f"  {'T_quoted_GeV':>12s} {'T_grid_GeV':>14s} {'Delta':>13s} {'100 Delta':>10s} "
            f"{'100(e^D - 1)':>13s} {'issue %':>8s}"
        )
        for Tq in T_QUOTE_GEV:
            j = int(np.argmin(np.abs(T - Tq)))
            d = float(Delta[j])
            pct = 100.0 * math.expm1(d)
            iss = ISSUE_PERCENT.get(Tq, "")
            if Tq == 1e-5:
                iss = "(end of table)"
            say(
                f"  {Tq:>12g} {T[j]:>14.6g} {d:>+13.5e} {100 * d:>+10.4f} {pct:>+13.4f} {iss:>8s}"
            )
            rows.append(
                dict(
                    table=label,
                    T_quoted_GeV=repr(Tq),
                    T_grid_GeV=repr(float(T[j])),
                    Delta=repr(d),
                    percent_100_Delta=repr(100 * d),
                    percent_expm1=repr(pct),
                    issue_percent=ISSUE_PERCENT.get(Tq, ""),
                    source="nb grid",
                )
            )
        d = float(Delta_ext[0])
        pct = 100.0 * math.expm1(d)
        say(
            f"  {'today':>12s} {T_cmb:>14.6g} {d:>+13.5e} {100 * d:>+10.4f} {pct:>+13.4f} "
            f"{ISSUE_PERCENT['today']:>8s}  (continued below 1e-5 GeV, {n_ext + 1} points)"
        )
        rows.append(
            dict(
                table=label,
                T_quoted_GeV="today",
                T_grid_GeV=repr(T_cmb),
                Delta=repr(d),
                percent_100_Delta=repr(100 * d),
                percent_expm1=repr(pct),
                issue_percent=ISSUE_PERCENT["today"],
                source="continued below the grid",
            )
        )
        j = int(np.argmin(Delta))
        say(
            f"  least Delta on the grid: {Delta[j]:+.5e} ({100 * math.expm1(Delta[j]):+.4f} %) "
            f"at T = {T[j]:.6g} GeV"
        )
        j = int(np.argmax(Delta))
        say(
            f"  largest Delta on the grid: {Delta[j]:+.5e} ({100 * math.expm1(Delta[j]):+.4f} %) "
            f"at T = {T[j]:.6g} GeV"
        )

    say()
    say("new - old, Delta:")
    for Tq in T_QUOTE_GEV:
        j = int(np.argmin(np.abs(T - Tq)))
        dd = results["new"]["Delta"][j] - results["old"]["Delta"][j]
        say(f"  T = {T[j]:.6g} GeV: {dd:+.5e}")
    dd = results["new"]["Delta_today"] - results["old"]["Delta_today"]
    say(f"  today: {dd:+.5e}")

    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "bath_tables.txt").write_text("\n".join(lines) + "\n")
    with open(args.out / "bath_tables.csv", "w", newline="") as fh:
        wr = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        wr.writeheader()
        wr.writerows(rows)
    return 0


def _drift(row) -> float:
    g = float(row["gstar_rho"])
    T = float(row["T_Jordan_GeV"])
    return float(row["log_rhorad_Jordan_GeV4"]) - math.log(math.pi**2 / 30.0 * g * T**4)


def histories(args) -> int:
    lines = []
    rows_out = []

    def say(s=""):
        print(s)
        lines.append(s)

    say("bath_drift.py histories (electroweak-eos prompt 02, §1.3)")
    say(
        "d = log_rhorad_Jordan_GeV4 - ln((pi^2/30) gstar_rho T_Jordan_GeV^4); at each target the "
        "first adjacent pair of samples (file order) whose T_J bracket it, linear in ln T_J"
    )
    for spec in args.history:
        label, path = spec.split("=", 1)
        path = Path(path)
        with open(path, newline="") as fh:
            rows = list(csv.DictReader(fh))
        d = [_drift(r) for r in rows]
        lnT = [math.log(float(r["T_Jordan_GeV"])) for r in rows]
        say()
        say(f"[{label}] {path}")
        say(
            f"  samples {len(rows)}; T_J first {rows[0]['T_Jordan_GeV']} GeV, last {rows[-1]['T_Jordan_GeV']} GeV"
        )
        out = dict(label=label, path=str(path), samples=len(rows))
        say(f"  first sample: d = {d[0]:+.6e} ({100 * math.expm1(d[0]):+.5f} %)")
        out["d_first"] = repr(d[0])
        for Tq in HISTORY_TARGETS_GEV:
            lq = math.log(Tq)
            found = None
            for i in range(len(rows) - 1):
                a, b = lnT[i], lnT[i + 1]
                if (a - lq) * (b - lq) <= 0.0 and a != b:
                    found = i
                    break
            if found is None:
                say(f"  T_J = {Tq:g} GeV: not bracketed")
                out[f"d_{Tq:g}GeV"] = ""
                continue
            i = found
            t = (lq - lnT[i]) / (lnT[i + 1] - lnT[i])
            dq = d[i] + t * (d[i + 1] - d[i])
            say(
                f"  T_J = {Tq:g} GeV: d = {dq:+.6e} ({100 * math.expm1(dq):+.5f} %), "
                f"samples {i},{i + 1} (T_J {rows[i]['T_Jordan_GeV']}, {rows[i + 1]['T_Jordan_GeV']} GeV)"
            )
            out[f"d_{Tq:g}GeV"] = repr(dq)
            out[f"pair_{Tq:g}GeV"] = f"{i},{i + 1}"
        say(
            f"  last sample (T_J {rows[-1]['T_Jordan_GeV']} GeV): d = {d[-1]:+.6e} "
            f"({100 * math.expm1(d[-1]):+.5f} %)"
        )
        out["T_last_GeV"] = rows[-1]["T_Jordan_GeV"]
        out["d_last"] = repr(d[-1])
        rows_out.append(out)

    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "bath_histories.txt").write_text("\n".join(lines) + "\n")
    keys = []
    for r in rows_out:
        for k in r:
            if k not in keys:
                keys.append(k)
    with open(args.out / "bath_histories.csv", "w", newline="") as fh:
        wr = csv.DictWriter(fh, fieldnames=keys)
        wr.writeheader()
        wr.writerows(rows_out)
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    pt = sub.add_parser("tables")
    pt.add_argument("--old", type=Path, default=OLD)
    pt.add_argument("--new", type=Path, default=NEW)
    pt.add_argument("--out", type=Path, default=HERE / "out")
    ph = sub.add_parser("histories")
    ph.add_argument("--history", action="append", required=True)
    ph.add_argument("--out", type=Path, default=HERE / "out")
    args = parser.parse_args(argv)
    return tables(args) if args.cmd == "tables" else histories(args)


if __name__ == "__main__":
    sys.exit(main())
