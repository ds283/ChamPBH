"""
What the new w table does to Sigma = 1 - 3w (electroweak-eos prompt 01, §1.8; the Table gate).

Sigma is evaluated through `Xav_EOS_spline.w`, the production class, on two tables: the table
before this campaign (prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv, by default) and the
installed one (CosmologyModels/GenericEOS/Xav_EOS_data.csv). The class reads its CSV by a fixed
path; this script points that one read (`pandas.read_csv` as the class's module calls it) at each
table in turn, so the class's own code makes both splines.

It records, on a grid of 1000 points per decade from 1e-5 to 3e4 GeV:
  - the largest |Delta Sigma| = |Sigma_new - Sigma_old| in each decade, and over [1e-5 GeV, T0) and
    [T0, 22 GeV], where the rows of the two tables are the same values (a difference there comes
    from the spline's global coupling, and from how pandas parses the rows' text, both reported);
  - the electroweak peak (T, Sigma) on both tables, the argmax on that grid between 20 GeV and 1 TeV;
  - int Sigma d ln T over 30-100, 100-300 and 300-25000 GeV on both tables (trapezoid on 20001
    points uniform in ln T, as the audit's addendum computed its own), beside the addendum's figures
    (.documents/peakewsurf-kicking-function/data/laine_meyer_stats.json, "int_Sigma_dlnT": its
    "merged" is the same rows splined the same way above 50.1 keV; its "pipeline" is the old rows
    joined linearly in ln T, not splined).

Measurements, not bounds: nothing here is tested against a threshold.

Run from the repository root:

    PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/table/sigma_change.py

It writes out/sigma_change.txt and out/sigma_change.json beside itself. About 15 s.
"""

import argparse
import json
import sys
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

import CosmologyModels.GenericEOS.Xav_EOS_spline as xav_module
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline
from Units import GeV_units

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
OLD = ROOT / "prompts" / "electroweak-eos" / "data" / "pipeline_w_table_ec6bca8.csv"
NEW = ROOT / "CosmologyModels" / "GenericEOS" / "Xav_EOS_data.csv"
STATS = (
    ROOT
    / ".documents"
    / "peakewsurf-kicking-function"
    / "data"
    / "laine_meyer_stats.json"
)

T0_GEV = 5.011872336272725e-05
SHARED_TOP_GEV = 22.0
GRID_LO, GRID_HI, PER_DECADE = 1e-5, 3e4, 1000
INTEGRALS = [(30.0, 100.0), (100.0, 300.0), (300.0, 25000.0)]


def _shown(path: Path) -> str:
    path = path.resolve()
    return str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path)


def eos_on(path: Path, units) -> Xav_EOS_spline:
    real = pd.read_csv

    def redirected(arg, *args, **kwargs):
        return real(path, *args, **kwargs)

    with mock.patch.object(xav_module.pd, "read_csv", redirected):
        return Xav_EOS_spline(units)


def sigma_array(eos, units, Ts) -> np.ndarray:
    return np.array([1.0 - 3.0 * float(eos.w(T * units.GeV)) for T in Ts])


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--old", type=Path, default=OLD)
    parser.add_argument("--new", type=Path, default=NEW)
    parser.add_argument("--out", type=Path, default=HERE / "out")
    args = parser.parse_args(argv)

    units = GeV_units()
    eos = {"old": eos_on(args.old, units), "new": eos_on(args.new, units)}

    n = int(round(PER_DECADE * np.log10(GRID_HI / GRID_LO))) + 1
    Ts = np.logspace(np.log10(GRID_LO), np.log10(GRID_HI), n)
    S = {k: sigma_array(e, units, Ts) for k, e in eos.items()}
    dS = S["new"] - S["old"]

    res = {
        "tables": {k: _shown(getattr(args, k)) for k in ("old", "new")},
        "grid": {
            "T_lo_GeV": GRID_LO,
            "T_hi_GeV": GRID_HI,
            "per_decade": PER_DECADE,
            "points": n,
        },
        "ends": {
            k: {
                "T_min_GeV": float(e._T_min),
                "T_max_GeV": float(e._T_max),
                "rows": len(e._T_series),
            }
            for k, e in eos.items()
        },
    }

    # the largest |Delta Sigma| per decade
    decades = []
    for d in range(-5, 5):
        lo, hi = 10.0**d, min(10.0 ** (d + 1), GRID_HI)
        m = (Ts >= lo) & ((Ts < hi) if d < 4 else (Ts <= hi))
        k = int(np.argmax(np.abs(dS[m])))
        decades.append(
            {
                "T_lo_GeV": lo,
                "T_hi_GeV": hi,
                "points": int(m.sum()),
                "max_abs_dSigma": float(abs(dS[m][k])),
                "at_T_GeV": float(Ts[m][k]),
                "dSigma_there": float(dS[m][k]),
            }
        )
    res["max_abs_dSigma_by_decade"] = decades

    # where the rows are the same values
    shared = {}
    for name, m in (
        ("1e-5 to T0", (Ts >= GRID_LO) & (Ts < T0_GEV)),
        ("T0 to 22 GeV", (Ts >= T0_GEV) & (Ts <= SHARED_TOP_GEV)),
    ):
        k = int(np.argmax(np.abs(dS[m])))
        shared[name] = {
            "points": int(m.sum()),
            "max_abs_dSigma": float(abs(dS[m][k])),
            "at_T_GeV": float(Ts[m][k]),
            "dSigma_there": float(dS[m][k]),
        }
    res["max_abs_dSigma_where_rows_agree"] = shared

    # the spline's nodes as pandas reads them, where the rows are the same values
    nodes = {}
    for k, e in eos.items():
        nodes[k] = (
            np.asarray(e._T_series, dtype=float),
            np.asarray(e._w_series, dtype=float),
        )
    agree = {}
    # pandas' default parser can move a T by ~1e-12 relative, so the edges carry a margin
    edge = T0_GEV * (1.0 - 1e-9)
    To, wo = nodes["old"]
    Tn, wn = nodes["new"]
    for name, mo, mn in (
        ("below T0", To < edge, Tn < edge),
        (
            "T0 to 22 GeV",
            (To >= edge) & (To <= SHARED_TOP_GEV),
            (Tn >= edge) & (Tn <= SHARED_TOP_GEV),
        ),
    ):
        same = int(mo.sum()) == int(mn.sum())
        agree[name] = {
            "rows_old": int(mo.sum()),
            "rows_new": int(mn.sum()),
            "T_bitwise_equal": int((To[mo] == Tn[mn]).sum()) if same else None,
            "w_bitwise_equal": int((wo[mo] == wn[mn]).sum()) if same else None,
            "max_rel_dT": (
                float(np.max(np.abs(Tn[mn] / To[mo] - 1.0))) if same else None
            ),
            "max_abs_dw": float(np.max(np.abs(wn[mn] - wo[mo]))) if same else None,
        }
    res["spline_nodes_as_read_by_the_class"] = agree

    # the electroweak peak
    m = (Ts >= 20.0) & (Ts <= 1e3)
    peaks = {}
    for k in S:
        j = int(np.argmax(S[k][m]))
        peaks[k] = {"T_GeV": float(Ts[m][j]), "Sigma": float(S[k][m][j])}
    res["electroweak_peak"] = peaks

    # the integrals
    stats = json.loads(STATS.read_text())["int_Sigma_dlnT"]
    addendum = {
        "30-100": {k: stats["30-100"][k] for k in ("merged", "pipeline", "LM")},
        "100-300": {k: stats["100-300"][k] for k in ("merged", "pipeline", "LM")},
        "300-25000": {
            k: stats["300-1000"][k] + stats["1000-25000"][k]
            for k in ("merged", "pipeline", "LM")
        },
    }
    integ = {}
    for a, b in INTEGRALS:
        x = np.linspace(np.log(a), np.log(b), 20001)
        key = f"{a:g}-{b:g}"
        integ[key] = {
            k: float(np.trapezoid(sigma_array(e, units, np.exp(x)), x))
            for k, e in eos.items()
        }
        integ[key]["addendum"] = addendum[key]
    res["int_Sigma_dlnT"] = integ

    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "sigma_change.json").write_text(json.dumps(res, indent=1) + "\n")

    lines = [
        "Sigma = 1 - 3w through Xav_EOS_spline, old table against new",
        f"  old: {res['tables']['old']} ({res['ends']['old']['rows']} rows, "
        f"{res['ends']['old']['T_min_GeV']:.6g} to {res['ends']['old']['T_max_GeV']:.8g} GeV)",
        f"  new: {res['tables']['new']} ({res['ends']['new']['rows']} rows, "
        f"{res['ends']['new']['T_min_GeV']:.6g} to {res['ends']['new']['T_max_GeV']:.8g} GeV)",
        f"  grid: {n} points, {PER_DECADE} per decade, {GRID_LO:g} to {GRID_HI:g} GeV",
        "",
        "largest |Delta Sigma| by decade (Delta = new - old)",
        f"  {'decade [GeV]':<22}{'max |dSigma|':>14}{'at T [GeV]':>14}{'dSigma':>14}",
    ]
    for d in decades:
        lines.append(
            f"  {d['T_lo_GeV']:>8.0e} - {d['T_hi_GeV']:<10.0e}{d['max_abs_dSigma']:>14.3e}"
            f"{d['at_T_GeV']:>14.5g}{d['dSigma_there']:>14.3e}"
        )
    lines += ["", "where the rows are the same values"]
    for name, v in shared.items():
        lines.append(
            f"  {name:<16} max |dSigma| = {v['max_abs_dSigma']:.3e} at {v['at_T_GeV']:.5g} GeV "
            f"({v['points']} points)"
        )
    lines += [
        "",
        "the spline's nodes as the class reads them (pandas.read_csv, default parser)",
    ]
    for name, v in agree.items():
        lines.append(
            f"  {name:<14} rows {v['rows_old']} old, {v['rows_new']} new; T bitwise equal "
            f"{v['T_bitwise_equal']}, w bitwise equal {v['w_bitwise_equal']}; "
            f"max |T_new/T_old - 1| = {v['max_rel_dT']:.3g}, max |dw| = {v['max_abs_dw']:.3g}"
        )
    lines += ["", "electroweak peak (argmax on the grid, 20 GeV - 1 TeV)"]
    for k, v in peaks.items():
        lines.append(f"  {k}: Sigma = {v['Sigma']:.6f} at {v['T_GeV']:.5g} GeV")
    lines += [
        "",
        "int Sigma d ln T (trapezoid, 20001 points in ln T)",
        f"  {'T [GeV]':<12}{'old':>12}{'new':>12}{'add. merged':>14}{'add. pipeline':>15}{'add. LM':>12}",
    ]
    for key, v in integ.items():
        a = v["addendum"]
        lines.append(
            f"  {key:<12}{v['old']:>12.6f}{v['new']:>12.6f}{a['merged']:>14.6f}"
            f"{a['pipeline']:>15.6f}{a['LM']:>12.6f}"
        )
    text = "\n".join(lines) + "\n"
    (args.out / "sigma_change.txt").write_text(text)
    print(text, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
