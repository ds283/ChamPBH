"""
The reference rows of two tests that compare a tuned surfing history with a committed scan row, remade on
electroweak-eos prompt 01's w table (the prompt's §1.6; the log's re-pins):

    tools/tests/test_surfing_scan.py              test_beta_1_5_tuned                                   beta = 1.5
    tools/tests/test_plot_surfing_histories.py    test_beta_3_is_not_captured_and_bounces_as_the_...   beta = 3.0

Both tests read their row from a scan of surfing-temperature prompt 04,
.documents/surfing-temperature/out/surfing_scan_phi5_M1e-3.csv and surfing_scan_phi5_M1e-3_near3.csv, made by
tools/surfing_scan.py at e7ed715, before jordan-normalization prompt 08: untuned, at Lambda = 1 meV, stopped at
T_J = 1 MeV, on the table before this prompt. Those files are the record of that table and are not rewritten
(CLAUDE.md invariant 6), so the tests read the rows this script writes instead.

Each row is made as those scans made theirs: one integrate_history call at the normalization 4 ln(1 meV) and
phi_ref = 0 (golden_history.normalization), ExponentialModel(M = 1e-3 M_P, n = 1, beta), phi* = 5 M_P, pi* = 0,
T* = 2e4 GeV, T_stop = 1 MeV, atol = rtol = 1e-8, on the production sampling axis (golden_history.make_z_grid);
the crossings by surfing_turning_points, the bounce by first_bounce, the row by tools/surfing_scan.summarise,
with find_kicks and reachability_roots on the same cosmology. Also recorded, not read by the tests: the least
g = 1 + beta pi/M_P before the bounce on 4001 points of the dense output, where it is, and the accepted steps.

The script runs each beta on two tables:
  - the table before this prompt (prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv, read through a
    redirect of the class's pandas.read_csv), where it states each row against the committed scan's row, column
    by column, so that the method is shown to reproduce the committed rows;
  - the installed table, whose rows it writes to out/surfing_reference_rows.csv (repr of each float, read
    back exactly).
Its report goes to stdout:

    PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/table/surfing_reference.py \\
        > .documents/electroweak-eos/table/out/surfing_reference.txt

Four integrations to 1 MeV, about a minute under load. No Ray, no store, no PRyMordial.
"""

import contextlib
import csv
import importlib
import io
from math import exp
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

import CosmologyModels.GenericEOS.Xav_EOS_spline as xav_module
import tools.surfing_scan as ss
from ComputeTargets.tests import golden_history as gh
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel

SM = importlib.import_module("ComputeTargets.ScalarHistory")

HERE = Path(__file__).resolve().parent
OLD_TABLE = Path("prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv")
COMMITTED = {
    1.5: Path(".documents/surfing-temperature/out/surfing_scan_phi5_M1e-3.csv"),
    3.0: Path(".documents/surfing-temperature/out/surfing_scan_phi5_M1e-3_near3.csv"),
}
OUT_CSV = HERE / "out" / "surfing_reference_rows.csv"

BETAS = (1.5, 3.0)
M_OVER_MP = 1.0e-3
PHI_INIT_OVER_MP = 5.0
T_INIT_GEV = 2.0e4
T_STOP_GEV = 1.0e-3
TOL = 1.0e-8
G_POINTS = 4001

ROW_COLUMNS = [
    "beta",
    "captured",
    "capture_kick",
    "T_capture_GeV",
    "N_capture",
    "T_max_GeV",
    "N_max",
    "T_second_dip_GeV",
    "N_second_dip",
    "T_deliver_GeV",
    "N_deliver",
    "deliver_reflected",
    "n_crossings",
    "T_hot_GeV",
    "T_cold_GeV",
]
EXTRA_COLUMNS = ["min_g", "N_min_g", "T_min_g_GeV", "accepted_steps"]


def make_setup(table):
    """surfing_scan.make_setup, with the class's CSV read redirected to `table` if it is given."""
    with contextlib.ExitStack() as stack:
        if table is not None:
            real = pd.read_csv
            stack.enter_context(
                mock.patch.object(
                    xav_module.pd,
                    "read_csv",
                    lambda _path, *a, **k: real(table, *a, **k),
                )
            )
        stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        return ss.make_setup()


def reference_row(setup, beta: float) -> dict:
    units = setup.units
    GeV, Mp = units.GeV, units.PlanckMass
    cosmology = setup.cosmology
    kicks = ss.find_kicks(
        cosmology, cosmology._eos._T_min * GeV, cosmology._eos._T_max * GeV
    )
    roots = ss.reachability_roots(cosmology, beta, kicks)

    model = ExponentialModel(
        0, M_value(0, M_OVER_MP * Mp), 1, beta_value(0, beta), units
    )
    realized = model.realize(gh.normalization(units), 0.0)
    captured = []
    inner = SM.integrate_scalar_history

    def capturing(*a, **k):
        result = inner(*a, **k)
        captured.append(result)
        return result

    with mock.patch.object(
        SM, "integrate_scalar_history", capturing
    ), contextlib.redirect_stdout(io.StringIO()):
        data = SM.integrate_history(
            cosmology,
            temperature(0, T_INIT_GEV * GeV),
            temperature(1, T_STOP_GEV * GeV),
            phi_value(0, PHI_INIT_OVER_MP * Mp),
            pi_value(0, 0.0),
            gh.make_z_grid(),
            realized,
            task_label="surfing_reference",
            atol=TOL,
            rtol=TOL,
        )
    if data.get("failure", False):
        raise RuntimeError(f"beta = {beta}: {data.get('failure_reason')}")
    result = captured[-1]
    crossings = SM.surfing_turning_points(result, realized)
    bounce = data["first_bounce"]
    row = ss.summarise(crossings, bounce, kicks, roots, units)

    N_end = bounce.N if bounce is not None else result.N_final
    Ns = np.linspace(result.solution.ts[0], N_end, G_POINTS)
    Y = result.solution(Ns)
    g = 1.0 + beta * Y[1] / Mp
    i = int(np.argmin(g))
    row = {"beta": beta, **row}
    row["min_g"] = float(g[i])
    row["N_min_g"] = float(Ns[i])
    row["T_min_g_GeV"] = exp(float(Y[4][i])) / GeV
    row["accepted_steps"] = result.accepted_steps
    return row


def committed_row(beta: float) -> dict:
    with open(COMMITTED[beta], newline="") as f:
        return next(r for r in csv.DictReader(f) if float(r["beta"]) == beta)


def compare(row: dict, committed: dict) -> list:
    lines = []
    for column in ROW_COLUMNS[1:]:
        ours = ss.format_field(row[column])
        theirs = committed.get(column, "")
        note = ""
        if (
            ours
            and theirs
            and ours not in ("true", "false")
            and column != "capture_kick"
        ):
            try:
                note = f"   rel {float(ours) / float(theirs) - 1.0:+.3e}"
            except (ValueError, ZeroDivisionError):
                note = ""
        same = "equal" if ours == theirs else ("same value" if note else "DIFFERS")
        if note:
            same = ""
        lines.append(
            f"    {column:<18} {ours or '-':>24}   committed {theirs or '-':>24}{note} {same}"
        )
    return lines


def main() -> int:
    old = make_setup(OLD_TABLE)
    new = make_setup(None)

    print(
        "The table before electroweak-eos prompt 01, against the committed surfing-temperature scans:"
    )
    for beta in BETAS:
        row = reference_row(old, beta)
        print(f"  beta = {beta} ({COMMITTED[beta]})")
        for line in compare(row, committed_row(beta)):
            print(line)
        print(
            f"    least g before the bounce {row['min_g']:.6g} at N = {row['N_min_g']:.6f}, "
            f"T_J = {row['T_min_g_GeV']:.6g} GeV; accepted steps {row['accepted_steps']}"
        )

    print()
    print(f"The installed table, written to {OUT_CSV.relative_to(Path.cwd())}:")
    rows = []
    for beta in BETAS:
        row = reference_row(new, beta)
        rows.append(row)
        print(f"  beta = {beta}")
        for column in ROW_COLUMNS[1:] + EXTRA_COLUMNS:
            print(f"    {column:<18} {ss.format_field(row[column]) or '-'}")

    OUT_CSV.parent.mkdir(exist_ok=True)
    with open(OUT_CSV, "w", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(ROW_COLUMNS + EXTRA_COLUMNS)
        for row in rows:
            writer.writerow(
                [ss.format_field(row[c]) for c in ROW_COLUMNS + EXTRA_COLUMNS]
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
