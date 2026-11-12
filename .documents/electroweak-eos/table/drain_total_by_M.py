"""
The reference of test_wall_rounding's item (3), remade on electroweak-eos prompt 01's w table (the log's re-pins).

test_wall_rounding.TestAHistoryInTheBand.test_3_the_drain_is_log_06s compares, at M = 1e-16 M_P, the sum over the
candidates of Delta rho/rho_R plus the parking leftover with log 06 §5.4's value at M = 1e-30 M_P (inelastic-
reflection prompt 06), 0.012616907, within that value's M-dependence across 1e-7 to 1e-30 M_P (2.5e-8 relative)
and half a unit of its last printed digit. The value is a measurement on the table before this prompt, so this
script measures it again, the same way, on both tables:

  beta = 2, alpha = 0.1, test_injection's setup (n = 1, the model's seed realization, phi* = 5 M_P, pi* = 0,
  T* = 2e4 GeV, to T_CMB, the default tolerances), through test_injection.integrate_captured; the quantity is
  sum(injection.delta_rho_over_rho_rad) + parking.delta_rho_over_rho_rad, as the test forms it.

On the table before this prompt (prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv, read through a
redirect of the class's pandas.read_csv) at M = 1e-30 and 1e-16, to show the method gives log 06's value; on
the installed table at M = 1e-30, 1e-25, 1e-20, 1e-16, 1e-12 and 1e-7, for the new value and its M-dependence.

    PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/table/drain_total_by_M.py \\
        > .documents/electroweak-eos/table/out/drain_total_by_M.txt

Eight integrations to T_CMB, about a minute under load. No Ray, no store, no PRyMordial.
"""

import contextlib
import io
from pathlib import Path
from unittest import mock

import pandas as pd

import ComputeTargets.tests.test_injection as ti
import CosmologyModels.GenericEOS.Xav_EOS_spline as xav_module

OLD_TABLE = Path("prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv")
LOG_06_VALUE = 0.012616907
OLD_MS = (1e-30, 1e-16)
NEW_MS = (1e-30, 1e-25, 1e-20, 1e-16, 1e-12, 1e-7)


def total(M: float) -> tuple:
    with contextlib.redirect_stdout(io.StringIO()):
        data, result, _ = ti.integrate_captured(2.0, M, 0.1)
    value = sum(x.delta_rho_over_rho_rad for x in result.injections)
    value += result.parking.delta_rho_over_rho_rad
    return (
        value,
        len(result.injections),
        result.parking.candidate_index,
        result.parking.N,
    )


def run(Ms) -> list:
    rows = []
    for M in Ms:
        value, n, parked_at, N_park = total(M)
        rows.append(value)
        print(
            f"  M = {M:g} M_P: {value!r}  ({value:.8g}); {n} candidates, parked at candidate "
            f"{parked_at}, N = {N_park:.6f}"
        )
    return rows


def main() -> int:
    print("The table before electroweak-eos prompt 01:")
    real = pd.read_csv
    with mock.patch.object(
        xav_module.pd, "read_csv", lambda _path, *a, **k: real(OLD_TABLE, *a, **k)
    ):
        old = run(OLD_MS)
    print(
        f"  log 06 §5.4's value {LOG_06_VALUE}; M = 1e-30 here, to eight figures: {old[0]:.8g}"
    )
    print()
    print("The installed table:")
    new = run(NEW_MS)
    spread = (max(new) - min(new)) / new[0]
    print(f"  M = 1e-30, to eight figures: {new[0]:.8g}")
    print(
        f"  the M-dependence, (max - min)/value(1e-30) over 1e-7 to 1e-30 M_P: {spread:.3e}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
