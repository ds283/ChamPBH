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
Exports for the tests of tools/quanta_measures.py and the Derive gate's scripts (one-loop-drain prompt 05 §1.7, §2), written
by 04's writers (extract_common's write_histories_csv, build_fields_frame through plot_ScalarHistory.py's own write_csv_file,
and write_bounces_csv), in the plot scripts' layout: histories.csv at the root, and each history's fields.csv and
bounces.csv in root / alpha_path(A, model) / "csv" / history_directory(beta, M_eV, n).

- `write_constructed`: histories from constructed rows (BounceRow tuples and sample stand-ins), with K(N) and r_chi made by
  a DarkRadiationLedger as the integrator makes them;
- `export_history`: one history read back from a store (ScalarHistory objects), as plot_by_beta.py and
  plot_ScalarHistory.py write it, with BBN and the adiabatic history unavailable stand-ins (note 04, ruling 6).

Not a test module: nothing here is collected.
"""

import ast
from math import exp, log, nan
from pathlib import Path
from types import SimpleNamespace
from typing import List, Optional, Sequence

import pandas as pd

import extract_common as ec
from ComputeTargets import AdiabaticHistory, BBNData, ScalarHistory
from ComputeTargets.ScalarHistory import BounceRow
from ComputeTargets.dark_radiation import DarkRadiationLedger
from Units.base import UnitsLike

ROOT = Path(__file__).resolve().parents[2]
UNAVAILABLE = SimpleNamespace(available=False)
LABEL = "QCD_Cosmology"


def extract_function(path: Path, name: str, namespace: dict):
    """The function `name` of the file at `path`, extracted by ast and executed in `namespace`, unedited (export-04's)."""
    tree = ast.parse(path.read_text(), str(path))
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name]
    assert len(nodes) == 1, (path, name)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)
    return namespace[name]


def write_csv_file():
    """plot_ScalarHistory.py's write_csv_file, extracted by ast (that script parses its arguments on import)."""
    return extract_function(
        ROOT / "plot_ScalarHistory.py",
        "write_csv_file",
        {
            "pd": pd,
            "Path": Path,
            "BBNData": BBNData,
            "AdiabaticHistory": AdiabaticHistory,
            "ScalarHistory": ScalarHistory,
            "UnitsLike": UnitsLike,
            "build_fields_frame": ec.build_fields_frame,
        },
    )


def history_csv_dir(
    root: Path, A: float, drain_model: str, beta: float, M_eV: float, n: int
) -> Path:
    """The history's csv directory as plot_ScalarHistory.py forms it."""
    return ec.alpha_path(Path(root), A, drain_model) / (
        f"csv/{ec.history_directory(beta, M_eV, n)}"
    )


# ---------------------------------------------------------------------------------------------------------------------
# Constructed exports
# ---------------------------------------------------------------------------------------------------------------------


def candidate(
    ordinal,
    index,
    N_turn,
    N,
    delta_K,
    K_before,
    log_q=None,
    log_k_ex=None,
    log_m=None,
    log_E=None,
    log_H=None,
    regime="one_loop",
):
    """A candidate BounceRow (fields the tool does not read are plausible stand-ins)."""
    row = dict.fromkeys(BounceRow._fields)
    row.update(
        ordinal=ordinal,
        source="candidate",
        candidate_index=index,
        N=N,
        N_turn=N_turn,
        kind="unresolved",
        resolved=False,
        log_T_Jordan=-10.0 - N,
        delta_K=delta_K,
        log_rho_kin=-50.0,
        nonadiabaticity=10.0,
        alpha_eff=0.1 if delta_K > 0.0 else 0.0,
        w_gate=1.0,
        K_before=K_before,
        regime=regime,
        log_E=log_E,
        log_H=log_H,
        log_k_ex=log_k_ex,
        log_m=log_m,
        log_q=log_q,
    )
    if log_q is not None:
        row.update(share=0.1, weighted=True, birth_margin=5.0)
    return BounceRow(**row)


def event(ordinal, N, log_m, source="plateau"):
    row = dict.fromkeys(BounceRow._fields)
    row.update(ordinal=ordinal, source=source, N=N, log_T_Jordan=-10.0 - N, log_m=log_m)
    return BounceRow(**row)


def sample_value(k, N, log_rhorad, r_chi, log_m_eff, log_m_tracker, units):
    """A ScalarHistoryValue stand-in carrying what build_fields_frame reads."""
    return SimpleNamespace(
        z=SimpleNamespace(store_id=k, z=exp(10.0 - N) - 1.0, log_one_plus_z=10.0 - N),
        log_one_plus_z_Jordan=10.0 - N,
        raw_N=N,
        phi_Einstein=0.1 * units.PlanckMass,
        pi_Einstein=0.0,
        H_Einstein=1e-20 * units.PlanckMass,
        H_Jordan=1e-20 * units.PlanckMass,
        log_rhorad_Einstein=log_rhorad,
        log_rhorad_Jordan=log_rhorad,
        log_fm=-5.0,
        log_T_Jordan=log(units.GeV) - N,
        gstar_rho=10.0,
        gstar_s=10.0,
        dgstar_rho_dlogT=0.0,
        dgstar_s_dlogT=0.0,
        Sigma=1.0,
        friction_term=0.0,
        reflecting_term=0.0,
        kicking_term=0.0,
        rho_chi_over_rho_rad=r_chi,
        log_m_eff=log_m_eff,
        log_m_tracker=log_m_tracker,
    )


def ledger_of(K_initial: float, entries: Sequence[tuple]) -> DarkRadiationLedger:
    """A ledger with K_initial (M_P^4 in Planck units) and entries (N, delta_K), in order."""
    ledger = DarkRadiationLedger(K_initial)
    for N, dK in entries:
        ledger.add(N, dK)
    return ledger


def history_record(
    beta,
    M_Mp,
    n,
    A,
    drain_model,
    normalization: Optional[float] = -276.6,
    parking_share: Optional[float] = 0.0,
    bounce_rows: Optional[int] = None,
):
    """A histories.csv record with CSV_COLUMNS, constructed (blank where not given)."""
    record = {name: nan for name in ec.CSV_COLUMNS}
    record.update(
        beta=beta,
        M_Mp=M_Mp,
        n=n,
        phi_init_Mp=5.0,
        alpha=A,
        drain_model=drain_model,
        failure_reasons="",
        normalization=nan if normalization is None else normalization,
        parking_share=nan if parking_share is None else parking_share,
        bounce_rows=nan if bounce_rows is None else bounce_rows,
    )
    return record


def write_constructed(
    root: Path,
    units: UnitsLike,
    record: dict,
    rows: Sequence[BounceRow],
    samples: Sequence,
    M_eV: Optional[float] = None,
    write_fields: bool = True,
    write_bounces: bool = True,
) -> Path:
    """One constructed history's fields.csv and bounces.csv, in its directory; returns the directory. The caller
    writes histories.csv (write_histories_csv over every record)."""
    if M_eV is None:
        M_eV = record["M_Mp"] * units.PlanckMass / units.eV
    d = history_csv_dir(
        root, record["alpha"], record["drain_model"], record["beta"], M_eV, record["n"]
    )
    model = SimpleNamespace(available=True, values=list(samples))
    if write_fields:
        write_csv_file()(UNAVAILABLE, UNAVAILABLE, d, model, units)
    if write_bounces:
        ec.write_bounces_csv(rows, units, d / "bounces.csv")
    return d


# ---------------------------------------------------------------------------------------------------------------------
# A history read back from a store
# ---------------------------------------------------------------------------------------------------------------------


def export_history(
    root: Path,
    units: UnitsLike,
    record_object,
    full_object,
    beta: float,
    M_Mp: float,
    n: int,
    A: float,
    drain_model: str,
    phi_init_Mp: float = 5.0,
) -> (dict, Path):
    """
    One history's exports, as the plot scripts write them: its histories.csv record (build_history_record on the object
    read with _do_not_populate, bbn and baseline None), and fields.csv and bounces.csv from the populated object, in the
    directory plot_ScalarHistory.py gives it (M in eV from the object's model, as there). Returns (record, directory).
    """
    record = ec.build_history_record(
        beta=beta,
        M_Mp=M_Mp,
        n=n,
        phi_init_Mp=phi_init_Mp,
        alpha=A,
        drain_model=drain_model,
        scalar=record_object,
        bbn=None,
        baseline=None,
        units=units,
    )
    lagrangian = full_object.model
    base = ec.alpha_path(
        Path(root), full_object.alpha.value, full_object.alpha.drain_model
    )
    d = base / (
        f"csv/{ec.history_directory(lagrangian._beta.as_float, lagrangian._M.as_float / units.eV, lagrangian._n)}"
    )
    write_csv_file()(UNAVAILABLE, UNAVAILABLE, d, full_object, units)
    ec.write_bounces_csv(full_object.bounces, units, d / "bounces.csv")
    return record, d
