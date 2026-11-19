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
Rebuild a stored history's BBN input from a ChamPBH datastore and re-run
PRyMordial on it, optionally with the low-temperature network's solve_ivp
tolerances changed by interception.

The history's ScalarHistoryValue rows are read from the store's shards
**read-only** (sqlite3 URI `mode=ro`; `datastorekit` is never used to
open the store). The ratio rho_NP / rho_R,J is rebuilt with the arithmetic of
`sqla_ScalarHistory_factory.build`'s read path and of `compute_BBN_data`, in the
same order, so that the grid is bitwise the one production built
(`ratio_grid`). The callback is the production `build_rho_NP_callback`, and
the solve goes through the production boundary `_run_PRyMordial`. With
`--sm-baseline` the input is `compute_SM_baseline` (rho_NP = 0) instead.

Tolerances are changed by `stage_tolerance_override` and its special case
`lowT_tolerance_override`, which replace `PRyM.PRyM_main.solve_ivp` (and
observe `PRyM.PRyM_main._limited`) for their duration; nothing in `PRyM/`
changes. Without `--lowT-rtol`/`--lowT-atol` nothing is overridden and the
solve is production's.

Run from the repository root, since PRyMordial reads PRyMrates/ and the EOS its
table from the working directory:

    ./venv/bin/python tools/bbn_from_store.py STORE_STEM --beta B --M-Mp M \\
        [--phi-init-Mp 5] [--model exponential|inverse-power] [--n N] [--drain-law fixed|one_loop] [--alpha A] \\
        [--variant prod pert12 pert9 ...] [--small-network] \\
        [--wall-clock-limit SECS] [--lowT-rtol X] [--lowT-atol X|NAME] \\
        [--csv PATH] [--tag TEXT]
    ./venv/bin/python tools/bbn_from_store.py --sm-baseline [--small-network] \\
        [--lowT-rtol X] [--lowT-atol X|NAME] [--csv PATH] [--tag TEXT]

STORE_STEM is the path without `-shardNNNN.db`, for example
`~/ChamPBH-stores/science-2026.6.0`. M is in units of the reduced Planck mass.
A VARIANT is `prod` (the production callback on the stored grid, the default),
`pertE` (the production callback on r * (1 + 10^-E), for example `pert12`),
`linear` (r linear in ln T) or `pchip` (a monotone cubic in ln T). A NAME for
`--lowT-atol` is a key of `NAMED_ATOL`. `--wall-clock-limit 0` disables the
limit; the SM baseline never has one, as in production. `--csv` appends one row
per solve (`CSV_FIELDS`), under an exclusive lock, so several invocations may
share a file.

`--drain-law` (default `fixed`) names the history's drain law, as `tools/history_and_bbn.py`'s option does:
`--alpha` is alpha under `fixed` (0 <= alpha < 1, default 0) and A under `one_loop` (finite and > 0,
required), and the history is matched under that law's drain model (the key of `alpha_value` is the pair
(value, drain_model), one-loop-drain prompt 04): `fixed`, or under `one_loop` the model's own, below.

`--model {exponential,inverse-power}` (default exponential) and `--n N` (an integer >= 1, default 1) name the
history's model, as `tools/history_and_bbn.py`'s options do (power-law-wall prompt 05 §1.5, README §6.9 P37, P38):
the find query joins that model's table (ExponentialModel or InversePowerModel) and matches the history's
`model_type` and the model's `n_value`, since the two tables' serials repeat each other. Under `one_loop` the drain
model matched is the model's own (`one_loop_drain_model` at n: `one-loop-exponential-v2` for the exponential and
`one-loop-inverse-power-v1` for the power law, each at n = 1 and 2, since power-law-wall prompt 07c); a model with no
one-loop law at n (either potential at n other than 1 and 2) is refused at parse, exit 2, naming `--n`. A shard without the model's table
holds none of its histories; a shard whose ScalarHistory has no `model_type` column, or whose model table has no
`n_value` column (the hand-written imitations of the tests; no store of format 5 or later lacks them), is read
without that match, as before.

Prints one header line per history (`model=<model> n=<n>` after `M=` only away from the defaults, power-law-wall
prompt 05; `alpha=A` after `phi=` only at A > 0, inelastic-reflection prompt 10; ` A=<A> (<drain_model>)` under
`one_loop`), then one line per variant:

    store beta=... M=... phi=...: shard=... serial=... RHS=... window_samples=... stored: ...
      prod   Yp=... DoH=... He3oH=... Li7oH=... wall=... s
      pert12 FAILURE stage='...' t=... / ... wall=... s reason=...

Written for bbn-tolerance prompt 01, as a port of the Claude Science brief's
`bbn_from_store.py` (prompts/bbn-tolerance/source/).
"""

import argparse
import contextlib
import csv
import fcntl
import glob
import importlib
import os
import sqlite3
import subprocess
import sys
import time
from collections import namedtuple
from datetime import datetime, timezone
from math import exp, log
from pathlib import Path

import numpy as np

# make the repository root importable when run as `python tools/bbn_from_store.py`
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import PRyM.PRyM_main as PRyMmain  # noqa: E402
from Units import Planck_units  # noqa: E402
from config.argument_parser import alpha_fraction, positive_int  # noqa: E402
from CosmologyConcepts import M_value, beta_value  # noqa: E402
from CosmologyConcepts.drain_model import (  # noqa: E402
    DRAIN_LAW_FIXED,
    DRAIN_LAW_ONE_LOOP,
    DRAIN_LAWS,
    DRAIN_MODEL_FIXED,
    alpha_label,
    check_drain_model,
)
from CosmologyConcepts.Models import ExponentialModel, InversePowerModel  # noqa: E402
from CosmologyConcepts.Models.model_ids import (  # noqa: E402
    EXPONENTIAL_MODEL,
    INVERSE_POWER_MODEL,
)
from config.defaults import (  # noqa: E402
    DEFAULT_DIMENSIONLESS_QUANTITY_PRECISION,
    DEFAULT_DIMENSIONLESS_QUANTITY_RELATIVE_PRECISION,
)

# the module, not the BBNData class the package re-exports under the same name
B = importlib.import_module("ComputeTargets.BBNData")

# PRyMordial's stage names, as PRyM_main passes them to _limited,
# _check_wall_clock and _check_solve_ivp
STAGE_THERMO_NP = "thermodynamics (with NP)"
STAGE_THERMO_SM = "thermodynamics (no NP)"
STAGE_A_OF_T = "a(T)"
STAGE_HIGH_T = "high-T n <-> p"
STAGE_MID_T_SMALL = "mid-T nuclear network (small)"
STAGE_MID_T_FULL = "mid-T nuclear network (full)"
STAGE_LOW_T_SMALL = "low-T nuclear network (small)"
STAGE_LOW_T_FULL = "low-T nuclear network (full)"
LOW_T_STAGES = (STAGE_LOW_T_SMALL, STAGE_LOW_T_FULL)
ALL_STAGES = (
    STAGE_THERMO_NP,
    STAGE_THERMO_SM,
    STAGE_A_OF_T,
    STAGE_HIGH_T,
    STAGE_MID_T_SMALL,
    STAGE_MID_T_FULL,
    STAGE_LOW_T_SMALL,
    STAGE_LOW_T_FULL,
)

# the order of the network's abundances in PRyMordial's Y vector (full network;
# the small network has the first eight)
SPECIES = ("n", "p", "d", "t", "He3", "He4", "Li7", "Be7", "He6", "Li8", "Li6", "B8")

# Named per-species atol vectors for --lowT-atol NAME (full network only; a
# vector of the wrong length is refused by solve_ivp). Designed in bbn-tolerance
# log 01, scan block S2, from the abundances at the end of the low-T stage.
#
# "reported": for the seven species that enter a reported abundance (p, d, t,
# He3, He4, Li7, Be7), 1e-7 of the species' abundance at the end of the full
# low-T stage on the SM baseline (low-T rtol 1e-6; logs/01-probes/
# final_abundances_rtol1e-6.txt), to two figures, so that at the final value
# the absolute term is at most a tenth of the relative one for any rtol >= 1e-6;
# PRyMordial's own 1e-15 for the five that enter none (n, He6, Li8, Li6, B8),
# whose final abundances are at or below it.
NAMED_ATOL = {
    "reported": (
        1e-15,  # n
        7.5e-8,  # p
        1.9e-12,  # d
        5.9e-15,  # t
        7.8e-13,  # He3
        6.2e-9,  # He4
        2.2e-18,  # Li7
        3.9e-17,  # Be7
        1e-15,  # He6
        1e-15,  # Li8
        1e-15,  # Li6
        1e-15,  # B8
    ),
}

# the window and the domain compute_BBN_data uses, by its own defaults
T_BBN_KEV_SPLINE_MIN = 0.2
T_BBN_MEV_SPLINE_MAX = 100.0

CSV_FIELDS = (
    "utc",
    "commit",
    "tag",
    "input",
    "beta",
    "M_Mp",
    "phi_init_Mp",
    "shard",
    "serial",
    "variant",
    "network",
    "lowT_rtol",
    "lowT_atol",
    "wall_clock_limit",
    "status",
    "failure_stage",
    "t_reached",
    "t_target",
    "Yp",
    "DOverH",
    "He3OverH",
    "Li7OverH",
    "wall_s",
    "failure_reason",
)

StoredHistory = namedtuple(
    "StoredHistory",
    ["shard", "serial", "RHS_evaluations", "rows", "bbn", "first_bounce_log_T_Jordan"],
)
StoredHistory.__doc__ = """
One history read from the store. `rows` is an (n, 5) array of
(raw_N, log_T_Jordan_GeV, H_Jordan_Mp, log_rhorad_Jordan_Mp4, log_fm) in
production's read order (redshift descending). `bbn` is None, or a dict of the
BBNData row's failure, failure_reason, Yp_BBN, DOverH, He3OverH, Li7OverH,
small_network and PRyM_version.
"""


# ---------------------------------------------------------------------------
# the store, read-only


def connect_ro(path) -> sqlite3.Connection:
    """
    A read-only connection to one SQLite file: URI `file:PATH?mode=ro`. A write
    through it raises sqlite3.OperationalError. Every connection this tool
    opens is made here.
    """
    return sqlite3.connect(f"file:{os.fspath(path)}?mode=ro", uri=True)


# the tool reads a success's samples, so it asks for successes only: a history retried with
# --retry-failed-scalar has its failure row beside its success, under the same key, and a
# failure has no samples (tuning-followups prompt 01)
_FIND_QUERY = """
    select sm.serial, mv.value_eV, sm.RHS_evaluations, sm.first_bounce_log_T_Jordan
    from ScalarHistory sm
    join ExponentialModel m on m.serial = sm.model_serial
    join beta_value b on b.serial = m.beta_serial
    join M_value mv on mv.serial = m.M_serial
    join phi_value ph on ph.serial = sm.phi_Einstein_init_serial
    join alpha_value a on a.serial = sm.alpha_serial
    where abs(b.value - ?) < 1e-9 and abs(ph.value_PlanckMass - ?) < 1e-9
      and sm.failure = 0
"""

# the history's alpha (the drained fraction, a key since inelastic-reflection prompt 09) is matched
# as the dimensionless factory matches a value: absolutely at 0, relatively otherwise, with
# config.defaults' precisions (prompt 10, ruling 8)
_ALPHA_ZERO = f" and abs(a.value - ?) < {DEFAULT_DIMENSIONLESS_QUANTITY_PRECISION!r}"
_ALPHA_RELATIVE = f" and abs((a.value - ?) / ?) < {DEFAULT_DIMENSIONLESS_QUANTITY_RELATIVE_PRECISION!r}"

# a shard with no alpha_value table holds histories of before alpha was a key, which are alpha = 0
# (a store of before inelastic-reflection prompt 09, or an imitation of one): the query without
# its alpha join, for alpha = 0 only
_FIND_QUERY_WITHOUT_ALPHA = """
    select sm.serial, mv.value_eV, sm.RHS_evaluations, sm.first_bounce_log_T_Jordan
    from ScalarHistory sm
    join ExponentialModel m on m.serial = sm.model_serial
    join beta_value b on b.serial = m.beta_serial
    join M_value mv on mv.serial = m.M_serial
    join phi_value ph on ph.serial = sm.phi_Einstein_init_serial
    where abs(b.value - ?) < 1e-9 and abs(ph.value_PlanckMass - ?) < 1e-9
      and sm.failure = 0
"""

# --model and --n (power-law-wall prompt 05 §1.5, README §6.9 P37, P38): the option's values, as
# tools/history_and_bbn.py's, each with its class, its table and its type id. The two queries above are the
# exponential's; the power law's join its own table, and are otherwise the same
MODEL_EXPONENTIAL = "exponential"
MODEL_INVERSE_POWER = "inverse-power"
MODEL_CLASSES = {
    MODEL_EXPONENTIAL: ExponentialModel,
    MODEL_INVERSE_POWER: InversePowerModel,
}
MODEL_TABLES = {
    MODEL_EXPONENTIAL: ("ExponentialModel", EXPONENTIAL_MODEL),
    MODEL_INVERSE_POWER: ("InversePowerModel", INVERSE_POWER_MODEL),
}
MODEL_DEFAULT = MODEL_EXPONENTIAL
N_DEFAULT = 1

_EXPONENTIAL_JOIN = "join ExponentialModel m on m.serial = sm.model_serial"
_FIND_QUERIES = {
    MODEL_EXPONENTIAL: (_FIND_QUERY, _FIND_QUERY_WITHOUT_ALPHA),
    MODEL_INVERSE_POWER: tuple(
        query.replace(
            _EXPONENTIAL_JOIN, "join InversePowerModel m on m.serial = sm.model_serial"
        )
        for query in (_FIND_QUERY, _FIND_QUERY_WITHOUT_ALPHA)
    ),
}

# the history's model type and the model's n, bound; appended before the alpha match, so that the drain model's
# parameter stays the last
_MODEL_TYPE_MATCH = " and sm.model_type = ?"
_N_MATCH = " and m.n_value = ?"


# the model of the history's alpha_value row (a key since one-loop-drain prompt 03), bound from the caller's
# constant; a shard whose alpha_value has no drain_model column is of a format before 7 and holds fixed rows only
_DRAIN_MODEL_MATCH = " and a.drain_model = ?"


def _has_table(con, name: str) -> bool:
    return (
        con.execute(
            "select 1 from sqlite_master where type = 'table' and name = ?", (name,)
        ).fetchone()
        is not None
    )


def _has_column(con, table: str, column: str) -> bool:
    return any(
        row[1] == column
        for row in con.execute(f"pragma table_info({table})").fetchall()
    )


def _find_query(
    con,
    beta,
    phi_init_Mp,
    alpha,
    drain_model=DRAIN_MODEL_FIXED,
    model=MODEL_DEFAULT,
    n=N_DEFAULT,
):
    """
    (sql, parameters) of the shard's find query for this alpha under this drain model, of this model (an option
    value of --model) at this n; None if the shard cannot hold it. A shard with no alpha_value table, or one whose
    alpha_value has no drain_model column, holds the fixed model only (one-loop-drain prompt 04, P39), so it cannot
    hold another. The query joins the model's table and matches the history's model_type and the model's n_value
    (power-law-wall prompt 05 §1.5, P37, P38): a shard without the power law's table holds none of its histories,
    and a shard whose ScalarHistory has no model_type column, or whose model table no n_value column, is read
    without that match (only the tests' hand-written imitations lack them).
    """
    table, type_id = MODEL_TABLES[model]
    with_alpha, without_alpha = _FIND_QUERIES[model]
    if model != MODEL_EXPONENTIAL and not _has_table(con, table):
        return None
    model_match, model_bound = "", ()
    if _has_column(con, "ScalarHistory", "model_type"):
        model_match, model_bound = _MODEL_TYPE_MATCH, (type_id,)
    elif model != MODEL_EXPONENTIAL:
        return None
    if _has_column(con, table, "n_value"):
        model_match, model_bound = model_match + _N_MATCH, (*model_bound, n)

    has_alpha = _has_table(con, "alpha_value")
    if not has_alpha:
        if alpha == 0.0 and drain_model == DRAIN_MODEL_FIXED:
            if model_match == "":
                return without_alpha, (beta, phi_init_Mp)
            return without_alpha + model_match, (beta, phi_init_Mp, *model_bound)
        return None
    has_model = any(
        row[1] == "drain_model"
        for row in con.execute("pragma table_info(alpha_value)").fetchall()
    )
    if has_model:
        match, bound = _DRAIN_MODEL_MATCH, (drain_model,)
    elif drain_model == DRAIN_MODEL_FIXED:
        match, bound = "", ()
    else:
        return None
    if alpha == 0.0:
        return with_alpha + model_match + _ALPHA_ZERO + match, (
            beta,
            phi_init_Mp,
            *model_bound,
            alpha,
            *bound,
        )
    return with_alpha + model_match + _ALPHA_RELATIVE + match, (
        beta,
        phi_init_Mp,
        *model_bound,
        alpha,
        alpha,
        *bound,
    )


def model_label(model: str, n: int) -> str:
    """
    What the header and a LookupError gain for the model, as tools/history_and_bbn.py's model_label: "" at the
    defaults (exponential, n = 1), so that every line is as before; otherwise " model=<model> n=<n>".
    """
    if model == MODEL_DEFAULT and n == N_DEFAULT:
        return ""
    return f" model={model} n={n}"


def _alpha_text(alpha: float, drain_model: str = DRAIN_MODEL_FIXED) -> str:
    """
    Under the fixed model ` alpha=<α:g>` at alpha > 0 and nothing at 0, so that every message at 0 is as
    before; under any other model ` A=<A:g> (<drain_model>)` (alpha_label without its spaces around `=`).
    """
    if drain_model != DRAIN_MODEL_FIXED:
        return " " + alpha_label(alpha, drain_model).replace(" = ", "=", 1)
    return f" alpha={alpha:g}" if alpha > 0.0 else ""


# production's order: sqla_ScalarHistoryFactory.build reads the samples ordered
# by ln(1 + z), descending (the axis since jordan-normalization prompt 03)
_VALUES_QUERY = """
    select v.raw_N, v.log_T_Jordan_GeV, v.H_Jordan_Mp, v.log_rhorad_Jordan_Mp4, v.log_fm
    from ScalarHistoryValue v
    join redshift r on r.serial = v.z_serial
    where v.history_serial = ?
    order by r.log_one_plus_z desc
"""

_BBN_QUERY = """
    select failure, failure_reason, Yp_BBN, DOverH, He3OverH, Li7OverH,
           small_network, PRyM_version
    from BBNData where scalar_history_serial = ?
"""
_BBN_KEYS = (
    "failure",
    "failure_reason",
    "Yp_BBN",
    "DOverH",
    "He3OverH",
    "Li7OverH",
    "small_network",
    "PRyM_version",
)


def find_model(
    stem,
    beta: float,
    M_Mp: float,
    phi_init_Mp: float = 5.0,
    units=None,
    *,
    alpha=0.0,
    drain_model=DRAIN_MODEL_FIXED,
    model=MODEL_DEFAULT,
    n=N_DEFAULT,
):
    """
    The one successful ScalarHistory (`failure = 0`; tuning-followups prompt 01)
    in the shards `STEM-shard*.db` with this beta and initial phi (to 1e-9
    absolute) and this M (to 1e-3 relative, M_value being in eV and M_Mp in
    units of the reduced Planck mass, units.PlanckMass / units.eV). Returns a
    StoredHistory. Raises LookupError if there is no match or more than one.
    Every shard is opened by `connect_ro`. ``alpha`` (keyword only, default 0) is the history's
    drained fraction, matched as the dimensionless factory matches it: to
    DEFAULT_DIMENSIONLESS_QUANTITY_PRECISION absolutely at 0, to the relative precision otherwise
    (inelastic-reflection prompt 10, ruling 8). ``drain_model`` (keyword only, default the fixed model) is the
    model of the history's alpha_value row, and ``alpha`` is then alpha under it or A under another model
    (one-loop-drain prompt 04, P39). ``model`` and ``n`` (keyword only, defaults exponential and 1) are the
    history's model, an option value of --model, and its index: the query joins that model's table and matches the
    history's model_type and the model's n_value (power-law-wall prompt 05, P37, P38).
    """
    if units is None:
        units = Planck_units()
    Mp_eV = units.PlanckMass / units.eV

    shards = sorted(glob.glob(os.fspath(stem) + "-shard*.db"))
    if len(shards) == 0:
        raise LookupError(f"no shards match {os.fspath(stem)}-shard*.db")

    found = []
    for shard in shards:
        con = connect_ro(shard)
        try:
            query = _find_query(con, beta, phi_init_Mp, alpha, drain_model, model, n)
            for serial, M_eV, rhs, fb_logT in (
                con.execute(*query) if query is not None else ()
            ):
                if abs(M_eV / Mp_eV / M_Mp - 1.0) < 1e-3:
                    rows = con.execute(_VALUES_QUERY, (serial,)).fetchall()
                    bbn_rows = con.execute(_BBN_QUERY, (serial,)).fetchall()
                    if len(bbn_rows) > 1:
                        raise LookupError(
                            f"{len(bbn_rows)} BBNData rows for serial {serial} in {shard}"
                        )
                    bbn = dict(zip(_BBN_KEYS, bbn_rows[0])) if bbn_rows else None
                    found.append(
                        StoredHistory(
                            shard=shard,
                            serial=serial,
                            RHS_evaluations=rhs,
                            rows=np.array(rows, dtype=float).reshape(-1, 5),
                            bbn=bbn,
                            first_bounce_log_T_Jordan=fb_logT,
                        )
                    )
        finally:
            con.close()

    if len(found) == 0:
        raise LookupError(
            f"no ScalarHistory with beta={beta:g} M={M_Mp:g}{model_label(model, n)} phi={phi_init_Mp:g}"
            f"{_alpha_text(alpha, drain_model)} in {stem}"
        )
    if len(found) > 1:
        where = ", ".join(f"{Path(h.shard).name}:{h.serial}" for h in found)
        raise LookupError(
            f"{len(found)} ScalarHistories match beta={beta:g} M={M_Mp:g}{model_label(model, n)} "
            f"phi={phi_init_Mp:g}{_alpha_text(alpha, drain_model)}: {where}"
        )
    return found[0]


# ---------------------------------------------------------------------------
# the ratio grid, bitwise production's


def ratio_grid(
    rows,
    units,
    T_BBN_keV_spline_min: float = T_BBN_KEV_SPLINE_MIN,
    T_BBN_MeV_spline_max: float = T_BBN_MEV_SPLINE_MAX,
):
    """
    (log(T_J / MeV), rho_NP / rho_R,J) on the samples in
    [T_BBN_keV_spline_min keV, T_BBN_MeV_spline_max MeV], from rows of
    (raw_N, log_T_Jordan_GeV, H_Jordan_Mp, log_rhorad_Jordan_Mp4, log_fm) in the
    store's units and production's order. Returns two float arrays.

    The arithmetic is that of sqla_ScalarHistory_factory.build's read path (the
    store's units to `units`) followed by compute_BBN_data's loop, operation for
    operation and in the same order, so that the arrays are bitwise the ones
    compute_BBN_data hands build_rho_NP_callback. Do not "simplify" it:
    PRyMordial's D/H moves by up to ~1e-3 under ulp-level changes to the ratio
    at its default low-T tolerance (bbn-tolerance log 01), and the brief
    (section 2.1) reports that an earlier reconstruction differing at the ulp
    level moved a control's D/H by 3.7e-4.
    """
    # the factory's read path
    log_Mp = log(units.PlanckMass)
    log_GeV = log(units.GeV)
    # compute_BBN_data
    CONST_MP_SQ = units.PlanckMass * units.PlanckMass
    CONST_3_MP_SQ = 3.0 * CONST_MP_SQ
    T_BBN_spline_max = T_BBN_MeV_spline_max * units.MeV
    T_BBN_spline_min = T_BBN_keV_spline_min * units.keV
    log_MeV = log(units.MeV)

    out_logT, out_r = [], []
    for raw_N, log_T_Jordan_GeV, H_Jordan_Mp, log_rhorad_Jordan_Mp4, log_fm in rows:
        # ScalarHistoryValue as the factory builds it
        log_rhorad_Jordan = float(log_rhorad_Jordan_Mp4) + 4.0 * log_Mp
        log_T_Jordan = float(log_T_Jordan_GeV) + log_GeV
        H_Jordan = float(H_Jordan_Mp) * units.PlanckMass

        # compute_BBN_data's loop
        T_Jordan = exp(log_T_Jordan)
        if T_BBN_spline_min <= T_Jordan <= T_BBN_spline_max:
            rhorad_Jordan = exp(log_rhorad_Jordan)
            H2_Jordan = H_Jordan * H_Jordan
            fm = exp(float(log_fm))
            LHS = H2_Jordan * CONST_3_MP_SQ
            density_NP = LHS - rhorad_Jordan * (1.0 + fm)
            out_logT.append(log_T_Jordan - log_MeV)
            out_r.append(density_NP / rhorad_Jordan)

    return np.array(out_logT, dtype=float), np.array(out_r, dtype=float)


def callback_domain_MeV(
    units,
    T_BBN_keV_spline_min: float = T_BBN_KEV_SPLINE_MIN,
    T_BBN_MeV_spline_max: float = T_BBN_MEV_SPLINE_MAX,
):
    """(T_min_MeV, T_max_MeV) as compute_BBN_data forms them."""
    return (
        T_BBN_keV_spline_min * units.keV / units.MeV,
        T_BBN_MeV_spline_max * units.MeV / units.MeV,
    )


def make_callback(kind: str, logT, r, rho_SM, T_min_MeV, T_max_MeV, label: str):
    """
    The rho_NP callback for one variant: `prod` is build_rho_NP_callback on the
    grid; `pertE` the same on r * (1 + 10^-E); `linear` and `pchip` replace the
    production cubic with r linear in ln T or a monotone cubic.
    """
    if kind == "prod":
        return B.build_rho_NP_callback(
            list(logT), list(r), rho_SM, T_min_MeV, T_max_MeV, label
        )
    if kind.startswith("pert"):
        eps = 10.0 ** (-int(kind[4:]))
        return B.build_rho_NP_callback(
            list(logT),
            [x * (1.0 + eps) for x in r],
            rho_SM,
            T_min_MeV,
            T_max_MeV,
            label,
        )
    if kind not in ("linear", "pchip"):
        raise ValueError(f"unknown variant {kind!r}")

    from scipy.interpolate import PchipInterpolator

    x, y = np.asarray(logT)[::-1], np.asarray(r)[::-1]
    if kind == "linear":
        f = lambda u: np.interp(u, x, y)  # noqa: E731
    else:
        f = PchipInterpolator(x, y, extrapolate=False)

    def cb(T):
        if T <= 0:
            return 0.0
        if T < T_min_MeV or T > T_max_MeV:
            raise B.ComputationFailureError(
                f"T_in_MeV={T:.5g} outside [{T_min_MeV}, {T_max_MeV}]"
            )
        return float(f(log(T))) * rho_SM(T)

    return cb


# ---------------------------------------------------------------------------
# the interception


class StageCall:
    """
    One solve_ivp call as the override saw it: the stage it belongs to (None
    if no stage was announced), t_span, y0, the keyword arguments actually
    passed to SciPy, and the result's status, success and last t.
    """

    def __init__(self, stage, t_span, y0, kwargs):
        self.stage = stage
        self.t_span = (float(t_span[0]), float(t_span[1]))
        self.y0 = np.array(y0, dtype=float)
        self.kwargs = dict(kwargs)
        self.status = None
        self.success = None
        self.t_reached = None
        self.sol = None


class _StageInterceptor:
    """
    Stands in for PRyM_main.solve_ivp and observes PRyM_main._limited.

    How the stage is recognised: every Python-branch solve_ivp call in
    PRyM_main passes its fun (and jac) through `_limited(fn, stage, ...)` in
    its own argument list, so `_limited` is called with the call's stage name
    immediately before that solve_ivp runs and at no other time (its wrapper,
    when the wall-clock limit is set, calls _check_wall_clock, not _limited).
    The observer records the last stage named and returns `_limited`'s own
    result unchanged, so the fun and jac PRyMordial passes are the same
    objects as without the override. The stage is consumed by the solve_ivp
    call that follows; a call with no stage announced is passed through
    untouched and recorded with stage None. This depends on neither the call's
    position nor its tolerances, so it keeps working once a tolerance is
    added to the low-T call.
    """

    def __init__(self, settings, methods=None, keep_sol=(), record=None):
        for stage in list(settings) + list(methods or {}):
            if stage not in ALL_STAGES:
                raise ValueError(f"unknown PRyMordial stage {stage!r}")
        self.settings = dict(settings)
        self.methods = dict(methods or {})
        self.keep_sol = tuple(keep_sol)
        self.calls = [] if record is None else record
        self._pending = None
        self._solve_ivp = PRyMmain.solve_ivp
        self._limited = PRyMmain._limited

    def limited(self, fn, stage, t_start, limit):
        self._pending = stage
        return self._limited(fn, stage, t_start, limit)

    def solve_ivp(self, fun, t_span, y0, **kwargs):
        stage, self._pending = self._pending, None
        if stage in self.settings:
            rtol, atol = self.settings[stage]
            if rtol is not None:
                kwargs["rtol"] = rtol
            if atol is not None:
                kwargs["atol"] = atol
        if stage in self.methods:
            kwargs["method"] = self.methods[stage]
        call = StageCall(stage, t_span, y0, kwargs)
        self.calls.append(call)
        sol = self._solve_ivp(fun, t_span, y0, **kwargs)
        call.status = sol.status
        call.success = bool(sol.success)
        call.t_reached = float(sol.t[-1]) if len(sol.t) > 0 else float("nan")
        if stage in self.keep_sol:
            call.sol = sol
        return sol


@contextlib.contextmanager
def stage_tolerance_override(settings, methods=None, keep_sol=(), record=None):
    """
    For the duration of the block, PRyMordial's solve_ivp call in each stage
    named in `settings` ({stage: (rtol, atol)}; None leaves that argument as
    PRyMordial passes it) gets those keyword arguments, and in each stage named
    in `methods` ({stage: OdeSolver subclass}) that method. Every other call,
    and every other argument, is passed through unchanged. Stage names are
    ALL_STAGES. Yields the list of StageCall records, one per solve_ivp call
    (`record`, if given, is appended to instead); `keep_sol` names the stages
    whose full solve_ivp result is kept on the record. Restores both names on
    exit, whether or not the block raised.
    """
    spy = _StageInterceptor(settings, methods=methods, keep_sol=keep_sol, record=record)
    saved = (PRyMmain.solve_ivp, PRyMmain._limited)
    PRyMmain.solve_ivp, PRyMmain._limited = spy.solve_ivp, spy.limited
    try:
        yield spy.calls
    finally:
        PRyMmain.solve_ivp, PRyMmain._limited = saved


def lowT_tolerance_override(
    rtol=None, atol=None, methods=None, keep_sol=(), record=None
):
    """
    `stage_tolerance_override` on the low-temperature nuclear network only, both
    the full and the small network's call: rtol and atol (None: as PRyMordial
    passes it, that is no rtol, so SciPy's 1e-3, and atol 1e-15 full, 1e-11
    small, on 4ae25b4). A context manager yielding the StageCall records.
    """
    settings = {stage: (rtol, atol) for stage in LOW_T_STAGES}
    return stage_tolerance_override(
        settings, methods=methods, keep_sol=keep_sol, record=record
    )


def resolve_atol(text):
    """--lowT-atol: None, a float, or a NAMED_ATOL key (an array)."""
    if text is None:
        return None
    if text in NAMED_ATOL:
        return np.array(NAMED_ATOL[text], dtype=float)
    return float(text)


# ---------------------------------------------------------------------------
# one solve


def _outcome(result: dict, calls, wall: float) -> dict:
    out = {
        "status": "ok",
        "failure_stage": "",
        "t_reached": "",
        "t_target": "",
        "Yp": "",
        "DOverH": "",
        "He3OverH": "",
        "Li7OverH": "",
        "failure_reason": "",
        "wall_s": wall,
    }
    failed = [c for c in calls if c.success is False]
    if failed:
        c = failed[-1]
        out["failure_stage"] = c.stage or ""
        out["t_reached"] = c.t_reached
        out["t_target"] = c.t_span[1]
    if result.get("failure", False):
        out["status"] = "FAILURE"
        out["failure_reason"] = result.get("failure_reason", "")
    else:
        for key, name in (
            ("Yp_BBN", "Yp"),
            ("DOverH", "DOverH"),
            ("He3OverH", "He3OverH"),
            ("Li7OverH", "Li7OverH"),
        ):
            out[name] = float(result[key])
    return out


def solve_history_variant(
    kind,
    logT,
    r,
    rho_SM,
    T_min_MeV,
    T_max_MeV,
    label,
    small_network=False,
    wall_clock_limit=B.DEFAULT_BBN_WALL_CLOCK_LIMIT,
    rtol=None,
    atol=None,
):
    """
    One PRyMordial solve of one variant through `_run_PRyMordial`, under
    `lowT_tolerance_override(rtol, atol)` when either is given (and otherwise
    under an override that changes nothing, which only records the calls).
    Returns (outcome dict, StageCall records).
    """
    cb = make_callback(kind, logT, r, rho_SM, T_min_MeV, T_max_MeV, label)
    start = time.perf_counter()
    with lowT_tolerance_override(rtol=rtol, atol=atol) as calls:
        result = B._run_PRyMordial(cb, small_network, wall_clock_limit)
    wall = time.perf_counter() - start
    return _outcome(result, calls, wall), calls


def solve_SM_baseline(small_network=False, rtol=None, atol=None):
    """
    compute_SM_baseline under `lowT_tolerance_override(rtol, atol)`. A failed
    baseline raises in production; here the exception is returned as a
    FAILURE outcome. Returns (outcome dict, StageCall records).
    """
    start = time.perf_counter()
    with lowT_tolerance_override(rtol=rtol, atol=atol) as calls:
        try:
            result = B.compute_SM_baseline(small_network)
        except Exception as e:
            result = {"failure": True, "failure_reason": f"{type(e).__name__}: {e}"}
    wall = time.perf_counter() - start
    return _outcome(result, calls, wall), calls


# ---------------------------------------------------------------------------
# the command line


def _commit() -> str:
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        return sha + ("+dirty" if dirty else "")
    except Exception:
        return "unknown"


def _append_csv(path, row: dict):
    path = Path(path)
    with open(path, "a", newline="") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            fh.seek(0, os.SEEK_END)
            writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
            if fh.tell() == 0:
                writer.writeheader()
            writer.writerow({k: row.get(k, "") for k in CSV_FIELDS})
            fh.flush()
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def _fmt(x) -> str:
    return "" if x == "" or x is None else f"{x:.10g}"


def _print_outcome(kind: str, out: dict):
    if out["status"] == "ok":
        print(
            f"  {kind:6s} Yp={_fmt(out['Yp'])} DoH={_fmt(out['DOverH'])} "
            f"He3oH={_fmt(out['He3OverH'])} Li7oH={_fmt(out['Li7OverH'])} "
            f"wall={out['wall_s']:.1f} s",
            flush=True,
        )
    else:
        where = ""
        if out["failure_stage"]:
            where = (
                f"stage={out['failure_stage']!r} t={out['t_reached']:.6g} / "
                f"{out['t_target']:.6g} "
            )
        print(
            f"  {kind:6s} FAILURE {where}wall={out['wall_s']:.1f} s "
            f"reason={out['failure_reason'][:200]}",
            flush=True,
        )


def _law_alpha(parser, text, drain_law: str, drain_model: str) -> float:
    """
    --alpha under the drain law (one-loop-drain prompt 04, orchestrator note 04 ruling 4; after
    tools/history_and_bbn.py's _law_alpha). Under the fixed law, alpha_fraction's check and message, as
    argparse gave them when it was --alpha's type, and 0 where absent; under one_loop, A, required, checked by
    check_drain_model under drain_model, the model's own one-loop drain model (power-law-wall prompt 05, P38). A
    refusal is parser.error: exit 2, naming the option.
    """
    if drain_law == DRAIN_LAW_FIXED:
        if text is None:
            return 0.0
        try:
            return alpha_fraction(text)
        except argparse.ArgumentTypeError as e:
            parser.error(f"argument --alpha: {e}")
        except ValueError:
            parser.error(f"argument --alpha: invalid alpha_fraction value: {text!r}")
    if text is None:
        parser.error(
            "argument --alpha: --drain-law one_loop needs --alpha A, the law's strength, finite "
            "and > 0"
        )
    try:
        value = float(text) + 0.0
    except ValueError:
        parser.error(f"argument --alpha: invalid value: {text!r}")
    try:
        check_drain_model(drain_model, value, "argument --alpha")
    except ValueError as e:
        parser.error(str(e))
    return value


def _drain_model(parser, args) -> str:
    """
    The drain model the history's key names (power-law-wall prompt 05 §1.5, P38): DRAIN_MODEL_FIXED under the fixed
    law, and under one_loop the model's own one-loop drain model at --n (AbstractModel.one_loop_drain_model),
    never a law-wide default (model_of_law, removed by power-law-wall prompt 07c), read from the model built at the
    run's M and beta (M_P and 1 where not given; the hook reads neither). A model with no one-loop law at n is
    parser.error: exit 2, naming --n.
    """
    if args.drain_law == DRAIN_LAW_FIXED:
        return DRAIN_MODEL_FIXED
    units = Planck_units()
    M_Mp = 1.0 if args.M_Mp is None else args.M_Mp
    beta = 1.0 if args.beta is None else args.beta
    instance = MODEL_CLASSES[args.model](
        0, M_value(0, M_Mp * units.PlanckMass), args.n, beta_value(0, beta), units
    )
    try:
        return instance.one_loop_drain_model
    except (NotImplementedError, ValueError) as e:
        parser.error(f"argument --n: --drain-law one_loop: {e}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("stem", nargs="?", help="the store path without -shardNNNN.db")
    parser.add_argument("--beta", type=float, help="the coupling's beta")
    parser.add_argument(
        "--M-Mp", type=float, help="the potential's M, in units of the reduced M_P"
    )
    parser.add_argument(
        "--phi-init-Mp",
        type=float,
        default=5.0,
        help="initial phi in units of M_P (default 5)",
    )
    parser.add_argument(
        "--model",
        choices=tuple(MODEL_CLASSES),
        default=MODEL_DEFAULT,
        help="the model (default %(default)s): exponential, V_0 exp((M/phi)^n); inverse-power, "
        "V_0 [1 + (M/phi)^n] (power-law-wall prompt 01). Away from the defaults every label names "
        "the model and n",
    )
    parser.add_argument(
        "--n",
        type=positive_int,
        default=N_DEFAULT,
        help="the model's index n, an integer >= 1 (default %(default)s)",
    )
    parser.add_argument(
        "--drain-law",
        choices=DRAIN_LAWS,
        default=DRAIN_LAW_FIXED,
        help="the history's drain law (default %(default)s), as tools/history_and_bbn.py: fixed, with --alpha "
        "its drained fraction; one_loop, the model's own one-loop model, with --alpha as A "
        "(one-loop-drain prompt 04; power-law-wall prompt 05)",
    )
    parser.add_argument(
        "--alpha",
        default=None,
        help="the history's drained fraction alpha, 0 <= alpha < 1 (default 0: natural mode), under "
        "--drain-law fixed; A, finite and > 0 and required, under one_loop",
    )
    parser.add_argument(
        "--sm-baseline",
        action="store_true",
        help="run compute_SM_baseline (rho_NP = 0) instead of a stored history",
    )
    parser.add_argument(
        "--variant",
        nargs="+",
        default=["prod"],
        help="prod, pertE, linear, pchip (default: prod)",
    )
    parser.add_argument(
        "--small-network",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="PRyMordial's small network (default: the full network, which logs 01 and 01c's "
        "reproduction commands assume; main.py runs the small network since bbn-tolerance prompt 02)",
    )
    parser.add_argument(
        "--wall-clock-limit",
        type=float,
        default=B.DEFAULT_BBN_WALL_CLOCK_LIMIT,
        metavar="SECS",
        help=f"wall-clock limit per solve (default {B.DEFAULT_BBN_WALL_CLOCK_LIMIT:g}, "
        f"as production; 0 disables it); ignored with --sm-baseline",
    )
    parser.add_argument(
        "--lowT-rtol", type=float, default=None, help="rtol for the low-T call"
    )
    parser.add_argument(
        "--lowT-atol",
        default=None,
        help=f"atol for the low-T call: a number or one of {sorted(NAMED_ATOL)}",
    )
    parser.add_argument("--csv", default=None, help="append one row per solve here")
    parser.add_argument("--tag", default="", help="a label written to the CSV rows")
    args = parser.parse_args(argv)
    args.drain_model = _drain_model(parser, args)
    args.alpha = _law_alpha(parser, args.alpha, args.drain_law, args.drain_model)

    if not Path(os.getcwd(), "PRyMrates").is_dir():
        print(
            f"!! bbn_from_store: run from the repository root; PRyMordial reads PRyMrates/ "
            f"from the working directory ({os.getcwd()})"
        )
        return 2

    atol = resolve_atol(args.lowT_atol)
    network = "small" if args.small_network else "full"
    base = {
        "commit": _commit(),
        "tag": args.tag,
        "network": network,
        "lowT_rtol": "" if args.lowT_rtol is None else repr(args.lowT_rtol),
        "lowT_atol": "" if args.lowT_atol is None else args.lowT_atol,
    }
    setting = (
        f"lowT rtol={'default' if args.lowT_rtol is None else repr(args.lowT_rtol)} "
        f"atol={'default' if args.lowT_atol is None else args.lowT_atol}, {network} network"
    )

    if args.sm_baseline:
        print(f"SM baseline (rho_NP = 0), {setting}", flush=True)
        out, _ = solve_SM_baseline(args.small_network, args.lowT_rtol, atol)
        _print_outcome("SM", out)
        if args.csv:
            row = dict(base, **out)
            row.update(
                utc=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                input="SM",
                variant="SM",
                wall_clock_limit="none",
            )
            _append_csv(args.csv, row)
        return 0

    if args.stem is None or args.beta is None or args.M_Mp is None:
        parser.error("STORE_STEM, --beta and --M-Mp are required without --sm-baseline")

    wall_clock_limit = None if args.wall_clock_limit == 0 else args.wall_clock_limit
    units = Planck_units()
    from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
    from CosmologyModels.LambdaCDM import Planck2018

    cosmology = QCD_Cosmology(0, units, Planck2018())
    stem = os.path.expanduser(args.stem)
    h = find_model(
        stem,
        args.beta,
        args.M_Mp,
        args.phi_init_Mp,
        units,
        alpha=args.alpha,
        drain_model=args.drain_model,
        model=args.model,
        n=args.n,
    )
    logT, r = ratio_grid(h.rows, units)
    T_min_MeV, T_max_MeV = callback_domain_MeV(units)
    rho_SM = B.thermodynamic_rho_SM(cosmology, units)

    bbn = h.bbn or {}
    stored = (
        f"failure={bbn.get('failure')} Yp={bbn.get('Yp_BBN')} DoH={bbn.get('DOverH')} "
        f"PRyM_version={bbn.get('PRyM_version')} reason={(bbn.get('failure_reason') or '')[:160]}"
    )
    shard_id = Path(h.shard).name[-7:-3]
    print(
        f"store beta={args.beta:g} M={args.M_Mp:g}{model_label(args.model, args.n)} "
        f"phi={args.phi_init_Mp:g}"
        f"{_alpha_text(args.alpha, args.drain_model)}: "
        f"shard={shard_id} serial={h.serial} RHS={h.RHS_evaluations} "
        f"window_samples={len(r)} {setting}\n  stored: {stored}",
        flush=True,
    )

    label = f"bbn_from_store-beta{args.beta:g}-M{args.M_Mp:g}"
    for kind in args.variant:
        out, _ = solve_history_variant(
            kind,
            logT,
            r,
            rho_SM,
            T_min_MeV,
            T_max_MeV,
            label,
            small_network=args.small_network,
            wall_clock_limit=wall_clock_limit,
            rtol=args.lowT_rtol,
            atol=atol,
        )
        _print_outcome(kind, out)
        if args.csv:
            row = dict(base, **out)
            row.update(
                utc=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                input=f"beta={args.beta:g} M={args.M_Mp:g}"
                + model_label(args.model, args.n),
                beta=args.beta,
                M_Mp=args.M_Mp,
                phi_init_Mp=args.phi_init_Mp,
                shard=shard_id,
                serial=h.serial,
                variant=kind,
                wall_clock_limit=(
                    "none" if wall_clock_limit is None else wall_clock_limit
                ),
            )
            _append_csv(args.csv, row)
    return 0


if __name__ == "__main__":
    sys.exit(main())
