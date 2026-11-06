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
The derivation tool (one-loop-drain prompt 05 §1.3-§1.5; README §8.3, §8.13 P42-P46): the spectrum-dependent measures of
every one-loop history of a plot run's exports, from the exports alone. No store is opened and no Ray is started.

    PYTHONPATH=. ./venv/bin/python tools/quanta_measures.py EXPORT_ROOT [--assoc-scale C]

EXPORT_ROOT is a plot run's `<output>/<label>/`, holding histories.csv and the per-row directories (log 04 §7). C is the
associated piece's scale c_a (default 1; P46), the only choice of spectrum: U4's shape (U16), with U17's end
(ComputeTargets/quanta_spectrum.py, SPECTRUM_MODEL).

**Finding a history.** Each histories.csv row whose drain_model is not `fixed` and whose `normalization` is not blank names
one directory, EXPORT_ROOT / alpha_path(A, drain_model) / "csv" / history_directory(beta, M_eV, n), by extract_common's
helpers (M_eV = M_Mp x M_P/eV, from the Units package; n parsed as an integer). Two rows that give one directory are both
refused, naming each other. A row whose directory lacks fields.csv or bounces.csv is counted as missing. A fixed-law row is
skipped and counted; a one-loop row with a blank normalization (a failed history) is counted.

**The generations** are the candidate rows (source `candidate`) with delta_K_Mp4 > 0 and a log_q_Mp, in ordinal order:
born at N_turn, with ln Q_g = log_q_Mp (M_P) and Delta K_g = delta_K_Mp4 (M_P^4). A candidate with delta_K > 0 and no log_q
(unassessed, or with its margins not computed) is in K and not a generation (orchestrator note 05, ruling 8); the tool
counts such rows on the history's line.

**K(N)**, rebuilt from the candidate rows (P43; ruling 6), right-continuous, changing only at the drain N (`N`) of a
candidate with delta_K > 0, where it becomes K_before + delta_K; below the first candidate it is the first K_before (the
ledger's K_initial). In ordinal order the candidate rows must have ordinals 0, 1, ..., strictly increasing candidate_index
and non-decreasing N and N_turn, or the history is refused. The parking leftover is a jump of K_before above the previous
K after by more than K_ULP_TOLERANCE relative: it is placed at the previous candidate's N (the cut candidate's, where
dark_radiation.py's ledger enters it), and its size must equal parking_share x K_final to K_ULP_TOLERANCE x K_final. Where
no row brackets it and parking_share > 0, it is placed at the last candidate's N, with K_final = K_last / (1 -
parking_share). A second jump, a fall, or a jump with no parking share refuses the history.
**The check** (§1.3): at every sample, K(N) against r_chi rho_R,E e^{4N} from fields.csv (rho_chi_over_rho_rad,
log_rhorad_Einstein_GeV4 converted to M_P^4 with ln(GeV/M_P) from the Units package, raw_N), the relative difference to
K_CHECK_TOLERANCE; where K(N) = 0, rho_chi_over_rho_rad must be exactly 0. A history that fails is reported and nothing of
it is written.

**The measures** (P44; prompt §1.4), in float64:
- ln n_g = ln A_g + ln s(x_p) per generation, A_g = 2 pi^2 Delta K_g / (Q_g^4 I);
- n_bar_j = sum over generations g born strictly before generation j (N_turn_g < N_turn_j) of A_g Omega(ln Q_g - ln Q_j),
  untruncated: O(G^2) evaluations of Omega per history; delta_stim = sum_j 2 n_bar_j Delta K_j / K_final, the fractional
  change of the final rho_chi that stimulated emission would make at first order with random phases. Rescattering is
  bounded by it (about 2n, README §8.3); the bound is stated, not computed;
- E(N) at every sample, for the field's mass and the tracker's: sum over g with N_turn_g <= N of Delta K_g G(m e^N / Q_g) /
  K(N), with ln y = (N + ln m) - ln Q_g; blank where the mass is blank, and E, r_chi E and the parking fraction are blank
  where K(N) = 0 (ruling 7). Where K(N) > 0 and nothing is born, E = 0.0. The parking leftover has no spectrum
  ([00-the-parking-leftover-has-no-spectrum]): it is in K(N) and not in the sum, and parking_fraction = K_parking/K(N) is
  written beside E (0.0 before it enters);
- the phase slip as source/probe/drain/phase_slip.py defines it, over the assessed candidates (log_k_ex_Mp not blank) in
  ordinal order: for an interior row x between rows a and b, Q = exp(log_k_ex_x + N_x), theta(p, q) = Q e^{-(N_p +
  N_q)/2} (N_q - N_p) / H_p with H_p = exp(log_H_p), and slip = |theta(x, b) - theta(a, x)|; k_ex/H = exp(log_k_ex_x -
  log_H_x) at every assessed candidate;
- the backreaction ratio at every candidate row with log_m_Mp and log_E_Mp4: n_chi a^3 = sum over g with N_turn_g < N_turn_k
  of Delta K_g J / Q_g, and the ratio n_chi a^3 e^{-3 N_turn_k} m_k / E_k.

**The outputs** (P45; ruling 10). Every row of every file begins with `spectrum_model` and `assoc_scale`. At c_a = 1 the
files are quanta_bounces.csv and quanta_samples.csv beside each measured history's bounces.csv, and quanta_histories.csv at
EXPORT_ROOT; at any other scale each name carries the suffix `-assoc<c:g>` (quanta_bounces-assoc0.csv). Floats are written
as repr, integers as integers, NULL as an empty cell. The tool overwrites its own files, and removes a history's own files
of the same scale when that history is refused or fails the check. It prints one line per history and a summary.

**Exit status:** 2 on a usage error or an unreadable export root; otherwise 1 if any row was refused or any history failed
the K check (after writing everything else), else 0.
"""

import argparse
import csv
import sys
import time
from bisect import bisect_right
from math import exp, expm1, inf, isfinite, log
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import numpy as np

from ComputeTargets import quanta_spectrum as qs
from CosmologyConcepts.drain_model import DRAIN_MODEL_FIXED
from Units import Planck_units
from Units.base import UnitsLike
from extract_common import alpha_path, history_directory

# K_before + delta_K against the next K_before, and the leftover's size against parking_share x K_final: relative to the
# larger K (ruling 6). Each converted cell is one division by M_P^4 (half an ulp), and the ledger's sum one addition (half
# an ulp), so the two sides differ by at most about 2.5 ulps relative in any units; 8 epsilon is that with a margin. In
# Planck_units the conversion is exact and they are equal bit for bit (counted, never asserted)
K_ULP_TOLERANCE = 8.0 * 2.0**-52

# the check of K(N) against r_chi rho_R,E e^{4N} (§1.3; ruling 6), relative. r_chi was formed by the integrator as
# exp(ln K - 4N - ln rho_R,E) and the tool re-forms ln K = ln r_chi + ln rho_R,E[GeV^4] + 4 ln(GeV/M_P) + 4N: each pass
# carries an absolute error of about epsilon times the magnitudes summed, |ln K| + 4|N| + |ln rho_R,E| + 4|ln(GeV/M_P)|,
# below 1.3e3 over 02d's sample (verification §5), so about 3e-13 for the two passes. After a leftover that no row
# brackets, K_final = K_last/(1 - parking_share) carries a relative error of about epsilon parking_share/(1 -
# parking_share), 1.4e5 epsilon = 3.2e-11 at 02d's largest share (0.999993, C-3016-A0.05). 1e-9 is the sum with a margin
# of 30
K_CHECK_TOLERANCE = 1.0e-9

BOUNCE_OUTPUT_COLUMNS = [
    "spectrum_model",
    "assoc_scale",
    "ordinal",
    "source",
    "N",
    "N_turn",
    "ln_n_g",
    "n_bar",
    "phase_slip",
    "k_ex_over_H",
    "backreaction_ratio",
]
SAMPLE_OUTPUT_COLUMNS = [
    "spectrum_model",
    "assoc_scale",
    "raw_N",
    "log_one_plus_z",
    "K_Mp4",
    "parking_fraction",
    "E_field",
    "E_tracker",
    "r_chi_E_field",
    "r_chi_E_tracker",
]
KEY_COLUMNS = ["drain_model", "alpha", "M_Mp", "n", "beta"]
HISTORY_OUTPUT_COLUMNS = [
    "spectrum_model",
    "assoc_scale",
    *KEY_COLUMNS,
    "generations",
    "delta_stim",
    "n_bar_max",
    "n_bar_max_ordinal",
    "ln_n_g_max",
    "ln_n_g_median",
    "E_field_max",
    "E_field_max_N",
    "E_field_today",
    "r_chi_E_field_max",
    "E_tracker_max",
    "E_tracker_max_N",
    "E_tracker_today",
    "r_chi_E_tracker_max",
    "phase_slip_min_producing",
    "phase_slip_min",
    "phase_slips_below_1",
    "k_ex_over_H_min",
    "backreaction_max",
    "backreaction_max_N",
    "parking_share",
    "K_check_max_rel",
]

LEFTOVER_NONE = "none"
LEFTOVER_BRACKETED = "bracketed"
LEFTOVER_AFTER_LAST = "after the last candidate"


class Refused(Exception):
    """A history the tool will not measure, with the reason."""


def suffix(assoc_scale: float) -> str:
    """'' at c_a = 1, else '-assoc<c:g>' (ruling 10)."""
    return "" if assoc_scale == 1.0 else f"-assoc{assoc_scale:g}"


def _float(cell: Optional[str]) -> Optional[float]:
    if cell is None or cell == "":
        return None
    return float(cell)


def _int(cell: Optional[str]) -> Optional[int]:
    if cell is None or cell == "":
        return None
    return int(cell)


def _cell(x):
    if x is None:
        return ""
    if isinstance(x, (bool, np.bool_)):
        return int(x)
    if isinstance(x, (int, np.integer)):
        return int(x)
    if isinstance(x, (float, np.floating)):
        return repr(float(x))
    return x


def read_csv(path: Path):
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def write_csv(path: Path, columns: Sequence[str], rows: Sequence[dict]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for r in rows:
            writer.writerow([_cell(r.get(c)) for c in columns])


# ---------------------------------------------------------------------------------------------------------------------
# The history's directory
# ---------------------------------------------------------------------------------------------------------------------


def history_dir(root: Path, row: dict, units: UnitsLike) -> Path:
    """The csv directory of one histories.csv row, by alpha_path and history_directory (ruling 9)."""
    n_cell = row["n"]
    try:
        n = int(n_cell)
    except ValueError:
        raise Refused(f"n = {n_cell!r} is not an integer")
    M_eV = float(row["M_Mp"]) * (units.PlanckMass / units.eV)
    base = alpha_path(Path(root), float(row["alpha"]), row["drain_model"])
    return base / f"csv/{history_directory(float(row['beta']), M_eV, n)}"


def row_label(row: dict) -> str:
    return (
        f"{row['drain_model']} A={row['alpha']} M_Mp={row['M_Mp']} n={row['n']} "
        f"beta={row['beta']}"
    )


# ---------------------------------------------------------------------------------------------------------------------
# The inputs
# ---------------------------------------------------------------------------------------------------------------------


class Bounce:
    """One bounces.csv row, parsed (None for a blank)."""

    FIELDS = (
        ("ordinal", _int),
        ("candidate_index", _int),
        ("N", _float),
        ("N_turn", _float),
        ("delta_K_Mp4", _float),
        ("K_before_Mp4", _float),
        ("log_k_ex_Mp", _float),
        ("log_m_Mp", _float),
        ("log_q_Mp", _float),
        ("log_E_Mp4", _float),
        ("log_H_Mp", _float),
    )

    def __init__(self, row: dict):
        self.source: str = row["source"]
        for name, parse in self.FIELDS:
            setattr(self, name, parse(row.get(name)))
        self.raw = row


class Sample:
    """One fields.csv row, parsed."""

    def __init__(self, row: dict):
        self.raw_N = _float(row["raw_N"])
        self.log_one_plus_z = _float(row["log_one_plus_z"])
        self.r_chi = _float(row["rho_chi_over_rho_rad"])
        self.log_rhorad_GeV4 = _float(row["log_rhorad_Einstein_GeV4"])
        self.log_m_eff = _float(row["log_m_eff_Mp"])
        self.log_m_tracker = _float(row["log_m_tracker_Mp"])
        self.raw = row


# ---------------------------------------------------------------------------------------------------------------------
# K(N) (ruling 6)
# ---------------------------------------------------------------------------------------------------------------------


class KHistory:
    """K(N) rebuilt from the candidate rows: right-continuous, its entries (N, K after) in order."""

    def __init__(self, candidates: Sequence[Bounce], parking_share: Optional[float]):
        ps = 0.0 if parking_share is None else float(parking_share)
        self.entries_N: List[float] = []
        self.entries_K: List[float] = []
        self.leftover: float = 0.0
        self.leftover_N: Optional[float] = None
        self.placement: str = LEFTOVER_NONE
        # K_before + delta_K equal to the next K_before bit for bit (a measurement, ruling 6), and the pairs compared
        self.bitwise_equal = 0
        self.compared = 0
        if len(candidates) == 0:
            if ps > 0.0:
                raise Refused("a parking share with no candidate row")
            self.K_initial = 0.0
            self.K_final = 0.0
            return
        for c in candidates:
            if c.K_before_Mp4 is None or c.delta_K_Mp4 is None or c.N is None:
                raise Refused(
                    f"candidate row {c.ordinal} has a blank K_before_Mp4, delta_K_Mp4 or N"
                )
        self.K_initial = candidates[0].K_before_Mp4
        K = self.K_initial
        for i, c in enumerate(candidates):
            kb = c.K_before_Mp4
            if i > 0:
                self.compared += 1
                self.bitwise_equal += kb == K
                d = kb - K
                if abs(d) > K_ULP_TOLERANCE * max(abs(kb), abs(K)):
                    if d < 0.0:
                        raise Refused(
                            f"K falls before candidate row {c.ordinal}: K_before {kb!r} below {K!r}"
                        )
                    if self.leftover_N is not None:
                        raise Refused(
                            f"a second jump of K before candidate row {c.ordinal}"
                        )
                    self.leftover = d
                    self.leftover_N = candidates[i - 1].N
                    self.placement = LEFTOVER_BRACKETED
                    self.entries_N.append(self.leftover_N)
                    self.entries_K.append(kb)
            K = kb
            if c.delta_K_Mp4 > 0.0:
                K = kb + c.delta_K_Mp4
                self.entries_N.append(c.N)
                self.entries_K.append(K)
        if self.placement == LEFTOVER_BRACKETED:
            self.K_final = K
            if not ps > 0.0:
                raise Refused(
                    f"K jumps by {self.leftover!r} at N = {self.leftover_N!r} with no parking share"
                )
            if abs(self.leftover - ps * self.K_final) > K_ULP_TOLERANCE * self.K_final:
                raise Refused(
                    f"the leftover {self.leftover!r} is not parking_share x K_final = {ps * self.K_final!r}"
                )
        elif ps > 0.0:
            self.K_final = K / (1.0 - ps)
            self.leftover = self.K_final - K
            self.leftover_N = candidates[-1].N
            self.placement = LEFTOVER_AFTER_LAST
            self.entries_N.append(self.leftover_N)
            self.entries_K.append(self.K_final)
        else:
            self.K_final = K

    def K(self, N: float) -> float:
        j = bisect_right(self.entries_N, N)
        return self.K_initial if j == 0 else self.entries_K[j - 1]

    def K_parking(self, N: float) -> float:
        if self.leftover_N is None or N < self.leftover_N:
            return 0.0
        return self.leftover


def K_check(
    K: KHistory, samples: Sequence[Sample], units: UnitsLike
) -> (float, List[str]):
    """
    (the largest relative difference, the failures) of K(N) against r_chi rho_R,E e^{4N} at every sample (§1.3).
    ln(GeV/M_P) is the UnitsLike's; the result does not depend on which UnitsLike gives it beyond the tolerance.
    """
    log_GeV4_over_Mp4 = 4.0 * log(units.GeV / units.PlanckMass)
    worst = 0.0
    failures = []
    for s in samples:
        k = K.K(s.raw_N)
        if k == 0.0:
            if s.r_chi != 0.0:
                failures.append(f"K(N) = 0 at N = {s.raw_N!r} but r_chi = {s.r_chi!r}")
            continue
        if s.r_chi is None or not s.r_chi > 0.0:
            failures.append(f"K(N) = {k!r} at N = {s.raw_N!r} but r_chi = {s.r_chi!r}")
            worst = inf
            continue
        log_K_fields = (
            log(s.r_chi) + s.log_rhorad_GeV4 + log_GeV4_over_Mp4 + 4.0 * s.raw_N
        )
        rel = abs(expm1(log_K_fields - log(k)))
        worst = max(worst, rel)
        if not rel <= K_CHECK_TOLERANCE:
            failures.append(
                f"K(N) = {k!r} at N = {s.raw_N!r} differs from r_chi rho_R e^4N by {rel:.3g} relative"
            )
    return worst, failures


# ---------------------------------------------------------------------------------------------------------------------
# The measures (prompt §1.4)
# ---------------------------------------------------------------------------------------------------------------------


class Generations:
    """The generations of a history, in ordinal order: arrays of N_turn, ln Q, Delta K and their rows."""

    def __init__(self, candidates: Sequence[Bounce]):
        self.rows = [
            c
            for c in candidates
            if c.delta_K_Mp4 is not None
            and c.delta_K_Mp4 > 0.0
            and c.log_q_Mp is not None
        ]
        self.without_spectrum = sum(
            1
            for c in candidates
            if c.delta_K_Mp4 is not None and c.delta_K_Mp4 > 0.0 and c.log_q_Mp is None
        )
        self.N_turn = np.array([g.N_turn for g in self.rows], dtype=float)
        self.log_q = np.array([g.log_q_Mp for g in self.rows], dtype=float)
        self.delta_K = np.array([g.delta_K_Mp4 for g in self.rows], dtype=float)
        self.N = np.array([g.N for g in self.rows], dtype=float)

    def __len__(self):
        return len(self.rows)


def matter_excess(spectrum, gens: Generations, Ns, log_m, Ks) -> List[Optional[float]]:
    """
    E(N) at each sample (prompt §1.4): sum over g with N_turn_g <= N of Delta K_g G(y) / K(N), ln y = (N + ln m) - ln Q_g,
    with G the spectrum's (the only route by which E reads a kernel, so that the Derive gate's peak-collapsed spectrum
    tests this code; ruling 4). None where K(N) = 0 or the mass is None; 0.0 where nothing is born.
    """
    out: List[Optional[float]] = []
    for N, lm, K in zip(Ns, log_m, Ks):
        if K == 0.0 or lm is None:
            out.append(None)
            continue
        born = int(np.searchsorted(gens.N_turn, N, side="right"))
        if born == 0:
            out.append(0.0)
            continue
        log_y = (N + lm) - gens.log_q[:born]
        out.append(float(np.sum(gens.delta_K[:born] * spectrum.G_of_log_y(log_y)) / K))
    return out


def occupation_overlaps(spectrum, gens: Generations) -> np.ndarray:
    """n_bar_j for each generation j: sum over g with N_turn_g < N_turn_j of A_g Omega(ln Q_g - ln Q_j), untruncated."""
    log_A = (
        qs.LOG_TWO_PI_SQUARED + np.log(gens.delta_K) - 4.0 * gens.log_q - spectrum.log_I
    )
    out = np.zeros(len(gens))
    for j in range(len(gens)):
        before = int(np.searchsorted(gens.N_turn, gens.N_turn[j], side="left"))
        if before == 0:
            continue
        u = gens.log_q[:before] - gens.log_q[j]
        out[j] = float(np.sum(np.exp(log_A[:before] + spectrum.log_Omega(u))))
    return out


def phase_slips(candidates: Sequence[Bounce]):
    """
    {ordinal: (slip or None, k_ex/H)} over the assessed candidates in ordinal order, by phase_slip.py's formula; the
    slip is defined at interior rows only.
    """
    c = [x for x in candidates if x.log_k_ex_Mp is not None]
    out = {}
    for j, x in enumerate(c):
        kH = exp(x.log_k_ex_Mp - x.log_H_Mp) if x.log_H_Mp is not None else None
        slip = None
        if 0 < j < len(c) - 1:
            a, b = c[j - 1], c[j + 1]
            if a.log_H_Mp is not None and x.log_H_Mp is not None:
                log_Q = x.log_k_ex_Mp + x.N

                def theta(p, q):
                    return exp(log_Q - 0.5 * (p.N + q.N) - p.log_H_Mp) * (q.N - p.N)

                slip = abs(theta(x, b) - theta(a, x))
        out[x.ordinal] = (slip, kH)
    return out


def backreaction_ratios(spectrum, gens: Generations, candidates: Sequence[Bounce]):
    """{ordinal: ratio} at each candidate row with log_m and log_E: n_chi a^3 e^{-3 N_turn} m / E, n_chi a^3 = sum over
    g with N_turn_g < N_turn_k of Delta K_g J / Q_g."""
    weights = gens.delta_K * np.exp(-gens.log_q)
    prefix = np.concatenate([[0.0], np.cumsum(weights)])
    out = {}
    for k in candidates:
        if k.log_m_Mp is None or k.log_E_Mp4 is None:
            continue
        before = int(np.searchsorted(gens.N_turn, k.N_turn, side="left"))
        S = float(prefix[before])
        if S == 0.0:
            out[k.ordinal] = 0.0
            continue
        out[k.ordinal] = exp(
            log(S) + spectrum.log_J - 3.0 * k.N_turn + k.log_m_Mp - k.log_E_Mp4
        )
    return out


def _max_with(values, at):
    best, where = None, None
    for v, n in zip(values, at):
        if v is not None and (best is None or v > best):
            best, where = v, n
    return best, where


def measure_history(
    spectrum,
    history_row: dict,
    bounces: Sequence[Bounce],
    samples: Sequence[Sample],
    units: UnitsLike,
):
    """
    Every measure of one history: (bounce rows, sample rows, history row, info) for the three files and the printed line.
    Raises Refused for an input the tool will not measure. A failed K check returns info["K_failures"], non-empty.
    """
    # the order (ruling 6)
    for i, b in enumerate(bounces):
        if b.ordinal != i:
            raise Refused(
                f"bounces.csv is not in ordinal order: row {i} has ordinal {b.ordinal}"
            )
    candidates = [b for b in bounces if b.source == "candidate"]
    for prev, c in zip(candidates[:-1], candidates[1:]):
        if not (
            c.candidate_index is not None
            and prev.candidate_index is not None
            and c.candidate_index > prev.candidate_index
        ):
            raise Refused(
                f"candidate_index does not increase strictly at ordinal {c.ordinal}"
            )
        if not c.N >= prev.N:
            raise Refused(f"N decreases at ordinal {c.ordinal}")
        if not c.N_turn >= prev.N_turn:
            raise Refused(f"N_turn decreases at ordinal {c.ordinal}")
    expected_rows = _int(history_row.get("bounce_rows"))
    if expected_rows is not None and expected_rows != len(bounces):
        raise Refused(
            f"bounces.csv has {len(bounces)} rows, histories.csv's bounce_rows {expected_rows}"
        )
    for s in samples:
        if s.raw_N is None or s.r_chi is None or s.log_rhorad_GeV4 is None:
            raise Refused("a fields.csv row has a blank raw_N, r_chi or rho_R,E")

    parking_share = _float(history_row.get("parking_share"))
    K = KHistory(candidates, parking_share)
    worst, failures = K_check(K, samples, units)
    info = dict(
        K_failures=failures,
        K_check_max_rel=worst,
        placement=K.placement,
        K_bitwise_equal=K.bitwise_equal,
        K_compared=K.compared,
    )
    if failures:
        return None, None, None, info

    gens = Generations(candidates)
    info["generations"] = len(gens)
    info["without_spectrum"] = gens.without_spectrum

    Ns = [s.raw_N for s in samples]
    Ks = [K.K(N) for N in Ns]
    info["K_zero_with_born"] = sum(
        1
        for N, k in zip(Ns, Ks)
        if k == 0.0 and np.searchsorted(gens.N_turn, N, side="right") > 0
    )
    info["between_birth_and_drain"] = sum(
        1 for N in Ns if bool(np.any((gens.N_turn <= N) & (N < gens.N)))
    )

    E_field = matter_excess(spectrum, gens, Ns, [s.log_m_eff for s in samples], Ks)
    E_tracker = matter_excess(
        spectrum, gens, Ns, [s.log_m_tracker for s in samples], Ks
    )

    sample_rows = []
    for s, k, ef, et in zip(samples, Ks, E_field, E_tracker):
        sample_rows.append(
            dict(
                raw_N=s.raw_N,
                log_one_plus_z=s.log_one_plus_z,
                K_Mp4=k,
                parking_fraction=None if k == 0.0 else K.K_parking(s.raw_N) / k,
                E_field=ef,
                E_tracker=et,
                r_chi_E_field=None if ef is None else s.r_chi * ef,
                r_chi_E_tracker=None if et is None else s.r_chi * et,
            )
        )

    # per generation and per producing bounce
    ln_n_g = (
        qs.LOG_TWO_PI_SQUARED
        + np.log(gens.delta_K)
        - 4.0 * gens.log_q
        - spectrum.log_I
        + spectrum.log_s_peak
        if len(gens) > 0
        else np.zeros(0)
    )
    n_bar = occupation_overlaps(spectrum, gens) if len(gens) > 0 else np.zeros(0)
    by_gen = {
        g.ordinal: (float(a), float(b)) for g, a, b in zip(gens.rows, ln_n_g, n_bar)
    }
    slips = phase_slips(candidates)
    back = backreaction_ratios(spectrum, gens, candidates)

    bounce_rows = []
    for b in bounces:
        g = by_gen.get(b.ordinal)
        slip, kH = slips.get(b.ordinal, (None, None))
        bounce_rows.append(
            dict(
                ordinal=b.ordinal,
                source=b.source,
                N=b.N,
                N_turn=b.N_turn,
                ln_n_g=None if g is None else g[0],
                n_bar=None if g is None else g[1],
                phase_slip=slip,
                k_ex_over_H=kH,
                backreaction_ratio=back.get(b.ordinal),
            )
        )

    # per history
    h = {c: history_row[c] for c in KEY_COLUMNS}
    h["generations"] = len(gens)
    if len(gens) > 0:
        h["delta_stim"] = float(np.sum(2.0 * n_bar * gens.delta_K) / K.K_final)
        j = int(np.argmax(n_bar))
        h["n_bar_max"] = float(n_bar[j])
        h["n_bar_max_ordinal"] = gens.rows[j].ordinal
        h["ln_n_g_max"] = float(np.max(ln_n_g))
        h["ln_n_g_median"] = float(np.median(ln_n_g))
    for tag, E in (("field", E_field), ("tracker", E_tracker)):
        h[f"E_{tag}_max"], h[f"E_{tag}_max_N"] = _max_with(E, Ns)
        h[f"E_{tag}_today"] = E[-1] if len(E) > 0 else None
        rE = [None if e is None else s.r_chi * e for e, s in zip(E, samples)]
        h[f"r_chi_E_{tag}_max"], _ = _max_with(rE, Ns)
    producing = {c.ordinal for c in candidates if (c.delta_K_Mp4 or 0.0) > 0.0}
    all_slips = [(o, s) for o, (s, _) in slips.items() if s is not None]
    if all_slips:
        h["phase_slip_min"] = min(s for _, s in all_slips)
        prod = [s for o, s in all_slips if o in producing]
        h["phase_slip_min_producing"] = min(prod) if prod else None
        h["phase_slips_below_1"] = sum(1 for _, s in all_slips if s < 1.0)
    kHs = [kH for _, kH in slips.values() if kH is not None]
    h["k_ex_over_H_min"] = min(kHs) if kHs else None
    if back:
        o = max(back, key=lambda key: back[key])
        h["backreaction_max"] = back[o]
        h["backreaction_max_N"] = bounces[o].N_turn
    h["parking_share"] = parking_share
    h["K_check_max_rel"] = worst
    return bounce_rows, sample_rows, h, info


# ---------------------------------------------------------------------------------------------------------------------
# The run over an export root
# ---------------------------------------------------------------------------------------------------------------------


def run(
    root: Path, assoc_scale: float = qs.DEFAULT_ASSOC_SCALE, units=None, out=None
) -> int:
    """Measure every one-loop history under root; write the three files; print the lines. Returns the exit status."""
    out = sys.stdout if out is None else out
    units = Planck_units() if units is None else units
    root = Path(root)
    histories_csv = root / "histories.csv"
    try:
        _, rows = read_csv(histories_csv)
    except (OSError, csv.Error, UnicodeDecodeError) as e:
        print(f"!! quanta_measures: cannot read {histories_csv}: {e}", file=sys.stderr)
        return 2
    spectrum = qs.spectrum(assoc_scale)
    tag = suffix(spectrum.assoc_scale)
    model = dict(spectrum_model=spectrum.model, assoc_scale=spectrum.assoc_scale)
    t_start = time.perf_counter()

    counts = dict(
        measured=0, fixed=0, failed_history=0, missing=0, refused=0, K_failed=0
    )
    candidates = []
    for row in rows:
        if row["drain_model"] == DRAIN_MODEL_FIXED:
            counts["fixed"] += 1
            continue
        if row.get("normalization", "") == "":
            counts["failed_history"] += 1
            continue
        try:
            d = history_dir(root, row, units)
        except Refused as e:
            counts["refused"] += 1
            print(f"REFUSED {row_label(row)}: {e}", file=out)
            continue
        candidates.append((row, d))

    by_dir: Dict[Path, list] = {}
    for row, d in candidates:
        by_dir.setdefault(d, []).append(row)

    history_rows = []
    for row, d in candidates:
        label = row_label(row)
        files = (
            d / f"quanta_bounces{tag}.csv",
            d / f"quanta_samples{tag}.csv",
        )

        def drop_own():
            for p in files:
                if p.exists():
                    p.unlink()

        if len(by_dir[d]) > 1:
            others = [row_label(r) for r in by_dir[d] if r is not row]
            counts["refused"] += 1
            print(
                f"REFUSED {label}: its directory {d} is also that of {'; '.join(others)}",
                file=out,
            )
            drop_own()
            continue
        if not (d / "fields.csv").exists() or not (d / "bounces.csv").exists():
            counts["missing"] += 1
            print(f"MISSING {label}: no fields.csv or bounces.csv in {d}", file=out)
            continue
        t0 = time.perf_counter()
        try:
            _, b_rows = read_csv(d / "bounces.csv")
            _, f_rows = read_csv(d / "fields.csv")
            bounces = [Bounce(r) for r in b_rows]
            samples = [Sample(r) for r in f_rows]
            b_out, s_out, h_out, info = measure_history(
                spectrum, row, bounces, samples, units
            )
        except (Refused, ValueError, KeyError, TypeError) as e:
            counts["refused"] += 1
            print(f"REFUSED {label}: {type(e).__name__}: {e}", file=out)
            drop_own()
            continue
        wall = time.perf_counter() - t0
        if info["K_failures"]:
            counts["K_failed"] += 1
            print(
                f"K CHECK FAILED {label}: {len(info['K_failures'])} samples; largest "
                f"{info['K_check_max_rel']:.3g}; first: {info['K_failures'][0]}",
                file=out,
            )
            drop_own()
            continue
        write_csv(files[0], BOUNCE_OUTPUT_COLUMNS, [dict(model, **r) for r in b_out])
        write_csv(files[1], SAMPLE_OUTPUT_COLUMNS, [dict(model, **r) for r in s_out])
        history_rows.append(dict(model, **h_out))
        counts["measured"] += 1
        print(
            f"{label}: generations {info['generations']}; drains without spectrum {info['without_spectrum']}; "
            f"leftover {info['placement']}; K bitwise {info['K_bitwise_equal']}/{info['K_compared']}; "
            f"K check {info['K_check_max_rel']:.3g}; K(N) = 0 with a generation born {info['K_zero_with_born']}; "
            f"between N_turn and N {info['between_birth_and_drain']}; wall {wall:.3f} s",
            file=out,
        )

    write_csv(root / f"quanta_histories{tag}.csv", HISTORY_OUTPUT_COLUMNS, history_rows)
    wall = time.perf_counter() - t_start
    print(
        f"quanta_measures ({spectrum.model}, assoc scale {spectrum.assoc_scale:g}): measured {counts['measured']}, "
        f"skipped as fixed-law {counts['fixed']}, failed histories {counts['failed_history']}, missing files "
        f"{counts['missing']}, refused {counts['refused']}, failed the K check {counts['K_failed']}; wall {wall:.1f} s",
        file=out,
    )
    return 1 if counts["refused"] or counts["K_failed"] else 0


def _scale(text: str) -> float:
    try:
        c = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a number: {text!r}")
    if not isfinite(c) or c < 0.0:
        raise argparse.ArgumentTypeError(
            f"the associated piece's scale must be finite and non-negative, not {text!r}"
        )
    return c


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="The spectrum-dependent measures of a plot run's one-loop exports (one-loop-drain prompt 05)"
    )
    parser.add_argument("export_root", help="a plot run's <output>/<label>/")
    parser.add_argument(
        "--assoc-scale",
        type=_scale,
        default=qs.DEFAULT_ASSOC_SCALE,
        help="the associated piece's scale c_a (default 1)",
    )
    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        return 2 if e.code else 0
    root = Path(args.export_root)
    if not root.is_dir():
        print(f"!! quanta_measures: {root} is not a directory", file=sys.stderr)
        return 2
    return run(root, args.assoc_scale)


if __name__ == "__main__":
    sys.exit(main())
