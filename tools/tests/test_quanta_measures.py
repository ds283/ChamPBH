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
tools/quanta_measures.py, the derivation tool (one-loop-drain prompt 05 §1.3-§1.5, §1.7, §2.2; orchestrator note 05,
rulings 4-10).

**On constructed exports** (histories.csv rows and their directories written by 04's writers from constructed rows, with
r_chi made by a DarkRadiationLedger as the integrator makes it; tools/tests/quanta_exports.py):
- every measure against a direct computation by quadrature over the full spectrum in q (no kernel: the occupations
  n_g(q) = A_g s(q/Q_g) integrated as defined), to 1e-6 relative, with five generations at known scales (two of them
  beyond Omega's table), an unassessed drain, a candidate that drains nothing and a plateau event, at c_a = 1, 0 and 10;
- K(N) in each of §1.3's cases: no leftover, the leftover after the last candidate, and the leftover before a later
  candidate (bracketed), against the ledger that made the rows; the refusals of ruling 6 (order, a fall, a leftover that
  is not parking_share x K_final); the check's failure (nothing written, exit 1); the check in three UnitsLike;
- K(N) = 0 and a blank mass give blanks; nothing born gives 0.0; the counts of ruling 7 and 8;
- two rows giving one directory refused; a fixed-law row and a failed history skipped; a missing bounces.csv counted;
  n not an integer refused; every output column, its blanks and the model's two columns; the suffix at c_a = 0 and 10;
  the exit statuses.

**One stored history**, P-2197 at A = 1 (one_loop_payload, one integration, about 2-4 s; shared through
test_drain_store's cache), written on a temporary store by _DrainStoreCase, read back, exported by 04's writers and
measured:
- K(N) at every sample equals the payload ledger's K(N), bitwise before the leftover and to its tolerance after it (no
  row brackets it);
- the generations equal the payload's (count, N_turn, ln q, Delta K);
- one value of each measure against the direct computation;
- the Derive gate's §2.2: E with the spectrum collapsed to its peak against the scoping probe's `excess`, extracted by
  ast from source/probe/all_generations.py and run unedited, at every sample and for both masses, to the tolerance of
  orchestrator note 05's ruling 5.

No Ray, no persistent store, no PRyMordial solve.
"""

import ast
import contextlib
import csv
import io
import shutil
import tempfile
import unittest
from math import exp, isfinite, log, pi, sqrt
from pathlib import Path

import numpy as np
from scipy.integrate import quad

import extract_common as ec
from ComputeTargets import quanta_spectrum as qs
from CosmologyConcepts.drain_model import DRAIN_MODEL_FIXED, DRAIN_MODEL_ONE_LOOP
from ObjectFactories.ScalarHistory import LINEAR_MP4, to_column
from Units import GeV_units, Mpc_units, Planck_units
from tools import quanta_measures as qm
from tools.tests import quanta_exports as qe

ONE_LOOP = DRAIN_MODEL_ONE_LOOP
EPS = 2.0**-52
PROBE = qe.ROOT / "prompts/one-loop-drain/source/probe/all_generations.py"

# ruling 5's tolerance: |E_tool - E_probe| <= a eps sum_born dK/K + b |E_probe|, b = B eps (number born)
PROBE_A = 2.0
PROBE_B = 8.0


# ---------------------------------------------------------------------------------------------------------------------
# Direct computations, by quadrature over the full spectrum in q (no kernel)
# ---------------------------------------------------------------------------------------------------------------------


def _quad_x(f, extra=()):
    """int_0^inf f(x) dx for an integrand carrying s(x) of a generation (x = q/Q), split at x_p and the extra points."""
    xp, k = qs.X_PEAK, qs.KAPPA
    pts = sorted(
        {0.0, xp, *[p for p in extra if p > 0.0], xp + 4 / k, xp + 16 / k, xp + 60 / k}
    )
    pts = [p for p in pts if p <= xp + 60 / k]
    scale = sum(
        abs(quad(f, a, b, epsrel=1e-8, limit=200)[0]) for a, b in zip(pts, pts[1:])
    )
    return sum(
        quad(f, a, b, epsabs=1e-16 * scale, epsrel=1e-12, limit=400)[0]
        for a, b in zip(pts, pts[1:])
    )


def direct_I(c):
    return _quad_x(lambda x: x**3 * float(qs.shape(x, c)))


def direct_A(dK, log_q, c):
    """A_g = 2 pi^2 dK / (Q^4 I), I by quadrature."""
    return 2.0 * pi * pi * dK / (exp(4.0 * log_q) * direct_I(c))


def direct_energy_excess(dK, log_q, m_a, c):
    """(1/2 pi^2) int q^3 n_g(q) [sqrt(1 + (m a/q)^2) - 1] dq, in x = q/Q."""
    Q = exp(log_q)
    A = direct_A(dK, log_q, c)
    y = m_a / Q
    # (1/2pi^2) Q^4 A int x^3 s(x) [sqrt(1 + y^2/x^2) - 1] dx, the bracket as y^2/(x(sqrt(x^2+y^2)+x))
    g = lambda x: x * x * float(qs.shape(x, c)) * y * y / (sqrt(x * x + y * y) + x)
    return Q**4 * A / (2.0 * pi * pi) * _quad_x(g, extra=(y / 10, y, 10 * y))


def direct_E(gens, N, log_m, K, c):
    """E(N) by quadrature: sum over N_turn <= N of the excess, over K."""
    total = 0.0
    for N_turn, log_q, dK in gens:
        if N_turn <= N:
            total += direct_energy_excess(dK, log_q, exp(N + log_m), c)
    return total / K


def direct_n_bar(gens, j, c):
    """int q^3 s(q/Q_j) sum_{g before j} n_g(q) dq / int q^3 s(q/Q_j) dq, in x = q/Q_j."""
    N_j, lq_j, _ = gens[j]
    num = 0.0
    for N_g, lq_g, dK in gens:
        if N_g < N_j:
            A = direct_A(dK, lq_g, c)
            r = exp(lq_j - lq_g)
            num += A * _quad_x(
                lambda x: x**3 * float(qs.shape(x, c)) * float(qs.shape(x * r, c)),
                extra=(
                    qs.X_PEAK / r,
                    *(qs.X_PEAK / r + k / (qs.KAPPA * r) for k in (1, 4, 16, 60)),
                ),
            )
    den = _quad_x(lambda x: x**3 * float(qs.shape(x, c)))
    return num / den


def direct_n_chi_a3(gens, N_k, c):
    """sum over N_turn < N_k of (1/2 pi^2) int q^2 n_g(q) dq."""
    total = 0.0
    for N_g, lq, dK in gens:
        if N_g < N_k:
            Q = exp(lq)
            A = direct_A(dK, lq, c)
            total += (
                Q**3
                * A
                / (2 * pi * pi)
                * _quad_x(lambda x: x * x * float(qs.shape(x, c)))
            )
    return total


def probe_slip(rows):
    """phase_slip.py's loop, written out on the assessed candidate rows (dicts with N, log_k_ex, H_before)."""
    slips, kH = [], []
    for j in range(1, len(rows) - 1):
        a, x, b = rows[j - 1], rows[j], rows[j + 1]
        Q = exp(x["log_k_ex"] + x["N"])

        def theta(p, q):
            return Q * exp(-0.5 * (p["N"] + q["N"])) * (q["N"] - p["N"]) / p["H_before"]

        slips.append(abs(theta(x, b) - theta(a, x)))
        kH.append(exp(x["log_k_ex"]) / x["H_before"])
    return slips, kH


def rel(a, b):
    return abs(a / b - 1.0)


# ---------------------------------------------------------------------------------------------------------------------
# Constructed histories
# ---------------------------------------------------------------------------------------------------------------------

UNITS = Planck_units()


def constructed(leftover="after", parking_share=0.25, K_initial=0.0):
    """
    (rows, ledger, parking_share, generations): candidate rows in index order with a plateau event, their K_before from a
    ledger that adds the drains and the parking leftover as the integrator does. leftover: "none", "after" (after the
    last candidate) or "bracketed" (after the generation at N = 5.1, before the two later candidates).
    """
    spec = [
        # (N_turn, N, dK, ln q, ln k_ex, ln m, ln E, ln H, regime)
        (1.0, 1.05, 1.0e-3, log(2.0), log(2.0) - 1.0, -3.0, -20.0, -8.0, "one_loop"),
        (2.0, 2.1, 2.0e-3, log(5.0), log(5.0) - 2.0, -2.5, -22.0, -9.0, "one_loop"),
        (2.5, 2.6, 5.0e-4, None, None, -2.0, None, -9.5, "unassessed"),
        (3.1, 3.2, 1.0e-3, 0.0, -3.1, -2.0, -23.0, -10.0, "one_loop"),
        (4.0, 4.1, 0.0, None, -3.0, -1.5, -24.0, -11.0, "classical"),
        (5.0, 5.1, 4.0e-3, 6.0, 6.0 - 5.0, -1.0, -25.0, -12.0, "one_loop"),
        (6.0, 6.13, 3.0e-3, -5.0, -5.0 - 6.0, -0.5, -26.0, -13.0, "one_loop"),
    ]
    from ComputeTargets.dark_radiation import DarkRadiationLedger

    ledger = DarkRadiationLedger(K_initial)
    K_before = []
    cut = 5 if leftover == "bracketed" else len(spec) - 1
    left = 0.0
    for index, (Nt, N, dK, *_rest) in enumerate(spec):
        K_before.append(ledger.cumulative_K[-1] if ledger.cumulative_K else K_initial)
        if dK > 0.0:
            ledger.add(N, dK)
        if leftover != "none" and index == cut:
            # a leftover of the share asked at the end: x = ps (K + x) for the drains of the whole history
            total_drains = K_initial + sum(s[2] for s in spec)
            left = parking_share * total_drains / (1.0 - parking_share)
            ledger.add(N, left)
    K_final = ledger.K(1e9)
    ps = left / K_final if leftover != "none" else 0.0
    rows = []
    ordinal = 0
    for index, (Nt, N, dK, lq, lk, lm, lE, lH, regime) in enumerate(spec):
        if index == 1:
            rows.append(qe.event(ordinal, 1.5, -2.0))
            ordinal += 1
        rows.append(
            qe.candidate(
                ordinal,
                index,
                Nt,
                N,
                dK,
                K_before[index],
                log_q=lq,
                log_k_ex=lk,
                log_m=lm,
                log_E=lE,
                log_H=lH,
                regime=regime,
            )
        )
        ordinal += 1
    gens = [(s[0], s[3], s[2]) for s in spec if s[2] > 0.0 and s[3] is not None]
    return rows, ledger, ps, gens


def samples_for(ledger, units=UNITS, blank_mass_at=(7,), Ns=None):
    """Samples on N in [0.5, 8], r_chi from the ledger as the integrator forms it; ln m_eff blank at the given ks."""
    out = []
    Ns = np.linspace(0.5, 8.0, 31) if Ns is None else Ns
    for k, N in enumerate(Ns):
        N = float(N)
        log_rho = -30.0 - 4.0 * N + 4.0 * log(units.PlanckMass)
        r = ledger.ratio(N, log_rho) if not ledger.is_zero(N) else 0.0
        out.append(
            qe.sample_value(
                k + 1,
                N,
                log_rho,
                r,
                None if k in blank_mass_at else -9.0 + 1.6 * N + log(units.PlanckMass),
                -10.0 + 1.8 * N + log(units.PlanckMass),
                units,
            )
        )
    return out


class _ExportCase(unittest.TestCase):
    def setUp(self):
        self._dir = Path(tempfile.mkdtemp(prefix="quanta-measures-"))
        self.root = self._dir / "out" / qe.LABEL
        self.root.mkdir(parents=True)
        self.records = []

    def tearDown(self):
        shutil.rmtree(self._dir, ignore_errors=True)

    def add(
        self, rows, samples, ps, beta=2.0, M_Mp=1e-20, A=1.0, n=1, model=ONE_LOOP, **kw
    ):
        record = qe.history_record(
            beta, M_Mp, n, A, model, parking_share=ps, bounce_rows=len(rows)
        )
        record.update(kw.pop("record", {}))
        self.records.append(record)
        return qe.write_constructed(self.root, UNITS, record, rows, samples, **kw)

    def run_tool(self, c=1.0, units=None):
        ec.write_histories_csv(self.records, self.root / "histories.csv")
        out = io.StringIO()
        rc = qm.run(self.root, c, units=units, out=out)
        return rc, out.getvalue()

    def read(self, path):
        with open(path, newline="") as f:
            return list(csv.DictReader(f))


def f(cell):
    return None if cell == "" else float(cell)


class TestTheMeasuresAgainstQuadrature(_ExportCase):
    def check_scale(self, c):
        rows, ledger, ps, gens = constructed("after", 0.25)
        samples = samples_for(ledger)
        d = self.add(rows, samples, ps)
        rc, text = self.run_tool(c)
        self.assertEqual(rc, 0, text)
        sfx = qm.suffix(c)
        b = self.read(d / f"quanta_bounces{sfx}.csv")
        s = self.read(d / f"quanta_samples{sfx}.csv")
        h = self.read(self.root / f"quanta_histories{sfx}.csv")
        self.assertEqual(len(h), 1)
        h = h[0]
        K_final = ledger.K(1e9)

        # E at every sample, both masses, against quadrature
        for row, sv in zip(s, samples):
            N = sv.raw_N
            K = ledger.K(N)
            for col, lm in (("E_field", sv.log_m_eff), ("E_tracker", sv.log_m_tracker)):
                if K == 0.0 or lm is None:
                    self.assertEqual(row[col], "")
                    continue
                expected = direct_E(gens, N, lm, K, c)
                got = f(row[col])
                if expected == 0.0:
                    self.assertEqual(got, 0.0)
                else:
                    self.assertLess(rel(got, expected), 1e-6, (col, N))
                self.assertEqual(f(row[f"r_chi_{col}"]), sv.rho_chi_over_rho_rad * got)

        # n_g, n_bar, delta_stim, the backreaction ratio, the phase slip
        by_ordinal = {int(r["ordinal"]): r for r in b}
        gen_rows = [
            r
            for r in rows
            if r.source == "candidate" and r.delta_K > 0 and r.log_q is not None
        ]
        n_bars = []
        for j, g in enumerate(gen_rows):
            out = by_ordinal[g.ordinal]
            A = direct_A(g.delta_K, g.log_q, c)
            self.assertLess(
                abs(f(out["ln_n_g"]) - log(A * float(qs.shape(qs.X_PEAK, c)))), 1e-6
            )
            nb = direct_n_bar(gens, j, c)
            n_bars.append(nb)
            if j == 0:
                self.assertEqual(f(out["n_bar"]), 0.0)
            else:
                self.assertLess(rel(f(out["n_bar"]), nb), 1e-6, j)
        delta_stim = sum(2 * nb * g[2] for nb, g in zip(n_bars, gens)) / K_final
        self.assertLess(rel(f(h["delta_stim"]), delta_stim), 1e-6)
        self.assertLess(rel(f(h["n_bar_max"]), max(n_bars)), 1e-6)
        self.assertEqual(
            int(h["n_bar_max_ordinal"]), gen_rows[int(np.argmax(n_bars))].ordinal
        )
        for r in rows:
            out = by_ordinal[r.ordinal]
            if r.source != "candidate" or r.log_m is None or r.log_E is None:
                self.assertEqual(out["backreaction_ratio"], "")
                continue
            n_chi = direct_n_chi_a3(gens, r.N_turn, c)
            expected = n_chi * exp(-3 * r.N_turn) * exp(r.log_m) / exp(r.log_E)
            if expected == 0.0:
                self.assertEqual(f(out["backreaction_ratio"]), 0.0)
            else:
                self.assertLess(rel(f(out["backreaction_ratio"]), expected), 1e-6)
        assessed = [
            dict(N=r.N, log_k_ex=r.log_k_ex, H_before=exp(r.log_H), ordinal=r.ordinal)
            for r in rows
            if r.source == "candidate" and r.log_k_ex is not None
        ]
        slips, kH = probe_slip(assessed)
        for i, a in enumerate(assessed):
            out = by_ordinal[a["ordinal"]]
            if 0 < i < len(assessed) - 1:
                self.assertLess(rel(f(out["phase_slip"]), slips[i - 1]), 1e-12)
                self.assertLess(rel(f(out["k_ex_over_H"]), kH[i - 1]), 1e-12)
            else:
                self.assertEqual(out["phase_slip"], "")
        self.assertLess(rel(f(h["phase_slip_min"]), min(slips)), 1e-12)
        return h, s

    def test_at_scale_1(self):
        self.check_scale(1.0)

    def test_at_scale_0(self):
        self.check_scale(0.0)

    def test_at_scale_10(self):
        self.check_scale(10.0)


class TestKOfN(_ExportCase):
    def measured_K(self, leftover, ps=0.25, K_initial=0.0):
        rows, ledger, ps, gens = constructed(leftover, ps, K_initial)
        samples = samples_for(
            ledger,
            Ns=np.concatenate([np.linspace(0.5, 8.0, 31), [1.05, 2.1, 5.1, 6.13]]),
        )
        samples.sort(key=lambda s: s.raw_N)
        d = self.add(rows, samples, ps)
        rc, text = self.run_tool()
        self.assertEqual(rc, 0, text)
        s = self.read(d / "quanta_samples.csv")
        return s, ledger, ps, text

    def test_no_leftover(self):
        s, ledger, _, text = self.measured_K("none")
        self.assertIn("leftover none", text)
        for row in s:
            self.assertEqual(f(row["K_Mp4"]), ledger.K(f(row["raw_N"])))
            if ledger.K(f(row["raw_N"])) > 0:
                self.assertEqual(f(row["parking_fraction"]), 0.0)
        # K(N) = 0 before the first drain: blanks; and the counts of ruling 7
        first = [row for row in s if f(row["raw_N"]) < 1.05]
        self.assertTrue(first)
        for row in first:
            for col in (
                "parking_fraction",
                "E_field",
                "E_tracker",
                "r_chi_E_field",
                "r_chi_E_tracker",
            ):
                self.assertEqual(row[col], "")
        self.assertIn("K(N) = 0 with a generation born 1", text)

    def test_the_leftover_after_the_last_candidate(self):
        s, ledger, ps, text = self.measured_K("after")
        self.assertIn("leftover after the last candidate", text)
        K_final = ledger.K(1e9)
        for row in s:
            N = f(row["raw_N"])
            K = ledger.K(N)
            if N < 6.13:
                self.assertEqual(f(row["K_Mp4"]), K)
            else:
                self.assertLess(rel(f(row["K_Mp4"]), K), 8 * EPS * (1 + ps / (1 - ps)))
                self.assertLess(
                    rel(f(row["parking_fraction"]), ps * K_final / K), 1e-12
                )

    def test_the_leftover_before_a_later_candidate(self):
        s, ledger, ps, text = self.measured_K("bracketed", 0.1)
        self.assertIn("leftover bracketed", text)
        left = ps * ledger.K(1e9)
        for row in s:
            N = f(row["raw_N"])
            self.assertEqual(f(row["K_Mp4"]), ledger.K(N))
            pf = f(row["parking_fraction"])
            if N >= 5.1:
                self.assertLess(rel(pf, left / ledger.K(N)), 1e-12)
            elif pf is not None:
                self.assertEqual(pf, 0.0)

    def test_K_initial_nonzero(self):
        s, ledger, _, _ = self.measured_K("none", K_initial=5e-4)
        for row in s:
            self.assertEqual(f(row["K_Mp4"]), ledger.K(f(row["raw_N"])))
            self.assertNotEqual(row["E_tracker"], "")

    def test_the_refusals_of_ruling_6(self):
        rows, ledger, ps, _ = constructed("after", 0.25)
        samples = samples_for(ledger)
        cands = [i for i, r in enumerate(rows) if r.source == "candidate"]
        # candidate_index not increasing
        swapped = list(rows)
        a, b = cands[1], cands[2]
        swapped[a] = rows[a]._replace(candidate_index=rows[b].candidate_index)
        swapped[b] = rows[b]._replace(candidate_index=rows[a].candidate_index)
        self.add(swapped, samples, ps, beta=2.0)
        # K falls
        fall = list(rows)
        fall[cands[3]] = rows[cands[3]]._replace(K_before=rows[cands[3]].K_before * 0.5)
        self.add(fall, samples, ps, beta=2.1)
        # a jump that is not parking_share x K_final
        rows_b, ledger_b, ps_b, _ = constructed("bracketed", 0.1)
        self.add(rows_b, samples_for(ledger_b), 0.2, beta=2.2)
        # a jump with no parking share
        self.add(rows_b, samples_for(ledger_b), 0.0, beta=2.3)
        rc, text = self.run_tool()
        self.assertEqual(rc, 1)
        self.assertEqual(text.count("REFUSED"), 4, text)
        self.assertIn("candidate_index does not increase strictly", text)
        self.assertIn("K falls before candidate row", text)
        self.assertIn("is not parking_share x K_final", text)
        self.assertIn("with no parking share", text)
        self.assertEqual(len(self.read(self.root / "quanta_histories.csv")), 0)

    def test_a_failed_check_writes_nothing(self):
        rows, ledger, ps, _ = constructed("after", 0.25)
        samples = samples_for(ledger)
        samples[20].rho_chi_over_rho_rad *= 1.0 + 1e-6
        d = self.add(rows, samples, ps)
        rows2, ledger2, ps2, _ = constructed("none")
        d2 = self.add(rows2, samples_for(ledger2), ps2, beta=3.0)
        rc, text = self.run_tool()
        self.assertEqual(rc, 1)
        self.assertIn("K CHECK FAILED", text)
        self.assertFalse((d / "quanta_samples.csv").exists())
        self.assertFalse((d / "quanta_bounces.csv").exists())
        self.assertTrue((d2 / "quanta_samples.csv").exists())
        self.assertEqual(len(self.read(self.root / "quanta_histories.csv")), 1)
        self.assertIn("failed the K check 1", text)

    def test_K_zero_needs_r_chi_zero(self):
        rows, ledger, ps, _ = constructed("none")
        samples = samples_for(ledger)
        samples[0].rho_chi_over_rho_rad = 1e-30
        self.add(rows, samples, ps)
        rc, text = self.run_tool()
        self.assertEqual(rc, 1)
        self.assertIn("K(N) = 0 at N", text)

    def test_the_check_in_any_units(self):
        """ln(GeV/M_P) from three UnitsLike: the check's largest difference agrees beyond the tolerance."""
        rows, ledger, ps, _ = constructed("after", 0.25)
        samples = samples_for(ledger)
        bounces = [qm.Bounce(r) for r in self._rows_as_csv(rows)]
        parsed = [qm.Sample(r) for r in self._samples_as_csv(samples)]
        K = qm.KHistory([b for b in bounces if b.source == "candidate"], ps)
        worst = {}
        for units in (Planck_units(), GeV_units(), Mpc_units()):
            w, failures = qm.K_check(K, parsed, units)
            self.assertEqual(failures, [])
            worst[units.system_name] = w
        values = list(worst.values())
        self.assertLess(max(values) - min(values), qm.K_CHECK_TOLERANCE, worst)

    def _rows_as_csv(self, rows):
        d = self._dir / "rows"
        ec.write_bounces_csv(rows, UNITS, d / "bounces.csv")
        return self.read(d / "bounces.csv")

    def _samples_as_csv(self, samples):
        from types import SimpleNamespace

        d = self._dir / "samples"
        qe.write_csv_file()(
            qe.UNAVAILABLE,
            qe.UNAVAILABLE,
            d,
            SimpleNamespace(available=True, values=samples),
            UNITS,
        )
        return self.read(d / "fields.csv")


class TestFindingAndTheFiles(_ExportCase):
    def test_skips_refusals_and_missing(self):
        rows, ledger, ps, _ = constructed("after", 0.25)
        samples = samples_for(ledger)
        good = self.add(rows, samples, ps, beta=2.0)
        # two rows giving one directory (beta 2.5 and 2.500001 format alike to .5g)
        self.add(rows, samples, ps, beta=2.5)
        self.add(rows, samples, ps, beta=2.500001)
        # a missing bounces.csv
        self.add(rows, samples, ps, beta=3.5, write_bounces=False)
        # a fixed-law row (no bounces.csv) and a failed one-loop history
        self.records.append(qe.history_record(2.0, 1e-20, 1, 0.1, DRAIN_MODEL_FIXED))
        self.records.append(
            qe.history_record(2.0, 1e-20, 1, 2.0, ONE_LOOP, normalization=None)
        )
        # n not an integer
        self.add(
            rows,
            samples,
            ps,
            beta=4.0,
            record=dict(n=1.5),
            M_eV=1e-20 * UNITS.PlanckMass / UNITS.eV,
        )
        rc, text = self.run_tool()
        self.assertEqual(rc, 1)
        self.assertIn("measured 1,", text)
        self.assertIn("skipped as fixed-law 1", text)
        self.assertIn("failed histories 1", text)
        self.assertIn("missing files 1", text)
        self.assertIn("refused 3", text)
        self.assertIn("is also that of", text)
        self.assertIn("is not an integer", text)
        self.assertEqual(text.count("is also that of"), 2)
        self.assertTrue((good / "quanta_bounces.csv").exists())

    def test_every_column_and_the_model(self):
        rows, ledger, ps, _ = constructed("after", 0.25)
        samples = samples_for(ledger)
        d = self.add(rows, samples, ps)
        for c, sfx in ((1.0, ""), (0.0, "-assoc0"), (10.0, "-assoc10")):
            rc, text = self.run_tool(c)
            self.assertEqual(rc, 0, text)
            with open(d / f"quanta_bounces{sfx}.csv") as fh:
                header = fh.readline().strip().split(",")
            self.assertEqual(header, qm.BOUNCE_OUTPUT_COLUMNS)
            with open(d / f"quanta_samples{sfx}.csv") as fh:
                self.assertEqual(
                    fh.readline().strip().split(","), qm.SAMPLE_OUTPUT_COLUMNS
                )
            with open(self.root / f"quanta_histories{sfx}.csv") as fh:
                self.assertEqual(
                    fh.readline().strip().split(","), qm.HISTORY_OUTPUT_COLUMNS
                )
            for path in (
                d / f"quanta_bounces{sfx}.csv",
                d / f"quanta_samples{sfx}.csv",
                self.root / f"quanta_histories{sfx}.csv",
            ):
                for row in self.read(path):
                    self.assertEqual(row["spectrum_model"], qs.SPECTRUM_MODEL)
                    self.assertEqual(float(row["assoc_scale"]), c)
        b = {int(r["ordinal"]): r for r in self.read(d / "quanta_bounces.csv")}
        self.assertEqual(len(b), len(rows))
        for r in rows:
            out = b[r.ordinal]
            self.assertEqual(out["source"], r.source)
            self.assertEqual(f(out["N"]), r.N)
            if r.source != "candidate":
                for col in (
                    "N_turn",
                    "ln_n_g",
                    "n_bar",
                    "phase_slip",
                    "k_ex_over_H",
                    "backreaction_ratio",
                ):
                    self.assertEqual(out[col], "")
                continue
            self.assertEqual(f(out["N_turn"]), r.N_turn)
            is_gen = r.delta_K > 0 and r.log_q is not None
            self.assertEqual(out["ln_n_g"] != "", is_gen)
            self.assertEqual(out["n_bar"] != "", is_gen)
            self.assertEqual(out["k_ex_over_H"] != "", r.log_k_ex is not None)
        h = self.read(self.root / "quanta_histories.csv")[0]
        for col in qm.HISTORY_OUTPUT_COLUMNS:
            self.assertNotEqual(h[col], "", col)
        self.assertEqual(h["generations"], "5")
        self.assertEqual(h["phase_slips_below_1"], str(int(h["phase_slips_below_1"])))
        self.assertIn("drains without spectrum 1", self.run_tool()[1])

    def test_the_exit_statuses(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(qm.main([str(self._dir / "nowhere")]), 2)
            self.assertEqual(qm.main([str(self.root), "--assoc-scale", "-1"]), 2)
            self.assertEqual(qm.main([]), 2)
            self.assertEqual(qm.main([str(self.root)]), 2)  # no histories.csv
        rows, ledger, ps, _ = constructed("none")
        self.add(rows, samples_for(ledger), ps)
        ec.write_histories_csv(self.records, self.root / "histories.csv")
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(qm.main([str(self.root), "--assoc-scale", "10"]), 0)
        self.assertTrue((self.root / "quanta_histories-assoc10.csv").exists())


# ---------------------------------------------------------------------------------------------------------------------
# One stored history, and the Derive gate's §2.2
# ---------------------------------------------------------------------------------------------------------------------

from ObjectFactories.tests.test_drain_store import (
    P2197,
    _DrainStoreCase,
    one_loop_payload,
)  # noqa: E402


def probe_excess():
    """`excess` of source/probe/all_generations.py, extracted by ast, unedited."""
    tree = ast.parse(PROBE.read_text(), str(PROBE))
    nodes = [
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "excess"
    ]
    assert len(nodes) == 1
    return ast.Module(body=nodes, type_ignores=[])


def run_probe(payload, which: str):
    """The probe's E with the names it reads bound to the payload's values (ruling 5): (Ns, E)."""
    from ComputeTargets.ScalarHistory import SampleValues

    sample = [SampleValues._make(tuple(s)) for s in payload["sample"]]
    masses = payload[which]
    pairs = sorted(zip((s.raw_N for s in sample), masses), key=lambda p: p[0])
    ledger = payload["dark_radiation"]
    gens = payload["generations"]
    K_final = payload["bounces"].K_final  # ledger.K(N_final), the probe's K_final
    namespace = dict(
        np=np,
        isfinite=isfinite,
        Ns=np.array([p[0] for p in pairs]),
        gN=np.array([g.N_turn for g in gens]),
        gq=np.array([g.log_q for g in gens]),
        gdK=np.array([g.share for g in gens]) * K_final,
        ledger=ledger,
    )
    exec(compile(probe_excess(), str(PROBE), "exec"), namespace)
    lm = np.array([np.nan if p[1] is None else p[1] for p in pairs])
    with np.errstate(divide="ignore", invalid="ignore"):
        return namespace["Ns"], namespace["excess"](lm)


def compare_with_probe(tool_E, probe_E, Ns, tool_K, gN, dK, masses):
    """ruling 5's comparison: (largest a-part ratio, largest b-part ratio, matched counts, failures)."""
    worst_a = worst_b = 0.0
    matched = dict(mass_none=0, K_zero=0, compared=0)
    failures = []
    for E_t, E_p, N, K, m in zip(tool_E, probe_E, Ns, tool_K, masses):
        if K == 0.0:
            if E_t is None and (E_p == 0.0 or not np.isfinite(E_p)):
                matched["K_zero"] += 1
            else:
                failures.append((N, "K = 0", E_t, E_p))
            continue
        if m is None:
            if E_t is None and E_p == 0.0:
                matched["mass_none"] += 1
            else:
                failures.append((N, "mass None", E_t, E_p))
            continue
        born = gN <= N
        n_born = int(born.sum())
        a_part = PROBE_A * EPS * float(np.sum(dK[born])) / K
        b_part = PROBE_B * EPS * n_born * abs(E_p)
        diff = abs(E_t - E_p)
        matched["compared"] += 1
        if a_part > 0:
            worst_a = max(worst_a, diff / (a_part + b_part))
        if diff > a_part + b_part:
            failures.append((N, "beyond", E_t, E_p, diff, a_part, b_part))
        if E_p != 0.0:
            worst_b = max(worst_b, diff / abs(E_p) / (EPS * max(n_born, 1)))
    return worst_a, worst_b, matched, failures


class TestAStoredHistory(_DrainStoreCase):
    """P-2197 at A = 1 (one integration, about 2-4 s, shared through test_drain_store's cache), written on a temporary
    store, read back, exported by 04's writers and measured; and the Derive gate's §2.2 on it.
    """

    def setUp(self):
        super().setUp()
        self.out = Path(self._dir) / "export" / qe.LABEL
        self.payload = one_loop_payload(*P2197)
        _, serial = self.write_one_loop(P2197)
        full = self.read_one_loop(P2197)
        lean = self.read_one_loop(P2197, _do_not_populate=True)
        beta, M, A = P2197
        record, self.d = qe.export_history(
            self.out, self.units, lean, full, beta, M, 1, A, ONE_LOOP
        )
        ec.write_histories_csv([record], self.out / "histories.csv")

    def rows(self, path):
        with open(path, newline="") as fh:
            return list(csv.DictReader(fh))

    def test_K_and_the_generations_and_one_value_of_each(self):
        out = io.StringIO()
        self.assertEqual(qm.run(self.out, 1.0, out=out), 0, out.getvalue())
        self.assertIn("leftover after the last candidate", out.getvalue())
        payload = self.payload
        ledger = payload["dark_radiation"]
        ps = payload["parking"].delta_K / ledger.K(1e9)
        samples = self.rows(self.d / "quanta_samples.csv")
        bitwise = 0
        for row in samples:
            N = float(row["raw_N"])
            expected = to_column(ledger.K(N), LINEAR_MP4, self.units)
            got = float(row["K_Mp4"])
            if N < payload["parking"].N:
                self.assertEqual(got, expected, N)
                bitwise += 1
            else:
                self.assertLess(rel(got, expected), 8 * EPS * (1 + ps / (1 - ps)))
        self.assertGreater(bitwise, 0)

        # the generations: count, N_turn, ln q, Delta K
        gens = payload["generations"]
        bounces = self.rows(self.d / "quanta_bounces.csv")
        gen_rows = [r for r in bounces if r["ln_n_g"] != ""]
        self.assertEqual(len(gen_rows), len(gens))
        raw = {int(r["ordinal"]): r for r in self.rows(self.d / "bounces.csv")}
        for g, r in zip(gens, gen_rows):
            b = raw[int(r["ordinal"])]
            self.assertEqual(float(b["N_turn"]), g.N_turn)
            self.assertEqual(float(b["log_q_Mp"]), g.log_q)
            self.assertEqual(
                float(b["delta_K_Mp4"]), payload["injections"][g.index].delta_K
            )

        # one value of each measure against the direct computation
        c = 1.0
        G = [(g.N_turn, g.log_q, payload["injections"][g.index].delta_K) for g in gens]
        h = self.rows(self.out / "quanta_histories.csv")[0]
        last = samples[-1]
        lm = float(self.rows(self.d / "fields.csv")[-1]["log_m_eff_Mp"])
        N = float(last["raw_N"])
        self.assertLess(
            rel(float(last["E_field"]), direct_E(G, N, lm, ledger.K(N), c)), 1e-6
        )
        j = len(gens) - 1
        self.assertLess(rel(float(gen_rows[j]["n_bar"]), direct_n_bar(G, j, c)), 1e-6)
        A = direct_A(G[0][2], G[0][1], c)
        self.assertLess(
            abs(float(gen_rows[0]["ln_n_g"]) - log(A * float(qs.shape(qs.X_PEAK, c)))),
            1e-6,
        )
        K_final = ledger.K(1e9)
        ds = sum(2 * direct_n_bar(G, k, c) * G[k][2] for k in range(len(G))) / K_final
        self.assertLess(rel(float(h["delta_stim"]), ds), 1e-6)
        b = raw[int(gen_rows[j]["ordinal"])]
        n_chi = direct_n_chi_a3(G, float(b["N_turn"]), c)
        expected = n_chi * exp(
            -3 * float(b["N_turn"]) + float(b["log_m_Mp"]) - float(b["log_E_Mp4"])
        )
        self.assertLess(rel(float(gen_rows[j]["backreaction_ratio"]), expected), 1e-6)
        assessed = [
            dict(
                N=float(r["N"]),
                log_k_ex=float(r["log_k_ex_Mp"]),
                H_before=exp(float(r["log_H_Mp"])),
                ordinal=int(r["ordinal"]),
            )
            for r in raw.values()
            if r["source"] == "candidate" and r["log_k_ex_Mp"] != ""
        ]
        slips, _ = probe_slip(assessed)
        self.assertLess(rel(float(h["phase_slip_min"]), min(slips)), 1e-12)

    def test_the_probe_with_the_peak_collapsed(self):
        """The Derive gate's §2.2: the tool's E with s = delta(x - 1), read from the exports, against the probe's
        `excess` run unedited on the payload, at every sample, for both masses."""
        payload = self.payload
        hrow = self.rows(self.out / "histories.csv")[0]
        bounces = [qm.Bounce(r) for r in self.rows(self.d / "bounces.csv")]
        samples = [qm.Sample(r) for r in self.rows(self.d / "fields.csv")]
        cands = [b for b in bounces if b.source == "candidate"]
        K = qm.KHistory(cands, float(hrow["parking_share"]))
        gens = qm.Generations(cands)
        Ns = [s.raw_N for s in samples]
        Ks = [K.K(N) for N in Ns]
        for which, attr in (
            ("log_m_eff", "log_m_eff"),
            ("log_m_tracker", "log_m_tracker"),
        ):
            with self.subTest(mass=which):
                tool = qm.matter_excess(
                    qs.PEAK_SPECTRUM, gens, Ns, [getattr(s, attr) for s in samples], Ks
                )
                pN, probe = run_probe(payload, which)
                np.testing.assert_array_equal(pN, np.array(Ns))
                worst_a, worst_b, matched, failures = compare_with_probe(
                    tool,
                    probe,
                    Ns,
                    Ks,
                    gens.N_turn,
                    gens.delta_K,
                    [getattr(s, attr) for s in samples],
                )
                self.assertEqual(failures, [])
                self.assertEqual(sum(matched.values()), len(Ns))
                self.assertGreater(matched["compared"], 0)


if __name__ == "__main__":
    unittest.main()
