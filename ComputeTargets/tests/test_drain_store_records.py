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
What one-loop-drain prompt 03 computes for the store, before the store (prompt 03 §1.8, ComputeTargets; orchestrator
note 03, rulings 1-8):

- **the key**: the drain model's leaf module, its model -> law mapping and its refusals; alpha_value's pair; the
  names ComputeTargets.ScalarHistory imports from it are the same objects (ruling 1);
- **the bounce rows** (bounce_rows) on constructed records: candidates of every regime, delta_K = 0, an unassessed
  candidate, events of both sources, ties in N; and with the margins failed (ruling 5);
- **K before each candidate** (ledger_K_before; ruling 6) on a constructed ledger with a parking leftover and K_0;
- **the history record** (drain_history_values) and its shares' sum, on constructed payloads, K_final = 0 included
  (ruling 8);
- **the all-generation margins** (all_generation_margins) against the core with MARGIN_WEIGHT_SHARE patched to 0;
- **the sample masses' NULLs** (sample_masses) with the model calls patched to raise;
- **compute_scalar_history** passes drain_law through, the default the fixed law; ScalarHistory.compute passes the
  key's law;
- **2197 under the one-loop law at A = 1**, one integration (test_drain_law.run_2197, about 2-4 s at a load of 4-8):
  ln R of every row equal to the PeakEstimate's bitwise, the regime and the estimate in correspondence, the K
  chain against the ledger, the shares against tools/history_and_bbn.drain_values bitwise; and tune_history's masses
  against integrate_history's for the same integration (one tuning integration, about 2-4 s; ruling 2).

No Ray, no store.
"""

import contextlib
import importlib
import io
import unittest
from math import inf
from unittest import mock

import numpy as np

import CosmologyConcepts.drain_model as DM
from ComputeTargets.dark_radiation import DarkRadiationLedger
from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import alpha_value

SM = importlib.import_module("ComputeTargets.ScalarHistory")


def injection(N, kind="turning_point", alpha_eff=0.1, delta_K=1.0, N_turn=None, **kw):
    """An Injection with the fields the builder reads; the rest stand-ins."""
    fields = dict(
        N=N,
        kind=kind,
        N_turn=N if N_turn is None else N_turn,
        phi_turn=1.0e-20,
        pi_before=-1.0,
        pi_after=0.9,
        log_rho_kin=-200.0 - N,
        alpha_eff=alpha_eff,
        delta_K=delta_K,
        delta_rho_over_rho_rad=0.01,
        H_Einstein_before=1.0e-40,
        H_Einstein_after=1.0e-40,
        nonadiabaticity=3.0 + N,
        w_gate=0.9 if kind == "turning_point" else 1.0,
        resolved=kind == "turning_point",
        phi_assessed=None,
        log_E_assessed=None,
        classical_turning_point=None,
    )
    fields.update(kw)
    return SM.Injection(**fields)


def drain_candidate(regime, ell=None, log_R=None, alpha_eff=0.1, L=None):
    return SM.DrainCandidate(
        regime=regime,
        ell=ell,
        alpha_eff=alpha_eff,
        w_gate=1.0,
        log_E=None if regime == "unassessed" else -150.0,
        log_H=-90.0,
        L=L if L is not None else (None if regime == "unassessed" else 30.0),
        log_R=log_R,
    )


# ---- the key (ruling 1) ----


class TestTheKey(unittest.TestCase):
    def test_the_names_are_the_leaf_modules(self):
        for name in (
            "DRAIN_LAW_FIXED",
            "DRAIN_LAW_ONE_LOOP",
            "DRAIN_LAWS",
            "DRAIN_MODEL_FIXED",
            "DRAIN_MODEL_ONE_LOOP",
            "DRAIN_MODELS",
            "check_drain_law",
            "law_of",
        ):
            with self.subTest(name=name):
                self.assertIs(getattr(SM, name), getattr(DM, name))
        self.assertEqual(DM.DRAIN_MODELS, ("fixed", "one-loop-measured-v1"))

    def test_the_mapping(self):
        self.assertEqual(DM.law_of("fixed"), "fixed")
        self.assertEqual(DM.law_of("one-loop-measured-v1"), "one_loop")
        for bad in ("one_loop", "one-loop-measured-v0", "", None, 1):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError) as caught:
                    DM.law_of(bad, "ctx")
                self.assertTrue(str(caught.exception).startswith("ctx: drain_model"))

    def test_the_leaf_module_imports_nothing_of_the_packages(self):
        import ast
        from pathlib import Path

        tree = ast.parse(Path(DM.__file__).read_text())
        modules = [
            n.module if isinstance(n, ast.ImportFrom) else a.name
            for n in ast.walk(tree)
            if isinstance(n, (ast.Import, ast.ImportFrom))
            for a in (n.names if isinstance(n, ast.Import) else [None])
        ]
        self.assertEqual(modules, ["math"])

    def test_alpha_value_checks_the_pair(self):
        a = alpha_value(1, 0.1)
        self.assertEqual((a.drain_model, a.drain_law), ("fixed", "fixed"))
        b = alpha_value(2, 5.0, "one-loop-measured-v1")
        self.assertEqual(
            (b.drain_model, b.drain_law), ("one-loop-measured-v1", "one_loop")
        )
        # alpha = 0 is natural mode under the fixed model only; A > 0 has no upper bound
        alpha_value(3, 0.0)
        alpha_value(4, 1.0e6, "one-loop-measured-v1")
        for args in (
            (5, 0.0, "one-loop-measured-v1"),
            (5, -0.0, "one-loop-measured-v1"),
            (5, inf, "one-loop-measured-v1"),
            (5, 1.0),
            (5, -0.1),
            (5, 0.1, "bogus"),
        ):
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    alpha_value(*args)

    def test_check_drain_laws_messages_are_unchanged(self):
        with self.assertRaises(ValueError) as caught:
            SM.check_drain_law("fixed", 1.0, "integrate_history")
        self.assertEqual(
            str(caught.exception),
            "integrate_history: alpha must satisfy 0 <= alpha < 1, not 1.0",
        )


# ---- K before each candidate (ruling 6) ----


class TestKBefore(unittest.TestCase):
    """A ledger with K_0, three draining candidates, one that does not, and a parking leftover at the cut
    candidate's N; K_before read by position, and the ledger's own K after each entry K_before + delta_K.
    """

    def setUp(self):
        self.ledger = DarkRadiationLedger(0.125)
        self.injections = [
            injection(1.0, delta_K=0.5),
            injection(2.0, alpha_eff=0.0, delta_K=0.0),
            injection(3.0, kind="floor", delta_K=0.25),
            injection(3.0, kind="unresolved", delta_K=1.0e-300),
            injection(4.0, delta_K=0.0625),
        ]
        for i in self.injections:
            if i.alpha_eff > 0.0:
                self.ledger.add(i.N, i.delta_K)
                if i.N == 3.0 and i.kind == "unresolved":
                    self.ledger.add(3.0, 0.03125)  # the leftover after candidate 3
        self.parking = mock.Mock(candidate_index=3, N=3.0, delta_K=0.03125)

    def test_by_position(self):
        K = SM.ledger_K_before(self.injections, self.parking, self.ledger)
        cumulative = self.ledger.cumulative_K
        self.assertEqual(
            K,
            (0.125, cumulative[0], cumulative[0], cumulative[1], cumulative[3]),
        )
        # at every draining candidate, the ledger's K after its entry is K_before + delta_K bitwise, and
        # K_before is the previous entry's cumulative K (K_0 at the first)
        entry = 0
        previous = self.ledger.K_initial
        for k, i in enumerate(self.injections):
            if i.alpha_eff > 0.0:
                self.assertEqual(K[k], previous)
                self.assertEqual(cumulative[entry], K[k] + i.delta_K)
                previous = cumulative[entry]
                entry += 1
                if k == 3:
                    previous = cumulative[entry]
                    entry += 1
        # K(N) at a candidate's N would include its own entry: not what is read
        self.assertNotEqual(K[0], self.ledger.K(1.0))

    def test_a_ledger_that_does_not_match_is_refused(self):
        with self.assertRaises(RuntimeError):
            SM.ledger_K_before(self.injections, None, self.ledger)
        with self.assertRaises(RuntimeError):
            SM.ledger_K_before(self.injections[:2], self.parking, self.ledger)


# ---- the bounce rows on constructed records (rulings 4, 5, 7) ----


def constructed():
    """
    Five candidates (one-loop, gap with delta_K = 0, classical, unassessed, classical below the weight) and four
    events: a plateau tied in N with candidate 1 (the candidate first), two events tied with each other, and a
    wall-side event; candidates 2 and 3 tied in N_turn (index order kept). Through the core, so that the
    Generations are the core's.
    """
    injections = [
        injection(1.0),
        injection(2.0, delta_K=0.0, alpha_eff=0.0),
        injection(3.0, kind="floor", delta_K=0.5, alpha_eff=2.3e-108),
        injection(3.0, kind="unresolved", delta_K=0.25),
        injection(5.0, delta_K=1.0e-6),
    ]
    candidates = [
        drain_candidate("one_loop", ell=8.0, log_R=40.0),
        drain_candidate("gap", log_R=1.0, alpha_eff=0.0),
        drain_candidate("classical", log_R=-3.0, L=1.0e-11, alpha_eff=2.3e-108),
        drain_candidate("unassessed"),
        drain_candidate("classical", log_R=-1.0),
    ]
    estimates = [
        SM.PeakEstimate(SM.ESTIMATE_ONE_LOOP, -60.0, -70.0, 40.0, None),
        SM.PeakEstimate(SM.ESTIMATE_CLASSICAL_GAP, -62.0, -64.0, 1.0, "raised"),
        SM.PeakEstimate(SM.ESTIMATE_CLASSICAL, -63.0, -65.0, -3.0, None),
        None,
        SM.PeakEstimate(SM.ESTIMATE_CLASSICAL, -64.0, -66.0, -1.0, None),
    ]
    margin_candidates = [
        SM.MarginCandidate(
            index=k,
            kind=i.kind,
            N_turn=i.N_turn,
            delta_K=i.delta_K,
            estimate=None if e is None else e.estimate,
            log_k_ex=None if e is None else e.log_k_ex,
            log_m=-67.5 if e is None else e.log_m,
            log_R=None if e is None else e.log_R,
            gap_reason=None if e is None else e.gap_reason,
        )
        for k, (i, e) in enumerate(zip(injections, estimates))
    ]
    events = [
        SM.MarginEvent(2.0, -80.0, "plateau"),
        SM.MarginEvent(4.0, -81.0, "plateau"),
        SM.MarginEvent(4.0, -82.0, "wall"),
        SM.MarginEvent(4.5, -83.0, "wall"),
    ]
    generations, margins = SM.relativistic_margins(
        margin_candidates, events, 4.5, [(6.0, -90.0)], (7.0, -91.0), 2.0, 0.25
    )
    return (
        dict(
            injections=injections,
            drain_candidates=candidates,
            estimates=estimates,
            candidate_log_T=[-10.0 - k for k in range(5)],
            K_before=[0.0, 1.0, 1.0, 1.5, 1.75],
            margin_candidates=margin_candidates,
            events=events,
            event_log_T=[-20.0 - k for k in range(4)],
            generations=generations,
        ),
        margins,
    )


class TestTheBounceRows(unittest.TestCase):
    def setUp(self):
        self.inputs, self.margins = constructed()
        self.rows = SM.bounce_rows(**self.inputs)

    def test_the_order_and_the_ordinals(self):
        self.assertEqual(
            [(r.source, r.candidate_index, r.N) for r in self.rows],
            [
                ("candidate", 0, 1.0),
                ("candidate", 1, 2.0),
                ("plateau", None, 2.0),
                ("candidate", 2, 3.0),
                ("candidate", 3, 3.0),
                ("plateau", None, 4.0),
                ("wall", None, 4.0),
                ("wall", None, 4.5),
                ("candidate", 4, 5.0),
            ],
        )
        self.assertEqual([r.ordinal for r in self.rows], list(range(9)))
        # the order is relativistic_margins' own: its stable sort of (N_turn, log_m) of the candidates then
        # the events
        core = sorted(
            [(c.N_turn, c.log_m) for c in self.inputs["margin_candidates"]]
            + [(e.N, e.log_m) for e in self.inputs["events"]],
            key=lambda e: e[0],
        )
        self.assertEqual(
            [
                (r.N if r.source != "candidate" else r.N_turn, r.log_m)
                for r in self.rows
            ],
            core,
        )

    def test_a_candidate_row_carries_its_records(self):
        by_index = {r.candidate_index: r for r in self.rows if r.source == "candidate"}
        for k, row in by_index.items():
            i = self.inputs["injections"][k]
            c = self.inputs["drain_candidates"][k]
            with self.subTest(candidate=k):
                for field in (
                    "N",
                    "N_turn",
                    "kind",
                    "resolved",
                    "delta_K",
                    "log_rho_kin",
                    "nonadiabaticity",
                    "alpha_eff",
                    "w_gate",
                ):
                    self.assertEqual(getattr(row, field), getattr(i, field), field)
                for field in ("regime", "ell", "L", "log_R", "log_E", "log_H"):
                    self.assertEqual(getattr(row, field), getattr(c, field), field)
                self.assertEqual(row.log_T_Jordan, self.inputs["candidate_log_T"][k])
                self.assertEqual(row.K_before, self.inputs["K_before"][k])
        # the extremes are carried as they are
        self.assertEqual(by_index[2].alpha_eff, 2.3e-108)
        self.assertEqual(by_index[2].L, 1.0e-11)

    def test_the_generation_columns_only_where_a_generation(self):
        generations = {g.index: g for g in self.inputs["generations"]}
        self.assertEqual(sorted(generations), [0, 2, 4])
        for row in self.rows:
            g = (
                generations.get(row.candidate_index)
                if row.source == "candidate"
                else None
            )
            with self.subTest(ordinal=row.ordinal):
                for field in (
                    "log_q",
                    "share",
                    "weighted",
                    "birth_margin",
                    "reflection_margin",
                    "reflection_margin_N",
                    "tracker_margin",
                    "tracker_margin_N",
                ):
                    self.assertEqual(
                        getattr(row, field), None if g is None else getattr(g, field)
                    )
        # candidate 4 is below the weight, and its row says so
        self.assertFalse([r for r in self.rows if r.candidate_index == 4][0].weighted)

    def test_the_peak_and_the_events_mass(self):
        rows = {(r.source, r.candidate_index, r.N): r for r in self.rows}
        self.assertEqual(
            (rows["candidate", 0, 1.0].log_k_ex, rows["candidate", 0, 1.0].log_m),
            (-60.0, -70.0),
        )
        # unassessed: the field's own mass, the gatherer's
        self.assertEqual(
            (rows["candidate", 3, 3.0].log_k_ex, rows["candidate", 3, 3.0].log_m),
            (None, -67.5),
        )
        event = rows["wall", None, 4.5]
        self.assertEqual((event.log_m, event.log_T_Jordan), (-83.0, -23.0))
        for field in ("N_turn", "kind", "resolved", "delta_K", "regime", "K_before"):
            self.assertIsNone(getattr(event, field))

    def test_with_the_margins_failed(self):
        """Ruling 5: candidate rows in full but the Generation columns and an unassessed candidate's ln m;
        no event row."""
        inputs = dict(
            self.inputs,
            margin_candidates=None,
            events=None,
            event_log_T=None,
            generations=None,
        )
        rows = SM.bounce_rows(**inputs)
        self.assertEqual([r.candidate_index for r in rows], [0, 1, 2, 3, 4])
        self.assertTrue(all(r.source == "candidate" for r in rows))
        for r in rows:
            self.assertIsNone(r.share)
            self.assertIsNone(r.birth_margin)
            self.assertIsNotNone(r.K_before)
        self.assertIsNone(rows[3].log_m)
        self.assertEqual(rows[0].log_m, -70.0)


# ---- the all-generation margins against the core with the weight patched to 0 ----


class TestTheAllGenerationMargins(unittest.TestCase):
    """Generations above and below the weight, a tie in the worst margin, a None reflection margin: each
    value equals the core's with MARGIN_WEIGHT_SHARE 0, bitwise."""

    def test_against_the_patched_core(self):
        candidates = [
            SM.MarginCandidate(
                0, "turning_point", 1.0, 0.5, SM.ESTIMATE_ONE_LOOP, 2.0, 1.0, 9.0, None
            ),
            # below the weight, with the worst birth margin and failing
            SM.MarginCandidate(
                1, "floor", 2.0, 1.0e-6, SM.ESTIMATE_CLASSICAL, 0.5, 0.25, -3.0, None
            ),
            # the same birth margin as #1 (a tie: the lowest index is kept)
            SM.MarginCandidate(
                2, "floor", 3.0, 1.0e-7, SM.ESTIMATE_CLASSICAL, 1.5, 1.25, -2.0, None
            ),
            # the last: no event after it, so its reflection margin is None
            SM.MarginCandidate(
                3, "floor", 9.0, 0.25, SM.ESTIMATE_CLASSICAL, 7.0, 1.0, -1.0, None
            ),
        ]
        events = [SM.MarginEvent(2.5, 4.0, "plateau")]
        args = (candidates, events, 9.0, [(10.0, 3.0)], (11.0, 2.0), 1.0, 0.0)
        generations, _ = SM.relativistic_margins(*args)
        with mock.patch.object(SM, "MARGIN_WEIGHT_SHARE", 0.0):
            _, patched = SM.relativistic_margins(*args)
        self.assertIsNone(generations[3].reflection_margin)
        self.assertFalse(generations[1].weighted)
        got = SM.all_generation_margins(generations)
        for kind in SM.MARGIN_KINDS:
            with self.subTest(kind=kind):
                for ours, core in (
                    (f"all_worst_{kind}_margin", f"worst_{kind}_margin"),
                    (f"all_worst_{kind}_margin_N", f"worst_{kind}_margin_N"),
                    (f"all_{kind}_failing_share", f"{kind}_failing_share"),
                ):
                    self.assertEqual(repr(got[ours]), repr(getattr(patched, core)))
        # the tie: generation 1 (N_turn 2.0), not 2
        self.assertEqual(got["all_worst_birth_margin_N"], 2.0)


# ---- the history record ----


class TestTheHistoryRecord(unittest.TestCase):
    def payload(self, K_0=0.0, parked=True):
        injections = [
            injection(1.0, delta_K=0.5),
            injection(2.0, alpha_eff=0.0, delta_K=0.0),
            injection(3.0, kind="floor", delta_K=0.25),
            injection(4.0, kind="unresolved", delta_K=0.0625),
            injection(5.0, delta_K=0.03125),
        ]
        candidates = (
            drain_candidate("one_loop", ell=9.0, log_R=50.0),
            drain_candidate("gap", log_R=1.0, alpha_eff=0.0),
            drain_candidate("classical", log_R=-3.0),
            drain_candidate("unassessed"),
            drain_candidate("one_loop", ell=7.5, log_R=30.0),
        )
        ledger = DarkRadiationLedger(K_0)
        for i in injections:
            if i.alpha_eff > 0.0:
                ledger.add(i.N, i.delta_K)
        leftover = 0.0078125
        parking = mock.Mock(delta_K=leftover) if parked else None
        if parked:
            ledger.add(5.0, leftover)
        K_final = ledger.K(5.0)
        generations, margins = SM.relativistic_margins(
            [
                SM.MarginCandidate(
                    0,
                    "turning_point",
                    1.0,
                    0.5,
                    SM.ESTIMATE_ONE_LOOP,
                    2.0,
                    1.0,
                    50.0,
                    None,
                ),
                SM.MarginCandidate(
                    4,
                    "turning_point",
                    5.0,
                    0.03125,
                    SM.ESTIMATE_ONE_LOOP,
                    3.0,
                    0.5,
                    30.0,
                    None,
                ),
            ],
            [],
            5.0,
            [],
            (6.0, 1.0),
            K_final,
            0.0,
        )
        return dict(
            bounces=SM.BounceRecord(rows=(None,) * 7, K_final=K_final),
            drain=SM.DrainRecord(
                "one_loop", 1.0, "one-loop-measured-v1", 3, candidates, 2
            ),
            injections=injections,
            parking=parking,
            generations=generations,
            margins=margins,
            dark_radiation=ledger,
        )

    def test_the_values(self):
        data = self.payload()
        v = SM.drain_history_values(data)
        K_final = data["bounces"].K_final
        self.assertEqual(v.bounce_rows, 7)
        self.assertEqual(
            (
                v.candidates_one_loop,
                v.candidates_gap,
                v.candidates_classical,
                v.candidates_unassessed,
            ),
            (2, 1, 1, 1),
        )
        self.assertEqual(v.drained_share_one_loop, (0.5 + 0.03125) / K_final)
        self.assertEqual(v.drained_share_gap, 0.0)
        self.assertEqual(v.drained_share_classical, 0.25 / K_final)
        self.assertEqual(v.drained_share_unassessed, 0.0625 / K_final)
        self.assertEqual(v.parking_share, 0.0078125 / K_final)
        self.assertEqual(v.ell_min, 7.5)
        # the least ln R at a drain: the gap candidate drained nothing, the unassessed one has no ln R
        self.assertEqual(v.log_R_min_drain, -3.0)
        self.assertEqual((v.unresolvable_assessments, v.unresolved_declined), (3, 2))
        self.assertIsNotNone(v.all_worst_birth_margin)

    def test_the_shares_sum_to_one(self):
        for K_0, parked in ((0.0, True), (0.375, True), (0.0, False), (0.375, False)):
            with self.subTest(K_0=K_0, parked=parked):
                data = self.payload(K_0, parked)
                v = SM.drain_history_values(data)
                total = (
                    v.drained_share_one_loop
                    + v.drained_share_gap
                    + v.drained_share_classical
                    + v.drained_share_unassessed
                    + v.parking_share
                    + K_0 / data["bounces"].K_final
                )
                self.assertAlmostEqual(total, 1.0, delta=8 * 2.0**-52)
                if not parked:
                    self.assertEqual(v.parking_share, 0.0)

    def test_K_final_zero(self):
        data = self.payload()
        data["bounces"] = SM.BounceRecord(rows=(), K_final=0.0)
        v = SM.drain_history_values(data)
        for regime in SM.DRAIN_REGIMES:
            self.assertIsNone(getattr(v, f"drained_share_{regime}"))
        self.assertIsNone(v.parking_share)
        self.assertEqual(v.candidates_one_loop, 2)

    def test_none_without_bounces_and_no_margins(self):
        self.assertIsNone(SM.drain_history_values({"bounces": None}))
        self.assertIsNone(SM.drain_history_values({}))
        data = self.payload()
        data["margins"] = None
        v = SM.drain_history_values(data)
        for kind in SM.MARGIN_KINDS:
            self.assertIsNone(getattr(v, f"all_worst_{kind}_margin"))
            self.assertIsNone(getattr(v, f"all_{kind}_failing_share"))


# ---- the sample masses ----


class TestTheSampleMasses(unittest.TestCase):
    def test_the_nulls(self):
        sample = [mock.Mock(phi_Einstein=float(k)) for k in range(5)]

        def field(model, cosmology, state):
            if state[0] in (1.0, 3.0):
                raise ComputationFailureError("m_eff^2 <= 0 (stub)")
            return np.float64(-state[0])

        def tracker(model, phi, *args):
            if phi == 3.0 or phi == 4.0:
                raise ComputationFailureError("no tracker minimum (stub)")
            return np.float64(10.0 - phi)

        with mock.patch.object(SM, "field_log_m", field), mock.patch.object(
            SM, "tracker_log_m", tracker
        ):
            eff, tr = SM.sample_masses(None, None, sample)
        self.assertEqual(eff, (-0.0, None, -2.0, None, -4.0))
        self.assertEqual(tr, (10.0, 9.0, 8.0, None, None))
        # Python floats, whatever the model returns (here numpy's)
        self.assertTrue(all(type(x) is float for x in eff + tr if x is not None))

    def test_another_exception_propagates(self):
        def field(*args):
            raise ZeroDivisionError("not a computation failure")

        with mock.patch.object(SM, "field_log_m", field):
            with self.assertRaises(ZeroDivisionError):
                SM.sample_masses(None, None, [mock.Mock(phi_Einstein=1.0)])


# ---- the law through the stored path ----


class TestTheLawThroughTheStoredPath(unittest.TestCase):
    def call(self, **kwargs):
        with mock.patch.object(SM, "tune_history", return_value="stand-in") as tune:
            out = SM.compute_scalar_history._function(
                "cosmology", "model", "T", "phi", "pi", "z", **kwargs
            )
        self.assertEqual(out, "stand-in")
        return tune.call_args.kwargs

    def test_the_default_is_the_fixed_law(self):
        self.assertEqual(self.call(alpha=0.1)["drain_law"], SM.DRAIN_LAW_FIXED)
        self.assertEqual(self.call()["alpha"], 0.0)

    def test_the_law_is_passed_through(self):
        kwargs = self.call(alpha=2.0, drain_law=SM.DRAIN_LAW_ONE_LOOP)
        self.assertEqual((kwargs["alpha"], kwargs["drain_law"]), (2.0, "one_loop"))

    def test_compute_passes_the_keys_law(self):
        for alpha, law in (
            (alpha_value(7, 0.1), "fixed"),
            (alpha_value(8, 0.5, "one-loop-measured-v1"), "one_loop"),
        ):
            with self.subTest(law=law):
                history = SM.ScalarHistory(
                    payload=None,
                    solver_labels={},
                    cosmology=mock.Mock(),
                    T_Jordan_init=mock.Mock(),
                    phi_Einstein_init=mock.Mock(),
                    pi_Einstein_init=mock.Mock(),
                    model=mock.Mock(),
                    tuning_tol=mock.Mock(tol=1e-5),
                    alpha=alpha,
                    atol=mock.Mock(tol=1e-8),
                    rtol=mock.Mock(tol=1e-8),
                    z_grid=mock.Mock(),
                    label="stand-in",
                )
                with mock.patch.object(SM, "compute_scalar_history") as remote:
                    history.compute()
                kwargs = remote.remote.call_args.kwargs
                self.assertEqual(
                    (kwargs["alpha"], kwargs["drain_law"]), (alpha.value, law)
                )


# ---- 2197 at A = 1 under the one-loop law ----


class TestOn2197(unittest.TestCase):
    """2197 at its stored normalization, untuned, A = 1 (test_drain_law.run_2197): about 2-4 s."""

    @classmethod
    def setUpClass(cls):
        from ComputeTargets.tests.test_drain_law import run_2197

        cls.data, cls.result, cls.policy, cls.model = run_2197(1.0)
        cls.rows = cls.data["bounces"].rows

    def test_log_R_and_the_estimate(self):
        candidates = self.data["drain"].candidates
        estimates = self.result.peak_estimates
        expected = {
            "one_loop": SM.ESTIMATE_ONE_LOOP,
            "gap": SM.ESTIMATE_CLASSICAL_GAP,
            "classical": SM.ESTIMATE_CLASSICAL,
        }
        for c, e in zip(candidates, estimates):
            if c.regime == "unassessed":
                self.assertIsNone(e)
                continue
            self.assertEqual(c.log_R.hex(), e.log_R.hex())
            self.assertEqual(expected[c.regime], e.estimate)
        for row in self.rows:
            if row.source == "candidate":
                self.assertEqual(row.log_R, candidates[row.candidate_index].log_R)

    def test_the_rows(self):
        n_candidates = len(self.result.injections)
        events = [r for r in self.rows if r.source != "candidate"]
        self.assertEqual(len(self.rows) - len(events), n_candidates)
        self.assertGreater(len(events), 0)
        self.assertEqual([r.ordinal for r in self.rows], list(range(len(self.rows))))
        keys = [r.N_turn if r.source == "candidate" else r.N for r in self.rows]
        self.assertEqual(keys, sorted(keys))

    def test_the_K_chain(self):
        ledger = self.data["dark_radiation"]
        cumulative = ledger.cumulative_K
        entry = 0
        previous = ledger.K_initial
        parking = self.data["parking"]
        for row in (r for r in self.rows if r.source == "candidate"):
            k = row.candidate_index
            self.assertEqual(row.K_before, previous)
            if self.result.injections[k].alpha_eff > 0.0:
                self.assertEqual(cumulative[entry], row.K_before + row.delta_K)
                previous = cumulative[entry]
                entry += 1
            if parking is not None and parking.candidate_index == k:
                previous = cumulative[entry]
                entry += 1
        self.assertEqual(entry, len(cumulative))

    def test_the_shares_are_the_tools(self):
        import tools.history_and_bbn as hb

        tool = hb.drain_values(self.data, self.result)
        v = SM.drain_history_values(self.data)
        for regime in SM.DRAIN_REGIMES:
            self.assertEqual(
                repr(getattr(v, f"drained_share_{regime}")),
                repr(tool[f"K_share_{regime}"]),
            )
            self.assertEqual(getattr(v, f"candidates_{regime}"), tool[regime])
        self.assertEqual(repr(v.parking_share), repr(tool["K_share_parking"]))
        self.assertEqual(v.ell_min, tool["ell_min"])
        self.assertEqual(
            self.data["bounces"].K_final,
            self.data["dark_radiation"].K(self.result.N_final),
        )

    def test_the_masses_are_parallel_to_the_samples(self):
        self.assertEqual(len(self.data["log_m_eff"]), len(self.data["sample"]))
        self.assertEqual(len(self.data["log_m_tracker"]), len(self.data["sample"]))
        self.assertEqual(
            (self.data["log_m_eff"], self.data["log_m_tracker"]),
            SM.sample_masses(self.model, self.policy.cosmology, self.data["sample"]),
        )


class TestTheTuningsMasses(unittest.TestCase):
    """
    Ruling 2: tune_history computes the masses once, on the converged payload, and they equal integrate_history's
    for the same integration, bitwise. 2197 at A = 1 converges at its seed (one integration, about 2-4 s), and
    integrate_history is run again at the tuning's realization (another 2-4 s).
    """

    def test_tune_and_integrate_agree(self):
        from ComputeTargets.tests.test_drain_law import H2197
        from CosmologyConcepts import M_value, beta_value, temperature
        from CosmologyConcepts.FieldValues import phi_value, pi_value
        from CosmologyConcepts.Models.ExponentialModel import ExponentialModel

        cosmology = gth.make_cosmology()
        units = cosmology.units
        model = ExponentialModel(
            0,
            M_value(0, H2197["M_eV"] * units.eV),
            1,
            beta_value(0, H2197["beta"]),
            units,
        )
        args = (
            temperature(0, 2.0e4 * units.GeV),
            phi_value(0, 5.0 * units.PlanckMass),
            pi_value(0, 0.0),
            gth.make_z_grid(),
        )
        calls = []
        integrate = SM.integrate_history

        def recording(*a, **kw):
            calls.append(kw.get("compute_sample_masses"))
            return integrate(*a, **kw)

        with mock.patch.object(
            SM, "integrate_history", recording
        ), contextlib.redirect_stdout(io.StringIO()):
            tuned = SM.tune_history(
                cosmology, model, *args, alpha=1.0, drain_law=SM.DRAIN_LAW_ONE_LOOP
            )
        self.assertFalse(tuned.get("failure", False))
        self.assertEqual(set(calls), {False})
        with contextlib.redirect_stdout(io.StringIO()):
            direct = SM.integrate_history(
                cosmology,
                args[0],
                temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
                args[1],
                args[2],
                args[3],
                model.realize(tuned["normalization"], tuned["phi_ref"]),
                alpha=1.0,
                drain_law=SM.DRAIN_LAW_ONE_LOOP,
            )
        self.assertEqual(repr(tuned["sample"]), repr(direct["sample"]))
        self.assertEqual(repr(tuned["log_m_eff"]), repr(direct["log_m_eff"]))
        self.assertEqual(repr(tuned["log_m_tracker"]), repr(direct["log_m_tracker"]))
        self.assertEqual(repr(tuned["bounces"]), repr(direct["bounces"]))


if __name__ == "__main__":
    unittest.main()
