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
The release (inelastic-reflection prompt 07; README §8.4, gate "Release").

A parked field whose zero-lag tracker falls below m_eff/H_E = PARKING_ADIABATICITY_THRESHOLD at a
parked step end is released at the root of m_eff/H_E = the threshold on that step's ParkedSegment.
The parked phase hands the released state back to the loop, which starts a new Radau instance there,
with no drain armed, no infall assessment and the gate's half-oscillation begun at the release; the
history then goes on in the oscillating phase, draining at its candidates, and never parks again
(P24). The items are those of prompt §1.6, with orchestrator note 07's rulings:

  (1) what does not release is 06's: the existing tests, unedited (not here);
  (2) M = 0.5 M_P, beta = 5: parked at #105, released once inside the parked step on which 06 failed,
      at m_eff/H_E = 10 to the root's tolerance; the record; T_CMB; the readers;
  (3) bitwise the full history up to the release, from a window ending in the parked phase;
  (4) continuity at the release: the segments and the dense output, H_E, the ledger;
  (5) after the release: the candidates drain; neither parking_ratio_test nor cut_tracker is called
      (P24), though later candidates would have passed both (ruling 5); the gate is reset, shown by a
      sentinel (ruling 6);
  (6) the lag after the release against the first-order lag delta_1, as a sign-and-order test
      (ruling 9 as amended by the user, option (c));
  (7) M = 1e-3 M_P, beta = 1.2: released near 0.27 meV, with no candidate after it;
  (8) the ends: a T_final between the release and the end of its step, a T_final above the release,
      an N_stop inside the release step and one past it (ruling 8);
  (R7) the reflection branch's hand-back (ruling 7): M = 1e-7 M_P, beta = 1.2, with the threshold
      patched to 100.
Item (9), the change to test_parking's item-7 test at beta = 5, is in test_parking.

Every history uses test_injection's setup (n = 1, phi* = 5 M_P, pi* = 0, T* = 2e4 GeV, to T_CMB,
atol = rtol = 1e-8, main.py's axis, the model's seed realization, untuned) at alpha = 0.1, unless
it says otherwise. No Ray, no store, no PRyMordial solve. Each class that integrates gives its cost
and the load in its docstring.
"""

import contextlib
import functools
import importlib
import io
import unittest
from math import ceil, exp, log, nextafter, pi
from types import SimpleNamespace
from unittest import mock

import numpy as np

from ComputeTargets.tests import golden_tuned_history as gth
from ComputeTargets.tests import test_unresolved_turnarounds as tut
from ComputeTargets.tests.test_parking import root_tolerance
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel

SM = importlib.import_module("ComputeTargets.ScalarHistory")

EPS = 2.0**-52
ALPHA = 0.1

# the step loop as this module was imported, before any test patches the module's name
_INTEGRATE = SM.integrate_scalar_history

# log 06 §5.2: at M = 0.5 M_P, beta = 5 the parked history's tracker crosses m/H = 10 at N = 40.25454,
# inside the parked step [40.19089, 40.29089] whose end check failed at 06 (N = 40.29089, m/H = 8.94).
# The crossing is printed to seven digits: half a unit of the last is 5e-6 (orchestrator note 07,
# ruling 8). test_parking's BETA_5_CROSSING_N is the oscillating history's crossing, 40.2547
BETA_5_PARKED_CROSSING_N = 40.25454
BETA_5_PARKED_CROSSING_ROUNDING = 5e-6
BETA_5_FAILED_CHECK_N = 40.29089

# item (3)'s window, inside the parked phase (the switch is at N = 38.004) and before the release
# step [40.19089, 40.29089]; test_parking's window at beta = 5 ends here too
WINDOW_IN_THE_PARKED_PHASE = 40.0

# item (5)'s sentinel (ruling 6): larger than any non-adiabaticity a real history has (A_k <= 1.3 at
# M = 0.5); and the window, to a little after the first candidate after the release (N = 41.301)
SENTINEL = 1.0e3
SENTINEL_WINDOW = 41.5

# item (6): the first eight oscillations after the release, on a grid of LAG_GRID_PER_EFOLD points per
# e-fold (spacing 5e-4 e-folds, the derivative grid of d pi_min/dN), with dF/dphi and d2F/dphi2 by
# differences at LAG_PHI_STEP phi_min
LAG_OSCILLATIONS = 8
LAG_GRID_PER_EFOLD = 2000
LAG_PHI_STEP = 1.0e-6

# item (8)'s ends at beta = 5 (the release at 9.745 MeV, N = 40.2545; its step ends at N = 40.29089,
# 9.396 MeV; the first candidate after it is at N = 41.301)
T_FINAL_INSIDE_THE_RELEASE_STEP_MEV = 9.5
T_FINAL_ABOVE_THE_RELEASE_MEV = 10.0
N_STOP_INSIDE_THE_RELEASE_STEP_OFFSET = 0.01
N_STOP_PAST_THE_RELEASE_STEP = 40.5

# ruling 7: M = 1e-7 M_P, beta = 1.2 parks at #6, an unresolved reflection, with m/H 1.52e3 at the
# switch and 72.3 at its smallest (log 06 §5.4); with the threshold at 100 it is released at
# N = 45.883, on the step ending at N = 45.944, and the history ends at T_CMB at N = 46.075
REFLECTION_BRANCH_THRESHOLD = 100.0
REFLECTION_BRANCH_N_STOP = 45.95


def integrate(beta: float, M: float, patches=(), N_stop=None, T_final_MeV=None):
    """
    integrate_history at test_injection's setup (alpha = 0.1) under the patches, with the
    IntegrationResult, the RHS and its supervisor captured, to T_CMB or to T_final_MeV, and to
    N_stop if given. A failure's payload is returned, not raised.
    """
    cosmology = gth.make_cosmology()
    units = cosmology.units
    Mp = units.PlanckMass
    model = ExponentialModel(0, M_value(0, M * Mp), 1, beta_value(0, beta), units)
    seed = model.seed(cosmology)
    realized = model.realize(seed.normalization, seed.phi_ref)
    captured = {}

    def capturing(RHS, supervisor, *args, **kwargs):
        if N_stop is not None:
            kwargs["N_stop"] = N_stop
        captured["policy"], captured["rhs"], captured["supervisor"] = (
            RHS.policy,
            RHS,
            supervisor,
        )
        captured["result"] = _INTEGRATE(RHS, supervisor, *args, **kwargs)
        return captured["result"]

    T_stop = (
        cosmology.T_CMB_Kelvin * units.Kelvin
        if T_final_MeV is None
        else T_final_MeV * units.MeV
    )
    with contextlib.ExitStack() as stack:
        stack.enter_context(
            mock.patch.object(SM, "integrate_scalar_history", capturing)
        )
        for patch in patches:
            stack.enter_context(patch)
        stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        data = SM.integrate_history(
            cosmology,
            temperature(0, 2.0e4 * units.GeV),
            temperature(1, T_stop),
            phi_value(0, 5.0 * Mp),
            pi_value(0, 0.0),
            gth.make_z_grid(),
            realized,
            task_label="test_release",
            alpha=ALPHA,
        )
    return SimpleNamespace(
        data=data,
        result=captured.get("result"),
        policy=captured.get("policy"),
        rhs=captured.get("rhs"),
        supervisor=captured.get("supervisor"),
        model=realized,
        cosmology=cosmology,
        units=units,
        log_T_stop=log(T_stop),
    )


@functools.lru_cache(maxsize=None)
def beta_5():
    """
    M = 0.5 M_P, beta = 5 to T_CMB (about 6 s at a load of 5-15), with parking_ratio_test and
    cut_tracker spied through the module's names (ruling 5): each spy wraps the function, so the
    history is the unspied one. Shared by items (2)-(6) and (8).
    """
    with mock.patch.object(
        SM, "parking_ratio_test", wraps=SM.parking_ratio_test
    ) as ratio_spy, mock.patch.object(
        SM, "cut_tracker", wraps=SM.cut_tracker
    ) as tracker_spy:
        history = integrate(5.0, 0.5)
    history.ratio_test_calls = [tuple(c.args) for c in ratio_spy.call_args_list]
    history.cut_tracker_calls = [tuple(c.args) for c in tracker_spy.call_args_list]
    return history


def hubble_of(policy):
    return SM.HubblePolicy(policy.model, policy.units)


def m_over_H(policy, N: float, y) -> float:
    return SM.tracker_m_eff_over_H(policy, hubble_of(policy), N, np.asarray(y))[0]


def release_step(result):
    """(k, the release's ParkedSegment): ts[k] is the release's N, interpolants[k - 1] its segment."""
    ts = list(result.solution.ts)
    k = ts.index(result.release.N)
    return k, result.solution.interpolants[k - 1]


def root_tolerance_N(N: float) -> float:
    """
    brentq stops once its bracket is narrower than xtol + rtol |N| (RELEASE_ROOT_XTOL,
    RELEASE_ROOT_RTOL), so its root lies within that of a sign change of g; two roots of one g lie
    within twice that.
    """
    return 2.0 * (SM.RELEASE_ROOT_XTOL + SM.RELEASE_ROOT_RTOL * abs(N))


def release_tolerances(result, policy):
    """
    The tolerances of the release's root (log 07 §1.2), from the release's own segment:
      - slope: d(m_eff/H_E)/dN there, a central difference on the segment (h = 1e-4, inside it);
      - noise: g's own noise, |d(m_eff/H_E)/d ln phi| times the jitter of phi_min's root,
        root_tolerance(phi) in ln phi (minimum_on_trace's xtol and rtol);
      - in N: root_tolerance_N plus twice the noise over |slope|;
      - on m_eff/H_E: |slope| root_tolerance_N plus twice the noise, plus the rounding of m/H,
        exp(ln m - ln H) with logs of order 300, 1e3 eps of the threshold.
    Returns (slope, noise, the tolerance in N, the tolerance on m_eff/H_E).
    """
    _, segment = release_step(result)
    N = result.release.N
    h = 1e-4
    slope = (
        m_over_H(policy, N + h, segment(N + h))
        - m_over_H(policy, N - h, segment(N - h))
    ) / (2.0 * h)
    y = segment(N)
    yp, ym = y.copy(), y.copy()
    yp[0] *= 1.0 + 1e-6
    ym[0] *= 1.0 - 1e-6
    d_m_d_log_phi = (m_over_H(policy, N, yp) - m_over_H(policy, N, ym)) / 2e-6
    noise = abs(d_m_d_log_phi) * root_tolerance(y[0])
    tolerance_N = root_tolerance_N(N) + 2.0 * noise / abs(slope)
    tolerance_m = (
        abs(slope) * root_tolerance_N(N)
        + 2.0 * noise
        + 1e3 * EPS * SM.PARKING_ADIABATICITY_THRESHOLD
    )
    return slope, noise, tolerance_N, tolerance_m


def dop853_root_shift(result, policy, rtol: float = 1e-8, atol: float = 1e-8) -> float:
    """
    The bound on the shift of the release's root between two DOP853 segments of the same step with
    different step ends (item 8, ruling 8): each interpolant holds the reduced state within the
    solver's tolerance, atol + rtol |y_j| per component, so g moves by at most the sum of
    |d(m_eff/H_E)/dy_j| (atol + rtol |y_j|) over (ln rho_R,E, ln f_m, ln T_J), the derivatives by
    central differences on the full state (phi_min, pi_min held), and the root by that over |slope|.
    Twice that, for the two segments.
    """
    _, segment = release_step(result)
    N = result.release.N
    y = segment(N)
    slope, _, _, _ = release_tolerances(result, policy)
    shift = 0.0
    for j in (2, 3, 4):
        h = 1e-6 * max(1.0, abs(y[j]))
        yp, ym = y.copy(), y.copy()
        yp[j] += h
        ym[j] -= h
        d = (m_over_H(policy, N, yp) - m_over_H(policy, N, ym)) / (2.0 * h)
        shift += abs(d) * (atol + rtol * abs(y[j]))
    return 2.0 * shift / abs(slope)


def lag_by_oscillation(history, delta1_scale: float = 1.0):
    """
    Item (6) (prompt §0.2; ruling 9 as amended). Over the first LAG_OSCILLATIONS oscillations after
    the release, from each wall-side turning point to the next, on a grid of LAG_GRID_PER_EFOLD points
    per e-fold:
      - phi_min from parked_tracker on the history's own trace (its reduced state from the dense
        output), searched from the release's phi;
      - delta_1 = (d pi_min/dN - F)/(dF/dphi), F the right-hand side's pi' at (phi_min, pi_min), the
        code's own equation linearised about the zero-lag tracker; d pi_min/dN by np.gradient on the
        grid, dF/dphi and d2F/dphi2 by central differences at h = LAG_PHI_STEP phi_min;
    and per oscillation the means of x = phi - phi_min and of delta_1 (trapezoid over the grid points
    inside it, over their span) and the bound of log 07 §1.7:
      - T1 = |x(b) - x(a)|/(2 pi), the non-closure: an oscillation whose amplitude changes by Delta A
        between two points of equal phase has a window mean (Delta A/2 pi) sin(phase), at most
        Delta A/2 pi; x(a), x(b) at the turning points themselves;
      - T2 = |d2F/dphi2 / dF/dphi| S^2/4, the anharmonic shift of an oscillation of half-swing S,
        S = (max x - min x)/2 over the oscillation, and the derivatives' means there.
    delta_1 is multiplied by delta1_scale (ruling 9's checks). Returns a list of dicts.
    """
    r = history.result
    release = r.release
    model, cosmology, rhs, supervisor = (
        history.model,
        history.cosmology,
        history.rhs,
        history.supervisor,
    )
    walls = [t.N for t in r.turning_points if t.N > release.N and t.kind == "wall"]
    walls = walls[: LAG_OSCILLATIONS + 1]
    N_end = walls[-1]
    n = int(ceil((N_end - release.N) * LAG_GRID_PER_EFOLD)) + 1
    Ns = np.linspace(release.N, N_end, n)
    anchor = release.phi_Einstein

    def x_at(N):
        y = r.solution(N)
        return float(y[0]) - SM.parked_tracker(model, cosmology, anchor, y[2:])[0]

    phi = np.empty(n)
    phi_min = np.empty(n)
    pi_min = np.empty(n)
    F = np.empty(n)
    F_phi = np.empty(n)
    F_phiphi = np.empty(n)
    trackers = []
    for j, N in enumerate(Ns):
        y = r.solution(N)
        phi[j] = y[0]
        phi_min[j], pi_min[j] = SM.parked_tracker(model, cosmology, anchor, y[2:])
        trackers.append(np.array([phi_min[j], pi_min[j], *y[2:]], dtype=float))
    d_pi_min = np.gradient(pi_min, Ns)
    for j, N in enumerate(Ns):
        y = trackers[j]
        h = LAG_PHI_STEP * phi_min[j]
        step = np.array([h, 0.0, 0.0, 0.0, 0.0])
        F0 = rhs(N, y, supervisor)[1]
        Fp = rhs(N, y + step, supervisor)[1]
        Fm = rhs(N, y - step, supervisor)[1]
        F[j] = F0
        F_phi[j] = (Fp - Fm) / (2.0 * h)
        F_phiphi[j] = (Fp - 2.0 * F0 + Fm) / (h * h)
    delta1 = delta1_scale * (d_pi_min - F) / F_phi
    x = phi - phi_min

    rows = []
    for a, b in zip(walls[:-1], walls[1:]):
        m = (Ns >= a) & (Ns <= b)
        span = Ns[m][-1] - Ns[m][0]
        mean_x = np.trapezoid(x[m], Ns[m]) / span
        mean_delta1 = np.trapezoid(delta1[m], Ns[m]) / span
        S = 0.5 * (x[m].max() - x[m].min())
        T1 = abs(x_at(b) - x_at(a)) / (2.0 * pi)
        T2 = abs(np.mean(F_phiphi[m]) / np.mean(F_phi[m])) * S * S / 4.0
        rows.append(
            dict(
                a=a,
                b=b,
                mean_x=mean_x,
                mean_delta1=mean_delta1,
                half_swing=S,
                T1=T1,
                T2=T2,
                bound=T1 + T2,
            )
        )
    return rows


def lag_failures(rows):
    """The oscillations that fail item (6): the signs differ, or |<x> - <delta_1>| > T1 + T2."""
    failures = []
    for k, row in enumerate(rows):
        if (row["mean_x"] > 0.0) != (row["mean_delta1"] > 0.0):
            failures.append((k + 1, "sign", row))
        elif abs(row["mean_x"] - row["mean_delta1"]) > row["bound"]:
            failures.append((k + 1, "bound", row))
    return failures


def sentinel_patch(N_low: float, N_high: float):
    """
    Ruling 6: step_nonadiabaticity, through the module's name, returning SENTINEL at every sample with
    N_low < N <= N_high. Returns (the patch, a list of the number of samples replaced per call).
    """
    original = SM.step_nonadiabaticity
    replaced = []

    def patched(N_samples, log_m2_samples, H_samples):
        A = original(N_samples, log_m2_samples, H_samples)
        mask = (N_samples > N_low) & (N_samples <= N_high)
        if mask.any():
            A = A.copy()
            A[mask] = SENTINEL
            replaced.append(int(mask.sum()))
        return A

    return mock.patch.object(SM, "step_nonadiabaticity", patched), replaced


def assert_continuous_at_the_release(test: unittest.TestCase, result, policy):
    """
    Item (4) (ruling 3): the last parked segment and the first Radau segment return the same floats
    at N_rel, the dense output returns them, H_E either side is the same float, and the ledger is the
    same either side.
    """
    release = result.release
    k, segment = release_step(result)
    radau = result.solution.interpolants[k]
    test.assertIsInstance(segment, SM.ParkedSegment)
    test.assertNotIsInstance(radau, SM.ParkedSegment)
    y_parked = segment(release.N)
    y_radau = radau(release.N)
    test.assertEqual(tuple(y_parked), tuple(y_radau))
    test.assertEqual(tuple(result.solution(release.N)), tuple(y_radau))
    test.assertEqual(
        (release.phi_Einstein, release.pi_Einstein, release.log_T_Jordan),
        (float(y_radau[0]), float(y_radau[1]), float(y_radau[4])),
    )
    hubble = hubble_of(policy)
    H_parked = SM.tracker_m_eff_over_H(policy, hubble, release.N, y_parked)[1]
    H_radau = SM.tracker_m_eff_over_H(policy, hubble, release.N, y_radau)[1]
    test.assertEqual(H_parked, H_radau)
    test.assertEqual(H_parked, release.H_Einstein)
    ledger = policy.dark_radiation
    test.assertEqual(ledger.K(nextafter(release.N, 0.0)), ledger.K(release.N))


# ---- (2), (4), (5), (6) M = 0.5 M_P, beta = 5 ----


class TestTheReleaseAtHalfThePlanckMass(unittest.TestCase):
    """
    (2), (4), (5) and (6): M = 0.5 M_P, beta = 5, to T_CMB (beta_5(); about 6 s), and the lag's grid of
    5 550 points over the eight oscillations (about 3 s), at a load of 5-15.
    """

    @classmethod
    def setUpClass(cls):
        cls.h = beta_5()
        cls.data, cls.result, cls.policy = cls.h.data, cls.h.result, cls.h.policy
        # a failed history (as under 06's failure, B6) has no result: the first test then fails on
        # the failure, rather than the class on its setup
        if cls.result is not None and cls.result.release is not None:
            cls.release = cls.result.release
            cls.parking = cls.result.parking
            cls.ts = list(cls.result.solution.ts)
            cls.k, cls.segment = release_step(cls.result)

    def test_2_it_reaches_T_CMB_and_integrate_history_succeeds(self):
        self.assertFalse(self.data.get("failure", False), self.data)
        T_CMB = self.h.cosmology.T_CMB_Kelvin * self.h.units.Kelvin
        self.assertAlmostEqual(self.result.final_state.log_T_Jordan, log(T_CMB), 9)
        self.assertEqual(self.data["release"], self.release)
        self.assertEqual(self.data["parking"], self.parking)

    def test_2_it_parks_at_105_and_is_released_once_inside_the_step_06_failed_on(self):
        p, rel = self.parking, self.release
        self.assertEqual(p.candidate_index, 105)
        self.assertEqual(self.result.injections[105].kind, "turning_point")
        self.assertIsNotNone(rel)
        # the check that failed is 06's failure, to the digits log 06 prints
        self.assertEqual(round(rel.N_check, 5), BETA_5_FAILED_CHECK_N)
        # strictly inside that step: its left node, the release, the check
        N_left = self.ts[self.k - 1]
        self.assertEqual(N_left, self.segment.t_min)
        self.assertLess(N_left, rel.N)
        self.assertLess(rel.N, rel.N_check)
        self.assertEqual(self.segment.t_max, rel.N_check)
        _, _, tolerance_N, _ = release_tolerances(self.result, self.policy)
        self.assertLessEqual(
            abs(rel.N - BETA_5_PARKED_CROSSING_N),
            BETA_5_PARKED_CROSSING_ROUNDING + tolerance_N,
        )
        # one park and one release: the parked segments are the parked phase's steps, all before the
        # release; every segment after it is Radau's
        segments = self.result.solution.interpolants
        parked = [j for j, s in enumerate(segments) if isinstance(s, SM.ParkedSegment)]
        self.assertEqual(len(parked), p.parked_steps)
        self.assertEqual(parked[-1], self.k - 1)
        self.assertEqual(parked, list(range(self.k - p.parked_steps, self.k)))
        self.assertEqual(self.ts[self.k - p.parked_steps], p.N)

    def test_2_m_over_H_at_the_release_is_the_threshold(self):
        rel = self.release
        slope, noise, _, tolerance = release_tolerances(self.result, self.policy)
        self.assertLess(slope, 0.0)
        self.assertLessEqual(
            abs(rel.m_eff_over_H - SM.PARKING_ADIABATICITY_THRESHOLD),
            tolerance,
            (rel.m_eff_over_H, slope, noise, tolerance),
        )
        self.assertLess(tolerance, 1e-9)

    def test_2_the_release_record_by_construction(self):
        rel, p, r = self.release, self.parking, self.result
        y = self.segment(rel.N)
        self.assertEqual(rel.N, self.ts[self.k])
        self.assertEqual(rel.log_T_Jordan, float(y[4]))
        self.assertEqual(
            (rel.phi_Einstein, rel.pi_Einstein), (float(y[0]), float(y[1]))
        )
        m, H = SM.tracker_m_eff_over_H(self.policy, hubble_of(self.policy), rel.N, y)
        self.assertEqual((rel.m_eff_over_H, rel.H_Einstein), (m, H))
        self.assertLess(rel.m_eff_over_H_check, SM.PARKING_ADIABATICITY_THRESHOLD)
        self.assertEqual(
            rel.candidate_index, len([x for x in r.injections if x.N < rel.N])
        )
        self.assertEqual(rel.candidate_index, 106)
        self.assertEqual(rel.steps_after, len(self.ts) - 1 - self.k)
        self.assertEqual(r.accepted_steps, len(self.ts) - 1)
        # ruling 4: the failing check is not in the minimum; the root's value is, and is the
        # smallest of the parked phase here
        self.assertEqual(p.min_m_eff_over_H, rel.m_eff_over_H)
        self.assertGreater(rel.m_eff_over_H_check, 0.0)
        for value in rel:
            self.assertIsInstance(value, (float, int), rel)

    def test_2_the_readers(self):
        """first_bounce before the switch; T_J = 1 MeV and 70 keV after the release."""
        bounce = self.data["first_bounce"]
        self.assertIsNotNone(bounce)
        self.assertLess(bounce.N, self.parking.N)
        for value in self.data["fixed_T_values"]:
            self.assertIsNotNone(value)
        for T_MeV in (SM.FIXED_T_JORDAN_HIGH_MEV, SM.FIXED_T_JORDAN_LOW_MEV):
            N = SM.T_Jordan_crossing(self.result, log(T_MeV * self.h.units.MeV))
            self.assertGreater(N, self.release.N, T_MeV)

    def test_4_continuity_at_the_release(self):
        assert_continuous_at_the_release(self, self.result, self.policy)
        # nothing in the ledger between the switch and the first candidate after the release
        first_after = self.result.injections[self.release.candidate_index]
        self.assertFalse(
            [
                e
                for e in self.policy.dark_radiation.injections
                if self.parking.N < e[0] < first_after.N
            ]
        )

    def test_5_the_candidates_after_the_release_drain(self):
        """The ledger is the candidates' Delta K and the leftover, in order (15 after the release)."""
        r, p = self.result, self.parking
        after = r.injections[self.release.candidate_index :]
        self.assertEqual(len(after), 15)
        for x in after:
            self.assertEqual(x.kind, "turning_point")
            self.assertGreater(x.alpha_eff, 0.0)
        expected = []
        for index, x in enumerate(r.injections):
            if x.alpha_eff > 0.0:
                expected.append((x.N, x.delta_K))
            if index == p.candidate_index:
                expected.append((p.N, p.delta_K))
        self.assertEqual(list(self.policy.dark_radiation.injections), expected)

    def test_5_no_ratio_test_and_no_tracker_after_the_release(self):
        """
        P24 (ruling 5): parking_ratio_test is called at candidates 0-105 only, and cut_tracker at
        none after the switch. Not vacuous: at least one candidate after the release has
        rho_kin/rho_R,E < epsilon and a tracker with m_eff/H_E >= 10 at its post-candidate state,
        cut_tracker called here, unspied.
        """
        p = self.parking
        self.assertEqual(
            [c[0] for c in self.h.ratio_test_calls], list(range(p.candidate_index + 1))
        )
        self.assertGreaterEqual(len(self.h.cut_tracker_calls), 1)
        self.assertEqual(self.h.cut_tracker_calls[-1][2], p.N)
        for call in self.h.cut_tracker_calls:
            self.assertLessEqual(call[2], p.N)
        would_park = []
        hubble = hubble_of(self.policy)
        for index in range(self.release.candidate_index, len(self.result.injections)):
            x = self.result.injections[index]
            y = self.result.solution(x.N)
            ratio = exp(x.log_rho_kin - float(y[2]))
            if SM.parking_ratio_test(index, ratio):
                tracker = SM.cut_tracker(self.policy, hubble, x.N, y)
                if SM.tracker_adiabatic(tracker.m_eff_over_H):
                    would_park.append(index)
        self.assertGreaterEqual(len(would_park), 1)

    def test_6_the_lag_after_the_release_against_delta_1(self):
        """
        Ruling 9 as amended by the user (option (c)): on each of the first eight oscillations,
        <phi - phi_min> has the sign of <delta_1>, and |<phi - phi_min> - <delta_1>| is within
        T1 + T2 (lag_by_oscillation), computed from that oscillation. delta_1 x -1 fails it; x 2 and
        x 1/2 are within the bound and are not caught (log 07 §1.7).
        """
        rows = lag_by_oscillation(self.h)
        self.assertEqual(len(rows), LAG_OSCILLATIONS)
        self.assertEqual(lag_failures(rows), [])
        for row in rows:
            self.assertLess(row["mean_delta1"], 0.0)


class TestTheGateIsResetAtTheRelease(unittest.TestCase):
    """
    (5), ruling 6: M = 0.5 M_P, beta = 5 to N = 41.5, after the first candidate after the release
    (N = 41.301), with and without the sentinel on the samples between the wall-side turning point
    before the switch and the switch (about 5 + 5 s at a load of 5-15).
    """

    @classmethod
    def setUpClass(cls):
        full = beta_5().result
        p = full.parking
        cls.N_turn = full.injections[p.candidate_index].N_turn
        cls.N_switch = p.N
        cls.plain, cls.plain_policy, _ = tut.window(5.0, 0.5, ALPHA, SENTINEL_WINDOW)
        patch, cls.replaced = sentinel_patch(cls.N_turn, cls.N_switch)
        cls.marked, cls.marked_policy, _ = tut.window(
            5.0, 0.5, ALPHA, SENTINEL_WINDOW, patches=(patch,)
        )

    def test_5_the_sentinel_marked_samples_before_the_switch(self):
        self.assertGreater(sum(self.replaced), 0)
        self.assertLess(self.N_turn, self.N_switch)
        self.assertIsNotNone(self.marked.release)
        self.assertGreater(
            len(self.marked.injections), self.marked.release.candidate_index
        )

    def test_5_the_first_turning_point_and_candidate_after_the_release(self):
        """
        Only the half-oscillation that the switch abandons holds the marked samples, so with the reset
        the marked history is the plain one in every record, and the first turning point and the
        first candidate after the release carry no sentinel.
        """
        a, b = self.marked, self.plain
        rel = a.release
        first_tp = [t for t in a.turning_points if t.N > rel.N][0]
        first_candidate = a.injections[rel.candidate_index]
        self.assertLess(first_tp.nonadiabaticity, SENTINEL)
        self.assertLess(first_candidate.nonadiabaticity, SENTINEL)
        self.assertEqual(a.turning_points, b.turning_points)
        self.assertEqual(a.injections, b.injections)
        self.assertEqual(a.parking, b.parking)
        self.assertEqual(a.release, b.release)
        self.assertEqual(list(a.solution.ts), list(b.solution.ts))
        self.assertEqual(tuple(a.final_state), tuple(b.final_state))
        self.assertEqual(
            self.marked_policy.dark_radiation.injections,
            self.plain_policy.dark_radiation.injections,
        )


# ---- (3) bitwise up to the release ----


class TestBitwiseUpToTheRelease(unittest.TestCase):
    """
    (3): M = 0.5 M_P, beta = 5 to N_stop = 40.0, inside the parked phase and before the release step,
    against the full history (about 5 s at a load of 5-15).
    """

    @classmethod
    def setUpClass(cls):
        cls.full = beta_5().result
        cls.window, _, _ = tut.window(5.0, 0.5, ALPHA, WINDOW_IN_THE_PARKED_PHASE)

    def test_3_the_records(self):
        a, b = self.window, self.full
        self.assertIsNone(a.release)
        self.assertEqual(a.injections, b.injections[: len(a.injections)])
        self.assertEqual(len(a.injections), b.parking.candidate_index + 1)
        self.assertEqual(a.reflections, b.reflections)
        self.assertEqual(
            a.turning_points,
            [t for t in b.turning_points if t.N <= WINDOW_IN_THE_PARKED_PHASE],
        )
        self.assertEqual(
            a.parking[:-2], b.parking[:-2]
        )  # all but parked_steps and min_m_eff_over_H
        self.assertEqual(a.warnings, b.warnings)

    def test_3_ts_and_every_segment_at_its_nodes(self):
        """Every node but the window's last, and every segment but its last, at both its nodes."""
        a, b = self.window, self.full
        ts_a, ts_b = list(a.solution.ts), list(b.solution.ts)
        n = len(ts_a) - 1
        self.assertEqual(ts_a[:n], ts_b[:n])
        self.assertGreater(n, self.full.parking.parked_steps)
        for k in range(n - 1):
            for N in (ts_a[k], ts_a[k + 1]):
                self.assertEqual(
                    tuple(a.solution.interpolants[k](N)),
                    tuple(b.solution.interpolants[k](N)),
                    (k, N),
                )


# ---- (7) M = 1e-3 M_P ----


class TestALateRelease(unittest.TestCase):
    """(7): M = 1e-3 M_P, beta = 1.2, to T_CMB (about 2 s at a load of 5-15)."""

    @classmethod
    def setUpClass(cls):
        cls.h = integrate(1.2, 1e-3)

    def test_7_parks_at_6_is_released_near_0_27_meV_and_reaches_T_CMB(self):
        data, r = self.h.data, self.h.result
        self.assertFalse(data.get("failure", False), data)
        self.assertEqual(r.parking.candidate_index, 6)
        rel = r.release
        self.assertIsNotNone(rel)
        self.assertAlmostEqual(rel.N, 45.91, 2)
        # T_J = 2.66e-10 MeV = 0.27 meV, just above T_CMB = 0.235 meV (prompt §0.2 and log 06 §5.3
        # print it as 0.27 eV and 0.25 eV)
        self.assertAlmostEqual(exp(rel.log_T_Jordan) / self.h.units.MeV * 1e9, 0.27, 2)
        _, _, _, tolerance = release_tolerances(r, self.h.policy)
        self.assertLessEqual(
            abs(rel.m_eff_over_H - SM.PARKING_ADIABATICITY_THRESHOLD), tolerance
        )
        T_CMB = self.h.cosmology.T_CMB_Kelvin * self.h.units.Kelvin
        self.assertAlmostEqual(r.final_state.log_T_Jordan, log(T_CMB), 9)
        self.assertEqual(rel.candidate_index, len(r.injections))
        self.assertFalse([t for t in r.turning_points if t.N > rel.N])
        self.assertGreater(rel.steps_after, 0)
        assert_continuous_at_the_release(self, r, self.h.policy)


# ---- (8) the ends ----


class TestTheEnds(unittest.TestCase):
    """
    (8), ruling 8: M = 0.5 M_P, beta = 5 to T_final = 9.5 MeV (between the release, 9.745 MeV, and the
    end of its step, 9.396 MeV) and 10 MeV, and to N_stop = N_rel + 0.01 (inside the release step) and
    40.5 (past it, before the first drain after the release); about 4 x 5 s at a load of 5-15.
    """

    @classmethod
    def setUpClass(cls):
        cls.full = beta_5()
        cls.N_rel = cls.full.result.release.N
        cls.inside = integrate(
            5.0, 0.5, T_final_MeV=T_FINAL_INSIDE_THE_RELEASE_STEP_MEV
        )
        cls.above = integrate(5.0, 0.5, T_final_MeV=T_FINAL_ABOVE_THE_RELEASE_MEV)
        cls.stop_inside = integrate(
            5.0, 0.5, N_stop=cls.N_rel + N_STOP_INSIDE_THE_RELEASE_STEP_OFFSET
        )
        cls.stop_past = integrate(5.0, 0.5, N_stop=N_STOP_PAST_THE_RELEASE_STEP)

    def assert_ends_oscillating(self, h):
        self.assertFalse(h.data.get("failure", False), h.data)
        r = h.result
        self.assertIsNotNone(r.release)
        self.assertGreater(r.N_final, r.release.N)
        self.assertNotIsInstance(r.solution.interpolants[-1], SM.ParkedSegment)
        assert_continuous_at_the_release(self, r, h.policy)

    def test_8_a_T_final_inside_the_release_step_releases_and_ends_oscillating(self):
        """
        The parked step ends the history at 9.5 MeV with m/H below 10 there, so the release comes
        first: the same DOP853 step, its root sought on a shorter bracket, so N_rel is the full
        history's within the root's tolerance in N (release_tolerances), not bitwise.
        """
        h = self.inside
        self.assert_ends_oscillating(h)
        r = h.result
        self.assertAlmostEqual(r.final_state.log_T_Jordan, h.log_T_stop, 9)
        k, segment = release_step(r)
        # the check that failed is the parked step's end at T_final
        self.assertAlmostEqual(float(segment(r.release.N_check)[4]), h.log_T_stop, 9)
        self.assertLess(r.release.N_check, self.full.result.release.N_check)
        _, _, tolerance_N, _ = release_tolerances(self.full.result, self.full.policy)
        self.assertLessEqual(abs(r.release.N - self.N_rel), tolerance_N)

    def test_8_a_T_final_above_the_release_ends_parked(self):
        h = self.above
        self.assertFalse(h.data.get("failure", False), h.data)
        r = h.result
        self.assertIsNotNone(r.parking)
        self.assertIsNone(r.release)
        self.assertIsNone(h.data["release"])
        self.assertIsInstance(r.solution.interpolants[-1], SM.ParkedSegment)
        self.assertLess(r.N_final, self.N_rel)
        self.assertAlmostEqual(r.final_state.log_T_Jordan, h.log_T_stop, 9)
        self.assertGreaterEqual(
            r.parking.min_m_eff_over_H, SM.PARKING_ADIABATICITY_THRESHOLD
        )

    def test_8_an_N_stop_inside_the_release_step(self):
        """
        DOP853's last parked step is cut at N_stop, so the release's segment is another interpolant:
        N_rel within dop853_root_shift plus the root's tolerance of the full history's.
        """
        h = self.stop_inside
        self.assert_ends_oscillating(h)
        r = h.result
        N_stop = self.N_rel + N_STOP_INSIDE_THE_RELEASE_STEP_OFFSET
        self.assertEqual(r.N_final, N_stop)
        self.assertEqual(r.release.N_check, N_stop)
        _, _, tolerance_N, _ = release_tolerances(self.full.result, self.full.policy)
        bound = tolerance_N + dop853_root_shift(self.full.result, self.full.policy)
        self.assertLessEqual(abs(r.release.N - self.N_rel), bound)
        self.assertNotEqual(r.release.N, self.N_rel)

    def test_8_an_N_stop_past_the_release_step_is_bitwise(self):
        h = self.stop_past
        self.assert_ends_oscillating(h)
        r = h.result
        self.assertEqual(r.N_final, N_STOP_PAST_THE_RELEASE_STEP)
        self.assertEqual(r.release[:-1], self.full.result.release[:-1])
        self.assertEqual(r.release.candidate_index, len(r.injections))


# ---- ruling 7: the reflection branch ----


class TestTheReflectionBranchHandsBack(unittest.TestCase):
    """
    Ruling 7: M = 1e-7 M_P, beta = 1.2, with PARKING_ADIABATICITY_THRESHOLD patched to 100, between the
    switch's m/H (1.52e3) and the parked phase's smallest (72.3), to N_stop = 45.95, after the release
    at N = 45.883 (about 2 s at a load of 5-15). The switch is at an unresolved reflection, so the
    release is handed back through the loop's reflection branch.
    """

    @classmethod
    def setUpClass(cls):
        threshold = mock.patch.object(
            SM, "PARKING_ADIABATICITY_THRESHOLD", REFLECTION_BRANCH_THRESHOLD
        )
        cls.result, cls.policy, _ = tut.window(
            1.2, 1e-7, ALPHA, REFLECTION_BRANCH_N_STOP, patches=(threshold,)
        )
        with threshold:
            cls.tolerance = release_tolerances(cls.result, cls.policy)[3]

    def test_R7_parked_at_a_reflection_released_and_handed_back(self):
        r = self.result
        p = r.parking
        x = r.injections[p.candidate_index]
        self.assertEqual(x.kind, "unresolved")
        self.assertIn(p.N, [f.N for f in r.reflections])
        rel = r.release
        self.assertIsNotNone(rel)
        self.assertLessEqual(
            abs(rel.m_eff_over_H - REFLECTION_BRANCH_THRESHOLD), self.tolerance
        )
        self.assertLess(rel.m_eff_over_H_check, REFLECTION_BRANCH_THRESHOLD)
        # the history goes on in Radau segments to N_stop
        self.assertEqual(r.N_final, REFLECTION_BRANCH_N_STOP)
        k, _ = release_step(r)
        after = r.solution.interpolants[k:]
        self.assertGreater(len(after), 1)
        for segment in after:
            self.assertNotIsInstance(segment, SM.ParkedSegment)
        self.assertEqual(rel.steps_after, len(after))
        assert_continuous_at_the_release(self, r, self.policy)


if __name__ == "__main__":
    unittest.main()
