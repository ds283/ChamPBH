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
The wall search at the rounding of ln V (inelastic-reflection prompt 07b; README §8.4, gate "Band").

The wall search forms ln(V - V_floor) from ln V and ln V_floor (`_log_V_minus_floor`). Where
ln V - ln V_floor is a few ulps of ln V it is quantized, and two probes of the search can see the
same residual. The helper now returns -inf where the difference is within WALL_ROUNDING_QUANTA ulps
of max(|ln V|, |ln V_floor|), as it does where V rounds to V_floor. The width is derived in log 07b
§1 from the search's closest comparison of two finite values. The items are those of prompt §1.4:

  (1) the helper at its edge, on synthetic floats built with nextafter from the exponential's
      normalization at M = 1e-16 M_P;
  (2) classical_turning_point at the inputs on which the shipped search failed (the orchestrator's
      probe, band_shipped.out), against the exponential's closed form; and one synthetic case that
      makes the search's binding comparison, a halving midpoint 2^-WALL_SEARCH_HALVINGS e-folds
      from a finite end just above the width (orchestrator note 07b, ruling 4);
  (3) one history in the band, M = 1e-16 M_P, beta = 2, alpha = 0.1, to T_CMB.
Item (4), that the golden tests and the integrator's tests pass unedited, is the suites'.

No store, no Ray, no PRyMordial solve. Items (1) and (2) take milliseconds; item (3) integrates.
"""

import contextlib
import importlib
import io
import sys
import unittest
from math import exp, expm1, inf, log, log1p, nextafter, ulp
from unittest import mock

from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from Units import Planck_units

AM = importlib.import_module("CosmologyConcepts.Models.AbstractModel")
SM = importlib.import_module("ComputeTargets.ScalarHistory")

EPS = sys.float_info.epsilon
UNITS = Planck_units()
Mp = UNITS.PlanckMass

# the exponential's normalization at M = 1e-16 M_P, beta = 2, n = 1, the seed realization
# (band_shipped.out, the probe of 2026-10-30 at 0b8f420)
LOG_V_FLOOR = -276.6307286951731

# the inputs on which the shipped search raised "not monotone" (band_shipped.out, printed by repr, so
# that each literal is the float the probe used): (beta, M/M_P, normalization, phi in M_P, ln E)
BAND_SHIPPED = (
    (1.2, 1e-16, -276.63072867237236, 0.0032139686095901216, -174.4719252767463),
    (1.2, 1e-20, -276.6307285948015, 3.369669680676171e-07, -174.4872967838901),
    (2.0, 1e-13, -276.6307317928209, 3.495213815287529, -143.23235698717016),
    (2.0, 1e-14, -276.6307296055691, 0.3204328372746271, -168.01052773888304),
    (2.0, 1e-15, -276.6307289138986, 0.03506252138301913, -170.3052025063662),
    (2.0, 1e-16, -276.6307286951731, 0.0034528354908497376, -170.5592843374548),
    (2.0, 1e-18, -276.6307286041335, 3.348350993624157e-05, -170.58676785437373),
    (2.0, 1e-20, -276.6307285950295, 3.2472921315731173e-07, -170.58703436436073),
    (2.0, 1e-22, -276.6307285941191, 3.4990037592711036e-09, -170.5870369455268),
    (2.0, 1e-25, -276.63072859402115, 3.2377065156715494e-12, -205.16329433642025),
    (3.0, 1e-16, -276.63072871790723, 0.0034262383918000766, -168.2870977122513),
    (3.0, 1e-20, -276.63072859525687, 3.231219569446254e-07, -168.32559995251538),
)

# log 06 §5.4: Sigma + leftover at M = 1e-30 M_P, beta = 2, printed to eight significant figures, and
# its M-dependence across 1e-7 to 1e-30 M_P at beta = 2, "2e-8" relative, taken at the upper end of
# its one-digit print (orchestrator note 07b, ruling 6). Re-measured by electroweak-eos prompt 01 on its
# w table, the same way (.documents/electroweak-eos/table/drain_total_by_M.py, which gives log 06's
# 0.012616907 on the table before): 0.0126169558132557 at M = 1e-30 M_P, printed to eight figures here;
# its M-dependence there is 6.5e-9, inside the 2.5e-8 kept below
SIGMA_PLUS_LEFTOVER_AT_1E_30 = 0.012616956
SIGMA_M_DEPENDENCE = 2.5e-8
SIGMA_HALF_LAST_DIGIT = 5.0e-10


def _derived_width(n: float = 1.0, rho: float = 0.5) -> int:
    """
    The width K in quanta that log 07b §1 derives: the smallest integer with
    (e^{s 2^-H} - 1)(K + 1 - rho) > 2 rho, H = WALL_SEARCH_HALVINGS, for the exponential with
    s = n = 1 and rho = 1/2 (ln V rounded once to the quantum, ln V_floor exact). It is computed here
    from the inequality, not read from the module, so that the test sees a width that is not derived.
    """
    g = expm1(n * 2.0**-AM.WALL_SEARCH_HALVINGS)
    k = 0
    while not g * (k + 1 - rho) > 2.0 * rho:
        k += 1
    return k


K = _derived_width()


def _shipped(log_V: float, log_V_floor: float) -> float:
    """The helper as 04 shipped it, written here: -inf where V rounds to V_floor or below."""
    if not log_V > log_V_floor:
        return float("-inf")
    return log_V + log(-expm1(log_V_floor - log_V))


def _up(x: float, j: int) -> float:
    """x moved j floats towards +inf."""
    for _ in range(j):
        x = nextafter(x, inf)
    return x


def _model(beta: float, M_over_Mp: float) -> ExponentialModel:
    return ExponentialModel(
        0, M_value(0, M_over_Mp * Mp), 1, beta_value(0, beta), UNITS
    )


def _closed_form_log_phi_t(M: float, normalization: float, log_E: float) -> float:
    """ln phi_t on the exponential, n = 1: M/phi_t = log1p(E/V_0), V_0 = e^normalization."""
    return log(M) - log(log1p(exp(log_E - normalization)))


def _log_phi_t_tolerance(M: float, normalization: float, log_E: float) -> float:
    """
    The tolerance on ln phi_t (orchestrator note 07b, ruling 9): the sum of three named terms, with
    x_t = ln phi_t, u_t = M/phi_t = log1p(E/V_0) and ln V_t = normalization + u_t.
    - brentq's: SciPy stops once the sign change is bracketed within WALL_ROOT_XTOL +
      WALL_ROOT_RTOL |x_t|.
    - The residual's rounding over its slope at the root. The residual is ln(V - V_floor) - ln E =
      ln V + log(-expm1(ln V_floor - ln V)) - ln E. ln V = normalization + M/phi is rounded within
      ulp(ln V_t)/2 (the sum) + eps u_t/2 (the division), and that error in the difference reaches
      ln(V - V_floor) multiplied by 1/(-expm1(-u_t)); where the difference is a few quanta this is
      the quantization, half a quantum in u_t. The log, the sum and the subtraction of ln E add
      eps |ln E| at most. The slope is |d residual/d ln phi| = u_t/(-expm1(-u_t)).
    - The closed form's own rounding: ln E - normalization is rounded within
      eps (|ln E| + |normalization|)/2, which exp and log1p carry into u_t as a relative error of
      f = z/((1 + z) log1p z) times it, z = E/V_0, plus eps each; ln M - ln u_t, the method's e^x
      and this test's log of phi_t add eps (2|x_t| + |ln u_t| + |ln M|) at most.
    At the twelve inputs the terms are 4.1-6.5e-14, 4.6-9.5e-16 and 2.5-4.4e-14, and the errors at
    most 7.1e-15 (log 07b §4.2).
    """
    z = exp(log_E - normalization)
    u_t = log1p(z)
    x_t = log(M) - log(u_t)
    log_V_t = normalization + u_t
    brentq_term = AM.WALL_ROOT_XTOL + AM.WALL_ROOT_RTOL * abs(x_t)
    rounding = (0.5 * ulp(abs(log_V_t)) + 0.5 * EPS * u_t) / u_t + EPS * abs(
        log_E
    ) * -expm1(-u_t) / u_t
    f = z / ((1.0 + z) * u_t)
    closed_form_term = f * EPS * (abs(log_E) + abs(normalization) + 2.0) + EPS * (
        2.0 * abs(x_t) + abs(log(u_t)) + abs(log(M)) + 4.0
    )
    return brentq_term + rounding + closed_form_term


class TestTheHelperAtItsEdge(unittest.TestCase):
    """
    (1) On synthetic floats: ln V_floor is the exponential's normalization at M = 1e-16 M_P, and
    ln V = ln V_floor + j quanta, j floats above it (nextafter); the quantum is
    ulp(|ln V_floor|) = 2^-44 = 5.68e-14, the same for every j used here.
    """

    def test_1_the_width_is_the_derived_one(self):
        """WALL_ROUNDING_QUANTA is the smallest integer the inequality of log 07b §1 admits: 16."""
        self.assertEqual(K, 16)
        self.assertEqual(AM.WALL_ROUNDING_QUANTA, K)
        g = expm1(2.0**-AM.WALL_SEARCH_HALVINGS)
        self.assertGreater(g * (K + 0.5), 1.0)
        self.assertLessEqual(g * (K - 1 + 0.5), 1.0)

    def test_1_the_difference_is_j_quanta(self):
        q = ulp(abs(LOG_V_FLOOR))
        self.assertEqual(q, 2.0**-44)
        for j in range(0, 2 * K + 3):
            self.assertEqual(_up(LOG_V_FLOOR, j) - LOG_V_FLOOR, j * q, j)

    def test_1_j_from_0_to_K_returns_minus_inf(self):
        for j in range(0, K + 1):
            log_V = _up(LOG_V_FLOOR, j)
            self.assertEqual(
                AM._log_V_minus_floor(log_V, LOG_V_FLOOR), float("-inf"), j
            )

    def test_1_j_equal_to_K_plus_1_returns_the_shipped_expression_bitwise(self):
        for j in (K + 1, K + 2, 2 * K):
            log_V = _up(LOG_V_FLOOR, j)
            value = AM._log_V_minus_floor(log_V, LOG_V_FLOOR)
            self.assertEqual(value, _shipped(log_V, LOG_V_FLOOR), j)
            self.assertGreater(value, float("-inf"), j)

    def test_1_V_below_its_floor_returns_minus_inf(self):
        for log_V in (
            nextafter(LOG_V_FLOOR, -inf),
            LOG_V_FLOOR - 1.0,
            float("-inf"),
        ):
            self.assertEqual(
                AM._log_V_minus_floor(log_V, LOG_V_FLOOR), float("-inf"), log_V
            )

    def test_1_a_difference_of_order_one_returns_the_shipped_expression_bitwise(self):
        for d in (0.5, 1.0, 3.0, 100.0):
            log_V = LOG_V_FLOOR + d
            value = AM._log_V_minus_floor(log_V, LOG_V_FLOOR)
            self.assertEqual(value, _shipped(log_V, LOG_V_FLOOR), d)

    def test_1_an_infinite_log_V_keeps_the_shipped_value(self):
        """ln V = +inf is not within the rounding: +inf, as shipped, which the search refuses."""
        self.assertEqual(AM._log_V_minus_floor(inf, LOG_V_FLOOR), inf)
        self.assertEqual(_shipped(inf, LOG_V_FLOOR), inf)


class TestTheTurningPointsTheShippedSearchRefused(unittest.TestCase):
    """
    (2) classical_turning_point at the twelve inputs of band_shipped.out, each against the
    exponential's closed form (orchestrator note 07b, ruling 9); and ruling 4's synthetic case.
    """

    def test_2_the_twelve_inputs_return_the_closed_form(self):
        for beta, M_over_Mp, normalization, phi, log_E in BAND_SHIPPED:
            with self.subTest(beta=beta, M=M_over_Mp):
                model = _model(beta, M_over_Mp)
                M = M_over_Mp * Mp
                # the shipped helper raised here (band_shipped.out): the input is the one it refused
                with mock.patch.object(AM, "_log_V_minus_floor", _shipped):
                    with self.assertRaises(ComputationFailureError) as ctx:
                        model.classical_turning_point(normalization, phi, log_E)
                self.assertIn("not monotone", str(ctx.exception))

                c = model.classical_turning_point(normalization, phi, log_E)
                self.assertIsNotNone(c)
                expected = _closed_form_log_phi_t(M, normalization, log_E)
                tolerance = _log_phi_t_tolerance(M, normalization, log_E)
                self.assertLessEqual(
                    abs(log(c.phi_t) - expected), tolerance, (c.phi_t, tolerance)
                )

    def test_2_the_comparison_that_binds_the_width(self):
        """
        Ruling 4: the field just above the width, at M/phi = 16.6 quanta (ln V - ln V_floor = 17
        quanta, finite), and the root at 17.5 quanta, within the first e-fold. The doubling's first
        probe brackets it; the halvings move the inner end to 1/2, 1/4 and 1/8 of an e-fold, and the
        fourth midpoint, 1/16 of an e-fold from the field, is compared with both ends: 17, 18 and 19
        quanta, all finite. That is the comparison K is derived from (log 07b §1). The tolerance is
        item 2's: here its rounding term, half a quantum in 17.5, is 0.029 in ln phi_t.
        """
        model = _model(2.0, 1e-16)
        M = 1e-16 * Mp
        q = ulp(abs(LOG_V_FLOOR))
        phi = M / (16.6 * q)
        log_E = LOG_V_FLOOR + log(expm1(17.5 * q))

        probes = []
        evaluate = AM._evaluate

        def spy(residual, x, context):
            r = evaluate(residual, x, context)
            probes.append((x, r))
            return r

        with mock.patch.object(AM, "_evaluate", spy):
            c = model.classical_turning_point(LOG_V_FLOOR, phi, log_E)

        # the probes, in quanta of ln V - ln V_floor: the field, the doubling's first probe, then the
        # four midpoints of the halvings, before brentq
        quanta = [
            (model._log_V(LOG_V_FLOOR, exp(x)) - LOG_V_FLOOR) / q for x, _ in probes
        ]
        self.assertEqual(quanta[:6], [17.0, 45.0, 27.0, 21.0, 19.0, 18.0])
        x_0 = probes[0][0]
        self.assertAlmostEqual(probes[5][0] - x_0, -1.0 / 16.0, delta=1e-14)
        for x, r in probes[:6]:
            self.assertGreater(r, float("-inf"), x)
        self.assertLess(probes[0][1], probes[5][1])
        self.assertLess(probes[5][1], probes[4][1])
        self.assertGreater(quanta[0], K)

        expected = _closed_form_log_phi_t(M, LOG_V_FLOOR, log_E)
        tolerance = _log_phi_t_tolerance(M, LOG_V_FLOOR, log_E)
        self.assertLessEqual(abs(log(c.phi_t) - expected), tolerance)


class TestAHistoryInTheBand(unittest.TestCase):
    """
    (3) M = 1e-16 M_P, beta = 2, alpha = 0.1, test_injection's setup (n = 1, phi* = 5 M_P, pi* = 0,
    T* = 2e4 GeV, to T_CMB, default tolerances, the model's seed realization, untuned). At the
    dispatch commit it fails at N = 20.345, at its first infall, in the wall search. About 7 s at a
    load of about 8.
    """

    @classmethod
    def setUpClass(cls):
        with contextlib.redirect_stdout(io.StringIO()):
            cosmology = gth.make_cosmology()
        units = cosmology.units
        model = _model(2.0, 1e-16)
        seed = model.seed(cosmology)
        realized = model.realize(seed.normalization, seed.phi_ref)
        captured = []
        inner = SM.integrate_scalar_history

        def capturing(RHS, *args, **kwargs):
            result = inner(RHS, *args, **kwargs)
            captured.append(result)
            return result

        with mock.patch.object(
            SM, "integrate_scalar_history", capturing
        ), contextlib.redirect_stdout(io.StringIO()):
            cls.data = SM.integrate_history(
                cosmology,
                temperature(0, 2.0e4 * units.GeV),
                temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
                phi_value(0, 5.0 * units.PlanckMass),
                pi_value(0, 0.0),
                gth.make_z_grid(),
                realized,
                task_label="test_wall_rounding",
                alpha=0.1,
            )
        cls.result = captured[0] if captured else None
        cls.T_CMB = cosmology.T_CMB_Kelvin * units.Kelvin

    def test_3_it_reaches_T_CMB(self):
        self.assertFalse(
            self.data.get("failure", False), self.data.get("failure_reason")
        )
        self.assertAlmostEqual(self.result.final_state.log_T_Jordan, log(self.T_CMB), 9)

    def test_3_it_parks_at_candidate_48_a_floor_reflection(self):
        self.assertFalse(
            self.data.get("failure", False), self.data.get("failure_reason")
        )
        p = self.result.parking
        self.assertIsNotNone(p)
        self.assertEqual(p.candidate_index, 48)
        self.assertEqual(self.result.injections[48].kind, "floor")
        self.assertAlmostEqual(p.N, 30.349, 3)
        self.assertIsNone(self.result.release)

    def test_3_the_drain_is_log_06s(self):
        """
        Sigma Delta rho/rho_R over the candidates, with the leftover, against log 06 §5.4's value at
        M = 1e-30 M_P: within its M-dependence at beta = 2 (2.5e-8 relative) and half a unit of the
        reference's last printed digit (5e-10) (orchestrator note 07b, ruling 6). Since electroweak-eos
        prompt 01 the value is that measurement remade on the new w table.
        """
        self.assertFalse(
            self.data.get("failure", False), self.data.get("failure_reason")
        )
        r = self.result
        total = sum(x.delta_rho_over_rho_rad for x in r.injections)
        total += r.parking.delta_rho_over_rho_rad
        bound = (
            SIGMA_PLUS_LEFTOVER_AT_1E_30 * SIGMA_M_DEPENDENCE + SIGMA_HALF_LAST_DIGIT
        )
        self.assertLessEqual(
            abs(total - SIGMA_PLUS_LEFTOVER_AT_1E_30), bound, (total, bound)
        )


if __name__ == "__main__":
    unittest.main()
