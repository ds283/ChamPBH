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
The wall search's unresolvable turning points (one-loop-drain prompt 01 §1.6, §1.8; README §8.8 P21;
orchestrator note 01, ruling 6).

`_log_V_minus_floor` returns -inf within WALL_ROUNDING_QUANTA quanta of ln V_floor (inelastic-reflection
prompt 07b). A classical turning point whose ln E lies below the least finite value it can return sits
inside the region the search reads as -inf: every finite residual is positive, the outer end stays -inf,
and the search raises. `_turning_point_unresolvable(log_V_floor, log_E)` says so, and
`classical_turning_point` now returns None there, after its own r_0 >= 0 test and before the search.
The items are those of prompt §1.8:

  (1) the predicate at its boundary, at the laboratory-M and the 10^-12 M_P normalizations: the last ln E
      for which it is True and the first for which it is False, with the least finite value found by a
      walk of this module's own;
  (2) where it is True, `_search_log_field` on the same residual raises, called directly, and so does a
      copy of the method as it was before the fix (`classical_turning_point_before_fix`, kept here);
  (3) where it is False, `classical_turning_point` returns what the copy returns, bitwise, or raises
      with the copy's message: at the boundary and above it, and at the twelve inputs of
      test_wall_rounding that the shipped search refused;
  (4) the 19 failing inputs of orchestrator note 01 (2197 and 2188 from the drain probe's logs, the nine
      stored failures of the 2026.8.0 store and the eight failures of the nine on the fixed tree), as
      literals: each now returns None with the predicate holding, and each raises with the copy;
  (5) the cache.

No store, no Ray, no integration: milliseconds.
"""

import importlib
import sys
import time
import unittest
from math import exp, inf, isfinite, log, nextafter

from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests.test_wall_rounding import BAND_SHIPPED
from CosmologyConcepts import M_value, beta_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from Units import Planck_units

AM = importlib.import_module("CosmologyConcepts.Models.AbstractModel")

UNITS = Planck_units()
Mp = UNITS.PlanckMass

# the normalizations of the boundary tests: 2197's stored one (the seed's at M = 4e-31 M_P, the
# laboratory M), and 3233's (M = 1e-12 M_P, beta = 1.0), both read from the 2026.8.0 store by repr
LAB_NORMALIZATION = -276.63072859401797
LARGE_M_NORMALIZATION = -276.63073574674894

# the 19 inputs measured to raise (orchestrator note 01, ruling 6): (source, beta, M/M_P, normalization,
# phi in M_P, ln E), each a repr, so that each literal is the float of its source
#   - "probe": the drain probe's logs (reflection-restart close/failing_call.py, PROBE), on 2d50b95;
#   - "stored": the stored failure_reason of the nine of reflection-restart 04 §0.2 in the 2026.8.0 store
#     (made before reflection-restart; read read-only by .documents/one-loop-drain/law/failing_inputs.py,
#     which shows that these literals equal the store's);
#   - "fixed tree": the eight of the nine that fail on the fixed tree (var/reflection-restart/04/
#     the-nine.jsonl; close/out/nine_margins.txt, "this tree")
FAILING_INPUTS = (
    (
        "probe 2197",
        1.185,
        4e-31,
        -276.63072859401797,
        2.950090845531173e-07,
        -306.7166549210319,
    ),
    (
        "probe 2188",
        1.06,
        4e-31,
        -276.63072859401797,
        2.647733963294608e-07,
        -306.6698578858307,
    ),
    (
        "stored shard0002#1592",
        1.1775,
        4e-31,
        -276.63072859401797,
        6.143779548042237e-10,
        -308.6129970063558,
    ),
    (
        "stored shard0003#2208",
        1.18,
        4e-31,
        -276.63072859401797,
        6.242270073944803e-10,
        -309.37538319488175,
    ),
    (
        "stored shard0004#2199",
        1.1825,
        4e-31,
        -276.63072859401797,
        6.112859129007901e-10,
        -308.6585130622487,
    ),
    (
        "stored shard0000#1001",
        0.75,
        4e-31,
        -276.63072859401797,
        8.943323416413437e-09,
        -304.3236804061682,
    ),
    (
        "stored shard0001#766",
        0.9,
        4e-31,
        -276.63072859401797,
        3.7072307387094975e-08,
        -306.2985617947364,
    ),
    (
        "stored shard0015#403",
        0.5,
        4e-31,
        -276.63072859401797,
        1.4103964409699267e-08,
        -308.8526551068955,
    ),
    (
        "stored shard0000#2735",
        0.75,
        1e-33,
        -276.63072859401797,
        5.2012071586237315e-08,
        -305.91588537339726,
    ),
    (
        "stored shard0015#2480",
        0.5,
        1e-33,
        -276.63072859401797,
        4.5197701189944676e-08,
        -308.24395716177537,
    ),
    (
        "stored shard0015#2386",
        0.5,
        1e-30,
        -276.63072859401797,
        1.2947244448110153e-08,
        -306.8183703090743,
    ),
    (
        "fixed tree shard0000#1001",
        0.75,
        4e-31,
        -276.63072859401797,
        8.565111714898026e-08,
        -306.3563620503721,
    ),
    (
        "fixed tree shard0001#766",
        0.9,
        4e-31,
        -276.63072859401797,
        1.7971701865103972e-07,
        -307.5556578621605,
    ),
    (
        "fixed tree shard0015#403",
        0.5,
        4e-31,
        -276.63072859401797,
        6.909460075895113e-08,
        -305.33592552406515,
    ),
    (
        "fixed tree shard0000#2735",
        0.75,
        1e-33,
        -276.63072859401797,
        8.56511209635465e-08,
        -306.3567396216211,
    ),
    (
        "fixed tree shard0015#2480",
        0.5,
        1e-33,
        -276.63072859401797,
        6.909457787672749e-08,
        -305.28832358387046,
    ),
    (
        "fixed tree shard0002#1592",
        1.1775,
        4e-31,
        -276.63072859401797,
        2.926694256797149e-07,
        -305.3632323353481,
    ),
    (
        "fixed tree shard0003#2208",
        1.18,
        4e-31,
        -276.63072859401797,
        2.934558128332355e-07,
        -306.87941516628615,
    ),
    (
        "fixed tree shard0004#2199",
        1.1825,
        4e-31,
        -276.63072859401797,
        2.938921526353341e-07,
        -305.2276525761867,
    ),
)

# the two messages with which the search raises where the predicate holds (§0.3)
RAISES_WITH = (
    "the searched quantity is not monotone along the search",
    "the bracket's outer end is still -inf",
)


def make_model(beta: float, M_over_Mp: float) -> ExponentialModel:
    """The exponential model as the scanner and the probe build it (n = 1, Planck units)."""
    return ExponentialModel(
        0, M_value(0, M_over_Mp * Mp), 1, beta_value(0, beta), UNITS
    )


def classical_turning_point_before_fix(model, normalization, phi: float, log_E: float):
    """
    AbstractModel.classical_turning_point as it was on the merged tree (1561b60), before this prompt:
    the method's body verbatim, with self written as model and the module's names qualified by AM.
    """
    context = model._wall_context(
        "classical_turning_point", normalization, phi=phi, log_E=log_E
    )
    log_V_floor = model._wall_floor(context, normalization, phi, log_E)

    def residual(p: float) -> float:
        return (
            AM._log_V_minus_floor(model._log_V(normalization, p), log_V_floor) - log_E
        )

    x_0 = log(phi)
    r_0 = AM._evaluate(residual, x_0, context)
    if r_0 >= 0.0:
        return None

    phi_t = exp(AM._search_log_field(residual, x_0, r_0, -1.0, context))

    d1 = model._d_logV_dphi(normalization, phi_t)
    q = model._d2_logV_dphi2(normalization, phi_t) + d1 * d1
    if not q > 0.0:
        raise AM._failure(
            f"{context}: V'' <= 0 at the classical turning point phi_t = {phi_t!r} "
            f"((ln V)'' + (ln V)'^2 = {q!r}); a reflecting wall is convex there"
        )
    lambda_t = -d1
    if not lambda_t > 0.0:
        raise AM._failure(
            f"{context}: -d ln V/dphi = {lambda_t!r} <= 0 at phi_t = {phi_t!r}; "
            f"the wall is not repulsive there"
        )
    log_omega_t_squared = model._log_V(normalization, phi_t) + log(q)
    log_k_ex = log(lambda_t) + 0.5 * log_E - AM._LOG_2
    result = AM.ClassicalTurningPoint(
        phi_t=phi_t,
        log_omega_t_squared=log_omega_t_squared,
        log_R=2.0 * log_omega_t_squared - AM._LOG_8_PI_SQUARED - log_E,
        lambda_t=lambda_t,
        log_k_ex=log_k_ex,
        ell_t=log_k_ex - 0.5 * log_omega_t_squared,
    )
    if not all(isfinite(v) for v in result):
        raise AM._failure(f"{context}: a non-finite value in {result!r}")
    return result


def outcome(call):
    """("returned", the result as a tuple of float.hex, or None) or ("raised", the message)."""
    try:
        result = call()
    except ComputationFailureError as e:
        return ("raised", e.message)
    if result is None:
        return ("returned", None)
    return ("returned", tuple(float(v).hex() for v in result))


def least_finite(log_V_floor: float):
    """
    The least finite value of _log_V_minus_floor(., log_V_floor) and the number of steps taken, by this
    module's own walk up from ln V_floor through the representable floats.
    """
    log_V = log_V_floor
    steps = 0
    while True:
        log_V = nextafter(log_V, inf)
        steps += 1
        value = AM._log_V_minus_floor(log_V, log_V_floor)
        if value > -inf:
            return value, log_V, steps


class TestThePredicateAtItsBoundary(unittest.TestCase):
    """(1) and (2) at the two normalizations."""

    NORMALIZATIONS = (LAB_NORMALIZATION, LARGE_M_NORMALIZATION)

    def test_1_the_last_true_and_the_first_false(self):
        for log_V_floor in self.NORMALIZATIONS:
            with self.subTest(log_V_floor=log_V_floor):
                least, _, steps = least_finite(log_V_floor)
                self.assertLessEqual(steps, AM.WALL_ROUNDING_QUANTA + 2)
                self.assertTrue(
                    AM._turning_point_unresolvable(log_V_floor, nextafter(least, -inf))
                )
                self.assertFalse(AM._turning_point_unresolvable(log_V_floor, least))
                self.assertFalse(
                    AM._turning_point_unresolvable(log_V_floor, nextafter(least, inf))
                )
                # far below and far above
                self.assertTrue(
                    AM._turning_point_unresolvable(log_V_floor, least - 50.0)
                )
                self.assertFalse(
                    AM._turning_point_unresolvable(log_V_floor, least + 50.0)
                )

    def test_1_the_least_value_is_the_first_finite_one(self):
        """_log_V_minus_floor does not fall below its first finite value over the next 10^4 floats."""
        for log_V_floor in self.NORMALIZATIONS:
            with self.subTest(log_V_floor=log_V_floor):
                least, log_V, _ = least_finite(log_V_floor)
                previous = least
                for _ in range(10_000):
                    log_V = nextafter(log_V, inf)
                    value = AM._log_V_minus_floor(log_V, log_V_floor)
                    self.assertGreaterEqual(value, previous)
                    previous = value

    def test_2_where_it_holds_the_search_raises(self):
        """
        The residual of classical_turning_point at a field far out in the tail (ln V rounds to ln V_floor,
        r_0 = -inf), at ln E just below the boundary and further below: _search_log_field raises,
        called directly, with one of the two messages, and the pre-fix copy raises the same.
        """
        cases = (
            (LAB_NORMALIZATION, make_model(1.185, 4e-31), 2.950090845531173e-07),
            (LARGE_M_NORMALIZATION, make_model(1.0, 1e-12), 1.0e3),
        )
        for log_V_floor, model, phi in cases:
            least, _, _ = least_finite(log_V_floor)
            for log_E in (
                nextafter(least, -inf),
                least - 1e-12,
                least - 1.0,
                least - 30.0,
            ):
                with self.subTest(log_V_floor=log_V_floor, log_E=log_E):
                    self.assertTrue(AM._turning_point_unresolvable(log_V_floor, log_E))

                    def residual(p: float) -> float:
                        return (
                            AM._log_V_minus_floor(
                                model._log_V(log_V_floor, p), log_V_floor
                            )
                            - log_E
                        )

                    x_0 = log(phi)
                    r_0 = AM._evaluate(residual, x_0, "test")
                    self.assertEqual(r_0, -inf)
                    with self.assertRaises(ComputationFailureError) as caught:
                        AM._search_log_field(residual, x_0, r_0, -1.0, "test")
                    self.assertTrue(
                        any(m in caught.exception.message for m in RAISES_WITH),
                        caught.exception.message,
                    )
                    kind, message = outcome(
                        lambda: classical_turning_point_before_fix(
                            model, log_V_floor, phi, log_E
                        )
                    )
                    self.assertEqual(kind, "raised")
                    self.assertTrue(any(m in message for m in RAISES_WITH), message)
                    self.assertIsNone(
                        model.classical_turning_point(log_V_floor, phi, log_E)
                    )


class TestWhereItDoesNotHold(unittest.TestCase):
    """(3): the method is the pre-fix copy, bitwise, wherever the predicate is False."""

    def test_3_at_and_above_the_boundary(self):
        cases = (
            (LAB_NORMALIZATION, make_model(1.185, 4e-31), 2.950090845531173e-07),
            (LARGE_M_NORMALIZATION, make_model(1.0, 1e-12), 1.0e3),
        )
        for log_V_floor, model, phi in cases:
            least, _, _ = least_finite(log_V_floor)
            for log_E in (
                least,
                nextafter(least, inf),
                least + 1e-12,
                least + 1.0,
                least + 30.0,
                log_V_floor,
            ):
                with self.subTest(log_V_floor=log_V_floor, log_E=log_E):
                    self.assertFalse(AM._turning_point_unresolvable(log_V_floor, log_E))
                    self.assertEqual(
                        outcome(
                            lambda: model.classical_turning_point(
                                log_V_floor, phi, log_E
                            )
                        ),
                        outcome(
                            lambda: classical_turning_point_before_fix(
                                model, log_V_floor, phi, log_E
                            )
                        ),
                    )

    def test_3_the_twelve_inputs_test_wall_rounding_pins(self):
        for beta, M, normalization, phi, log_E in BAND_SHIPPED:
            with self.subTest(beta=beta, M=M):
                model = make_model(beta, M)
                self.assertFalse(AM._turning_point_unresolvable(normalization, log_E))
                now = outcome(
                    lambda: model.classical_turning_point(
                        normalization, phi * Mp, log_E
                    )
                )
                self.assertEqual(now[0], "returned")
                self.assertIsNotNone(now[1])
                self.assertEqual(
                    now,
                    outcome(
                        lambda: classical_turning_point_before_fix(
                            model, normalization, phi * Mp, log_E
                        )
                    ),
                )


class TestTheNineteenFailingInputs(unittest.TestCase):
    """(4), orchestrator note 01, ruling 6."""

    def test_4_there_are_nineteen(self):
        self.assertEqual(len(FAILING_INPUTS), 19)
        self.assertEqual(len({row[0] for row in FAILING_INPUTS}), 19)

    def test_4_each_returns_None_now_with_the_predicate_holding(self):
        for source, beta, M, normalization, phi, log_E in FAILING_INPUTS:
            with self.subTest(source=source):
                model = make_model(beta, M)
                self.assertEqual(model._log_V_floor(normalization), normalization)
                self.assertTrue(AM._turning_point_unresolvable(normalization, log_E))
                self.assertIsNone(
                    model.classical_turning_point(normalization, phi * Mp, log_E)
                )
                realized = model.realize(normalization, 0.0)
                self.assertIsNone(realized.classical_turning_point(phi * Mp, log_E))

    def test_4_each_raises_with_the_pre_fix_copy(self):
        for source, beta, M, normalization, phi, log_E in FAILING_INPUTS:
            with self.subTest(source=source):
                kind, message = outcome(
                    lambda: classical_turning_point_before_fix(
                        make_model(beta, M), normalization, phi * Mp, log_E
                    )
                )
                self.assertEqual(kind, "raised")
                self.assertTrue(
                    message.startswith("classical_turning_point ("), message
                )
                self.assertTrue(any(m in message for m in RAISES_WITH), message)


class TestTheCache(unittest.TestCase):
    """(5): the least value is computed once per ln V_floor and WALL_ROUNDING_QUANTA."""

    def test_5_a_second_call_does_not_walk_again(self):
        log_V_floor = LAB_NORMALIZATION
        AM._turning_point_unresolvable(log_V_floor, 0.0)
        calls = []
        original = AM._log_V_minus_floor

        def counting(*args):
            calls.append(args)
            return original(*args)

        AM._log_V_minus_floor = counting
        try:
            for _ in range(100):
                AM._turning_point_unresolvable(log_V_floor, -300.0)
        finally:
            AM._log_V_minus_floor = original
        self.assertEqual(calls, [])

    def test_5_the_cached_value_is_the_walks(self):
        for log_V_floor in (LAB_NORMALIZATION, LARGE_M_NORMALIZATION, -1.0, 2.5):
            with self.subTest(log_V_floor=log_V_floor):
                least, _, _ = least_finite(log_V_floor)
                self.assertEqual(AM._least_finite_log_V_minus_floor(log_V_floor), least)

    def test_5_a_call_costs_microseconds(self):
        AM._turning_point_unresolvable(LAB_NORMALIZATION, 0.0)
        start = time.perf_counter()
        for _ in range(10_000):
            AM._turning_point_unresolvable(LAB_NORMALIZATION, -300.0)
        per_call = (time.perf_counter() - start) / 10_000
        self.assertLess(per_call, 1e-4)


if __name__ == "__main__":
    unittest.main()
