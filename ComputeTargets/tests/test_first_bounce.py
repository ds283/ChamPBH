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
The first bounce of a history (science-readiness prompt 03, README §0.2 P5, §2 (g), §6.4).

`first_bounce(result)` walks the accepted steps of an `IntegrationResult` and returns the first
negative-to-positive turning point of pi, at the root of pi on that step's interpolant, or the
first elastic reflection if that comes earlier, or None. Each test drives
`integrate_scalar_history` from one of the integrator audit's probe states with the helpers of
`test_kinematic_cap_loop` (`integrate`, `wall_bounces`, `interpolated_minima`). No Ray cluster,
no datastore, no PRyMordial solve. The round trip through a store is in
`Datastore/tests/test_first_bounce_round_trip.py`.

(d) checks P5's premise: on every window it is run on, the first negative-to-positive turning
point is the first wall bounce of the test helper's phi < 1.5 M rule, on the same step.

On `HEAD~1` there is no `first_bounce`, and every test here fails.
"""

import unittest
from bisect import bisect_right

import ComputeTargets.tests.test_kinematic_cap_loop as kcl

SM = kcl.SM


def rel(a: float, b: float) -> float:
    return abs(a - b) / abs(b)


class TestFirstBounce(unittest.TestCase):
    def test_a_P1_window_at_M_0p5(self):
        """(a) P1 (beta = 2, M = 0.5) to N = 21; about 0.2 s."""
        result, _ = kcl.integrate(kcl.P1, 0.5, 21.0)
        bounce = SM.first_bounce(result)

        self.assertIsInstance(bounce, SM.FirstBounce)
        self.assertFalse(bounce.reflected)
        self.assertLessEqual(abs(bounce.N - 20.343028), 1e-5)
        self.assertLessEqual(rel(bounce.phi_Einstein, 4.57371e-3), 1e-4)

        # the dense-output minimum of the test helper, located the same way
        N_min, phi_min = kcl.interpolated_minima(result, 0.5)[0]
        self.assertLessEqual(abs(bounce.N - N_min), 1e-12)
        self.assertLessEqual(rel(bounce.phi_Einstein, phi_min), 1e-12)

        # ln T_J is the state's, at the bounce
        self.assertEqual(bounce.log_T_Jordan, result.solution(bounce.N)[4])

    def test_b_reflection_at_M_1em10(self):
        """(b) P1 at M = 1e-10: one elastic reflection, which is the first bounce; about 0.2 s."""
        result, _ = kcl.integrate(kcl.P1, 1e-10, 21.0)
        self.assertEqual(len(result.reflections), 1)
        reflection = result.reflections[0]

        bounce = SM.first_bounce(result)
        self.assertIsInstance(bounce, SM.FirstBounce)
        self.assertTrue(bounce.reflected)
        self.assertEqual(bounce.N, reflection.N)
        self.assertGreaterEqual(bounce.phi_Einstein, 1e-11)
        self.assertLessEqual(bounce.phi_Einstein, 1e-10)

        # the state is read from the step that ends at the reflection: it is the accepted state
        # at which the reflection was made, to rounding in the interpolant
        self.assertLessEqual(rel(bounce.phi_Einstein, reflection.phi_Einstein), 1e-9)
        ts = result.solution.ts
        k = list(ts).index(reflection.N) - 1
        self.assertGreaterEqual(k, 0)
        self.assertEqual(
            bounce.log_T_Jordan,
            result.solution.interpolants[k](reflection.N)[4],
        )

        # no negative-to-positive turning point comes before it
        for j in range(k + 1):
            a, b = ts[j], ts[j + 1]
            interpolant = result.solution.interpolants[j]
            self.assertFalse(interpolant(a)[1] < 0.0 < interpolant(b)[1])

    def test_c_no_bounce(self):
        """(c) P2 (pi > 0, moving outwards) from N0 to N0 + 0.01: no bounce; well under a second."""
        N0 = kcl.P2[1]
        result, _ = kcl.integrate(kcl.P2, 0.5, N0 + 0.01)
        self.assertEqual(len(result.reflections), 0)
        self.assertIsNone(SM.first_bounce(result))

    def check_agrees_with_wall_bounces(self, probe, M: float, N_stop: float):
        result, _ = kcl.integrate(probe, M, N_stop)
        bounce = SM.first_bounce(result)
        self.assertIsNotNone(bounce)
        self.assertFalse(bounce.reflected)

        # the step first_bounce lies on
        ts = result.solution.ts
        k = bisect_right(list(ts), bounce.N) - 1
        self.assertLess(ts[k], bounce.N)
        self.assertLess(bounce.N, ts[k + 1])

        # wall_bounces records the turn at the accepted state that ends the same step
        walls = kcl.wall_bounces(result, M)
        self.assertGreaterEqual(len(walls), 1)
        self.assertEqual(walls[0][0], ts[k + 1])

        # and it is a wall bounce under the helper's rule, and the helper's dense-output minimum
        self.assertLess(bounce.phi_Einstein, 1.5 * M)
        N_min, phi_min = kcl.interpolated_minima(result, M)[0]
        self.assertEqual(bounce.N, N_min)
        self.assertEqual(bounce.phi_Einstein, phi_min)

    def test_d_agrees_with_wall_bounces_P1_M_0p5(self):
        """(d) P1 at M = 0.5 to N = 21; about 0.2 s."""
        self.check_agrees_with_wall_bounces(kcl.P1, 0.5, 21.0)

    def test_d_agrees_with_wall_bounces_P1_M_0p01(self):
        """(d) P1 at M = 0.01 to N = 21; about 0.2 s."""
        self.check_agrees_with_wall_bounces(kcl.P1, 0.01, 21.0)

    def test_d_agrees_with_wall_bounces_P1_M_0p001(self):
        """(d) P1 at M = 0.001 to N = 21; about 0.2 s."""
        self.check_agrees_with_wall_bounces(kcl.P1, 0.001, 21.0)

    def test_d_agrees_with_wall_bounces_P3(self):
        """(d) P3 (beta = 1.2, M = 0.01) to N = 37.5: 38 547 RHS, about 2 s."""
        self.check_agrees_with_wall_bounces(kcl.P3, 0.01, 37.5)


if __name__ == "__main__":
    unittest.main()
