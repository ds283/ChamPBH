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
The turning points of T_J before the first bounce (surfing-temperature prompt 02, T1 and T4.1).

`surfing_turning_points(result, coupling)` walks the accepted steps of an `IntegrationResult` up to
`first_bounce(result).N` and returns every zero of g = 1 + (d ln Omega/dphi) pi, with its
direction. These tests drive it on stub interpolants: an object with `solution.ts`,
`solution.interpolants` and `reflections`, as `first_bounce` reads them, and a stub coupling whose
d ln Omega/dphi is a constant D. Each stub chooses g(N) and sets pi = (g - 1)/D, so the zeros of
g, and the first bounce (g rising through 1, which is pi rising through 0), are known exactly.

The main stub is a damped oscillation of beta pi/M_P = g - 1 about -1,

    g(N) = 0.9 exp(-N/20) cos N            for N < 7 pi/2,
    g(N) = 2 cos N                         for N >= 7 pi/2,

continuous at 7 pi/2, where cos N = 0. g < 1 before 7 pi/2, so pi < 0 there; afterwards g rises
through 1 at cos N = 1/2, N_b = 11 pi/3, which is the first bounce. The zeros of g are at
(j + 1/2) pi: "min" for even j, "max" for odd j. Four lie before N_b, and the rest after it.

No integration runs; the whole module takes well under a second. The real history is in
`tools/tests/test_surfing_scan.py`.
"""

import importlib
import unittest
from math import cos, exp, pi
from types import SimpleNamespace

import numpy as np

# ComputeTargets re-exports the ScalarModel class under the module's name, so import the module
SM = importlib.import_module("ComputeTargets.ScalarModel")

D = 2.0  # the stub coupling's d ln Omega/dphi, as beta/M_P for beta = 2, M_P = 1


class StubCoupling:
    def d_logOmega_dphi(self, phi) -> float:
        return D


class StubInterpolant:
    """
    The state (phi, pi, ln rho_rad, ln f_m, ln T_J) of a stub history at t, for a scalar t or an
    array of t, as SciPy's dense output returns it. pi = (g - 1)/D; phi and ln T_J are smooth
    stand-ins (only pi decides the bounce and the crossings).
    """

    def __init__(self, g):
        self._g = g

    def __call__(self, t):
        t_arr = np.asarray(t, dtype=float)
        g = np.vectorize(self._g, otypes=[float])(t_arr)
        y = np.array(
            [
                1.0 + 0.01 * t_arr,
                (g - 1.0) / D,
                -t_arr,
                np.zeros_like(t_arr),
                -0.5 * t_arr,
            ]
        )
        return y


def stub_result(g, ts, reflection_Ns=()):
    interpolant = StubInterpolant(g)
    return SimpleNamespace(
        solution=SimpleNamespace(
            ts=np.array(ts, dtype=float),
            interpolants=[interpolant] * (len(ts) - 1),
        ),
        reflections=[
            SM.Reflection(N=N, phi_Einstein=1.0, pi_Einstein_in=-1.0)
            for N in reflection_Ns
        ],
    )


def g_damped(N: float) -> float:
    if N < 3.5 * pi:
        return 0.9 * exp(-N / 20.0) * cos(N)
    return 2.0 * cos(N)


# steps of 0.1 e-fold, the global cap, from 0 to 25
TS_MAIN = [k / 10.0 for k in range(251)]
N_BOUNCE = 11.0 * pi / 3.0
ZEROS_BEFORE = [
    (0.5 * pi, "min"),
    (1.5 * pi, "max"),
    (2.5 * pi, "min"),
    (3.5 * pi, "max"),
]
ZEROS_AFTER = [(4.5 * pi, "min"), (5.5 * pi, "max"), (6.5 * pi, "min")]


class TestSurfingTurningPoints(unittest.TestCase):
    def test_a_every_crossing_in_order_with_direction(self):
        """(a) the damped stub: the four zeros before N_b, in order, to 1e-12, with direction."""
        result = stub_result(g_damped, TS_MAIN)

        bounce = SM.first_bounce(result)
        self.assertIsNotNone(bounce)
        self.assertLessEqual(abs(bounce.N - N_BOUNCE), 1e-12)

        crossings = SM.surfing_turning_points(result, StubCoupling())
        self.assertEqual(len(crossings), len(ZEROS_BEFORE))
        for c, (N, direction) in zip(crossings, ZEROS_BEFORE):
            self.assertIsInstance(c, SM.SurfingCrossing)
            self.assertLessEqual(abs(c.N - N), 1e-12)
            self.assertEqual(c.direction, direction)
            # the state is the interpolant's at the crossing, and g vanishes there
            self.assertEqual(c.pi_Einstein, (g_damped(c.N) - 1.0) / D)
            self.assertEqual(c.log_T_Jordan, -0.5 * c.N)
            self.assertLessEqual(abs(1.0 + D * c.pi_Einstein), 1e-12)

    def test_b_none_after_the_first_bounce(self):
        """(b) the zeros after N_b are not reported; without the bounce they would be."""
        result = stub_result(g_damped, TS_MAIN)
        crossings = SM.surfing_turning_points(result, StubCoupling())
        self.assertTrue(all(c.N < N_BOUNCE for c in crossings))
        for N, _ in ZEROS_AFTER:
            self.assertFalse(any(abs(c.N - N) < 1e-6 for c in crossings))

        # the stub's g does have those zeros, on steps the walk does not reach
        for N, _ in ZEROS_AFTER:
            self.assertLess(abs(g_damped(N)), 1e-14)
            self.assertLess(N, TS_MAIN[-1])

    def test_c_two_zeros_inside_one_step(self):
        """(c) g = (N - 1.03)(N - 1.07): both zeros lie in the step [1.0, 1.1], g > 0 at both ends."""
        z1, z2 = 1.03, 1.07

        def g(N: float) -> float:
            return (N - z1) * (N - z2)

        ts = [k / 10.0 for k in range(31)]
        result = stub_result(g, ts)
        self.assertGreater(g(1.0), 0.0)
        self.assertGreater(g(1.1), 0.0)

        crossings = SM.surfing_turning_points(result, StubCoupling())
        self.assertEqual([c.direction for c in crossings], ["min", "max"])
        self.assertLessEqual(abs(crossings[0].N - z1), 1e-12)
        self.assertLessEqual(abs(crossings[1].N - z2), 1e-12)

        # and both are before the first bounce, where g rises through 1
        bounce = SM.first_bounce(result)
        self.assertIsNotNone(bounce)
        self.assertLess(crossings[1].N, bounce.N)

    def test_d_no_sign_change(self):
        """(d) g = 0.5 throughout: no crossing, no bounce, an empty list."""
        result = stub_result(lambda N: 0.5, TS_MAIN)
        self.assertIsNone(SM.first_bounce(result))
        self.assertEqual(SM.surfing_turning_points(result, StubCoupling()), [])

    def test_e_reflection_before_any_crossing(self):
        """(e) a reflection at N = 1.0, before the first zero (pi/2): the walk ends there, empty."""
        result = stub_result(g_damped, TS_MAIN, reflection_Ns=(1.0,))
        bounce = SM.first_bounce(result)
        self.assertTrue(bounce.reflected)
        self.assertEqual(bounce.N, 1.0)
        self.assertEqual(SM.surfing_turning_points(result, StubCoupling()), [])

    def test_f_reflection_between_crossings(self):
        """(f) a reflection at N = 3.0, between the first two zeros: only the first is reported."""
        result = stub_result(g_damped, TS_MAIN, reflection_Ns=(3.0,))
        crossings = SM.surfing_turning_points(result, StubCoupling())
        self.assertEqual(len(crossings), 1)
        self.assertLessEqual(abs(crossings[0].N - 0.5 * pi), 1e-12)
        self.assertEqual(crossings[0].direction, "min")

    def test_g_zero_at_a_step_boundary_counted_once(self):
        """(g) g = 2.5 - N on steps of 0.5: g is exactly zero at the node 2.5, counted once."""
        ts = [k / 2.0 for k in range(11)]
        result = stub_result(lambda N: 2.5 - N, ts)
        self.assertIsNone(SM.first_bounce(result))

        crossings = SM.surfing_turning_points(result, StubCoupling())
        self.assertEqual(len(crossings), 1)
        self.assertEqual(crossings[0].N, 2.5)
        self.assertEqual(crossings[0].direction, "min")

    def test_h_crossing_on_the_bounce_step(self):
        """
        (h) g = 50 (N - 1.05): a "max" at 1.05 and the bounce (g = 1) at 1.07, on the same step
        [1.0, 1.1]. The walk runs up to the bounce on its own step, as at the wall.
        """
        result = stub_result(lambda N: 50.0 * (N - 1.05), [k / 10.0 for k in range(31)])
        bounce = SM.first_bounce(result)
        self.assertLessEqual(abs(bounce.N - 1.07), 1e-12)

        crossings = SM.surfing_turning_points(result, StubCoupling())
        self.assertEqual(len(crossings), 1)
        self.assertLessEqual(abs(crossings[0].N - 1.05), 1e-12)
        self.assertEqual(crossings[0].direction, "max")

    def test_i_read_through_the_coupling(self):
        """(i) the coupling's d ln Omega/dphi is used: with half of it, g = 1 + pi has no zero."""

        class HalfCoupling:
            def d_logOmega_dphi(self, phi) -> float:
                return 0.5 * D

        # pi = (g_damped - 1)/D lies in [-0.95, 0), so 1 + (D/2) pi > 0.5 throughout
        result = stub_result(g_damped, TS_MAIN)
        self.assertEqual(SM.surfing_turning_points(result, HalfCoupling()), [])


if __name__ == "__main__":
    unittest.main()
