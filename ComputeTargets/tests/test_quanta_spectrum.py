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
The spectrum model and its kernels (one-loop-drain prompt 05 §1.1, §1.2, §1.7; rewritten for the measured shape by wall by
power-law-wall prompt 03 §1.3, §3; README §6.5 R8, §6.6 R9, R10, P19, P20; orchestrator note 03, ruling 5), on each of
the three walls:

- the walls and their names: spectrum(wall, ...) needs the wall, an unknown wall is refused naming the known ones, and
  each wall's SPECTRUM_MODELS string is its spectrum's model;
- the tables' ends: s_dir = x^2 below x_lo, continuous there; the matched exponential above x_hi, with s and d ln s/dx
  continuous at x_hi; the spline through the table's nodes; s_dir <= x^2 above x_lo (the Omega asymptote's bound);
- x_p, the spline's maximum, against its derivative and the nodes;
- the associated piece's form (1/x to x_p, then (C_1/x) s_dir/s_dir(x_p); R10) and coefficient C_1 = 49 x_* s_dir(x_*),
  so that s(x_*) = 50 s_dir(x_*) at c_a = 1;
- I, J and the moments against direct quadrature, to 1e-12;
- G and Omega at grid nodes, between nodes, at each end of the grid and beyond each end, against direct quadrature
  (scipy's adaptive quad on the pieces between the shape's breaks), to 1e-7 relative (ruling 5), at c_a = 1, 0 and 10;
  the quadratures' own stated error below 1e-9;
- the asymptotes, term by term at the grid's ends; the Omega asymptote's remainder bound at OMEGA_U_MAX; Omega's
  symmetry e^{4u} Omega(-u);
- the peak-collapsed spectrum (unchanged).

A few seconds a wall and scale (each pair's two tables take about 2 s to build; the quadratures are adaptive); about a
minute in all.
"""

import unittest
from math import exp, log, sqrt

import numpy as np

from ComputeTargets import quanta_spectrum as qs

SCALES = (1.0, 0.0, 10.0)
TOL = 1e-7
QUAD_TOL = 1e-9


def g_points():
    """ln y: two grid nodes, two midpoints, each end, a decade beyond each end, and far beyond."""
    lo, hi = log(qs.G_Y_MIN), log(qs.G_Y_MAX)
    h = qs.G_LOG_Y_STEP
    return [
        lo + 100 * h,
        lo + 731 * h,
        lo + 100.5 * h,
        lo + 731.5 * h,
        lo,
        hi,
        lo - log(10.0),
        hi + log(10.0),
        lo - log(1e4),
        hi + log(1e3),
    ]


def omega_points():
    """u: nodes, midpoints, 0, each end of the table (+-U), a decade beyond each end (+-(U + ln 10)), and beyond."""
    U, h = qs.OMEGA_U_MAX, qs.OMEGA_U_STEP
    return [
        0.0,
        37 * h,
        37.5 * h,
        401.5 * h,
        -37.5 * h,
        -401.5 * h,
        899.5 * h,
        U,
        -U,
        U + log(10.0),
        -(U + log(10.0)),
        U + 6.0,
        -(U + 6.0),
    ]


class TestTheWalls(unittest.TestCase):
    def test_the_walls_and_their_models(self):
        self.assertEqual(qs.WALLS, ("exponential", "power-n1", "power-n2"))
        self.assertEqual(len(set(qs.SPECTRUM_MODELS.values())), 3)
        for w in qs.WALLS:
            with self.subTest(wall=w):
                self.assertEqual(qs.spectrum_model(w), qs.SPECTRUM_MODELS[w])
                self.assertIn(w, qs.SPECTRUM_MODELS[w])
                self.assertNotEqual(qs.SPECTRUM_MODELS[w], "U4-U17-v1")
                s = qs.spectrum(w, 1.0)
                self.assertEqual(s.model, qs.SPECTRUM_MODELS[w])
                self.assertEqual(s.wall, w)
                self.assertIs(qs.spectrum(w), s)  # the default scale is 1, cached

    def test_an_unknown_wall_is_refused(self):
        for bad in ("exp", "power-n3", 1.0, None):
            with self.subTest(wall=bad):
                with self.assertRaises(ValueError) as ctx:
                    qs.spectrum(bad, 1.0)
                for w in qs.WALLS:
                    self.assertIn(w, str(ctx.exception))
        with self.assertRaises(ValueError):
            qs.spectrum_model("power-n3")
        with self.assertRaises(TypeError):
            qs.spectrum()  # the wall has no default (P19)

    def test_a_negative_scale_is_refused(self):
        with self.assertRaises(ValueError):
            qs.spectrum(qs.WALL_EXPONENTIAL, -1.0)
        with self.assertRaises(ValueError):
            qs.QuantaSpectrum(qs.WALL_POWER_N1, float("nan"))

    def test_the_removed_constants(self):
        """The analytic shape's constants went with U4-U17-v1 (prompt §1.3)."""
        for name in (
            "X_PEAK",
            "KAPPA",
            "KAPPA_BY_L",
            "BETA2_MAX_BY_L",
            "kappa_fit",
            "SPECTRUM_MODEL",
        ):
            self.assertFalse(hasattr(qs, name), name)
        # x_* in the one k_ex (power-law-wall prompt 07c, P104): 0.0029 omega_t over the exponential reference's
        # k_ex/omega_t, 0.5163952511757223; 0.0058 before, over k_ex = omega_t/2
        self.assertEqual(qs.X_STAR, 0.005615853347600149)
        self.assertAlmostEqual(qs.X_STAR, 0.0029 / 0.5163952511757223, places=17)
        self.assertEqual(qs.ASSOC_COEFFICIENT, 49.0)


class TestTheTables(unittest.TestCase):
    """The tables' ends, the spline, x_p (R8)."""

    def test_the_table_reference_points(self):
        t = qs._TABLES
        # in the one k_ex, E^{1/2}|V'''/V''|/2 (power-law-wall prompt 07c, 07b's spectrum_tables.txt); on the power wall
        # (n + 2)/(2 sqrt(n(n + 1)))
        self.assertEqual(t["exponential"].k_ex_over_omega_t, 0.5163952511757223)
        self.assertEqual(t["power-n1"].k_ex_over_omega_t, 1.0606601717798214)
        self.assertEqual(t["power-n2"].k_ex_over_omega_t, 0.816496580927726)
        for w, n in (("power-n1", 1), ("power-n2", 2)):
            self.assertAlmostEqual(
                t[w].k_ex_over_omega_t, (n + 2) / (2.0 * sqrt(n * (n + 1))), places=15
            )
        for w in qs.WALLS:
            with self.subTest(wall=w):
                # the lower edge is k/omega_t = 1e-3 (R8)
                self.assertAlmostEqual(
                    t[w].x_lo * t[w].k_ex_over_omega_t, 1e-3, places=17
                )
                self.assertEqual(
                    len(t[w].ln_beta2),
                    {"exponential": 151, "power-n1": 163, "power-n2": 158}[w],
                )
        self.assertEqual(
            (t["exponential"].x_hi, t["power-n1"].x_hi, t["power-n2"].x_hi),
            (3.5, 3.0, 3.0),
        )

    def test_x_squared_below_the_table(self):
        for w in qs.WALLS:
            d = qs._DIRECT[w]
            with self.subTest(wall=w):
                for x in (1e-9, 1e-4, 0.5 * d.x_lo, d.x_lo):
                    self.assertEqual(float(qs.s_dir(w, x)), x * x)
                above = np.nextafter(d.x_lo, 1.0)
                self.assertLess(abs(float(qs.s_dir(w, above)) / d.x_lo**2 - 1.0), 1e-14)
                # the spline passes through the first node, normalized to x_lo^2
                self.assertLess(
                    abs(exp(float(d.spline(d.t_lo))) / d.x_lo**2 - 1.0), 1e-14
                )

    def test_the_matched_tail_above_the_table(self):
        """s and d ln s/dx continuous at x_hi; the exponential beyond with kappa_hi from the spline's end slope."""
        for w in qs.WALLS:
            d = qs._DIRECT[w]
            with self.subTest(wall=w):
                k = d.kappa_hi
                self.assertEqual(k, -float(d.spline(d.t_hi, 1)) / d.x_hi)
                below = float(qs.s_dir(w, np.nextafter(d.x_hi, 0.0)))
                at = float(qs.s_dir(w, d.x_hi))
                self.assertLess(abs(below / at - 1.0), 1e-13)
                for dx in (0.1, 1.0, 10.0):
                    self.assertLess(
                        abs(
                            float(qs.s_dir(w, d.x_hi + dx)) / (at * exp(-k * dx)) - 1.0
                        ),
                        1e-14,
                    )
                # the slope from each side, by one-sided differences of s itself
                h = 1e-6 * d.x_hi
                left = (log(at) - log(float(qs.s_dir(w, d.x_hi - h)))) / h
                right = (log(float(qs.s_dir(w, d.x_hi + h))) - log(at)) / h
                self.assertLess(abs(left + k) / k, 1e-5)
                self.assertLess(abs(right + k) / k, 1e-9)

    def test_the_spline_through_the_nodes(self):
        for w in qs.WALLS:
            d = qs._DIRECT[w]
            with self.subTest(wall=w):
                x = np.exp(d.t[1:-1])
                np.testing.assert_allclose(
                    np.log(qs.s_dir(w, x)), d.ln_s[1:-1], rtol=0.0, atol=1e-13
                )

    def test_s_dir_below_x_squared(self):
        """s_dir(z) <= z^2 for z >= x_lo, which the Omega asymptote's remainder bound uses."""
        for w in qs.WALLS:
            d = qs._DIRECT[w]
            z = np.geomspace(d.x_lo, d.x_end, 20001)
            with self.subTest(wall=w):
                self.assertTrue(np.all(qs.s_dir(w, z) <= z * z * (1.0 + 1e-14)))

    def test_x_p(self):
        """x_p is the spline's maximum: its derivative vanishes there, and no node exceeds it."""
        expected = {"exponential": 0.619424, "power-n1": 0.155250, "power-n2": 0.253372}
        for w in qs.WALLS:
            d = qs._DIRECT[w]
            with self.subTest(wall=w):
                self.assertEqual(qs.x_peak(w), d.x_p)
                self.assertAlmostEqual(d.x_p, expected[w], places=6)
                self.assertLess(abs(float(d.spline(log(d.x_p), 1))), 1e-12)
                self.assertLessEqual(d.ln_s.max(), log(d.s_p) + 1e-15)
                self.assertEqual(qs.spectrum(w, 1.0).x_p, d.x_p)


class TestTheAssociatedPiece(unittest.TestCase):
    """R10 (U9 as proposed)."""

    def test_fifty_at_x_star(self):
        for w in qs.WALLS:
            x = qs.X_STAR
            with self.subTest(wall=w):
                self.assertAlmostEqual(
                    float(qs.shape(w, x, 1.0) / qs.s_dir(w, x)), 50.0, places=12
                )
                self.assertEqual(float(qs.shape(w, x, 0.0)), float(qs.s_dir(w, x)))

    def test_the_coefficient(self):
        for w in qs.WALLS:
            with self.subTest(wall=w):
                C1 = 49.0 * qs.X_STAR * float(qs.s_dir(w, qs.X_STAR))
                self.assertEqual(qs.assoc_coefficient(w, 1.0), C1)
                self.assertAlmostEqual(
                    qs.assoc_coefficient(w, 2.5) / (2.5 * C1), 1.0, places=15
                )
                self.assertAlmostEqual(
                    qs.spectrum(w, 10.0).C / (10.0 * C1), 1.0, places=15
                )
                if w == qs.WALL_EXPONENTIAL:
                    # where s_dir(x_*) ~ x_*^2 (the measured power is 2.000 there), C_1 ~ 49 x_*^3 (U4's)
                    self.assertAlmostEqual(C1 / (49.0 * qs.X_STAR**3), 1.0, places=3)

    def test_the_form(self):
        for w in qs.WALLS:
            d = qs._DIRECT[w]
            C1 = qs.assoc_coefficient(w, 1.0)
            with self.subTest(wall=w):
                for x in (1e-6, 0.01, 0.5 * d.x_p, d.x_p):
                    self.assertAlmostEqual(
                        float(qs.s_assoc(w, x)) / (C1 / x), 1.0, places=14
                    )
                for x in (1.2 * d.x_p, 2.0, d.x_hi + 3.0):
                    self.assertAlmostEqual(
                        float(qs.s_assoc(w, x))
                        / (C1 / x * float(qs.s_dir(w, x)) / d.s_p),
                        1.0,
                        places=14,
                    )
                # continuous at x_p, and s at c_a against the pieces
                lo = float(qs.s_assoc(w, np.nextafter(d.x_p, 0.0)))
                hi = float(qs.s_assoc(w, np.nextafter(d.x_p, 1.0)))
                self.assertAlmostEqual(lo / hi, 1.0, places=12)
                s = qs.spectrum(w, 1.0)
                for x in (0.003, 0.3, d.x_p, 1.7, d.x_hi + 1.0):
                    self.assertAlmostEqual(
                        float(s.s(x)) / float(qs.shape(w, x, 1.0)), 1.0, places=14
                    )
                    self.assertAlmostEqual(
                        s.s_scalar(x) / float(s.s(x)), 1.0, places=14
                    )


class TestTheMoments(unittest.TestCase):
    def test_I_J_and_the_moments_against_quadrature(self):
        for w in qs.WALLS:
            for c in SCALES:
                s = qs.spectrum(w, c)
                with self.subTest(wall=w, c=c):
                    I, eI = s.I_quad()
                    J, eJ = s.J_quad()
                    self.assertLess(eI, QUAD_TOL)
                    self.assertLess(eJ, QUAD_TOL)
                    self.assertLess(abs(s.I / I - 1.0), 1e-12)
                    self.assertLess(abs(s.J / J - 1.0), 1e-12)
                    for k in (1, 2, 4, 5, 6, 8, 10):
                        m, e = s.moment_quad(k)
                        self.assertLess(e, QUAD_TOL)
                        self.assertLess(abs(s.moment(k) / m - 1.0), 1e-12, k)

    def test_the_associated_piece_moves_I_by_its_share(self):
        """
        At c_a = 1 the associated piece carries a few 1e-5 of I on the exponential, about 1.6e-3 on the power wall at
        n = 2 and 1.6e-2 at n = 1 (in the one k_ex of power-law-wall prompt 07c, where x_* = 0.0056 lies nearer
        power-n1's peak, x_p = 0.155; before it, up to 1e-3 on the power wall).
        """
        shares = {}
        for w in qs.WALLS:
            shares[w] = (qs.spectrum(w, 1.0).I - qs.spectrum(w, 0.0).I) / qs.spectrum(
                w, 1.0
            ).I
        self.assertGreater(shares["exponential"], 4e-5)
        self.assertLess(shares["exponential"], 7e-5)
        self.assertGreater(shares["power-n2"], 1e-4)
        self.assertLess(shares["power-n2"], 2e-3)
        self.assertGreater(shares["power-n1"], 1e-2)
        self.assertLess(shares["power-n1"], 2e-2)

    def test_the_shape_s_measurements(self):
        """
        J and M1/M3 at c_a = 0 as 07b's spectrum_kex.py measured them in the one k_ex (.documents/power-law-wall/kex-07b/
        spectrum.txt; power-law-wall prompt 07c), to its trapezoid's accuracy.
        """
        expected = {
            "exponential": (0.872770, 0.903405),
            "power-n1": (1.775255, 4.178280),
            "power-n2": (1.378406, 2.435723),
        }
        for w, (J, r1) in expected.items():
            s = qs.spectrum(w, 0.0)
            with self.subTest(wall=w):
                self.assertAlmostEqual(s.J, J, places=5)
                self.assertAlmostEqual(s.moment(1) / s.I, r1, places=5)
        # log_s_peak is ln s(x_p)
        for w in qs.WALLS:
            s = qs.spectrum(w, 1.0)
            self.assertAlmostEqual(s.log_s_peak, log(float(s.s(s.x_p))), places=13)


class TestTheKernels(unittest.TestCase):
    def test_G_against_quadrature(self):
        for w in qs.WALLS:
            for c in SCALES:
                s = qs.spectrum(w, c)
                for t in g_points():
                    with self.subTest(wall=w, c=c, log_y=t):
                        ref, err = s.G_quad(exp(t))
                        self.assertLess(err, QUAD_TOL)
                        self.assertLess(abs(float(s.G_of_log_y(t)) / ref - 1.0), TOL)
                        self.assertLess(abs(float(s.G(exp(t))) / ref - 1.0), TOL)

    def test_Omega_against_quadrature(self):
        for w in qs.WALLS:
            for c in SCALES:
                s = qs.spectrum(w, c)
                for u in omega_points():
                    with self.subTest(wall=w, c=c, u=u):
                        ref, err = s.Omega_quad(u)
                        self.assertLess(err, QUAD_TOL)
                        self.assertLess(abs(float(s.Omega(u)) / ref - 1.0), TOL)
                        self.assertLess(abs(float(s.log_Omega(u)) - log(ref)), TOL)

    def test_the_kernels_take_arrays(self):
        s = qs.spectrum(qs.WALL_POWER_N1, 1.0)
        t = np.array(g_points())
        self.assertEqual(s.G_of_log_y(t).shape, t.shape)
        np.testing.assert_array_equal(
            s.G_of_log_y(t), [float(s.G_of_log_y(x)) for x in t]
        )
        u = np.array(omega_points())
        np.testing.assert_array_equal(
            s.log_Omega(u), [float(s.log_Omega(x)) for x in u]
        )


class TestTheAsymptotes(unittest.TestCase):
    def test_G_small_y(self):
        """At the grid's lower end, the series with its y^3 and y^4 terms is within 1e-7 of quadrature, and the y^3
        term is needed at c_a = 10 (and absent at c_a = 0)."""
        y = qs.G_Y_MIN
        for w in qs.WALLS:
            for c in SCALES:
                s = qs.spectrum(w, c)
                with self.subTest(wall=w, c=c):
                    ref, _ = s.G_quad(y)
                    self.assertLess(abs(float(s.G_small(y)) / ref - 1.0), 1e-7)
                    leading = s.moment(1) * y * y / 2.0 / s.I
                    cubic = s.C * y**3 / 3.0 / s.I
                    if c == 0.0:
                        self.assertEqual(cubic, 0.0)
                    if c == 10.0:
                        self.assertGreater(cubic / leading, 1e-7)
        # the leading term alone, against quadrature as y -> 0
        s = qs.spectrum(qs.WALL_POWER_N2, 1.0)
        y = 1e-6
        ref, _ = s.G_quad(y)
        self.assertLess(abs((s.moment(1) * y * y / 2.0 / s.I) / ref - 1.0), 1e-6)

    def test_G_large_y(self):
        """At the grid's upper end the series M_2 y/I - 1 + ... is within 1e-7, the -1 is needed, and the left-out
        5 M_10/(128 y^7) is below 1e-11 of G."""
        y = qs.G_Y_MAX
        for w in qs.WALLS:
            for c in SCALES:
                s = qs.spectrum(w, c)
                with self.subTest(wall=w, c=c):
                    ref, _ = s.G_quad(y)
                    self.assertLess(abs(float(s.G_large(y)) / ref - 1.0), 1e-7)
                    leading = s.moment(2) * y / s.I
                    self.assertGreater(abs(leading / ref - 1.0), 1e-7)
                    self.assertLess(
                        5.0 * s.moment(10) / (128.0 * y**7) / s.I / ref, 1e-11
                    )
        s = qs.spectrum(qs.WALL_POWER_N1, 1.0)
        y = 1e9
        ref, _ = s.G_quad(y)
        self.assertLess(abs((s.moment(2) * y / s.I) / ref - 1.0), 1e-6)

    def test_Omega_large_u_and_the_symmetry(self):
        U = qs.OMEGA_U_MAX
        for w in qs.WALLS:
            d = qs._DIRECT[w]
            for c in SCALES:
                s = qs.spectrum(w, c)
                with self.subTest(wall=w, c=c):
                    ref, _ = s.Omega_quad(U)
                    self.assertLess(
                        abs(exp(float(s.log_Omega_large(U))) / ref - 1.0), 1e-7
                    )
                    for u in (0.7, 2.3, U + 1.0):
                        ref_neg, _ = s.Omega_quad(-u)
                        ref_pos, _ = s.Omega_quad(u)
                        self.assertLess(
                            abs(exp(4.0 * -u) * ref_pos / ref_neg - 1.0), 1e-9
                        )
                    # the remainder's bound at U: the weight of x^5 s and x^2 s above x_lo e^U
                    X = d.x_lo * exp(U)
                    xs = np.linspace(X, d.x_end, 200001)
                    self.assertLess(
                        np.trapezoid(xs**5 * s.s(xs), xs) / s.moment(5), 1e-14
                    )
                    self.assertLess(
                        np.trapezoid(xs**2 * s.s(xs), xs) / s.moment(2), 1e-14
                    )
        # c_a = 0: Omega I -> e^{-2u} M_5; c_a > 0: C e^u M_2 dominates
        s0, s1 = qs.spectrum(qs.WALL_EXPONENTIAL, 0.0), qs.spectrum(
            qs.WALL_EXPONENTIAL, 1.0
        )
        u = 12.0
        self.assertAlmostEqual(
            float(s0.log_Omega(u)), log(s0.moment(5)) - 2.0 * u - s0.log_I, places=12
        )
        self.assertAlmostEqual(
            float(s1.log_Omega(u)),
            log(s1.C * s1.moment(2) + exp(-3.0 * u) * s1.moment(5)) + u - s1.log_I,
            places=12,
        )


class TestThePeakSpectrum(unittest.TestCase):
    def test_peak_collapsed(self):
        p = qs.PEAK_SPECTRUM
        self.assertEqual((p.I, p.J), (1.0, 1.0))
        for y in (1e-12, 1e-3, 0.5, 1.0, 7.0, 1e6, 1e12):
            self.assertAlmostEqual(
                float(p.G(y)) / (y * y / (sqrt(1.0 + y * y) + 1.0)), 1.0, places=14
            )
        with self.assertRaises(NotImplementedError):
            p.log_Omega(0.0)


if __name__ == "__main__":
    unittest.main()
