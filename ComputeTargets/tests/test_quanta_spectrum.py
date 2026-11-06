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
The spectrum model and its kernels (one-loop-drain prompt 05 §1.1, §1.2, §1.7; orchestrator note 05, rulings 1-4).

- the shape's pieces, s(x_*) = 50 s_dir(x_*) at c_a = 1, continuity at x_p, U17's end of the associated piece, and kappa
  against its fit (ruling 2);
- I, J and the moments against direct quadrature;
- G and Omega at grid nodes, between nodes, at each end of the grid and a decade beyond each end, against direct
  quadrature (scipy's adaptive quad, split at x_p and at y or x_p e^u), to 1e-6 relative, at c_a = 1, 0 and 10; the
  quadratures' own stated error below 1e-9 (ruling 3);
- the asymptotes, term by term at the grid's ends; Omega's symmetry e^{4u} Omega(-u);
- the peak-collapsed spectrum (ruling 4) and SPECTRUM_MODEL.

Instant to a few seconds (the tables are built once per scale, about 0.2 s each; the quadratures are adaptive).
"""

import unittest
from math import exp, log, sqrt

import numpy as np

from ComputeTargets import quanta_spectrum as qs

SCALES = (1.0, 0.0, 10.0)
TOL = 1e-6
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
        U,
        -U,
        U + log(10.0),
        -(U + log(10.0)),
        U + 6.0,
        -(U + 6.0),
    ]


class TestTheShape(unittest.TestCase):
    def test_the_constants_and_the_version(self):
        self.assertEqual(qs.X_PEAK, 0.62)
        self.assertEqual(qs.X_STAR, 0.0058)
        self.assertEqual(qs.ASSOC_COEFFICIENT, 49.0)
        self.assertEqual(qs.KAPPA_FIT_L, 90)
        self.assertIsInstance(qs.SPECTRUM_MODEL, str)
        self.assertTrue(qs.SPECTRUM_MODEL)
        self.assertEqual(qs.spectrum(1.0).model, qs.SPECTRUM_MODEL)

    def test_kappa_against_its_fit(self):
        """kappa = ln(1.43e-4 / 1e-20)/(10 - 0.62) at L = 90, the median of the Table's five rows (ruling 2)."""
        expected = log(1.43e-4 / 1.0e-20) / (10.0 - 0.62)
        self.assertEqual(qs.KAPPA, expected)
        five = sorted(qs.KAPPA_BY_L.values())
        self.assertEqual(len(five), 5)
        self.assertEqual(five[2], qs.KAPPA)
        # the fit holds: s_dir(10)/s_dir(x_p) = 1e-20/|beta|^2_max at L = 90
        ratio = float(qs.s_dir(10.0) / qs.s_dir(qs.X_PEAK))
        self.assertAlmostEqual(log(ratio), log(1e-20 / 1.43e-4), places=12)

    def test_fifty_at_x_star(self):
        x = qs.X_STAR
        self.assertAlmostEqual(float(qs.shape(x, 1.0) / qs.s_dir(x)), 50.0, places=12)
        self.assertEqual(float(qs.shape(x, 0.0)), x * x)

    def test_the_pieces_and_continuity_at_x_p(self):
        xp = qs.X_PEAK
        below, above = np.nextafter(xp, 0.0), np.nextafter(xp, 1.0)
        for c in SCALES:
            s = qs.spectrum(c)
            self.assertAlmostEqual(
                float(s.s(below)) / float(s.s(above)), 1.0, places=12
            )
            self.assertEqual(float(s.s(xp)), xp * xp + s.C / xp)
        # the direct piece
        self.assertEqual(float(qs.s_dir(0.3)), 0.09)
        self.assertAlmostEqual(
            float(qs.s_dir(2.0)), xp * xp * exp(-qs.KAPPA * (2.0 - xp)), places=15
        )
        # U17: the associated piece is 49 x_*^3/x to x_p, and falls with the direct piece's kappa above
        C1 = 49.0 * qs.X_STAR**3
        self.assertAlmostEqual(float(qs.s_assoc(0.1)) / (C1 / 0.1), 1.0, places=14)
        self.assertAlmostEqual(
            float(qs.s_assoc(3.0)) / (C1 / 3.0 * exp(-qs.KAPPA * (3.0 - xp))),
            1.0,
            places=14,
        )

    def test_a_negative_scale_is_refused(self):
        with self.assertRaises(ValueError):
            qs.spectrum(-1.0)
        with self.assertRaises(ValueError):
            qs.QuantaSpectrum(float("nan"))


class TestTheMoments(unittest.TestCase):
    def test_I_J_and_the_moments_against_quadrature(self):
        for c in SCALES:
            s = qs.spectrum(c)
            with self.subTest(c=c):
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
        """U17's end: the associated piece carries about 3e-5 of I at c_a = 1 (orchestrator note 05 §0)."""
        share = (qs.spectrum(1.0).I - qs.spectrum(0.0).I) / qs.spectrum(1.0).I
        self.assertGreater(share, 2.5e-5)
        self.assertLess(share, 3.5e-5)


class TestTheKernels(unittest.TestCase):
    def test_G_against_quadrature(self):
        for c in SCALES:
            s = qs.spectrum(c)
            for t in g_points():
                with self.subTest(c=c, log_y=t):
                    ref, err = s.G_quad(exp(t))
                    self.assertLess(err, QUAD_TOL)
                    self.assertLess(abs(float(s.G_of_log_y(t)) / ref - 1.0), TOL)
                    self.assertLess(abs(float(s.G(exp(t))) / ref - 1.0), TOL)

    def test_Omega_against_quadrature(self):
        for c in SCALES:
            s = qs.spectrum(c)
            for u in omega_points():
                with self.subTest(c=c, u=u):
                    ref, err = s.Omega_quad(u)
                    self.assertLess(err, QUAD_TOL)
                    self.assertLess(abs(float(s.Omega(u)) / ref - 1.0), TOL)
                    self.assertLess(abs(float(s.log_Omega(u)) - log(ref)), TOL)

    def test_the_kernels_take_arrays(self):
        s = qs.spectrum(1.0)
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
        for c in SCALES:
            s = qs.spectrum(c)
            with self.subTest(c=c):
                ref, _ = s.G_quad(y)
                self.assertLess(abs(float(s.G_small(y)) / ref - 1.0), 1e-7)
                leading = s.moment(1) * y * y / 2.0 / s.I
                cubic = s.C * y**3 / 3.0 / s.I
                if c == 0.0:
                    self.assertEqual(cubic, 0.0)
                if c == 10.0:
                    self.assertGreater(cubic / leading, 1e-7)
        # the leading term alone, against quadrature as y -> 0 (prompt §1.2)
        s = qs.spectrum(1.0)
        y = 1e-6
        ref, _ = s.G_quad(y)
        self.assertLess(abs((s.moment(1) * y * y / 2.0 / s.I) / ref - 1.0), 1e-6)

    def test_G_large_y(self):
        """At the grid's upper end the series M_2 y/I - 1 + ... is within 1e-7, and the -1 is needed (ruling 3)."""
        y = qs.G_Y_MAX
        for c in SCALES:
            s = qs.spectrum(c)
            with self.subTest(c=c):
                ref, _ = s.G_quad(y)
                self.assertLess(abs(float(s.G_large(y)) / ref - 1.0), 1e-7)
                leading = s.moment(2) * y / s.I
                self.assertGreater(abs(leading / ref - 1.0), 1e-7)
        # y * int x^2 s / I as y -> infinity (prompt §1.2)
        s = qs.spectrum(1.0)
        y = 1e9
        ref, _ = s.G_quad(y)
        self.assertLess(abs((s.moment(2) * y / s.I) / ref - 1.0), 1e-6)

    def test_Omega_large_u_and_the_symmetry(self):
        for c in SCALES:
            s = qs.spectrum(c)
            U = qs.OMEGA_U_MAX
            with self.subTest(c=c):
                ref, _ = s.Omega_quad(U)
                self.assertLess(abs(exp(float(s.log_Omega_large(U))) / ref - 1.0), 1e-7)
                for u in (0.7, 2.3, U + 1.0):
                    ref_neg, _ = s.Omega_quad(-u)
                    ref_pos, _ = s.Omega_quad(u)
                    self.assertLess(abs(exp(4.0 * -u) * ref_pos / ref_neg - 1.0), 1e-9)
        # c_a = 0: Omega I -> e^{-2u} M_5; c_a > 0: C e^u M_2 dominates
        s0, s1 = qs.spectrum(0.0), qs.spectrum(1.0)
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
