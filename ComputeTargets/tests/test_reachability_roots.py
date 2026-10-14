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
The kicks of Sigma and the roots of the reachability condition
(surfing-temperature prompt 01, items R1-R3): extract_common.find_kicks and
extract_common.reachability_roots.

(a) a stub cosmology whose Sigma is a sum of two well-separated Gaussians in ln T, solved in
closed form; (b) a stub with two overlapping Gaussians whose trough lies above c, so that the
interval is merged; (c) a stub with a ripple below the kick floor that crosses c, which sets
multiple_crossings; (d) the production EOS, QCD_Cosmology (building it takes a few seconds):
four kicks, their peaks and thresholds, the roots, and the merge of QCD with mu-pi.

No Ray cluster, no datastore, no solve.
"""

import unittest
from math import exp, log, sqrt
from types import SimpleNamespace

from extract_common import find_kicks, kick_threshold_curve, reachability_roots


def gaussian_stub(components):
    """
    A cosmology with Sigma(T) = sum_j A_j exp(-(ln T - m_j)^2 / (2 s_j^2)), components a list of
    (A, m, s), through w = (1 - Sigma)/3 and dw_dlogT = -(d Sigma/d ln T)/3.
    """

    def Sigma(T):
        x = log(T)
        return sum(A * exp(-((x - m) ** 2) / (2.0 * s * s)) for A, m, s in components)

    def dSigma(T):
        x = log(T)
        return sum(
            -A * (x - m) / (s * s) * exp(-((x - m) ** 2) / (2.0 * s * s))
            for A, m, s in components
        )

    return SimpleNamespace(
        w=lambda T: (1.0 - Sigma(T)) / 3.0,
        dw_dlogT=lambda T: -dSigma(T) / 3.0,
    )


def analytic_roots(A, m, s, beta):
    """
    (T_hot, T_cold) of the single Gaussian A exp(-(x - m)^2/(2 s^2)): Sigma_eff = c is
    Sigma = c/(1 - c/2), c = 1/(3 beta^2).
    """
    c = 1.0 / (3.0 * beta * beta)
    Sigma_star = c / (1.0 - 0.5 * c)
    half = s * sqrt(2.0 * log(A / Sigma_star))
    return exp(m + half), exp(m - half)


def rel(a, b):
    return abs(a - b) / abs(b)


A_STRONG, A_WEAK = 0.30, 0.05
M_STRONG, M_WEAK = 0.0, -12.0
S_GAUSS = 0.5
T_LO, T_HI = exp(-20.0), exp(6.0)


class TestTwoSeparatedGaussians(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cosmology = gaussian_stub(
            [(A_STRONG, M_STRONG, S_GAUSS), (A_WEAK, M_WEAK, S_GAUSS)]
        )
        cls.kicks = find_kicks(cls.cosmology, T_LO, T_HI)

    def test_a1_kicks(self):
        kicks = self.kicks
        self.assertEqual([k["label"] for k in kicks], ["kick0", "kick1"])
        self.assertLess(rel(kicks[0]["T_peak"], exp(M_STRONG)), 1e-8)
        self.assertLess(rel(kicks[1]["T_peak"], exp(M_WEAK)), 1e-8)
        self.assertLess(rel(kicks[0]["Sigma_peak"], A_STRONG), 1e-8)
        self.assertLess(rel(kicks[1]["Sigma_peak"], A_WEAK), 1e-8)
        # the windows tile [T_LO, T_HI], hot to cold, with the trough between the peaks
        self.assertEqual(kicks[0]["T_window_hot"], T_HI)
        self.assertEqual(kicks[1]["T_window_cold"], T_LO)
        self.assertEqual(kicks[0]["T_window_cold"], kicks[1]["T_window_hot"])
        self.assertLess(kicks[1]["T_peak"], kicks[0]["T_window_cold"])
        self.assertLess(kicks[0]["T_window_cold"], kicks[0]["T_peak"])
        self.assertEqual(kicks[0]["Sigma_at_cold_edge"], kicks[1]["Sigma_at_hot_edge"])
        self.assertLess(kicks[0]["Sigma_at_cold_edge"], A_WEAK)

    def test_a2_roots_above_both_thresholds(self):
        beta = 5.0
        records = reachability_roots(self.cosmology, beta, self.kicks)
        self.assertEqual([r["kick"] for r in records], ["kick0", "kick1"])
        for record, (A, m) in zip(records, [(A_STRONG, M_STRONG), (A_WEAK, M_WEAK)]):
            T_hot, T_cold = analytic_roots(A, m, S_GAUSS, beta)
            self.assertLess(rel(record["T_hot"], T_hot), 1e-9)
            self.assertLess(rel(record["T_cold"], T_cold), 1e-9)
            self.assertEqual(record["beta"], beta)
            self.assertIsNone(record["merged"])
            self.assertFalse(record["multiple_crossings"])
            # the hot root is the attractor, the cold root the saddle
            self.assertLess(record["slope_hot"], 0.0)
            self.assertGreater(record["slope_cold"], 0.0)

    def test_a3_slopes_are_those_of_the_gaussian(self):
        beta = 5.0
        record = reachability_roots(self.cosmology, beta, self.kicks)[0]
        # d ln Sigma/d ln T = -(x - m)/s^2 for a single Gaussian
        self.assertLess(
            abs(
                record["slope_hot"] + log(record["T_hot"] / exp(M_STRONG)) / S_GAUSS**2
            ),
            1e-9,
        )
        self.assertLess(
            abs(
                record["slope_cold"]
                + log(record["T_cold"] / exp(M_STRONG)) / S_GAUSS**2
            ),
            1e-9,
        )

    def test_a4_central_difference_slope_without_dw_dlogT(self):
        beta = 5.0
        bare = SimpleNamespace(w=self.cosmology.w)
        with_derivative = reachability_roots(self.cosmology, beta, self.kicks)[0]
        without = reachability_roots(bare, beta, self.kicks)[0]
        self.assertLess(rel(without["T_hot"], with_derivative["T_hot"]), 1e-12)
        self.assertLess(abs(without["slope_hot"] - with_derivative["slope_hot"]), 1e-6)
        self.assertLess(
            abs(without["slope_cold"] - with_derivative["slope_cold"]), 1e-6
        )

    def test_a5_beta_between_the_thresholds(self):
        # thresholds: 1/sqrt(3 Sigma_eff,peak) = 1.13 (strong) and 2.61 (weak)
        beta = 2.0
        strong, weak = reachability_roots(self.cosmology, beta, self.kicks)
        T_hot, T_cold = analytic_roots(A_STRONG, M_STRONG, S_GAUSS, beta)
        self.assertLess(rel(strong["T_hot"], T_hot), 1e-9)
        self.assertLess(rel(strong["T_cold"], T_cold), 1e-9)
        for key in ("T_hot", "T_cold", "slope_hot", "slope_cold", "merged"):
            self.assertIsNone(weak[key])
        self.assertFalse(weak["multiple_crossings"])

    def test_a6_beta_below_both(self):
        for record in reachability_roots(self.cosmology, 1.0, self.kicks):
            for key in ("T_hot", "T_cold", "slope_hot", "slope_cold", "merged"):
                self.assertIsNone(record[key])


class TestMergedInterval(unittest.TestCase):
    """Two Gaussians (0.30 at ln T = 0, 0.20 at ln T = -4, s = 1) whose trough lies above c."""

    @classmethod
    def setUpClass(cls):
        cls.components = [(0.30, 0.0, 1.0), (0.20, -4.0, 1.0)]
        cls.cosmology = gaussian_stub(cls.components)
        cls.kicks = find_kicks(cls.cosmology, T_LO, T_HI)

    def test_b1_merged_above_the_trough(self):
        trough = self.kicks[0]["T_window_cold"]
        Sigma_trough = self.kicks[0]["Sigma_at_cold_edge"]
        beta = 5.0
        c = 1.0 / (3.0 * beta * beta)
        self.assertGreater(Sigma_trough / (1.0 + 0.5 * Sigma_trough), c)
        hot, cold = reachability_roots(self.cosmology, beta, self.kicks)
        self.assertEqual(hot["merged"], "kick1")
        self.assertEqual(cold["merged"], "kick0")
        # one interval: both records carry its roots
        self.assertEqual(hot["T_hot"], cold["T_hot"])
        self.assertEqual(hot["T_cold"], cold["T_cold"])
        self.assertEqual(hot["slope_hot"], cold["slope_hot"])
        self.assertEqual(hot["slope_cold"], cold["slope_cold"])
        # the hot root is above the hotter peak, the cold root below the colder, none at the trough
        self.assertGreater(hot["T_hot"], self.kicks[0]["T_peak"])
        self.assertLess(hot["T_cold"], self.kicks[1]["T_peak"])
        self.assertGreater(abs(log(hot["T_hot"] / trough)), 1.0)
        self.assertGreater(abs(log(hot["T_cold"] / trough)), 1.0)
        self.assertLess(hot["slope_hot"], 0.0)
        self.assertGreater(hot["slope_cold"], 0.0)
        # each is a root of Sigma_eff = c
        for T in (hot["T_hot"], hot["T_cold"]):
            S = 1.0 - 3.0 * self.cosmology.w(T)
            self.assertLess(abs(S / (1.0 + 0.5 * S) - c), 1e-12)

    def test_b2_separate_when_c_is_above_the_trough(self):
        # c = 0.1 is between Sigma_eff of the trough (0.066) and of the weaker peak (0.18)
        beta = sqrt(1.0 / 0.3)
        hot, cold = reachability_roots(self.cosmology, beta, self.kicks)
        self.assertIsNone(hot["merged"])
        self.assertIsNone(cold["merged"])
        self.assertGreater(hot["T_cold"], cold["T_hot"])
        self.assertGreater(hot["T_cold"], self.kicks[0]["T_window_cold"])
        self.assertLess(cold["T_hot"], self.kicks[0]["T_window_cold"])


class TestMultipleCrossings(unittest.TestCase):
    """A ripple of 5e-5, below the kick floor, that crosses c = 3.3e-5 at beta = 100."""

    def test_c1_flag_and_outermost_pair(self):
        cosmology = gaussian_stub([(0.30, 0.0, 0.5), (5e-5, -8.0, 0.3)])
        kicks = find_kicks(cosmology, T_LO, T_HI)
        self.assertEqual([k["label"] for k in kicks], ["kick0"])
        (record,) = reachability_roots(cosmology, 100.0, kicks)
        self.assertTrue(record["multiple_crossings"])
        T_hot, _ = analytic_roots(0.30, 0.0, 0.5, 100.0)
        _, T_cold = analytic_roots(5e-5, -8.0, 0.3, 100.0)
        self.assertLess(rel(record["T_hot"], T_hot), 1e-9)
        self.assertLess(rel(record["T_cold"], T_cold), 1e-9)
        # a single Gaussian does not set it
        single = gaussian_stub([(0.30, 0.0, 0.5)])
        (record,) = reachability_roots(single, 100.0, find_kicks(single, T_LO, T_HI))
        self.assertFalse(record["multiple_crossings"])


class TestProductionEOS(unittest.TestCase):
    """
    The production EOS, QCD_Cosmology(0, Planck_units(), Planck2018()). Building it takes a few
    seconds. No solve.
    """

    @classmethod
    def setUpClass(cls):
        from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
        from CosmologyModels.LambdaCDM import Planck2018
        from Units import Planck_units

        cls.units = Planck_units()
        cls.cosmology = QCD_Cosmology(0, cls.units, Planck2018())
        eos = cls.cosmology._eos
        # the table's ends, 1e-5 GeV and 25118.86 GeV, read from the EOS
        cls.T_lo = float(eos._T_min) * cls.units.GeV
        cls.T_hi = float(eos._T_max) * cls.units.GeV
        cls.kicks = find_kicks(cls.cosmology, cls.T_lo, cls.T_hi)

    def GeV(self, T):
        return T / self.units.GeV

    def test_d1_four_kicks_and_their_peaks(self):
        kicks = self.kicks
        self.assertEqual([k["label"] for k in kicks], ["EW", "QCD", "mu-pi", "ee"])
        # Sigma_peak to the digits of README section 0.3
        for kick, value, delta in zip(
            kicks, [0.037436, 0.31453, 0.085365, 0.10073], [5e-7, 5e-6, 5e-7, 5e-6]
        ):
            self.assertAlmostEqual(kick["Sigma_peak"], value, delta=delta)
        # T_peak against the refined values measured on 2026-10-14, to 1e-3
        for kick, value in zip(kicks, [53.2623, 0.181893, 0.0431559, 1.60450e-4]):
            self.assertLess(rel(self.GeV(kick["T_peak"]), value), 1e-3)
        # ordered hot to cold, and the windows tile the table
        peaks = [k["T_peak"] for k in kicks]
        self.assertEqual(peaks, sorted(peaks, reverse=True))
        self.assertEqual(kicks[0]["T_window_hot"], self.T_hi)
        self.assertEqual(kicks[-1]["T_window_cold"], self.T_lo)
        for hotter, colder in zip(kicks[:-1], kicks[1:]):
            self.assertEqual(hotter["T_window_cold"], colder["T_window_hot"])
            self.assertLess(colder["T_peak"], hotter["T_window_cold"])
            self.assertLess(hotter["T_window_cold"], hotter["T_peak"])
            self.assertEqual(hotter["Sigma_at_cold_edge"], colder["Sigma_at_hot_edge"])
            self.assertLess(hotter["Sigma_at_cold_edge"], hotter["Sigma_peak"])
            self.assertLess(hotter["Sigma_at_cold_edge"], colder["Sigma_peak"])

    def test_d2_thresholds(self):
        thresholds = [
            sqrt((2.0 + k["Sigma_peak"]) / (6.0 * k["Sigma_peak"])) for k in self.kicks
        ]
        # the paper's 3.01, 1.107 and 1.86, and README section 0.3's 2.018 for mu-pi
        for value, expected, delta in zip(
            thresholds, [3.01, 1.107, 2.018, 1.86], [5e-3, 5e-4, 5e-4, 5e-3]
        ):
            self.assertAlmostEqual(value, expected, delta=delta)

    def test_d3_roots_straddle_the_peak_and_solve_the_threshold_curve(self):
        for kick in self.kicks:
            threshold = sqrt((2.0 + kick["Sigma_peak"]) / (6.0 * kick["Sigma_peak"]))
            beta = threshold + 0.01
            records = reachability_roots(self.cosmology, beta, self.kicks)
            record = next(r for r in records if r["kick"] == kick["label"])
            self.assertIsNotNone(record["T_hot"], kick["label"])
            self.assertGreater(record["T_hot"], kick["T_peak"])
            self.assertLess(record["T_cold"], kick["T_peak"])
            self.assertLess(record["slope_hot"], 0.0)
            self.assertGreater(record["slope_cold"], 0.0)
            for T in (record["T_hot"], record["T_cold"]):
                _, beta_th = kick_threshold_curve(self.cosmology, [T])
                self.assertLess(abs(beta_th[0] - beta), 1e-9 * beta)

    def test_d4_every_root_solves_the_threshold_curve(self):
        for beta in (1.2, 2.0, 2.1, 2.2, 3.1, 5.0):
            for record in reachability_roots(self.cosmology, beta, self.kicks):
                for key in ("T_hot", "T_cold"):
                    if record[key] is None:
                        continue
                    _, beta_th = kick_threshold_curve(self.cosmology, [record[key]])
                    self.assertLess(abs(beta_th[0] - beta), 1e-9 * beta)

    def test_d5_the_merge_of_qcd_with_mu_pi(self):
        def by_label(beta):
            return {
                r["kick"]: r
                for r in reachability_roots(self.cosmology, beta, self.kicks)
            }

        low = by_label(2.1)
        self.assertIsNone(low["QCD"]["merged"])
        self.assertIsNone(low["mu-pi"]["merged"])
        self.assertIsNotNone(low["mu-pi"]["T_hot"])
        self.assertGreater(low["QCD"]["T_cold"], low["mu-pi"]["T_hot"])

        high = by_label(2.2)
        self.assertEqual(high["QCD"]["merged"], "mu-pi")
        self.assertEqual(high["mu-pi"]["merged"], "QCD")
        for key in ("T_hot", "T_cold", "slope_hot", "slope_cold"):
            self.assertEqual(high["QCD"][key], high["mu-pi"][key])
        # the hot root is in QCD's window, the cold in mu-pi's; none at the trough
        qcd, mu_pi = self.kicks[1], self.kicks[2]
        self.assertGreater(high["QCD"]["T_hot"], qcd["T_peak"])
        self.assertLess(high["QCD"]["T_cold"], mu_pi["T_peak"])
        self.assertGreater(high["QCD"]["T_cold"], mu_pi["T_window_cold"])
        # the other two kicks are not merged at 2.2
        self.assertIsNone(high["EW"]["merged"])
        self.assertIsNone(high["ee"]["merged"])

    def test_d6_beta_table_and_troughs(self):
        """
        The beta table and the troughs, printed so that the log can record them as measured
        (README rule 10: measurements, not bounds). The only assertion is that no interval
        is flagged multiple_crossings and that no kick is reported twice as merged with itself.
        """
        print()
        for hotter, colder in zip(self.kicks[:-1], self.kicks[1:]):
            Sigma = hotter["Sigma_at_cold_edge"]
            print(
                f"trough {hotter['label']}-{colder['label']}: "
                f"T = {self.GeV(hotter['T_window_cold']):.6g} GeV, Sigma = {Sigma:.6g}, "
                f"Sigma_eff = {Sigma / (1.0 + 0.5 * Sigma):.6g}, "
                f"exceeds 1/(3*5^2): {Sigma / (1.0 + 0.5 * Sigma) > 1.0 / 75.0}"
            )
        for beta in (1.10, 1.2, 1.5, 2.0, 2.1, 2.2, 3.0, 3.1, 4.0, 5.0):
            for r in reachability_roots(self.cosmology, beta, self.kicks):
                self.assertFalse(r["multiple_crossings"])
                self.assertNotEqual(r["merged"], r["kick"])


if __name__ == "__main__":
    unittest.main()
