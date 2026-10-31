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
BBN sees ρ_χ through H_J (inelastic-reflection prompt 09, §1.9 test 9; README §6.2 P9; orchestrator
note 09, ruling 11). BBN is not changed.

BBN's density ratio is r_BBN = (3M_P²H_J² − ρ_R,J(1 + f_m))/ρ_R,J (`compute_BBN_data`,
`ComputeTargets/BBNData.py` :462–:469). With H_J = H_E(1 + (ln A)′π)/A, the constraint
3M_P²H_E²G = V + ρ_R,E(1 + f_m) + ρ_χ,E and ρ_R,J = A⁻⁴ρ_R,E, the part of r_BBN linear in ρ_χ is
exactly F·r_χ, with

    F = A²(1 + (ln A)′π)²/G

(log 09 §1.2). So at one state the ratio formed with the history's ledger, less the ratio formed with
an empty one, is F·r_χ. The test forms both at every sample of the history inside BBN's window
(0.2 keV to 100 MeV, `compute_BBN_data`'s defaults) where r_χ > 0, from `ODEPolicy` and
`HubblePolicy` at the sample's state, by `compute_BBN_data`'s expression, and asserts the identity.

**The tolerance** is that of the subtraction's rounding. Each ratio is R − (1 + f_m), with
R = 3M_P²H_J²/ρ_R,J. `HubblePolicy` forms H_E² as exp(ln V − ln(V/3H²M_P²))/3M_P², so R carries a
relative rounding of about ε(|ln V| + |ln(V/3H²M_P²)|) from the exponent, and a few ε more from the
products; 1 + f_m carries ε. So

    |Δ − F r_χ| ≤ c ε [(4 + |ln V| + |ln(V/3H²M_P²)|)(R₁ + R₀) + 2(1 + f_m) + F r_χ], c = 8,

ln V being −277 to −280 in Planck units here. The largest |Δ − F r_χ| over the bound is reported in
the failure message and is below 1 on every sample (log 09 §4.2 gives it).

**Why not the limit the prompt asked for** ("holds ρ_χ/ρ_R,J to a tolerance derived from what it
neglects"): F is not near 1 in general. The orchestrator's probe found 0.0098–0.0107 at M = 0.5 M_P,
β = 5 in 1 keV–10 MeV. The second test shows it on that history: F < 0.05 at the samples, and the
identity holds where the limit Δ ≈ r_χ is out by a factor of about 100.

The histories are `dissipation_histories`' M = 0.5 M_P, β = 2 and β = 5 at α = 0.1 (test 1's and test
2's released history), untuned. Cost: the two integrations, 2.6 s and 5.7 s unloaded (cached), and
about 0.1 s of policy calls. No Ray, no store, no PRyMordial solve.
"""

import contextlib
import io
import unittest
from math import exp, fabs, log

import numpy as np

from ComputeTargets.tests import dissipation_histories as dh

SM = dh.SM

EPS = float(np.finfo(float).eps)
C = 8.0

# compute_BBN_data's defaults: its window in T_J
T_BBN_KEV_SPLINE_MIN = 0.2
T_BBN_MEV_SPLINE_MAX = 100.0


def bbn_ratio(policy, hubble, N, state, log_rhorad_Jordan, CONST_3_MP_SQ):
    """compute_BBN_data's density_NP / rhorad_Jordan at (N, state), and R = 3 M_P^2 H_J^2/rho_R,J."""
    data = policy(N, state)
    H_Jordan = hubble(data, state).H_Jordan
    rhorad_Jordan = exp(log_rhorad_Jordan)
    H2_Jordan = H_Jordan * H_Jordan
    fm = exp(state.log_fm)
    LHS = H2_Jordan * CONST_3_MP_SQ
    density_NP = LHS - rhorad_Jordan * (1.0 + fm)
    return density_NP / rhorad_Jordan, LHS / rhorad_Jordan, data, H_Jordan


class TestBBNSeesRhoChi(unittest.TestCase):
    def check(self, key):
        """Every sample in BBN's window with r_chi > 0; returns the F seen."""
        data, _, rm = dh.history(*key)
        cos = dh.cosmology()
        units = cos.units
        Mp = units.PlanckMass
        CONST_3_MP_SQ = 3.0 * Mp * Mp
        T_min = T_BBN_KEV_SPLINE_MIN * units.keV
        T_max = T_BBN_MEV_SPLINE_MAX * units.MeV

        with_ledger = SM.ODEPolicy(
            "test_bbn", cos, rm, dark_radiation=data["dark_radiation"]
        )
        without = SM.ODEPolicy("test_bbn", cos, rm, dark_radiation=None)
        hubble = SM.HubblePolicy(rm, units)

        Fs, worst, checked = [], 0.0, 0
        with contextlib.redirect_stdout(io.StringIO()):
            for s in data["sample"]:
                if not (T_min <= exp(s.log_T_Jordan) <= T_max):
                    continue
                state = SM.StateVector(
                    phi_Einstein=s.phi_Einstein,
                    pi_Einstein=s.pi_Einstein,
                    log_rhorad_Einstein=s.log_rhorad_Einstein,
                    log_fm=s.log_fm,
                    log_T_Jordan=s.log_T_Jordan,
                )
                r1, R1, d1, H_J = bbn_ratio(
                    with_ledger,
                    hubble,
                    s.raw_N,
                    state,
                    s.log_rhorad_Jordan,
                    CONST_3_MP_SQ,
                )
                r_chi = d1.rho_chi_over_rho_rad
                if r_chi == 0.0:
                    continue
                # the H_J BBN reads is the sample's own
                self.assertEqual(H_J, s.H_Jordan)
                r0, R0, d0, _ = bbn_ratio(
                    without, hubble, s.raw_N, state, s.log_rhorad_Jordan, CONST_3_MP_SQ
                )
                self.assertEqual(d0.rho_chi_over_rho_rad, 0.0)

                phi, pi_E = s.phi_Einstein, s.pi_Einstein
                A = rm.Omega(phi)
                b = 1.0 + rm.d_logOmega_dphi(phi) * pi_E
                G = 1.0 - pi_E * pi_E / (6.0 * Mp * Mp)
                F = A * A * b * b / G
                Fs.append(F)

                fm = exp(s.log_fm)
                log_V = rm.log_V(phi)
                bound = (
                    C
                    * EPS
                    * (
                        (4.0 + fabs(log_V) + fabs(log(d1.V_over_3H2Mp2)))
                        * (fabs(R1) + fabs(R0))
                        + 2.0 * (1.0 + fm)
                        + F * r_chi
                    )
                )
                miss = fabs((r1 - r0) - F * r_chi)
                worst = max(worst, miss / bound)
                self.assertLessEqual(
                    miss,
                    bound,
                    f"N={s.raw_N!r}: r1 - r0 = {r1 - r0!r}, F r_chi = {F * r_chi!r}",
                )
                checked += 1
        self.assertGreater(checked, 10)
        print(
            f"[test_bbn_sees_dark_radiation] beta={key[0]:g}, M={key[1]:g}: {checked} samples, "
            f"F in [{min(Fs):.4g}, {max(Fs):.4g}], largest miss/bound {worst:.3g}"
        )
        return Fs

    def test_9_the_identity_on_test_1s_history(self):
        """M = 0.5 M_P, beta = 2: the ratio with the ledger less the ratio without is F r_chi."""
        self.check((2.0, 0.5, 0.1))

    def test_9_the_identity_where_F_is_far_from_1(self):
        """M = 0.5 M_P, beta = 5 (released): F < 0.05 in BBN's window, so the limit
        Delta = r_chi is out by about 100, and the identity still holds."""
        Fs = self.check((5.0, 0.5, 0.1))
        self.assertLess(min(Fs), 0.05)
