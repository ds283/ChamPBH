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
`AdiabaticHistory` with the dark radiation (inelastic-reflection prompt 09, §1.5 and §1.9 test 8;
README §8.1 R15, §8.2 P29; orchestrator note 09, rulings 9 and 10).

`compute_adiabatic_values` passes each stored sample's `rho_chi_over_rho_rad` as `r_chi` to every
policy call, and the conformal mass reads E = ρ_R,E(1 + f_m)/3H²M_P², the bath's and matter's share,
which is `matter_and_dark_radiation_fractions`'s E where r_χ ≠ 0 and G − V/3H²M_P² where r_χ = 0.0
(log 09 §1.1). Each history is stored and read back through `_TempStoreCase`, so that the ratio the
policy reads is the store's.

- **(a) α = 0 is bitwise the dispatch commit's.** The tuned golden setup
  (`golden_tuned_history.py`: β = 3, M = 10⁻² M_P), one integration at the golden JSON's tuning, as
  `test_tuned_history_round_trip` makes it, stored at α = 0 and read back. `max_abs_Q_values` by
  `repr`, and the SHA-256 of `repr((raw_N_grid, [abs_Q_samples[l] for l in Q_labels]))`, equal the
  pins, which were taken at the dispatch commit `03b769e` by the script quoted whole in log 09 §4.3
  (its mode `pins`), in a temporary worktree of that commit, on the same setup and harness.
  `electroweak-eos` prompt 01 re-pinned them on its w table, at the regenerated tuned golden record.
- **(b) α = 0.1 reads ρ_χ.** On M = 0.5 M_P, β = 2 (test 1's history): at each sample, M²_eff/H² and
  Ḣ/H² equal a recomputation from the policy's parts with `r_chi` and §1.5's E, bitwise; and
  `compute_adiabatic_values`'s |Q| equal Q formed from the recomputed grids, bitwise.
- **(c) Q on the parked samples.** On M = 10⁻³⁰ M_P, β = 2 (parked from N ≈ 30.3 to the end), every
  |Q| is finite, the parked samples' included (R15).

No Ray, no persistent datastore, no PRyMordial solve. Cost: the golden integration (about 5 s
unloaded), and the two shared histories of `dissipation_histories` (2.6 s and 6 s unloaded, cached);
then about 2 s per test for the samples written, read back and assessed. About 30 s at a load of 10.
"""

import contextlib
import hashlib
import io
import json
from math import exp, fabs, isfinite
from types import SimpleNamespace as NS

from ComputeTargets.AdiabaticHistory import (
    AdiabaticComputePolicy,
    AdiabaticHistory,
    Q_numerator,
    compute_adiabatic_values,
    conformal_mass_over_H2,
)
from ComputeTargets.tests import dissipation_histories as dh
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from ObjectFactories.tests.test_dissipation_store import (
    PARKED,
    TEST_1,
    _DissipationStoreCase,
)

SM = dh.SM

# (a)'s pins, taken at the dispatch commit 03b769e (log 09 §4.3). Re-pinned by electroweak-eos prompt 01
# when the w table changed: the tuned golden history it integrates (now golden_history_ee01_tuned.json)
# bounces first at T_J = 48.5 GeV, not 1.68 GeV, so |Q| moves. The old pins were
# {'kp_over_H_1E1': 18.9909249914034, 'kp_over_H_1E2': 1.2320960122983178,
# 'kp_over_H_1E3': 0.016026391467678723, 'kp_over_H_1E4': 5.683352478895833e-05} and
# a59c5cc762e2cdd2db0d51b005099d25050a1621634d293ff54bb39114f96c0f; the sample count is unchanged
PIN_MAX_ABS_Q = (
    "{'kp_over_H_1E1': 13.966453760977432, 'kp_over_H_1E2': 0.8964169114084297, "
    "'kp_over_H_1E3': 0.022162546864849465, 'kp_over_H_1E4': 9.212758841702059e-05}"
)
PIN_SHA256 = "740c6c8d6d9de04a6348851357154469b60c2f2d2d7e8bf27c563cc5f54c1cd9"
PIN_SAMPLES = 5960


def _golden_payload(cosmology):
    """One integration of the tuned golden setup at the JSON's tuning, with its tuning keys."""
    golden = json.loads(gth.GOLDEN_TUNED_PATH.read_text())
    tuning = {
        key: (
            float.fromhex(value)
            if isinstance(value, str) and key != "seed_kind"
            else value
        )
        for key, value in golden["tuning"].items()
    }
    units = cosmology.units
    realized = gth.make_model(units).realize(tuning["normalization"], tuning["phi_ref"])
    with contextlib.redirect_stdout(io.StringIO()):
        data = SM.integrate_history(
            cosmology,
            temperature(0, gth.T_INIT_GEV * units.GeV),
            cosmology.T_CMB_Kelvin * units.Kelvin,
            phi_value(0, gth.PHI_INIT_OVER_MP * units.PlanckMass),
            pi_value(0, gth.PI_INIT),
            dh.z_grid(),
            realized,
            task_label=gth.TASK_LABEL,
            atol=gth.ATOL,
            rtol=gth.RTOL,
        )
    payload = dict(data)
    payload.update({key: tuning[key] for key in SM.TUNING_OUTPUT_KEYS})
    return payload


def _adiabatic(history, label="test_adiabatic_dark_radiation"):
    with contextlib.redirect_stdout(io.StringIO()):
        return compute_adiabatic_values._function(
            NS(get=lambda: history), AdiabaticHistory.Q_labels, label
        )


class TestAdiabaticHistory(_DissipationStoreCase):
    def test_8a_alpha_zero_is_the_dispatch_commits(self):
        """(a) The tuned golden setup at alpha = 0, stored and read back: the pins."""
        # the golden setup's parameter rows are this harness's (T*, phi*, pi*, the tolerances)
        self.assertEqual(
            (gth.T_INIT_GEV, gth.PHI_INIT_OVER_MP, gth.PI_INIT),
            (dh.T_INIT_GEV, dh.PHI_INIT_OVER_MP, dh.PI_INIT),
        )
        self.assertEqual((gth.ATOL, gth.RTOL, gth.TOLERANCE), (1e-8, 1e-8, 1e-5))

        payload = _golden_payload(self.cosmology)
        self.assertEqual(set(payload["rho_chi_over_rho_rad"]), {0.0})
        self.write(payload, gth.BETA, gth.M_OVER_MP, 0.0)
        got = self.read(gth.BETA, gth.M_OVER_MP, 0.0)
        out = _adiabatic(got)

        self.assertEqual(len(out["raw_N_grid"]), PIN_SAMPLES)
        self.assertEqual(repr(out["max_abs_Q_values"]), PIN_MAX_ABS_Q)
        rep = repr(
            (
                out["raw_N_grid"],
                [out["abs_Q_samples"][l] for l in AdiabaticHistory.Q_labels],
            )
        )
        self.assertEqual(hashlib.sha256(rep.encode()).hexdigest(), PIN_SHA256)

    def test_8b_alpha_reads_rho_chi(self):
        """(b) At alpha = 0.1, M^2_eff/H^2, Hdot/H^2 and Q equal a recomputation with r_chi and
        section 1.5's E, bitwise."""
        self.write(dh.payload(*TEST_1), *TEST_1)
        got = self.read(*TEST_1)
        rm = got.realized_model
        cos = self.cosmology
        Mp = self.units.PlanckMass
        three_Mp2 = 3.0 * Mp * Mp
        policy = AdiabaticComputePolicy("test", cos, rm)
        V = policy.V_policy

        m_grid, h_grid, positive = [], [], 0
        with contextlib.redirect_stdout(io.StringIO()):
            for v in got.values:
                phi, pi_E, log_rho = (
                    v.phi_Einstein,
                    v.pi_Einstein,
                    v.log_rhorad_Einstein,
                )
                fm = exp(v.log_fm)
                T = exp(v.log_T_Jordan)
                r = v.rho_chi_over_rho_rad
                positive += r > 0.0

                # the recomputation (log 09 §1.1): the three places rho_chi enters
                U = V.V_over_3H2Mp2(phi, pi_E, log_rho, fm, r_chi=r)
                Vpp = V.Vprimeprime_over_3H2Mp2(phi, pi_E, log_rho, fm, r_chi=r)
                G = 1.0 - pi_E * pi_E / (6.0 * Mp * Mp)
                if r == 0.0:
                    E = G - U
                else:
                    E, _ = V.matter_and_dark_radiation_fractions(G, U, fm, r)
                E = max(E, 0.0)
                Sigma_T = -3.0 * float(cos.dw_dlogT(T))
                x = float(cos.dG_s_dlogT(T)) / float(cos.G_s(T)) / 3.0
                conformal = conformal_mass_over_H2(
                    three_Mp2,
                    E,
                    v.Sigma,
                    fm,
                    rm.d_logOmega_dphi(phi),
                    rm.d2_logOmega_dphi2(phi),
                    Sigma_T,
                    x,
                )
                hp3 = V.Hdot_over_H2_plus_3(phi, pi_E, log_rho, v.Sigma, fm, r_chi=r)
                m = three_Mp2 * Vpp + conformal + (1.0 - hp3)
                h = hp3 - 3.0

                self.assertEqual(
                    policy.M2eff_over_H2(phi, pi_E, log_rho, v.Sigma, fm, T, r).hex(),
                    m.hex(),
                    f"M2eff/H2 at N={v.raw_N!r}",
                )
                self.assertEqual(
                    policy.Hdot_over_H2(phi, pi_E, log_rho, v.Sigma, fm, r).hex(),
                    h.hex(),
                    f"Hdot/H2 at N={v.raw_N!r}",
                )
                m_grid.append(m)
                h_grid.append(h)
        self.assertGreater(positive, 1000)

        # Q through compute_adiabatic_values, against Q formed from the recomputed grids
        out = _adiabatic(got)
        N_grid = [v.raw_N for v in got.values]
        AC = Q_numerator(N_grid, m_grid, h_grid)
        for label, kp_over_H in AdiabaticHistory.Q_labels.items():
            expected = [
                fabs(ac / pow(fabs(m + kp_over_H * kp_over_H), 3.0 / 2.0))
                for ac, m in zip(AC, m_grid)
            ]
            self.assertEqual(
                [q.hex() for q in out["abs_Q_samples"][label]],
                [q.hex() for q in expected],
                label,
            )

    def test_8c_Q_is_finite_on_the_parked_samples(self):
        """(c) M = 1e-30 M_P, beta = 2 parks: every |Q| is finite, the parked samples' included."""
        payload = dh.payload(*PARKED)
        parking = payload["parking"]
        self.assertIsNotNone(parking)
        self.assertIsNone(payload["release"])
        self.write(payload, *PARKED)
        got = self.read(*PARKED)
        out = _adiabatic(got)

        parked = [i for i, N in enumerate(out["raw_N_grid"]) if N > parking.N]
        self.assertGreater(len(parked), 1000)
        for label in AdiabaticHistory.Q_labels:
            samples = out["abs_Q_samples"][label]
            self.assertEqual(len(samples), len(out["raw_N_grid"]))
            bad = [i for i, q in enumerate(samples) if not isfinite(q)]
            self.assertEqual(bad, [], label)
            self.assertTrue(isfinite(out["max_abs_Q_values"][label]), label)
