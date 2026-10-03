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
The PRyMordial new-physics callback built by `build_rho_NP_callback`, the
density compute_BBN_data hands it, and the Standard-Model baseline.

Written for review-remediation prompt 04 (item R3). Before that prompt the
callbacks splined asinh(rho_NP / MeV^4) against ln T; they now spline the ratio
r = rho_NP / rho_R,J and multiply back by rho_SM(T). Since science-readiness
prompt 01 there is one callback, rho_NP: the Hubble-only route reads nothing
else, so the pressure and density-derivative callbacks, the Jordan-frame Hdot/H^2
expression that built the pressure, and their tests went. Every remaining test
keeps its purpose for rho_NP, and test (g), which tested the Hdot/H^2
expression, is replaced by a test of what compute_BBN_data now computes in its
place. The synthetic geometry is that of
`.documents/audit-2026-09-29/spline_test.py`: knots at 250 per decade of T over
[1e-7, 1e2] MeV (the pipeline's spline domain), errors measured on 3,000 points
in [0.02, 5] MeV.

rho_SM is the Saikawa-Shirai g_rho through `SaikawaShirai_EOS_spline` (the
class whose G_rho the production EOS inherits), passed through
`thermodynamic_rho_SM`. No cosmology object is needed.

Errors are reported as in spline_test.py: |rho_callback - rho_true| / rho_SM(T),
the spurious fractional change of H^2.

**Tests (h) and (i) run PRyMordial, one solve each, about 10 s each.** Test (g)
stubs PRyMclass and runs no solve. Nothing here needs a Ray cluster or a
datastore. Run from the repository root, since PRyMordial reads `PRyMrates/`
from the working directory:

    PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .
"""

import os
import unittest
from math import exp, log, log10, pi, sin, sqrt
from types import SimpleNamespace
from unittest import mock

import numpy as np
from scipy.interpolate import make_interp_spline

from ComputeTargets.BBNData import (
    PRYM_VERSION,
    build_rho_NP_callback,
    compute_BBN_data,
    compute_SM_baseline,
    thermodynamic_rho_SM,
)
from ComputeTargets.exceptions import ComputationFailureError
from ComputeTargets.tests.prym_fixtures import (
    RES_D_OVER_H_E5,
    RES_YP_BBN,
    SavedPRyMGlobals,
    run_prym,
)
from ComputeTargets.tests.test_prym_passenger import (
    CONST_HONLY_FULL_D_OVER_H_E5,
    CONST_HONLY_FULL_YP,
)
from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import (
    SaikawaShirai_EOS_spline,
)
from Units import GeV_units, Planck_units

REPORT = bool(os.environ.get("CHAMPBH_TEST_REPORT"))

# the builder tests' own spline domain, 1e-4 keV to 100 MeV: compute_BBN_data's defaults
# before science-readiness prompt 06, which moved the floor to 0.2 keV
T_MIN_MEV = 1e-7
T_MAX_MEV = 100.0
KNOTS_PER_DECADE = 250

# the window PRyMordial cares about, and the evaluation density
T_EVAL_LO_MEV = 0.02
T_EVAL_HI_MEV = 5.0
N_EVAL = 3000

# (a) constant ratio
CONSTANT_RATIO = 0.08
CONSTANT_VALUE_TOLERANCE = 1e-12

# (b) oscillating ratio: review-remediation README section 6.3 (spline_test.py
# measured 9.7e-9)
OSCILLATING_VALUE_BOUND = 2e-8

# (f) rho_SM at 1 MeV is about 3.5 MeV^4 (g_rho about 10.5)
RHO_SM_1MEV_LO = 3.4
RHO_SM_1MEV_HI = 3.6
UNITS_AGREEMENT_RTOL = 1e-10

# (g) the stand-in history handed to compute_BBN_data: samples from 1 GeV to
# 1e-8 keV in T_Jordan, of which those in [0.2 keV, 100 MeV] are in the window
G_N_SAMPLES = 120
G_LOG10_T_MEV_HI = 3.0
G_LOG10_T_MEV_LO = -11.0
G_DENSITY_RTOL = 1e-12
G_RATIO_ATOL = 1e-12
G_CALLBACK_RTOL = 1e-10
G_WALL_CLOCK_LIMIT = 123.0
G_STUB_RESULTS = [3.04, 0.0, 0.0, 0.245, 0.247, 2.46, 1.04, 5.42]

# (h) The constant ratio 0.08 through the callback builder, on this module's
# knots, into PRyMordial, full network. The reference is the same callback on
# the tree before science-readiness prompt 01 (7b518c9): that tree's
# build_NP_callbacks on the same knots, through the "honly" route (its rho_NP,
# the pressure set to -rho_NP and the density derivative to 0, the third LSODA
# component inert), measured by prompt 01's scratch probe
# `honly_builder_reference.py full` (log 01, Verification). That is what the
# patched route must reproduce, so the tolerance is test_prym_passenger (c)'s
# 1e-6.
#
# Until prompt 01 this test compared the builder's callback against the exact
# constant family (prym_fixtures.CONSTANT) to 1e-4 relative. The two callbacks
# differ by a few ulp (the spline of a constant, and the order of the product),
# and PRyMordial moves D/H by 1e-4 under such changes
# (review-remediation board, [03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]):
# the comparison passed at 8.85e-5 on the old route and misses at 1.06e-4 on
# the new one, where the old tree gives the same 1.06e-4 for the same callback
# through the honly route. That offset is PRyMordial's, not the builder's, so it
# is printed and not bounded.
#
# Re-pinned 2026-10-03 by bbn-tolerance prompt 02, which gives the full low-T
# call rtol 1e-5 (it passed none, so SciPy's 1e-3): the same provenance
# re-derived on 7b518c9 with that call at rtol 1e-5 by
# prompts/bbn-tolerance/logs/01-probes/pinned_reference_7b518c9.py
# builder-honly-full --rtol 1e-5 (bbn-tolerance log 01, Verification item 11;
# log 02). The values before, at the default rtol (pinned on 7b518c9,
# unchanged through 2bc124b): Yp 0.2536761805, D/H x1e5 2.648529359. The bound
# is unchanged.
BUILDER_CONST_HONLY_FULL_YP = 0.2536745605
BUILDER_CONST_HONLY_FULL_D_OVER_H_E5 = 2.649973638
END_TO_END_RTOL = 1e-6

# (i) The SM baseline plot_by_beta.py draws: the production network, which is
# the small network since bbn-tolerance prompt 02 (that campaign's U4, P16), at
# its low-T rtol 1e-6. The values are the SM row of
# prompts/bbn-tolerance/logs/01c-probes/scan.csv at lowT_rtol 1e-06 (tag T1;
# tools/bbn_from_store.py --sm-baseline --small-network --lowT-rtol 1e-6 on
# 893a5b1), to 10 significant figures, which the patched tree reproduces with
# no override (bbn-tolerance log 02). Re-pinned 2026-10-03 by bbn-tolerance
# prompt 02. Before, it was review-remediation README section 2 (f) row 1 to
# its quoted figures, the full network at the default low-T rtol (pinned by
# review-remediation prompt 04; unchanged through 2bc124b): Yp 0.24689, D/H
# x1e5 2.4623, 3He/H x1e5 1.042, 7Li/H x1e10 5.423. The bound is unchanged.
README_BASELINE = {
    "Yp_BBN": 0.2468802117,
    "DOverH": 2.458287893,
    "He3OverH": 1.041932695,
    "Li7OverH": 5.486373007,
}
BASELINE_RTOL = 1e-4


def ratio_constant(T_MeV):
    return CONSTANT_RATIO + 0.0 * np.log(T_MeV)


def ratio_oscillating(T_MeV):
    """README section 2 (d): 0.08 + 0.3 sin(2 pi x) exp(-(x/1.5)^2), x = ln(T/0.3 MeV)."""
    x = np.log(T_MeV / 0.3)
    return 0.08 + 0.3 * np.sin(2.0 * pi * x) * np.exp(-((x / 1.5) ** 2))


FAMILIES = {
    "constant": ratio_constant,
    "oscillating": ratio_oscillating,
}


class _RecordingPRyMclass:
    """
    Stands in for PRyMclass in test (g): records the positional and keyword
    arguments it is built with, and returns fixed results inside the output
    checks. No solve.
    """

    calls = []

    def __init__(self, *args, **kwargs):
        type(self).calls.append((args, kwargs))

    def PRyMresults(self):
        return list(G_STUB_RESULTS)


class TestBBNCallbacks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.units = GeV_units()
        cls.eos = SaikawaShirai_EOS_spline(cls.units)
        # staticmethod, so that self.rho_SM(T) is not called as a bound method
        cls.rho_SM = staticmethod(thermodynamic_rho_SM(cls.eos, cls.units))

        # knots, in the order the solver produces them (decreasing T)
        n_knots = int(round(KNOTS_PER_DECADE * log10(T_MAX_MEV / T_MIN_MEV))) + 1
        x_increasing = np.linspace(log(T_MIN_MEV), log(T_MAX_MEV), n_knots)
        cls.x_knots = x_increasing[::-1].copy()
        cls.T_knots = np.exp(cls.x_knots)
        cls.rho_SM_knots = np.array([cls.rho_SM(T) for T in cls.T_knots])

        # the evaluation grid
        cls.x_eval = np.linspace(log(T_EVAL_LO_MEV), log(T_EVAL_HI_MEV), N_EVAL)
        cls.T_eval = np.exp(cls.x_eval)
        cls.rho_SM_eval = np.array([cls.rho_SM(T) for T in cls.T_eval])

    # helpers

    def _ratio_callback(self, family: str):
        ratio = FAMILIES[family]
        return build_rho_NP_callback(
            self.x_knots,
            ratio(self.T_knots),
            self.rho_SM,
            T_min_MeV=T_MIN_MEV,
            T_max_MeV=T_MAX_MEV,
            task_label=f"test-{family}",
        )

    def _asinh_callback(self, family: str):
        """
        The representation before review-remediation prompt 04, rebuilt here
        from its construction (BBNData.py at ec206a3): asinh(rho / MeV^4)
        splined against ln(T/MeV) and inverted with sinh.
        """
        ratio = FAMILIES[family]
        rho_knots = ratio(self.T_knots) * self.rho_SM_knots
        x = self.x_knots[::-1]
        rho_spline = make_interp_spline(x, np.arcsinh(rho_knots[::-1]), k=3)

        def rho(T):
            return float(np.sinh(rho_spline(log(T))))

        return rho

    def _error(self, rho, family: str) -> float:
        """The maximum of |rho(T) - rho_true(T)| / rho_SM(T) on the eval grid."""
        rho_true = FAMILIES[family](self.T_eval) * self.rho_SM_eval
        rho_c = np.array([rho(T) for T in self.T_eval])
        return float(np.max(np.abs(rho_c - rho_true) / self.rho_SM_eval))

    def _report(self, label, error):
        if REPORT:
            print(f"\n[{label}] max rho_NP/rho_SM error {error:.3e}")

    # the cases

    def test_a_constant_ratio_is_exact(self):
        """(a) r = 0.08: rho_NP is r rho_SM to 1e-12 of rho_SM."""
        error = self._error(self._ratio_callback("constant"), "constant")
        self._report("constant, ratio", error)
        self.assertLessEqual(error, CONSTANT_VALUE_TOLERANCE)

    def test_b_oscillating_ratio_bounds(self):
        """(b) README section 2 (d)'s oscillating ratio: <= 2e-8 in
        rho_NP/rho_SM. Prints the maximum."""
        error = self._error(self._ratio_callback("oscillating"), "oscillating")
        print(
            f"\n[test_bbn_callbacks (b)] oscillating ratio: max rho_NP/rho_SM error "
            f"{error:.3e}"
        )
        self.assertLessEqual(error, OSCILLATING_VALUE_BOUND)

    def test_c_no_worse_than_asinh(self):
        """(c) On both families the ratio representation's maximum rho_NP error
        is no larger than the asinh representation's."""
        for family in FAMILIES:
            ratio_error = self._error(self._ratio_callback(family), family)
            asinh_error = self._error(self._asinh_callback(family), family)
            self._report(f"{family}, ratio", ratio_error)
            self._report(f"{family}, asinh", asinh_error)
            with self.subTest(family=family):
                self.assertLessEqual(ratio_error, asinh_error)

    def test_d_non_monotonic_input_is_refused(self):
        """(d) log_T_MeV not strictly decreasing raises ComputationFailureError
        naming the first offending pair; an equal pair is refused too."""
        r = ratio_constant(self.T_knots)
        k = 1000

        swapped = self.x_knots.copy()
        swapped[k], swapped[k + 1] = swapped[k + 1], swapped[k]

        repeated = self.x_knots.copy()
        repeated[k + 1] = repeated[k]

        for label, x in (("swapped", swapped), ("repeated", repeated)):
            with self.subTest(label):
                with self.assertRaises(ComputationFailureError) as ctx:
                    build_rho_NP_callback(
                        x,
                        r,
                        self.rho_SM,
                        T_min_MeV=T_MIN_MEV,
                        T_max_MeV=T_MAX_MEV,
                        task_label="test-non-monotonic",
                    )
                message = str(ctx.exception)
                self.assertIn(f"sample {k} has log(T/MeV)={x[k]:.10g}", message)
                self.assertIn(f"sample {k + 1} has log(T/MeV)={x[k + 1]:.10g}", message)
                self.assertIn("test-non-monotonic", message)

    def test_e_domain_guards(self):
        """(e) Below T_min and above T_max the callback raises
        ComputationFailureError; a negative T returns 0."""
        rho_NP = self._ratio_callback("constant")
        self.assertEqual(rho_NP(-1.0), 0.0)
        with self.assertRaises(ComputationFailureError):
            rho_NP(0.5 * T_MIN_MEV)
        with self.assertRaises(ComputationFailureError):
            rho_NP(2.0 * T_MAX_MEV)
        # the endpoints themselves are inside the domain
        rho_NP(T_MIN_MEV)
        rho_NP(T_MAX_MEV)

    def test_f_units(self):
        """(f) T in MeV gives MeV^4. rho_SM(1 MeV) is about 3.5 MeV^4 and is the
        same whatever units the EOS was built in, and rho_NP(T) / T^4 is
        (pi^2/30) g_rho(T) r: wrong by a power of the unit if T were not in MeV."""
        rho_SM_1 = self.rho_SM(1.0)
        self.assertGreater(rho_SM_1, RHO_SM_1MEV_LO)
        self.assertLess(rho_SM_1, RHO_SM_1MEV_HI)

        planck = Planck_units()
        rho_SM_planck = thermodynamic_rho_SM(SaikawaShirai_EOS_spline(planck), planck)
        for T in (0.02, 0.3, 1.0, 5.0):
            with self.subTest(T=T):
                self.assertAlmostEqual(
                    rho_SM_planck(T) / self.rho_SM(T), 1.0, delta=UNITS_AGREEMENT_RTOL
                )

        rho_NP = self._ratio_callback("constant")
        self.assertAlmostEqual(rho_NP(1.0), CONSTANT_RATIO * rho_SM_1, delta=1e-12)
        for T in (0.1, 2.0):
            with self.subTest(T=T):
                g = float(self.eos.G_rho(T * 1e-3 * self.units.GeV))
                self.assertAlmostEqual(
                    rho_NP(T) / T**4 / ((pi * pi / 30.0) * g * CONSTANT_RATIO),
                    1.0,
                    delta=1e-10,
                )

    def test_g_compute_BBN_data_hands_prymordial_the_density(self):
        """(g) compute_BBN_data on a stand-in history (PRyMclass stubbed: no
        solve). Replaces the test of the Jordan-frame Hdot/H^2 expression, which
        built the pressure the Hubble-only route no longer reads (science-
        readiness prompt 01). Each sample in [0.2 keV, 100 MeV] (the default
        floor since science-readiness prompt 06; it was 1e-4 keV) is kept, with
        density_NP = 3 M_P^2 H_J^2 - rho_R,J (1 + f_m) and
        density_NP_ratio = density_NP / rho_R,J; the samples carry exactly
        raw_N, log_T_Jordan, density_NP and density_NP_ratio; PRyMclass is built
        with one positional callback, which at every sample temperature is
        ratio * rho_SM(T), and with the wall-clock limit passed in. No potential
        or coupling is needed: nothing evaluates the field equation."""
        units = Planck_units()
        eos = SaikawaShirai_EOS_spline(units)
        cosmology = SimpleNamespace(units=units, G_rho=eos.G_rho)
        M_P2 = units.PlanckMass * units.PlanckMass

        values, expected = [], []
        log10_T = np.linspace(G_LOG10_T_MEV_HI, G_LOG10_T_MEV_LO, G_N_SAMPLES)
        for i, lt in enumerate(log10_T):
            T = 10.0**lt * units.MeV
            rho_R = (pi * pi / 30.0) * 10.0 * T**4
            f_m = 1e-3 * (units.MeV / T) ** 0.5
            r = 0.05 + 0.01 * sin(0.7 * i)
            H_J = sqrt(rho_R * (1.0 + f_m + r) / (3.0 * M_P2))
            values.append(
                SimpleNamespace(
                    z=SimpleNamespace(store_id=i),
                    raw_N=0.1 * i,
                    log_T_Jordan=log(T),
                    log_rhorad_Jordan=log(rho_R),
                    log_fm=log(f_m),
                    H_Jordan=H_J,
                )
            )
            # the stored values are logarithms; compute_BBN_data exponentiates them
            T_s, rho_R_s, f_m_s = exp(log(T)), exp(log(rho_R)), exp(log(f_m))
            if 0.2 * units.keV <= T_s <= 100.0 * units.MeV:
                expected.append((i, T_s, rho_R_s, f_m_s, H_J))

        model = SimpleNamespace(
            _cosmology=cosmology,
            potential=None,
            coupling=None,
            T_Jordan_stop=SimpleNamespace(as_float=1e-3 * units.eV),
            values=values,
        )
        proxy = SimpleNamespace(get=lambda: model)

        import PRyM.PRyM_main as PRyMmain

        _RecordingPRyMclass.calls = []
        with SavedPRyMGlobals(), mock.patch.object(
            PRyMmain, "PRyMclass", _RecordingPRyMclass
        ):
            result = compute_BBN_data._function(
                proxy, task_label="test-g", wall_clock_limit=G_WALL_CLOCK_LIMIT
            )

        self.assertFalse(result.get("failure", False), result)
        samples = result["samples"]
        self.assertEqual(len(samples), len(expected))
        self.assertGreaterEqual(len(expected), 4)
        self.assertEqual(
            set(samples[0]._fields),
            {"raw_N", "log_T_Jordan", "density_NP", "density_NP_ratio"},
        )

        rho_SM_MeV4 = thermodynamic_rho_SM(cosmology, units)
        self.assertEqual(len(_RecordingPRyMclass.calls), 1)
        args, kwargs = _RecordingPRyMclass.calls[0]
        self.assertEqual(len(args), 1)
        self.assertEqual(kwargs, {"wall_clock_limit": G_WALL_CLOCK_LIMIT})
        rho_NP = args[0]

        for sample, (i, T, rho_R, f_m, H_J) in zip(samples, expected):
            with self.subTest(i=i):
                density = H_J * H_J * (3.0 * M_P2) - rho_R * (1.0 + f_m)
                self.assertEqual(sample.raw_N, 0.1 * i)
                self.assertLessEqual(
                    abs(sample.density_NP - density) / abs(density), G_DENSITY_RTOL
                )
                self.assertAlmostEqual(
                    sample.density_NP_ratio, density / rho_R, delta=G_RATIO_ATOL
                )
                T_MeV = T / units.MeV
                target = sample.density_NP_ratio * rho_SM_MeV4(T_MeV)
                self.assertLessEqual(
                    abs(rho_NP(T_MeV) - target) / abs(target), G_CALLBACK_RTOL
                )

    def test_h_end_to_end_constant_ratio(self):
        """(h) The constant ratio 0.08 through build_rho_NP_callback into
        PRyMordial's Hubble-only route (full network) reproduces the same
        callback through the honly route on 7b518c9, Yp and D/H to 1e-6
        relative. The offset from the exact constant family's const-honly
        values is printed, not bounded (see END_TO_END_RTOL).
        **Runs one PRyMordial solve, about 10 s.**"""
        res = run_prym(self._ratio_callback("constant"))

        Yp, DoH = res[RES_YP_BBN], res[RES_D_OVER_H_E5]
        dYp = abs(Yp - BUILDER_CONST_HONLY_FULL_YP) / BUILDER_CONST_HONLY_FULL_YP
        dDoH = (
            abs(DoH - BUILDER_CONST_HONLY_FULL_D_OVER_H_E5)
            / BUILDER_CONST_HONLY_FULL_D_OVER_H_E5
        )
        dYp_exact = abs(Yp - CONST_HONLY_FULL_YP) / CONST_HONLY_FULL_YP
        dDoH_exact = (
            abs(DoH - CONST_HONLY_FULL_D_OVER_H_E5) / CONST_HONLY_FULL_D_OVER_H_E5
        )
        print(
            f"\n[test_bbn_callbacks (h)] Yp {Yp:.10g} ({dYp:.2e}), "
            f"D/H x1e5 {DoH:.10g} ({dDoH:.2e}) against the builder's honly reference; "
            f"against the exact family's const-honly (full): Yp {dYp_exact:.2e}, "
            f"D/H {dDoH_exact:.2e} (not bounded)"
        )
        with self.subTest("Yp"):
            self.assertLessEqual(dYp, END_TO_END_RTOL)
        with self.subTest("D/H"):
            self.assertLessEqual(dDoH, END_TO_END_RTOL)

    def test_i_SM_baseline(self):
        """(i) compute_SM_baseline(True), the production network since
        bbn-tolerance prompt 02, reproduces bbn-tolerance log 01c's small-network
        SM row at low-T rtol 1e-6 to 1e-4 relative in all four abundances, and
        names the PRyMordial version.
        **Runs one small-network PRyMordial solve, about 10 s.**"""
        with SavedPRyMGlobals():
            baseline = compute_SM_baseline(True)

        print(
            "\n[test_bbn_callbacks (i)] baseline "
            + ", ".join(f"{k} {baseline[k]:.10g}" for k in README_BASELINE)
        )
        self.assertIs(baseline["small_network"], True)
        self.assertEqual(baseline["PRyM_version"], PRYM_VERSION)
        for key, reference in README_BASELINE.items():
            with self.subTest(key):
                self.assertLessEqual(
                    abs(baseline[key] - reference) / reference, BASELINE_RTOL
                )


if __name__ == "__main__":
    unittest.main()
