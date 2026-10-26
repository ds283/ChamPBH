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
The model interface (jordan-normalization prompt 04, M7): `AbstractModel`, `RealizedModel` and
`ExponentialModel` (CosmologyConcepts/Models/).

- (a) `TestTheNames`: the model's name, type id and shard key, on the golden histories' model
  realized at 4 log Lambda and phi_ref = 0. Until jordan-normalization prompt 07 this class was
  `TestTheAdapterIsBitwise`, whose two other tests compared the adapter `ExponentialModel.from_rows`
  with the potential and coupling rows bit for bit, and refused rows of another class; prompt 07
  deleted the rows and the adapter, and those two tests with them.
- (b) `TestTheReferencePoint`: A(phi_ref) = 1 exactly, for five phi_ref, on `ExponentialModel` and
  on `_MinimalModel`, a test-only model that implements only the hooks and does nothing about
  phi_ref. A base class that did not subtract ln A(phi_ref) fails here.
- (c) and (d) `TestTheGoldenHistories`: both golden histories (golden_history.py) are run once in
  setUpClass, about 10 s together (0.6 s at beta = 1.5, 9.4 s at beta = 5.5, at a load of about 7).
  - (c) the shift law: at every sample of the beta = 1.5 history, `fixed_T_value_at` itself, on a
    one-step stub result whose interpolant returns the sample's Einstein-frame state, gives
    1 + r + f_m; realized at phi_ref = delta it is the phi_ref = 0 value times e^{-2 beta delta/M_P},
    for delta = -0.01, +0.03 and +0.1 M_P, to 1e-12 of the rounding scale of the sum,
    (1 + f_m + |r|) e^{-2 beta delta/M_P} (see the test);
  - (d) Eq. 7: at every stored sample of both histories, bounces included,
    3 M_P^2 H_J^2/rho_R,J = A^2 (1 + beta phi'/M_P)^2/(1 - phi'^2/6 M_P^2) (1 + f_m + V/rho_R,E), to
    1e-12 relative, with A, V and beta/M_P from the realized model.
- (e) `TestRenormalize`: `renormalize` makes V(phi) = V_target to 1e-12 relative, and the
  normalization coordinate round-trips exactly.
- (f) `TestEffectiveMinimum`: the generic root agrees with a bisection to the last representable
  halving to 1e-12 relative at three states, and is a minimum.
- (g) `TestSeed`: `seed` reproduces the planning probe's analytic seeds.
- (h) `TestPickle`: a `RealizedModel` survives a pickle round trip, as Ray sends it to a worker.

Relative difference means |a - b|/max(|a|, |b|), and 0 where both are 0. No Ray cluster, no
datastore, no PRyMordial solve.
"""

import contextlib
import io
import pickle
import unittest
from math import exp, log
from types import SimpleNamespace

import importlib
import numpy as np

from ComputeTargets.tests import golden_history as gh
from CosmologyConcepts import M_value, beta_value
from CosmologyConcepts.Models import (
    AbstractModel,
    ExponentialModel,
    RealizedModel,
    Seed,
)
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import Planck_units

# ComputeTargets re-exports the ScalarHistory class under the module's name, so import the module
SM = importlib.import_module("ComputeTargets.ScalarHistory")

_units = Planck_units()
Mp = _units.PlanckMass

# the methods the integrator reads: V's, and the conformal factor's
POTENTIAL_METHODS = ("log_V", "d_logV_dphi", "d2_logV_dphi2")
COUPLING_METHODS = ("log_Omega", "Omega", "d_logOmega_dphi", "d2_logOmega_dphi2")

PHI_REFS_OVER_MP = (0.0, 0.01, 0.0349624, 0.3, 5.0)
DELTAS_OVER_MP = (-0.01, 0.03, 0.1)

_cosmology = None


def cosmology():
    """QCD_Cosmology on Planck2018 in Planck_units, built once (it prints its name)."""
    global _cosmology
    if _cosmology is None:
        with contextlib.redirect_stdout(io.StringIO()):
            _cosmology = QCD_Cosmology(0, _units, Planck2018())
    return _cosmology


def rel(a: float, b: float) -> float:
    scale = max(abs(a), abs(b))
    return 0.0 if scale == 0.0 else abs(a - b) / scale


def exponential_model(beta: float, M_over_Mp: float = 1.0e-3, n: int = 1):
    return ExponentialModel(
        0, M_value(0, M_over_Mp * Mp), n, beta_value(0, beta), _units
    )


class _MinimalModel(AbstractModel):
    """
    A test-only model with only the hooks (and the identity a DatastoreObject needs): constant V,
    and ln Omega = gamma phi, with no handling of phi_ref.
    """

    def __init__(self, gamma: float):
        super().__init__(0, _units)
        self._gamma = gamma

    @property
    def name(self) -> str:
        return f"MinimalModel(gamma={self._gamma:g})"

    @property
    def type_id(self) -> int:
        return -1

    @property
    def shard_key(self):
        return None

    def _log_V(self, normalization, phi) -> float:
        return normalization

    def _d_logV_dphi(self, normalization, phi) -> float:
        return 0.0

    def _d2_logV_dphi2(self, normalization, phi) -> float:
        return 0.0

    def _log_Omega(self, phi) -> float:
        return self._gamma * phi

    def _d_logOmega_dphi(self, phi) -> float:
        return self._gamma

    def _d2_logOmega_dphi2(self, phi) -> float:
        return 0.0


def golden_realized_model(beta: float) -> RealizedModel:
    """The golden histories' model, realized at their normalization and phi_ref = 0."""
    return gh.model(beta, _units).realize(gh.normalization(_units), 0.0)


class TestTheNames(unittest.TestCase):
    """(a) The model's name, type id and shard key."""

    def test_a_the_names(self):
        rm = golden_realized_model(1.5)
        self.assertEqual(rm.model.name, "ExponentialModel(M=0.001Mp,n=1,beta=1.5)")
        self.assertTrue(rm.name.startswith(rm.model.name), rm.name)
        self.assertEqual(rm.model.type_id, 1)
        self.assertEqual(float(rm.model.shard_key), 1.5)


class TestTheReferencePoint(unittest.TestCase):
    """(b) A(phi_ref) = 1 exactly; the base class subtracts ln A(phi_ref), not the model."""

    def check(self, model: AbstractModel, normalization: float):
        for x in PHI_REFS_OVER_MP:
            phi_ref = x * Mp
            rm = model.realize(normalization, phi_ref)
            self.assertEqual(rm.log_Omega(phi_ref), 0.0, (model.name, x))
            self.assertEqual(rm.Omega(phi_ref), 1.0, (model.name, x))
            self.assertEqual(rm.phi_ref, phi_ref)
            self.assertEqual(rm.normalization, normalization)

    def test_b_exponential_model(self):
        self.check(exponential_model(1.5), 4.0 * log(1.0e-3 * _units.eV))

    def test_b_a_model_that_does_nothing_about_phi_ref(self):
        self.check(_MinimalModel(gamma=2.0), -276.0)

    def test_b_a_realized_model_is_immutable(self):
        rm = exponential_model(1.5).realize(-276.0, 0.0)
        with self.assertRaises(AttributeError):
            rm._phi_ref = 1.0
        with self.assertRaises(AttributeError):
            rm.phi_ref = 1.0


class TestTheGoldenHistories(unittest.TestCase):
    """
    (c) the shift law and (d) Eq. 7, on the golden histories, run once here: about 10 s
    (0.6 s at beta = 1.5 and 9.4 s at beta = 5.5, at a load of about 7).
    """

    @classmethod
    def setUpClass(cls):
        with contextlib.redirect_stdout(io.StringIO()):
            cls.data = {beta: gh.run_history(beta) for beta in gh.BETAS}
        cls.models = {beta: golden_realized_model(beta) for beta in gh.BETAS}

    def r_and_fm(self, policy, model, s) -> tuple:
        """(r, f_m): fixed_T_value_at's r, on a stub result holding the sample's state, and f_m."""
        state = (
            s.phi_Einstein,
            s.pi_Einstein,
            s.log_rhorad_Einstein,
            s.log_fm,
            s.log_T_Jordan,
        )
        stub = SimpleNamespace(
            solution=SimpleNamespace(interpolants=[lambda N, state=state: state])
        )
        _, r = SM.fixed_T_value_at(stub, 0, s.raw_N, policy, model, _units)
        return r, exp(s.log_fm)

    def test_c_the_shift_law(self):
        """
        v = 1 + r + f_m at phi_ref = delta against v_ref e^{-2 beta delta/M_P}, v_ref at phi_ref = 0.
        The difference is measured against (1 + f_m + |r|) e^{-2 beta delta/M_P}, r that of
        phi_ref = 0: the rounding scale of the summed terms. A plain relative difference measures
        the wrong thing at the T_J turning points (1 + beta phi'/M_P about -2.2e-3, H_J about 0):
        there fixed_T_value_at's r is about -(1 + f_m), 1 + r + f_m about 5e-6 is a cancelled sum,
        and the relative difference (up to 2.8e-11) is r's rounding, not the law (the
        orchestrator's ruling after prompt 04's stop). The plain relative difference is printed
        for the record.
        """
        beta = 1.5
        plain = self.models[beta]
        self.assertEqual(plain.phi_ref, 0.0)
        samples = self.data[beta]["sample"]
        cosmo = cosmology()

        plain_policy = SM.ODEPolicy("test_model_interface", cosmo, plain)
        reference = [self.r_and_fm(plain_policy, plain, s) for s in samples]

        worst, worst_rel = (0.0, None), (0.0, None)
        for x in DELTAS_OVER_MP:
            delta = x * Mp
            shifted = plain.model.realize(plain.normalization, delta)
            policy = SM.ODEPolicy("test_model_interface", cosmo, shifted)
            factor = exp(-2.0 * beta * delta / Mp)
            for i, s in enumerate(samples):
                r, fm = self.r_and_fm(policy, shifted, s)
                r_ref, fm_ref = reference[i]
                value = 1.0 + r + fm
                expected = (1.0 + r_ref + fm_ref) * factor
                scale = (1.0 + fm_ref + abs(r_ref)) * factor
                d = abs(value - expected) / scale
                worst = max(worst, (d, (x, i)))
                worst_rel = max(worst_rel, (rel(value, expected), (x, i)))
                self.assertLessEqual(d, 1e-12, (x, i))

        print(
            f"\n[test_model_interface (c)] shift law, beta = 1.5, {len(samples)} samples x "
            f"{len(DELTAS_OVER_MP)} delta: largest difference on the scale 1 + f_m + |r| "
            f"{worst[0]:.3e} (delta/M_P, sample) = {worst[1]}; largest plain relative "
            f"difference {worst_rel[0]:.3e} at {worst_rel[1]}"
        )

    def test_d_eq_7_at_the_stored_samples(self):
        worst = {}
        for beta in gh.BETAS:
            rm = self.models[beta]
            worst[beta] = (0.0, None)
            for i, s in enumerate(self.data[beta]["sample"]):
                phi, pi = s.phi_Einstein, s.pi_Einstein
                lhs = 3.0 * Mp**2 * s.H_Jordan**2 / exp(s.log_rhorad_Jordan)

                A = rm.Omega(phi)
                bp = 1.0 + rm.d_logOmega_dphi(phi) * pi
                G = 1.0 - pi * pi / (6.0 * Mp * Mp)
                V = exp(rm.log_V(phi))
                rhs = (
                    A**2
                    * bp**2
                    / G
                    * (1.0 + exp(s.log_fm) + V / exp(s.log_rhorad_Einstein))
                )

                d = rel(lhs, rhs)
                if d > worst[beta][0]:
                    worst[beta] = (d, i)
                self.assertLessEqual(d, 1e-12, (beta, i))

        print(
            "\n[test_model_interface (d)] Eq. 7: largest relative difference "
            + ", ".join(
                f"{worst[b][0]:.3e} at beta = {b} (sample {worst[b][1]})"
                for b in gh.BETAS
            )
        )


class TestRenormalize(unittest.TestCase):
    """(e) The frozen-state step and the normalization coordinate."""

    def test_e_renormalize_reaches_the_target(self):
        model = exponential_model(1.5)
        normalization = 4.0 * log(1.0e-3 * _units.eV)
        for x in (1.0e-4, 0.0349624, 2.0):
            phi = x * Mp
            for V_target in (1.0e-121, 3.0e-120):
                new = model.renormalize(normalization, phi, V_target)
                V = exp(model.realize(new, 0.0).log_V(phi))
                self.assertLessEqual(rel(V, V_target), 1e-12, (x, V_target))

    def test_e_a_target_that_is_not_positive_is_refused(self):
        model = exponential_model(1.5)
        for V_target in (0.0, -1.0e-121):
            with self.assertRaises(ValueError):
                model.renormalize(-276.0, 0.03 * Mp, V_target)

    def test_e_the_coordinate_round_trips_exactly(self):
        model = exponential_model(1.5)
        for normalization in (-279.87, 4.0 * log(2.125913e-3 * _units.eV), 0.0, 12.5):
            x = model.normalization_coordinate(normalization)
            self.assertEqual(model.normalization_from_coordinate(x), normalization)


def bisect_to_the_last_halving(f, low: float, high: float) -> float:
    """The root of f in [low, high] by bisection, until the midpoint is no longer between them."""
    f_low = f(low)
    while True:
        mid = 0.5 * (low + high)
        if not low < mid < high:
            return low
        f_mid = f(mid)
        if f_mid == 0.0:
            return mid
        if (f_mid < 0.0) == (f_low < 0.0):
            low, f_low = mid, f_mid
        else:
            high = mid


class TestEffectiveMinimum(unittest.TestCase):
    """
    (f) The generic effective_minimum, against a bisection. The three states are README §0.3's
    converged points at M = 1e-3 M_P, phi* = 5 M_P: Lambda (meV), phi_ref/M_P and phi_min/phi_0 of
    its table give the normalization 4 ln Lambda, phi_ref, and phi_0 = phi_ref/(phi_min/phi_0); the
    matter density today is rho_m,E,0 = A(phi_0)^4 rho_m0, with Planck2018's rho_m0.
    """

    STATES = (
        # beta, Lambda in meV, phi_ref/M_P, phi_min/phi_0
        (1.5, 2.125913, 0.0349624, 1.046),
        (5.5, 2.021177, 0.0167195, 1.006),
        (25.0, 1.843465, 0.0068090, 1.000),
    )

    def test_f_the_root_is_the_bisection_root_and_a_minimum(self):
        rho_m0 = cosmology().rho_m0
        for beta, Lambda_meV, phi_ref_over_Mp, ratio in self.STATES:
            model = exponential_model(beta)
            normalization = 4.0 * log(Lambda_meV * 1.0e-3 * _units.eV)
            phi_ref = phi_ref_over_Mp * Mp
            phi_0 = phi_ref / ratio
            rm = model.realize(normalization, phi_ref)
            rho_m_E_0 = exp(4.0 * rm.log_Omega(phi_0)) * rho_m0

            root = model.effective_minimum(normalization, phi_ref, phi_0, rho_m_E_0)

            def dV_eff(x):
                return exp(rm.log_V(x)) * rm.d_logV_dphi(x) + rm.d_logOmega_dphi(
                    x
                ) * rho_m_E_0 * exp(rm.log_Omega(x) - rm.log_Omega(phi_0))

            reference = bisect_to_the_last_halving(dV_eff, 0.05 * phi_0, 20.0 * phi_0)
            self.assertLessEqual(rel(root, reference), 1e-12, beta)
            self.assertLess(dV_eff(root * (1.0 - 1e-6)), 0.0, beta)
            self.assertGreater(dV_eff(root * (1.0 + 1e-6)), 0.0, beta)

    def test_f_a_bracket_without_a_sign_change_is_refused(self):
        beta, Lambda_meV, phi_ref_over_Mp, ratio = self.STATES[0]
        model = exponential_model(beta)
        normalization = 4.0 * log(Lambda_meV * 1.0e-3 * _units.eV)
        phi_0 = phi_ref_over_Mp * Mp / ratio
        with self.assertRaises(ValueError) as ctx:
            model.effective_minimum(
                normalization,
                phi_ref_over_Mp * Mp,
                phi_0,
                cosmology().rho_m0,
                bracket=(10.0 * phi_0, 20.0 * phi_0),
            )
        self.assertIn("bracket", str(ctx.exception))


class TestSeed(unittest.TestCase):
    """
    (g) The seed. The expected values were generated by a copy of analytic() of
    prompts/jordan-normalization/source/planning-probes-2026-10-20/analytic_seed.py, at this
    tree's units and cosmology (Planck_units, QCD_Cosmology on Planck2018), in the implementing
    agent's scratch directory (agent04/analytic_copy.py); the probe imports
    ComputeTargets.ScalarModel and cannot be imported here. Lambda^4 = V e^{-M/phi} and phi are
    the probe's; the plain normalization is ln(3 M_P^2 H0^2 (1 - Omega_m)).
    """

    def check_analytic(self, seed: Seed, kind: str, Lambda4_hex: str, phi_hex: str):
        self.assertIsInstance(seed, Seed)
        self.assertEqual(seed.kind, kind)
        self.assertLessEqual(
            rel(exp(seed.normalization), float.fromhex(Lambda4_hex)), 1e-12
        )
        self.assertLessEqual(rel(seed.phi_ref, float.fromhex(phi_hex)), 1e-12)

    def test_g_tracking_at_beta_1_5(self):
        # the probe: Lambda = 2.110131124 meV, phi = 3.433537145e-2 M_P
        seed = exponential_model(1.5, 1.0e-3).seed(cosmology())
        self.check_analytic(
            seed, "tracking", "0x1.74311d9d83c1dp-400", "0x1.19467e2f45537p-5"
        )

    def test_g_static_where_tracking_has_no_solution(self):
        # the tracking loop raises (math domain error); static: Lambda = 1.943956227 meV,
        # phi = 8.590899833e-1 M_P
        seed = exponential_model(1.5, 0.5).seed(cosmology())
        self.check_analytic(
            seed, "static", "0x1.0c1627f2edec7p-400", "0x1.b7daa46d0060cp-1"
        )

    def test_g_plain_where_neither_has_a_solution(self):
        # beta = -1.5: both loops raise (math domain error)
        plain = float.fromhex("-0x1.14a16ebc655f3p+8")
        seed = exponential_model(-1.5, 1.0e-3).seed(cosmology())
        self.assertEqual(seed, Seed(kind="plain", normalization=plain, phi_ref=0.0))
        # beta = 0: both loops divide by zero
        seed = exponential_model(0.0, 1.0e-3).seed(cosmology())
        self.assertEqual(seed, Seed(kind="plain", normalization=plain, phi_ref=0.0))
        # n != 1: the analytic seeds assume n = 1
        seed = exponential_model(1.5, 1.0e-3, n=2).seed(cosmology())
        self.assertEqual(seed, Seed(kind="plain", normalization=plain, phi_ref=0.0))
        # the base class
        self.assertEqual(
            _MinimalModel(gamma=2.0).seed(cosmology()),
            Seed(kind="plain", normalization=plain, phi_ref=0.0),
        )


class TestPickle(unittest.TestCase):
    """(h) A RealizedModel survives a pickle round trip, as Ray sends it to a worker."""

    def test_h_round_trip(self):
        plain = golden_realized_model(5.5)
        for rm in (plain, plain.model.realize(plain.normalization, 0.03 * Mp)):
            loaded = pickle.loads(pickle.dumps(rm))
            self.assertIsInstance(loaded, RealizedModel)
            self.assertEqual(loaded.name, rm.name)
            self.assertEqual(loaded.normalization, rm.normalization)
            self.assertEqual(loaded.phi_ref, rm.phi_ref)
            self.assertEqual(loaded.log_V_floor, rm.log_V_floor)
            self.assertEqual(loaded.reflects_at_origin, rm.reflects_at_origin)
            for phi in (1.0e-4 * Mp, 0.03 * Mp, 2.0 * Mp):
                for method in POTENTIAL_METHODS + COUPLING_METHODS:
                    self.assertEqual(
                        getattr(loaded, method)(phi), getattr(rm, method)(phi)
                    )


if __name__ == "__main__":
    unittest.main()
