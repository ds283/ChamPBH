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
The one-loop drain law on the power wall (power-law-wall prompt 03 §1.2, §1.4, §3; README §6.5 R6, R7, §6.6 P15-P18;
orchestrator note 03, rulings 3 and 4).

- (a) The drain models: law_of of one-loop-inverse-power-v1 is the one-loop law; DRAIN_MODELS keeps the two it had first;
  model_of_law is gone (power-law-wall prompt 07c, P101); alpha_value accepts the pair (A, one-loop-inverse-power-v1) and refuses A = 0 under it; the
  regimes are the same objects in drain_model and ComputeTargets.ScalarHistory.
- (b) The hooks: each model's one-loop drain model (ExponentialModel one-loop-exponential-v2; InversePowerModel at n = 1, 2
  one-loop-inverse-power-v1, a ValueError naming the class, the hook and n at n = 3), realized or not; AbstractModel's
  _drain_fraction and one_loop_drain_model raise, naming the class.
- (c) The check at entry (check_drain_law_model) under the one-loop law: refuses a model with no _drain_fraction
  (since power-law-wall prompt 07c, README §6.14 P99, the fraction is required and _drain_L optional, so a model with
  both is accepted), one with no one_loop_drain_model, and InversePowerModel at n = 3, naming the class,
  the hooks and n; accepts InversePowerModel at n = 1, 2 and ExponentialModel; the fixed law checks nothing.
- (d) The law: InversePowerModel._drain_fraction against fraction_power, copied verbatim below from
  prompts/power-law-wall/source/wall/fit_fn.py with its literals (ruling 3), bitwise, at n = 1, 2 over
  ln R in [-20, 600] (through the switch and ln R = 0) under two UnitsLike; the classical branch R S_n; the continuity at
  the switch; 0 unassessed; the regime refused when unknown; and through drain_alpha_effective with the model:
  min(0.99, A f) w_gate, reading neither ell nor L.
- (e) A short one-loop integration on the power wall (n = 2, M = 4e-31 M_P, beta = 2, A = 1, untuned at the seed, from
  T* = 2e4 GeV to T_J = 100 MeV): it succeeds, its record names one-loop-inverse-power-v1, and every candidate has L None,
  and alpha_eff equal to drain_alpha_effective at its own regime, ln E and ln R.

Everything but (e) is instant; (e) is timed in its docstring. No Ray, no store, no PRyMordial.
"""

import contextlib
import importlib
import io
import time
import unittest
from math import exp, log, nextafter

import numpy as np

from ComputeTargets.tests.test_model_interface import cosmology
from CosmologyConcepts import M_value, alpha_value, beta_value, temperature
from CosmologyConcepts import drain_model as dm
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.AbstractModel import AbstractModel
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from CosmologyConcepts.Models.InversePowerModel import InversePowerModel
from Units import GeV_units, Planck_units

SM = importlib.import_module("ComputeTargets.ScalarHistory")

_units = Planck_units()
Mp = _units.PlanckMass
NORMALIZATION = -276.63

# ---- the reference, verbatim (ruling 3) ------------------------------------------------------------------------------
#
# fraction_power and its literals, copied verbatim from prompts/power-law-wall/source/wall/fit_fn.py (power-law-wall
# prompt 03's commit)

FN_S = {1: 1.02620e-2, 2: 5.21615e-3}
FN_G = {1: 2.86769e-3, 2: 2.85913e-3}
FN_R0 = {1: -1.27414, 2: -1.10903}
FN_P = {1: 2.22126, 2: 2.45613}
FN_A = {1: 27.9671, 2: 19.6100}
FN_C = {1: 14.9835, 2: 10.9039}
FN_SWITCH = {1: 23.08943252571177, 2: 16.710187997698853}


def fraction_power(n: int, log_R: float) -> float:
    """f_n(ln R) on the power wall (R6, R7): exp(ln R) S_n at ln R <= 0; else 1 - exp(-y), y the rising branch
    G (ln R - R0)^P below the switch and the falling branch A/(ln R - c) at and above it.
    """
    if log_R <= 0.0:
        return exp(log_R) * FN_S[n]
    if log_R < FN_SWITCH[n]:
        y = FN_G[n] * (log_R - FN_R0[n]) ** FN_P[n]
    else:
        y = FN_A[n] / (log_R - FN_C[n])
    return 1.0 - exp(-y)


# ---- the models -------------------------------------------------------------------------------------------------------


def power(n: int, units=_units, M_over_Mp: float = 4.0e-31) -> InversePowerModel:
    Mp_u = units.PlanckMass
    return InversePowerModel(
        0, M_value(0, M_over_Mp * Mp_u), n, beta_value(0, 2.0), units
    )


def exponential(n: int = 1) -> ExponentialModel:
    return ExponentialModel(0, M_value(0, 4.0e-31 * Mp), n, beta_value(0, 2.0), _units)


class _Neither(ExponentialModel):
    """ExponentialModel with AbstractModel's _drain_L and _drain_fraction restored: neither L nor its own fraction."""

    _drain_L = AbstractModel._drain_L
    _drain_fraction = AbstractModel._drain_fraction


class _Both(InversePowerModel):
    """InversePowerModel with a _drain_L as well as its own fraction."""

    def _drain_L(self, normalization, log_E):
        return 1.0


class _NoDrainModel(ExponentialModel):
    """ExponentialModel without its one-loop drain model."""

    one_loop_drain_model = AbstractModel.one_loop_drain_model


def log_R_grid():
    """ln R over [-20, 600]: a uniform grid, 0 and its neighbours, the switches and theirs, and the fit's windows."""
    g = list(np.linspace(-20.0, 600.0, 6201))
    g += [0.0, nextafter(0.0, 1.0), nextafter(0.0, -1.0), 5e-324, 0.3, 20.0, 28.0]
    for s in FN_SWITCH.values():
        g += [s, nextafter(s, 0.0), nextafter(s, 100.0)]
    return [float(v) for v in g]


class TestTheDrainModels(unittest.TestCase):
    """(a)"""

    def test_a_the_new_model(self):
        self.assertEqual(
            dm.DRAIN_MODEL_ONE_LOOP_INVERSE_POWER, "one-loop-inverse-power-v1"
        )
        self.assertEqual(
            dm.law_of(dm.DRAIN_MODEL_ONE_LOOP_INVERSE_POWER), dm.DRAIN_LAW_ONE_LOOP
        )
        self.assertEqual(
            dm.DRAIN_MODELS,
            ("fixed", "one-loop-exponential-v2", "one-loop-inverse-power-v1"),
        )
        self.assertFalse(hasattr(dm, "model_of_law"))
        self.assertEqual(
            dm.check_drain_model(dm.DRAIN_MODEL_ONE_LOOP_INVERSE_POWER, 1.0, "test"),
            dm.DRAIN_LAW_ONE_LOOP,
        )
        self.assertEqual(
            dm.alpha_label(1.0, dm.DRAIN_MODEL_ONE_LOOP_INVERSE_POWER),
            "A = 1 (one-loop-inverse-power-v1)",
        )

    def test_a_the_retired_names_are_refused(self):
        """
        Power-law-wall prompt 07c (README §6.14 P101): "one-loop-measured-v1" (R13) and "one-loop-measured-pl-v1" (U11)
        stay known and are refused wherever a drain model is read, each with a message naming its ruling and its
        successor: law_of, check_drain_model, alpha_label, alpha_value, and alpha_value's factory before any query.
        """
        from ObjectFactories.alpha_value import sqla_alpha_value_factory

        self.assertEqual(
            set(dm.RETIRED_DRAIN_MODELS),
            {"one-loop-measured-v1", "one-loop-measured-pl-v1"},
        )
        for name, ruling, successor in (
            ("one-loop-measured-v1", "R13", "one-loop-exponential-v2"),
            ("one-loop-measured-pl-v1", "U11", "one-loop-inverse-power-v1"),
        ):
            self.assertNotIn(name, dm.DRAIN_MODELS)
            for call in (
                lambda: dm.law_of(name, "ctx"),
                lambda: dm.check_drain_model(name, 1.0, "ctx"),
                lambda: dm.alpha_label(1.0, name),
                lambda: alpha_value(0, 1.0, name),
                lambda: sqla_alpha_value_factory().build(
                    {"value": 1.0, "drain_model": name}, None, None, None, None, None
                ),
            ):
                with self.subTest(name=name):
                    with self.assertRaises(ValueError) as ctx:
                        call()
                    message = str(ctx.exception)
                    for needle in (name, "retired", ruling, successor):
                        self.assertIn(needle, message)

    def test_a_alpha_value_takes_the_pair(self):
        row = alpha_value(0, 1.0, dm.DRAIN_MODEL_ONE_LOOP_INVERSE_POWER)
        self.assertEqual(row.drain_model, "one-loop-inverse-power-v1")
        self.assertEqual(float(row), 1.0)
        for bad in (0.0, -1.0, float("inf")):
            with self.assertRaises(ValueError):
                alpha_value(0, bad, dm.DRAIN_MODEL_ONE_LOOP_INVERSE_POWER)

    def test_a_the_regimes_are_one_set(self):
        for name in (
            "REGIME_ONE_LOOP",
            "REGIME_GAP",
            "REGIME_CLASSICAL",
            "REGIME_UNASSESSED",
            "DRAIN_REGIMES",
        ):
            self.assertIs(getattr(SM, name), getattr(dm, name))
        self.assertEqual(
            dm.DRAIN_REGIMES, ("one_loop", "gap", "classical", "unassessed")
        )


class TestTheHooks(unittest.TestCase):
    """(b)"""

    def test_b_each_models_one_loop_drain_model(self):
        for n in (1, 2):
            e = exponential(n)
            self.assertEqual(
                e.one_loop_drain_model, dm.DRAIN_MODEL_ONE_LOOP_EXPONENTIAL
            )
            self.assertEqual(
                e.realize(NORMALIZATION, 0.0).one_loop_drain_model,
                dm.DRAIN_MODEL_ONE_LOOP_EXPONENTIAL,
            )
            # since power-law-wall prompt 07c the exponential's law is its own fraction, and L is recorded (P99)
            self.assertTrue(e.drain_L_implemented)
            self.assertTrue(e.drain_fraction_implemented)
            p = power(n)
            self.assertEqual(
                p.one_loop_drain_model, dm.DRAIN_MODEL_ONE_LOOP_INVERSE_POWER
            )
            self.assertEqual(
                p.realize(NORMALIZATION, 0.0).one_loop_drain_model,
                dm.DRAIN_MODEL_ONE_LOOP_INVERSE_POWER,
            )
            self.assertFalse(p.drain_L_implemented)
            self.assertTrue(p.drain_fraction_implemented)
            self.assertTrue(p.realize(NORMALIZATION, 0.0).drain_fraction_implemented)

    def test_b_undefined_at_n_3(self):
        p = power(3)
        for subject in (p, p.realize(NORMALIZATION, 0.0)):
            with self.assertRaises(ValueError) as ctx:
                subject.one_loop_drain_model
            msg = str(ctx.exception)
            for needle in ("InversePowerModel", "n=3", "one_loop_drain_model"):
                self.assertIn(needle, msg)
        with self.assertRaises(ValueError) as ctx:
            p.realize(NORMALIZATION, 0.0).drain_fraction("gap", None, 0.0, 10.0)
        self.assertIn("_drain_fraction", str(ctx.exception))
        self.assertIn("n=3", str(ctx.exception))

    def test_b_the_base_raises(self):
        m = _Neither(0, M_value(0, 4.0e-31 * Mp), 1, beta_value(0, 2.0), _units)
        with self.assertRaises(NotImplementedError) as ctx:
            m._drain_fraction(NORMALIZATION, "gap", None, 0.0, 10.0)
        self.assertIn("_Neither", str(ctx.exception))
        self.assertIn("_drain_fraction", str(ctx.exception))
        n = _NoDrainModel(0, M_value(0, 4.0e-31 * Mp), 1, beta_value(0, 2.0), _units)
        with self.assertRaises(NotImplementedError) as ctx:
            n.one_loop_drain_model
        self.assertIn("_NoDrainModel", str(ctx.exception))
        self.assertIn("one_loop_drain_model", str(ctx.exception))


class TestTheCheckAtEntry(unittest.TestCase):
    """(c)"""

    def refused(self, subject, *needles):
        for s in (subject, subject.realize(NORMALIZATION, 0.0)):
            with self.assertRaises(ValueError) as ctx:
                SM.check_drain_law_model(SM.DRAIN_LAW_ONE_LOOP, s, "ctx")
            msg = str(ctx.exception)
            self.assertTrue(msg.startswith("ctx: "), msg)
            for needle in needles:
                self.assertIn(needle, msg)
            # the fixed law checks nothing
            self.assertIsNone(SM.check_drain_law_model(SM.DRAIN_LAW_FIXED, s, "ctx"))

    def test_c_neither(self):
        m = _Neither(0, M_value(0, 4.0e-31 * Mp), 1, beta_value(0, 2.0), _units)
        self.refused(m, "_Neither", "_drain_fraction", "does not implement")

    def test_c_both(self):
        """P99: _drain_fraction is required under the one-loop law and _drain_L optional, so both is accepted."""
        m = _Both(0, M_value(0, 4.0e-31 * Mp), 1, beta_value(0, 2.0), _units)
        for s in (m, m.realize(NORMALIZATION, 0.0)):
            for law in (SM.DRAIN_LAW_ONE_LOOP, SM.DRAIN_LAW_FIXED):
                self.assertIsNone(SM.check_drain_law_model(law, s, "ctx"))

    def test_c_no_drain_model(self):
        m = _NoDrainModel(0, M_value(0, 4.0e-31 * Mp), 1, beta_value(0, 2.0), _units)
        self.refused(m, "_NoDrainModel", "one_loop_drain_model")

    def test_c_inverse_power_at_n_3(self):
        self.refused(power(3), "InversePowerModel", "n=3", "one_loop_drain_model")

    def test_c_accepted(self):
        for m in (power(1), power(2), exponential(1), exponential(2)):
            for s in (m, m.realize(NORMALIZATION, 0.0)):
                for law in (SM.DRAIN_LAW_ONE_LOOP, SM.DRAIN_LAW_FIXED):
                    self.assertIsNone(SM.check_drain_law_model(law, s, "ctx"))


class TestTheLaw(unittest.TestCase):
    """(d)"""

    def test_d_against_the_reference_bitwise(self):
        for units in (Planck_units(), GeV_units()):
            for n in (1, 2):
                m = power(n, units).realize(-123.4, 0.0)
                with self.subTest(units=type(units).__name__, n=n):
                    for lr in log_R_grid():
                        regime = (
                            "classical"
                            if lr <= 0.0
                            else ("gap" if lr < 30.0 else "one_loop")
                        )
                        self.assertEqual(
                            m.drain_fraction(regime, None, 5.0, lr),
                            fraction_power(n, lr),
                            lr,
                        )
                        if lr > 0.0:
                            # one-loop and gap alike; ell and ln E are not read
                            for reg, ell in (
                                ("one_loop", 0.3),
                                ("one_loop", 7.0),
                                ("gap", None),
                            ):
                                self.assertEqual(
                                    m.drain_fraction(reg, ell, -300.0, lr),
                                    fraction_power(n, lr),
                                )

    def test_d_the_literals(self):
        ipm = importlib.import_module("CosmologyConcepts.Models.InversePowerModel")
        self.assertEqual(ipm.DRAIN_PL_S, FN_S)
        self.assertEqual(ipm.DRAIN_PL_G, FN_G)
        self.assertEqual(ipm.DRAIN_PL_R0, FN_R0)
        self.assertEqual(ipm.DRAIN_PL_P, FN_P)
        self.assertEqual(ipm.DRAIN_PL_A, FN_A)
        self.assertEqual(ipm.DRAIN_PL_C, FN_C)
        self.assertEqual(ipm.DRAIN_PL_SWITCH, FN_SWITCH)

    def test_d_the_classical_branch(self):
        """R6: f = R S_n, S_1 = 1.02620e-2 and S_2 = 5.21615e-3."""
        for n, S in ((1, 1.02620e-2), (2, 5.21615e-3)):
            m = power(n).realize(NORMALIZATION, 0.0)
            for lr in (-20.0, -3.0, -0.5, 0.0):
                self.assertEqual(
                    m.drain_fraction("classical", None, 0.0, lr), exp(lr) * S
                )
            self.assertEqual(m.drain_fraction("classical", None, 0.0, 0.0), S)

    def test_d_continuity_at_the_switch(self):
        for n in (1, 2):
            m = power(n).realize(NORMALIZATION, 0.0)
            s = FN_SWITCH[n]
            below = m.drain_fraction("one_loop", 0.5, 0.0, nextafter(s, 0.0))
            at = m.drain_fraction("one_loop", 0.5, 0.0, s)
            # the rising branch's y at the switch equals the falling branch's to rounding
            rising_at = 1.0 - exp(-FN_G[n] * (s - FN_R0[n]) ** FN_P[n])
            self.assertLess(abs(at - rising_at), 4e-16)
            self.assertLess(abs(at - below), 4e-16)
            # the peak: f at the switch is the largest on a fine grid about it
            zz = np.linspace(s - 3.0, s + 3.0, 6001)
            self.assertLessEqual(
                max(m.drain_fraction("gap", None, 0.0, float(z)) for z in zz),
                at + 1e-15,
            )

    def test_d_unassessed_and_unknown(self):
        m = power(1).realize(NORMALIZATION, 0.0)
        self.assertEqual(m.drain_fraction("unassessed", None, None, None), 0.0)
        with self.assertRaises(ValueError):
            m.drain_fraction("edge", None, 0.0, 1.0)

    def test_d_through_drain_alpha_effective(self):
        for n in (1, 2):
            m = power(n).realize(NORMALIZATION, 0.0)
            for lr in (-2.0, 0.1, 5.0, FN_SWITCH[n], 40.0, 500.0):
                regime = "classical" if lr <= 0.0 else "gap"
                for A, w in ((1.0, 1.0), (0.05, 1.0), (10.0, 0.5), (1.0, 0.0)):
                    got = SM.drain_alpha_effective(
                        regime, None, lr, A, w, model=m, log_E=3.0
                    )
                    self.assertEqual(got, min(0.99, A * fraction_power(n, lr)) * w)
            self.assertEqual(
                SM.drain_alpha_effective(
                    "unassessed", None, None, 1.0, 1.0, model=m, log_E=None
                ),
                0.0,
            )
        # the exponential through the same call takes its own fraction too (power-law-wall prompt 07c, P99), and the
        # call without a model is refused: the module holds no law of its own
        e = exponential(1).realize(NORMALIZATION, 0.0)
        for regime, ell, lr in (
            ("one_loop", 12.0, 300.0),
            ("gap", None, 10.0),
            ("classical", None, -1.0),
        ):
            self.assertEqual(
                SM.drain_alpha_effective(regime, ell, lr, 1.0, 1.0, model=e, log_E=3.0),
                min(0.99, e.drain_fraction(regime, ell, 3.0, lr)),
            )
            with self.assertRaises(TypeError):
                SM.drain_alpha_effective(regime, ell, lr, 1.0, 1.0)


class TestAShortIntegration(unittest.TestCase):
    """
    (e) integrate_history under the one-loop law at A = 1 on InversePowerModel(n = 2, M = 4e-31 M_P, beta = 2), realized
    at its seed, from phi* = 5 M_P, pi* = 0, T* = 2e4 GeV to T_J = 100 MeV on test_inverse_power_model (h)'s grid.
    Measured 2026-11-15: succeeds with its first bounce, about 1 s at a load of about 6.
    """

    def test_e_the_record(self):
        from CosmologyConcepts import redshift, redshift_array
        from CosmologyConcepts.redshift import log_one_plus_z_grid

        m = power(2)
        seed = m.seed(cosmology())
        xs = log_one_plus_z_grid(20, 300.0)
        z = redshift_array(z_array=[redshift(i, x) for i, x in enumerate(xs)])
        rm = m.realize(seed.normalization, seed.phi_ref)
        start = time.perf_counter()
        with contextlib.redirect_stdout(io.StringIO()):
            data = SM.integrate_history(
                cosmology(),
                temperature(0, 2.0e4 * _units.GeV),
                temperature(1, 0.1 * _units.GeV),
                phi_value(0, 5.0 * Mp),
                pi_value(0, 0.0),
                z,
                rm,
                task_label="test_power_wall_law",
                alpha=1.0,
                drain_law=SM.DRAIN_LAW_ONE_LOOP,
            )
        wall = time.perf_counter() - start
        self.assertFalse(data.get("failure", False), data.get("failure_reason"))
        drain = data["drain"]
        self.assertEqual(drain.law, SM.DRAIN_LAW_ONE_LOOP)
        self.assertEqual(drain.model, "one-loop-inverse-power-v1")
        self.assertEqual(drain.A, 1.0)
        self.assertGreater(len(drain.candidates), 0)
        self.assertEqual(len(drain.candidates), len(data["injections"]))
        for c in drain.candidates:
            self.assertIsNone(c.L)
            self.assertEqual(
                c.alpha_eff,
                SM.drain_alpha_effective(
                    c.regime,
                    c.ell,
                    c.log_R,
                    1.0,
                    c.w_gate,
                    model=rm,
                    log_E=c.log_E,
                ),
            )
            if c.regime != "unassessed":
                self.assertEqual(
                    c.alpha_eff, min(0.99, fraction_power(2, c.log_R)) * c.w_gate
                )
        print(
            f"\n[test_power_wall_law (e)] one-loop A = 1, n = 2: candidates {len(drain.candidates)}, regimes "
            f"{sorted(set(c.regime for c in drain.candidates))}, wall {wall:.1f} s"
        )


if __name__ == "__main__":
    unittest.main()
