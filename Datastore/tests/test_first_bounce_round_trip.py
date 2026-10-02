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
A ScalarModel row keeps its first bounce (science-readiness prompt 03, README §2 (g), §6.4 (e)).

`compute_scalar_model`'s payload carries `first_bounce`, a `FirstBounce` or None.
`ScalarModel.store()` takes it, the factory's `store` writes it to the four nullable columns
`first_bounce_N`, `first_bounce_log_T_Jordan`, `first_bounce_phi_Einstein` and
`first_bounce_reflected`, and `build` reads it back. Each test goes the whole way on a temporary
SQLite file, as `test_scalarmodel_failure_reason` does: `ScalarModel.store()` is run with
`ray.wait` and `ray.get` patched to hand it a payload, the factory's `store` is called with the
datastore's own inserter, and the row is read back through `object_get`.

The stand-in cosmology carries `Planck_units()`, so that the conversions to the stored units
(phi in M_P, ln T_J with T_J in GeV) are exercised; the values read back are compared to the
last bit.

No Ray cluster, no persistent datastore. About 1 s.

On `HEAD~1` the table has no `first_bounce_*` columns and `ScalarModel` no `first_bounce`, so
every test here fails.
"""

from math import log
from types import SimpleNamespace as NS
from unittest import mock

import sqlalchemy as sqla

import ComputeTargets.tests.test_kinematic_cap_loop as kcl
from Datastore.tests.test_scalarmodel_failure_reason import _write
from Datastore.tests.test_version_keyed_lookups import (
    LABEL_A,
    _TempStoreCase,
    _atol,
    _coupling,
    _insert,
    _phi,
    _pi,
    _potential,
    _rtol,
    _scalar_model_query,
    _T_init,
    _T_stop,
)
from Units import Planck_units

SM = kcl.SM

STEPPER = "Radau+kinematic-cap-stepping0"

_units = Planck_units()

# the same store_id and type_id as test_version_keyed_lookups' stand-in, with real units
_cosmology = NS(store_id=1, type_id=7, units=_units)

# the first bounce of beta = 2, M = 0.5 from the P1 state (test_first_bounce (a)), and the
# reflection of P1 at M = 1e-10 (test_first_bounce (b)), as integrate_scalar_history gave them
TURNING_POINT = SM.FirstBounce(
    N=20.343026850496464,
    phi_Einstein=0.004573704679806311 * _units.PlanckMass,
    log_T_Jordan=-42.62906819875382,
    reflected=False,
)
REFLECTION = SM.FirstBounce(
    N=20.352100380348762,
    phi_Einstein=4.702273820088335e-11 * _units.PlanckMass,
    log_T_Jordan=-42.62899904487363,
    reflected=True,
)

COLUMNS = (
    "first_bounce_N",
    "first_bounce_log_T_Jordan",
    "first_bounce_phi_Einstein",
    "first_bounce_reflected",
)


# the fixed-temperature values of a history that reaches neither temperature (prompt 06b)
NO_FIXED_T = SM.FixedTValues(None, None, None, None)


def _success_payload(bounce, fixed_T=NO_FIXED_T):
    """The keys of compute_scalar_model's success payload that ScalarModel.store() reads."""
    return {
        "metadata": NS(
            compute_time=1.0,
            compute_steps=1,
            RHS_evaluations=1,
            mean_RHS_time=1.0,
            max_RHS_time=1.0,
            min_RHS_time=1.0,
        ),
        "z_grid": [],
        "sample": [],
        "reflections": 0,
        "first_bounce": bounce,
        "fixed_T_values": fixed_T,
        "cap_fraction": 0.1,
        "cap_floor": 1e-11,
        "cap_global_max_step": 0.1,
        "jacobian_factor_max": 1e-4,
        "accepted_steps": 1,
        "steps_rejected_by_exception": 0,
        "largest_RHS_values": None,
        "smallest_RHS_values": None,
        "mean_RHS_values": None,
        "solver_label": STEPPER,
    }


def _model(compute_result):
    """A ScalarModel on the stand-ins, populated by store() from compute_result."""
    model = SM.ScalarModel(
        payload=None,
        solver_labels={STEPPER: NS(store_id=1)},
        cosmology=_cosmology,
        T_Jordan_init=_T_init,
        T_Jordan_stop=_T_stop,
        phi_Einstein_init=_phi,
        pi_Einstein_init=_pi,
        potential=_potential,
        coupling=_coupling,
        atol=_atol,
        rtol=_rtol,
        label="m",
        tags=[],
    )
    model._compute_ref = object()
    with mock.patch.object(
        SM.ray, "wait", lambda refs, timeout: (refs, [])
    ), mock.patch.object(SM.ray, "get", lambda ref: compute_result):
        assert model.store() is True
    return model


class TestFirstBounceRoundTrip(_TempStoreCase):
    def _store(self):
        store = self.open(LABEL_A)
        for serial, log10_tol in ((14, -10.0), (15, -8.0)):
            _insert(store, "tolerance", dict(serial=serial, log10_tol=log10_tol))
        _insert(store, "IntegrationSolver", dict(serial=1, label=STEPPER, stepping=0))
        return store

    def _raw_columns(self, store):
        table = store._tables["ScalarModel"]
        with store._engine.begin() as conn:
            row = conn.execute(sqla.select(*(table.c[name] for name in COLUMNS))).one()
        return tuple(row)

    def _read_back(self, store, failure: bool):
        query = _scalar_model_query(failure=failure, cosmology=_cosmology)
        if not failure:
            query["solver_labels"] = [STEPPER]
        got = store.object_get("ScalarModel", **query)
        self.assertTrue(got.available)
        return got

    def check_bounce_round_trips(self, bounce):
        store = self._store()
        model = _model(_success_payload(bounce))
        self.assertEqual(model.first_bounce, bounce)
        _write(store, model)

        # the stored units: N as it is, phi in M_P, ln T_J with T_J in GeV
        N, log_T_GeV, phi_Mp, reflected = self._raw_columns(store)
        self.assertEqual(N, bounce.N)
        self.assertEqual(phi_Mp, bounce.phi_Einstein / _units.PlanckMass)
        self.assertEqual(log_T_GeV, bounce.log_T_Jordan - log(_units.GeV))
        self.assertIs(reflected, bounce.reflected)

        got = self._read_back(store, failure=False).first_bounce
        self.assertIsInstance(got, SM.FirstBounce)
        # floats to the last bit
        self.assertEqual(got.N, bounce.N)
        self.assertEqual(got.phi_Einstein, bounce.phi_Einstein)
        self.assertEqual(got.log_T_Jordan, bounce.log_T_Jordan)
        self.assertIs(got.reflected, bounce.reflected)

        # a fresh connection to the same file gives the same tuple
        again = self.open(LABEL_A).object_get(
            "ScalarModel",
            **dict(
                _scalar_model_query(failure=False, cosmology=_cosmology),
                solver_labels=[STEPPER],
            ),
        )
        self.assertEqual(tuple(again.first_bounce), tuple(bounce))

    def test_e_turning_point_round_trips(self):
        """(e) A row with a turning-point bounce reads back all four values."""
        self.check_bounce_round_trips(TURNING_POINT)

    def test_e_reflection_round_trips(self):
        """(e) A row whose first bounce is a reflection reads back reflected = True."""
        self.check_bounce_round_trips(REFLECTION)

    def test_e_no_bounce_reads_back_none(self):
        """(e) A row without a bounce stores four NULLs and reads back None."""
        store = self._store()
        model = _model(_success_payload(None))
        self.assertIsNone(model.first_bounce)
        _write(store, model)

        self.assertEqual(self._raw_columns(store), (None, None, None, None))
        self.assertIsNone(self._read_back(store, failure=False).first_bounce)

    def test_e_failure_row_raises(self):
        """(e) A failure row stores four NULLs, and first_bounce raises on it."""
        store = self._store()
        model = _model({"failure": True, "failure_reason": "step budget exhausted: x"})
        with self.assertRaises(RuntimeError):
            model.first_bounce
        _write(store, model)

        self.assertEqual(self._raw_columns(store), (None, None, None, None))
        got = self._read_back(store, failure=True)
        self.assertTrue(got.failure)
        with self.assertRaises(RuntimeError):
            got.first_bounce

    def test_e_unpopulated_model_raises(self):
        """An object with no row behind it refuses to report a first bounce."""
        store = self._store()
        got = store.object_get(
            "ScalarModel", **_scalar_model_query(failure=False, cosmology=_cosmology)
        )
        self.assertFalse(got.available)
        with self.assertRaises(RuntimeError):
            got.first_bounce


if __name__ == "__main__":
    import unittest

    unittest.main()
