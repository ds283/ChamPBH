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
A ScalarModel failure row keeps its reason (science-readiness prompt 02, README §6.3 (a)).

A failed history's payload carries `failure_reason`. `ScalarModel.store()` takes it, the
factory's `store` writes it to the new `failure_reason` column, and `build` reads it back.
Each test goes the whole way on a temporary SQLite file, as `test_version_keyed_lookups`
does: `ScalarModel.store()` is run with `ray.wait` and `ray.get` patched to hand it a
payload, then the factory's `store` is called with the datastore's own inserter, and the
row is read back through `object_get`. A success row reads back `None`.

No Ray cluster, no persistent datastore. About 1 s.

On `HEAD~1` the table has no `failure_reason` column and `ScalarModel` no such property, so
the read-back object has no reason: the reason the history printed is not in the row.
"""

from types import SimpleNamespace as NS
from unittest import mock

import sqlalchemy as sqla

import ComputeTargets.tests.test_kinematic_cap_loop as kcl
from CosmologyConcepts import temperature
from Datastore.tests.test_version_keyed_lookups import (
    LABEL_A,
    _TempStoreCase,
    _atol,
    _coupling,
    _cosmology,
    _insert,
    _phi,
    _pi,
    _potential,
    _rtol,
    _scalar_model_query,
    _T_init,
    _T_stop,
)
from config.defaults import DEFAULT_STRING_LENGTH

# `import ComputeTargets.ScalarModel` gives the class (the package re-exports it); the module
# is reached through the test helper that already does so
SM = kcl.SM

REASON_300 = "step budget exhausted: " + "x" * 277


def _model(compute_result):
    """An unpopulated ScalarModel on the stand-ins, with store() about to resolve compute_result."""
    model = SM.ScalarModel(
        payload=None,
        solver_labels={"Radau+kinematic-cap-stepping0": NS(store_id=1)},
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


def _write(store, model):
    factory = store._factories["ScalarModel"]
    with store._engine.begin() as conn:
        factory.store(
            model,
            conn,
            store._tables["ScalarModel"],
            # there is no serial broker here, so the row is given an explicit serial
            lambda c, payload: store._inserters["ScalarModel"](
                c, dict(payload, serial=1)
            ),
            store._tables,
            store._inserters,
        )
        conn.execute(
            sqla.update(store._tables["ScalarModel"])
            .where(store._tables["ScalarModel"].c.serial == model.store_id)
            .values(validated=True)
        )
        conn.commit()


class TestFailureReasonRoundTrip(_TempStoreCase):
    def _store_with_tolerances(self):
        store = self.open(LABEL_A)
        for serial, log10_tol in ((14, -10.0), (15, -8.0)):
            _insert(store, "tolerance", dict(serial=serial, log10_tol=log10_tol))
        return store

    def test_a_failure_reason_round_trips_truncated_to_256(self):
        """(a) A 300-character reason is stored as its first 256 characters and read back."""
        store = self._store_with_tolerances()
        self.assertEqual(len(REASON_300), 300)

        model = _model({"failure": True, "failure_reason": REASON_300})
        self.assertEqual(model.failure_reason, REASON_300[:DEFAULT_STRING_LENGTH])
        _write(store, model)

        got = store.object_get("ScalarModel", **_scalar_model_query(failure=True))
        self.assertTrue(got.available)
        self.assertTrue(got.failure)
        self.assertEqual(len(got.failure_reason), DEFAULT_STRING_LENGTH)
        self.assertEqual(got.failure_reason, REASON_300[:DEFAULT_STRING_LENGTH])

        # a fresh connection to the same file, and the vectorized route, give the same reason
        again = self.open(LABEL_A).object_get(
            "ScalarModel", payload_data=[_scalar_model_query(failure=True)]
        )
        self.assertEqual(again[0].failure_reason, REASON_300[:DEFAULT_STRING_LENGTH])

    def test_a2_a_failure_without_a_reason_reads_back_none(self):
        store = self._store_with_tolerances()
        _write(store, _model({"failure": True}))
        got = store.object_get("ScalarModel", **_scalar_model_query(failure=True))
        self.assertTrue(got.failure)
        self.assertIsNone(got.failure_reason)

    def test_a3_a_success_row_reads_back_none(self):
        store = self._store_with_tolerances()
        _insert(
            store,
            "IntegrationSolver",
            dict(serial=1, label="Radau+kinematic-cap-stepping0", stepping=0),
        )

        model = SM.ScalarModel(
            payload=None,
            solver_labels={},
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
        model._failure = False
        model._failure_reason = None
        model._values = []
        model._extra_data = None
        model._solver = NS(store_id=1)
        model._metadata = NS(
            compute_time=1.0,
            compute_steps=1,
            RHS_evaluations=1,
            mean_RHS_time=1.0,
            max_RHS_time=1.0,
            min_RHS_time=1.0,
        )
        _write(store, model)

        got = store.object_get(
            "ScalarModel",
            **_scalar_model_query(
                failure=False, solver_labels=["Radau+kinematic-cap-stepping0"]
            ),
        )
        self.assertTrue(got.available)
        self.assertFalse(got.failure)
        self.assertIsNone(got.failure_reason)

    def test_a4_an_unpopulated_model_refuses_to_report_a_reason(self):
        store = self._store_with_tolerances()
        got = store.object_get("ScalarModel", **_scalar_model_query(failure=True))
        self.assertFalse(got.available)
        with self.assertRaises(RuntimeError):
            got.failure_reason
