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
A ScalarModel row keeps its fixed-temperature values (science-readiness prompt 06b, README §2 (n),
§6.7b, test (d)).

`compute_scalar_model`'s payload carries `fixed_T_values`, a `FixedTValues`. `ScalarModel.store()`
takes it, the factory's `store` writes it to the four nullable columns `phi_Einstein_1MeV`,
`density_NP_ratio_1MeV`, `phi_Einstein_70keV` and `density_NP_ratio_70keV`, and `build` reads it
back. The tests go the whole way on a temporary SQLite file, with the helpers of
`test_first_bounce_round_trip`.

The point of the prompt is the read with `_do_not_populate=True`, which loads no sample: the four
values are on the parent row, so `fixed_T_values` returns them on such an object.

No Ray cluster, no persistent datastore. About 1 s.

On `HEAD~1` the table has none of the four columns and `ScalarModel` no `fixed_T_values`, so every
test here fails.
"""

import sqlalchemy as sqla

from Datastore.tests.test_first_bounce_round_trip import (
    STEPPER,
    TURNING_POINT,
    SM,
    _cosmology,
    _model,
    _success_payload,
    _units,
)
from Datastore.tests.test_scalarmodel_failure_reason import _write
from Datastore.tests.test_version_keyed_lookups import (
    LABEL_A,
    _TempStoreCase,
    _insert,
    _scalar_model_query,
)

# the values the driver printed for beta = 2, M = 0.5 on this prompt's tree (log 06b)
ALL_FOUR = SM.FixedTValues(
    phi_Einstein_1MeV=1.138197048e-02 * _units.PlanckMass,
    density_NP_ratio_1MeV=-4.810565953e-02,
    phi_Einstein_70keV=8.695012936e-03 * _units.PlanckMass,
    density_NP_ratio_70keV=6.742167855e-02,
)

# a history stopped between the two temperatures: 1 MeV reached, 70 keV not
ONLY_1MEV = SM.FixedTValues(
    phi_Einstein_1MeV=ALL_FOUR.phi_Einstein_1MeV,
    density_NP_ratio_1MeV=ALL_FOUR.density_NP_ratio_1MeV,
    phi_Einstein_70keV=None,
    density_NP_ratio_70keV=None,
)

COLUMNS = (
    "phi_Einstein_1MeV",
    "density_NP_ratio_1MeV",
    "phi_Einstein_70keV",
    "density_NP_ratio_70keV",
)


class TestFixedTValuesRoundTrip(_TempStoreCase):
    def _store(self):
        # as test_first_bounce_round_trip's (the class is not imported, so that unittest does
        # not collect its tests a second time here)
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

    def _read_back(self, store, failure: bool, do_not_populate: bool):
        query = _scalar_model_query(
            failure=failure, cosmology=_cosmology, _do_not_populate=do_not_populate
        )
        if not failure:
            query["solver_labels"] = [STEPPER]
        got = store.object_get("ScalarModel", **query)
        self.assertTrue(got.available)
        return got

    def _write_success(self, fixed_T):
        store = self._store()
        model = _model(_success_payload(TURNING_POINT, fixed_T))
        self.assertEqual(model.fixed_T_values, fixed_T)
        _write(store, model)
        return store

    def check_equal_to_the_bit(self, got, expected):
        self.assertIsInstance(got, SM.FixedTValues)
        for name in SM.FixedTValues._fields:
            a, b = getattr(got, name), getattr(expected, name)
            if b is None:
                self.assertIsNone(a, name)
            else:
                self.assertEqual(a, b, name)

    def test_d_all_four_round_trip(self):
        """(d) A row with all four values reads them back, floats to the last bit."""
        store = self._write_success(ALL_FOUR)

        # the stored units: phi in M_P, the ratio as it is
        phi_1, ratio_1, phi_70, ratio_70 = self._raw_columns(store)
        self.assertEqual(phi_1, ALL_FOUR.phi_Einstein_1MeV / _units.PlanckMass)
        self.assertEqual(ratio_1, ALL_FOUR.density_NP_ratio_1MeV)
        self.assertEqual(phi_70, ALL_FOUR.phi_Einstein_70keV / _units.PlanckMass)
        self.assertEqual(ratio_70, ALL_FOUR.density_NP_ratio_70keV)

        got = self._read_back(store, failure=False, do_not_populate=False)
        self.check_equal_to_the_bit(got.fixed_T_values, ALL_FOUR)

    def test_d_70keV_not_reached_reads_back_none(self):
        """(d) A row that does not reach 70 keV reads back None for those two fields."""
        store = self._write_success(ONLY_1MEV)
        self.assertEqual(self._raw_columns(store)[2:], (None, None))

        got = self._read_back(store, failure=False, do_not_populate=False)
        self.check_equal_to_the_bit(got.fixed_T_values, ONLY_1MEV)

    def test_d_failure_row_raises(self):
        """(d) A failure row stores four NULLs, and fixed_T_values raises on it."""
        store = self._store()
        model = _model({"failure": True, "failure_reason": "step budget exhausted: x"})
        with self.assertRaises(RuntimeError):
            model.fixed_T_values
        _write(store, model)

        self.assertEqual(self._raw_columns(store), (None, None, None, None))
        for do_not_populate in (True, False):
            got = self._read_back(store, failure=True, do_not_populate=do_not_populate)
            self.assertTrue(got.failure)
            with self.assertRaises(RuntimeError):
                got.fixed_T_values

    def test_d_do_not_populate_returns_the_values(self):
        """(d) A read with _do_not_populate=True loads no sample and returns the four values."""
        store = self._write_success(ALL_FOUR)

        got = self._read_back(store, failure=False, do_not_populate=True)
        # it is an unpopulated read: the samples were not loaded and cannot be read
        self.assertTrue(getattr(got, "_do_not_populate", False))
        self.assertIsNone(got._values)
        with self.assertRaises(RuntimeError):
            got.values

        self.check_equal_to_the_bit(got.fixed_T_values, ALL_FOUR)

        # the same through a fresh connection to the same file
        again = self.open(LABEL_A).object_get(
            "ScalarModel",
            **dict(
                _scalar_model_query(failure=False, cosmology=_cosmology),
                solver_labels=[STEPPER],
            ),
        )
        self.assertTrue(getattr(again, "_do_not_populate", False))
        self.assertEqual(tuple(again.fixed_T_values), tuple(ALL_FOUR))

    def test_d_unpopulated_model_raises(self):
        """An object with no row behind it refuses to report fixed-temperature values."""
        store = self._store()
        got = store.object_get(
            "ScalarModel", **_scalar_model_query(failure=False, cosmology=_cosmology)
        )
        self.assertFalse(got.available)
        with self.assertRaises(RuntimeError):
            got.fixed_T_values


if __name__ == "__main__":
    import unittest

    unittest.main()
