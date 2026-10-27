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
Which ScalarHistory row a lookup returns when one key holds more than one row
(tuning-followups prompt 01; README §6.2 P4).

`--retry-failed-scalar` recomputes a history stored as a failure and stores the new row beside
the failure, under the same key. Every lookup but the retry's own second pass is made with no
`failure` in its payload (`failure=None`): the scalar stage's first pass, the adiabatic and BBN
stages, and both plot scripts. Before prompt 01 that lookup took no order and no limit and called
`one_or_none()`, so a key holding two rows raised `MultipleResultsFound`. It now follows
`sqla_BBNDataFactory.build` (run-integrity prompt 03): the success if one exists, else the newest
failure. `failure=True` returns the newest failure; `failure=False` is unchanged, a filter.

On the tree before prompt 01, (a), (a2), (b), (c) and (d1) raise `MultipleResultsFound`. (d2),
(e1), (e2), (f) and (g) pass there too: they are regression guards for what does not change, and
say so. (f) is
the premise of the retry's second pass (`main.py` adds `failure=False` to its payload under the
flag): on a key holding only a failure, that lookup finds nothing, so an unpopulated history is
returned and computed.

No Ray cluster, no PRyMordial solve, no persistent datastore: each test builds the package's
undecorated actor on a SQLite file in a `tempfile` directory (`_TempStoreCase`, from
`test_version_keyed_lookups`), as `test_bbn_failure_lookup` does. Rows go through the datastore's
own inserter with explicit, increasing serials, so a later row has the larger serial and a
timestamp no earlier. A success row carries a solver row (`build` raises on a success with no
solver label) and NULL in every other column it does not key on: with `_do_not_populate` no
sample is read, and the tuning's columns read back as None. Lookups go through `object_get`, so
the version key is present. About 1 s (0.7 s alone at load 5, 2026-10-27).
"""

import unittest

from ObjectFactories.tests.test_version_keyed_lookups import (
    LABEL_A,
    _TempStoreCase,
    _insert,
    _scalar_history_query,
)

_SOLVER_SERIAL = 1


def _key_columns():
    # the key of _scalar_history_query()'s stand-ins (test_version_keyed_lookups)
    return dict(
        label="m",
        cosmology_type=7,
        cosmology_serial=1,
        model_type=3,
        model_serial=2,
        atol_serial=14,
        rtol_serial=15,
        tuning_tol_serial=16,
        phi_Einstein_init_serial=12,
        pi_Einstein_init_serial=13,
        T_Jordan_init_serial=10,
        validated=True,
    )


def _insert_failure(store, serial, reason):
    return _insert(
        store,
        "ScalarHistory",
        _key_columns() | dict(serial=serial, failure=True, failure_reason=reason),
    )


def _insert_success(store, serial):
    return _insert(
        store,
        "ScalarHistory",
        _key_columns()
        | dict(serial=serial, failure=False, solver_serial=_SOLVER_SERIAL),
    )


def _query(failure):
    # failure=None is the payload with no `failure` key, as every lookup but the retry's
    # second pass makes it
    if failure is None:
        return _scalar_history_query()
    return _scalar_history_query(failure=failure)


def _lookup(store, failure):
    return store.object_get("ScalarHistory", **_query(failure))


class TestScalarHistoryFailureLookup(_TempStoreCase):
    def _store(self):
        store = self.open(LABEL_A)
        for serial, log10_tol in ((14, -10.0), (15, -8.0), (16, -5.0)):
            _insert(store, "tolerance", dict(serial=serial, log10_tol=log10_tol))
        _insert(
            store,
            "IntegrationSolver",
            dict(serial=_SOLVER_SERIAL, label="stand-in solver", stepping=0),
        )
        return store

    def _failure_then_success(self):
        store = self._store()
        _insert_failure(store, 1, "earlier failure")
        _insert_success(store, 2)
        return store

    def _two_failures(self):
        store = self._store()
        _insert_failure(store, 1, "first failure")
        _insert_failure(store, 2, "second failure")
        return store

    def _success_then_failure(self):
        store = self._store()
        _insert_success(store, 1)
        _insert_failure(store, 2, "later failure")
        return store

    def _check_success(self, m, serial):
        self.assertTrue(m.available)
        self.assertFalse(m.failure)
        self.assertEqual(m.store_id, serial)

    def _check_failure(self, m, serial, reason):
        self.assertTrue(m.available)
        self.assertTrue(m.failure)
        self.assertEqual(m.store_id, serial)
        self.assertEqual(m.failure_reason, reason)

    def test_a_any_row_prefers_a_later_success(self):
        """(a) A failure, then a later success (a retry that converged): failure=None returns the success."""
        self._check_success(_lookup(self._failure_then_success(), None), 2)

    def test_a2_the_vectorized_lookup_prefers_the_success_too(self):
        """(a) As (a), through the vectorized route (payload_data), which the scalar stage's first pass takes."""
        store = self._failure_then_success()
        got = store.object_get("ScalarHistory", payload_data=[_query(None)] * 2)
        self.assertEqual(len(got), 2)
        for m in got:
            self._check_success(m, 2)

    def test_b_any_row_with_two_failures_returns_the_newer(self):
        """(b) Two failures (a retry that failed again): failure=None returns the newer, rather than raising."""
        self._check_failure(_lookup(self._two_failures(), None), 2, "second failure")

    def test_c_any_row_prefers_an_earlier_success(self):
        """(c) A success, then a later failure: failure=None returns the success."""
        self._check_success(_lookup(self._success_then_failure(), None), 1)

    def test_d1_failure_true_with_two_failures_returns_the_newer(self):
        """(d) failure=True with two failures returns the newer."""
        self._check_failure(_lookup(self._two_failures(), True), 2, "second failure")

    def test_d2_failure_true_beside_a_success_returns_the_failure(self):
        """(d) failure=True on a failure and a later success returns the failure.

        A regression guard: this passes on the tree before prompt 01 as well (one failure
        matches the filter)."""
        self._check_failure(
            _lookup(self._failure_then_success(), True), 1, "earlier failure"
        )

    def test_e1_failure_false_after_failure_then_success(self):
        """(e) failure=False on a failure and a later success returns the success.

        A regression guard: this passes on the tree before prompt 01 as well (the filter is
        unchanged)."""
        self._check_success(_lookup(self._failure_then_success(), False), 2)

    def test_e2_failure_false_after_success_then_failure(self):
        """(e) failure=False on a success and a later failure returns the success.

        A regression guard: this passes on the tree before prompt 01 as well."""
        self._check_success(_lookup(self._success_then_failure(), False), 1)

    def test_f_failure_false_on_a_stored_failure_finds_nothing(self):
        """(f) The premise of the retry's second pass: a key holding only a failure, looked up with failure=False, gives an unavailable history.

        A regression guard: this passes on the tree before prompt 01 as well."""
        store = self._store()
        _insert_failure(store, 1, "the only row")

        m = _lookup(store, False)
        self.assertFalse(m.available)

        # and the lookup the stage's first pass makes still finds the failure
        self._check_failure(_lookup(store, None), 1, "the only row")

    def test_g_one_row_per_key_is_returned_as_before(self):
        """(g) A key holding one row: every lookup that matches it returns it, as before prompt 01.

        A regression guard: this passes on the tree before prompt 01 as well. The ordering is
        invisible while a key holds one row."""
        store = self._store()
        _insert_success(store, 1)
        self._check_success(_lookup(store, None), 1)
        self._check_success(_lookup(store, False), 1)
        self.assertFalse(_lookup(store, True).available)


if __name__ == "__main__":
    unittest.main()
