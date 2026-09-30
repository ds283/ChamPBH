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
Which BBNData row a lookup returns when failed rows are stored
(run-integrity prompt 03, README §6.3).

`main.py`'s BBN stage looks up with `failure=None`, so that a stored failure counts
as done. Before prompt 03, `failure=None` took no order and no limit, and raised
`MultipleResultsFound` once two failed rows existed for one model. It now returns
the success if one exists, and otherwise the newest failure. `failure=True` (the
newest failure) and `failure=False` (the success only) are unchanged.

(a), (b1) and (b2) raise `MultipleResultsFound` on the tree before prompt 03. (c1)-(c3)
pass there too: they are regression guards for the unchanged filters, and say so.

No Ray cluster, no PRyMordial solve, no persistent datastore: each test builds the
undecorated `Datastore.__ray_actor_class__` on a SQLite file in a `tempfile`
directory, as `test_version_keyed_lookups` does, and reuses its stand-ins. Rows go
through the datastore's own inserter with explicit, increasing serials; lookups go
through `object_get`, so the version key is present. About 1 s.
"""

import unittest

from Datastore.tests.test_version_keyed_lookups import (
    LABEL_A,
    _TempStoreCase,
    _insert_failed_bbn,
    _insert_failed_scalar_model,
    _insert_successful_bbn,
    _model_proxy,
)


def _lookup(store, failure):
    return store.object_get(
        "BBNData",
        model_proxy=_model_proxy(),
        tags=[],
        failure=failure,
        _do_not_populate=True,
    )


class TestBBNFailureLookup(_TempStoreCase):
    def _store_with_model(self):
        store = self.open(LABEL_A)
        _insert_failed_scalar_model(store)
        return store

    def _two_failures(self):
        store = self._store_with_model()
        _insert_failed_bbn(store, 1, "first failure")
        _insert_failed_bbn(store, 2, "second failure")
        return store

    def _failure_then_success(self):
        store = self._store_with_model()
        _insert_failed_bbn(store, 1, "earlier failure")
        _insert_successful_bbn(store, 2)
        return store

    def _success_then_failure(self):
        store = self._store_with_model()
        _insert_successful_bbn(store, 1)
        _insert_failed_bbn(store, 2, "later failure")
        return store

    def test_a_any_row_with_two_failures_returns_the_newer(self):
        """(a) Two failed rows for one model: failure=None returns the newer, rather than raising MultipleResultsFound."""
        store = self._two_failures()

        d = _lookup(store, None)
        self.assertTrue(d.available)
        self.assertTrue(d.failure)
        self.assertEqual(d.store_id, 2)
        self.assertEqual(d.failure_reason, "second failure")

    def test_b1_any_row_prefers_a_later_success(self):
        """(b) A failure, then a later success: failure=None returns the success."""
        store = self._failure_then_success()

        d = _lookup(store, None)
        self.assertTrue(d.available)
        self.assertFalse(d.failure)
        self.assertEqual(d.store_id, 2)

    def test_b2_any_row_prefers_an_earlier_success(self):
        """(b) A success, then a later failure: failure=None returns the success."""
        store = self._success_then_failure()

        d = _lookup(store, None)
        self.assertTrue(d.available)
        self.assertFalse(d.failure)
        self.assertEqual(d.store_id, 1)

    def test_c1_failure_true_and_false_with_two_failures(self):
        """(c) On (a)'s data: failure=True returns the newest failure; failure=False returns nothing."""
        store = self._two_failures()

        d = _lookup(store, True)
        self.assertTrue(d.available)
        self.assertEqual(d.store_id, 2)
        self.assertEqual(d.failure_reason, "second failure")

        self.assertFalse(_lookup(store, False).available)

    def _check_success_and_failure(self, store, failed_serial, success_serial):
        d = _lookup(store, True)
        self.assertTrue(d.available)
        self.assertTrue(d.failure)
        self.assertEqual(d.store_id, failed_serial)

        d = _lookup(store, False)
        self.assertTrue(d.available)
        self.assertFalse(d.failure)
        self.assertEqual(d.store_id, success_serial)

    def test_c2_failure_true_and_false_after_failure_then_success(self):
        """(c) On (b)'s first data set: failure=True returns the failure; failure=False returns the success."""
        self._check_success_and_failure(
            self._failure_then_success(), failed_serial=1, success_serial=2
        )

    def test_c3_failure_true_and_false_after_success_then_failure(self):
        """(c) On (b)'s second data set: failure=True returns the failure; failure=False returns the success."""
        self._check_success_and_failure(
            self._success_then_failure(), failed_serial=2, success_serial=1
        )


if __name__ == "__main__":
    unittest.main()
