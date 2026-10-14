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
`AdiabaticHistory.build` and `BBNData.build` return only validated rows, as `ScalarModel.build`
does (2026-10-14; recorded by `dk-and-rpk-migration` log 09, Observation 3, and by
`read-only-plots` README §1).

A row is stored with `validated=False` and marked validated only after its values are checked
(`main.py` calls `object_validate` on the object it stored, by its `store_id`, never through
`build`). An unvalidated row is therefore one whose write was interrupted. A pool that prunes at
open removes it; a read-only plot pool (`prune_unvalidated=False`) and a `main.py` run with
`--no-prune-unvalidated` do not, and before this fix their lookups found it.

No Ray cluster, no PRyMordial solve, no persistent datastore. About 1 s. The harness is
`_TempStoreCase` (`test_version_keyed_lookups.py`).
"""

import unittest

from ObjectFactories.tests.test_version_keyed_lookups import (
    LABEL_A,
    _MODEL_SERIAL,
    _TempStoreCase,
    _insert,
    _insert_adiabatic_history,
    _insert_failed_bbn,
    _insert_failed_scalar_model,
    _model_proxy,
    _sid,
)


def _insert_unvalidated_adiabatic_history(store, serial):
    return _insert(
        store,
        "AdiabaticHistory",
        dict(
            serial=serial,
            model_serial=_MODEL_SERIAL,
            label="unvalidated",
            z_samples=0,
            compute_time=1.0,
            validated=False,
        ),
    )


def _insert_bbn(store, serial, failure, validated, reason=None):
    row = dict(
        serial=serial,
        model_serial=_MODEL_SERIAL,
        failure=failure,
        validated=validated,
    )
    if failure:
        row["failure_reason"] = reason
    else:
        row.update(
            Yp_BBN=0.2468872958,
            DOverH=2.462251065,
            He3OverH=1.042050273,
            Li7OverH=5.423441017,
            small_network=True,
            PRyM_version="test",
            z_samples=0,
        )
    return _insert(store, "BBNData", row)


def _bbn(store, failure):
    query = dict(model_proxy=_model_proxy(), tags=[], _do_not_populate=True)
    if failure != "default":
        query["failure"] = failure
    return store.object_get("BBNData", **query)


class TestAdiabaticHistoryValidated(_TempStoreCase):
    def test_a_an_unvalidated_history_is_not_returned(self):
        """(a) The only row is unvalidated: the lookup returns an unavailable object."""
        store = self.open(LABEL_A)
        _insert_failed_scalar_model(store)
        _insert_unvalidated_adiabatic_history(store, 1)

        h = store.object_get("AdiabaticHistory", model_proxy=_model_proxy(), tags=[])
        self.assertFalse(
            h.available,
            f"an unvalidated AdiabaticHistory was returned (store_id={_sid(h)})",
        )

    def test_b_the_validated_history_is_returned_beside_an_unvalidated_one(self):
        """(b) One validated and one unvalidated row: the validated one, with no MultipleResultsFound."""
        store = self.open(LABEL_A)
        _insert_failed_scalar_model(store)
        _insert_unvalidated_adiabatic_history(store, 1)
        serial = _insert_adiabatic_history(store, serial=2)

        h = store.object_get("AdiabaticHistory", model_proxy=_model_proxy(), tags=[])
        self.assertTrue(h.available)
        self.assertEqual(h.store_id, serial)


class TestBBNDataValidated(_TempStoreCase):
    def test_a_an_unvalidated_success_is_not_returned(self):
        """(a) The only row is an unvalidated success: every lookup returns an unavailable object."""
        store = self.open(LABEL_A)
        _insert_failed_scalar_model(store)
        _insert_bbn(store, 1, failure=False, validated=False)

        for failure in ("default", None, False):
            with self.subTest(failure=failure):
                d = _bbn(store, failure)
                self.assertFalse(
                    d.available,
                    f"an unvalidated BBNData was returned (store_id={_sid(d)})",
                )

    def test_b_any_row_skips_an_unvalidated_success_for_a_validated_failure(self):
        """(b) failure=None prefers a success, but not an unvalidated one: the validated failure is returned."""
        store = self.open(LABEL_A)
        _insert_failed_scalar_model(store)
        failed = _insert_failed_bbn(store, 1, "validated failure")
        _insert_bbn(store, 2, failure=False, validated=False)

        d = _bbn(store, None)
        self.assertTrue(d.available)
        self.assertEqual(d.store_id, failed)
        self.assertTrue(d.failure)

    def test_c_failure_true_skips_a_newer_unvalidated_failure(self):
        """(c) failure=True returns the newest failure, but only among validated rows."""
        store = self.open(LABEL_A)
        _insert_failed_scalar_model(store)
        older = _insert_failed_bbn(store, 1, "validated failure")
        _insert_bbn(store, 2, failure=True, validated=False, reason="unvalidated")

        d = _bbn(store, True)
        self.assertTrue(d.available)
        self.assertEqual(d.store_id, older)
        self.assertEqual(d.failure_reason, "validated failure")

    def test_d_the_validated_success_is_returned_beside_an_unvalidated_one(self):
        """(d) The default lookup (failure=False) with one validated and one unvalidated success: the validated one, with no MultipleResultsFound."""
        store = self.open(LABEL_A)
        _insert_failed_scalar_model(store)
        _insert_bbn(store, 1, failure=False, validated=False)
        serial = _insert_bbn(store, 2, failure=False, validated=True)

        d = _bbn(store, "default")
        self.assertTrue(d.available)
        self.assertEqual(d.store_id, serial)


if __name__ == "__main__":
    unittest.main()
