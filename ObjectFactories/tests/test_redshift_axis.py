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
The redshift factory on the sampling axis, and the value factory's refusal (jordan-normalization
prompt 03, Z2 and Z7).

The table `redshift` stores `log_one_plus_z`, ln(1 + z), and `sqla_redshift_factory.build` matches a
point by absolute difference below `DEFAULT_LOG_ONE_PLUS_Z_PRECISION` (1e-10), in one branch with no
special case at zero. Before prompt 03 it matched z relatively, and at z = 0 it filtered on
`abs(z_col - z < 1e-7)`, the absolute value of a comparison, which is true for every row below
1e-7, a negative one included (`[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]`).

- `TestTheLookup`: issue 09, at 0 against rows at -1e-6 and at 1e-12; three adjacent grid points
  get three serials, twice.
- `TestReadTable`: `read_table` orders by ln(1 + z), and with a `model_proxy` returns only the
  points of that history's value rows. Nothing in the tree calls it; this is its only witness.
- `TestTheValueFactory`: `sqla_ScalarHistoryValue_factory.build` refuses, naming
  `sqla_ScalarHistoryFactory`, through which the value rows are written and read.

Each test builds a one-shard store in a temporary directory through the package's actor inside
the stand-in cluster (`_TempStoreCase`); no Ray, no store outside the temporary directory.
"""

import contextlib
import io
from types import SimpleNamespace as NS

from CosmologyConcepts.redshift import log_one_plus_z_grid
from config.datastore import factories
from ObjectFactories.tests.test_version_keyed_lookups import _TempStoreCase

LABEL = "2026.3.0"


def _insert(store, cls_name, row):
    """A row written by the table's own inserter, past the factory (and the class's checks)."""
    with store._engine.begin() as conn:
        serial = store._schema[cls_name]["insert"](conn, row)
        conn.commit()
    return serial


def _count(store, cls_name):
    table = store._tables[cls_name]
    with store._engine.begin() as conn:
        return len(conn.execute(table.select()).fetchall())


class TestTheLookup(_TempStoreCase):
    def test_a_lookup_at_zero_does_not_match_a_small_negative_row(self):
        """Issue 09: a row at -1e-6 (written by the inserter; the class refuses a negative
        ln(1 + z)) is not matched by a lookup at 0, which inserts a new row. The old filter matched
        it."""
        store = self.open(LABEL)
        negative = _insert(store, "redshift", {"log_one_plus_z": -1.0e-6})

        with contextlib.redirect_stdout(io.StringIO()):
            got = store.object_get("redshift", log_one_plus_z=0.0)
        self.assertNotEqual(got.store_id, negative)
        self.assertTrue(getattr(got, "_new_insert", False))
        self.assertEqual(got.log_one_plus_z, 0.0)
        self.assertEqual(_count(store, "redshift"), 2)

    def test_a_lookup_at_zero_matches_a_row_within_the_tolerance(self):
        """A row at 1e-12 is the point 0: a lookup at 0 returns its serial and inserts nothing."""
        store = self.open(LABEL)
        near = _insert(store, "redshift", {"log_one_plus_z": 1.0e-12})

        with contextlib.redirect_stdout(io.StringIO()):
            got = store.object_get("redshift", log_one_plus_z=0.0)
        self.assertEqual(got.store_id, near)
        self.assertTrue(getattr(got, "_deserialized", False))
        self.assertEqual(_count(store, "redshift"), 1)

    def test_adjacent_grid_points_do_not_collide(self):
        """The first three points of the production grid (9.2e-3 apart) get three distinct serials,
        and building them again returns the same three."""
        points = log_one_plus_z_grid(250, 85)[:3]
        store = self.open(LABEL)
        with contextlib.redirect_stdout(io.StringIO()):
            first = store.object_get(
                "redshift", payload_data=[{"log_one_plus_z": x} for x in points]
            )
            again = store.object_get(
                "redshift", payload_data=[{"log_one_plus_z": x} for x in points]
            )
        serials = [p.store_id for p in first]
        self.assertEqual(len(set(serials)), 3)
        self.assertEqual([p.store_id for p in again], serials)
        self.assertEqual([p.log_one_plus_z for p in first], points)
        self.assertEqual(_count(store, "redshift"), 3)


class TestReadTable(_TempStoreCase):
    def _read(self, store, model_proxy=None):
        factory = factories["redshift"]
        with store._engine.begin() as conn:
            return factory.read_table(
                conn,
                store._tables["redshift"],
                store._tables,
                model_proxy=model_proxy,
            )

    def test_rows_come_back_in_ascending_order(self):
        store = self.open(LABEL)
        values = (2.0, 0.0, 1.5, 0.5)
        serials = {x: _insert(store, "redshift", {"log_one_plus_z": x}) for x in values}

        rows = self._read(store)
        self.assertEqual([r.log_one_plus_z for r in rows], sorted(values))
        self.assertEqual(
            [r.store_id for r in rows], [serials[x] for x in sorted(values)]
        )

    def test_a_model_proxy_keeps_only_that_historys_points(self):
        """With a model_proxy, the join on ScalarHistoryValue keeps only the points that carry a
        value row of that history: two of four here, the other history's third point excluded.
        """
        store = self.open(LABEL)
        serials = [
            _insert(store, "redshift", {"log_one_plus_z": x})
            for x in (3.0, 0.0, 2.0, 1.0)
        ]

        def value_row(history, z_serial):
            row = {"history_serial": history, "z_serial": z_serial}
            table = store._tables["ScalarHistoryValue"]
            for column in table.columns:
                if column.name not in row and column.name != "serial":
                    row[column.name] = 0.0
            _insert(store, "ScalarHistoryValue", row)

        mine, other = 101, 102
        for z_serial in (serials[0], serials[2]):
            value_row(mine, z_serial)
        value_row(other, serials[3])

        proxy = NS(get=lambda: NS(store_id=mine))
        rows = self._read(store, model_proxy=proxy)
        self.assertEqual([r.log_one_plus_z for r in rows], [2.0, 3.0])
        self.assertEqual([r.store_id for r in rows], [serials[2], serials[0]])

        everything = self._read(store)
        self.assertEqual(len(everything), 4)


class TestTheValueFactory(_TempStoreCase):
    def test_a_value_row_is_not_built_on_its_own(self):
        """`object_get("ScalarHistoryValue", ...)` has no caller; its build refuses, naming the
        factory through which value rows are written and read. Its phi-against-pi check went with
        it ([01-scalarmodelvalue-build-checks-the-stored-phi-against-pi])."""
        store = self.open(LABEL)
        with self.assertRaises(NotImplementedError) as ctx:
            with contextlib.redirect_stdout(io.StringIO()):
                store.object_get(
                    "ScalarHistoryValue", history_serial=1, z=NS(store_id=1)
                )
        self.assertIn("sqla_ScalarHistoryFactory", str(ctx.exception))
        self.assertEqual(_count(store, "ScalarHistoryValue"), 0)


if __name__ == "__main__":
    import unittest

    unittest.main()
