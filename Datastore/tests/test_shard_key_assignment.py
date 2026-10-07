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
Shard-key persistence in `ShardedPool._assign_shard_keys`.

The shard-key map is held twice: in memory (`ShardedPool._shard_keys`) and in the
primary's `shard_keys` table, from which `_read_shard_data` rebuilds it when a store
is reopened. The two must agree, or a reopened store routes a key to a shard that does
not hold its rows. This is the fix of SecondaryGWKit `2610abe` (B1, B5).

No Ray cluster and no persistent datastore. The pool is built with `object.__new__`,
so no actor is started, and is given only the attributes `_assign_shard_keys` reads:
a SQLite engine on a file in a `tempfile` directory, and the `shards` and
`shard_keys` tables as `ShardedPool.__init__` defines them (copied here, since the
constructor builds them inline). About 0.1 s.
"""

import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

import sqlalchemy as sqla

from Datastore.SQL.ShardedPool import ShardedPool


class _Key:
    """A stand-in shard key: `_assign_shard_keys` reads only `store_id`."""

    def __init__(self, store_id):
        self.store_id = store_id


class TestShardKeyAssignment(unittest.TestCase):
    SHARDS = 3

    def setUp(self):
        self._dir = tempfile.mkdtemp()
        engine = sqla.create_engine(
            f"sqlite:///{Path(self._dir) / 'primary.db'}", future=True
        )
        metadata = sqla.MetaData()
        shard_file_table = sqla.Table(
            "shards",
            metadata,
            sqla.Column("serial", sqla.Integer, primary_key=True, nullable=False),
            sqla.Column("filename", sqla.String(256), nullable=False),
        )
        shard_key_table = sqla.Table(
            "shard_keys",
            metadata,
            sqla.Column("key_serial", sqla.Integer, primary_key=True, nullable=False),
            sqla.Column(
                "shard_id",
                sqla.Integer,
                sqla.ForeignKey("shards.serial"),
                index=True,
                nullable=False,
            ),
        )
        metadata.create_all(engine)
        with engine.begin() as conn:
            conn.execute(
                sqla.insert(shard_file_table),
                [
                    {"serial": i, "filename": f"shard{i:04d}.db"}
                    for i in range(self.SHARDS)
                ],
            )

        pool = object.__new__(ShardedPool)
        pool._engine = engine
        pool._shard_key_table = shard_key_table
        pool._shards = {i: None for i in range(self.SHARDS)}
        pool._shard_keys = {}
        pool._ShardKeyType = _Key
        pool._ShardKeyType_name = "_Key"
        self.pool = pool

    def tearDown(self):
        self.pool._engine.dispose()
        shutil.rmtree(self._dir)

    def _assign(self, store_ids):
        out = StringIO()
        with redirect_stdout(out):
            self.pool._assign_shard_keys([_Key(s) for s in store_ids])
        return out.getvalue()

    def _stored_map(self):
        """The map `_read_shard_data` would rebuild on reopening the store."""
        with self.pool._engine.connect() as conn:
            rows = conn.execute(
                sqla.select(
                    self.pool._shard_key_table.c.key_serial,
                    self.pool._shard_key_table.c.shard_id,
                )
            )
            return {row.key_serial: row.shard_id for row in rows}

    def test_stored_map_matches_memory_for_keys_out_of_order(self):
        """Keys assigned out of store_id order are stored under their own store_id."""
        printed = self._assign([7, 3, 11, 5])
        self.assertEqual(set(self.pool._shard_keys), {3, 5, 7, 11})
        self.assertEqual(self._stored_map(), self.pool._shard_keys)
        self.assertNotIn("MISMATCH", printed)

    def test_repeated_key_in_one_batch_is_assigned_once(self):
        """A key repeated within one batch gets one row and is counted once for load."""
        self._assign([4, 9, 4, 4])
        stored = self._stored_map()
        self.assertEqual(set(stored), {4, 9})
        self.assertEqual(stored, self.pool._shard_keys)
        self.assertNotEqual(
            stored[4], stored[9], "two keys on three empty shards share a shard"
        )

    def test_later_batch_adds_only_new_keys(self):
        """A second batch leaves known keys where they are and stores only new ones."""
        self._assign([20, 10])
        first = dict(self.pool._shard_keys)
        self._assign([10, 30, 20, 30])
        stored = self._stored_map()
        self.assertEqual(set(stored), {10, 20, 30})
        self.assertEqual(stored, self.pool._shard_keys)
        for key, shard in first.items():
            self.assertEqual(stored[key], shard)

    def test_load_is_balanced_across_shards(self):
        """Six new keys on three empty shards go two to a shard."""
        self._assign([60, 50, 40, 30, 20, 10])
        counts = {}
        for shard in self._stored_map().values():
            counts[shard] = counts.get(shard, 0) + 1
        self.assertEqual(counts, {0: 2, 1: 2, 2: 2})


if __name__ == "__main__":
    unittest.main()
