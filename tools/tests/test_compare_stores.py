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
The comparison of two stores by value, `tools/compare_stores.py` (dk-and-rpk-migration prompt
01, item T2; README section 4.2), and the rehearsal log parser
`.documents/dk-and-rpk-migration/rehearsal/stage_counts.py` (item T3).

**Imitation stores.** Tests 1-12 build their stores with `sqlite3` alone, in a `tempfile`
directory: a primary with the layer's tables (`shards`, `shard_key_config`, `shard_keys`,
`replicated_tables`, `sharded_tables`) and two shards, each holding a `version` table, a
replicated parameter table `param`, two replicated tables `Alpha` and `Beta` that a polymorphic
pair (`Parent.thing_type`/`thing_serial`) names, a replicated `store_tag`, a sharded parent table
`Parent` with a foreign key into `param` and a `version` column, a sharded value table `Value`
with a foreign key into `Parent`, and a tag table `Parent_tags` with no serial. `Parent.label` is
declared bookkeeping for these tests; `serial` and `timestamp` are the tool's own global
bookkeeping. The primary also lists a replicated table `Ghost` that no shard holds, as a ChamPBH
primary lists `LambdaCDM`.

**A schema-faithful store.** Tests 13 and 14 read the factories' `register()` (an instance
method, which needs no store and no Ray), and test 14 builds a store whose shards have exactly
the tables ChamPBH's actor would create from them, through SQLAlchemy's `MetaData.create_all` on
temporary files (never through the layer), with the primary's lists from `config.sharding`.

No Ray, no PRyMordial solve, no persistent datastore: no store outside a `tempfile` directory is
opened. A few seconds, most of it importing the factories.
"""

import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import math
import os
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from urllib.parse import unquote

import tools.compare_stores as cs

ROOT = Path(__file__).resolve().parents[2]

# the imitation's declared maps, added to the tool's own for the duration of each test
IMITATION_BOOKKEEPING = {
    "Parent": {"label": "a job name and a time, as ScalarHistory.label"}
}
IMITATION_POLYMORPHIC = {
    "Parent": {"thing_serial": ("thing_type", {0: "Alpha", 1: "Beta"})}
}

IMITATION_SCHEMA = """
create table version (serial integer primary key, label varchar(256));
create table store_tag (serial integer primary key, timestamp datetime, label varchar(256));
create table param (serial integer primary key, timestamp datetime, value float);
create index ix_param_value on param (value);
create table Alpha (
    serial integer primary key,
    version integer references version (serial),
    timestamp datetime,
    param_serial integer not null references param (serial)
);
create table Beta (serial integer primary key, timestamp datetime, n_value integer not null);
create table Parent (
    serial integer primary key,
    version integer references version (serial),
    timestamp datetime,
    label varchar(256),
    param_serial integer not null references param (serial),
    thing_type integer not null,
    thing_serial integer not null,
    x float,
    failure boolean not null,
    failure_reason varchar(256)
);
create index ix_Parent_param_serial on Parent (param_serial);
create table Parent_tags (
    parent_serial integer not null references Parent (serial),
    tag_serial integer not null references store_tag (serial),
    timestamp datetime,
    primary key (parent_serial, tag_serial)
);
create table Value (
    serial integer primary key,
    parent_serial integer not null references Parent (serial),
    z_serial integer not null references param (serial),
    y float not null
);
create index ix_Value_parent_serial on Value (parent_serial);
"""

IMITATION_REPLICATED = ["version", "store_tag", "param", "Alpha", "Beta", "Ghost"]
IMITATION_SHARDED = {"Parent": "shard_key", "Value": "shard_key"}

CONTENT = {
    "versions": ["2026.6.0"],
    "tags": ["SamplesPerLog10Z_100"],
    "params": [1.1, 1.15, 0.5, 0.25, 0.125],
    "alphas": [1.1],
    "betas": [4],
    "parents": [
        {
            "param": 1.1,
            "thing": ("Alpha", 0),
            "x": 0.1,
            "failure": 0,
            "failure_reason": None,
            "label": "job-A-ScalarHistory-2026-10-11T12:00:00",
            "values": [(0.5, 2.5), (0.25, 3.5), (0.125, 4.5)],
            "tags": ["SamplesPerLog10Z_100"],
            "shard": 0,
        },
        {
            "param": 1.15,
            "thing": ("Beta", 0),
            "x": 0.2,
            "failure": 0,
            "failure_reason": None,
            "label": "job-A-ScalarHistory-2026-10-11T12:01:00",
            "values": [(0.5, 5.5), (0.25, 6.5)],
            "tags": [],
            "shard": 1,
        },
    ],
}

TYPE_OF = {"Alpha": 0, "Beta": 1}


def _write_primary(path: Path, shard_records, replicated, sharded):
    conn = sqlite3.connect(path)
    with conn:
        conn.executescript("""
            create table shards (serial integer primary key not null,
                                 filename varchar(256) not null);
            create table shard_key_config (key_type varchar(256) primary key not null);
            create table shard_keys (key_serial integer primary key not null,
                                     shard_id integer not null references shards (serial));
            create table replicated_tables (serial integer primary key not null,
                                            "table" varchar(256) not null);
            create table sharded_tables (serial integer primary key not null,
                                         "table" varchar(256) not null,
                                         key_attr varchar(256) not null);
            """)
        conn.executemany(
            "insert into shards (serial, filename) values (?, ?)",
            list(enumerate(shard_records)),
        )
        conn.execute("insert into shard_key_config (key_type) values ('beta_value')")
        conn.executemany(
            'insert into replicated_tables (serial, "table") values (?, ?)',
            list(enumerate(replicated)),
        )
        conn.executemany(
            'insert into sharded_tables (serial, "table", key_attr) values (?, ?, ?)',
            [(n, t, k) for n, (t, k) in enumerate(sharded.items())],
        )
    conn.close()


def build_store(
    directory: Path,
    stem: str,
    content=None,
    *,
    shards: int = 2,
    serial_offset: int = 0,
    shard_of=None,
    records: str = "bare",
    timestamp: str = "2026-10-11 12:00:00",
    absolute_records=None,
) -> Path:
    """
    An imitation store at directory/stem.db with `shards` shards. Replicated rows carry the same
    serials on every shard; `serial_offset` shifts every serial, and `shard_of` (a list, one
    entry per parent) moves the parents and their values to other shards. `records` is "bare"
    (the package's form) or "absolute" (ChamPBH's), and `absolute_records`, if given, are the
    absolute paths recorded instead (which need not exist).
    """
    content = copy.deepcopy(CONTENT if content is None else content)
    directory.mkdir(parents=True, exist_ok=True)
    primary = directory / f"{stem}.db"
    shard_paths = [directory / f"{stem}-shard{i:04d}.db" for i in range(shards)]

    if absolute_records is not None:
        recorded = [str(p) for p in absolute_records]
    elif records == "absolute":
        recorded = [str(p.resolve()) for p in shard_paths]
    else:
        recorded = [p.name for p in shard_paths]
    _write_primary(primary, recorded, IMITATION_REPLICATED, IMITATION_SHARDED)

    off = serial_offset
    version_serial = {label: off + 1 + i for i, label in enumerate(content["versions"])}
    tag_serial = {label: off + 10 + i for i, label in enumerate(content["tags"])}
    param_serial = {value: off + 100 + i for i, value in enumerate(content["params"])}
    alpha_serial = [off + 200 + i for i in range(len(content["alphas"]))]
    beta_serial = [off + 300 + i for i in range(len(content["betas"]))]
    version = version_serial[content["versions"][0]]

    conns = []
    for path in shard_paths:
        conn = sqlite3.connect(path)
        conn.executescript(IMITATION_SCHEMA)
        for label, serial in version_serial.items():
            conn.execute(
                "insert into version (serial, label) values (?, ?)", (serial, label)
            )
        for label, serial in tag_serial.items():
            conn.execute(
                "insert into store_tag (serial, timestamp, label) values (?, ?, ?)",
                (serial, timestamp, label),
            )
        for value, serial in param_serial.items():
            conn.execute(
                "insert into param (serial, timestamp, value) values (?, ?, ?)",
                (serial, timestamp, value),
            )
        for value, serial in zip(content["alphas"], alpha_serial):
            conn.execute(
                "insert into Alpha (serial, version, timestamp, param_serial) "
                "values (?, ?, ?, ?)",
                (serial, version, timestamp, param_serial[value]),
            )
        for n, serial in zip(content["betas"], beta_serial):
            conn.execute(
                "insert into Beta (serial, timestamp, n_value) values (?, ?, ?)",
                (serial, timestamp, n),
            )
        conns.append(conn)

    for i, parent in enumerate(content["parents"]):
        shard = parent["shard"] if shard_of is None else shard_of[i]
        conn = conns[shard]
        serial = off + 1000 + 10 * i
        kind, index = parent["thing"]
        thing_serial = (alpha_serial if kind == "Alpha" else beta_serial)[index]
        conn.execute(
            "insert into Parent (serial, version, timestamp, label, param_serial, thing_type, "
            "thing_serial, x, failure, failure_reason) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                serial,
                version,
                parent.get("timestamp", timestamp),
                parent["label"],
                param_serial[parent["param"]],
                parent.get("thing_type", TYPE_OF[kind]),
                thing_serial,
                parent["x"],
                parent["failure"],
                parent["failure_reason"],
            ),
        )
        for tag in parent["tags"]:
            conn.execute(
                "insert into Parent_tags (parent_serial, tag_serial, timestamp) "
                "values (?, ?, ?)",
                (serial, tag_serial[tag], timestamp),
            )
        for j, (z, y) in enumerate(parent["values"]):
            conn.execute(
                "insert into Value (serial, parent_serial, z_serial, y) values (?, ?, ?, ?)",
                (off + 5000 + 100 * i + j, serial, param_serial[z], y),
            )

    for conn in conns:
        conn.commit()
        conn.close()
    return primary


def _tree_state(directory: Path) -> dict:
    """Every file under `directory`: its sha256, size and modification time."""
    state = {}
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            stat = path.stat()
            state[str(path)] = (
                hashlib.sha256(path.read_bytes()).hexdigest(),
                stat.st_size,
                stat.st_mtime_ns,
            )
    return state


class _CompareCase(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="champbh-compare-stores-test-"))
        patchers = [
            mock.patch.dict(cs.BOOKKEEPING_COLUMNS, IMITATION_BOOKKEEPING),
            mock.patch.dict(cs.POLYMORPHIC_REFERENCES, IMITATION_POLYMORPHIC),
        ]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def run_tool(self, *argv):
        """(exit code, stdout, the --json result or None) of the tool's main()."""
        out = io.StringIO()
        json_path = self.dir / "result.json"
        if json_path.exists():
            json_path.unlink()
        args = list(map(str, argv))
        if len(args) == 2 and not args[0].startswith("--"):
            args += ["--json", str(json_path)]
        with contextlib.redirect_stdout(out):
            code = cs.main(args)
        result = json.loads(json_path.read_text()) if json_path.exists() else None
        return code, out.getvalue(), result

    def two(self, content_b=None, **kwargs_b):
        a = build_store(self.dir / "a", "store")
        b = build_store(self.dir / "b", "store", content_b, **kwargs_b)
        return a, b


class TestCompareStores(_CompareCase):
    def test_01_equal_stores(self):
        """(1) Two equal stores: nothing differs, no problem, exit 0."""
        a, b = self.two()
        code, text, result = self.run_tool(a, b)
        self.assertEqual(code, cs.EXIT_EQUAL, text)
        self.assertTrue(result["equal"])
        self.assertEqual(result["problems"], [])
        for table, entry in result["tables"].items():
            self.assertEqual(entry["a_only"], 0, table)
            self.assertEqual(entry["b_only"], 0, table)
        self.assertEqual(result["tables"]["Value"]["matched"], 5)
        self.assertEqual(result["tables"]["Parent_tags"]["matched"], 1)
        self.assertEqual(result["tables"]["param"]["matched"], 5)
        self.assertIn("RESULT: equal", text)

    def test_02_one_float_changed_in_its_last_bit(self):
        """(2) One float moved by one ulp (math.nextafter): named with both values, exit 1."""
        content = copy.deepcopy(CONTENT)
        old = content["parents"][0]["x"]
        new = math.nextafter(old, math.inf)
        content["parents"][0]["x"] = new
        a, b = self.two(content)
        code, text, result = self.run_tool(a, b)
        self.assertEqual(code, cs.EXIT_DIFFERENT, text)
        self.assertFalse(result["equal"])
        parent = result["tables"]["Parent"]
        self.assertEqual((parent["a_only"], parent["b_only"]), (1, 1))
        self.assertEqual(len(parent["near_misses"]), 1)
        (diff,) = parent["near_misses"][0]["differing"]
        self.assertEqual(diff["column"], "x")
        self.assertEqual((diff["a"], diff["b"]), (old, new))
        self.assertEqual((diff["a_hex"], diff["b_hex"]), (old.hex(), new.hex()))
        self.assertGreater(diff["relative_difference"], 0.0)
        self.assertIn(repr(new), text)
        self.assertIn(new.hex(), text)

    def test_03_a_row_in_one_store_only(self):
        """(3) A value row in B only is named by its physical labels: its parent's columns and
        its z, not a serial or a digest."""
        content = copy.deepcopy(CONTENT)
        content["parents"][1]["values"].append((0.125, 7.5))
        a, b = self.two(content)
        code, text, result = self.run_tool(a, b)
        self.assertEqual(code, cs.EXIT_DIFFERENT, text)
        value = result["tables"]["Value"]
        self.assertEqual((value["a_only"], value["b_only"]), (0, 1))
        self.assertEqual(value["near_misses"], [])
        (row,) = value["rows_b_only"]
        self.assertEqual(row["y"], 7.5)
        self.assertEqual(row["z_serial"]["row"], {"value": 0.125})
        parent = row["parent_serial"]["row"]
        self.assertEqual(parent["x"], 0.2)
        self.assertEqual(parent["param_serial"]["row"], {"value": 1.15})
        self.assertEqual(parent["thing_serial"]["table"], "Beta")
        self.assertEqual(parent["thing_serial"]["row"], {"n_value": 4})
        self.assertEqual(parent["version"]["row"], {"label": "2026.6.0"})
        self.assertNotIn("serial", row)
        self.assertNotIn("label", parent)
        self.assertIn("Value{parent_serial=Parent{", text)
        self.assertIn("y=7.5", text)

    def test_04_serials_and_shard_assignment_do_not_matter(self):
        """(4) Equal content under other serials, with the parents on other shards: equal."""
        a, b = self.two(serial_offset=70000, shard_of=[1, 0])
        code, text, result = self.run_tool(a, b)
        self.assertEqual(code, cs.EXIT_EQUAL, text)
        self.assertTrue(result["equal"])
        self.assertEqual(result["problems"], [])

    def test_05_a_bookkeeping_column_that_differs(self):
        """(5) timestamp (everywhere) and Parent.label (declared) differ: reported apart from
        the result, exit 0."""
        content = copy.deepcopy(CONTENT)
        for parent in content["parents"]:
            parent["label"] = parent["label"].replace("job-A", "job-B")
        a, b = self.two(content, timestamp="2026-10-12 09:30:00")
        code, text, result = self.run_tool(a, b)
        self.assertEqual(code, cs.EXIT_EQUAL, text)
        self.assertTrue(result["equal"])
        bookkeeping = result["bookkeeping_differences"]
        self.assertEqual(bookkeeping["Parent"], {"label": 2, "timestamp": 2})
        self.assertEqual(bookkeeping["param"], {"timestamp": 5})
        self.assertIn("Parent.timestamp: 2 matched row(s)", text)
        self.assertIn("Parent.label: 2 matched row(s)", text)

    def test_06_absolute_and_bare_shard_records(self):
        """(6) A primary recording absolute paths and one recording bare names are both read,
        by the glob; no recorded path is ever opened. Every connection is mode=ro on the
        primary or a globbed shard."""
        bare = build_store(self.dir / "bare", "store", records="bare")
        absolute = build_store(self.dir / "absolute", "store", records="absolute")
        nowhere = [self.dir / "nowhere" / f"gone-shard{i:04d}.db" for i in range(2)]
        missing = build_store(self.dir / "missing", "store", absolute_records=nowhere)
        # the hazard: a copied primary whose absolute records name another store's shards
        other = copy.deepcopy(CONTENT)
        other["parents"][0]["x"] = 99.0
        decoy = build_store(self.dir / "decoy", "store", other)
        decoy_shards = sorted((self.dir / "decoy").glob("store-shard*.db"))
        copied = build_store(
            self.dir / "copied", "store", absolute_records=decoy_shards
        )

        opened = []
        real_connect = sqlite3.connect

        def recording_connect(database, *args, **kwargs):
            opened.append(database)
            return real_connect(database, *args, **kwargs)

        cases = [(bare, absolute), (bare, missing), (bare, copied)]
        for a, b in cases:
            with self.subTest(b=b.parent.name):
                opened.clear()
                with mock.patch("sqlite3.connect", recording_connect):
                    code, text, result = self.run_tool(a, b)
                self.assertEqual(code, cs.EXIT_EQUAL, text)
                allowed = {
                    str(p)
                    for store in (a, b)
                    for p in [store] + sorted(store.parent.glob("store-shard*.db"))
                }
                # the primary and both shards of each store, and nothing else
                self.assertEqual(len(opened), 6)
                paths = []
                for uri in opened:
                    self.assertTrue(uri.startswith("file:"), uri)
                    self.assertTrue(uri.endswith("?mode=ro"), uri)
                    paths.append(unquote(uri[len("file:") : -len("?mode=ro")]))
                self.assertEqual(set(paths), allowed)
                for path in nowhere + decoy_shards:
                    self.assertNotIn(str(path), paths)
        self.assertNotEqual(decoy, copied)

    def test_07_a_shard_count_that_disagrees(self):
        """(7) A shard more or less than the primary's shards table records: exit 2."""
        a = build_store(self.dir / "a", "store")
        fewer = build_store(self.dir / "fewer", "store")
        (self.dir / "fewer" / "store-shard0001.db").unlink()
        more = build_store(self.dir / "more", "store")
        shutil.copy(
            self.dir / "more" / "store-shard0001.db",
            self.dir / "more" / "store-shard0002.db",
        )
        for b in (fewer, more):
            with self.subTest(b=b.parent.name):
                code, text, result = self.run_tool(a, b)
                self.assertEqual(code, cs.EXIT_UNREADABLE, text)
                self.assertIsNone(result)
                self.assertIn("shards table records 2 shard(s)", text)
        code, text, _ = self.run_tool(a, self.dir / "absent.db")
        self.assertEqual(code, cs.EXIT_UNREADABLE, text)

    def test_08_replicated_copies_that_differ(self):
        """(8) One shard's copy of a replicated table differs from the other's: a named problem
        (exit 1), though the lowest shard's copies of the two stores agree."""
        a, b = self.two()
        conn = sqlite3.connect(self.dir / "b" / "store-shard0001.db")
        with conn:
            conn.execute("update param set value = 0.0625 where value = 0.125")
        conn.close()
        code, text, result = self.run_tool(a, b)
        self.assertEqual(code, cs.EXIT_DIFFERENT, text)
        kinds = [(p["store"], p["kind"]) for p in result["problems"]]
        self.assertIn(("B", "replicated copies differ"), kinds)
        (problem,) = [
            p for p in result["problems"] if p["kind"] == "replicated copies differ"
        ]
        self.assertIn("param", problem["message"])
        self.assertEqual(result["tables"]["param"]["matched"], 5)

    def test_09_an_unknown_polymorphic_type_value(self):
        """(9) A type value the map lacks is a named problem, not an exception."""
        content = copy.deepcopy(CONTENT)
        content["parents"][0]["thing_type"] = 7
        a, b = self.two(content)
        code, text, result = self.run_tool(a, b)
        self.assertEqual(code, cs.EXIT_DIFFERENT, text)
        (problem,) = [
            p for p in result["problems"] if p["kind"] == "unknown type value"
        ]
        self.assertEqual(problem["store"], "B")
        self.assertIn("Parent.thing_type = 7", problem["message"])
        # one row, counted once, though the row is read again to render it
        self.assertEqual(problem["count"], 1)
        self.assertEqual(result["tables"]["Parent"]["b_only"], 1)

    def test_10_a_near_miss_is_paired(self):
        """(10) A value row whose float differs is paired with its counterpart as one near miss,
        not listed as two unrelated rows."""
        content = copy.deepcopy(CONTENT)
        content["parents"][0]["values"][1] = (0.25, 3.5 * (1 + 1e-9))
        a, b = self.two(content)
        code, text, result = self.run_tool(a, b)
        self.assertEqual(code, cs.EXIT_DIFFERENT, text)
        value = result["tables"]["Value"]
        self.assertEqual((value["a_only"], value["b_only"]), (1, 1))
        self.assertEqual(value["rows_a_only"], [])
        self.assertEqual(value["rows_b_only"], [])
        (pair,) = value["near_misses"]
        (diff,) = pair["differing"]
        self.assertEqual(diff["column"], "y")
        self.assertAlmostEqual(diff["relative_difference"], 1e-9, delta=1e-12)
        self.assertEqual(pair["a"]["z_serial"]["row"], {"value": 0.25})
        self.assertEqual(result["tables"]["Parent"]["matched"], 2)

    def test_10b_children_of_a_near_miss_pair_with_their_counterparts(self):
        """(10, continued) When a parent is a near miss, its values and tags pair with their
        counterparts, differing only through the parent; a value that also differs in its own
        float lists that float. Each child is paired with the counterpart of its own z.
        """
        content = copy.deepcopy(CONTENT)
        content["parents"][0]["x"] = math.nextafter(0.1, 1.0)
        content["parents"][0]["values"][2] = (0.125, 4.5 * (1 + 1e-6))
        a, b = self.two(content, serial_offset=11, shard_of=[1, 0])
        code, text, result = self.run_tool(a, b)
        self.assertEqual(code, cs.EXIT_DIFFERENT, text)
        value = result["tables"]["Value"]
        self.assertEqual((value["a_only"], value["b_only"]), (3, 3))
        self.assertEqual((value["rows_a_only"], value["rows_b_only"]), ([], []))
        self.assertEqual(len(value["near_misses"]), 3)
        for pair in value["near_misses"]:
            self.assertEqual(pair["through_references"], ["parent_serial"])
            self.assertEqual(pair["a"]["z_serial"], pair["b"]["z_serial"])
        own = [p for p in value["near_misses"] if p["differing"]]
        (pair,) = own
        self.assertEqual(pair["a"]["z_serial"]["row"], {"value": 0.125})
        self.assertEqual([d["column"] for d in pair["differing"]], ["y"])
        tags = result["tables"]["Parent_tags"]
        (tag_pair,) = tags["near_misses"]
        self.assertEqual(tag_pair["differing"], [])
        self.assertEqual(tag_pair["through_references"], ["parent_serial"])
        self.assertIn("differing only through parent_serial", text)

    def test_11_schema_snapshot(self):
        """(11) The schema snapshot round-trips (exit 0 against itself); a dropped index, a
        changed column and a dropped table are each reported (exit 1)."""
        a = build_store(self.dir / "a", "store")
        snapshot = self.dir / "a.json"
        code, text, _ = self.run_tool("--snapshot", a, "--out", snapshot)
        self.assertEqual(code, cs.EXIT_EQUAL, text)
        data = json.loads(snapshot.read_text())
        self.assertEqual(data["format"], cs.SNAPSHOT_FORMAT)
        self.assertIn(
            ["parent_serial", "tag_serial"],
            [i[0] for i in data["tables"]["Parent_tags"]["indices"]],
        )
        self.assertIn(
            [["param_serial"], "param", ["serial"]],
            data["tables"]["Parent"]["foreign_keys"],
        )
        code, text, _ = self.run_tool("--schema", a, "--against", snapshot)
        self.assertEqual(code, cs.EXIT_EQUAL, text)
        self.assertIn("RESULT: the schemas agree", text)

        b = build_store(self.dir / "b", "store")
        conn = sqlite3.connect(self.dir / "b" / "store-shard0000.db")
        with conn:
            conn.execute("drop index ix_Value_parent_serial")
            conn.execute("alter table Parent add column extra float")
        conn.close()
        code, text, _ = self.run_tool("--schema", b, "--against", snapshot)
        self.assertEqual(code, cs.EXIT_DIFFERENT, text)
        self.assertIn(
            'Value: index [["parent_serial"], false] in the reference only', text
        )
        self.assertIn("Parent: column extra", text)

        # column order is not a difference
        c = build_store(self.dir / "c", "store")
        conn = sqlite3.connect(self.dir / "c" / "store-shard0000.db")
        with conn:
            conn.executescript("""
                create table Beta2 (n_value integer not null, timestamp datetime,
                                    serial integer primary key);
                insert into Beta2 select n_value, timestamp, serial from Beta;
                drop table Beta;
                alter table Beta2 rename to Beta;
                """)
        conn.close()
        code, text, _ = self.run_tool("--schema", c, "--against", snapshot)
        self.assertEqual(code, cs.EXIT_EQUAL, text)

    def test_12_no_file_the_tool_reads_changes(self):
        """(12) Every file's checksum, size and modification time are the same after the tool,
        and it leaves no new file (no journal) beside the stores."""
        a, b = self.two(serial_offset=3)
        before = _tree_state(self.dir)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cs.main([str(a), str(b)])
            cs.main(["--snapshot", str(a)])
        self.assertEqual(code, cs.EXIT_EQUAL, out.getvalue())
        self.assertEqual(_tree_state(self.dir), before)

    def test_a_listed_replicated_table_that_no_shard_holds(self):
        """A table the primary lists as replicated but no shard holds (as LambdaCDM in a
        ChamPBH store) is noted, not compared, and changes nothing in the result."""
        a, b = self.two()
        code, text, result = self.run_tool(a, b)
        self.assertEqual(code, cs.EXIT_EQUAL, text)
        self.assertNotIn("Ghost", result["tables"])
        self.assertTrue(any("Ghost" in note for note in result["notes"]))

    def test_classification(self):
        """Listed tables take the primary's class; the unlisted tag table, whose parent is
        sharded, is sharded; no layer table is a client table."""
        a, b = self.two()
        _, _, result = self.run_tool(a, b)
        kinds = {t: e["kind_a"] for t, e in result["tables"].items()}
        self.assertEqual(
            kinds,
            {
                "Alpha": "replicated",
                "Beta": "replicated",
                "Parent": "sharded",
                "Parent_tags": "sharded",
                "Value": "sharded",
                "param": "replicated",
                "store_tag": "replicated",
                "version": "replicated",
            },
        )

    def test_a_masked_fragment(self):
        """A run-dependent fragment of a physics column (the seconds elapsed in a wall-clock
        failure) is masked in the identity, and the raw difference is reported as
        bookkeeping."""
        message = (
            "PRyMordial: PRyMWallClockLimitError: wall-clock limit of 600 s exceeded in stage "
            "'low-T nuclear network (small)': {} s elapsed"
        )
        content_a = copy.deepcopy(CONTENT)
        content_a["parents"][1]["failure"] = 1
        content_a["parents"][1]["failure_reason"] = message.format("612.3")
        content_b = copy.deepcopy(content_a)
        content_b["parents"][1]["failure_reason"] = message.format("655.01")
        masked = {
            "Parent": {
                "failure_reason": cs.MASKED_FRAGMENTS["BBNData"]["failure_reason"]
            }
        }
        a = build_store(self.dir / "a", "store", content_a)
        b = build_store(self.dir / "b", "store", content_b)
        result = cs.compare_stores(a, b, masked=masked)
        self.assertTrue(result["equal"])
        self.assertEqual(
            result["bookkeeping_differences"]["Parent"], {"failure_reason": 1}
        )
        # another stage is a difference
        content_c = copy.deepcopy(content_a)
        content_c["parents"][1]["failure_reason"] = message.format("612.3").replace(
            "low-T", "mid-T"
        )
        c = build_store(self.dir / "c", "store", content_c)
        self.assertFalse(cs.compare_stores(a, c, masked=masked)["equal"])

    def test_help(self):
        """--help prints the usage and exits 0."""
        out = io.StringIO()
        with contextlib.redirect_stdout(out), self.assertRaises(SystemExit) as ctx:
            cs.main(["--help"])
        self.assertEqual(ctx.exception.code, 0)
        self.assertIn("--snapshot", out.getvalue())


# ---------------------------------------------------------------------------
# the declared maps against the factories


def _registry():
    """The factory registry, config.datastore's. Importing it imports ray, but needs no cluster."""
    from config.datastore import factories

    return factories


def _registered_columns(registration):
    """The column names a ChamPBH actor gives a table, from its register() data."""
    names = []
    if registration.get("serial", True):
        names.append("serial")
    if registration.get("version", False):
        names.append("version")
    if registration.get("timestamp", False):
        names.append("timestamp")
    stepping = registration.get("stepping", False)
    if stepping is True or stepping in ("minimum", "exact"):
        names.append("stepping")
    return names + [c.name for c in registration.get("columns", [])]


def _model_ids(module):
    return {
        value
        for name, value in vars(module).items()
        if name.isupper() and isinstance(value, int)
    }


class TestDeclaredMaps(unittest.TestCase):
    def test_13_bookkeeping_and_polymorphic_maps_name_declared_columns(self):
        """(13) Every bookkeeping entry, masked column and polymorphic pair names a table and a
        column the factories declare; each polymorphic target is a registered table; the type
        values cover every identifier the code defines; and every *_serial column without a
        foreign key, and every *_type column, is a declared polymorphic pair."""
        from CosmologyConcepts.Models import model_ids
        from CosmologyModels import model_ids as cosmology_ids

        registry = _registry()
        registrations = {name: factory.register() for name, factory in registry.items()}
        columns = {
            name: _registered_columns(r) for name, r in registrations.items() if r
        }

        for table, entries in cs.BOOKKEEPING_COLUMNS.items():
            for column in entries:
                with self.subTest(bookkeeping=f"{table}.{column}"):
                    self.assertIn(table, columns)
                    self.assertIn(column, columns[table])
        for column in cs.GLOBAL_BOOKKEEPING:
            with self.subTest(bookkeeping=column):
                self.assertTrue(any(column in cols for cols in columns.values()))
        for table, entries in cs.MASKED_FRAGMENTS.items():
            for column in entries:
                with self.subTest(masked=f"{table}.{column}"):
                    self.assertIn(column, columns[table])

        declared_pairs = set()
        for table, pairs in cs.POLYMORPHIC_REFERENCES.items():
            by_name = {c.name: c for c in registrations[table]["columns"]}
            for serial_column, (type_column, mapping) in pairs.items():
                declared_pairs |= {(table, serial_column), (table, type_column)}
                with self.subTest(pair=f"{table}.{serial_column}"):
                    self.assertIn(serial_column, by_name)
                    self.assertIn(type_column, by_name)
                    self.assertEqual(len(by_name[serial_column].foreign_keys), 0)
                    for target in mapping.values():
                        self.assertIn(target, columns)
                        self.assertIn("serial", columns[target])

        # the type values: every identifier the code defines is mapped or declared unstored
        spaces = {
            "cosmology_type": cosmology_ids,
            "model_type": model_ids,
        }
        pairs = cs.POLYMORPHIC_REFERENCES["ScalarHistory"]
        for serial_column, (type_column, mapping) in pairs.items():
            with self.subTest(type_values=type_column):
                unstored = cs.UNSTORED_TYPE_VALUES.get("ScalarHistory", {}).get(
                    type_column, {}
                )
                self.assertEqual(set(mapping) & set(unstored), set())
                self.assertEqual(
                    set(mapping) | set(unstored), _model_ids(spaces[type_column])
                )

        # nothing polymorphic is left undeclared
        for table, registration in registrations.items():
            if not registration:
                continue
            for column in registration.get("columns", []):
                undeclared_serial = (
                    column.name.endswith("_serial") and len(column.foreign_keys) == 0
                )
                if undeclared_serial or column.name.endswith("_type"):
                    with self.subTest(column=f"{table}.{column.name}"):
                        self.assertIn((table, column.name), declared_pairs)

    def test_13b_the_model_map_follows_type_id(self):
        """Each mapped model table is the class of that name, whose type_id returns the mapped
        value (read from the class's source with ast; nothing is instantiated). Until
        jordan-normalization prompt 07 the maps were the potential's and the coupling's.
        """
        import ast
        import inspect

        import CosmologyConcepts.Models as models
        from CosmologyConcepts.Models import model_ids

        pairs = cs.POLYMORPHIC_REFERENCES["ScalarHistory"]
        for column, package, ids in (("model_serial", models, model_ids),):
            for value, table in pairs[column][1].items():
                with self.subTest(table=table):
                    cls = getattr(package, table)
                    tree = ast.parse(inspect.getsource(cls))
                    (method,) = [
                        node
                        for node in ast.walk(tree)
                        if isinstance(node, ast.FunctionDef) and node.name == "type_id"
                    ]
                    (ret,) = [n for n in ast.walk(method) if isinstance(n, ast.Return)]
                    self.assertIsInstance(ret.value, ast.Name)
                    self.assertEqual(getattr(ids, ret.value.id), value)


# ---------------------------------------------------------------------------
# a store with ChamPBH's own schema


def _create_shard(path: Path, registry):
    """A shard file with the tables datastorekit's actor creates from the factories' register()
    (datastorekit/SQL/Datastore.py _build_schema), built by SQLAlchemy, not by the package.
    """
    import sqlalchemy as sqla

    metadata = sqla.MetaData()
    for name, factory in registry.items():
        registration = factory.register()
        if registration is None:
            continue
        table = sqla.Table(name, metadata)
        if registration.get("serial", True):
            table.append_column(sqla.Column("serial", sqla.Integer, primary_key=True))
        if registration.get("version", False):
            table.append_column(
                sqla.Column(
                    "version",
                    sqla.Integer,
                    sqla.ForeignKey("version.serial"),
                    index=True,
                )
            )
        if registration.get("timestamp", False):
            table.append_column(sqla.Column("timestamp", sqla.DateTime()))
        stepping = registration.get("stepping", False)
        if stepping is True or stepping in ("minimum", "exact"):
            table.append_column(sqla.Column("stepping", sqla.Integer))
        for column in registration.get("columns", []):
            table.append_column(column)
    engine = sqla.create_engine(f"sqlite:///{path}")
    metadata.create_all(engine)
    engine.dispose()


def _insert(conn, table, **values):
    names = ", ".join(f'"{k}"' for k in values)
    marks = ", ".join("?" for _ in values)
    conn.execute(
        f'insert into "{table}" ({names}) values ({marks})', list(values.values())
    )


TS = "2026-10-11 12:00:00.000000"


def _fill_champbh_shard(conn, sm_shard: bool, offset: int, bbn_reason: str, bk: str):
    """Replicated rows on every shard; one history (ScalarHistory, its values, tags,
    AdiabaticHistory, BBNData) on the shard where `sm_shard` is true."""
    o = offset
    _insert(conn, "version", serial=o + 1, label="2026.6.0")
    _insert(conn, "store_tag", serial=o + 2, timestamp=TS, label="SamplesPerLog10Z_100")
    _insert(conn, "tolerance", serial=o + 3, timestamp=TS, log10_tol=-8.0)
    _insert(conn, "tolerance", serial=o + 4, timestamp=TS, log10_tol=-10.0)
    _insert(conn, "beta_value", serial=o + 5, timestamp=TS, value=1.15)
    _insert(conn, "M_value", serial=o + 6, timestamp=TS, value_eV=2.435e24)
    # the tuning tolerance, in the serial Lambda_value had until jordan-normalization prompt 07
    _insert(conn, "tolerance", serial=o + 7, timestamp=TS, log10_tol=-5.0)
    _insert(conn, "temperature", serial=o + 8, timestamp=TS, value_GeV=2e4)
    _insert(conn, "phi_value", serial=o + 10, timestamp=TS, value_PlanckMass=5.0)
    _insert(conn, "pi_value", serial=o + 11, timestamp=TS, value_PlanckMass=0.0)
    _insert(
        conn, "redshift", serial=o + 12, timestamp=TS, log_one_plus_z=math.log1p(1e12)
    )
    _insert(
        conn, "redshift", serial=o + 13, timestamp=TS, log_one_plus_z=math.log1p(1e9)
    )
    _insert(
        conn,
        "QCD_Cosmology",
        serial=o + 15,
        timestamp=TS,
        name="QCD equation of state",
        omega_m=0.3,
        omega_cc=0.7,
        h=0.68,
        f_baryon=0.157,
        T_CMB_Kelvin=2.7255,
        Neff=3.046,
    )
    _insert(
        conn,
        "IntegrationSolver",
        serial=o + 16,
        timestamp=TS,
        stepping=0,
        label="solve_ivp+Radau",
    )
    if not sm_shard:
        return
    _insert(
        conn,
        "ExponentialModel",
        serial=o + 20,
        version=o + 1,
        timestamp=TS,
        M_serial=o + 6,
        n_value=1,
        beta_serial=o + 5,
    )
    _insert(
        conn,
        "ScalarHistory",
        serial=o + 30,
        version=o + 1,
        timestamp=TS,
        label=f"{bk}-ScalarHistory",
        cosmology_type=3,
        cosmology_serial=o + 15,
        model_type=1,
        model_serial=o + 20,
        atol_serial=o + 4,
        rtol_serial=o + 3,
        tuning_tol_serial=o + 7,
        phi_Einstein_init_serial=o + 10,
        pi_Einstein_init_serial=o + 11,
        T_Jordan_init_serial=o + 8,
        failure=0,
        solver_serial=o + 16,
        z_samples=2,
        compute_time=12.5 if bk == "a" else 13.75,
        compute_steps=1234,
        RHS_evaluations=5678,
        mean_RHS_time=1e-4,
        max_RHS_time=1e-3,
        min_RHS_time=1e-5,
        first_bounce_N=3.25,
        validated=1,
        extra_data=json.dumps({"reflections": 2, "accepted_steps": 900}),
    )
    _insert(
        conn,
        "ScalarHistory_tags",
        history_serial=o + 30,
        tag_serial=o + 2,
        timestamp=TS,
    )
    for i, z in enumerate((o + 12, o + 13)):
        values = {
            c: 0.5 + 0.25 * i + 0.01 * k
            for k, c in enumerate(
                (
                    "raw_N",
                    "phi_Einstein_Mp",
                    "pi_Einstein_Mp",
                    "log_rhorad_Einstein_Mp4",
                    "log_rhorad_Jordan_Mp4",
                    "log_fm",
                    "log_T_Jordan_GeV",
                    "H_Einstein_Mp",
                    "H_Jordan_Mp",
                    "gstar_rho",
                    "gstar_s",
                    "dgstar_rho_dlogT",
                    "dgstar_s_dlogT",
                    "Sigma",
                    "friction_term_Mp",
                    "reflecting_term_Mp",
                    "kicking_term_Mp",
                    "log_one_plus_z_Jordan",
                )
            )
        }
        _insert(
            conn,
            "ScalarHistoryValue",
            serial=o + 40 + i,
            history_serial=o + 30,
            z_serial=z,
            **values,
        )
    _insert(
        conn,
        "AdiabaticHistory",
        serial=o + 50,
        version=o + 1,
        timestamp=TS,
        scalar_history_serial=o + 30,
        compute_time=1.5 if bk == "a" else 1.75,
        max_abs_Q_kp_over_H_1E1=0.25,
        label=f"{bk}-AdiabaticHistory",
        z_samples=1,
        validated=1,
    )
    _insert(
        conn,
        "AdiabaticHistoryValue",
        serial=o + 51,
        history_serial=o + 50,
        z_serial=o + 12,
        raw_N=0.5,
        abs_Q_kp_over_H_1E1=0.125,
        abs_Q_kp_over_H_1E2=0.0625,
        abs_Q_kp_over_H_1E3=0.03125,
        abs_Q_kp_over_H_1E4=0.015625,
    )
    _insert(
        conn,
        "BBNData",
        serial=o + 60,
        version=o + 1,
        timestamp=TS,
        scalar_history_serial=o + 30,
        failure=1,
        failure_reason=bbn_reason,
        label=f"{bk}-BBNData",
        validated=1,
    )


class TestChamPBHSchema(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="champbh-compare-stores-schema-test-"))

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def build(self, name, sm_shard, offset, bbn_reason, bk):
        import config.sharding as sharding

        directory = self.dir / name
        directory.mkdir()
        primary = directory / "store.db"
        shards = [directory / f"store-shard{i:04d}.db" for i in range(2)]
        _write_primary(
            primary,
            [str(p) for p in shards],
            sharding.replicated_tables,
            sharding.sharded_tables,
        )
        registry = _registry()
        for i, path in enumerate(shards):
            _create_shard(path, registry)
            conn = sqlite3.connect(path)
            with conn:
                _fill_champbh_shard(conn, i == sm_shard, offset, bbn_reason, bk)
            conn.close()
        return primary

    def test_14_a_store_with_champbhs_schema(self):
        """(14) On shards with exactly the tables the factories register: the polymorphic map
        resolves ScalarHistory's two references (types 3 and 1) with no problem;
        ExponentialModel and the *_tags tables are sharded (until jordan-normalization prompt
        07, three references, and ExponentialCoupling sharded); LambdaCDM is not noted, since
        config.sharding no longer lists it (dk-and-rpk-migration P8); a copy under
        other serials, another shard, other timings and labels, and another elapsed time in a
        wall-clock failure is equal; and one ulp in one ScalarHistoryValue column is a near
        miss."""
        reason = (
            "PRyMordial: PRyMWallClockLimitError: wall-clock limit of 600 s exceeded in stage "
            "'low-T nuclear network (small)': {} s elapsed"
        )
        a = self.build("a", 1, 0, reason.format("601.25"), "a")
        b = self.build("b", 0, 900, reason.format("640.5"), "b")
        result = cs.compare_stores(a, b)
        self.assertEqual(result["problems"], [], cs.format_result(result))
        self.assertTrue(result["equal"], cs.format_result(result))
        tables = result["tables"]
        self.assertEqual(tables["ExponentialModel"]["kind_a"], "sharded")
        for tags in ("ScalarHistory_tags", "AdiabaticHistory_tags", "BBNData_tags"):
            self.assertEqual(tables[tags]["kind_a"], "sharded")
        self.assertEqual(tables["QCD_Cosmology"]["kind_a"], "replicated")
        self.assertEqual(tables["ScalarHistory"]["matched"], 1)
        self.assertEqual(tables["ScalarHistoryValue"]["matched"], 2)
        self.assertEqual(tables["ScalarHistory_tags"]["matched"], 1)
        self.assertNotIn("LambdaCDM", tables)
        self.assertFalse(any("LambdaCDM" in note for note in result["notes"]))
        self.assertEqual(
            result["bookkeeping_differences"]["ScalarHistory"],
            {"compute_time": 1, "label": 1},
        )
        self.assertEqual(
            result["bookkeeping_differences"]["BBNData"],
            {"failure_reason": 1, "label": 1},
        )

        # every ScalarHistory reference is resolved to its row's labels
        conn = sqlite3.connect(self.dir / "b" / "store-shard0000.db")
        with conn:
            (value,) = conn.execute(
                "select Sigma from ScalarHistoryValue where serial = 941"
            ).fetchone()
            conn.execute(
                "update ScalarHistoryValue set Sigma = ? where serial = 941",
                (math.nextafter(value, 0.0),),
            )
        conn.close()
        result = cs.compare_stores(a, b)
        self.assertFalse(result["equal"])
        smv = result["tables"]["ScalarHistoryValue"]
        (pair,) = smv["near_misses"]
        self.assertEqual([d["column"] for d in pair["differing"]], ["Sigma"])
        model = pair["a"]["history_serial"]["row"]
        self.assertEqual(model["cosmology_serial"]["table"], "QCD_Cosmology")
        self.assertEqual(model["model_serial"]["table"], "ExponentialModel")
        self.assertEqual(
            model["model_serial"]["row"]["beta_serial"]["row"], {"value": 1.15}
        )
        self.assertNotIn("compute_time", model)
        self.assertEqual(model["compute_steps"], 1234)


# ---------------------------------------------------------------------------
# stage_counts.py


def _stage_counts():
    path = (
        ROOT / ".documents" / "dk-and-rpk-migration" / "rehearsal" / "stage_counts.py"
    )
    spec = importlib.util.spec_from_file_location("stage_counts", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# A log in the shape of main.py's output (written for this test, after the lines main.py,
# RayWorkPool and build.sh print; not copied from any run): a resumed run in which the first
# stage reads everything from the store, and the second is cut off by a signal.
FIXTURE_RESUMED = """\
2026-10-12 10:00:00,000\tINFO worker.py:1821 -- Connecting to existing Ray cluster at address: 127.0.0.1:6379...
>> packages: datastorekit not installed; raypoolkit not installed
>> Opened existing sharded datastore "/tmp/x/02-resumed.db" with 16 shards

** BUILDING GRID OF MODELS TO SAMPLE
   -- populated beta sample grid with 3 values = [ 1.1, 2, 30 ]
!! warning: beta=30, phi*=5 M_P: Omega(phi*) T* = 1.2e+50 M_P (super-Planckian start)
\x1b[36m(Datastore pid=123)\x1b[0m @@ Parametrized equation-of-state LambdaCDM-like model

>> RUNNING PIPELINE FOR MODEL QCD_Cosmology

** CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID
   -- ALL WORK ITEMS COMPLETE in time 12.5s
      Queue summary: 3 lookup (avg 864/hr), 0 compute, 0 store, 0 validation
      (no compute items were evaluated - all assets read from datastore)

** CALCULATE ADIABATIC TRANSGRESSION PARAMETERS
   -- 2026-10-12 10:05:00 (5m 0s running): 1/3 work items remaining = 66.67% complete
      inflight: 0 lookup, 1 compute | completed: 3 lookup (avg 36/hr), 2 compute (avg 24/hr)
** driver exit status 143
"""

FIXTURE_COMPLETE = """\
** CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID
   -- ALL WORK ITEMS COMPLETE in time 3m 2s
      Queue summary: 3 lookup (avg 59.3/hr), 3 compute (avg 59.3/hr), 3 store (avg 59.3/hr), 3 validation (avg 59.3/hr)

** CALCULATE ADIABATIC TRANSGRESSION PARAMETERS
   -- ALL WORK ITEMS COMPLETE in time 1m 1s
      Queue summary: 3 lookup (avg 177/hr), 2 compute (avg 118/hr), 2 store (avg 118/hr), 2 validation (avg 118/hr)
-- AdiabaticHistory: 1 models skipped because their ScalarHistory failed

** CALCULATE BBN DATA
   -- ALL WORK ITEMS COMPLETE in time 40s
      Queue summary: 3 lookup (avg 270/hr), 1 compute (avg 90/hr), 1 store (avg 90/hr), 1 validation (avg 90/hr)
-- BBNData: 1 models skipped because their ScalarHistory failed; 1 computations skipped because of a stored failure
** driver exit status 0
"""


class TestStageCounts(unittest.TestCase):
    def test_stage_counts_parses_a_complete_and_an_interrupted_log(self):
        """stage_counts.py: each CALCULATE banner's completion and queue-summary counts
        without rates, the 'models skipped' lines, and build.sh's exit status; its printed
        output carries no time."""
        sc = _stage_counts()

        complete = sc.parse_log(FIXTURE_COMPLETE)
        self.assertEqual(complete["exit_status"], 0)
        self.assertEqual(
            [s["stage"] for s in complete["stages"]],
            [
                "CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID",
                "CALCULATE ADIABATIC TRANSGRESSION PARAMETERS",
                "CALCULATE BBN DATA",
            ],
        )
        self.assertTrue(all(s["complete"] for s in complete["stages"]))
        self.assertEqual(
            [s["counts"] for s in complete["stages"]],
            [
                {"lookup": 3, "compute": 3, "store": 3, "validation": 3},
                {"lookup": 3, "compute": 2, "store": 2, "validation": 2},
                {"lookup": 3, "compute": 1, "store": 1, "validation": 1},
            ],
        )
        self.assertEqual(complete["stages"][0]["skipped"], [])
        (adiabatic,) = complete["stages"][1]["skipped"]
        self.assertEqual(
            (adiabatic["table"], adiabatic["models_skipped"]), ("AdiabaticHistory", 1)
        )
        (bbn,) = complete["stages"][2]["skipped"]
        self.assertEqual(
            (bbn["table"], bbn["models_skipped"], bbn["stored_failures_skipped"]),
            ("BBNData", 1, 1),
        )

        resumed = sc.parse_log(FIXTURE_RESUMED)
        self.assertEqual(resumed["exit_status"], 143)
        first, second = resumed["stages"]
        self.assertTrue(first["complete"])
        self.assertTrue(first["no_compute"])
        self.assertEqual(
            first["counts"], {"lookup": 3, "compute": 0, "store": 0, "validation": 0}
        )
        self.assertFalse(second["complete"])
        self.assertIsNone(second["counts"])

        text = sc.format_counts("resumed.log", resumed)
        self.assertNotIn("/hr", text)
        self.assertNotIn("12.5s", text)
        self.assertIn("CALCULATE ADIABATIC TRANSGRESSION PARAMETERS: INCOMPLETE", text)
        self.assertIn("driver exit status: 143", text)


if __name__ == "__main__":
    unittest.main()
