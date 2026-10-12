# Log 08 — adopt `datastorekit`

**Subject:** Adopt datastorekit and delete ChamPBH's datastore layer
**Commit:** `857ec73`, on branch `dk-and-rpk-migration`, on top of `63f60e1` (code as at `617a1dc`)
**Date:** 2026-10-12, by Claude Opus 5.5
**Result:** **COMPLETE.** No §4 stop condition was met.
- A1: the 7 layer modules `git rm`'d; `Datastore/__init__.py` and `Datastore/SQL/__init__.py` are
  0-byte files; the factories stay in `Datastore/SQL/ObjectFactories/`.
- A2–A3: 40 import lines rewritten by the module map (13 `DatastoreObject`, 17 `SQLAFactoryBase`,
  4 `ShardedPool`, 3 `ProfileAgent`, 3 `require_version_serial`); `config/version.py` keeps only
  `VERSION_LABEL`. Each factory's diff is its import line(s), nothing else.
- A4–A5: the three pools take `factories=` and `serial_batch_sizes=` from `config.datastore`,
  `main.py`'s `drop_tables=`; `LambdaCDM` is out of `replicated_tables` and `serial_batch_sizes`;
  `config.datastore.tables_to_drop` refuses `--drop scalar-model` before Ray with the pinned message.
- A6: `Datastore/tests/standin_pool.py` (ChamPBH's copy of the tag's, two functions changed);
  `_TempStoreCase` rebuilt on the package's actor; the five importers unchanged; test a's one line.
- A7–A8: 4 + 2 + 3 tests deleted, test 14's one assertion inverted, `_registry()`'s fallback gone;
  12 new tests. Suites 18 / 114 (61 + 53) / 76 (73 − 9 + 12), all OK.
- A9: `--inventory` refused before Ray (exit 2); the refresh route names `sharded_store copy`;
  `config/datastore.py`, `config/version.py`, `CLAUDE.md`, the black-clean issue made true.
- DatastoreKit's script at a dangling commit of the staged tree: outside `prompts/`, 0 layer
  imports, 0 literals, 0 `import_module`; 0 imports of the moved names; `prompts/`'s one kept.
- The stand-in store's schema agrees with R0's snapshot with **no difference at all**.
- Six deliberate breakages caught. One §3 issue closed
  (`[05-config-datastore-cannot-be-imported-before-the-layer]`), one opened
  (`[08-plot-scripts-open-read-write-pools]`); `[00-two-files-are-not-black-clean]` narrowed.

## What shipped

`git diff --cached --name-only`, as committed:

```text
.documents/OPEN_ISSUES.md
.documents/dk-and-rpk-migration/out/08-standin-schema.json
CLAUDE.md
ComputeTargets/AdiabaticHistory.py
ComputeTargets/BBNData.py
ComputeTargets/ScalarModel.py
CosmologyConcepts/ConformalCouplings/AbstractCoupling.py
CosmologyConcepts/DimensionfulQuantity.py
CosmologyConcepts/DimensionlessQuantity.py
CosmologyConcepts/Potentials/AbstractPotential.py
CosmologyConcepts/redshift.py
CosmologyModels/base.py
Datastore/SQL/ClientPool.py
Datastore/SQL/Datastore.py
Datastore/SQL/ObjectFactories/AdiabaticHistory.py
Datastore/SQL/ObjectFactories/BBNData.py
Datastore/SQL/ObjectFactories/DimensionfulQuantity.py
Datastore/SQL/ObjectFactories/DimensionlessQuantity.py
Datastore/SQL/ObjectFactories/ExponentialCoupling.py
Datastore/SQL/ObjectFactories/ExponentialPotential.py
Datastore/SQL/ObjectFactories/InversePowerPotential.py
Datastore/SQL/ObjectFactories/QCD_Cosmology.py
Datastore/SQL/ObjectFactories/ReclinerPotential.py
Datastore/SQL/ObjectFactories/ReflectingPotential.py
Datastore/SQL/ObjectFactories/ScalarModel.py
Datastore/SQL/ObjectFactories/Starobinsky.py
Datastore/SQL/ObjectFactories/base.py
Datastore/SQL/ObjectFactories/integration_metadata.py
Datastore/SQL/ObjectFactories/redshift.py
Datastore/SQL/ObjectFactories/store_tag.py
Datastore/SQL/ObjectFactories/tolerance.py
Datastore/SQL/ObjectFactories/version.py
Datastore/SQL/ProfileAgent.py
Datastore/SQL/SerialPoolBroker.py
Datastore/SQL/ShardedPool.py
Datastore/SQL/__init__.py
Datastore/__init__.py
Datastore/object.py
Datastore/tests/standin_pool.py
Datastore/tests/test_compare_stores.py
Datastore/tests/test_datastore_config.py
Datastore/tests/test_shard_key_assignment.py
Datastore/tests/test_version_keyed_lookups.py
MetadataConcepts/store_tag.py
MetadataConcepts/tolerance.py
MetadataConcepts/version.py
Quadrature/integration_metadata.py
config/datastore.py
config/model_list.py
config/sharding.py
config/version.py
main.py
pipeline_selection.py
plot_ScalarModel.py
plot_by_beta.py
prompts/dk-and-rpk-migration/IMPLEMENTATION_STATE.md
prompts/dk-and-rpk-migration/logs/08-adopt-datastorekit.md
prompts/review-remediation/IMPLEMENTATION_STATE.md
```

**A1, the files.** `git rm` of `Datastore/object.py`,
`Datastore/SQL/{ClientPool,Datastore,ProfileAgent,SerialPoolBroker,ShardedPool}.py` and
`Datastore/SQL/ObjectFactories/base.py`. `Datastore/__init__.py` (was `from .object import
DatastoreObject`) and `Datastore/SQL/__init__.py` (was `from .Datastore import Datastore`) are
0-byte files (blob `e69de29`). `Datastore/SQL/ObjectFactories/__init__.py` (0 bytes) and the 17
factory modules stay.

**A2–A3, the imports.** By the adoption README §2 module map, module path only, by `sed` on exact
whole lines:

| Old | New | Files |
|---|---|---|
| `from Datastore import DatastoreObject` | `from datastorekit import DatastoreObject` | 13: `ComputeTargets/{AdiabaticHistory,BBNData,ScalarModel}.py`, `CosmologyConcepts/{ConformalCouplings/AbstractCoupling,DimensionfulQuantity,DimensionlessQuantity,Potentials/AbstractPotential,redshift}.py`, `CosmologyModels/base.py`, `MetadataConcepts/{store_tag,tolerance,version}.py`, `Quadrature/integration_metadata.py` |
| `from Datastore.SQL.ObjectFactories.base import SQLAFactoryBase` | `from datastorekit.SQL.factory_base import SQLAFactoryBase` | the 17 factory modules |
| `from Datastore.SQL.ShardedPool import ShardedPool` | `from datastorekit.SQL.ShardedPool import ShardedPool` | `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py`, `config/model_list.py` (the type hint) |
| `from Datastore.SQL.ProfileAgent import ProfileAgent` | `from datastorekit.SQL.ProfileAgent import ProfileAgent` | the three drivers |
| `from config.version import require_version_serial` | `from datastorekit.contract import require_version_serial` | `Datastore/SQL/ObjectFactories/{AdiabaticHistory,BBNData,ScalarModel}.py` (P9) |

`git diff -U0 Datastore/SQL/ObjectFactories/`, every changed line, counted (E2):

```text
      3 -from config.version import require_version_serial
     17 -from Datastore.SQL.ObjectFactories.base import SQLAFactoryBase
      3 +from datastorekit.contract import require_version_serial
     17 +from datastorekit.SQL.factory_base import SQLAFactoryBase
```

(`AdiabaticHistory.py`, `BBNData.py`, `ScalarModel.py` 2+/2−; the other 14 1+/1−.) The test module's
`ShardedPool` import went with the module (A7.1), and the harness's actor import with the harness
(A6). `config/version.py` loses `VERSION_SERIAL_KEY`, `require_version_serial` and their comment
(old `:51-71`) and gains a five-line comment saying that both are now `datastorekit.contract`'s and
that the package's `VERSION_LABEL` is a column name, not this label (D8). `VERSION_LABEL` stays at
`:50`; test f still passes.

**A4, the pools.** Each `ShardedPool(` call loses `inventory_config=inventory_config` and gains
`factories=factories, serial_batch_sizes=serial_batch_sizes`; `main.py`'s `drop_actions=drop_actions`
becomes `drop_tables=drop_tables`. Each driver imports `from config.datastore import factories,
serial_batch_sizes` (`main.py` also `tables_to_drop`) and stops importing `inventory_config`. Every
other argument is unchanged. `"LambdaCDM"` leaves `config/sharding.py`'s `replicated_tables` (old
`:34`) and `config/datastore.py`'s `serial_batch_sizes` (P8). `config/sharding.py`'s
`inventory_config` stays (09's).

**A5, U4.** `config/datastore.py` gains `tables_to_drop(groups)` beside `drop_groups` (with two
private helpers, `_names_of` and `_joined`), and imports `build_schema`, `dependent_tables` and
`inventory_specs` from the package:

```python
def _names_of(tables: List[str], among: List[str]) -> List[str]:
    """
    Which of ``among`` name a table of ``tables`` directly, by the rule ``dependent_tables`` follows
    transitively (a foreign key, or a parent a factory's ``inventory_spec`` declares), in registry
    order. Used only to word ``tables_to_drop``'s refusal; which tables must go is
    ``dependent_tables``' answer alone.
    """
    built = build_schema(sqla.MetaData(), factories).tables
    named = {
        name: {fk.column.table.name for fk in table.foreign_keys}
        for name, table in built.items()
    }
    for name, spec in inventory_specs(factories).items():
        named.setdefault(name, set()).update(spec.dependencies())
    return [
        name
        for name in built
        if name in among and not named.get(name, set()).isdisjoint(tables)
    ]


def _joined(items: List[str]) -> str:
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " and " + items[-1]


def tables_to_drop(groups: Iterable[str]) -> List[str]:
    """
    The tables of the ``--drop`` groups named by ``groups``, each once, in ``drop_groups``' order.

    A name that is not a drop group raises ``ValueError`` naming it and the groups there are. A
    request that would leave rows naming rows it drops raises ``ValueError``: the tables that must
    go with it are ``datastorekit.SQL.schema.dependent_tables``' answer, from the registry, and the
    message names the requested groups, the tables that name their rows, and the ``--drop`` groups
    to add (a table that must go but is in no group is named as a table). So
    ``tables_to_drop(["scalar-model"])`` raises, since ``AdiabaticHistory`` and ``BBNData`` name
    ``ScalarModel`` rows; ``main.py`` calls this before Ray starts (dk-and-rpk-migration prompt 08,
    campaign README U4).
    """
    requested = list(dict.fromkeys(groups))
    unknown = [group for group in requested if group not in drop_groups]
    if len(unknown) > 0:
        raise ValueError(
            f"unknown drop group(s) {unknown}: the drop groups are {list(drop_groups)}"
        )

    tables = [
        table
        for group, group_tables in drop_groups.items()
        if group in requested
        for table in group_tables
    ]
    dependents = [
        table for table in dependent_tables(tables, factories) if table not in tables
    ]
    if len(dependents) == 0:
        return tables

    group_of = {
        table: group
        for group, group_tables in drop_groups.items()
        for table in group_tables
    }
    asked = [group for group in drop_groups if group in requested]
    to_add = [
        group
        for group in drop_groups
        if group not in requested and any(group_of.get(t) == group for t in dependents)
    ]
    loose = [table for table in dependents if table not in group_of]
    referrers = _names_of(tables, dependents)

    additions = []
    if len(to_add) > 0:
        additions.append(f"`--drop {' '.join(to_add)}`")
    if len(loose) > 0:
        additions.append(
            f"the table{'s' if len(loose) > 1 else ''} {_joined(loose)}, which no drop group holds"
        )
    raise ValueError(
        f"cannot drop {_joined(asked)} alone: {_joined(referrers)} "
        f"name{'s' if len(referrers) == 1 else ''} {'its' if len(asked) == 1 else 'their'} rows; "
        f"add {' and '.join(additions)}"
    )
```

Its outputs (measured by a one-liner before the tests were written; `ray.is_initialized()` `False`):

```text
['scalar-model'] !! cannot drop scalar-model alone: AdiabaticHistory and BBNData name its rows; add `--drop adiabatic-history bbn-data`
['scalar-model', 'adiabatic-history'] !! cannot drop scalar-model and adiabatic-history alone: BBNData names their rows; add `--drop bbn-data`
['scalar-model', 'bbn-data'] !! cannot drop scalar-model and bbn-data alone: AdiabaticHistory names their rows; add `--drop adiabatic-history`
['bbn-data'] -> ['BBNData_tags', 'BBNDataValue', 'BBNData']
['adiabatic-history'] -> ['AdiabaticHistory_tags', 'AdiabaticHistoryValue', 'AdiabaticHistory']
['adiabatic-history', 'bbn-data'] -> ['AdiabaticHistory_tags', 'AdiabaticHistoryValue', 'AdiabaticHistory', 'BBNData_tags', 'BBNDataValue', 'BBNData']
['scalar-model', 'adiabatic-history', 'bbn-data'] -> ['ScalarModel_tags', 'ScalarModelValue', 'ScalarModel', 'AdiabaticHistory_tags', 'AdiabaticHistoryValue', 'AdiabaticHistory', 'BBNData_tags', 'BBNDataValue', 'BBNData']
['bbn-data', 'scalar-model', 'adiabatic-history'] -> (the same nine, in drop_groups' order)
[] -> []
['nope'] !! unknown drop group(s) ['nope']: the drop groups are ['scalar-model', 'adiabatic-history', 'bbn-data']
```

**The U4 message** for `["scalar-model"]` is exactly
``cannot drop scalar-model alone: AdiabaticHistory and BBNData name its rows; add `--drop adiabatic-history bbn-data` ``,
the prompt's wording. Which tables must go is `dependent_tables`' answer alone (the closure: the
three `AdiabaticHistory` and three `BBNData` tables); the message names the tables of that closure
that name a requested table **directly** (`_names_of`: one step of the same rule, a foreign key or a
declared `inventory_spec` parent), which is why it names `AdiabaticHistory` and `BBNData` and not
their value and tag tables (D1). A table that must go but is in no group is named as a table (the
test `test_a_dependent_in_no_group_is_named_as_a_table` reaches it with a registry of its own;
ChamPBH's registry does not).

`main.py`'s new lines before `ray.init` (`:91-112`); the old `:97-98` moved above `ray.init`:

```python
# --inventory read the old layer's pool.inventory(), which datastorekit's pool does not have; it is
# refused until prompt 09 ports it, before Ray starts (dk-and-rpk-migration prompt 08; README P12)
if args.inventory:
    print(
        "!! --inventory: the datastore inventory is unavailable until dk-and-rpk-migration "
        "prompt 09 ports it to datastorekit; nothing was opened",
        file=sys.stderr,
    )
    sys.exit(2)

# the --drop groups become the tables the pool drops; a drop that would leave rows naming dropped
# rows is refused here, before Ray starts (dk-and-rpk-migration prompt 08; README U4)
specified_drop_actions = [x.lower() for x in args.drop]
drop_actions = [x for x in specified_drop_actions if x in allowed_drop_actions]
try:
    drop_tables = tables_to_drop(drop_actions)
except ValueError as e:
    print(f"!! --drop: {e}", file=sys.stderr)
    sys.exit(2)

# connect to ray cluster on supplied address; defaults to 'auto' meaning a locally running cluster
ray.init(address=args.ray_address)
```

The package's own refusal at open (`ShardedPool._refuse_drop_that_leaves_references`,
`datastorekit/SQL/ShardedPool.py:825-843` at the tag) stays behind it, unreached from the command
line.

**A6, the harness.**
1. **`Datastore/tests/standin_pool.py`**, ChamPBH's copy of `datastorekit/tests/standin_pool.py` at
   `v0.3.0` (`2f5417ea…`, 485 lines; the copy is 529). `diff -u <tag's file> Datastore/tests/standin_pool.py`:

```diff
--- datastorekit/tests/standin_pool.py (DatastoreKit v0.3.0)
+++ Datastore/tests/standin_pool.py
@@ -1,3 +1,34 @@
+# (c) University of Sussex 2026
+# Created by David Seery
+#
+# Licensed under the Apache License, Version 2.0 (the "License");
+# you may not use this file except in compliance with the License.
+# You may obtain a copy of the License at
+#
+#    http://www.apache.org/licenses/LICENSE-2.0
+#
+# Unless required by applicable law or agreed to in writing, software
+# distributed under the License is distributed on an "AS IS" BASIS,
+# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
+# See the License for the specific language governing permissions and
+# limitations under the License.
+
+# ChamPBH's copy of DatastoreKit's stand-in pool (dk-and-rpk-migration prompt 08).
+#
+# Copied from datastorekit/tests/standin_pool.py at DatastoreKit's tag v0.3.0, commit
+# 2f5417ea76cdcd0d15d64d80d9085429bfacad21 (https://github.com/ds283/DatastoreKit), which is
+# licensed under the Apache License 2.0, as ChamPBH is. ChamPBH keeps a copy because the package's
+# wheel does not ship its tests (DatastoreKit's docs/adoption/README.md §3: a client whose tests
+# use the stand-in pool keeps its own copy). ChamPBH's test harness builds the package's actor
+# through it (Datastore/tests/test_version_keyed_lookups.py, _TempStoreCase).
+#
+# Changed from the tag's file only where it must be: open_pool and open_pool_output imported the
+# package's neutral test client (datastorekit.tests.client.registry), which is not installed; here
+# they open a pool on ChamPBH's registry (config.datastore.factories) and tables (config.sharding:
+# replicated_tables, sharded_tables, ShardKeyType, get_shard_key_store_id), under a version_label
+# argument. Everything else is the tag's file unchanged. On a DatastoreKit upgrade, copy the new
+# tag's file and re-apply these two changes.
+
 """
 A real ``ShardedPool`` on stand-in shards, with no Ray.
 
@@ -349,24 +380,31 @@
     def clear_faults(self):
         self.faults.clear()
 
-    def open_pool(self, primary: Path, shards: int = 3, **kwargs):
-        """Open (or create) a pool on ``primary`` with the neutral client's table lists."""
-        from datastorekit.tests.client.registry import (
-            factories,
+    def open_pool(
+        self,
+        primary: Path,
+        shards: int = 3,
+        version_label: str = "standin",
+        **kwargs,
+    ):
+        """Open (or create) a pool on ``primary`` under ``version_label``, with ChamPBH's registry
+        and table lists."""
+        from config.datastore import factories
+        from config.sharding import (
             replicated_tables,
             sharded_tables,
-            shard_key_type,
-            shard_key_store_id,
+            ShardKeyType,
+            get_shard_key_store_id,
         )
 
         # the registry is the client's; a test may give a different one
         kwargs.setdefault("factories", factories)
         with contextlib.redirect_stdout(io.StringIO()):
             pool = sp_mod.ShardedPool(
-                version_label="standin",
+                version_label=version_label,
                 db_name=primary,
-                ShardKeyType=shard_key_type,
-                ShardKeyStoreIdGetter=shard_key_store_id,
+                ShardKeyType=ShardKeyType,
+                ShardKeyStoreIdGetter=get_shard_key_store_id,
                 replicated_tables=replicated_tables,
                 sharded_tables=sharded_tables,
                 shards=shards,
@@ -375,18 +413,24 @@
         self.pool = pool
         return pool
 
-    def open_pool_output(self, primary: Path, shards: int = 3, **kwargs):
+    def open_pool_output(
+        self,
+        primary: Path,
+        shards: int = 3,
+        version_label: str = "standin",
+        **kwargs,
+    ):
         """
         As ``open_pool``, but return ``(pool, printed)``: what the constructor printed, which
         ``open_pool`` discards (the check at open prints what it repaired). If the constructor
         raises, what it printed before raising is kept as ``self.last_output``.
         """
-        from datastorekit.tests.client.registry import (
-            factories,
+        from config.datastore import factories
+        from config.sharding import (
             replicated_tables,
             sharded_tables,
-            shard_key_type,
-            shard_key_store_id,
+            ShardKeyType,
+            get_shard_key_store_id,
         )
 
         # the registry is the client's; a test may give a different one
@@ -396,10 +440,10 @@
         try:
             with contextlib.redirect_stdout(out):
                 pool = sp_mod.ShardedPool(
-                    version_label="standin",
+                    version_label=version_label,
                     db_name=primary,
-                    ShardKeyType=shard_key_type,
-                    ShardKeyStoreIdGetter=shard_key_store_id,
+                    ShardKeyType=ShardKeyType,
+                    ShardKeyStoreIdGetter=get_shard_key_store_id,
                     replicated_tables=replicated_tables,
                     sharded_tables=sharded_tables,
                     shards=shards,
```

   Every hunk:
   - **`@@ -1,3 +1,34 @@`** — ChamPBH's full 14-line Apache licence header, copied from
     `utilities.py:1-14` (ending `# limitations under the License.`), then the provenance comment:
     the tag, commit and URL; that DatastoreKit is Apache 2.0 as ChamPBH is; why a copy is kept
     (adoption README §3: the wheel does not ship `datastorekit/tests/`); what changed; how to
     re-copy on an upgrade. The comment sits before the docstring, which stays the module's
     docstring, byte for byte the tag's.
   - **`@@ -349,24 +380,31 @@`** (`open_pool`) — the import of the neutral client's registry
     (`datastorekit.tests.client.registry`, not installed) is replaced by ChamPBH's
     (`config.datastore.factories`; `config.sharding`'s `replicated_tables`, `sharded_tables`,
     `ShardKeyType`, `get_shard_key_store_id`); the hard-coded `version_label="standin"` becomes a
     `version_label` argument, defaulting to `"standin"` so a call written for the tag's signature
     opens the same label; the two shard-key arguments take ChamPBH's names. `black` puts the
     longer signature on six lines. The one-line docstring says which registry it opens.
   - **`@@ -375,18 +413,24 @@`** and **`@@ -396,10 +440,10 @@`** (`open_pool_output`) — the same
     three changes; its docstring is unchanged ("As `open_pool`, …").
   Nothing else differs: the stand-in cluster, handles, `ray.get`/`ray.kill`, faults, hooks and the
   readers are the tag's. The module has no `test_` prefix, so discovery does not run it (the
   Datastore count by module, below, has no `standin_pool` entry).
2. **`_TempStoreCase`** (`Datastore/tests/test_version_keyed_lookups.py:196-264`), in the manner of
   the package's `TestTheActor` (`datastorekit/tests/test_version_keyed_lookups.py:309-341`):

```python
class _TempStoreCase(unittest.TestCase):
    """
    The package's actor on a one-shard store in a temporary directory, inside a stand-in cluster
    (the module docstring). Each test's actors share one stand-in serial broker; every actor is
    closed in tearDown, which also asserts that Ray was never initialised.
    """

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="champbh-datastore-test-")
        self.addCleanup(tmp.cleanup)
        self._dir = Path(tmp.name)
        self.db = self._dir / "store.db"

        self.cluster = sp.StandinCluster()
        active = self.cluster.active()
        active.__enter__()
        self.addCleanup(active.__exit__, None, None, None)

        # one broker for every actor of the test, as a pool gives its actors one
        self.broker = sp.Handle(
            sp.BrokerClass(name="SerialPoolBroker"),
            "SerialPoolBroker",
            self.cluster,
            None,
        )
        self._stores = []

    def tearDown(self):
        for store in self._stores:
            with contextlib.redirect_stdout(io.StringIO()):
                store.__exit__(None, None, None)
        self._stores = []
        self.assertFalse(ray.is_initialized())

    def open(self, label):
        """
        The package's actor on the store's one shard, under ``label``, with the label's version
        serial set, as ``ShardedPool`` builds and prepares each of its actors.
        """
        # a real pool on one stand-in shard finds or writes the label's version row
        pool = self.cluster.open_pool(
            self.db,
            shards=1,
            version_label=label,
            serial_batch_sizes=serial_batch_sizes,
        )
        serial = pool._version.store_id
        shard_file = pool._shard_db_files[0]
        self.cluster.close_pool(pool)

        with contextlib.redirect_stdout(io.StringIO()):
            store = sp.DatastoreClass(
                version_label=label,
                db_name=shard_file,
                replicated_tables=list(replicated_tables),
                factories=factories,
                serial_batch_sizes=serial_batch_sizes,
                my_name=f"probe{len(self._stores):04d}-store",
                serial_broker=self.broker,
            )
        self._stores.append(store)

        # the broker learns the file's largest serials before the actor inserts anything, and the
        # actor is given the label's serial, as ShardedPool does at open
        self.broker.notify_largest_store_ids.remote(
            store.read_largest_store_ids()
        ).get()
        store.set_version(serial)
        return store
```

   **Why this shape.** The package's actor inserts no version row, refuses a versioned insert
   before `set_version` (`datastorekit/SQL/Datastore.py:778-786`) and a keyed lookup before the
   lookup serial is set (`:608-614`), and leases serials from a broker. So `open` lets a real pool
   write the label's row, exactly as production does (a replicated write, on one stand-in shard),
   then builds the actor as the pool builds each of its own (`ShardedPool.py:332-348`: the
   undecorated class, `replicated_tables`, `factories=`, `serial_batch_sizes=`, a name and the
   broker), notifies the broker of the file's largest serials (`:351-359`), and calls `set_version`
   (`:383-388`). One shard is enough, so the actor's file is the pool's only shard
   (`store-shard0000.db`; the primary is `store.db`). Reopening the same primary under a second
   label, while the first label's actor is still open, makes the pool find the first row and write
   the second's: test a's two serials are 1 and 2. The broker is one stand-in `SerialPoolBroker` per
   test, shared by all of that test's actors, as a pool's actors share one. Every insert in today's
   tests passes an explicit serial, so no test leases one; a probe (not a test, kept in the
   scratchpad) showed the wiring: after actor A inserts `tolerance` serial 14, actor B (opened
   under a second label) inserts two `tolerance` rows with no serial and is leased 15 and 16.
   `tearDown` closes every actor (`__exit__`, which disposes the engine and releases leases through
   the broker while the stand-in `ray.get` is still patched) and asserts `ray.is_initialized()` is
   `False`; the cluster's patches and the temporary directory are undone by `addCleanup`, after it.
   The helpers that reach into the actor (`_insert`'s `_engine` and `_schema[cls]["insert"]`, test
   d's `_factories`, `_tables`, `_inserters`, test d3's `_schema`) work unchanged: the package's
   actor has each, and its inserter takes `(conn, payload)`.
3. **The importers** (`test_bbn_failure_lookup.py`, `test_first_bounce_round_trip.py`,
   `test_fixed_T_values_round_trip.py`, `test_scalarmodel_failure_reason.py`,
   `ComputeTargets/tests/test_foreign_bbn_provenance.py`) are **not changed at all**: every name
   they import (`LABEL_A`, `_TempStoreCase`, `_insert`, the stand-ins) is still there, and their 26
   tests pass on the rebuilt harness.
4. **Test a's one changed line** (`:279`):

```diff
-        self.assertNotEqual(store_a._version.store_id, store_b._version.store_id)
+        self.assertNotEqual(store_a._version_serial, store_b._version_serial)
```

   The harness module's docstring is rewritten to describe it (it said "builds the undecorated
   `Datastore.__ray_actor_class__`" and "There is no serial broker"); `shutil` and the actor import
   go, `contextlib`, `io`, `ray`, `standin_pool`, `config.datastore` and `config.sharding` come in.

**A7, the deletions.**
1. `Datastore/tests/test_shard_key_assignment.py` (4 tests) `git rm`'d.
2. `test_version_keyed_lookups.py`'s **d2** and **d4** deleted (d2 held the module's only import of
   `VERSION_SERIAL_KEY`; d4 the only use of `DS`). a, b, c1–c3, d, d3, e, f stay: 9 tests.
3. `test_datastore_config.py`: the module-level layer imports (old `:32-33`), the import-order
   comment (`:28-31`), the `import_module` and its comment (`:37-39`) and the now-unused `importlib`
   go; `TestLayerReadsConfig` (3 tests) is deleted; `EXPECTED_SERIAL_BATCH_SIZES` loses
   `"LambdaCDM": 5`. The three data tests and `TestDriversPassShardKeyMapping` are unchanged. The
   module docstring said "importing the layer builds the factories"; it now says
   `config.datastore` does, and names the two subprocess tests.
4. `test_compare_stores.py`: `_registry()` imports `config.datastore` directly, with no fallback; its
   docstring says so in one line. Test 14 (A7.4 as amended): `:1109`
   `assertTrue(any("LambdaCDM" in note …))` → `assertFalse(…)`, and its docstring's "LambdaCDM is
   noted" → "LambdaCDM is not noted, since config.sharding no longer lists it
   (dk-and-rpk-migration P8)"; `assertNotIn("LambdaCDM", tables)` (`:1108`) stays. Nothing else in
   the module changed.

**A8, the new tests** (all in `Datastore/tests/test_datastore_config.py`; 12 tests, each class's
`tearDown` asserts `ray.is_initialized()` is `False`):

| Item | Test | What it asserts |
|---|---|---|
| A8.1 | `TestTablesAreDeclared.test_every_listed_table_is_registered_and_LambdaCDM_is_in_none` | every `replicated_tables`/`sharded_tables` name is a `factories` key; `LambdaCDM` in none of the three lists (nor the registry) |
| A8.2 | `TestPoolsAreGivenTheConfig.test_each_pool_is_given_config_datastore` | by `ast`: exactly one `ShardedPool(` per driver (3 in all, asserted); `factories=` and `serial_batch_sizes=` bare names, bound only by `from config.datastore import` under their own names; `main.py`'s passes `drop_tables=`, the plot scripts' do not; none passes `drop_actions=` or `inventory_config=` |
| A8.3 | `TestDropPolicy.test_scalar_model_alone_is_refused_naming_the_groups_to_add` | the pinned message, exactly |
| A8.3 | `TestDropPolicy.test_scalar_model_with_one_dependent_group_is_refused` | the two mixed refusals, exactly (D2) |
| A8.3 | `TestDropPolicy.test_each_closed_request_is_accepted` | `[]`, `bbn-data`, `adiabatic-history`, both, all three (given out of order) return their tables in `drop_groups`' order |
| A8.3 | `TestDropPolicy.test_an_unknown_group_is_refused` | `ValueError` naming it (D2) |
| A8.3 | `TestDropPolicy.test_a_dependent_in_no_group_is_named_as_a_table` | with a test registry adding `Orphan` → `ScalarModel.serial`: the message names the table (D2) |
| A8.3 | `TestDropPolicy.test_the_command_line_choices_are_the_groups` | `allowed_drop_actions == list(drop_groups)` (§2.2) |
| A8.3 | `TestDropPolicy.test_main_computes_the_drop_before_ray_starts` | by `ast`: the one module-level statement calling `tables_to_drop` precedes the one calling `ray.init` |
| A8.4 | `TestConfigDatastoreImportsAlone.test_import_config_datastore_alone` | `python -c "import config.datastore"` from the root exits 0 |
| A8.5 | `TestInventoryRefuses.test_inventory_exits_non_zero_and_creates_nothing` | `main.py --database <tmp>/absent.db --ray-address 127.0.0.1:9 --inventory` exits 2, stderr has `!! --inventory:` and `prompt 09`, nothing is created in the temporary directory (D10) |
| A8.5 | `TestInventoryRefuses.test_the_refusal_comes_before_ray_starts` | by `ast`: the module-level `if args.inventory:` with a `sys.exit` precedes `ray.init` |

**A9, the rest.**
1. `--inventory`: `main.py` prints `!! --inventory: the datastore inventory is unavailable until
   dk-and-rpk-migration prompt 09 ports it to datastorekit; nothing was opened` to stderr and exits 2,
   before `ray.init`. The block old `main.py:1196-1344` (`redshift_inventory`,
   `dimensionless_value_inventory`, `dimensionful_value_inventory`, `object_inventory`,
   `inventory`; the four `pool.inventory(` calls) is deleted, and the pool's body is
   `execute(pool, units)`. `beta_value`, `M_value` and `Lambda_value`, imported only for that block,
   leave `main.py`'s import (D3). The drivers no longer import `inventory_config`.
2. `pipeline_selection.BBN_REFRESH_ROUTE` is now "to refresh BBN, copy the store with
   `` `python -m datastorekit.tools.sharded_store copy SRC DST` `` and run main.py on the copy with
   --drop bbn-data" (P13; test b2's `--drop bbn-data` assertion still holds).
3. Prose (P14): `config/datastore.py`'s docstring (no aliases, no cycle; `tables_to_drop`
   described; "can be imported first, alone"); `config/version.py` (A2); `CLAUDE.md`'s harness
   sentence (old `:68-70`) now names `datastorekit`'s undecorated actor, `standin_pool.DatastoreClass`,
   inside ChamPBH's stand-in cluster, `_TempStoreCase` as the harness, and "never through Ray";
   `.documents/OPEN_ISSUES.md`'s `[00-two-files-are-not-black-clean]` row and its board entry
   (`prompts/review-remediation/IMPLEMENTATION_STATE.md:213-220`, an added **Narrowed** line) now
   name the one remaining file, `CosmologyModels/LambdaCDM/Planck.py` (`black --check` still
   would reformat it). Not `tools/compare_stores.py`, not `architecture-summary.md` (below).
4. The issues: `[05-config-datastore-cannot-be-imported-before-the-layer]` moved to board §4
   (closed), its index row deleted; `[08-plot-scripts-open-read-write-pools]` opened in board §3
   with its index row; the index's count stays 26 (3 on this board) and its header prose says
   "prompt 08 closed one and opened one, 2026-10-12".

`out/08-standin-schema.json` is the stand-in store's snapshot (§2.3.4). The ignored bytecode of the
deleted modules (`Datastore/__pycache__/object.*.pyc`, `Datastore/SQL/__pycache__/{ClientPool,
Datastore,ProfileAgent,SerialPoolBroker,ShardedPool}.*.pyc`,
`Datastore/SQL/ObjectFactories/__pycache__/base.*.pyc`) was removed; it was inert.

Nothing was changed in DatastoreKit (`04a2ed9`, clean), RayPoolKit, SecondaryGWKit or
StochasticInstantons; nothing in the venv. No Ray was started; no store was opened but temporary
ones in the scratchpad and in `tempfile` directories.

## Deviations from the prompt

- **D1 — IMPLEMENTATION CHOICE (the U4 wording).** The message names the requested groups, then the
  tables that name the requested tables *directly* (`_names_of`, one step of `dependent_tables`'
  rule), then the groups to add, and any table in no group as a table:
  ``cannot drop <groups> alone: <tables> name(s) its/their rows; add `--drop <groups>` [and the
  table(s) <t>, which no drop group holds]``. The set of tables that must go is
  `dependent_tables`' alone; `_names_of` only chooses which of them the sentence names, so that
  "name its rows" is true (the value and tag tables name `AdiabaticHistory`/`BBNData`, not
  `ScalarModel`). An unknown group raises `ValueError` too (argparse's `choices` refuses it first on
  the command line).
- **D2 — IMPLEMENTATION CHOICE.** A8.3 is seven test methods: beyond the prompt's list, the two mixed
  refusals (the orchestrator's table), an unknown group, and the in-no-group branch with a test
  registry. The `allowed_drop_actions` check (§2.2) and the `ast` check are methods of their own.
  A8.5 is two methods (subprocess; `ast`). So N = 12.
- **D3 — IMPLEMENTATION CHOICE.** `main.py`'s `from CosmologyConcepts import (…)` loses `beta_value`,
  `M_value` and `Lambda_value`, used only by the deleted inventory block (A9.1 deletes "the block
  and its helpers"; these imports were its). `check_imports.py`'s count moves accordingly (below).
- **D4 — IMPLEMENTATION CHOICE.** Both refusals sit after the provenance line and immediately before
  `ray.init`, `--inventory` first; both print to stderr with a `!! --<option>: ` prefix and exit 2.
- **D5 — IMPLEMENTATION CHOICE (the stand-in copy).** `version_label` is a keyword argument after
  `shards`, defaulting to the tag's `"standin"`; `open_pool`'s one-line docstring names ChamPBH's
  registry; the provenance is a comment block between the licence and the docstring.
  `serial_batch_sizes` is not defaulted in the copy (the prompt lists what it opens with); callers
  pass it.
- **D6 — IMPLEMENTATION CHOICE (the harness).** It opens the pool through the copy's own
  `open_pool`/`close_pool` (so the changed functions are exercised by every harness test), where
  `TestTheActor` calls `sp.sp_mod.ShardedPool` directly; it passes `serial_batch_sizes` to the pool
  and to the actor, as the pool passes it to its actors; the actor's name is `probeNNNN-store`, as in
  `TestTheActor`.
- **D7 — IMPLEMENTATION CHOICE.** Test a reads the actor's `_version_serial` (one of the prompt's two
  options), so the harness records no serials of its own.
- **D8 — IMPLEMENTATION CHOICE.** `config/version.py` gains a five-line comment in place of the
  deleted prose: the two names are `datastorekit.contract`'s, and its `VERSION_LABEL` is not this
  one (README §6.3's hazard).
- **D9 — IMPLEMENTATION CHOICE.** `test_datastore_config.py`'s module docstring is rewritten (it said
  the layer builds the factories); the module's imports gain `os`, `subprocess`, `sys`, `tempfile`,
  `mock`, `ray`, `sqlalchemy`, `allowed_drop_actions` and the two `config.sharding` lists, and lose
  `importlib`.
- **D10 — IMPLEMENTATION CHOICE.** A8.5's subprocess passes `--ray-address 127.0.0.1:9`, an address
  nothing listens on, so that a regression that moved the refusal after `ray.init` would fail to
  connect rather than join a cluster that happens to be running.
- **D11 — IMPLEMENTATION CHOICE.** The review-remediation board's header date (2026-09-30, a closed
  campaign) is left; its entry carries its own dated **Narrowed** line.
- **Reading of §2.4.5** ("fail on the actor's refusal (`:778-786`), not on anything else"). Under
  breakage 5, 27 of the 30 errors are that refusal (`cannot insert into "ScalarModel"` 26,
  `"ExponentialCoupling"` 1); the other 3 (`test_a4_an_unpopulated_model_refuses_to_report_a_reason`,
  `test_d_unpopulated_model_raises`, `test_e_unpopulated_model_raises`) make a keyed lookup with no
  insert before it, and fail on the actor's other refusal for the same cause, the lookup before the
  serial is set (`datastorekit/SQL/Datastore.py:608-614`). No error is anything else. Both refusals
  are the contract's (contract §8; the actor's docstring), so this is not a §4 stop. The
  orchestrator's prototype measured only `test_version_keyed_lookups`, whose tests all insert first.
- No `UNINTENDED DRIFT` known.

## Verification performed

Everything from the repository root; DatastoreKit's script from DatastoreKit's root with its own
`./venv/bin/python`. Scratch: the session scratchpad's `agent08/work/`.

### §2.1 Before

**Tree.** `HEAD` `63f60e11253c454daee856c433531013fcde525d`, branch `dk-and-rpk-migration`; no tracked
change, nothing staged; the untracked set is the dispatch's. No Ray process (`pgrep -lf
'gcs_server|raylet|ray::'` empty); no rehearsal lock file. `git diff --stat 617a1dc HEAD -- .
':!prompts' ':!.documents'` empty. Load 4.51 / 7.98 / 10.40 at 22:04.

**Suites at `63f60e1`** (in the foreground, serially, output to scratch files):

| Suite | Result | Time | Load at start |
|---|---|---|---|
| `Datastore/tests` | `Ran 73 tests`, `OK` | 3.5 s | 4.51 / 7.98 / 10.40 (22:04) |
| `CosmologyModels/tests` | `Ran 18 tests`, `OK` | 80.9 s | 3.88 / 7.56 / 10.18 (22:04) |
| `ComputeTargets/tests`, `test_[a-f]*.py` | `Ran 61 tests`, `OK` | 120.1 s | 8.95 / 7.96 / 10.06 (22:05) |
| `ComputeTargets/tests`, `test_[g-z]*.py` | `Ran 53 tests`, `OK` | 104.8 s | 15.65 / 10.74 / 10.87 (22:08) |

By module (a `unittest` loader count, `count_tests.py` in scratch): Datastore `bbn_failure_lookup`
6, `compare_stores` 21, `datastore_config` 7, `first_bounce_round_trip` 5,
`fixed_T_values_round_trip` 5, `package_provenance` 10, `scalarmodel_failure_reason` 4,
`shard_key_assignment` 4, `version_keyed_lookups` 11 — as the prompt's §1. CosmologyModels
`eos_w_derivative` 6, `kicking_function` 6, `temperature_law` 6. ComputeTargets
`adiabatic_mass` 9, `bbn_callbacks` 9, `bbn_from_store` 3, `bbn_solver_failures` 9,
`bbn_spline_floor` 2, `extraction` 13, `first_bounce` 7, `fixed_T_values` 3,
`foreign_bbn_provenance` 6, `initial_field_option` 4, `integrator_exceptions` 10,
`kinematic_cap_loop` 15, `lowT_tolerance` 1, `network_flag` 3, `pipeline_selection` 6,
`prym_passenger` 4, `reflection_reporting` 6, `scalarmodel_failure_reason` 4.

**DatastoreKit's script at `63f60e1`** (`docs/extraction/measure_client_imports.py cpbh --commit
63f60e11…`, exit 0) equals the prompt's §1 and the orchestrator's measurement at `c4bd666` line for
line: **43 layer imports in 39 files**, 1 nested (`Datastore/tests/test_compare_stores.py:713`,
`try`), 0 string literals, **1 `import_module`** (`test_datastore_config.py:39`), 18 factory-module
imports (17 in `config/`, 1 top level), **4 imports of the moved names** (the three factories'
`require_version_serial` at `AdiabaticHistory.py:31`, `BBNData.py:20`, `ScalarModel.py:44`, and
`test_version_keyed_lookups.py:350`'s `VERSION_SERIAL_KEY`). By group: top level 6, `ComputeTargets/` 3,
`CosmologyConcepts/` 5, `CosmologyModels/` 1, `Datastore/SQL/ObjectFactories/` 17,
`Datastore/tests/` 5 (1 nested), `MetadataConcepts/` 3, `Quadrature/` 1, `config/` 1, `prompts/` 1.
201 tracked `.py` files outside the layer.

**The `git grep`s** (`git grep -n <pattern> -- ':!prompts'`), every hit classified:

| Pattern | Hits | Classification |
|---|---|---|
| `Datastore\.SQL\.\(Datastore\|ShardedPool\|ClientPool\|ProfileAgent\|SerialPoolBroker\)` | 25 | the layer's own imports (6, in files deleted); the 13 layer imports §1 lists in drivers, `config/model_list.py` and tests; `test_datastore_config.py:37` (the import-order comment, A7.3); `config/datastore.py:31-32`, `:38` (the docstring, A9.3) |
| `from Datastore import` | 13 | the 13 `DatastoreObject` imports of §1 |
| `ObjectFactories.base` | 20 | the 17 factories, `Datastore/SQL/Datastore.py:28` (deleted); `.documents/OPEN_ISSUES.md:51` (A9.3); `.documents/architecture-summary.md:357` (11's) |
| `_factories\|_drop_actions\|_drop_order\|_default_serial_batch_size` | 48 | the layer's own (deleted); the actor's **instance attribute** `store._factories` in tests (`test_scalarmodel_failure_reason.py:89`, `test_version_keyed_lookups.py:336` in d, `:389` in d4), which the package's actor also has; `test_compare_stores.py:713` (§1); `test_datastore_config.py:114` (a test name, `test_factories_registry`), `:146-153` (`TestLayerReadsConfig`, deleted); `config/datastore.py:25`, `:31-33` (docstring); `allowed_drop_actions` (`config/argument_parser.py:33`, `:267`; `main.py:53`, `:97-98`), which stays (§2.2); `.documents/architecture-summary.md:390`, `:1001`, `:1071-1073` (11's); `.documents/review-remediation-verification.md:1248` (history) |
| `VERSION_SERIAL_KEY\|require_version_serial` | 17 | `config/version.py:55-68` (the definitions, A2); the layer's `Datastore.py:34`, `:460-465` (deleted); the three factories' import and call lines (A2: imports only); `test_version_keyed_lookups.py:350`, `:359`, `:363` (d2, deleted); `.documents/architecture-summary.md:297` (11's) |
| `inventory_config` | 13 | its definition `config/sharding.py:58` (stays, 09's); the drivers' imports (`main.py:64`, `plot_ScalarModel.py:55`, `plot_by_beta.py:48`) and pool arguments (`:1362`, `:1593`, `:1023`), which go (A4, A9.1); the layer's `ShardedPool.py` (deleted); `.documents/architecture-summary.md:320` (11's) |
| `__ray_actor_class__` | 5 | `test_version_keyed_lookups.py:20` (its docstring, rewritten with the harness) and `:47` (`DS`, deleted with d4); `CLAUDE.md:70` (A9.3); `test_bbn_failure_lookup.py:30` and `test_foreign_bbn_provenance.py:38`, module docstrings **left as they are** (their modules change only their imports, if at all) and recorded for 11 |

`git grep -n 'test_version_keyed_lookups\|_TempStoreCase' -- ':!prompts' ':!.documents'`: the
harness's five importers, as §1 names them, and nothing else. **Nothing §1 and the orchestrator's
facts do not account for; no stop.**

**§2.1.3, the hooks.** What the package calls on a registry entry, read at the tag:
`register()` (`schema.build_schema`, actor and pool); `build(payload=, conn=, table=, inserter=,
tables=, inserters=)` (`Datastore.object_get` `:565`; the pool's version-row lookup `ShardedPool.py:521`);
`store(obj, conn=, table=, inserter=, tables=, inserters=)` (`:711`); `validate(obj, conn=, table=,
tables=)` (`:876`); `validate_on_startup(conn, tab, tables, prune)` positionally (`:492`, `:266`)
and, on replicated classes only, `validate_on_startup(conn, table, tables, prune=…)` (the pool's
prune, `ShardedPool.py:2289`, `:2324-2325`, reached only through `_prune_replicated_tables`);
`read_batch(payload=, conn=, table=, tables=)` (`:643`); `read_table(conn, tab, *args, **kwargs)`
(`:931`, the caller's arguments forwarded, `tables=` added when `read_table_config` says
`tables_arg`); `owned_serials(obj)` (replicated stores, `ShardedPool.py:3490`), `revalidate(serial,
conn, table, tables)` (classes declaring `validated_column`, `:2110`) and `inventory_spec()`
(`store_inventory.inventory_specs`, `schema.dependent_tables`) — the last three defaulted by the
package's base, and defined by no ChamPBH factory (`git grep` for them and for `validated_column`
in `Datastore/SQL/ObjectFactories` and `config`: nothing). Each ChamPBH factory's hooks, by `ast`
(`hooks.py` in scratch):

| Class | Hook | Parameters (as defined) | Package's call | Verdict | Where |
|---|---|---|---|---|---|
| `sqla_AdiabaticHistoryTagAssociation_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/AdiabaticHistory.py:38` |
| `sqla_AdiabaticHistoryTagAssociation_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/AdiabaticHistory.py:64` |
| `sqla_AdiabaticHistoryFactory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/AdiabaticHistory.py:95` |
| `sqla_AdiabaticHistoryFactory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/AdiabaticHistory.py:124` |
| `sqla_AdiabaticHistoryFactory` | `store` | `(self, obj, conn, table, inserter, tables, inserters)` | 1 positional; kw conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/AdiabaticHistory.py:247` |
| `sqla_AdiabaticHistoryFactory` | `validate` | `(self, obj, conn, table, tables)` | 1 positional; kw conn, table, tables | ok | `Datastore/SQL/ObjectFactories/AdiabaticHistory.py:291` |
| `sqla_AdiabaticHistoryFactory` | `validate_on_startup` | `(self, conn, table, tables, prune  (1 default))` | 4 positional | ok | `Datastore/SQL/ObjectFactories/AdiabaticHistory.py:324` |
| `sqla_AdiabaticHistoryFactory` | `inventory` | `(self, conn, table, tables)` | — | not called by the package | `Datastore/SQL/ObjectFactories/AdiabaticHistory.py:387` |
| `sqla_AdiabaticHistoryValue_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/AdiabaticHistory.py:446` |
| `sqla_AdiabaticHistoryValue_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/AdiabaticHistory.py:478` |
| `sqla_BBNDataTagAssociation_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/BBNData.py:27` |
| `sqla_BBNDataTagAssociation_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/BBNData.py:53` |
| `sqla_BBNDataFactory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/BBNData.py:82` |
| `sqla_BBNDataFactory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/BBNData.py:119` |
| `sqla_BBNDataFactory` | `store` | `(self, obj, conn, table, inserter, tables, inserters)` | 1 positional; kw conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/BBNData.py:294` |
| `sqla_BBNDataFactory` | `validate` | `(self, obj, conn, table, tables)` | 1 positional; kw conn, table, tables | ok | `Datastore/SQL/ObjectFactories/BBNData.py:347` |
| `sqla_BBNDataFactory` | `validate_on_startup` | `(self, conn, table, tables, prune  (1 default))` | 4 positional | ok | `Datastore/SQL/ObjectFactories/BBNData.py:386` |
| `sqla_BBNDataFactory` | `inventory` | `(self, conn, table, tables)` | — | not called by the package | `Datastore/SQL/ObjectFactories/BBNData.py:453` |
| `sqla_BBNDataValue_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/BBNData.py:518` |
| `sqla_BBNDataValue_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/BBNData.py:547` |
| `sqla_dimensionful_quantity_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/DimensionfulQuantity.py:34` |
| `sqla_dimensionful_quantity_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/DimensionfulQuantity.py:43` |
| `sqla_dimensionful_quantity_factory` | `read_table` | `(self, conn, table, units)` | 2 positional; kw <caller's *args, **kwargs>, tables= if tables_arg | ok (conn, table, then the caller's arguments) | `Datastore/SQL/ObjectFactories/DimensionfulQuantity.py:99` |
| `sqla_dimensionful_quantity_factory` | `inventory` | `(self, conn, table, tables, units)` | — | not called by the package | `Datastore/SQL/ObjectFactories/DimensionfulQuantity.py:122` |
| `sqla_dimensionless_quantity_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/DimensionlessQuantity.py:32` |
| `sqla_dimensionless_quantity_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/DimensionlessQuantity.py:41` |
| `sqla_dimensionless_quantity_factory` | `read_table` | `(self, conn, table)` | 2 positional; kw <caller's *args, **kwargs>, tables= if tables_arg | ok (conn, table, then the caller's arguments) | `Datastore/SQL/ObjectFactories/DimensionlessQuantity.py:80` |
| `sqla_dimensionless_quantity_factory` | `inventory` | `(self, conn, table, tables)` | — | not called by the package | `Datastore/SQL/ObjectFactories/DimensionlessQuantity.py:95` |
| `sqla_ExponentialCoupling_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/ExponentialCoupling.py:29` |
| `sqla_ExponentialCoupling_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/ExponentialCoupling.py:44` |
| `sqla_ExponentialPotential_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/ExponentialPotential.py:29` |
| `sqla_ExponentialPotential_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/ExponentialPotential.py:52` |
| `sqla_InversePowerPotential_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/InversePowerPotential.py:29` |
| `sqla_InversePowerPotential_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/InversePowerPotential.py:52` |
| `sqla_QCDCosmology_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/QCD_Cosmology.py:27` |
| `sqla_QCDCosmology_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/QCD_Cosmology.py:42` |
| `sqla_ReclinerPotential_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/ReclinerPotential.py:29` |
| `sqla_ReclinerPotential_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/ReclinerPotential.py:51` |
| `sqla_ReflectingPotential_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/ReflectingPotential.py:29` |
| `sqla_ReflectingPotential_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/ReflectingPotential.py:51` |
| `sqla_ScalarModelTagAssociation_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/ScalarModel.py:51` |
| `sqla_ScalarModelTagAssociation_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/ScalarModel.py:77` |
| `sqla_ScalarModelFactory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/ScalarModel.py:106` |
| `sqla_ScalarModelFactory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/ScalarModel.py:212` |
| `sqla_ScalarModelFactory` | `store` | `(self, obj, conn, table, inserter, tables, inserters)` | 1 positional; kw conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/ScalarModel.py:531` |
| `sqla_ScalarModelFactory` | `validate` | `(self, obj, conn, table, tables)` | 1 positional; kw conn, table, tables | ok | `Datastore/SQL/ObjectFactories/ScalarModel.py:672` |
| `sqla_ScalarModelFactory` | `validate_on_startup` | `(self, conn, table, tables, prune  (1 default))` | 4 positional | ok | `Datastore/SQL/ObjectFactories/ScalarModel.py:717` |
| `sqla_ScalarModelFactory` | `inventory` | `(self, conn, table, tables)` | — | not called by the package | `Datastore/SQL/ObjectFactories/ScalarModel.py:804` |
| `sqla_ScalarModelValue_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/ScalarModel.py:881` |
| `sqla_ScalarModelValue_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/ScalarModel.py:921` |
| `sqla_StarobinskyPotential_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/Starobinsky.py:29` |
| `sqla_StarobinskyPotential_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/Starobinsky.py:51` |
| `sqla_IntegrationSolver_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/integration_metadata.py:27` |
| `sqla_IntegrationSolver_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/integration_metadata.py:37` |
| `sqla_redshift_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/redshift.py:35` |
| `sqla_redshift_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/redshift.py:44` |
| `sqla_redshift_factory` | `read_table` | `(self, conn, table, tables, model_proxy  (1 default))` | 2 positional; kw <caller's *args, **kwargs>, tables= if tables_arg | ok (conn, table, then the caller's arguments) | `Datastore/SQL/ObjectFactories/redshift.py:81` |
| `sqla_redshift_factory` | `inventory` | `(self, conn, table, tables)` | — | not called by the package | `Datastore/SQL/ObjectFactories/redshift.py:118` |
| `sqla_store_tag_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/store_tag.py:27` |
| `sqla_store_tag_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/store_tag.py:34` |
| `sqla_tolerance_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/tolerance.py:29` |
| `sqla_tolerance_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/tolerance.py:36` |
| `sqla_version_factory` | `register` | `(self)` | () | ok | `Datastore/SQL/ObjectFactories/version.py:27` |
| `sqla_version_factory` | `build` | `(self, payload, conn, table, inserter, tables, inserters)` | kw payload, conn, table, inserter, tables, inserters | ok | `Datastore/SQL/ObjectFactories/version.py:34` |

hooks defined, by name: {'register': 23, 'build': 23, 'store': 3, 'validate': 3, 'validate_on_startup': 3, 'inventory': 6, 'read_table': 3}
mismatches: 0

**No mismatch: no stop.** The three `validate_on_startup` hooks (all on sharded classes) take
`prune` as a named parameter, so the pool's keyword call would be accepted too; the three
`read_table` hooks take `(conn, table, …)` and then the plot scripts' own arguments (`units`) or
`tables=` (`redshift`, the one `tables_arg: True`), as under the old layer, whose call was the same
(`Datastore/SQL/Datastore.py:698` at `63f60e1`). The six `inventory()` hooks are never called by the
package (09 deletes them).

**§2.1.4, the schema before.** R0's snapshot `.documents/dk-and-rpk-migration/out/02-reference-schema.json`
(27 client tables; 06's `06-reference-schema.json` equals it but for `source`).

### §2.3.1 The suites, after the change

| Suite | 07 / before | After | Time | Load at start → end |
|---|---|---|---|---|
| `Datastore/tests` | 73 | **76**, `OK` | 8.5 s | 5.40 / 6.86 / 8.33 (22:20) |
| `CosmologyModels/tests` | 18 | 18, `OK` | 78.0 s | 5.82 (22:21) → 8.45 (22:22) |
| `ComputeTargets/tests`, `test_[a-f]*.py` | 61 | 61, `OK` | 117.9 s | 8.18 (22:22) → 20.02 (22:24) |
| `ComputeTargets/tests`, `test_[g-z]*.py` | 53 | 53, `OK` | 104.7 s | 18.82 (22:24) → 7.76 (22:26) |

**Datastore, derived by module:**

| Module | Before | After | Why |
|---|---|---|---|
| `bbn_failure_lookup` | 6 | 6 | on the rebuilt harness, unchanged |
| `compare_stores` | 21 | 21 | `_registry()`, test 14's assertion |
| `datastore_config` | 7 | 16 | − 3 (`TestLayerReadsConfig`) + 12 (A8) |
| `first_bounce_round_trip` | 5 | 5 | unchanged |
| `fixed_T_values_round_trip` | 5 | 5 | unchanged |
| `package_provenance` | 10 | 10 | unchanged |
| `scalarmodel_failure_reason` | 4 | 4 | unchanged |
| `shard_key_assignment` | 4 | — | deleted (A7.1) |
| `version_keyed_lookups` | 11 | 9 | − d2, d4 (A7.2) |
| **total** | **73** | **76** | 73 − 4 − 2 − 3 + 12 |

ComputeTargets and CosmologyModels: the per-module counts are identical to before (`diff` of the two
`count_tests.py` outputs: empty); `test_foreign_bbn_provenance` 6 on the rebuilt harness. No test
failed at any point after the change, so none needed a re-run alone. No Ray process afterwards. The
harness and its importers, run together: `Ran 35 tests`, `OK`.

### §2.3.2 DatastoreKit's script at a dangling commit of the staged tree

The staged tree (all of this commit's code, the tests, `CLAUDE.md`, the index, the review-remediation
board and `out/08-standin-schema.json`; not yet this log or this board) was written with
`git write-tree` (`3e54fc8d19cde3a562127b3cfc6f350847f814a0`) and committed with `git commit-tree -p HEAD`
as the dangling **`1216c9a2a0fcb4d4350ec250c9d52a5d68845508`**, on no branch (`git branch --contains`
empty; no `update-ref`). `git ls-tree` of it: none of the 7 deleted modules; `Datastore/__init__.py`
and `Datastore/SQL/__init__.py` present, 0 bytes, blob `e69de29`. The script, `--commit 1216c9a2…`,
exit 0:

```text
Written by `docs/extraction/measure_client_imports.py`, reading through `git ls-tree` and `git show` only.

- **Repository:** `/Users/ds283/Documents/Code/ChamPBH`
- **Commit:** `1216c9a2a0fcb4d4350ec250c9d52a5d68845508` (given as `1216c9a2a0fcb4d4350ec250c9d52a5d68845508`), 2026-10-12 dk-and-rpk-migration prompt 08: staged tree, for measurement only (dangling)
- **Layer files** (9): `Datastore/__init__.py`, `Datastore/object.py`, `Datastore/SQL/__init__.py`, `Datastore/SQL/ClientPool.py`, `Datastore/SQL/Datastore.py`, `Datastore/SQL/ProfileAgent.py`, `Datastore/SQL/SerialPoolBroker.py`, `Datastore/SQL/ShardedPool.py`, `Datastore/SQL/ObjectFactories/base.py`
- **Layer files absent at this commit:** `Datastore/object.py`, `Datastore/SQL/ClientPool.py`, `Datastore/SQL/Datastore.py`, `Datastore/SQL/ProfileAgent.py`, `Datastore/SQL/SerialPoolBroker.py`, `Datastore/SQL/ShardedPool.py`, `Datastore/SQL/ObjectFactories/base.py`
- **Factory package:** `Datastore/SQL/ObjectFactories/` (its `base.py` is the layer's)
- **Files read:** 201 tracked `.py` files outside the layer; 0 do not parse

## Totals

| What | Count |
|---|---|
| Import statements naming a layer module | **1** in 1 files |
| of which nested (not at module level) | 0 |
| String literals naming a layer module | 0 (0 exactly its name) |
| `import_module` / `__import__` calls naming a layer module | 0 |
| Import statements naming a factory module (not layer imports) | 18 (18 outside `Datastore/SQL/ObjectFactories/`) |
| Imports of names that move into the package | 0 |

## Layer imports, by group

| Group | Statements | Nested | Files |
|---|---|---|---|
| `prompts/` | 1 | 0 | 1 |
| **total** | **1** | **0** | **1** |

## Layer imports, by module

A statement naming two layer modules is counted under each.

| Module | Statements | Names imported by `from` (statements) |
|---|---|---|
| `Datastore.SQL.Datastore` | 1 | `Datastore` 1 |

## Every layer import

### `prompts/` (1)

| File:line | Nested | Module | Names |
|---|---|---|---|
| `prompts/run-integrity/planning-probes/datastore_version_probe.py:18` | — | `Datastore.SQL.Datastore` | `Datastore` |

## String literals naming a layer module

None.

## `import_module` calls naming a layer module

None.

## Imports of factory modules, by group (count only)

| Group | Statements |
|---|---|
| `(top level)` | 1 |
| `config/` | 17 |
| **total** | **18** |

## Imports of names that move into the package

`config.version`: `VERSION_SERIAL_KEY`, `require_version_serial`.

None.

## Files that do not parse

None.
```

So: **Layer files absent**: the 7; outside `prompts/`, **0 import statements, 0 string literals and 0
`import_module` calls** naming a layer module; `Datastore/tests/` has no section (0); `prompts/`
keeps its one (`prompts/run-integrity/planning-probes/datastore_version_probe.py:18`); **0 imports
of the moved names**. The factory-module imports are unchanged (18). The review re-runs it at this
commit; the log and the board add no `.py` file.

### §2.3.3 Without Ray

- `./venv/bin/python -c "import config.datastore"` from the root: **exit 0**. With `PYTHONPATH=.`:
  27 entries, all instances of `datastorekit.SQL.factory_base.SQLAFactoryBase` subclasses (23
  classes); `ray.is_initialized()` `False`.
- `./venv/bin/python -m datastorekit.tools.sharded_store --help`: **exit 0** (usage
  `python -m datastorekit.tools.sharded_store [-h] {copy,move} src dst`).
- `check_imports.py` (log 04's, unchanged, re-created in scratch): **163 imported names, 0
  failures**, `ray.is_initialized() = False`, exit 0. 163 = 162 − 3 (`inventory_config`) − 3 (D3) + 7
  (`config.datastore`'s names: 3 in `main.py`, 2 in each plot script). The changed lines:

```text
main.py:44: from datastorekit.SQL.ProfileAgent import ProfileAgent: ok
main.py:45: from datastorekit.SQL.ShardedPool import ShardedPool: ok
main.py:51: from config.datastore import factories: ok
main.py:51: from config.datastore import serial_batch_sizes: ok
main.py:51: from config.datastore import tables_to_drop: ok
plot_by_beta.py:35: from datastorekit.SQL.ProfileAgent import ProfileAgent: ok
plot_by_beta.py:36: from datastorekit.SQL.ShardedPool import ShardedPool: ok
plot_by_beta.py:41: from config.datastore import factories: ok
plot_by_beta.py:41: from config.datastore import serial_batch_sizes: ok
plot_ScalarModel.py:41: from datastorekit.SQL.ProfileAgent import ProfileAgent: ok
plot_ScalarModel.py:42: from datastorekit.SQL.ShardedPool import ShardedPool: ok
plot_ScalarModel.py:48: from config.datastore import factories: ok
plot_ScalarModel.py:48: from config.datastore import serial_batch_sizes: ok
checked 163 imported names; failures 0; ray.is_initialized() = False
```

- `./venv/bin/python -m py_compile main.py plot_by_beta.py plot_ScalarModel.py`: exit 0.
- **The two refusals** (`<tmp>` an empty scratch directory; no Ray process before or after):

```text
$ ./venv/bin/python main.py --database <tmp>/x.db --inventory
>> packages: datastorekit 0.3.0 (https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21, requested v0.3.0); raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)
!! --inventory: the datastore inventory is unavailable until dk-and-rpk-migration prompt 09 ports it to datastorekit; nothing was opened
(exit 2)
$ ./venv/bin/python main.py --database <tmp>/x.db --drop scalar-model
>> packages: datastorekit 0.3.0 (…as above…)
!! --drop: cannot drop scalar-model alone: AdiabaticHistory and BBNData name its rows; add `--drop adiabatic-history bbn-data`
(exit 2)
```

  `<tmp>` was still empty afterwards; `pgrep` found no Ray process.

### §2.3.4 The schema

`build_standin_store.py` (scratch), run from the root:

```python
"""Build a 16-shard stand-in store under datastorekit's real ShardedPool, with ChamPBH's registry
and tables, label 2026.6.0, and close it. No Ray. Run from the repository root. Args: <outdir>."""
import sys
from pathlib import Path

sys.path.insert(0, ".")
import ray

from Datastore.tests import standin_pool as sp
from config.datastore import serial_batch_sizes
from config.version import VERSION_LABEL

out = Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=False)
primary = out / "store.db"
cluster = sp.StandinCluster()
with cluster.active():
    pool = cluster.open_pool(
        primary, shards=16, version_label=VERSION_LABEL, serial_batch_sizes=serial_batch_sizes
    )
    print("version serial", pool._version.store_id, "shards", len(pool._shard_db_files))
    cluster.close_pool(pool)
print("files", sorted(p.name for p in out.iterdir()))
print("built", primary, "label", VERSION_LABEL, "ray.is_initialized() =", ray.is_initialized())
```

```text
version serial 1 shards 16
files ['store-shard0000.db', …, 'store-shard0015.db', 'store.db']
built <scratch>/schema-store/store.db label 2026.6.0 ray.is_initialized() = False
$ ./venv/bin/python tools/compare_stores.py --snapshot <scratch>/schema-store/store.db --out .documents/dk-and-rpk-migration/out/08-standin-schema.json
compare_stores: schema snapshot written to .documents/dk-and-rpk-migration/out/08-standin-schema.json
(exit 0)
$ ./venv/bin/python tools/compare_stores.py --schema <scratch>/schema-store/store.db --against .documents/dk-and-rpk-migration/out/02-reference-schema.json
compare_stores: schema of <scratch>/schema-store/store.db against .documents/dk-and-rpk-migration/out/02-reference-schema.json
RESULT: the schemas agree
(exit 0)
```

And by `json`: `08-standin-schema.json` equals `02-reference-schema.json` in `format`, `version` and
`tables` (27 tables each), and differs only in `source` (the scratch store's path). **The schema
differences are none**: the package adds no column, index or type that ChamPBH's actor did not
make; no column of ChamPBH's own moved, vanished or changed type. 10 compares its store with
`out/08-standin-schema.json`.

### §2.3.5 `black`

`black --check` on the 44 Python files this commit touches or adds (the 43 modified and
`Datastore/tests/standin_pool.py`): `44 files would be left unchanged.` `black` was run on the
files this prompt edited by hand (`config/datastore.py`, `main.py`, `pipeline_selection.py`, the
three test modules, `standin_pool.py`); the `sed`-rewritten import sites were black-clean before and
after.

## The deliberate-breakage record

Each was applied to the working tree on top of the staged change (working tree equal to the index
before each), shown to be caught, and reverted with `git apply -R`. Each diff below is exactly the
`git diff` of the mutation; in `bash`, `git apply -R --check` passed on the mutated tree before the
revert and `git apply --check` on the restored tree after it, and `git diff --quiet` was true
afterwards. So each replays with `git apply` from a clean tree at this commit. Nothing was
committed.

**Breakage 1 — a factory's `SQLAFactoryBase` import pointed back at the layer.**

```diff
diff --git a/Datastore/SQL/ObjectFactories/tolerance.py b/Datastore/SQL/ObjectFactories/tolerance.py
index f037f83..9df9f6e 100644
--- a/Datastore/SQL/ObjectFactories/tolerance.py
+++ b/Datastore/SQL/ObjectFactories/tolerance.py
@@ -17,7 +17,7 @@ from math import log10
 
 import sqlalchemy as sqla
 
-from datastorekit.SQL.factory_base import SQLAFactoryBase
+from Datastore.SQL.ObjectFactories.base import SQLAFactoryBase
 from MetadataConcepts import tolerance
 from config.defaults import DEFAULT_FLOAT_PRECISION
 
```

Caught. `Datastore/tests`: `Ran 37 tests`, `FAILED (errors=8)`; six modules fail at import
(`test_bbn_failure_lookup`, `test_datastore_config`, `test_first_bounce_round_trip`,
`test_fixed_T_values_round_trip`, `test_scalarmodel_failure_reason`, `test_version_keyed_lookups`)
and two `test_compare_stores` tests (13, 14) error in `_registry()`; all eight with
`ModuleNotFoundError: No module named 'Datastore.SQL.ObjectFactories.base'`. `check_imports.py`:
`failures 7` (the seven names the drivers import from `config.datastore`), exit 1, each naming
the module.

**Breakage 2 — `from Datastore import DatastoreObject` restored in a `CosmologyConcepts` class.**

```diff
diff --git a/CosmologyConcepts/redshift.py b/CosmologyConcepts/redshift.py
index c2767c4..6847269 100644
--- a/CosmologyConcepts/redshift.py
+++ b/CosmologyConcepts/redshift.py
@@ -16,7 +16,7 @@
 from functools import total_ordering
 from typing import Iterable, Self
 
-from datastorekit import DatastoreObject
+from Datastore import DatastoreObject
 from config.defaults import DEFAULT_FLOAT_PRECISION
 
 
```

Caught by `check_imports.py`: `checked 163 imported names; failures 83`, exit 1; each failure is
`ImportError: cannot import name 'DatastoreObject' from 'Datastore'
(…/Datastore/__init__.py)`, or a module that imports `CosmologyConcepts` not found as a result.

**Breakage 3 — `"LambdaCDM"` restored to `replicated_tables`.**

```diff
diff --git a/config/sharding.py b/config/sharding.py
index 244e4d2..12a24f4 100644
--- a/config/sharding.py
+++ b/config/sharding.py
@@ -31,6 +31,7 @@ replicated_tables = [
     "ReflectingPotential",
     "ReclinerPotential",
     "StarobinskyPotential",
+    "LambdaCDM",
     "QCD_Cosmology",
     "IntegrationSolver",
 ]
```

Caught by A8.1: `TestTablesAreDeclared.test_every_listed_table_is_registered_and_LambdaCDM_is_in_none`
fails twice (its `subTest(table='LambdaCDM')`: `'LambdaCDM' not found in {…}`; then `'LambdaCDM'
unexpectedly found in [...]`); `FAILED (failures=2)`, every other test of the module passes.

**Breakage 4 — the `dependent_tables` check removed from `tables_to_drop`.**

```diff
diff --git a/config/datastore.py b/config/datastore.py
index e746f8a..e32b60a 100644
--- a/config/datastore.py
+++ b/config/datastore.py
@@ -218,9 +218,7 @@ def tables_to_drop(groups: Iterable[str]) -> List[str]:
         if group in requested
         for table in group_tables
     ]
-    dependents = [
-        table for table in dependent_tables(tables, factories) if table not in tables
-    ]
+    dependents = []
     if len(dependents) == 0:
         return tables
 
```

Caught by A8.3: `test_scalar_model_alone_is_refused_naming_the_groups_to_add` (`ValueError not
raised`), both subtests of `test_scalar_model_with_one_dependent_group_is_refused`, and
`test_a_dependent_in_no_group_is_named_as_a_table`; `FAILED (failures=4)`.

**Breakage 5 — `set_version` dropped from the harness.**

```diff
diff --git a/Datastore/tests/test_version_keyed_lookups.py b/Datastore/tests/test_version_keyed_lookups.py
index 1874694..65cfb6a 100644
--- a/Datastore/tests/test_version_keyed_lookups.py
+++ b/Datastore/tests/test_version_keyed_lookups.py
@@ -260,7 +260,6 @@ class _TempStoreCase(unittest.TestCase):
         self.broker.notify_largest_store_ids.remote(
             store.read_largest_store_ids()
         ).get()
-        store.set_version(serial)
         return store
 
 
```

Caught. The harness module and its five importers: `Ran 35 tests`, `FAILED (errors=30)`, every error
a `RuntimeError` from the package's actor: 26 `Datastore "probe0000-store": cannot insert into
"ScalarModel" before the version serial is set: …` and 1 `… "ExponentialCoupling" …` (test e), the
refusal at `datastorekit/SQL/Datastore.py:778-786`; and 3 `cannot look up "ScalarModel", whose
lookups are keyed on the version serial, before the serial of label "2026.3.0" is set …`
(`:608-614`), from the three tests that look up before inserting (the reading of §2.4.5 above). The
5 tests that pass are d3 and f (no insert, no lookup) and the 3 in `test_foreign_bbn_provenance`
that build no store.

**Breakage 6 — `plot_by_beta.py`'s `factories=` bound to a copy.**

```diff
diff --git a/plot_by_beta.py b/plot_by_beta.py
index 5320192..99e6b27 100644
--- a/plot_by_beta.py
+++ b/plot_by_beta.py
@@ -1020,7 +1020,7 @@ with ShardedPool(
     job_name="plot_ScalarModel",
     prune_unvalidated=False,
     read_table_config=read_table_config,
-    factories=factories,
+    factories=dict(factories),
     serial_batch_sizes=serial_batch_sizes,
 ) as pool:
     # build absolute and relative tolerances
```

Caught by A8.2: `test_each_pool_is_given_config_datastore` fails for `driver='plot_by_beta.py'`:
`<ast.Call …> is not an instance of <class 'ast.Name'> : plot_by_beta.py: factories= is not a bare
name`; `FAILED (failures=1)`.

## Observations not acted on

1. **`tools/compare_stores.py`'s stale citations** of the deleted layer (README §4: it is run
   unchanged): `:89-91` (`Datastore/SQL/ShardedPool.py:244-291`, `_write_shard_data`) and
   `:108-110` (`Datastore/SQL/Datastore.py:309-314, :670-674`, `:679-680`). What they say stays
   true of the package (its primary's tables, its serial lease and `datetime.now()`), only the
   paths are gone. **For 11.**
2. **`.documents/architecture-summary.md`'s database sections** (`:297`, `:320`, `:357`, `:390`,
   `:1001`, `:1071-1073`, and its layout of `Datastore/`) describe the deleted layer. **11's (P14).**
3. **Test docstrings and comments left as they are** (README §5 rule 7; their modules did not
   change): `Datastore/tests/test_bbn_failure_lookup.py:29-33` and
   `ComputeTargets/tests/test_foreign_bbn_provenance.py:38-40` say each test builds "the undecorated
   `Datastore.__ray_actor_class__`"; `test_scalarmodel_failure_reason.py:95`'s comment says "there is
   no serial broker here" (there is one now; its rows still pass an explicit serial, so the comment's
   conclusion holds); `test_bbn_failure_lookup.py:32` "explicit, increasing serials", true.
   `test_compare_stores.py:843-846`'s `_create_shard` docstring cites "`Datastore/SQL/Datastore.py`
   `_build_schema`" (A7.4 changes nothing else in that module); the schema it builds still agrees
   with the package's (test 14 passes). `test_datastore_config.py`'s `TestConfigDatastoreData`
   docstring says "exactly the data the layer held at prompt 04's tree", which is now less
   `LambdaCDM`. **For 11.**
4. **`config/argument_parser.py`**: `--inventory`'s help still says "show an inventory of the
   datastore content", and `--show-all` ("do not truncate long lists of inventory items") is now read
   by nothing; both are 09's, which restores the inventory. The file is not in §5's list.
5. **`plot_by_beta.py:1020`** passes `job_name="plot_ScalarModel"` (since before this campaign; a
   copy of `plot_ScalarModel.py:1590`). Cosmetic; a housekeeping candidate, not opened as an issue.
6. **The stand-in broker is exercised by no test**: every insert in today's harness tests passes an
   explicit serial. The probe above shows the wiring; 09's tests, which may insert without serials,
   will be the first to use it.
7. **Breakage 5's three lookup refusals** (the reading of §2.4.5): a harness test that looks up
   before it inserts fails on the keyed-lookup guard rather than the insert guard. Both guards are
   the package's contract.

No §3 issue is opened for these: 1–3 are 11's by the prompt, 4 is 09's, 5 is housekeeping, 6–7 are
measurements. The one issue opened is A9.4's `[08-plot-scripts-open-read-write-pools]`.

## State handed to the next prompt

- `HEAD` is `857ec73` on `dk-and-rpk-migration`, on top of `63f60e1`; planning and orchestration
  commits may land after it. No tracked change is left; the untracked entries of prompt 01's gate are
  not this campaign's.
- **The layer is gone.** `Datastore/object.py`, `Datastore/SQL/{ClientPool,Datastore,ProfileAgent,
  SerialPoolBroker,ShardedPool}.py` and `Datastore/SQL/ObjectFactories/base.py` are deleted;
  `Datastore/__init__.py` and `Datastore/SQL/__init__.py` are 0-byte files; the 17 factory modules
  and the 0-byte `Datastore/SQL/ObjectFactories/__init__.py` stay. ChamPBH imports `datastorekit`
  0.3.0 (`2f5417ea…`): `DatastoreObject` from `datastorekit`, `SQLAFactoryBase` from
  `datastorekit.SQL.factory_base`, `ShardedPool` and `ProfileAgent` from `datastorekit.SQL.<same>`,
  `require_version_serial` from `datastorekit.contract`. `config/version.py` holds `VERSION_LABEL`
  alone. DatastoreKit's script at a commit of this tree: outside `prompts/`, 0 layer imports, 0
  literals, 0 `import_module`; 0 imports of the moved names.
- **The registry and the pools.** `config.datastore` holds `factories` (27), `drop_groups` (3),
  `serial_batch_sizes` (23, no `LambdaCDM`) and `tables_to_drop(groups)`; it imports first, alone.
  `config.sharding.replicated_tables` has 17 names, no `LambdaCDM`; `inventory_config` is still
  defined there, imported by nothing (09 deletes it with the six `inventory()` hooks, which are
  still on the factories and are never called). The three drivers' pools are `main.py:1211-1227`,
  `plot_by_beta.py:1011-1025`, `plot_ScalarModel.py:1581-1595`, each passing `factories=` and
  `serial_batch_sizes=` from `config.datastore`; `main.py`'s passes `drop_tables=`, computed by
  `tables_to_drop` before `ray.init` (`main.py:101-109`; `ray.init` at `:112`).
- **`--inventory` exits 2 before Ray** (`main.py:91-99`), naming prompt 09. The old inventory
  block is deleted. 09 replaces the refusal and its test
  (`TestInventoryRefuses.test_inventory_exits_non_zero_and_creates_nothing` and
  `test_the_refusal_comes_before_ray_starts`, in `Datastore/tests/test_datastore_config.py`);
  `config/argument_parser.py`'s `--inventory` help and `--show-all` are 09's to make true again.
- **The harness.** `Datastore/tests/test_version_keyed_lookups._TempStoreCase` builds the package's
  undecorated actor inside ChamPBH's stand-in cluster: `open(label)` opens a real one-shard
  `ShardedPool` through `Datastore/tests/standin_pool.py`'s `open_pool(primary, shards=1,
  version_label=label, serial_batch_sizes=…)`, closes it, builds `standin_pool.DatastoreClass` on
  the shard file with the test's shared stand-in broker, notifies the broker, and calls
  `set_version`. Inserts with no serial lease from the broker (shown by a probe; no test does it
  yet). 09's test stores are built through it, or through `StandinCluster.open_pool` directly.
  `standin_pool.py` is the tag's file with ChamPBH's licence and provenance header and
  `open_pool`/`open_pool_output` opening ChamPBH's registry; re-copy it on a DatastoreKit upgrade.
- **The schema** of a 16-shard stand-in store under the package equals R0's snapshot outright
  (`RESULT: the schemas agree`; no difference). `out/08-standin-schema.json` is that store's
  snapshot; 10 compares its store with it.
- **Counts.** CosmologyModels 18, ComputeTargets 114 (61 + 53), **Datastore 76** (by module:
  `bbn_failure_lookup` 6, `compare_stores` 21, `datastore_config` 16, `first_bounce_round_trip` 5,
  `fixed_T_values_round_trip` 5, `package_provenance` 10, `scalarmodel_failure_reason` 4,
  `version_keyed_lookups` 9). `check_imports.py`: 163 names, 0 failures.
- **Issues.** `[05-config-datastore-cannot-be-imported-before-the-layer]` closed (board §4);
  `[08-plot-scripts-open-read-write-pools]` opened (board §3); `[00-two-files-are-not-black-clean]`
  narrowed to `CosmologyModels/LambdaCDM/Planck.py`. Index: 26 open, 3 on this board.
- **For 11:** the stale citations in `tools/compare_stores.py` (`:89-91`, `:108-110`), in
  `.documents/architecture-summary.md`'s database sections, and in the docstrings and comments listed
  in Observation 3; `CLAUDE.md`'s test commands.
- **The venv** is as 07 left it (`datastorekit` 0.3.0, `raypoolkit` 0.2.0); nothing was installed.
  No Ray was started; no store was opened but temporary ones.
- **Rollback** is `git revert` of `857ec73` alone; nothing in the venv (README §4.4). The
  installed `datastorekit` stays, and is inert once nothing imports it.
- **Nothing here has run under real Ray.** The package's pool, actors and broker in a real cluster,
  and a store built through them, are R2 (prompt 10), after 09.
