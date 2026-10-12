# Log 05 — move ChamPBH's data out of the layer

**Subject:** Move ChamPBH's data out of the layer
**Commit:** this commit, on branch `dk-and-rpk-migration`, on top of `891ae82` (its code equals `303324b`'s)
**Date:** 2026-10-12, by Claude Sonnet 5.5
**Result:** **COMPLETE.**
- M1: `config/datastore.py` added, holding `factories` (27 entries, 23 distinct classes),
  `drop_groups` (3 groups) and `serial_batch_sizes` (24 entries, `LambdaCDM` included).
- M2: `Datastore/SQL/Datastore.py` imports `factories as _factories` and `drop_groups as
  _drop_actions`; `Datastore/SQL/ClientPool.py` imports `serial_batch_sizes as
  _default_serial_batch_size`. `_drop_order` stays.
- M3: the seven vectorized gets in `main.py` pass `{"shard_key": x["shard_key"]}`.
- M4: `Datastore/tests/test_datastore_config.py`, 7 tests.
- The schema agrees with 04's tree and with R0's snapshot; the drivers' 162 imported names resolve;
  the suites are 18 / 114 / 73; four deliberate breakages caught. One §3 issue opened,
  `[05-config-datastore-cannot-be-imported-before-the-layer]`. No §4 stop condition was met.

## What shipped

`git diff --cached --name-only`, as committed:

```text
.documents/OPEN_ISSUES.md
Datastore/SQL/ClientPool.py
Datastore/SQL/Datastore.py
Datastore/tests/test_datastore_config.py
config/datastore.py
main.py
prompts/dk-and-rpk-migration/IMPLEMENTATION_STATE.md
prompts/dk-and-rpk-migration/logs/05-move-champbh-data-out-of-the-layer.md
```

The three diffs, `git diff` of the layer's two files and of `main.py` before staging (the removed
definitions are the ones captured below):

```diff
diff --git a/Datastore/SQL/ClientPool.py b/Datastore/SQL/ClientPool.py
index 90c0cc7..5266c3b 100644
--- a/Datastore/SQL/ClientPool.py
+++ b/Datastore/SQL/ClientPool.py
@@ -20,33 +20,7 @@ import ray
 from ray.actor import ActorHandle
 
 from Datastore.SQL.ProfileAgent import ProfileBatcher, ProfileBatchManager
-
-_default_serial_batch_size = {
-    "store_tag": 5,
-    "redshift": 200,
-    "tolerance": 5,
-    "beta_value": 200,
-    "M_value": 200,
-    "Lambda_value": 200,
-    "temperature": 200,
-    "phi_value": 200,
-    "pi_value": 200,
-    "ExponentialCoupling": 50,
-    "InversePowerPotential": 50,
-    "ExponentialPotential": 50,
-    "ReflectingPotential": 50,
-    "StarobinskyPotential": 50,
-    "ReclinerPotential": 50,
-    "LambdaCDM": 5,
-    "QCD_Cosmology": 5,
-    "IntegrationSolver": 6,
-    "ScalarModel": 20,
-    "ScalarModelValue": 250,
-    "AdiabaticHistory": 20,
-    "AdiabaticHistoryValue": 250,
-    "BBNData": 20,
-    "BBNDataValue": 250,
-}
+from config.datastore import serial_batch_sizes as _default_serial_batch_size
 
 
 class ClientPool:
diff --git a/Datastore/SQL/Datastore.py b/Datastore/SQL/Datastore.py
index 4443980..761a79a 100644
--- a/Datastore/SQL/Datastore.py
+++ b/Datastore/SQL/Datastore.py
@@ -24,64 +24,13 @@ import sqlalchemy as sqla
 from ray.actor import ActorHandle
 from sqlalchemy.exc import IntegrityError, SQLAlchemyError
 
-from CosmologyConcepts import (
-    beta_value,
-    Lambda_value,
-    M_value,
-    temperature,
-    phi_value,
-    pi_value,
-)
 from Datastore.SQL.ClientPool import SerialPoolManager, SerialLeaseManager
-from Datastore.SQL.ObjectFactories.AdiabaticHistory import (
-    sqla_AdiabaticHistoryFactory,
-    sqla_AdiabaticHistoryTagAssociation_factory,
-    sqla_AdiabaticHistoryValue_factory,
-)
-from Datastore.SQL.ObjectFactories.BBNData import (
-    sqla_BBNDataFactory,
-    sqla_BBNDataTagAssociation_factory,
-    sqla_BBNDataValue_factory,
-)
-from Datastore.SQL.ObjectFactories.DimensionfulQuantity import (
-    sqla_dimensionful_quantity_factory,
-)
-from Datastore.SQL.ObjectFactories.DimensionlessQuantity import (
-    sqla_dimensionless_quantity_factory,
-)
-from Datastore.SQL.ObjectFactories.ExponentialCoupling import (
-    sqla_ExponentialCoupling_factory,
-)
-from Datastore.SQL.ObjectFactories.ExponentialPotential import (
-    sqla_ExponentialPotential_factory,
-)
-from Datastore.SQL.ObjectFactories.InversePowerPotential import (
-    sqla_InversePowerPotential_factory,
-)
-from Datastore.SQL.ObjectFactories.QCD_Cosmology import sqla_QCDCosmology_factory
-from Datastore.SQL.ObjectFactories.ReclinerPotential import (
-    sqla_ReclinerPotential_factory,
-)
-from Datastore.SQL.ObjectFactories.ReflectingPotential import (
-    sqla_ReflectingPotential_factory,
-)
-from Datastore.SQL.ObjectFactories.ScalarModel import (
-    sqla_ScalarModelFactory,
-    sqla_ScalarModelTagAssociation_factory,
-    sqla_ScalarModelValue_factory,
-)
-from Datastore.SQL.ObjectFactories.Starobinsky import (
-    sqla_StarobinskyPotential_factory,
-)
 from Datastore.SQL.ObjectFactories.base import SQLAFactoryBase
-from Datastore.SQL.ObjectFactories.integration_metadata import (
-    sqla_IntegrationSolver_factory,
-)
-from Datastore.SQL.ObjectFactories.redshift import sqla_redshift_factory
-from Datastore.SQL.ObjectFactories.store_tag import sqla_store_tag_factory
-from Datastore.SQL.ObjectFactories.tolerance import sqla_tolerance_factory
-from Datastore.SQL.ObjectFactories.version import sqla_version_factory
 from Datastore.SQL.ProfileAgent import ProfileBatcher, ProfileBatchManager
+from config.datastore import (
+    drop_groups as _drop_actions,
+    factories as _factories,
+)
 from config.version import VERSION_SERIAL_KEY
 from utilities import WallclockTimer
 
@@ -91,49 +40,10 @@ VERSION_ID_LENGTH = 64
 PathType = Union[str, PathLike]
 
 
-_factories = {
-    "version": sqla_version_factory(),
-    "store_tag": sqla_store_tag_factory(),
-    "redshift": sqla_redshift_factory(),
-    "tolerance": sqla_tolerance_factory(),
-    "beta_value": sqla_dimensionless_quantity_factory(beta_value),
-    "M_value": sqla_dimensionful_quantity_factory(M_value),
-    "Lambda_value": sqla_dimensionful_quantity_factory(Lambda_value),
-    "temperature": sqla_dimensionful_quantity_factory(temperature),
-    "phi_value": sqla_dimensionful_quantity_factory(phi_value),
-    "pi_value": sqla_dimensionful_quantity_factory(pi_value),
-    "InversePowerPotential": sqla_InversePowerPotential_factory(),
-    "StarobinskyPotential": sqla_StarobinskyPotential_factory(),
-    "ExponentialPotential": sqla_ExponentialPotential_factory(),
-    "ReclinerPotential": sqla_ReclinerPotential_factory(),
-    "ReflectingPotential": sqla_ReflectingPotential_factory(),
-    "ExponentialCoupling": sqla_ExponentialCoupling_factory(),
-    "QCD_Cosmology": sqla_QCDCosmology_factory(),
-    "IntegrationSolver": sqla_IntegrationSolver_factory(),
-    "ScalarModel": sqla_ScalarModelFactory(),
-    "ScalarModel_tags": sqla_ScalarModelTagAssociation_factory(),
-    "ScalarModelValue": sqla_ScalarModelValue_factory(),
-    "AdiabaticHistory": sqla_AdiabaticHistoryFactory(),
-    "AdiabaticHistory_tags": sqla_AdiabaticHistoryTagAssociation_factory(),
-    "AdiabaticHistoryValue": sqla_AdiabaticHistoryValue_factory(),
-    "BBNData": sqla_BBNDataFactory(),
-    "BBNData_tags": sqla_BBNDataTagAssociation_factory(),
-    "BBNDataValue": sqla_BBNDataValue_factory(),
-}
-
 _FactoryMappingType = Mapping[str, SQLAFactoryBase]
 _TableMappingType = Mapping[str, sqla.Table]
 _InserterMappingType = Mapping[str, Callable]
 
-_drop_actions = {
-    "scalar-model": ["ScalarModel_tags", "ScalarModelValue", "ScalarModel"],
-    "adiabatic-history": [
-        "AdiabaticHistory_tags",
-        "AdiabaticHistoryValue",
-        "AdiabaticHistory",
-    ],
-    "bbn-data": ["BBNData_tags", "BBNDataValue", "BBNData"],
-}
 # should drop tables in a defined order, so that we do not violate foreign key constrints
 _drop_order = [
     "bbn-data",
diff --git a/main.py b/main.py
index 743c28d..e1817ef 100644
--- a/main.py
+++ b/main.py
@@ -202,7 +202,7 @@ def run_pipeline(
             pool,
             query_batch,
             task_builder=lambda x: pool.object_get_vectorized(
-                "ScalarModel", x["shard_key"], payload_data=x["payload"]
+                "ScalarModel", {"shard_key": x["shard_key"]}, payload_data=x["payload"]
             ),
             available_handler=None,
             compute_handler=None,
@@ -364,7 +364,7 @@ def run_pipeline(
             pool,
             model_query_batch,
             task_builder=lambda x: pool.object_get_vectorized(
-                "ScalarModel", x["shard_key"], payload_data=x["payload"]
+                "ScalarModel", {"shard_key": x["shard_key"]}, payload_data=x["payload"]
             ),
             available_handler=None,
             compute_handler=None,
@@ -414,7 +414,9 @@ def run_pipeline(
             pool,
             adiabatic_query_batch,
             task_builder=lambda x: pool.object_get_vectorized(
-                "AdiabaticHistory", x["shard_key"], payload_data=x["payload"]
+                "AdiabaticHistory",
+                {"shard_key": x["shard_key"]},
+                payload_data=x["payload"],
             ),
             available_handler=None,
             compute_handler=None,
@@ -482,7 +484,7 @@ def run_pipeline(
             pool,
             required_models_payload,
             task_builder=lambda x: pool.object_get_vectorized(
-                "ScalarModel", x["shard_key"], payload_data=x["payload"]
+                "ScalarModel", {"shard_key": x["shard_key"]}, payload_data=x["payload"]
             ),
             available_handler=None,
             compute_handler=None,
@@ -620,7 +622,7 @@ def run_pipeline(
             pool,
             model_query_batch,
             task_builder=lambda x: pool.object_get_vectorized(
-                "ScalarModel", x["shard_key"], payload_data=x["payload"]
+                "ScalarModel", {"shard_key": x["shard_key"]}, payload_data=x["payload"]
             ),
             available_handler=None,
             compute_handler=None,
@@ -672,7 +674,7 @@ def run_pipeline(
             pool,
             bbn_query_batch,
             task_builder=lambda x: pool.object_get_vectorized(
-                "BBNData", x["shard_key"], payload_data=x["payload"]
+                "BBNData", {"shard_key": x["shard_key"]}, payload_data=x["payload"]
             ),
             available_handler=None,
             compute_handler=None,
@@ -745,7 +747,7 @@ def run_pipeline(
             pool,
             required_models_payload,
             task_builder=lambda x: pool.object_get_vectorized(
-                "ScalarModel", x["shard_key"], payload_data=x["payload"]
+                "ScalarModel", {"shard_key": x["shard_key"]}, payload_data=x["payload"]
             ),
             available_handler=None,
             compute_handler=None,
```

`config/datastore.py` is the three definitions with the imports they need, under the new names,
and a docstring. `ast.dump` of each value equals the one at `HEAD` (script `ast_compare.py`):

```text
_factories -> factories ast.dump equal: True
_drop_actions -> drop_groups ast.dump equal: True
_default_serial_batch_size -> serial_batch_sizes ast.dump equal: True
```

`black` split one of the seven calls (`main.py:417-419`, the `AdiabaticHistory` one); the other six
stay on one line. So `main.py`'s lines after `:418` moved by +2.

## The captured data (M4.1), taken before the first edit

`HEAD` was `891ae82602268b1c3d65e28a42824cd763485543`, the working tree untouched. The script, `capture.py`, run from the
repository root with `PYTHONPATH=.`:

```python
"""Capture the registry, drop groups and batch sizes as the layer holds them (run from repo root)."""
import importlib
import pprint
import sys

sys.path.insert(0, ".")
import Datastore.SQL.Datastore  # noqa: F401  (binds the actor in the package)
import Datastore.SQL.ClientPool as cp

layer = importlib.import_module("Datastore.SQL.Datastore")

rows = []
for name, f in layer._factories.items():
    ot = getattr(f, "ObjectType", None)
    rows.append((name, type(f).__name__, None if ot is None else ot.__name__))
print("FACTORIES", len(rows), "distinct classes", len({r[1] for r in rows}))
pprint.pprint(rows, width=100, sort_dicts=False)
print("DROP_GROUPS", len(layer._drop_actions))
pprint.pprint(layer._drop_actions, width=100, sort_dicts=False)
print("SERIAL_BATCH_SIZES", len(cp._default_serial_batch_size))
pprint.pprint(cp._default_serial_batch_size, width=100, sort_dicts=False)
```

Its output at the unedited tree (`capture-before.txt`), which the test's literals are written from:

```text
FACTORIES 27 distinct classes 23
[('version', 'sqla_version_factory', None),
 ('store_tag', 'sqla_store_tag_factory', None),
 ('redshift', 'sqla_redshift_factory', None),
 ('tolerance', 'sqla_tolerance_factory', None),
 ('beta_value', 'sqla_dimensionless_quantity_factory', 'beta_value'),
 ('M_value', 'sqla_dimensionful_quantity_factory', 'M_value'),
 ('Lambda_value', 'sqla_dimensionful_quantity_factory', 'Lambda_value'),
 ('temperature', 'sqla_dimensionful_quantity_factory', 'temperature'),
 ('phi_value', 'sqla_dimensionful_quantity_factory', 'phi_value'),
 ('pi_value', 'sqla_dimensionful_quantity_factory', 'pi_value'),
 ('InversePowerPotential', 'sqla_InversePowerPotential_factory', None),
 ('StarobinskyPotential', 'sqla_StarobinskyPotential_factory', None),
 ('ExponentialPotential', 'sqla_ExponentialPotential_factory', None),
 ('ReclinerPotential', 'sqla_ReclinerPotential_factory', None),
 ('ReflectingPotential', 'sqla_ReflectingPotential_factory', None),
 ('ExponentialCoupling', 'sqla_ExponentialCoupling_factory', None),
 ('QCD_Cosmology', 'sqla_QCDCosmology_factory', None),
 ('IntegrationSolver', 'sqla_IntegrationSolver_factory', None),
 ('ScalarModel', 'sqla_ScalarModelFactory', None),
 ('ScalarModel_tags', 'sqla_ScalarModelTagAssociation_factory', None),
 ('ScalarModelValue', 'sqla_ScalarModelValue_factory', None),
 ('AdiabaticHistory', 'sqla_AdiabaticHistoryFactory', None),
 ('AdiabaticHistory_tags', 'sqla_AdiabaticHistoryTagAssociation_factory', None),
 ('AdiabaticHistoryValue', 'sqla_AdiabaticHistoryValue_factory', None),
 ('BBNData', 'sqla_BBNDataFactory', None),
 ('BBNData_tags', 'sqla_BBNDataTagAssociation_factory', None),
 ('BBNDataValue', 'sqla_BBNDataValue_factory', None)]
DROP_GROUPS 3
{'scalar-model': ['ScalarModel_tags', 'ScalarModelValue', 'ScalarModel'],
 'adiabatic-history': ['AdiabaticHistory_tags', 'AdiabaticHistoryValue', 'AdiabaticHistory'],
 'bbn-data': ['BBNData_tags', 'BBNDataValue', 'BBNData']}
SERIAL_BATCH_SIZES 24
{'store_tag': 5,
 'redshift': 200,
 'tolerance': 5,
 'beta_value': 200,
 'M_value': 200,
 'Lambda_value': 200,
 'temperature': 200,
 'phi_value': 200,
 'pi_value': 200,
 'ExponentialCoupling': 50,
 'InversePowerPotential': 50,
 'ExponentialPotential': 50,
 'ReflectingPotential': 50,
 'StarobinskyPotential': 50,
 'ReclinerPotential': 50,
 'LambdaCDM': 5,
 'QCD_Cosmology': 5,
 'IntegrationSolver': 6,
 'ScalarModel': 20,
 'ScalarModelValue': 250,
 'AdiabaticHistory': 20,
 'AdiabaticHistoryValue': 250,
 'BBNData': 20,
 'BBNDataValue': 250}
```

After the move the same script prints byte-identical output (`cmp` silent; the layer's old names
are now aliases of the new objects).

## The `git grep`s (§2.1.4)

```text
$ git grep -n '_factories\|_drop_actions\|_drop_order\|_default_serial_batch_size' -- ':!prompts' ':!.documents'
```
```text
Datastore/SQL/ClientPool.py:24:_default_serial_batch_size = {
Datastore/SQL/ClientPool.py:167:                default_batch_size=_default_serial_batch_size[table],
Datastore/SQL/Datastore.py:94:_factories = {
Datastore/SQL/Datastore.py:128:_drop_actions = {
Datastore/SQL/Datastore.py:138:_drop_order = [
Datastore/SQL/Datastore.py:186:        self._factories: _FactoryMappingType = {}
Datastore/SQL/Datastore.py:187:        self.register_factories(_factories)
Datastore/SQL/Datastore.py:208:            self._drop_actions(drop_actions)
Datastore/SQL/Datastore.py:260:    def register_factories(self, factories: _FactoryMappingType):
Datastore/SQL/Datastore.py:274:            if cls_name in self._factories:
Datastore/SQL/Datastore.py:279:            self._factories[cls_name] = factory
Datastore/SQL/Datastore.py:285:        for cls_name, factory in self._factories.items():
Datastore/SQL/Datastore.py:404:        if cls_name not in self._factories:
Datastore/SQL/Datastore.py:409:    def _drop_actions(self, actions):
Datastore/SQL/Datastore.py:418:        for action in _drop_order:
Datastore/SQL/Datastore.py:421:                drop_list = _drop_actions[action]
Datastore/SQL/Datastore.py:446:                factory = self._factories[cls_name]
Datastore/SQL/Datastore.py:490:            factory = self._factories[cls_name]
Datastore/SQL/Datastore.py:570:            factory = self._factories[cls_name]
Datastore/SQL/Datastore.py:620:                        factory = self._factories[cls_name]
Datastore/SQL/Datastore.py:730:                    factory = self._factories[cls_name]
Datastore/SQL/Datastore.py:776:            factory = self._factories[class_name]
Datastore/SQL/Datastore.py:834:            factory = self._factories[class_name]
Datastore/tests/test_compare_stores.py:713:        from Datastore.SQL.Datastore import _factories as factories
Datastore/tests/test_scalarmodel_failure_reason.py:89:    factory = store._factories["ScalarModel"]
Datastore/tests/test_version_keyed_lookups.py:336:                factory = store._factories[cls_name]
Datastore/tests/test_version_keyed_lookups.py:389:        store._factories = {"Bad": _BadFactory()}
config/argument_parser.py:33:allowed_drop_actions = ["scalar-model", "adiabatic-history", "bbn-data"]
config/argument_parser.py:267:        choices=allowed_drop_actions,
main.py:53:from config.argument_parser import create_argument_parser, allowed_drop_actions
main.py:97:specified_drop_actions = [x.lower() for x in args.drop]
main.py:98:drop_actions = [x for x in specified_drop_actions if x in allowed_drop_actions]
```

32 lines, classified:
- module-level definitions and uses §1 names: `Datastore.py:94` (`_factories`), `:128`
  (`_drop_actions`), `:138` (`_drop_order`), `:187` (`register_factories(_factories)`), `:418`
  (`_drop_order`), `:421` (`_drop_actions[action]`); `ClientPool.py:24` and `:167`;
  `Datastore/tests/test_compare_stores.py:713`.
- the actor's **instance attribute** `self._factories` (`Datastore.py:186`, `:274`, `:279`, `:285`,
  `:404`, `:446`, `:490`, `:570`, `:620`, `:730`, `:776`, `:834`) and the tests' `store._factories`
  (`test_scalarmodel_failure_reason.py:89`, `test_version_keyed_lookups.py:336`, `:389`): filled by
  `register_factories`, not the module's name.
- `register_factories` (`Datastore.py:260`), a method.
- **`self._drop_actions(...)`** (`Datastore.py:208`) and `def _drop_actions(self, actions)`
  (`:409`): an actor method of the same name as the module-level dict. The method body reads the
  module global (`:421`); the alias import restores that global under its old name, so the method
  is unaffected and nothing is renamed.
- `allowed_drop_actions` (`config/argument_parser.py:33`, `:267`; `main.py:53`, `:97`, `:98`),
  which prompt §2.2 leaves alone.

None is a stop.

```text
$ git grep -n object_get_vectorized -- '*.py' ':!prompts'
```
```text
Datastore/SQL/ShardedPool.py:581:    def object_get_vectorized(self, ObjectClass, shard_key, payload_data):
main.py:204:            task_builder=lambda x: pool.object_get_vectorized(
main.py:366:            task_builder=lambda x: pool.object_get_vectorized(
main.py:416:            task_builder=lambda x: pool.object_get_vectorized(
main.py:484:            task_builder=lambda x: pool.object_get_vectorized(
main.py:622:            task_builder=lambda x: pool.object_get_vectorized(
main.py:674:            task_builder=lambda x: pool.object_get_vectorized(
main.py:747:            task_builder=lambda x: pool.object_get_vectorized(
```

The definition and seven calls, all in `main.py`, none in the plot scripts or `tools/`. Each passed
`x["shard_key"]` on the following line (`:205`, `:367`, `:417`, `:485`, `:623`, `:675`, `:748`).
Accounted for by §1.

## Deviations from the prompt

- **The layer's imports that only the registry used leave it, `CosmologyConcepts` included**;
  `SQLAFactoryBase` (used by `_FactoryMappingType`), `ClientPool`, `ProfileAgent`, `config.version`
  and `utilities` stay. §1 M2 says "any it still uses stay", and does not list them. *IMPLEMENTATION
  CHOICE (the reading of M2).*
- **The import of the two names in `Datastore.py` is a parenthesised two-line form** (`from
  config.datastore import (drop_groups as _drop_actions, factories as _factories,)`), which `black`
  keeps because of the trailing comma. *IMPLEMENTATION CHOICE.*
- **The new module has seven tests, for the prompt's three numbered items**: M4.1 is three tests
  (registry, drop groups, batch sizes), M4.2 three (one per object), M4.3 one.
  *IMPLEMENTATION CHOICE.*
- **The tests find the layer module by `importlib.import_module("Datastore.SQL.Datastore")`**, as
  `import Datastore.SQL.Datastore as m` binds the actor (prompt §2.3.1). *STRUCTURALLY REQUIRED.*
- **`--schema` takes the store, `--against` the snapshot file**, not `--schema <yours>` as a JSON
  (the orchestrator's note). *STRUCTURALLY REQUIRED.*
- No `UNINTENDED DRIFT` known.

## Verification performed

**Baseline and counts.** `HEAD` `891ae82602268b1c3d65e28a42824cd763485543`. Before the first edit I did not re-run the suites;
the baselines are 04's (18 / 114 / 66), which the orchestrator re-measured at `0f1c25b`. After:

Load at the start of the run (09:10, `uptime`): 43.66 / 108.02 / 85.35 on 10 cores (macOS system
processes); at the end (09:18): 43.95 / 49.99 / 62.89. Each suite ran in the foreground from the
repository root, ComputeTargets in its two pattern groups, output to a file in the scratch.

| Suite | 04 | Now | Verdict |
|---|---|---|---|
| `CosmologyModels/tests` | 18 | 18 (`Ran 18 tests in 123.740s`) | `OK` |
| `ComputeTargets/tests`, `-p 'test_[a-f]*.py'` | 61 | 61 (`Ran 61 tests in 135.091s`) | `OK` |
| `ComputeTargets/tests`, `-p 'test_[g-z]*.py'` | 53 | 53 (`Ran 53 tests in 194.379s`) | `OK` |
| `Datastore/tests` | 66 | 73 (`Ran 73 tests in 5.438s`) | `OK` |

Datastore's 73 is 66 plus the 7 of `test_datastore_config.py`. No existing test failed, none was
edited. `test_compare_stores` alone: 21 OK.

**The schema does not change (§2.3.1).** `build_store.py`, run from the repository root with
`PYTHONPATH=.`, at the unedited tree (`store-before`) and at the edited one (`store-after`):

```python
"""Build a Ray-free one-shard store with the undecorated actor, and snapshot it. Args: <outdir>."""
import importlib
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, ".")
from Datastore.SQL.Datastore import Datastore as Actor  # the @ray.remote class

out = Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)
shard = out / "store-shard0000.db"
cls = Actor.__ray_actor_class__
store = cls(version_label="2026.6.0", db_name=str(shard))
store._engine.dispose()
con = sqlite3.connect(out / "store.db")
con.execute("CREATE TABLE shards (serial INTEGER PRIMARY KEY, filename TEXT)")
con.execute("INSERT INTO shards VALUES (0, ?)", (shard.name,))
con.commit()
con.close()
import ray

print("built", out, "ray.is_initialized() =", ray.is_initialized())
```

```text
$ PYTHONPATH=. ./venv/bin/python build_store.py <scratch>/store-before
built <scratch>/store-before ray.is_initialized() = False
$ PYTHONPATH=. ./venv/bin/python build_store.py <scratch>/store-after
built <scratch>/store-after ray.is_initialized() = False
$ ./venv/bin/python tools/compare_stores.py --snapshot <scratch>/store-before/store.db --out <scratch>/05-schema-before.json
$ ./venv/bin/python tools/compare_stores.py --snapshot <scratch>/store-after/store.db --out <scratch>/05-schema-after.json
$ ./venv/bin/python tools/compare_stores.py --schema <scratch>/store-after/store.db --against <scratch>/05-schema-before.json
RESULT: the schemas agree
$ ./venv/bin/python tools/compare_stores.py --schema <scratch>/store-after/store.db --against .documents/dk-and-rpk-migration/out/02-reference-schema.json
RESULT: the schemas agree
```

`ray.is_initialized()` is `False` at the end of each build. The "before" snapshot was also compared
with R0's (`RESULT: the schemas agree`) before any edit.

**The import-order hazard (§2.4).** Measured at this tree, in `bash`, from the repository root with
`PYTHONPATH=.`:

```text
$ python -c "import config.datastore"   -> exit 1
ImportError: cannot import name 'serial_batch_sizes' from partially initialized module 'config.datastore' (most likely due to a circular import) (/Users/ds283/Documents/Code/ChamPBH/config/datastore.p
$ python -c "import config.sharding, config.datastore"   -> exit 1
ImportError: cannot import name 'serial_batch_sizes' from partially initialized module 'config.datastore' (most likely due to a circular import) (/Users/ds283/Documents/Code/ChamPBH/config/datastore.p
$ python -c "import Datastore.SQL.Datastore, config.datastore"   -> exit 0
$ python -c "import Datastore.SQL.ClientPool, config.datastore"   -> exit 0
$ python -c "import Datastore.SQL.ShardedPool, config.datastore"   -> exit 0
```

The failing name is `serial_batch_sizes`, not `factories`: `Datastore.py` imports `ClientPool`
before its own `config.datastore` import, and `ClientPool` meets the partial module first.

`test_compare_stores._registry()` (`Datastore/tests/test_compare_stores.py:707-714`) tries
`from config.datastore import factories` first and falls back to the layer on `ImportError`. Run
first in a process that has not imported the layer (`registry_probe.py`):

```text
first attempt: ImportError: cannot import name 'serial_batch_sizes' from partially initialized mod
fallback: layer's _factories is config.datastore.factories: True
```

so its first attempt meets the cycle, and its fallback loads the layer, which returns
`config.datastore.factories` itself. `python -m unittest Datastore.tests.test_compare_stores`
alone: 21 tests, OK. This is not a §4 stop (it is the fresh-import case §2.4 names) and the test is
not edited (README §5 rule 7).

**The drivers' imports resolve (§2.3.3).** `check_imports.py` is log 04's, unchanged, run from the
repository root with `./venv/bin/python`:

```text
checked 162 imported names; failures 0; ray.is_initialized() = False
```

(162 lines `ok`, 0 containing `FAIL`.) `./venv/bin/python -m py_compile main.py`: exit 0.

**`black`.** `./venv/bin/black --check main.py Datastore/SQL/Datastore.py Datastore/SQL/ClientPool.py config/datastore.py Datastore/tests/test_datastore_config.py`:

```text
All done! ✨ 🍰 ✨
5 files would be left unchanged.
```

**The new tests**, `PYTHONPATH=. ./venv/bin/python -m unittest -v Datastore.tests.test_datastore_config`:

```text
test_drop_groups_literal_and_ordered (Datastore.tests.test_datastore_config.TestConfigDatastoreData.test_drop_groups_literal_and_ordered) ... ok
test_factories_registry (Datastore.tests.test_datastore_config.TestConfigDatastoreData.test_factories_registry) ... ok
test_serial_batch_sizes_literal_and_ordered (Datastore.tests.test_datastore_config.TestConfigDatastoreData.test_serial_batch_sizes_literal_and_ordered) ... ok
test_vectorized_gets_pass_a_mapping (Datastore.tests.test_datastore_config.TestDriversPassShardKeyMapping.test_vectorized_gets_pass_a_mapping) ... ok
test_client_pool_batch_sizes_is_config_serial_batch_sizes (Datastore.tests.test_datastore_config.TestLayerReadsConfig.test_client_pool_batch_sizes_is_config_serial_batch_sizes) ... ok
test_layer_drop_actions_is_config_drop_groups (Datastore.tests.test_datastore_config.TestLayerReadsConfig.test_layer_drop_actions_is_config_drop_groups) ... ok
test_layer_registry_is_config_registry (Datastore.tests.test_datastore_config.TestLayerReadsConfig.test_layer_registry_is_config_registry) ... ok

----------------------------------------------------------------------
Ran 7 tests in 0.058s

OK
```

## The deliberate-breakage record

Each applied to the working tree, run with `PYTHONPATH=. ./venv/bin/python -m unittest
Datastore.tests.test_datastore_config`, and reverted (`cmp` against a copy confirms each file was
restored). Each diff replays from a clean tree at this commit with `git apply`, and reverses with
`git apply -R`; `git apply --check` was run for both directions.

### Breakage 1 — One vectorized get reverted to the bare key

Caught by `TestDriversPassShardKeyMapping.test_vectorized_gets_pass_a_mapping` fails: `main.py:676: shard key is not a dict literal`. 1 failure.

```diff
--- a/main.py
+++ b/main.py
@@ -674,7 +674,7 @@
             pool,
             bbn_query_batch,
             task_builder=lambda x: pool.object_get_vectorized(
-                "BBNData", {"shard_key": x["shard_key"]}, payload_data=x["payload"]
+                "BBNData", x["shard_key"], payload_data=x["payload"]
             ),
             available_handler=None,
             compute_handler=None,
```

### Breakage 2 — `serial_batch_sizes`: `ScalarModel` 20 to 21

Caught by `TestConfigDatastoreData.test_serial_batch_sizes_literal_and_ordered` fails. 1 failure.

```diff
--- a/config/datastore.py
+++ b/config/datastore.py
@@ -154,7 +154,7 @@
     "LambdaCDM": 5,
     "QCD_Cosmology": 5,
     "IntegrationSolver": 6,
-    "ScalarModel": 20,
+    "ScalarModel": 21,
     "ScalarModelValue": 250,
     "AdiabaticHistory": 20,
     "AdiabaticHistoryValue": 250,
```

### Breakage 3 — A registry entry given another quantity type

Caught by `TestConfigDatastoreData.test_factories_registry` fails (`('M_value', ..., 'Lambda_value')` against `'M_value'`). 1 failure.

```diff
--- a/config/datastore.py
+++ b/config/datastore.py
@@ -101,7 +101,7 @@
     "redshift": sqla_redshift_factory(),
     "tolerance": sqla_tolerance_factory(),
     "beta_value": sqla_dimensionless_quantity_factory(beta_value),
-    "M_value": sqla_dimensionful_quantity_factory(M_value),
+    "M_value": sqla_dimensionful_quantity_factory(Lambda_value),
     "Lambda_value": sqla_dimensionful_quantity_factory(Lambda_value),
     "temperature": sqla_dimensionful_quantity_factory(temperature),
     "phi_value": sqla_dimensionful_quantity_factory(phi_value),
```

### Breakage 4 — `ClientPool.py`'s import replaced by a literal copy of the dict

Caught by `TestLayerReadsConfig.test_client_pool_batch_sizes_is_config_serial_batch_sizes` fails (`... is not ...`). **Only that test fails** (`FAILED (failures=1)`): the three data tests still pass, which is why the identity test exists.

```diff
--- a/Datastore/SQL/ClientPool.py
+++ b/Datastore/SQL/ClientPool.py
@@ -20,7 +20,32 @@
 from ray.actor import ActorHandle
 
 from Datastore.SQL.ProfileAgent import ProfileBatcher, ProfileBatchManager
-from config.datastore import serial_batch_sizes as _default_serial_batch_size
+_default_serial_batch_size = {
+    "store_tag": 5,
+    "redshift": 200,
+    "tolerance": 5,
+    "beta_value": 200,
+    "M_value": 200,
+    "Lambda_value": 200,
+    "temperature": 200,
+    "phi_value": 200,
+    "pi_value": 200,
+    "ExponentialCoupling": 50,
+    "InversePowerPotential": 50,
+    "ExponentialPotential": 50,
+    "ReflectingPotential": 50,
+    "StarobinskyPotential": 50,
+    "ReclinerPotential": 50,
+    "LambdaCDM": 5,
+    "QCD_Cosmology": 5,
+    "IntegrationSolver": 6,
+    "ScalarModel": 20,
+    "ScalarModelValue": 250,
+    "AdiabaticHistory": 20,
+    "AdiabaticHistoryValue": 250,
+    "BBNData": 20,
+    "BBNDataValue": 250,
+}
 
 
 class ClientPool:
```

## Observations not acted on

1. **`test_compare_stores._registry()` silently falls back** (see the import-order measurement). The
   `ImportError` it catches is the cycle; it would equally hide a real `ImportError` in
   `config.datastore`. Recorded in the §3 issue; not edited (README §5 rule 7).
2. **`config/argument_parser.py`'s `allowed_drop_actions`** repeats the three group names; it is
   08's (U4).
3. **`_drop_order`'s comment** (`Datastore.py`, "should drop tables in a defined order") now sits
   directly after the type aliases; unchanged, and deleted with the layer at 08.
4. **DatastoreKit's checklist has a fourth addendum (`v0.3.0`)**, noted by the orchestrator. It
   changes nothing for 05, which runs on ChamPBH's own layer; the pin is 07's, and the choice is
   the user's. Read, not acted on.
5. **`config/datastore.py` has no `tables_to_drop`**; that is 08's (U4), as the prompt says.

## State handed to the next prompt

- `HEAD` is this commit on `dk-and-rpk-migration`; planning and orchestration commits may land
  after it. The tree holds no tracked change; the untracked entries of prompt 01's gate are not
  this campaign's.
- **Code.** `config/datastore.py` holds `factories`, `drop_groups` and `serial_batch_sizes`. The old
  layer reads them under the old names (`_factories`, `_drop_actions`,
  `_default_serial_batch_size`), as the very same objects. `_drop_order` is still in the layer.
  `main.py`'s seven vectorized gets pass `{"shard_key": x["shard_key"]}`.
- **`main.py` lines moved by +2 after `:418`.** The `RayWorkPool(` sites are `:201`, `:284`,
  `:363`, `:413`, `:483`, `:543`, `:621`, `:673`, `:746`, `:830`; the compute sites are `:284`, `:543`, `:830` with handlers at `:288`,
  `:547`, `:834`; `with ShardedPool(` is `:1348`.
- **The schema** from the layer at this tree agrees with 04's tree and with R0's
  `02-reference-schema.json` (27 client tables).
- **Counts.** CosmologyModels 18, ComputeTargets 114 (61 + 53), Datastore 73 (66 + 7).
- **The venv** is as 04 left it: `raypoolkit` 0.2.0, no `datastorekit`. Nothing was installed.
  No Ray was started and no store was opened but the temporary ones.
- **One issue opened**, `[05-config-datastore-cannot-be-imported-before-the-layer]`: 08 removes the
  cycle (P6) and includes `python -c "import config.datastore"` from the root in its acceptance.
- **Rollback** is `git revert` of this commit alone; nothing in the venv changes.
- **06** runs R1 at this tree: the rehearsal is held to `RESULT: equal` against `02-reference.db`
  (README §4.1, §4.3).
