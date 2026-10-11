# Imports of the layer: ChamPBH (CPBH) at `52142d7`

Written by `docs/extraction/measure_client_imports.py`, reading through `git ls-tree` and `git show` only.

- **Repository:** `/Users/ds283/Documents/Code/ChamPBH`
- **Commit:** `52142d75aebe00855804e2daff4f63aee50ffc3e` (given as `52142d7`), 2026-10-07 Store each shard key under its own serial
- **Layer files** (9): `Datastore/__init__.py`, `Datastore/object.py`, `Datastore/SQL/__init__.py`, `Datastore/SQL/ClientPool.py`, `Datastore/SQL/Datastore.py`, `Datastore/SQL/ProfileAgent.py`, `Datastore/SQL/SerialPoolBroker.py`, `Datastore/SQL/ShardedPool.py`, `Datastore/SQL/ObjectFactories/base.py`
- **Layer files absent at this commit:** none
- **Factory package:** `Datastore/SQL/ObjectFactories/` (its `base.py` is the layer's)
- **Files read:** 196 tracked `.py` files outside the layer; 0 do not parse

## Totals

| What | Count |
|---|---|
| Import statements naming a layer module | **41** in 38 files |
| of which nested (not at module level) | 0 |
| String literals naming a layer module | 0 (0 exactly its name) |
| `import_module` / `__import__` calls naming a layer module | 0 |
| Import statements naming a factory module (not layer imports) | 1 (1 outside `Datastore/SQL/ObjectFactories/`) |
| Imports of names that move into the package | 4 |

## Layer imports, by group

| Group | Statements | Nested | Files |
|---|---|---|---|
| `(top level)` | 6 | 0 | 3 |
| `ComputeTargets/` | 3 | 0 | 3 |
| `CosmologyConcepts/` | 5 | 0 | 5 |
| `CosmologyModels/` | 1 | 0 | 1 |
| `Datastore/SQL/ObjectFactories/` | 17 | 0 | 17 |
| `Datastore/tests/` | 2 | 0 | 2 |
| `MetadataConcepts/` | 3 | 0 | 3 |
| `Quadrature/` | 1 | 0 | 1 |
| `RayTools/` | 1 | 0 | 1 |
| `config/` | 1 | 0 | 1 |
| `prompts/` | 1 | 0 | 1 |
| **total** | **41** | **0** | **38** |

## Layer imports, by module

A statement naming two layer modules is counted under each.

| Module | Statements | Names imported by `from` (statements) |
|---|---|---|
| `Datastore` | 13 | `DatastoreObject` 13 |
| `Datastore.SQL.Datastore` | 2 | `Datastore` 2 |
| `Datastore.SQL.ObjectFactories.base` | 17 | `SQLAFactoryBase` 17 |
| `Datastore.SQL.ProfileAgent` | 3 | `ProfileAgent` 3 |
| `Datastore.SQL.ShardedPool` | 6 | `ShardedPool` 6 |

## Every layer import

### `(top level)` (6)

| File:line | Nested | Module | Names |
|---|---|---|---|
| `main.py:47` | — | `Datastore.SQL.ProfileAgent` | `ProfileAgent` |
| `main.py:48` | — | `Datastore.SQL.ShardedPool` | `ShardedPool` |
| `plot_ScalarModel.py:41` | — | `Datastore.SQL.ProfileAgent` | `ProfileAgent` |
| `plot_ScalarModel.py:42` | — | `Datastore.SQL.ShardedPool` | `ShardedPool` |
| `plot_by_beta.py:35` | — | `Datastore.SQL.ProfileAgent` | `ProfileAgent` |
| `plot_by_beta.py:36` | — | `Datastore.SQL.ShardedPool` | `ShardedPool` |

### `ComputeTargets/` (3)

| File:line | Nested | Module | Names |
|---|---|---|---|
| `ComputeTargets/AdiabaticHistory.py:11` | — | `Datastore` | `DatastoreObject` |
| `ComputeTargets/BBNData.py:13` | — | `Datastore` | `DatastoreObject` |
| `ComputeTargets/ScalarModel.py:44` | — | `Datastore` | `DatastoreObject` |

### `CosmologyConcepts/` (5)

| File:line | Nested | Module | Names |
|---|---|---|---|
| `CosmologyConcepts/ConformalCouplings/AbstractCoupling.py:19` | — | `Datastore` | `DatastoreObject` |
| `CosmologyConcepts/DimensionfulQuantity.py:19` | — | `Datastore` | `DatastoreObject` |
| `CosmologyConcepts/DimensionlessQuantity.py:19` | — | `Datastore` | `DatastoreObject` |
| `CosmologyConcepts/Potentials/AbstractPotential.py:19` | — | `Datastore` | `DatastoreObject` |
| `CosmologyConcepts/redshift.py:19` | — | `Datastore` | `DatastoreObject` |

### `CosmologyModels/` (1)

| File:line | Nested | Module | Names |
|---|---|---|---|
| `CosmologyModels/base.py:18` | — | `Datastore` | `DatastoreObject` |

### `Datastore/SQL/ObjectFactories/` (17)

| File:line | Nested | Module | Names |
|---|---|---|---|
| `Datastore/SQL/ObjectFactories/AdiabaticHistory.py:28` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/BBNData.py:16` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/DimensionfulQuantity.py:20` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/DimensionlessQuantity.py:21` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/ExponentialCoupling.py:21` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/ExponentialPotential.py:21` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/InversePowerPotential.py:21` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/QCD_Cosmology.py:19` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/ReclinerPotential.py:21` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/ReflectingPotential.py:21` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/ScalarModel.py:39` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/Starobinsky.py:21` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/integration_metadata.py:18` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/redshift.py:24` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/store_tag.py:18` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/tolerance.py:20` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |
| `Datastore/SQL/ObjectFactories/version.py:18` | — | `Datastore.SQL.ObjectFactories.base` | `SQLAFactoryBase` |

### `Datastore/tests/` (2)

| File:line | Nested | Module | Names |
|---|---|---|---|
| `Datastore/tests/test_shard_key_assignment.py:40` | — | `Datastore.SQL.ShardedPool` | `ShardedPool` |
| `Datastore/tests/test_version_keyed_lookups.py:45` | — | `Datastore.SQL.Datastore` | `Datastore` |

### `MetadataConcepts/` (3)

| File:line | Nested | Module | Names |
|---|---|---|---|
| `MetadataConcepts/store_tag.py:16` | — | `Datastore` | `DatastoreObject` |
| `MetadataConcepts/tolerance.py:18` | — | `Datastore` | `DatastoreObject` |
| `MetadataConcepts/version.py:16` | — | `Datastore` | `DatastoreObject` |

### `Quadrature/` (1)

| File:line | Nested | Module | Names |
|---|---|---|---|
| `Quadrature/integration_metadata.py:18` | — | `Datastore` | `DatastoreObject` |

### `RayTools/` (1)

| File:line | Nested | Module | Names |
|---|---|---|---|
| `RayTools/RayWorkPool.py:23` | — | `Datastore.SQL.ShardedPool` | `ShardedPool` |

### `config/` (1)

| File:line | Nested | Module | Names |
|---|---|---|---|
| `config/model_list.py:19` | — | `Datastore.SQL.ShardedPool` | `ShardedPool` |

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
| **total** | **1** |

## Imports of names that move into the package

`config.version`: `VERSION_SERIAL_KEY`, `require_version_serial`.

| File:line | Module | Names |
|---|---|---|
| `Datastore/SQL/ObjectFactories/AdiabaticHistory.py:31` | `config.version` | `require_version_serial` |
| `Datastore/SQL/ObjectFactories/BBNData.py:20` | `config.version` | `require_version_serial` |
| `Datastore/SQL/ObjectFactories/ScalarModel.py:44` | `config.version` | `require_version_serial` |
| `Datastore/tests/test_version_keyed_lookups.py:350` | `config.version` | `VERSION_SERIAL_KEY` |

## Files that do not parse

None.
