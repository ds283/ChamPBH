# Imports of the layer: ChamPBH (CPBH) at `ee79455`

Written by `docs/extraction/measure_client_imports.py`, reading through `git ls-tree` and `git show` only.

- **Repository:** `/Users/ds283/Documents/Code/ChamPBH`
- **Commit:** `ee7945540bd6d8c29112edbfc42b1978d2d43d08` (given as `ee7945540bd6d8c29112edbfc42b1978d2d43d08`), 2026-10-13 Write the orchestrator note for dk-and-rpk-migration prompt 11
- **Layer files** (9): `Datastore/__init__.py`, `Datastore/object.py`, `Datastore/SQL/__init__.py`, `Datastore/SQL/ClientPool.py`, `Datastore/SQL/Datastore.py`, `Datastore/SQL/ProfileAgent.py`, `Datastore/SQL/SerialPoolBroker.py`, `Datastore/SQL/ShardedPool.py`, `Datastore/SQL/ObjectFactories/base.py`
- **Layer files absent at this commit:** `Datastore/object.py`, `Datastore/SQL/ClientPool.py`, `Datastore/SQL/Datastore.py`, `Datastore/SQL/ProfileAgent.py`, `Datastore/SQL/SerialPoolBroker.py`, `Datastore/SQL/ShardedPool.py`, `Datastore/SQL/ObjectFactories/base.py`
- **Factory package:** `Datastore/SQL/ObjectFactories/` (its `base.py` is the layer's)
- **Files read:** 204 tracked `.py` files outside the layer; 0 do not parse

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
