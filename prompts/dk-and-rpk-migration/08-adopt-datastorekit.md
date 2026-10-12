# Prompt 08 — adopt `datastorekit`

**Campaign:** [`README.md`](README.md) · **Board items:** **A1**–**A9** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-12, with prompts 07, 09 and 10, after 06's review (`fa874a5`). Measured
read-only that day at `fa874a5`; your own measurement at your `HEAD` governs (§2.1).
**Gate:**
- 07 landed and reviewed (board §1): the venv holds `datastorekit` 0.3.0 at `2f5417ea…` and
  `raypoolkit` 0.2.0 at `eaef0ce6…`, neither editable;
- on branch `dk-and-rpk-migration`, no tracked change in `git status` (the untracked entries listed
  in prompt 01's gate are expected, and are not yours);
- no ChamPBH Ray process is running (`ray_cluster.sh status`). You start none.

**Closes:** `[05-config-datastore-cannot-be-imported-before-the-layer]`. **Narrows:**
`[00-two-files-are-not-black-clean]` (the `review-remediation` board). **Opens:**
`[08-plot-scripts-open-read-write-pools]` (P11), and anything else you find.
**Recommended model:** **Opus**. One commit that cannot be split: nine files go, about forty
imports move, three pools and a test harness are rebuilt on the package, and the drop policy is
new. Every part is measured, and the harness is the hard part.

**Read first:**

1. `CLAUDE.md`, in full ("Repository mechanics" is where this prompt's P14 edit goes).
2. [`README.md`](README.md): **§0.1** (all seven items), §0.4, §2.2 (**08's charter**, as amended
   2026-10-12 when this prompt was written), §4, §5, and §6.2–§6.3 (P6, P8, P9, P11–P14).
3. DatastoreKit **at the tag**, through `git -C /Users/ds283/Documents/Code/DatastoreKit show
   v0.3.0:<path>` (its working tree is ahead; the tag governs). **Read; do not edit anything there**
   (README §5 rule 12):
   - `docs/adoption/champbh.md` items 2–8 and its four addenda, and `docs/adoption/README.md` §2
     (the module map) and §3 (what the wheel does not ship);
   - `docs/client-contract.md` §1 (the constructor), §3 (the hooks), §5 (the layer's own tables),
     §7, §8 (keyed lookups) and §9.5;
   - `datastorekit/SQL/ShardedPool.py:106-125` (the constructor), `:825-843` (the drop refusal);
     `datastorekit/SQL/Datastore.py:39-55` (the actor), `:148-188` (`set_version`,
     `set_lookup_version`), `:778-786` (the insert refused before the version);
     `datastorekit/SQL/schema.py:359-414` (`dependent_tables`);
   - **`datastorekit/tests/standin_pool.py`** (485 lines) and
     **`datastorekit/tests/test_version_keyed_lookups.py:105-145` and `:309-420`** (`_StandinCase`
     and `TestTheActor`): the worked example of a package actor built without Ray.
4. SecondaryGWKit's `config/datastore.py:194-209` (`tables_to_drop`), the model for U4; read at
   `/Users/ds283/Documents/Code/SecondaryGWKit`, not imported.
5. Prompt 05's log, and the board's §3 entry for `[05-config-datastore-cannot-be-imported-before-the-layer]`.

---

## 1. What is wanted

ChamPBH's own layer is deleted and the package takes its place. The factories, their tables and
everything a build stores stay as they are (README §5 rule 9); 10 proves that by a rehearsal. This
prompt proves what can be proved without Ray: the tree imports, the suites pass on the package's
actor, the schema is the one R0 measured but for what the package adds, and the drop policy refuses
what it should.

**The facts this prompt was written against** (at `fa874a5`; §2.1 re-measures):
- DatastoreKit's script (`docs/extraction/measure_client_imports.py cpbh --commit fa874a5`, from
  its root with its own `./venv/bin/python`) counts **43 layer imports in 39 files; outside
  `prompts/`, 42 in 38**, 1 of them nested, 1 `import_module` call naming a layer module, 0 string
  literals:
  - `DatastoreObject` from `Datastore`: 13 (`ComputeTargets/` 3, `CosmologyConcepts/` 5,
    `CosmologyModels/base.py:18`, `MetadataConcepts/` 3, `Quadrature/integration_metadata.py:18`);
  - `SQLAFactoryBase` from `Datastore.SQL.ObjectFactories.base`: 17, one per factory module;
  - `ShardedPool`: 5 (`main.py:48`, `plot_ScalarModel.py:42`, `plot_by_beta.py:36`,
    `config/model_list.py:19`, a type hint, and `Datastore/tests/test_shard_key_assignment.py:40`);
  - `ProfileAgent`: 3 (`main.py:47`, `plot_ScalarModel.py:41`, `plot_by_beta.py:35`);
  - the actor and the layer's names, in tests: `test_version_keyed_lookups.py:45`;
    `test_datastore_config.py:32` (`ClientPool`), `:33` (the actor), `:39` (the `import_module`);
    `test_compare_stores.py:713` (nested, in `_registry()`'s fallback);
  - `prompts/run-integrity/planning-probes/datastore_version_probe.py:18`, history (README §1), which
    stays.
- The two names that move to `datastorekit.contract` (P9): `require_version_serial`, imported by
  `Datastore/SQL/ObjectFactories/{AdiabaticHistory.py:31,BBNData.py:20,ScalarModel.py:44}`;
  `VERSION_SERIAL_KEY`, imported by the layer (`Datastore/SQL/Datastore.py:34`) and by test d2
  (`test_version_keyed_lookups.py:350`). Both are defined at `config/version.py:55-71`, beside
  `VERSION_LABEL` (`:50`), which stays.
- The pools: `main.py:1348-1363`, `plot_by_beta.py:1011-1024`, `plot_ScalarModel.py:1581-1594`.
  Each passes `inventory_config=`, and `main.py`'s also `drop_actions=`; none passes `factories=`.
  The package's constructor is `datastorekit/SQL/ShardedPool.py:106-125`: the same names but
  `drop_tables` for `drop_actions`, no `inventory_config`, a `read_only` flag, and keyword-only
  `factories` (required) and `serial_batch_sizes`.
- `config/datastore.py`: `factories` `:99-127` (27 instances), `drop_groups` `:129-137`,
  `serial_batch_sizes` `:139-164` with `"LambdaCDM": 5` at `:155`; its docstring (`:16-41`) describes
  the old layer's aliases and the import cycle. `config/sharding.py:18-37`, `replicated_tables`, has
  `"LambdaCDM"` at `:34`. `_drop_order` is still the layer's (`Datastore/SQL/Datastore.py:48-52`).
- `--drop`: `config/argument_parser.py:33` (`allowed_drop_actions`), `:262-270`; `main.py:97-98`
  turns it into `drop_actions`, **after** `ray.init` at `:95`. `AdiabaticHistory` and `BBNData` name
  `ScalarModel.serial` (`ObjectFactories/AdiabaticHistory.py:107`, `BBNData.py:94`).
- `--inventory`: `main.py:1367-1368` dispatches to `inventory()` inside the pool; the block
  `main.py:1196-1344` (four `pool.inventory(` calls at `:1198`, `:1225`, `:1254`, `:1288`, and their
  helpers) serves it alone.
- **The harness** is `Datastore/tests/test_version_keyed_lookups.py`'s `_TempStoreCase`
  (`:187-201`): `open(label)` builds `DS(version_label=label, db_name=self.db)` (`:199`), with
  `DS = Datastore.__ray_actor_class__` (`:47`), and relies on ChamPBH's actor inserting the version
  row and leasing serials with no broker. Test a reads `store._version.store_id` (`:216`), which the
  package's actor does not have (it keeps `_version_serial`). Its importers:
  `Datastore/tests/test_bbn_failure_lookup.py:38-45` (6 tests),
  `test_first_bounce_round_trip.py:45-58` (5), `test_fixed_T_values_round_trip.py:47-52` (5),
  `test_scalarmodel_failure_reason.py:39-53` (4), and
  `ComputeTargets/tests/test_foreign_bbn_provenance.py:49-56` (6).
- **What the package's actor needs that ChamPBH's did not** (`datastorekit/SQL/Datastore.py`):
  `replicated_tables` (positional) and `factories=` (keyword-only); no version row of its own; a
  versioned insert refused until `set_version` (`:778-786`), a keyed lookup until `set_version` or
  `set_lookup_version`; and **a serial broker**: with none, `lease_serial` returns `None` and every
  insert into a table with a serial raises (`datastorekit/SQL/ClientPool.py:121-123`, `:188-198`).
  The package's own tests supply one through `tests/standin_pool.py`, which **the wheel does not
  ship** (adoption README §3: "a client whose tests use any of these keeps its own copy").
- **Tests the charter did not name**, found at `fa874a5`:
  - `Datastore/tests/test_datastore_config.py` (7 tests) imports the layer at module level (`:32`,
    `:33`, `:39`), so the whole module fails once the layer is gone. Its `TestLayerReadsConfig`
    (`:142`), three tests, asserts that the layer's names are `config.datastore`'s objects: with no
    layer, there is nothing for them to test. `EXPECTED_SERIAL_BATCH_SIZES` holds `"LambdaCDM": 5`
    (`:99`), which P8 removes.
  - `Datastore/tests/test_compare_stores.py`'s `_registry()` (`:707-714`) falls back to the layer
    when `config.datastore` cannot be imported, which a fresh process does today (the cycle).
- Suites at `fa874a5`: CosmologyModels 18, ComputeTargets 114 (61 + 53), Datastore **73**:
  `bbn_failure_lookup` 6, `compare_stores` 21, `datastore_config` 7, `first_bounce_round_trip` 5,
  `fixed_T_values_round_trip` 5, `package_provenance` 10, `scalarmodel_failure_reason` 4,
  `shard_key_assignment` 4, `version_keyed_lookups` 11.

**The items.**

- **A1 — the files.** `git rm` the 7 modules: `Datastore/object.py`,
  `Datastore/SQL/{ClientPool,Datastore,ProfileAgent,SerialPoolBroker,ShardedPool}.py`,
  `Datastore/SQL/ObjectFactories/base.py`. `Datastore/__init__.py` and `Datastore/SQL/__init__.py`
  become **empty files** (0 bytes; P6). `Datastore/SQL/ObjectFactories/__init__.py` (0 bytes) and the
  17 factory modules stay where they are.
- **A2 — the imports**, by the adoption README §2 module map, module path only, the imported names
  unchanged: `from Datastore import DatastoreObject` → `from datastorekit import DatastoreObject`
  (13); `from Datastore.SQL.ObjectFactories.base import SQLAFactoryBase` →
  `from datastorekit.SQL.factory_base import SQLAFactoryBase` (17); `Datastore.SQL.ShardedPool` →
  `datastorekit.SQL.ShardedPool` (4, the test module's is deleted); `Datastore.SQL.ProfileAgent` →
  `datastorekit.SQL.ProfileAgent` (3). **P9:** the three factories import `require_version_serial`
  from `datastorekit.contract`, and `config/version.py` loses `VERSION_SERIAL_KEY` and
  `require_version_serial` (and the prose about them); `VERSION_LABEL` stays, and so does test f,
  which checks the drivers take it from `config.version`. **Never move `VERSION_LABEL`**: the
  package's `datastorekit.contract.VERSION_LABEL` is `"label"`, a column name (README §6.3).
- **A3 — the factories change nothing but those imports** (U3). They stay instances, their hooks keep
  `self`, the quantity factories keep their argument. 07 showed all 27 instantiate on the pinned base.
- **A4 — the pools.** All three constructions pass `factories=factories` and
  `serial_batch_sizes=serial_batch_sizes`, imported from `config.datastore`; `main.py`'s passes
  `drop_tables=` (A5); none passes `drop_actions=` or `inventory_config=`. Every other argument stays.
  **P8:** `LambdaCDM` leaves `config/sharding.py`'s `replicated_tables` and `config/datastore.py`'s
  `serial_batch_sizes`. `config/model_list.py:19`'s type hint imports from `datastorekit.SQL.ShardedPool`.
  `config/sharding.py`'s `inventory_config` is no longer passed by anything; it stays until 09
  deletes it with the six `inventory()` hooks.
- **A5 — U4, the drop policy.** In `config/datastore.py`, a `tables_to_drop(groups)` in the manner of
  SGK's, beside `drop_groups`: it maps the requested group names to their tables (in `drop_groups`'
  order) and computes the dependents with `datastorekit.SQL.schema.dependent_tables(tables,
  factories)`. If a dependent is not among the requested tables, it raises `ValueError` whose message
  names the groups to add: for `["scalar-model"]` alone, exactly
  ``cannot drop scalar-model alone: AdiabaticHistory and BBNData name its rows; add `--drop adiabatic-history bbn-data` ``
  (the wording is yours, logged; it names the requested group, the dependent tables and the
  `--drop` arguments to add, and the test pins it). A dependent table that is in no group is named as
  a table. `main.py` computes `drop_tables` this way **before `ray.init`** (move `:97-98` above
  `:95`); on `ValueError` it prints the message prefixed `!! --drop: ` to stderr and exits 2. The
  package's own refusal at open (`ShardedPool.py:825-843`) stays behind it as a second line, never
  reached from the command line.
- **A6 — the harness, on the package's actor.**
  1. **ChamPBH's copy of the stand-in.** `Datastore/tests/standin_pool.py`, copied from
     `datastorekit/tests/standin_pool.py` **at `v0.3.0`**, with a header naming that path, tag and
     commit (`2f5417ea…`), and why ChamPBH keeps a copy (adoption README §3). Change only what must:
     `open_pool` and `open_pool_output` import `datastorekit.tests.client.registry`, which is not
     installed; ChamPBH's open a pool with `config.datastore.factories`, `config.sharding`'s
     `replicated_tables`, `sharded_tables`, `ShardKeyType` and `get_shard_key_store_id`, and a
     `version_label` argument. The log gives `diff -u` against the tag's file, every hunk explained.
     The module has no `test_` prefix, so discovery does not run it.
  2. **`_TempStoreCase`**, rebuilt in the manner of the package's `TestTheActor`
     (`test_version_keyed_lookups.py:309-341` at the tag): each test runs inside a
     `StandinCluster().active()`; `open(label)` writes the label's `version` row through a real
     `ShardedPool` on stand-in shards (1 shard is enough) and closes it, then builds the undecorated
     package actor (`standin_pool.DatastoreClass`) on that shard's file with `replicated_tables`,
     `factories=`, a stand-in broker `Handle` shared by every actor of the test, notifies the broker
     of the file's largest serials (`read_largest_store_ids`) before the first insert, and calls
     `set_version` with the label's serial. `tearDown` closes every actor (`__exit__`) and asserts
     `ray.is_initialized()` is `False`. The helpers that reach into the actor (`_insert`'s `_engine`
     and `_schema[cls]["insert"]`) keep working or are adapted in the harness module.
  3. **Its importers change only their imports, if at all** (README §2.2). If a test body in one of
     the five would have to change, that is a stop.
  4. **The one test body that must change**: test a's `store_a._version.store_id` (`:216`), which
     becomes the serial the harness recorded for each label (or the actor's `_version_serial`). Named
     here, so README §5 rule 7 allows it.
- **A7 — the tests the package carries, and those the swap leaves with nothing to test** (each named
  here, so README §5 rule 7 allows it):
  1. `Datastore/tests/test_shard_key_assignment.py` (4) is deleted: the package pins the same fix.
  2. In `test_version_keyed_lookups.py`, **d2** (`:348`) and **d4** (`:376`) are deleted: they test
     the layer, and the package's own suite covers both (checklist item 7). a, b, c1–c3, d, d3, e and
     f stay.
  3. In `test_datastore_config.py`: the module-level layer imports, the import-order comment and the
     `import_module` go; **`TestLayerReadsConfig`'s three tests are deleted** (there is no layer to
     read the config); `EXPECTED_SERIAL_BATCH_SIZES` loses `"LambdaCDM": 5` (P8). The data tests and
     the vectorized-get test stay as they are.
  4. In `test_compare_stores.py`, `_registry()` imports `config.datastore` directly; the fallback to
     the layer goes, and its docstring with it. Nothing else in the module changes.
- **A8 — the new tests**, in `Datastore/tests/test_datastore_config.py` (one module, so the count is
  easy to derive):
  1. **The tables are declared.** Every name in `replicated_tables` and `sharded_tables` is a key of
     `factories`; `LambdaCDM` is in none of `replicated_tables`, `sharded_tables` and
     `serial_batch_sizes` (P8; the package's reader refuses a store that records an undeclared class).
  2. **The pools are given the config.** By `ast`, each of the three drivers' `ShardedPool(` calls
     passes `factories=` and `serial_batch_sizes=` bound to names the module imports from
     `config.datastore`, `main.py`'s passes `drop_tables=`, and none passes `drop_actions=` or
     `inventory_config=`. It asserts the count found (one per driver). This takes over from the
     identity tests A7.3 deletes: a copy of the registry in a driver fails it.
  3. **U4.** `tables_to_drop(["scalar-model"])` raises `ValueError` with the pinned message; each closed
     set is accepted and returns its tables: `["bbn-data"]`, `["adiabatic-history"]`,
     `["adiabatic-history", "bbn-data"]` and all three groups; `[]` returns `[]`. And, by `ast`, `main.py`
     calls `tables_to_drop` before `ray.init`.
  4. **The cycle is gone.** A subprocess `python -c "import config.datastore"` from the root exits 0.
  5. **`--inventory` refuses** (A9.1): a subprocess `main.py --database <tmp>/absent.db --inventory`,
     with a timeout, exits non-zero with the message and creates nothing at that path; by `ast`, the
     refusal comes before `ray.init`. 09 replaces this test.
- **A9 — the rest of what the swap makes false or unreachable.**
  1. **`--inventory`** prints that it is unavailable until 09 (P12), naming the prompt, and exits 2,
     **before `ray.init`**. The block `main.py:1196-1344` (the four `pool.inventory` calls and their
     helpers) is deleted; the drivers stop importing `inventory_config`.
  2. **The refresh route** (P13): `pipeline_selection.BBN_REFRESH_ROUTE` names
     `python -m datastorekit.tools.sharded_store copy SRC DST` in place of a copy (`:217-219`). A
     message only.
  3. **Prose made false by this commit** (P14):
     - `config/datastore.py`'s docstring: no aliases, no cycle; `tables_to_drop` described;
     - `config/version.py`'s prose about the two moved names;
     - `CLAUDE.md:68-72`: a test builds a temporary store through the package's undecorated actor,
       inside ChamPBH's stand-in cluster (`Datastore/tests/standin_pool.py`), never through Ray;
     - `.documents/OPEN_ISSUES.md`'s `[00-two-files-are-not-black-clean]` row (`:51-52`) and its board
       entry (`prompts/review-remediation/IMPLEMENTATION_STATE.md:213-215`) narrowed to the one file
       that remains;
     - **not** `tools/compare_stores.py`, whose comments cite the old layer's lines (`:90`, `:109-110`):
       it is run unchanged (README §4), and its citations are 11's to bring up to date. Record them.
     - **not** `.documents/architecture-summary.md`'s database sections: 11's (P14).
  4. **The issues.** Close `[05-config-datastore-cannot-be-imported-before-the-layer]` (board §3 →
     §4, its index row deleted). Open `[08-plot-scripts-open-read-write-pools]` (P11): the plot
     scripts open read-write pools, where a read-only pool would refuse a miss
     (`ReadOnlyMiss`, `ReadOnlyWrite`) and a lookup today may insert a parameter row; **the user wants
     them on read-only pools eventually**, so the entry states it as a goal, with what must be found
     first (which lookups insert). The index's count and date in the same commit.

---

## 2. What to do

### 2.1 Before

1. Record `HEAD` and the three suites (18 / 114 (61 + 53) / 73, by module as §1).
2. **Re-measure.** DatastoreKit's script at your `HEAD` (from its root, its own venv, `--commit`), and
   `git grep -n` for `Datastore\.SQL\.\(Datastore\|ShardedPool\|ClientPool\|ProfileAgent\|SerialPoolBroker\)`,
   `from Datastore import`, `ObjectFactories.base`, `_factories\|_drop_actions\|_drop_order\|_default_serial_batch_size`,
   `VERSION_SERIAL_KEY\|require_version_serial`, `inventory_config` and `__ray_actor_class__`, outside
   `prompts/`. Compare with §1. **Anything §1 does not account for is a stop.**
3. **The hooks' signatures.** For each hook the package's actor or pool calls on a factory (contract
   §3; `datastorekit/SQL/Datastore.py` at the tag), the arguments it passes, against each ChamPBH
   factory that defines the hook (an `ast` read). Record the table. A mismatch is a stop: the
   factories change nothing but an import (A3).
4. **The schema before.** Save R0's snapshot path (`out/02-reference-schema.json`); 06's
   `out/06-reference-schema.json` is the same but for `source`.

### 2.2 The change

A1–A9, as §1 says. `black` the files you change, and only those. `config/argument_parser.py`'s
`allowed_drop_actions` stays as the `--drop` choices; the A8.3 test also asserts it lists
`drop_groups`' keys in order.

### 2.3 Verification

1. **The suites.** Datastore gives **73 − 4 − 2 − 3 + the A8 tests** (shard-key 4; d2, d4; the three
   identity tests); the log derives it by module. ComputeTargets 114 (its
   `test_foreign_bbn_provenance` on the rebuilt harness) and CosmologyModels 18. Every test ends with
   `ray.is_initialized()` `False`. A count that falls by more than the deletions is a stop.
2. **DatastoreKit's script at the commit.** Measure the staged tree through a dangling commit
   (`git commit-tree` on your `HEAD`, on no branch, as 04 measured RayPoolKit's script), and say
   so: none of the 7 deleted modules present; the two `__init__.py` present and 0 bytes; outside
   `prompts/`, **0 import statements, 0 string literals and 0 `import_module` calls** naming a layer
   module; `prompts/` still holds its one; **0 imports of the two moved names** from `config.version`.
3. **Without Ray.**
   - `./venv/bin/python -c "import config.datastore"` from the root exits 0, and all 27 entries are
     instances of classes whose base is the package's.
   - `python -m datastorekit.tools.sharded_store --help` exits 0.
   - The drivers byte-compile, and every module they import resolves: prompt 04's log keeps
     `check_imports.py`; run it (it never executes a driver). `ray.is_initialized()` is `False`.
   - `main.py --database <tmp>/x.db --inventory` and `main.py --database <tmp>/x.db --drop scalar-model`
     each exit 2 with their message, start no Ray and create nothing.
4. **The schema.** In a temporary directory, open a real `ShardedPool` on stand-in shards
   (ChamPBH's `standin_pool`, 16 shards, ChamPBH's registry and tables, label `2026.6.0`), close it,
   and run `tools/compare_stores.py --snapshot` on its primary, then `--schema <primary> --against
   .documents/dk-and-rpk-migration/out/02-reference-schema.json`. **Every difference is listed in
   the log and explained** as a column or index the package adds or declares differently (contract
   §2: `serial`, `version`, `timestamp`, `stepping`); none may be a column of ChamPBH's own that
   moved, vanished or changed type. The snapshot goes to `out/08-standin-schema.json`; 10 compares
   its store with it.
5. `black --check` is clean on every Python file touched or added.

### 2.4 Deliberate breakage (README §5 rule 8)

Each applied to the working tree, shown to be caught, reverted, and recorded as a diff:
1. One factory's `SQLAFactoryBase` import pointed back at `Datastore.SQL.ObjectFactories.base`: the
   suites fail at import, naming the module.
2. One `from Datastore import DatastoreObject` restored (in a `CosmologyConcepts` class): the import
   check fails.
3. `"LambdaCDM"` restored to `replicated_tables`: A8.1 fails.
4. The `dependent_tables` check removed from `tables_to_drop`: A8.3 fails on `scalar-model` alone.
5. `set_version` dropped from the harness: the harness's tests fail on the actor's refusal
   (`datastorekit/SQL/Datastore.py:778-786`), not on anything else.
6. `factories=` in `plot_by_beta.py` bound to a copy (`dict(factories)`): A8.2 fails.

---

## 3. Acceptance

1. **A1–A2, by DatastoreKit's script** at the dangling commit (§2.3.2).
2. **The suites** give §2.3.1's counts, derived by module; `black --check` is clean.
3. **Without Ray** (§2.3.3): `config.datastore` imports alone, the tool runs, the drivers' imports
   resolve, and both refusals exit 2 before Ray.
4. **The schema** (§2.3.4) agrees with R0's but for the package's columns, each listed.
5. **The breakages of §2.4** are caught, and recorded as diffs.
6. **The records**, in the same commit:
   - this board's §1 row for 08, items A1–A9 and `### 5.13 After prompt 08 (<date>)`, with the
     schema differences and the rollback (`git revert` alone; nothing in the venv; README §4.4);
   - the three issue moves of §1 A9.4, each with `.documents/OPEN_ISSUES.md`'s row, count and date;
   - `out/08-standin-schema.json`.

---

## 4. Stop conditions — stop and ask

- §2.1.2 finds a layer import, a use of a moved name, or an importer of the harness that §1 does not
  account for.
- §2.1.3 finds a hook whose arguments the package passes differently from what a ChamPBH factory
  takes.
- A test body outside A6.4 and A7 would have to change, or an existing test fails for a reason other
  than its deletion here.
- The schema differs from R0's in a column of ChamPBH's own.
- Anything in the package behaves otherwise than its contract says (README §5 rule 11): record it
  and stop; never work around it here.
- A suite's count falls by more than the deletions, or any suite needs Ray.

---

## 5. What this prompt does not do

- **Files it changes:** the 7 deletions and the 2 emptied `__init__.py`; the 30 files whose layer
  import is rewritten (13 + 17); `config/version.py`, `config/datastore.py`, `config/sharding.py`
  (`LambdaCDM` only), `config/model_list.py`; `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py`;
  `pipeline_selection.py`; in `Datastore/tests/`: the new `standin_pool.py`, and
  `test_version_keyed_lookups.py`, `test_datastore_config.py`, `test_compare_stores.py`, the deleted
  `test_shard_key_assignment.py`, and the four harness importers only if their imports must change;
  `ComputeTargets/tests/test_foreign_bbn_provenance.py` likewise; `CLAUDE.md`;
  `.documents/OPEN_ISSUES.md`; `prompts/review-remediation/IMPLEMENTATION_STATE.md` (one entry
  narrowed); `.documents/dk-and-rpk-migration/out/08-standin-schema.json`; the log and this board.
- It ports no inventory (09), deletes no `inventory()` hook and keeps `inventory_config` (09).
- It changes no factory beyond its imports, `tools/` not at all, and no script of 01's or 05a's.
- It runs no build, starts no Ray, installs nothing, and opens no store but temporary ones. The
  rehearsal is 10's.
- It changes nothing in DatastoreKit, RayPoolKit or SecondaryGWKit.

---

## 6. The log

`logs/08-adopt-datastorekit.md`, using README §5.1. It holds §2.1's measurements and the hook
table; the script's output at the dangling commit; `standin_pool.py`'s diff against the tag; the
harness's design and why; the U4 message; the suites by module beside 07's; the schema differences;
the deliberate-breakage record as diffs; and every observation not acted on (at least the stale
citations in `tools/compare_stores.py` and `architecture-summary.md`, for 11).

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only (`git add`, `git rm`), after
showing `git diff --cached --name-only`, and made with `git commit -- <paths>`.
