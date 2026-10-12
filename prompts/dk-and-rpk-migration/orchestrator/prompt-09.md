# Orchestrator — prompt 09, port the inventory

Read [`../README.md`](../README.md) first: §0.1 items 4 and 6, §2.2 (09's charter, as amended), §5
(rules 4, 7, 8, 10–12), §6.2 P15 and §6.5. The conventions are those of
[`prompt-01.md`](prompt-01.md) §0 ("Conventions"), as [`prompt-08.md`](prompt-08.md) applied them,
and are not repeated here except where 09 changes them. 08's review (board §5.13) is the model for
this one.

**You do not write code.** You may:
- run the suites, the log's scripts and read-only Python checks, from the session scratchpad;
- build temporary stand-in stores with the agent's tree in the scratchpad, and read them with
  `read_inventory` and `main.py --inventory`;
- read a **copy** of a rehearsal store in the scratchpad (its `shards` table repointed at the
  copies), never the store itself;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`09-port-the-inventory.md`](../09-port-the-inventory.md), **as amended by this
note's commit** (I3, §2.4.1, §4; §0 below)
**Board items:** I1–I5 · **Closes:** nothing · **Opens:** only what the work finds ·
**Model:** Opus, as the prompt recommends · **No network**: DatastoreKit and SGK are read from their
local checkouts.

**Gate:**
- 08 landed (`857ec73`) and was reviewed (`aba9553`): the layer is gone, the pools are the
  package's, `--inventory` refuses, and `Datastore/tests/standin_pool.py` and the rebuilt harness
  exist.
- `HEAD` is the commit that adds this note, or a later one that touches no code.
  `git diff --stat 857ec73 HEAD -- . ':!prompts' ':!.documents'` is empty at `aba9553`.
- `ray_cluster.sh status` shows no ChamPBH process; 09 starts none and takes no lock.
- One prompt at a time in this checkout; nothing installs into the venv while 09 runs.

## 0. What makes this prompt unusual

**It adds a contract, not a swap.** Each owner's spec is data the package reads, and the fingerprint
RunRegistryKit will take later reads nothing else (README §6.5). A spec that names a wrong column is
caught by `inventory_classes` or by test 2; a spec that names the *right* columns but the wrong
identity (a leaf the lookup does not match, or a lookup column left out) is caught by nothing but
the table in the log, read against each `build`. That table is what the review reads hardest.

**The orchestrator prototyped the specs before dispatch** (below), by attaching §1.1's proposals to
the factory classes at runtime in a scratch script; the repository was not touched. They hold
unchanged: `inventory_classes` accepts the registry, every column resolves, U4's table does not
move, and a stand-in store reads back with no problem.

**The amendment made at dispatch.** The prompt says "08's refusal and its test go" and allows the
Datastore count to fall by one. 08 left **two** tests of the refusal (`TestInventoryRefuses`,
`Datastore/tests/test_datastore_config.py:440-487`): the subprocess test and the `ast` test that the
refusal precedes `ray.init`. Both test the refusal I3 removes, so both must go, and under the prompt
as written that is a §4 stop ("falls by more than the one test I3 replaces"). This note's commit
amends I3, §2.4.1 and §4 to name both, and README §2.2's acceptance 3 with them (board §5.13a). The
module docstring (`:24-25`, "Two tests run a subprocess … and ``main.py --inventory``") is made
true in the same edit. Test 5's `ast` check takes the second test's place. **Datastore after 09:
76 − 2 + the new tests**; `test_datastore_config` goes from 16 to 14.

**Where the acceptance can pass vacuously.**
- **The harness's row helpers make stores the inventory cannot resolve.**
  `_insert_failed_scalar_model` and its neighbours (`test_version_keyed_lookups.py:118-193`) insert a
  `ScalarModel` with `cosmology_type=7`, `potential_type=3`, `coupling_type=5` and parent serials
  (1, 2, 4, 10–13) that name no row, because the lookups they serve never join those tables. An
  inventory of such a store reports `unresolved-parent` and lists no model. Test 3 therefore needs
  rows of its own, with real parent rows and the type values of the three `model_ids.py` modules
  (cosmology `3`, potentials `0`–`4`, coupling `0`). The helpers may be imported; they are not
  modified (README §5 rule 7).
- **Test 3 can pass on a report that prints hex.** A key's float leaves are `float.hex` strings
  (`store_inventory.canonical`). A test that asserts "the value appears" by re-deriving it from the
  key passes whatever the renderer prints. The assertions must be on the rendered decimal text
  (`.5g`, the unit beside it), against values the test chose.
- **Test 4 can pass with short problems.** "However long" is tested only by a problem longer than
  any truncation width the renderer has; the orchestrator's prototype problems are about 150
  characters. The test should make one longer than any width the renderer uses, or assert that the
  full string `read_inventory` returned appears in the report unbroken.
- **I5 passes vacuously if its walk reads the specs it is testing.** The walk is over
  `config.datastore.factories`' keys; "declares a spec" is `inventory_spec()` returning a spec; the
  tag and value tables are named in the test by owner and slot, and the test checks that the
  owner's spec actually names that table in that slot. Breakage 5 is the proof.
- **The `ast` test of test 5** passes vacuously if it finds no `--inventory` branch; it asserts the
  count (one), as 08's did.

**The facts, checked by the orchestrator on 2026-10-12 at `aba9553`** (the code is `857ec73`'s).
Pass them on.
- **The tree.** Branch `dk-and-rpk-migration`, `HEAD` `aba9553` before this note. Untracked: the
  user's 17 `pilot-2026.6.0*.db` files, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`,
  `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh` and `prompts/jordan-normalization/`. None
  is the agent's. `git diff --cached` is empty.
- **Ray.** `ray_cluster.sh status`: lock free, no ChamPBH process, ports 6380/6381/8267/52367 free.
- **The venv** is as 07 left it: `pip freeze` 101 lines; `pip check` clean; site-packages 209
  entries. 09 installs nothing.
- **§2.1.3's `git grep -n "inventory" -- '*.py' ':!prompts'`** returns 26 hits. Every one is
  accounted for, and **none is a stop**:
  - *the six hooks* (I4): `AdiabaticHistory.py:387`, `BBNData.py:453`,
    `DimensionfulQuantity.py:122`, `DimensionlessQuantity.py:95`, `ScalarModel.py:804`,
    `redshift.py:118`;
  - *`inventory_config`* (I4): `config/sharding.py:57`, imported by nothing;
  - *08's refusal and its comment* (I3): `main.py:91`, `:93`, `:95`;
  - *08's two refusal tests and the docstring* (I3 as amended): `test_datastore_config.py:25`,
    `:441-483`;
  - *08's pool test* (`test_datastore_config.py:256`, `:283`), which asserts that no pool passes
    `drop_actions=` or `inventory_config=`. **It stays**: it is a guard on the pools, and still
    passes with the name gone;
  - *08's U4 reading the specs* (`config/datastore.py:43`, `:171`, `:180`): `tables_to_drop`'s
    wording reads `inventory_specs(factories)`, so the specs 09 declares feed it (below);
  - *`config/argument_parser.py:64-73`*, the `--inventory` and `--show-all` help. Both are true
    again once I3 lands ("show an inventory of the datastore content"; "do not truncate long lists
    of inventory items"), so **the file needs no change** and is not on §5's list. If the agent
    finds a reason to change it, that is an observation, not an edit.
- **The 21 owners are 17 classes in the 17 factory modules.** `sqla_dimensionful_quantity_factory`
  serves five entries (`M_value`, `Lambda_value`, `temperature`, `phi_value`, `pi_value`) and
  `sqla_dimensionless_quantity_factory` one (`beta_value`); each reads its value column from the
  instance (`self.value_col`, `value_<default_unit>`, or `value`). So §5's "the 21 owners' factory
  modules" is all 17 modules under `Datastore/SQL/ObjectFactories/`, and 17 `inventory_spec`
  definitions.
- **The specs, prototyped.** §1.1's table, attached at runtime, with `ScalarModel`'s leaves the three
  `*_type` columns and `failure`, and `BBNData`'s `failure` and `PRyM_version` (§1.2):
  - `inventory_classes(factories)` accepts the registry and orders it: `version`, `store_tag`,
    `redshift`, `tolerance`, the six quantities, `QCD_Cosmology`, `IntegrationSolver`,
    `InversePowerPotential`, `StarobinskyPotential`, `ExponentialPotential`, `ReclinerPotential`,
    `ReflectingPotential`, `ExponentialCoupling`, `ScalarModel`, `AdiabaticHistory`, `BBNData`;
  - every leaf, parent column, `type_column`, `validated`, tag and value slot names a column of the
    table `build_schema` builds (tags: `model_serial`, `history_serial`, `bbn_serial` beside
    `tag_serial`, which is `datastorekit.contract.TAG_SERIAL`; `store_tag`'s `label` is
    `TAG_LABEL`);
  - **no `cross_shard`**: every sharded parent is on its child's shard. `ExponentialCoupling`
    (sharded) names `beta_value` (replicated, so on every shard); `ScalarModel` names its coupling,
    on the same shard since both take their shard key from β; `AdiabaticHistory` and `BBNData` name
    their model, likewise. Everything else a spec names is replicated.
- **Three lookup details the log should state**, none a stop:
  - `IntegrationSolver.build` matches `label ==` and `stepping >=` (`integration_metadata.py`, a
    minimum, the `"stepping": "minimum"` registration), not equality. The leaves are
    `("label", "stepping")`, as SGK's.
  - **The five potentials and `ExponentialCoupling` register `"version": True`**, so their tables
    carry a `version` column, but their lookups do not match on it (no `key_on_version`). By §1.2's
    rule it is not in their keys. Only `ScalarModel`, `AdiabaticHistory` and `BBNData` key on
    version, and declare it as a parent.
  - `QCD_Cosmology.build` matches six floats and not `name`, so `name` is not a leaf.
- **U4 does not move.** With the prototype specs installed, `tables_to_drop` over all eight subsets
  of the three groups gives exactly what it gives without them: `scalar-model` alone refused with
  08's pinned message; with one of the other two, refused naming the third; the five closed sets
  accepted. No spec's parent is in a drop group other than through the foreign keys that were
  already there. So 08's `TestDropPolicy` passes unchanged, and the log says so (08's hand-on).
- **A stand-in store, prototyped** (the prompt's §2.3, as the harness does it): a one-shard pool
  through `StandinCluster.open_pool(…, version_label="2026.6.0", serial_batch_sizes=…)`, closed;
  `sp.DatastoreClass` on the shard with a stand-in broker, notified, `set_version`; rows inserted
  through `store._schema[cls]["insert"]` **with no explicit serial** (the broker leased every one:
  this is the first exercise of the broker by anything but 08's probe); the actor closed; then
  `read_inventory(<tmp>/store.db, factories)`. Result: 21 classes, **no problem**, `ray.is_initialized()`
  `False`. Three `ScalarModel` records (failed and validated; succeeded, validated, tagged `pilot`,
  three value rows; unvalidated), each resolving through `StoreInventory.resolve` to its
  temperatures, potential (class and `M`, `Λ`) and coupling. Adding an orphan `ScalarModelValue`
  (`model_serial=999`) and a `ScalarModel` with `cosmology_type=0` gives exactly
  `orphan-value: ScalarModel: 1 ScalarModelValue row(s) on shard #0 name 1 ScalarModel row(s) that
  are not there; e.g. parent serials 999` and `unresolved-parent: ScalarModel: 1 row(s) on shard #0
  reference a parent that cannot be resolved (field(s) cosmology), and are not records; e.g.
  serials 4`. SQLite does not enforce the foreign key, so the orphan inserts through the actor.
- **How the report gets physical labels.** A `Record`'s parent fields hold the parent's reference
  digest, not a value. `StoreInventory.resolve(name, record)` replaces each, recursively, with the
  parent's key and tags; `parent_class` gives a polymorphic parent's class. A float leaf is
  `float.hex` of the stored value: `float.fromhex` gives it back exactly. Knowing which leaves are
  floats needs the registry's columns (SGK's `_schema_tables`, `tools/inventory_report.py:161`).
- **Every pre-migration store is refused, and that is right** (README §0.1 item 6, D4). On a scratch
  copy of R1's `06-reference` store, `read_inventory` raises `RuntimeError` on the primary's
  absolute shard records ("… is not a bare file name …"); on a copy with the records made bare, on
  `replicated_tables` naming `LambdaCDM` ("… names the class(es) ['LambdaCDM'], which the registry it
  was given does not declare"). So the new `--inventory` exits 1 on R0's and R1's stores, on the
  user's untracked `pilot-2026.6.0.db`, and on the science store, until they are rebuilt (§7).
  **The agent runs `--inventory` on no store but its temporary ones**: §2.4.2's comparison is
  against the text log, as the prompt says.
- **Today's report** (`06-readers/1-inventory.log`, 72 lines) shows: redshifts (21,250), β, the two
  temperatures, M, Λ, φ and π, each with count, range and values; then `ScalarModel`,
  `AdiabaticHistory` and `BBNData`, each as validated and unvalidated groups with count, range,
  version and stored labels, `ScalarModel`'s followed by `T_init`, `T_stop`, `phiE_init`,
  `piE_init`, `failure`, BBN's by `failure`. It does not show `version`, `store_tag`, `tolerance`,
  `QCD_Cosmology`, `IntegrationSolver`, the potentials or `ExponentialCoupling`. The new report
  covers every class, so those are "new" rows of the P15 table.
- **SecondaryGWKit** at `a5a6f6d`, clean: `tools/inventory_report.py` 696 lines; its tests 19 and
  19; `main.py:283-305`. Its `--inventory` maps verbose from `args.inventory_verbose`; ChamPBH's is
  `--show-all`, `args.show_all`.
- **DatastoreKit** at `04a2ed9` (ahead of the tag), clean. The installed package equals
  `git archive v0.3.0` (`diff -r`, `tests/` and `__pycache__` aside). `store_inventory.py` is 1,045
  lines, at the lines the prompt gives. `inventory_specs` calls `hook()` on the registry entry, so
  an instance method serves, as the prompt says (the package's docstring says "static"; either
  works).
- **`black` 26.1.0** is clean at `HEAD` on the 17 factory modules, `config/sharding.py`, `main.py`,
  `test_datastore_config.py` and `tools/*.py` (25 files).
- **`.documents/OPEN_ISSUES.md`**: 26 open, 3 on this board (`### 1.11`, `:190`), last updated
  2026-10-12. 09 opens only what it finds.
- **Siblings:** SecondaryGWKit `a5a6f6d`, StochasticInstantons `5fb4a1d`, RayPoolKit `1b84df0`,
  DatastoreKit `04a2ed9`, each with a clean working tree. Nothing is written in any.
- **The rehearsal stores** (104 entries to depth 2 under
  `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/`) are 02's and 06's. 09 opens none.
- **The suites at `857ec73`** (08's review): CosmologyModels 18, ComputeTargets 114 (61 + 53),
  Datastore 76 (`bbn_failure_lookup` 6, `compare_stores` 21, `datastore_config` 16,
  `first_bounce_round_trip` 5, `fixed_T_values_round_trip` 5, `package_provenance` 10,
  `scalarmodel_failure_reason` 4, `version_keyed_lookups` 9).
- **The load** at dispatch: about 5 (5.02 / 4.61 / 5.36 at 23:53).

**What the review exists to establish.**
- **(E1) I1, the specs.** 17 definitions serving 21 owners; each row of the log's table against its
  factory's `build`, read by the orchestrator; `inventory_classes` accepts the registry; no factory's
  `register` or `build` changed.
- **(E2) I2, the report.** `format_inventory_report` reads nothing (no `open`, no engine, no store
  path opened); it shows P15's fields with §1.2's two changes, values in decimal with their unit,
  and every problem whole.
- **(E3) I3, the driver.** The `--inventory` branch before `ray.init`, refusing `--drop` (exit 2),
  failing cleanly on a missing store (exit 1, nothing created), exiting 0 on a store; `--show-all`
  maps to `verbose`; 08's refusal gone.
- **(E4) I4.** The six hooks and `inventory_config` gone; `git grep` finds nothing; the factory
  modules otherwise change only by the spec and the imports it needs (and imports the deleted hooks
  alone used).
- **(E5) I5.** The coverage test is a real walk of the registry with an empty exclusion table, and
  fails on both breakages.
- **(E6) The tests and the breakages.** Tests 1–6 not vacuous (§0's four points); the five breakages
  caught; U4's tests unchanged and passing.
- **(E7) Nothing else moved.** No file outside §5's list; the venv unchanged; no store opened but
  temporary ones; the siblings untouched; the drivers' imports resolve; the suites at 18 / 114 /
  74 + N.
- **(E8) The records.** The board's §1 row, I1–I5, `### 5.14 After prompt 09 (<date>)` with the P15
  table; anything opened in §3 and the index in the same commit; the log in README §5.1's form.

**Conventions particular to 09.** prompt-01.md's conventions and 08's hold. In addition:
- **git.** The only commit on `dk-and-rpk-migration` is the agent's one. Every path by explicit
  `git add`; `git diff --cached --name-only` shown, then `git commit -- <paths>` (the prompt's §6).
  No `checkout`, `switch`, `stash`, `reset --hard` or `rebase`.
- **DatastoreKit** is read through `git -C … show v0.3.0:<path>` (or an archive extracted into the
  agent's scratch), never written. **SGK** is read, not imported: nothing under
  `/Users/ds283/Documents/Code/SecondaryGWKit` goes on `sys.path`.
- **Scratch.** Probes and the sample report's store live in the agent's subdirectory of the session
  scratchpad. No scratch file under `config/`, `Datastore/`, `tools/` or `.documents/`.
- **Stores.** Only temporary ones, built through the stand-in. **`main.py --inventory` is run on no
  rehearsal store and on none of the user's `pilot-2026.6.0*.db` files** in the repository root,
  even though it is read-only: they are not the agent's, and §0 already says what it would print.
- **Ray.** None is started. Every new test asserts `ray.is_initialized()` `False`; the driver
  subprocesses use an address nothing listens on, as 08's did.

## 1. Before you dispatch

1. **The tree.** Record the branch and `HEAD`. `git status --short` shows only §0's untracked
   entries, and `git diff --cached` is empty. `git log --oneline aba9553..HEAD` touches no code.
2. **Ray.** `ray_cluster.sh status` as §0; `pgrep` finds nothing.
3. **The venv.** `pip freeze` saved in the scratchpad (101 lines, as §0); `pip check` clean.
4. **The load.** `uptime`, recorded and passed on.
5. **Hygiene baselines**, for check 12: `/usr/bin/stat -f '%m %z %N'` over the rehearsal root to
   depth 2 (104 entries); the four siblings' `HEAD`s and `git status --porcelain`; the mtimes of
   the user's 17 `pilot-2026.6.0*.db` files.
6. **The orchestrator's prototype**, kept in the scratchpad for checks 2 and 5: its spec script, its
   problem-store variant, and the two R1 copies (absolute and bare shard records).

The suites are not re-run at dispatch: no Python has changed since `857ec73`, where they were 18 /
114 / 76, and the agent runs them first (§2.1.1).

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt (amended);
- the campaign README and this board;
- prompt 08's log (the harness and the stand-in) and prompt 04's (for `check_imports.py`);
- `HEAD`, the load and §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the
  session scratchpad. The agent does not read `orchestrator/`, and does not read prompt 10.

Tell it plainly:
- **One commit, at the end, by pathspec.** Its log is `logs/09-port-the-inventory.md`, in README
  §5.1's form, with the prompt's §6 additions.
- **The files it may change** are the prompt's §5 list (the 17 factory modules among them). **Nothing
  in DatastoreKit, RayPoolKit or SGK, nothing in the venv, no factory's `register` or `build`, no
  existing test but `test_datastore_config.py`'s two refusal tests and its docstring.**
- **Measure before editing** (§2.1), and record the suites by module first.
- **Stop and ask** on any of the prompt's §4 conditions. §0's grep hits, the pre-migration refusal
  and the two refusal tests (now named) are not stops. A behaviour of the package that differs from
  its contract is recorded and stopped on, never worked around (README §5 rule 11).
- **It replies with:**
  - the log's path and the commit; `git diff --cached --name-only` before it and
    `git show --stat HEAD` after it;
  - §2.1's measurements: the suites by module, the grep classified, and the spec table, each row
    with its `build` reference;
  - `inventory_classes(factories)`'s order;
  - a sample report on a test store, whole, and `--show-all`'s on the same store;
  - the `--inventory` branch of `main.py`, and the three driver outcomes with exit codes;
  - the P15 table;
  - `git grep -n "def inventory(\|inventory_config" -- . ':!prompts'` (empty);
  - the new tests by name, and the five breakages with what caught each;
  - `TestDropPolicy`'s result, and whether U4's table moved;
  - `check_imports.py`'s output and `black --check`;
  - the three suites by module, with the load;
  - every deviation with its tag;
  - the "State handed to the next prompt" section, verbatim.

## 3. The review — twelve checks

1. **Scope.** At the agent's commit, `git show --stat HEAD` touches only: the 17 factory modules,
   `config/sharding.py`, `main.py`, `tools/inventory_report.py` (new),
   `Datastore/tests/test_inventory_specs.py` and `test_inventory_report.py` (new),
   `Datastore/tests/test_datastore_config.py`, the log, the board, and `.documents/OPEN_ISSUES.md`
   only if an issue moved. `git status` shows §0's untracked set and nothing new. `git log` since
   this note shows no other commit of the agent's; `git branch` no leftover branch.
2. **E1, the specs, read.** For each of the 21 owners, the orchestrator reads the factory's `build`
   and its spec side by side: the leaves and parents are exactly the lookup's match columns (§1.2's
   two additions aside, and `version` only where the lookup keys on it); polymorphic maps equal
   `tools/compare_stores.py:172-201`'s; tag and value slots name the right owner column. Then, from
   the scratchpad, `inventory_classes(factories)` gives §0's order, and the prototype's spec table
   equals the agent's (any difference explained in the log).
3. **E1/E4, the factories by `ast`.** For each of the 17 modules, `ast.dump` at `HEAD~1` and `HEAD`,
   less the class bodies' `inventory_spec` (added) and `inventory` (removed) and the import
   statements, is identical: no `register`, `build`, `store`, `validate`, `read_table` or other
   method moved. The imports added are the package's spec names; the imports removed were used only
   by the deleted hooks (checked by name). `config/sharding.py` loses `inventory_config` and nothing
   else (`ast` comparison of the other assignments).
4. **E2, the report.** Read `tools/inventory_report.py`: it opens nothing (no `open`, `sqlite3`,
   `create_engine`, `read_inventory` or `open_read_only` call), imports nothing from SGK, and
   renders from the `StoreInventory` alone (the registry only for float leaves, if used). Problems
   are emitted whole. The `.5g` and unit rule is applied to every quantity.
5. **E2/E3, by running it.** From the scratchpad, at the agent's commit: the orchestrator's own
   stand-in store (§0's prototype rows, rebuilt) through `main.py --inventory` and `--show-all`
   (exit 0; the report shows the three models' temperatures, φ\*, π\*, the potential's class, M, Λ,
   β, `failure`, the version label and the value count); the problem variant (both problems whole,
   exit 0); an absent path (exit 1, the reader's message after `!! --inventory:`, nothing created);
   `--inventory --drop bbn-data` (exit 2); the R1 copies (exit 1, each with the refusal of §0).
   `pgrep` empty after each.
6. **E3, the driver's diff.** `git diff HEAD~1 HEAD -- main.py`: 08's refusal and comment replaced by
   the read-only branch above `ray.init`, its imports, and nothing else.
7. **E5, coverage.** Read the coverage test: it walks `factories`' keys, places each in exactly one
   of the three, checks each tag/value table against its owner's spec slot, and checks the exclusion
   table's names are registered. The exclusion table is empty.
8. **E6, the tests and the breakages.** The two refusal tests deleted and nothing else of
   `test_datastore_config.py`'s bodies changed (`ast`); the docstring true. The new tests read
   against §0's vacuity points: test 3 asserts decimal values the test chose; test 4 a problem whole;
   test 5's `ast` check asserts one branch; every subprocess has a timeout. In `bash`, replay the five
   breakage diffs (`git apply --check`, `-R --check`, apply, run, revert): each fails as the prompt
   says. `git status` clean after.
9. **E7, without Ray.** `import config.datastore` alone exits 0; `check_imports.py` (log 04's) over
   the three drivers resolves every name; `py_compile` of the drivers; `black --check` on every
   touched or added Python file; `ray.is_initialized()` `False` throughout.
10. **E7, the suites.** The agent's: 18, 114, 74 + N, by module, with the load. The orchestrator
    re-runs `Datastore/tests` at the agent's commit; ComputeTargets in full only if the agent's
    figures are missing or odd. `TestDropPolicy` passes unchanged.
11. **E7, hygiene.** `pip freeze` equals §1's copy; `pip check` clean. The 104 rehearsal mtimes and
    the 17 pilot files' mtimes unchanged. The four siblings at their §1 `HEAD`s, clean. No Ray
    process. No scratch file in the repository; no `.db` under the repository but the user's
    untracked ones.
12. **E8, the records.** The board's §1 row (✅, "this commit", the log), I1–I5, and
    `### 5.14 After prompt 09 (<date>)` with the P15 table and the rollback (`git revert` alone).
    Any issue opened is in §3 and the index in the same commit, with the count and date. The log
    follows README §5.1: the spec table with each `build` reference, §1.2's two declared non-lookup
    leaves with their consequence, the P15 table, a sample report, the suites beside 08's, and the
    breakages as diffs.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 09* paragraph in §5.14: what each
  check found, and anything handed on to 10. Fix small residue in a follow-up commit of its own:
  - "this commit" → the SHA (board and log);
  - this note added to the board's list of orchestrator notes, marked "used for 09";
  - the status line: 10 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the specs in one line (21 owners, 17 definitions), and the two non-lookup leaves;
  - the report: what it shows that today's did not, and what it no longer shows;
  - `--inventory`'s three outcomes, and that every pre-migration store is refused until rebuilt;
  - the coverage test;
  - the tests deleted (the two refusals, amended at dispatch) and added, and the breakages;
  - the suites;
  - anything opened.
- **Then 10** can be dispatched; its note is written after this review. Hand on: R2's readers now
  include the new `--inventory` (read-only, before Ray), which on R2's fresh stores should read with
  no problem; the report's layout is new, so R2's printed lines differ from R1's there by design;
  and the R0/R1 stores cannot be inventoried by the package.
