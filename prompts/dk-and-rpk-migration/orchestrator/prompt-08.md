# Orchestrator — prompt 08, adopt `datastorekit`

Read [`../README.md`](../README.md) first: §0.1 (all seven items), §0.4, §2.2 (08's charter), §4
("08 ships"), §4.2, §4.4, §5 (rules 4, 7–12) and §6.2–§6.3 (P6, P8, P9, P11–P14). The conventions
are those of [`prompt-01.md`](prompt-01.md) §0 ("Conventions"), as [`prompt-05.md`](prompt-05.md)
applied them to a change of code, and are not repeated here except where 08 changes them. 05's
review (board §5.7) is the model for this one, at three times the size.

**You do not write code.** You may:
- run the suites, DatastoreKit's measuring script, the log's scripts and read-only Python checks,
  from the session scratchpad;
- build a temporary stand-in store with the agent's `Datastore/tests/standin_pool.py` in the
  scratchpad, and snapshot it;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`08-adopt-datastorekit.md`](../08-adopt-datastorekit.md), **as amended by this
note's commit** (A7.4, test 14; §0 below)
**Board items:** A1–A9 · **Closes:** `[05-config-datastore-cannot-be-imported-before-the-layer]` ·
**Narrows:** `[00-two-files-are-not-black-clean]` (the `review-remediation` board) · **Opens:**
`[08-plot-scripts-open-read-write-pools]` (P11), and only what else the work finds ·
**Model:** Opus, as the prompt recommends · **No network**: DatastoreKit and SGK are read from
their local checkouts.

**Gate:**
- 07 landed (`617a1dc`) and was reviewed (`c4bd666`): the venv holds `datastorekit` 0.3.0 at
  `2f5417ea…` and `raypoolkit` 0.2.0 at `eaef0ce6…`, neither editable.
- `HEAD` is the commit that adds this note, or a later one that touches no code ("code" as
  prompt-01.md's gate defines it). `git diff --stat 617a1dc HEAD -- . ':!prompts' ':!.documents'` is
  empty at `c4bd666`.
- `ray_cluster.sh status` shows no ChamPBH process; 08 starts none and takes no lock.
- One prompt at a time in this checkout; nothing installs into the venv while 08 runs.

## 0. What makes this prompt unusual

**It is the swap itself, and the tree cannot pass between its parts.** Nine files go (seven deleted,
two emptied), 30 imports move, three pools are rebuilt on the package's constructor, the drop policy
is new, and the test harness moves onto an actor that inserts no version row. Most of it is
mechanical and is proved by DatastoreKit's script and the import check. The harness is the hard
part, and **the orchestrator prototyped it before dispatch** (below): the prompt's design works, and
every harness importer passes on it unchanged.

**Where the acceptance can pass vacuously.**
- **DatastoreKit's script reads a commit, not the working tree.** Run at `HEAD` before the commit, it
  reports the old tree. §2.3.2 runs it on a dangling `git commit-tree` of the staged index; the
  review re-runs it at the agent's commit.
- **The schema comparison has nothing to list.** Measured on the prototype, a 16-shard stand-in
  store under the package gives `RESULT: the schemas agree` against R0's snapshot, with **no
  difference at all**: the package adds no column ChamPBH's actor did not. So §2.3.4's "every
  difference … explained" is an empty list, and **any difference the agent finds is a finding**, to
  be explained or stopped on, not a package column to be expected.
- **The stand-in broker is never exercised by today's tests.** Every insert in the five harness
  importers passes an explicit serial (`_insert`'s rows, `_write`, test e's `serial=`), and the
  package's `_insert` leases nothing when the payload holds one (`datastorekit/SQL/Datastore.py`,
  the `SerialLeaseManager` call). The prototype with `serial_broker=None` and no notify passes the
  same 37 tests. The prompt asks for the broker (09's tests may insert without serials); the review
  reads that it is there and wired as the pool wires it, since no test can show it.
- **A8.2's `ast` test** passes vacuously if it finds no `ShardedPool(` call; it must assert the count
  (three, one per driver), as the prompt says.

**A test the prompt did not name, found by the prototype, and the amendment made for it.**
`Datastore/tests/test_compare_stores.py`'s **test 14** (`TestChamPBHSchema.test_14_a_store_with_champbhs_schema`,
`:1086`) builds its primary's lists from `config.sharding` (`:1065-1076`) and asserts at `:1111`
that `compare_stores` notes `LambdaCDM`. P8 removes `LambdaCDM` from `replicated_tables`, so the
note is no longer made and the test fails (measured: `AssertionError: False is not true` at
`:1111`; nothing else in the module fails). Under the prompt as written that is a §4 stop ("an
existing test fails for a reason other than its deletion"). It is the direct consequence of P8,
which the user ruled, so **this note's commit amends A7.4**: the assertion becomes `assertFalse`,
and test 14's docstring says so; `:1110` (`assertNotIn("LambdaCDM", tables)`) stays; the general
case stays pinned by `test_a_listed_replicated_table_that_no_shard_holds` (`Ghost`, `:632`). With
it, the module's 21 pass on the prototype. The count is unchanged. The user is told.

**The facts, checked by the orchestrator on 2026-10-12 at `c4bd666`** (the code is `617a1dc`'s,
which is `fa874a5`'s but for `requirements.txt`, so the prompt's §1 line numbers hold). Pass them
on. The prototype is a `git archive` of `HEAD` in the scratchpad with the swap applied crudely; the
repository was not touched.
- **The tree.** Branch `dk-and-rpk-migration`, `HEAD` `c4bd666` before this note. Untracked: the
  user's 17 `pilot-2026.6.0*.db` files, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`,
  `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh` and `prompts/jordan-normalization/`. None
  is the agent's. `git diff --cached` is empty.
- **Ray.** `ray_cluster.sh status`: lock free (no lock file), no ChamPBH process, ports
  6380/6381/8267/52367 free. `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
- **The venv** is as 07 left it: `pip freeze` 101 lines, `datastorekit @ …@2f5417ea…` at `:17` and
  `raypoolkit @ …@eaef0ce6…` at `:80`; `pip check` clean; site-packages 209 entries. 08 installs
  nothing.
- **§2.1.2, DatastoreKit's script at `c4bd666`** (from its root, its venv, `--commit c4bd666`)
  equals the prompt's §1 exactly: **43 layer imports in 39 files** (42 in 38 outside `prompts/`),
  1 nested (`test_compare_stores.py:713`, `try`), 0 string literals, **1 `import_module`**
  (`test_datastore_config.py:39`), 18 factory-module imports (17 in `config/`, 1 at top level,
  `main.py:46`'s `tolerance`), and **4 imports of the moved names** (the three factories'
  `require_version_serial`, and `test_version_keyed_lookups.py:350`'s `VERSION_SERIAL_KEY`). Every
  layer import is at the line §1 gives.
- **§2.1.2's `git grep`s** return more than §1 names, because the patterns match prose and actor
  attributes. Every other hit is accounted for, and **none is a stop**:
  - *the layer's own lines* (`Datastore/SQL/*.py`), all deleted;
  - *prose 08 rewrites*: `config/datastore.py:25-38` (the docstring, A9.3), `CLAUDE.md:70` (A9.3),
    `config/version.py:55-71` (A2), `.documents/OPEN_ISSUES.md:51` (A9.3);
  - *the test module comment* `test_datastore_config.py:37` (the import-order comment, A7.3) and
    `:114`, `:146-153` (`TestLayerReadsConfig`, deleted);
  - **the actor's instance attributes reached into by tests**: `store._factories`
    (`test_scalarmodel_failure_reason.py:89`, `test_version_keyed_lookups.py:336` in d, `:389` in
    d4), and in the same bodies `_engine`, `_tables`, `_inserters`, `_schema[cls]["insert"]`. **The
    package's actor has every one of them** (`datastorekit/SQL/Datastore.py:113-120`, `:370`,
    `:410-425`), so these bodies run unchanged;
  - `allowed_drop_actions` (`config/argument_parser.py:33`, `:267`; `main.py:53`, `:98`), which
    stays as the `--drop` choices (§2.2); `main.py:97-98` move above `ray.init` (A5);
  - `inventory_config`: its definition (`config/sharding.py:58`, stays until 09), the three
    drivers' imports (`main.py:64`, `plot_ScalarModel.py:55`, `plot_by_beta.py:48`) and pool
    arguments (`:1362`, `:1593`, `:1023`), which go (A4, A9.1);
  - `__ray_actor_class__` in **module docstrings**: `test_version_keyed_lookups.py:20` (the
    harness's own module, whose docstring the rebuild makes false: rewrite it, as part of A6), and
    `test_bbn_failure_lookup.py:30`, `test_foreign_bbn_provenance.py:38` — **leave these two**
    (README §5 rule 7: their modules change only their imports, if at all) and record them for 11,
    with `test_first_bounce_round_trip.py`'s and the others' "as `test_version_keyed_lookups`
    does" prose;
  - `.documents/architecture-summary.md` (`:297`, `:320`, `:357`, `:390`, `:1001`, `:1071-1073`)
    and `.documents/review-remediation-verification.md:1248`: 11's and history respectively;
  - `tools/compare_stores.py:90`, `:109-110` (comments citing the old layer's lines): **not
    changed** (README §4: run unchanged), recorded for 11.
- **§2.1.3, the hooks.** What the package calls on a registry entry: the actor `register()` (via
  `schema.build_schema`), `build(payload=, conn=, table=, inserter=, tables=, inserters=)`,
  `store(obj, conn=, table=, inserter=, tables=, inserters=)`, `validate(obj, conn=, table=,
  tables=)`, `validate_on_startup(conn, tab, tables, prune)` (positional), `read_batch(payload=,
  conn=, table=, tables=)`, `read_table(conn, tab, *args, **kwargs)`; the pool `owned_serials`,
  `revalidate` and `validate_on_startup(…, prune=…)` **on replicated classes only**, and
  `factories["version"].build(…)`. ChamPBH's factories define `build(self, payload, conn, table,
  inserter, tables, inserters)` (all 23), `store`/`validate` with those names (3 classes),
  `validate_on_startup(self, conn, table, tables, prune=False)` (3 classes, **all sharded**, so the
  pool's keyword call never reaches them), `read_table` (3); none defines `owned_serials`,
  `revalidate` or `inventory_spec`, which the package's base defaults. **No mismatch expected.**
  ChamPBH's actor's calls (`Datastore/SQL/Datastore.py`) are the same but for `inventory`, which
  the package never calls.
- **The harness, prototyped** (the prompt's A6.2, as the package's `TestTheActor` does it):
  `open(label)` opens `sp.sp_mod.ShardedPool(version_label=label, db_name=<tmp>/store.db,
  ShardKeyType=…, ShardKeyStoreIdGetter=get_shard_key_store_id, replicated_tables=…,
  sharded_tables=…, shards=1, factories=factories)` inside `contextlib.redirect_stdout`, takes
  `pool._version.store_id` and `pool._shard_db_files[0]`, closes it (`__exit__`), and builds
  `sp.DatastoreClass(version_label=label, db_name=<shard file>,
  replicated_tables=list(replicated_tables), factories=factories, my_name=<unique>,
  serial_broker=<the test's broker Handle>)`; then
  `broker.notify_largest_store_ids.remote(actor.read_largest_store_ids()).get()` and
  `actor.set_version(serial)`. The broker is `sp.Handle(sp.BrokerClass(name="SerialPoolBroker"),
  "SerialPoolBroker", cluster, None)`, one per test. Re-opening the same primary under another
  label, while earlier actors are still open on the shard, works: the pool finds or writes the
  label's row and refuses nothing (no foreign-key check at open, one shard so nothing to
  reconcile).
  - **Results on the prototype:** `test_version_keyed_lookups` 11 run, only **d4** errors (it
    builds `DS.__new__(DS)`; deleted by A7.2); **d2 passes** (deleted anyway, as the prompt says);
    `test_bbn_failure_lookup` 6, `test_first_bounce_round_trip` 5, `test_fixed_T_values_round_trip`
    5, `test_scalarmodel_failure_reason` 4 and `ComputeTargets/tests/test_foreign_bbn_provenance`
    6 **all pass with no change to their bodies or imports**. Test a's one change (A6.4) was
    `store_a._version_serial` on each side.
  - **Breakage 5** on the prototype (`set_version` dropped): 8 errors, all
    `RuntimeError: Datastore "probe0000-store": cannot insert into "ScalarModel" before the version
    serial is set: …` (7) or `"ExponentialCoupling"` (1, test e), plus d4's. The refusal is on the
    insert, which every test reaches before its first keyed lookup.
  - The pool's primary is `store.db` and the actor's file is `store-shard0000.db`. No importer reads
    the harness's file path directly (`git grep` for `self.db`/`self._dir`).
- **The stand-in's copy** (A6.1). `datastorekit/tests/standin_pool.py` at `v0.3.0` is 485 lines,
  with no licence header; DatastoreKit is Apache 2.0 (`LICENSE` at the tag), as ChamPBH is. ChamPBH's
  copy carries **ChamPBH's full 14-line header** (`utilities.py`'s, ending
  `# limitations under the License.` — 05's review had to complete a truncated one, `e513e8c`),
  then the provenance. Its only import of what the wheel lacks is `open_pool`'s and
  `open_pool_output`'s `from datastorekit.tests.client.registry import …` (lazy, inside each
  function). The module-level imports (`datastorekit.SQL.ShardedPool`, `.Datastore`,
  `.SerialPoolBroker` through `importlib`) all resolve on the installed package.
- **The Datastore suite**, derived: 73 − 4 (`test_shard_key_assignment`) − 2 (d2, d4) − 3
  (`TestLayerReadsConfig`) = **64 before the A8 tests**: `bbn_failure_lookup` 6, `compare_stores` 21,
  `datastore_config` 4 + N, `first_bounce_round_trip` 5, `fixed_T_values_round_trip` 5,
  `package_provenance` 10, `scalarmodel_failure_reason` 4, `version_keyed_lookups` 9. On the
  prototype, the suite with only the deletions of the layer and test module ran 63 with
  `test_datastore_config` failing at import (`No module named 'Datastore.SQL.ClientPool'`), d4 and
  test 14; with test 14 amended, `test_compare_stores` gives 21 OK.
- **`import config.datastore` alone** exits 0 on the prototype (27 entries): the cycle is gone with
  the layer.
- **The schema** (§2.3.4), on the prototype: a stand-in pool, 16 shards, label `2026.6.0`,
  `factories` and `serial_batch_sizes` from `config.datastore`, closed, `ray.is_initialized()`
  `False`; `--schema <tmp>/store/store.db --against
  .documents/dk-and-rpk-migration/out/02-reference-schema.json`: **`RESULT: the schemas agree`,
  exit 0**. The orchestrator's snapshot is kept in the scratchpad for the review.
- **U4's closure** (`dependent_tables` over `drop_groups`, on the prototype; the tables a request
  leaves out):

  | `--drop` | Missing dependents | So |
  |---|---|---|
  | `scalar-model` | `AdiabaticHistory`, `AdiabaticHistoryValue`, `AdiabaticHistory_tags`, `BBNData`, `BBNDataValue`, `BBNData_tags` | refused: add `adiabatic-history bbn-data` |
  | `scalar-model adiabatic-history` | the three `BBNData` tables | refused: add `bbn-data` |
  | `scalar-model bbn-data` | the three `AdiabaticHistory` tables | refused: add `adiabatic-history` |
  | `adiabatic-history`, `bbn-data`, both, all three, none | — | accepted |

  Every dependent is in a group, so the "named as a table" branch is not reached by this registry
  (a test may reach it with a registry of its own). `dependent_tables` returns the registry's table
  order, not `drop_groups`'. The two mixed refusals are not in the prompt's A8.3 list; testing them
  is the agent's choice, logged.
- **The drivers.** Each `ShardedPool(` call passes `version_label`, `db_name`, `ShardKeyType`,
  `ShardKeyStoreIdGetter`, `replicated_tables`, `sharded_tables`, `timeout`, `profile_agent`,
  `job_name`, `prune_unvalidated`, `read_table_config`, `inventory_config`, and `main.py`'s also
  `shards` and `drop_actions`: every one but the last two is a name the package's constructor takes
  (`datastorekit/SQL/ShardedPool.py:106-125`). The package's `ProfileAgent.__init__(db_name,
  timeout, label, notify_time_interval)` equals ChamPBH's, so the three
  `ProfileAgent.options(…).remote(…)` calls are unchanged. `main.py --help` imports everything in
  about 2 s, so A8.5's subprocess needs a generous timeout, not a long one. `--drop` has
  `choices=allowed_drop_actions`, so argparse refuses an unknown group (exit 2) before
  `tables_to_drop` sees it.
- **`black` 26.1.0** is clean at `HEAD` on every file 08 will touch (52 files checked: the drivers,
  `pipeline_selection.py`, `config/*.py`, `Datastore/tests/*.py`,
  `test_foreign_bbn_provenance.py` and the 30 import sites). Only changed lines should move.
- **`.documents/OPEN_ISSUES.md`**: **26 open**, 3 on this board (`### 1.11`, `:188`), last updated
  2026-10-12. After 08: still 26 and 3 (one closed, one opened), the header's prose for this board
  extended ("prompt 08 closed one and opened one, 2026-10-12"). `[00-two-files-are-not-black-clean]`
  is at `:51-52`, and its board entry at `prompts/review-remediation/IMPLEMENTATION_STATE.md:213-215`,
  as §1 says.
- **Ignored `__pycache__/`** of the deleted modules stay under `Datastore/` and `Datastore/SQL/`
  unless removed. Python 3 never imports a `.pyc` from `__pycache__/` without its source, so they
  are inert; removing them (as 04 did for `RayTools/`) is allowed, and neither is a deviation.
- **Siblings:** SecondaryGWKit `a5a6f6d`, StochasticInstantons `5fb4a1d`, RayPoolKit `1b84df0`,
  DatastoreKit `04a2ed9` (ahead of the tag; read the tag through `git show v0.3.0:<path>`), each
  with a clean working tree. Nothing is written in any.
- **The rehearsal stores** (104 entries to depth 2 under
  `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/`) are 02's and 06's. 08 opens none.
- **The suites at `617a1dc`** (07's review): CosmologyModels 18, ComputeTargets 114 (61 + 53, in
  the two pattern groups `test_[a-f]*.py` and `test_[g-z]*.py`), Datastore 73.
- **The load** at dispatch: about 12 (11.2 / 10.0 / 12.0 at 21:51, 12.2 / 11.4 / 12.0 at 21:59).

**What the review exists to establish.**
- **(E1) A1–A2, by the script** at the agent's commit: the 7 modules absent, the two `__init__.py`
  present and 0 bytes, outside `prompts/` 0 layer imports, 0 literals, 0 `import_module`; 0 imports
  of the moved names; `prompts/`'s one kept.
- **(E2) A3, the factories.** Each of the 17 factory modules' diff is its one import line (and,
  for the three, the `require_version_serial` line); no other line of any factory moved.
- **(E3) A4–A5, the pools and U4.** Three constructions with `factories=` and
  `serial_batch_sizes=` from `config.datastore`, `main.py`'s with `drop_tables=` computed before
  `ray.init`; `LambdaCDM` out of both lists; `tables_to_drop` refuses as the table above, with the
  pinned message, and `main.py --drop scalar-model` exits 2 before Ray.
- **(E4) A6, the harness.** The stand-in copy differs from the tag's file by its header and the two
  registry functions only; the harness is `TestTheActor`'s pattern with a broker; the five
  importers are unchanged (or change only imports); test a's one change.
- **(E5) A7–A8, the tests.** Exactly the named deletions and changes (test 14's included), the new
  tests not vacuous, the six breakages caught.
- **(E6) The schema** agrees with R0's snapshot; `out/08-standin-schema.json` is the stand-in
  store's.
- **(E7) Nothing else moved.** No file outside §5's list; the venv unchanged; no store opened but
  temporary ones; the siblings untouched; the drivers' imports resolve; the suites at 18 / 114 /
  64 + N.
- **(E8) The records.** The board's §1 row, A1–A9, `### 5.13 After prompt 08 (<date>)`; the three
  issue moves with the index in the same commit; the log in README §5.1's form.

**Conventions particular to 08.** prompt-01.md's conventions and 05's hold. In addition:
- **git.** The only commit on `dk-and-rpk-migration` is the agent's one. `git rm` for the deletions;
  every other path by explicit `git add`; `git diff --cached --name-only` shown, then
  `git commit -- <paths>` (the prompt's §6). No `checkout`, `switch`, `stash`, `reset --hard` or
  `rebase`. The dangling commit of §2.3.2 is made with `git commit-tree` on a `git write-tree` of
  the index and is on no branch; no `update-ref`.
- **DatastoreKit** is read through `git -C … show v0.3.0:<path>` (or an archive extracted into the
  agent's scratch), never written; its measuring script runs from its root with its own venv. SGK
  is read, not imported.
- **Scratch.** The schema build script, the import check and any temporary store live in the
  agent's subdirectory of the session scratchpad. No scratch file under `config/`, `Datastore/`,
  `tools/` or `.documents/`. `out/08-standin-schema.json` is the one output committed.
- **Ray.** None is started; no driver is run except A8.5's and §2.3.3's refusals, which exit before
  `ray.init` (that is what they test). If one does not, it is stopped and that is a §4 stop.

## 1. Before you dispatch

1. **The tree.** Record the branch and `HEAD`. `git status --short` shows only §0's untracked
   entries, and `git diff --cached` is empty. `git log --oneline c4bd666..HEAD` touches no code.
2. **Ray.** `ray_cluster.sh status` as §0; `pgrep` finds nothing.
3. **The venv.** `pip freeze` saved in the scratchpad (101 lines, as §0); `pip check` clean.
4. **The load.** `uptime`, recorded and passed on.
5. **Hygiene baselines**, for check 12: `/usr/bin/stat -f '%m %z %N'` over the rehearsal root to
   depth 2 (104 entries); the four siblings' `HEAD`s and `git status --porcelain`.
6. **The orchestrator's prototype**, kept in the scratchpad for checks 6 and 9: its harness module,
   its schema build script and snapshot, and DatastoreKit's script output at `c4bd666`.

The suites are not re-run at dispatch: no Python has changed since `617a1dc`, where they were 18 /
114 / 73, and the agent runs them first (§2.1.1).

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt (amended);
- the campaign README and this board;
- prompt 04's log (for `check_imports.py`) and prompt 05's log;
- `HEAD`, the load and §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the
  session scratchpad. The agent does not read `orchestrator/`, and does not read prompts 09 onward.

Tell it plainly:
- **One commit, at the end, by pathspec.** Its log is `logs/08-adopt-datastorekit.md`, in README
  §5.1's form, with the prompt's §6 additions.
- **The files it may change** are the prompt's §5 list. **Nothing in DatastoreKit, RayPoolKit or
  SGK, nothing in the venv, no factory beyond its imports, no test beyond those A6–A8 name.**
- **Measure before editing** (§2.1), and record the suites by module first.
- **Stop and ask** on any of the prompt's §4 conditions. §0's grep hits, the docstrings left for
  11, and test 14 (now named) are not stops. A behaviour of the package that differs from its
  contract is recorded and stopped on, never worked around (README §5 rule 11).
- **It replies with:**
  - the log's path and the commit; `git diff --cached --name-only` before it and
    `git show --stat HEAD` after it;
  - §2.1's measurements: the script's totals, the greps classified, the hook table;
  - the script's output at the dangling commit (totals and the `Datastore/tests/` and `prompts/`
    sections), with the dangling commit's SHA;
  - `standin_pool.py`'s `diff -u` against the tag's file, every hunk explained;
  - the harness's `open`, `setUp` and `tearDown`, and test a's changed line;
  - `tables_to_drop` and its message; `main.py`'s new lines before `ray.init`;
  - the new tests by name, and the six breakages with what caught each;
  - the schema comparison's output;
  - `check_imports.py`'s output, the two refusals' output and exit codes, `import config.datastore`
    and `sharded_store --help`;
  - the three suites by module, with the load, and `black --check`;
  - every deviation with its tag;
  - the "State handed to the next prompt" section, verbatim.

## 3. The review — thirteen checks

1. **Scope.** At the agent's commit, `git show --stat HEAD` touches only the prompt's §5 list: the 7
   deletions, the 2 emptied `__init__.py`, the 30 import sites, `config/{version,datastore,sharding,model_list}.py`,
   the three drivers, `pipeline_selection.py`, the named test files (and the four harness importers
   and `test_foreign_bbn_provenance.py` only if their imports changed), `CLAUDE.md`,
   `.documents/OPEN_ISSUES.md`, `prompts/review-remediation/IMPLEMENTATION_STATE.md`,
   `out/08-standin-schema.json`, the log and the board. `git status` shows §0's untracked set and
   nothing new. `git log` since this note shows no other commit of the agent's; `git branch` no
   leftover branch.
2. **E1, by the script.** DatastoreKit's script at the agent's commit, from its root: layer files
   absent at the commit: the 7; the two `__init__.py` 0 bytes (`git cat-file -s`); outside
   `prompts/`, 0 imports, 0 literals, 0 `import_module`; `prompts/run-integrity/planning-probes/…:18`
   the one left; 0 imports of the moved names. It equals the log's dangling-commit output but for
   the commit line.
3. **E2, the factories, by `ast`.** From the scratchpad, for each of the 17 factory modules and the
   13 `DatastoreObject` sites: `ast.dump` of the module at `HEAD~1` and `HEAD`, with the one
   rewritten `ImportFrom` normalised (module name only), is identical. The three factories'
   `require_version_serial` import is from `datastorekit.contract`. `config/version.py` keeps
   `VERSION_LABEL` (`"2026.6.0"`) and defines neither moved name.
4. **E3, the pools.** `git diff HEAD~1 HEAD -- main.py plot_by_beta.py plot_ScalarModel.py`: the
   import lines, the pool arguments, `inventory_config` gone, the `--drop` computation above
   `ray.init`, the `--inventory` refusal, the inventory block (`:1196-1344`) deleted, and nothing
   else. Each pool's other arguments unchanged. `config/sharding.py` and `config/datastore.py` lose
   `LambdaCDM` and nothing else of their data (`ast` comparison of the three dicts and the list,
   less that entry).
5. **E3, U4.** In a fresh process: `tables_to_drop` for each row of §0's table; the refusals' messages
   name the groups to add; the accepted sets return their tables. `main.py --database
   <scratch>/x.db --drop scalar-model` and `… --inventory` each exit 2, print their message, create
   nothing at that path, and leave `pgrep` empty. `allowed_drop_actions` equals `list(drop_groups)`.
6. **E4, the harness.** `diff -u` of `Datastore/tests/standin_pool.py` against the tag's file: the
   header and the two registry functions (and nothing in the stand-ins' behaviour). The harness
   against §0's prototype: a pool per `open`, `set_version`, a broker shared by the test's actors
   and notified as the pool notifies it, `tearDown` closing every actor and asserting
   `ray.is_initialized()` `False`. `git diff HEAD~1 HEAD` of the five importers: imports only, or
   nothing. Test a: the one line.
7. **E5, the tests and the breakages.** The deletions are exactly A7's (4, d2, d4, the three
   identity tests) and test 14's one assertion and docstring; `git diff` of the surviving test
   bodies shows nothing else. Read the A8 tests: A8.2 asserts the count 3; A8.3 asserts `main.py`'s
   order by `ast`; A8.5 uses a timeout and checks nothing is created. In `bash`, replay the six
   breakage diffs (`git apply --check`, `-R --check`, apply, run, revert): each fails as the prompt
   says; breakage 5's failures are the actor's insert refusal. `git status` clean after.
8. **The vacuity checks.** With the stand-in broker removed from the harness (scratch copy), the
   suites still pass (§0): confirm the log does not claim a test exercises it. The schema has no
   difference to list.
9. **E6, the schema.** Run the log's build script at the agent's commit, into the scratchpad;
   `--schema <store> --against` R0's snapshot and against the orchestrator's prototype snapshot:
   `RESULT: the schemas agree` each time. `out/08-standin-schema.json` equals a fresh `--snapshot`
   of that store but for `source`.
10. **E7, without Ray.** `import config.datastore` alone exits 0 and its 27 entries are instances of
    subclasses of `datastorekit.SQL.factory_base.SQLAFactoryBase`; `sharded_store --help` exits 0;
    `check_imports.py` (log 04's) over the three drivers: every import resolves, `ray.is_initialized()`
    `False`; `py_compile` of the drivers; `black --check` on every touched or added Python file.
11. **E7, the suites.** The agent's: 18, 114, 64 + N, by module, with the load. The orchestrator
    re-runs `Datastore/tests` and `ComputeTargets/tests/test_foreign_bbn_provenance.py` at the
    agent's commit; ComputeTargets in full only if the agent's figures are missing or odd.
12. **E7, hygiene.** `pip freeze` equals §1's copy; `pip check` clean. The 104 rehearsal mtimes
    unchanged. The four siblings at their §1 `HEAD`s, clean. No Ray process. No scratch file in the
    repository; no `.db` under the repository but the user's untracked ones.
13. **E8, the records.** The board's §1 row (✅, "this commit", the log), A1–A9, and
    `### 5.13 After prompt 08 (<date>)` with the schema result and the rollback (`git revert` alone;
    nothing in the venv; README §4.4). The issue moves: `[05-…]` to §4 with its index row deleted;
    `[08-plot-scripts-open-read-write-pools]` in §3 with the user's goal and what must be found
    first, and its index row; the black-clean row narrowed in the index and on its board; count 26,
    this board 3, the date and the header prose, all in the agent's commit. `CLAUDE.md`'s paragraph
    names the stand-in cluster. The log follows README §5.1, with §2.1's measurements and every
    observation for 11 (the docstrings of §0 among them).

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 08* paragraph in §5.13: what each
  check found, and anything handed on to 09. Fix small residue in a follow-up commit of its own:
  - "this commit" → the SHA (board and log);
  - this note added to the board's list of orchestrator notes, marked "used for 08";
  - the status line: 9 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the swap in one line (files, imports, pools), and the script's zeros;
  - the harness, and that its importers ran unchanged;
  - U4's refusal and its message;
  - the schema result;
  - the tests deleted, changed (test 14, amended at dispatch) and added, and the breakages;
  - the suites;
  - the issues closed, opened and narrowed.
- **Then 09** can be dispatched; its note is written after this review. Hand on: the harness's
  shape, that `inventory_spec` declarations will feed `dependent_tables` (so U4's table can change
  at 09 and its test with it), and the stand-in copy's registry functions.
