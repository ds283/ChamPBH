# Orchestrator — prompt 05, move ChamPBH's data out of the layer

Read [`../README.md`](../README.md) first: §0.1 items 1 and 3, §2.2 (05's charter), §4 ("05 ships"),
§4.2, §5 (rules 4, 7, 8, 9, 10) and §6.2 (P6, P8). The conventions are those of
[`prompt-01.md`](prompt-01.md) §0 ("Conventions"), which are not repeated here except where 05
changes them.

**You do not write code.** You may:
- run the suites, the log's scripts and read-only Python checks, from the session scratchpad;
- build a temporary store with the undecorated actor in the scratchpad, and snapshot it;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`05-move-champbh-data-out-of-the-layer.md`](../05-move-champbh-data-out-of-the-layer.md)
**Board items:** M1–M4 · **Closes:** nothing · **Narrows:** nothing · **Opens:**
`[05-config-datastore-cannot-be-imported-before-the-layer]`, and only what else the work finds ·
**Model:** Sonnet, as the prompt recommends · **No network.**

**Gate:**
- 04 landed (`303324b`) and was reviewed (`cb0c675`); 05–06 were written (`0f1c25b`).
- `HEAD` is the commit that adds this note, or a later one that touches no code ("code" as
  prompt-01.md's gate defines it). `git diff --stat 303324b HEAD -- . ':!prompts' ':!.documents'` is
  empty at `0f1c25b`.
- One prompt at a time in this checkout.

## 0. What makes this prompt unusual

**It moves data that nothing compares by value, behind names that keep working whatever is
moved.** The layer keeps `_factories`, `_drop_actions` and `_default_serial_batch_size` as
module-level names, now bound by import. So every existing test, and the schema check, passes
whether the data moved, was copied, or was changed in the moving. Three pieces of evidence are not
vacuous, and the review leans on them:
- **the captured literals** (M4.1), taken from 04's tree *before* any edit, not from the agent's
  own new module. The review compares them with the orchestrator's own capture (below), entry by
  entry;
- **the identity test** (M4.2), without which a copy left in the layer passes M4.1. Breakage 4
  shows it;
- **the `ast` comparison of the moved definitions** (review check 3): the three dict literals in
  `config/datastore.py` dump identically to the ones removed from the layer, and no other statement
  of the layer changed.

**The schema check is necessary but weak.** The actor builds its tables from the registry, so if
the registry is the same objects the schema cannot move. It would catch an entry dropped or
reordered into a different table set, not a changed batch size or drop group. The data test covers
those.

**One test meets the import-order hazard already, and survives it silently.** 01 wrote
`Datastore/tests/test_compare_stores.py:707-714`'s `_registry()` to try
`from config.datastore import factories` first and fall back to the layer on `ImportError`. Run
alone, after the move, its first attempt hits the cycle and its fallback loads the layer, which
then loads `config.datastore` cleanly. See "The import-order hazard" below.

**What moves on purpose:**
- **New** `config/datastore.py`: a module docstring; the six `CosmologyConcepts` quantity classes
  and the factory imports the registry needs; `factories`, `drop_groups`, `serial_batch_sizes`.
- `Datastore/SQL/Datastore.py`: the `CosmologyConcepts` import (`:27-34`) and the 17 factory-module
  imports (`:36-75`, `:77-83`) go, with `_factories` (`:94-122`) and `_drop_actions` (`:128-136`);
  one import of the two old names from `config.datastore` comes in. **`ClientPool`, `base`
  (`SQLAFactoryBase`, used at `:124`), `ProfileAgent`, `config.version` and `utilities` stay.**
  `_drop_order` (`:137-142`, with its comment) stays.
- `Datastore/SQL/ClientPool.py`: `_default_serial_batch_size` (`:24-49`) goes; one import of it
  from `config.datastore` comes in. Nothing else.
- `main.py`: the seven shard-key arguments, and nothing else.
- **New** `Datastore/tests/test_datastore_config.py`.
- The log, the board, and `.documents/OPEN_ISSUES.md` (the §2.4 issue).

**The facts, checked by the orchestrator on 2026-10-12 at `0f1c25b`** (the code is `303324b`'s).
Pass them on. The orchestrator applied the move to a scratch copy of the tree (`git archive`) to
measure what follows; nothing in the repository was touched.
- **The tree.** Branch `dk-and-rpk-migration`, `HEAD` `0f1c25b` before this note. Untracked: the
  user's 17 `pilot-2026.6.0*.db` files, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`,
  `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh` and `prompts/jordan-normalization/`. None
  is the agent's. `git diff --cached` is empty.
- **Ray.** `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
- **The venv** is as 04 left it: `pip freeze` contains the one
  `raypoolkit @ git+https://github.com/ds283/RayPoolKit@eaef0ce6…` line and no `datastorekit`;
  `pip check` clean. 05 installs nothing.
- **The load** was high and climbing at dispatch: 172 / 66 / 29 at 09:00, 219 / 151 / 75 at 09:04
  (10 cores), from macOS system processes (`system_profiler`, `siriactionsd`), not from this
  campaign. `Datastore/tests` at `HEAD` still ran in 3.5 s: 66 OK. Run ComputeTargets in its two
  pattern groups.
- **§2.1.4's first `git grep`** returns more than the prompt's §1 names, because its pattern
  matches substrings. Every other hit is accounted for, and **none is a stop**:
  - the module-level uses: `Datastore.py:94` (`_factories`), `:187` (`register_factories(_factories)`),
    `:128` (`_drop_actions`), `:421` (`_drop_actions[action]`), `:138` and `:418` (`_drop_order`);
    `ClientPool.py:24` and `:167`; `Datastore/tests/test_compare_stores.py:713`;
  - **`self._factories`**, the actor's instance attribute (`Datastore.py:186`, `:274`, `:279`, `:285`,
    `:404`, `:446`, `:490`, `:570`, `:620`, `:730`, `:776`, `:834`) and the tests' `store._factories`
    (`test_scalarmodel_failure_reason.py:89`, `test_version_keyed_lookups.py:336`, `:389`): the
    actor's copy, filled by `register_factories`, not the module's name;
  - `register_factories` (`Datastore.py:260`), a method;
  - **`self._drop_actions(...)`** (`Datastore.py:208`) and `def _drop_actions(self, actions)`
    (`:409`): **an actor method of the same name as the module-level dict.** The method body reads
    the module global at `:421`. The alias import restores that global under its old name, so the
    method is unaffected; nothing is renamed;
  - `allowed_drop_actions` (`config/argument_parser.py:33`, `:267`; `main.py:53`, `:98`), which
    prompt §2.2 names and leaves alone.
- **§2.1.4's second `git grep`**: `ShardedPool.py:581` (the definition) and the seven calls,
  `main.py:204`, `:366`, `:416`, `:484`, `:622`, `:674`, `:747`. None in the plot scripts or `tools/`.
  Each passes `x["shard_key"]` on the following line, a `beta_value`
  (`config/sharding.py:109`, `ShardKeyType = beta_value`), and every sharded table's field is
  `"shard_key"` (`config/sharding.py:40-46`). So `ShardedPool.py:592-602` takes the mapping's
  field, the same key.
- **The captured data, at `0f1c25b`** (the orchestrator's capture, which the agent's must equal):
  - the registry: **27 entries, 23 distinct factory classes.** Six carry a quantity class as
    `self.ObjectType` (both quantity factories keep it so): `beta_value`
    (`sqla_dimensionless_quantity_factory`), and `M_value`, `Lambda_value`, `temperature`,
    `phi_value`, `pi_value` (`sqla_dimensionful_quantity_factory`). The other 21 have no
    `ObjectType`. The order is `Datastore.py:95-121`'s;
  - `drop_groups`: 3 groups, `scalar-model`, `adiabatic-history`, `bbn-data`, each a list of three
    tables in `:129-135`'s order;
  - `serial_batch_sizes`: 24 entries, `LambdaCDM: 5` among them (P8), in `ClientPool.py:25-48`'s
    order.

  The same capture on the scratch copy with the move applied gives byte-identical output.
- **The two module names.** `import Datastore.SQL.Datastore as m` binds the *actor* (prompt
  §2.3.1). `importlib.import_module("Datastore.SQL.Datastore")` and
  `sys.modules["Datastore.SQL.Datastore"]` give the module (checked). `import
  Datastore.SQL.ClientPool as c` gives the module, since `Datastore/SQL/__init__.py` re-exports
  only the actor.
- **The schema, measured both ways.** The prompt's build (undecorated actor,
  `version_label="2026.6.0"`, `db_name=<tmp>/store-shard0000.db`, engine disposed; a primary
  `<tmp>/store.db` holding `CREATE TABLE shards (serial INTEGER PRIMARY KEY, filename TEXT)` and one
  row naming the shard) works without Ray: `ray.is_initialized()` `False`. **`--schema` takes the
  store, `--against` the snapshot file**: the prompt's "`--schema <yours>`" means
  `--schema <tmp>/store.db`, not a JSON. Measured:
  - `--snapshot` of the store at `0f1c25b`'s tree, and of the moved copy's: the two JSON files differ
    only in `"source"`;
  - `--schema <moved>/store.db --against <0f1c25b's snapshot>`: `RESULT: the schemas agree`, exit 0;
  - `--schema … --against .documents/dk-and-rpk-migration/out/02-reference-schema.json` (27
    tables): `RESULT: the schemas agree`, at both trees.
- **The import-order hazard, measured on the moved copy** (`PYTHONPATH=. python -c "<stmt>"`):

  | First import | Outcome |
  |---|---|
  | `import config.datastore` | **`ImportError: cannot import name 'serial_batch_sizes' from partially initialized module 'config.datastore'`** |
  | `import config.sharding, config.datastore` | the same `ImportError` |
  | `import Datastore, config.datastore` | the same (`Datastore/__init__.py` does not import `Datastore.SQL`) |
  | `import Datastore.SQL.Datastore, config.datastore` | works |
  | `import Datastore.SQL.ClientPool, config.datastore` | works |
  | `import Datastore.SQL.ShardedPool, config.datastore` | works |
  | `import Datastore.SQL.ObjectFactories.ScalarModel, config.datastore` | works |
  | `import Datastore.SQL.ObjectFactories.base, config.datastore` | works |

  The name in the message is `serial_batch_sizes`, not `factories`: `Datastore.py` imports
  `ClientPool` before its own `config.datastore` import, and `ClientPool` meets the partial module
  first. The prompt's "`cannot import name …`" covers it.
  - **`test_compare_stores._registry()`, run first in a process**, meets the same `ImportError`,
    catches it, and falls back to the layer, which returns `config.datastore.factories` itself
    (identity checked). `python -m unittest Datastore.tests.test_compare_stores` alone: 21 OK. The
    whole `Datastore/tests` on the moved copy: 66 OK. **This is not a §4 stop**: it is a fresh
    `from config.datastore import …` in a process that has not imported the layer, which is the
    case §2.4 names, and 01's `try`/`except` was written for it. The log records it with the §2.4
    measurement, and the issue's impact statement names it. The test is not edited (README §5
    rule 7).
- **`black` on M3** (measured on the moved copy, black 26.1.0): six calls stay on one line; the
  `AdiabaticHistory` call at `main.py:416-418` is split into three argument lines. So `git diff
  main.py` is 7 hunks, 7 lines removed and 9 added, and **every `main.py` line after `:418` moves by
  +2**: the compute sites `:541` and `:828` become `:543` and `:830`, and the `ShardedPool(` at
  `:1346` becomes `:1348`. The pool sites after `:413` move likewise (`:481` … `:828` become `:483` …
  `:830`). The log gives the new pool-site lines, for 06.
- **The `ast` test's counts**: 7 `object_get_vectorized` calls in `main.py`, 0 in `plot_by_beta.py`,
  0 in `plot_ScalarModel.py` (by `git grep` at `0f1c25b`).
- **`black --check`** is clean at `0f1c25b` on `main.py`, `Datastore/SQL/Datastore.py`,
  `Datastore/SQL/ClientPool.py`, `config/*.py` and `Datastore/tests/test_compare_stores.py`.
- **The suites at `303324b`** (04's review): CosmologyModels 18, ComputeTargets 114 (61 + 53),
  Datastore 66. Expected after 05: 18, 114, 66 + the new module's tests.
- **`.documents/OPEN_ISSUES.md`** has **25 open**, 2 on this board, last updated 2026-10-12. After
  05: 26, and 3 on this board, with the header's prose for this board extended ("prompt 05 opened
  one, 2026-10-12"). The row goes in `### 1.11 dk-and-rpk-migration` (`:188`).
- **DatastoreKit's checklist has a fourth addendum**, *v0.3.0 (object-timestamp prompt 02,
  2026-10-12)*, added after this campaign was planned. It says ChamPBH may pin `v0.2.3` or
  `v0.3.0`, and that nothing changes for ChamPBH but one key in a `DatastoreObject`'s `vars()`.
  **It changes nothing for 05**, which runs on ChamPBH's own layer; the pin is 07's, and the choice
  is the user's. Its insertion moved the checklist's later lines by six: items 4 and 6 are found by
  heading. The agent reads it, does not act on it, and may mention it under "Observations not
  acted on". DatastoreKit's `HEAD` is `30d2fc8`, its tree clean.
- **SGK** at `cf978c6`, tree clean; `config/datastore.py` exists. **RayPoolKit** at `1b84df0`,
  clean. Nothing is written in any of the three.
- **The rehearsal stores** under `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/` (42
  entries with `logs/`) are 02's. 05 opens none of them; it reads only
  `.documents/dk-and-rpk-migration/out/02-reference-schema.json`.

**What the review exists to establish.**
- **(E1) M1, verbatim.** `config/datastore.py`'s three literals dump by `ast` identically to the
  ones the layer held at `HEAD~1`, under the new names; its imports are those the registry needs;
  its docstring says what each object is, that the old layer reads them until 08, and the hazard.
- **(E2) M2, and nothing else in the layer.** The layer's diff is the removed definitions, their
  now-unused imports, and the added imports; every name the layer still uses is still imported;
  `_drop_order` stays; the three old names are the same objects as `config.datastore`'s.
- **(E3) M3, and nothing else in `main.py`.** Seven hunks, each the shard-key argument becoming
  `{"shard_key": x["shard_key"]}`, black's split aside.
- **(E4) The schema** at the agent's commit agrees with 04's and with R0's snapshot.
- **(E5) The tests pin it.** The captured literals equal the orchestrator's capture; the identity
  and `ast` tests are not vacuous; the four breakages are caught.
- **(E6) Nothing else moved.** No file outside §5's list; the venv unchanged; no store opened; the
  sibling repositories untouched; the suites at 18 / 114 / 66 + N; the drivers' imports resolve.
- **(E7) The records.** The board's §1 row, M1–M4, `### 5.7 After prompt 05 (<date>)`; the §3
  issue with the index's row, count and date in the same commit.

**Conventions particular to 05.** prompt-01.md's conventions hold. In addition:
- **git.** The only commit on `dk-and-rpk-migration` is the agent's one. No `checkout`, `switch`,
  `stash`, `reset --hard` or `rebase`. The capture and the "before" schema are taken from the
  working tree before the first edit, not by checking out an older commit.
- **Scratch.** The capture script, the schema build script and any temporary store live in the
  agent's subdirectory of the session scratchpad. No scratch file under `config/`, `Datastore/`,
  `tools/` or `.documents/`.
- **Ray.** None is started, and no driver is imported or run.

## 1. Before you dispatch

1. **The tree.** Record the branch and `HEAD`. `git status --short` shows only the untracked
   entries of §0, and `git diff --cached` is empty. `git log --oneline 0f1c25b..HEAD` touches no
   code.
2. **Ray.** `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
3. **The venv.** `pip freeze` saved in the scratchpad; `pip check` clean.
4. **The load.** `uptime`, recorded and passed on.
5. **Hygiene baselines**, for check 11: the rehearsal stores' mtimes (`/usr/bin/stat -f '%m %z %N'`
   over the directory and its `logs/`, 42 entries); `HEAD` and `git status --porcelain` of
   DatastoreKit, RayPoolKit and SGK.
6. **The orchestrator's capture and snapshot**, kept in the scratchpad for checks 5 and 7: the
   capture's output, the `0f1c25b` schema snapshot, and the build and capture scripts.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README and this board;
- prompt 04's log (for `check_imports.py`);
- `HEAD`, the load and §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the
  session scratchpad. The agent does not read `orchestrator/`, and does not read prompt 06 onward.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/05-move-champbh-data-out-of-the-layer.md`, in README
  §5.1's form, with the prompt's §6 additions.
- **The files it may change** are the prompt's §5 list. **No existing test, nothing in DatastoreKit,
  RayPoolKit or SGK, nothing in the venv.**
- **Capture before editing**, and the schema before editing.
- **Stop and ask** on any of the prompt's §4 conditions. The grep hits and `_registry()`'s fallback
  of §0 are not stops.
- **It replies with:**
  - the log's path and the commit;
  - `git diff --cached --name-only` as committed;
  - the two `git grep`s, classified;
  - the capture's output and script;
  - the `git diff` of the layer's two files and of `main.py`;
  - the schema build script, the two comparisons and their results;
  - the import-order measurement, `_registry()`'s behaviour, and the issue's text;
  - the new tests, by name, and the four breakages with what caught each;
  - `check_imports.py`'s output and `py_compile`;
  - the three suites, with the load, and `black --check`;
  - every deviation with its tag;
  - the "State handed to the next prompt" section, verbatim.

## 3. The review — twelve checks

1. **Scope.** At the agent's commit, `git diff HEAD~1 HEAD --stat` touches only the two new files,
   `Datastore/SQL/Datastore.py`, `Datastore/SQL/ClientPool.py`, `main.py`, the log, the board and
   `.documents/OPEN_ISSUES.md`. `git status` shows the §1 untracked set and nothing new. `git log`
   since this note shows no other commit of the agent's, and `git branch` no leftover branch.
2. **E2, the layer's diff.** `git diff HEAD~1 HEAD -- Datastore/SQL/` is the removed definitions,
   the removed imports and the added imports, and nothing else. For each name the layer still uses
   (`SQLAFactoryBase`, `SerialPoolManager`, `SerialLeaseManager`, `ProfileBatcher`,
   `ProfileBatchManager`, `VERSION_SERIAL_KEY`, `WallclockTimer`), the import stays; for each import
   removed, no use is left (an `ast` name check from the scratchpad). `_drop_order` is at the layer.
3. **E1, verbatim, by `ast`.** From the scratchpad: parse `Datastore/SQL/Datastore.py` and
   `ClientPool.py` at `HEAD~1` (`git show`) and `config/datastore.py` at `HEAD`. The three dict
   values dump identically (`ast.dump`, no attributes). `config/datastore.py`'s imports are the
   removed ones (the six quantity classes and the 17 factory modules), and nothing the registry does
   not use. The docstring says what §1 M1 asks.
4. **E3, `main.py`.** `git diff HEAD~1 HEAD -- main.py` is seven hunks at the lines of §0. An `ast`
   comparison of `main.py` at `HEAD~1` and `HEAD`, with each `object_get_vectorized` call's second
   argument normalised, dumps identically.
5. **E5, the captured data.** The test's literals equal the orchestrator's capture of §1 step 6,
   entry by entry and in order. The log's capture script reads the layer, not `config.datastore`,
   and its output is the log's.
6. **E5, the tests and the breakages.** Read `test_datastore_config.py`: it imports the layer
   before `config.datastore`, with the comment; the identity test checks all three names against the
   module (not the actor); the `ast` test asserts 7 / 0 / 0. Run it. In `bash`, replay the four
   breakage diffs (`git apply --check`, `-R --check`, apply, run, revert): each fails the named test
   for the stated reason, and breakage 4 leaves the data test passing. `git status` clean after.
7. **E4, the schema.** Run the log's build script at the agent's commit, into the scratchpad;
   `--schema <store> --against` the orchestrator's `0f1c25b` snapshot, and against
   `02-reference-schema.json`: `RESULT: the schemas agree` each time; `ray.is_initialized()`
   `False`.
8. **The hazard and the issue.** From the root: `python -c "import config.datastore"` fails with
   the partially-initialised `ImportError`; `… -c "import Datastore.SQL.Datastore, config.datastore"`
   works; `python -m unittest Datastore.tests.test_compare_stores` alone passes (21). The board's §3
   entry `[05-config-datastore-cannot-be-imported-before-the-layer]` gives what, impact (naming
   `_registry()`'s fallback) and next step (08, P6, with `import config.datastore` in its
   acceptance). The index has the row under §1.11, the count 26 and this board's 3, and the date, in
   the same commit.
9. **E6, the drivers.** The log's `check_imports.py` (from log 04, re-run as the log keeps it): every
   import resolves, `ray.is_initialized()` `False`. `py_compile` of `main.py`. `black --check` on
   every touched or added Python file.
10. **E6, the suites.** The agent's: 18, 114, 66 + N, with the load. The orchestrator re-runs
    `Datastore/tests` at the agent's commit: 66 + N OK. ComputeTargets and CosmologyModels are not
    re-run unless the agent's figures are missing or odd: no test there imports `Datastore.SQL`.
11. **E6, hygiene.** `pip freeze` equals §1's copy; `pip check` clean. The 42 rehearsal mtimes
    unchanged. DatastoreKit, RayPoolKit and SGK at their §1 `HEAD`s, clean. No Ray process. No
    scratch file in the repository.
12. **E7, the records.** The board's §1 row (✅, "this commit", the log), M1–M4, and
    `### 5.7 After prompt 05 (<date>)` naming the rollback (`git revert` alone; nothing in the venv)
    and the new `main.py` pool-site lines for 06. The log follows README §5.1, with each check's
    command and output, and the breakages as diffs.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 05* paragraph in §5.7: what each
  check found, and anything handed on to 06. Fix small residue in a follow-up commit of its own:
  - "this commit" → the SHA (board and log);
  - this note added to the board's list of orchestrator notes, marked "used for 05";
  - the status line: 5 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the move in one line (three objects, two imports, seven arguments);
  - the schema result, and the captured data's counts;
  - the tests added, and the breakages;
  - the issue opened, and the `_registry()` fallback;
  - the suites;
  - DatastoreKit's `v0.3.0` addendum, as a decision for 07 that is the user's.
- **Then 06** (R1): its note is written after this review, with the pool-site lines as 05 left
  them.
