# Prompt 05 — move ChamPBH's data out of the layer

**Campaign:** [`README.md`](README.md) · **Board items:** **M1**–**M4** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-12, with prompt 06, after the reviews of 02 (`70184f1`) and 04 (`cb0c675`).
**Gate:**
- 04 landed and reviewed (board §1); `RayTools/` is gone and the drivers import `raypoolkit`;
- on branch `dk-and-rpk-migration`, no tracked change in `git status` (the untracked entries listed
  in prompt 01's gate are expected, and are not yours);
- the venv holds `raypoolkit` 0.2.0 at `eaef0ce6…` and no `datastorekit`.

**Closes:** nothing. **Opens:** one issue, named in §2.4, and anything else you find.
**Recommended model:** **Sonnet**. Three objects moved verbatim, two imports rewritten, seven call
arguments wrapped, two tests written, each measured.

**Read first:**

1. [`README.md`](README.md): §0.1 items 1 and 3, §2.2 (05's charter), §4 ("05 ships"), §4.2 (the
   schema snapshot), §5, and §6.2 (P6, P8).
2. DatastoreKit's ChamPBH checklist, `/Users/ds283/Documents/Code/DatastoreKit/docs/adoption/champbh.md`,
   items 4 ("What leaves the layer") and 6 ("Its call sites and pool construction"), and what the
   three addenda (`v0.2.1`, `v0.2.2`, `v0.2.3`) say of them: what the package will take
   (`ShardedPool(factories=…, serial_batch_sizes=…, drop_tables=…)`) and the mapping form of a
   vectorized get's shard key.
   **Read; do not edit anything in that repository** (README §5 rule 12).
3. SGK's `config/datastore.py` (`/Users/ds283/Documents/Code/SecondaryGWKit/config/datastore.py`),
   its docstring and the names it uses: `factories`, `drop_groups`, `serial_batch_sizes`. ChamPBH's
   module takes the same names. Its `tables_to_drop` is 08's (U4), not yours.
4. `Datastore/SQL/Datastore.py:1-142` (the imports, `_factories` at `:94-122`, `_drop_actions` at
   `:128-136`, `_drop_order` at `:138-142`) and the uses of each (`:186-187`, `:409-425`).
5. `Datastore/SQL/ClientPool.py:24-49` (`_default_serial_batch_size`) and its one use (`:167`).
6. `Datastore/SQL/ShardedPool.py:581-605` (`object_get_vectorized`: a `ShardKeyType` key, or a
   mapping holding the table's shard-key field, which `config/sharding.py` names `"shard_key"` for
   every sharded table).
7. The seven vectorized gets in `main.py`, each a `task_builder` lambda: `:204`, `:366`, `:416`,
   `:484`, `:622`, `:674`, `:747` (the shard-key argument is on the following line).

---

## 1. What is wanted

ChamPBH's own data lives inside its layer, and the package will be given it instead. This prompt
moves it out, **on ChamPBH's own layer**, so that 06 can show by a rehearsal that the move changed
nothing before 08 swaps the layer. No schema, behaviour or stored value changes.

- **M1 — `config/datastore.py`**, new, holding:
  - `factories`: `_factories` moved verbatim, with the imports it needs: 27 entries, each an
    instance, in the same order, as today;
  - `drop_groups`: `_drop_actions` moved verbatim: three groups;
  - `serial_batch_sizes`: `_default_serial_batch_size` moved verbatim: 24 entries, `LambdaCDM`
    included (P8: it leaves at 08).

  A module docstring in the manner of SGK's says what each object is, that the old layer reads them
  from here until 08 gives them to the package, and the import-order hazard of §2.4.
- **M2 — the old layer reads them from there.** `Datastore/SQL/Datastore.py` imports `factories`
  and `drop_groups`, and `Datastore/SQL/ClientPool.py` imports `serial_batch_sizes`, **under their
  old module-level names** (`from config.datastore import factories as _factories, …`), so that no
  other line of either file changes and `Datastore/tests/test_compare_stores.py:713`
  (`from Datastore.SQL.Datastore import _factories`) keeps working unedited (README §5 rule 7). The
  imports that only the registry used leave `Datastore.py`; any it still uses stay. `_drop_order`
  stays in the layer (08 deletes it with the layer: the package orders a drop by foreign key).
- **M3 — the seven vectorized gets pass a mapping.** In each, the second argument `x["shard_key"]`
  becomes `{"shard_key": x["shard_key"]}`. Nothing else in the calls changes. The old layer takes
  the field from the mapping and does what it did with the bare key (`ShardedPool.py:592-602`), so
  behaviour is unchanged; the package takes only the mapping. `black` will split some of these
  calls over more lines; that is formatting, not a change.
- **M4 — two tests**, in a new `Datastore/tests/test_datastore_config.py`:
  1. **The data, captured.** `config.datastore`'s three objects equal what the layer held at 04's
     tree, written into the test as literals: the registry as an ordered list of
     `(name, factory class name, constructor argument)`, the argument being the quantity class's
     name for the two quantity factories (they keep it as `self.ObjectType`) and `None` for the
     rest; `drop_groups` and `serial_batch_sizes` as literal dicts, order included. **Capture the
     literals from 04's tree before you edit anything** (a short script in your scratch, kept in the
     log), not by copying from your own new module.
  2. **The layer reads them.** `Datastore.SQL.Datastore._factories is config.datastore.factories`,
     likewise `_drop_actions is drop_groups` and `ClientPool._default_serial_batch_size is
     serial_batch_sizes`. Without this, a copy left in the layer passes test 1.
  3. **An `ast` test of the drivers**: every `object_get_vectorized(...)` call in `main.py`,
     `plot_by_beta.py` and `plot_ScalarModel.py` passes as its shard key (the second positional
     argument, or `shard_key=`) a dict literal whose only key is the string `"shard_key"`. It also
     asserts the count found, 7 in `main.py` and 0 in the plot scripts, so that it cannot pass by
     finding none.

  The test module imports the layer **before** `config.datastore`, with a comment saying why (§2.4).

---

## 2. What to do

### 2.1 Before

1. Record `HEAD`, and the three suites' counts (expected 18 / 114 / 66; ComputeTargets may be run in
   the two pattern groups `test_[a-f]*.py` and `test_[g-z]*.py`, 61 + 53).
2. **Capture the data** from 04's tree (M4.1), by importing the layer and reading `_factories`,
   `_drop_actions` and `_default_serial_batch_size`. Save the output.
3. **Build the 04 schema** (§2.3.1's method) and save it, before any edit.
4. `git grep -n '_factories\|_drop_actions\|_drop_order\|_default_serial_batch_size' -- ':!prompts'
   ':!.documents'` and `git grep -n object_get_vectorized -- '*.py' ':!prompts'`: record both, and
   check them against §1. Anything §1 does not account for is a stop.

### 2.2 The change

M1–M4, as §1 says. `config/argument_parser.py`'s `allowed_drop_actions` repeats the group names; it
is not touched (08's U4 replaces it).

### 2.3 Verification

1. **The schema does not change** (charter acceptance 1). The actor builds a store without Ray:
   - import the actor class (`from Datastore.SQL.Datastore import Datastore`, then
     `Datastore.__ray_actor_class__`; note that `import Datastore.SQL.Datastore as m` binds the
     actor, not the module, because `Datastore/SQL/__init__.py` re-exports it), and construct it with
     `version_label="2026.6.0"` and `db_name=<tmp>/store-shard0000.db`; dispose of its engine;
   - beside it, write a one-table primary `<tmp>/store.db` holding `shards` with one row naming the
     shard, which is all `tools/compare_stores.py` needs to read a store;
   - `tools/compare_stores.py --snapshot <tmp>/store.db --out <scratch>/NN-schema.json`.

   Do it at 04's tree (§2.1.3) and at yours, in temporary directories in your scratch. Then
   `--schema <yours> --against <04's>` and `--schema <yours> --against
   .documents/dk-and-rpk-migration/out/02-reference-schema.json` (R0's snapshot, 27 client tables):
   each must print `RESULT: the schemas agree`. The orchestrator built such a store at 04's tree
   and found it agrees with R0's snapshot. The build script, both commands and both results go in
   the log; `ray.is_initialized()` is `False` at its end.
2. **The new tests pass**, and the suites give 18 / 114 / 66 plus the new Datastore tests. A count
   that falls is a stop.
3. **The drivers' imports still resolve.** Prompt 04's log keeps its `check_imports.py`; run it from
   the repository root (it never executes a driver): every import resolves, `ray.is_initialized()`
   `False`. `py_compile` of `main.py` succeeds.
4. `git diff` of `Datastore/SQL/Datastore.py` and `Datastore/SQL/ClientPool.py` is the removed
   definitions and their now-unused imports, and the added imports; of `main.py`, the seven shard-key
   arguments.
5. `black --check` is clean on every Python file touched or added.

### 2.4 The import-order hazard, and the issue it opens

`Datastore/SQL/__init__.py` imports the actor (`from .Datastore import Datastore`), so importing any
factory module runs `Datastore/SQL/Datastore.py`. Once that file imports from `config.datastore`,
and `config.datastore` imports the factories, **importing `config.datastore` before the layer
fails**: `ImportError: cannot import name … from partially initialized module 'config.datastore'`.
The orchestrator reproduced it on a scratch copy of 04's tree with the move applied; importing
`Datastore.SQL.Datastore`, `ClientPool` or `ShardedPool` first works. Every driver and actor imports
the layer first, so nothing that runs today is affected.

- **Measure it at your tree** and record both outcomes in the log:
  `PYTHONPATH=. ./venv/bin/python -c "import config.datastore"` (expected to fail), and
  `… -c "import Datastore.SQL.Datastore, config.datastore"` (expected to work).
- **Do not work around it in the layer.** Emptying `Datastore/SQL/__init__.py` is 08's (P6), and
  `ShardedPool.py:24` imports the actor through it. A lazy import inside the actor would drop the
  module-level `_factories` that `test_compare_stores.py` imports.
- **Open it** as a §3 issue, `[05-config-datastore-cannot-be-imported-before-the-layer]`, with
  `.documents/OPEN_ISSUES.md`'s row, count and date in the same commit. Its next step: 08 removes
  the cycle by deleting the layer and emptying the two `__init__.py` files (P6), and its acceptance
  includes `python -c "import config.datastore"` from the root.

### 2.5 Deliberate breakage (README §5 rule 8)

Each applied to the working tree, shown to be caught, reverted, and recorded as a diff:
1. One vectorized get in `main.py` reverted to the bare `x["shard_key"]`: the `ast` test fails,
   naming the line.
2. One entry of `serial_batch_sizes` changed (`"ScalarModel": 20` → `21`): the data test fails.
3. A registry entry given another quantity type (`"M_value":
   sqla_dimensionful_quantity_factory(Lambda_value)`): the data test fails.
4. `ClientPool.py`'s import replaced by a literal copy of the dict: the identity test fails, and the
   data test still passes (which is why the identity test exists).

---

## 3. Acceptance

1. **M1–M2:** `config/datastore.py` holds the three objects, verbatim; the layer imports them under
   its old names; `_drop_order` is still in the layer.
2. **M3:** the seven vectorized gets pass `{"shard_key": …}`; `git diff main.py` changes nothing
   else.
3. **The schema does not change** (§2.3.1): agrees with 04's tree and with R0's snapshot.
4. **M4:** the new tests pass, and the suites give 04's counts plus them.
5. **The drivers' imports resolve**, and `black --check` is clean.
6. **The breakages of §2.5** are caught, and recorded as diffs.
7. **The records**, in the same commit: this board's §1 row for 05, items M1–M4, and
   `### 5.7 After prompt 05 (<date>)`; the §3 issue of §2.4 and any other, with
   `.documents/OPEN_ISSUES.md` in the same commit.

---

## 4. Stop conditions — stop and ask

- §2.1.4 finds a use of the moved data, or a vectorized get, that §1 does not account for.
- The schema snapshot differs from 04's or R0's.
- Any suite's count falls, or an existing test fails.
- A driver import does not resolve, or the import-order failure appears anywhere but a fresh
  `import config.datastore`.
- Moving the data needs a change to the layer beyond the imports of §1 M2.

---

## 5. What this prompt does not do

- **Files it changes:** adds `config/datastore.py` and `Datastore/tests/test_datastore_config.py`;
  changes `Datastore/SQL/Datastore.py` and `Datastore/SQL/ClientPool.py` (their imports and the moved
  definitions only), `main.py` (the seven shard-key arguments), the log, this board and
  `.documents/OPEN_ISSUES.md`. Nothing else.
- It changes no existing test, no factory, `config/sharding.py`, `config/argument_parser.py` or the
  plot scripts.
- It runs no driver, starts no Ray and opens no store but its temporary ones. The rehearsal is 06's.
- It installs nothing, and changes nothing in DatastoreKit, RayPoolKit or SecondaryGWKit.

---

## 6. The log

`logs/05-move-champbh-data-out-of-the-layer.md`, using README §5.1. It holds the captured data and
the script that captured it; the two `git grep`s; the schema build script and the two schema
comparisons; the import-order measurement; the deliberate-breakage record as diffs; and the three
suites beside 04's.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
