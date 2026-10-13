# Prompt 11 — close the migration

**Campaign:** [`README.md`](README.md) · **Board items:** **Z1**–**Z7** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-13, after 10's review (`3974f70`, `1954adf`), as README §2.1 says. The facts
below were measured at `2623905`, a housekeeping commit after `1954adf` that removed two unused
imports from `Datastore/tests/test_datastore_config.py` (log 09, Observation 4). The orchestrator's
note hands you any later count, and your own measurement governs.
**Gate:**
- 10 landed and reviewed, with **R2, RayPoolKit G3 and DatastoreKit G3 holding** (board §2);
- on branch `dk-and-rpk-migration`, no tracked change in `git status` (the untracked entries named
  in prompt 01's gate are expected, and are not yours);
- no ChamPBH Ray process is running (`.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh
  status`). You start none.

**Closes:** nothing on §3; the campaign itself. **Opens:** only what the work finds.
**Recommended model:** **Sonnet**. Prose, records and measurements. One small sweep of comments and
docstrings, each changing what a line says about a deleted path and nothing else.

**Read first:**

1. [`README.md`](README.md): §0.1, §1, §2.2 (**11's charter**, as amended 2026-10-13), §3, §5,
   §6.2 (P14, P16, P17), §6.4, §6.6 and **§7**.
2. This board in full: §1, §1.1, §2 (the gates), §3 (the four open issues) and §5.13–§5.15 with
   their reviews.
3. [`.documents/dk-and-rpk-migration-verification.md`](../../.documents/dk-and-rpk-migration-verification.md)
   §1–§3: the headers, each section's first paragraph, §3.9 (DatastoreKit's G3) in full.
4. The two G3 records drafted by earlier prompts, in their logs' "State handed to the next prompt":
   [log 04](logs/04-adopt-raypoolkit.md) (RayPoolKit's) and
   [log 10](logs/10-rehearse-through-both-packages.md) (DatastoreKit's). And each log's
   "Observations not acted on" for 08, 09 and 10: the lines below in Z3 come from there.
5. **The two packages, read and never edited** (`/Users/ds283/Documents/Code/DatastoreKit`,
   `/Users/ds283/Documents/Code/RayPoolKit`):
   - DatastoreKit at the tag (`git -C … show v0.3.0:<path>`): `docs/client-contract.md` (§1–§9)
     and `docs/adoption/champbh.md` §9 (the acceptance) and §10 (what goes stale), with its four
     addenda; its extraction board's §2, `prompts/extraction/IMPLEMENTATION_STATE.md` (read at
     the checkout's `HEAD`), rows **G2** (the form a holding gate takes) and **G3** (the cell your
     draft replaces);
   - RayPoolKit at the tag: `README.md` "Using it", and `docs/adoption/champbh.md` §8 (the
     acceptance) and §9 (what goes stale); its extraction board's §2 at `HEAD`, rows **G1**, **G2**
     and **G3**.
6. `CLAUDE.md` ("Repository mechanics"), [`prompts/INDEX.md`](../INDEX.md),
   [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) (its header and §1.11), and
   `.documents/review-remediation-verification.md` §4.11, the pattern for the handover you add.
7. The user's two run scripts, `full_run_2026.6.0.sh` and `full_run_2026.6.0-refresh.sh`, in the
   working tree (untracked; read them, never edit, run or stage them).

---

## 1. What is wanted

Both packages are adopted, R2 equals R0 bit for bit through them, and both gates hold. 11 leaves
the repository saying so: the documents that describe the code describe the packages, nothing
outside the dated records names a deleted file as current, the measurements are taken one last
time, each package's gate record is drafted for its own board, and the user's next run has its
handover.

### 1.1 The facts (at `2623905`)

- **Both packages' scripts at `1954adf`** (the code of `2623905` but for the two imports), each run
  from its package's root with its own venv:
  - DatastoreKit `docs/extraction/measure_client_imports.py cpbh --commit <sha>`: the 7 layer modules
    absent (the two `__init__.py` present); 204 tracked `.py` files read, 0 unparsed; **1** import
    statement naming a layer module, in `prompts/run-integrity/planning-probes/datastore_version_probe.py:18`
    (history), and **0 outside `prompts/`**; 0 string literals; 0 `import_module` calls; 18
    factory-module imports (17 in `config/`, 1 top level); 0 imports of the moved names.
  - RayPoolKit `docs/adoption/measure_client_pools.py cpbh --commit <sha>`: `RayTools/` absent; 0
    imports, 0 string literals, 0 `import_module` / `__import__` calls; **17** `RayWorkPool(...)`
    calls, `constructs` 17; 240 non-`.py` lines naming `RayTools` or `RayWorkPool` (25 in
    `.documents/`, 215 in `prompts/`); `ray.is_initialized()` False.
- **The suites** at `2623905`: CosmologyModels **18**, ComputeTargets **114** (61 with
  `-p 'test_[a-f]*.py'`, 53 with `-p 'test_[g-z]*.py'`; about 100 s each), Datastore **97**.
- **The venv**: `pip freeze` 101 lines, `datastorekit` at `2f5417ea…`, `raypoolkit` at
  `eaef0ce6…`, `pip check` clean; the provenance line is log 07's.
- **The stale-path sweep** (§2.1) prints **19** lines at `2623905`, in 8 files:
  - `.documents/architecture-summary.md` 7 (`:229`, `:246`, `:302`, `:320`, `:349`, `:357`, `:1068`);
  - `tools/compare_stores.py` 3 (`:90`, `:109`, `:110`);
  - `Datastore/tests/test_compare_stores.py:845`;
  - `Datastore/tests/test_datastore_config.py:254`, `:281`: the guard that no pool passes the old
    layer's arguments, which **stays**;
  - and six lines in dated records, which **stay**: `.documents/OPEN_ISSUES.md:53`,
    `.documents/dk-and-rpk-migration-verification.md:12`, `:362`, `:620`,
    `.documents/integrator-audit-2026-09-30/README.md:69`,
    `.documents/review-remediation-verification.md:1248`.
- **`.documents/architecture-summary.md`** (1,118 lines) describes the deleted layer well beyond
  what the sweep catches: the tree at `:37-63` (`Datastore/`'s seven deleted files);
  §4 "The Database Layer" (`:213-392`: `DatastoreObject`, the actor, `ShardedPool`'s constructor
  table with `drop_actions` and `inventory_config`, `pool.inventory`, `SQLAFactoryBase` "in
  `base.py`", the registry "in `_factories` in `Datastore.py`"); §5.2 (`:450-478`), whose
  `replicated_tables` still lists `LambdaCDM` (P8 removed it at 08) and which does not name
  `config/datastore.py`; §9's two-pass example (`:917-934`), whose `object_get_vectorized` passes
  `x["shard_key"]` where the drivers now pass `{"shard_key": …}` (05); §12 (`:975-1028`), whose
  list of components "used with zero or minimal modification" names the layer's files; §13
  (`:1030-1097`), whose Step 3 registers in `Datastore/SQL/Datastore.py`; and §14 (`:1100-1118`).
  §3.2 is the `raypoolkit` constructor since 04, and is checked, not rewritten.
- **Prose the earlier logs left for 11** (README §5 rule 7: you change these tests' prose by name,
  here, and nothing else in them):
  - `tools/compare_stores.py:89-92` (comment) and `:107-111` (`GLOBAL_BOOKKEEPING`'s two reason
    strings) cite the deleted layer's lines (log 08, Observation 1). The two strings are
    documentation: nothing prints them, and the only reader iterates the dict's keys
    (`test_compare_stores.py:758`). What they say stays true of the package, but for one point:
    the package gives a **replicated** row one timestamp from the pool, the same on every shard
    (verification §3.2), where the old layer called `datetime.now()` on each shard.
  - `Datastore/tests/test_bbn_failure_lookup.py:29-33` and
    `ComputeTargets/tests/test_foreign_bbn_provenance.py:38-40` (module docstrings) say each builds
    "the undecorated `Datastore.__ray_actor_class__`"; since 08 the harness builds the package's
    actor through ChamPBH's stand-in cluster (`CLAUDE.md:68-74`).
  - `Datastore/tests/test_scalarmodel_failure_reason.py:95` (comment): "there is no serial broker
    here, so the row is given an explicit serial". There is one since 08; the rows still pass
    explicit serials.
  - `Datastore/tests/test_compare_stores.py:843-846` (`_create_shard`'s docstring) cites
    "`Datastore/SQL/Datastore.py` `_build_schema`".
  - `Datastore/tests/test_datastore_config.py:112` (`TestConfigDatastoreData`'s docstring): "exactly
    the data the layer held at prompt 04's tree", which is now less `LambdaCDM` (P8), as the
    module docstring already says.
- **`CLAUDE.md`'s "Repository mechanics"** (`:56-…`): the test bullet (`:61-77`) gives the three
  commands and no counts; nothing names the packages, the rule that a package defect is fixed
  upstream (README §5 rule 11; U3 is the precedent), or how a Ray cluster is run on this machine
  (U5, README §6.6).
- **The records.** This board: status line "LIVE", row 11 "⬜", items "of 11 are added when it is
  written" (now added: Z1–Z7). `prompts/INDEX.md`: header "Last updated: 2026-10-11 · 7 campaigns:
  0 planned, 1 live, 6 closed"; this campaign's row is stale (it says 7 landed). `.documents/OPEN_ISSUES.md`:
  **27 open**, 4 on this board (§1.11), last updated 2026-10-13. 11 closes none: all four stay
  open, unassigned (each is outside this campaign's scope by README §1).

### 1.2 The items

- **Z1 — `.documents/architecture-summary.md`** (P14; DatastoreKit checklist §10, RayPoolKit
  checklist §9).
  - **The tree** (`:37-63`): `Datastore/` shows what is there now (the two empty `__init__.py`,
    `SQL/ObjectFactories/` and its modules without `base.py`, `tests/`), a line naming the layer as
    the `datastorekit` package, pinned in `requirements.txt`, as `:35` does for `raypoolkit`; and
    `config/datastore.py` beside `config/sharding.py`.
  - **§4, rewritten** for the package. Say what ChamPBH supplies (the registry of factory
    *instances* in `config/datastore.py`, the drop groups and `tables_to_drop` (U4), the batch
    sizes; `config/sharding.py`'s tables and shard key; each factory's `register`, `build`, and
    where defined `store`, `validate`, `validate_on_startup`, `inventory_spec`) and what the package
    does (the actors, the broker, the pool, the read-only reader and inventory, `copy_store`,
    `delete_store`, the two tools), with **pointers to the client contract at the tag, not copies of
    it**. Keep, re-pointed, what is ChamPBH's and still true: the version-keyed lookups (`:294-300`;
    `VERSION_SERIAL_KEY` and `require_version_serial` now from `datastorekit.contract`, P9), the
    two-pass pattern, the shard key in mapping form. Begin the section with a dated note: rewritten
    2026-10-13 by this campaign's prompt 11; the layer it described is in git before `857ec73`.
  - **§5.2**: `replicated_tables` as `config/sharding.py` holds it now (no `LambdaCDM`), and a
    sentence on `config/datastore.py`. **§9**'s example: the mapping form. **§12–§14**: rewritten
    where they name the layer. An instanton project adopts `datastorekit` and `raypoolkit` as
    packages; what it writes is a registry, factories and a sharding config. §13's Step 3 registers
    in `config/datastore.py`. §14's items stay where they are true of the package, re-pointed.
  - **Every other line** of the file that names `RayWorkPool` (20 of the 25 non-`.py` lines
    §1.1 counts in `.documents/`) is read and left if it is true. Say so in the log, line by line.
  - The dated notes already in the file (`> **Note, 2026-…**`) are records and are not edited,
    though a section around them is rewritten. Move none; if one sits inside text you rewrite, keep
    it where it is.
- **Z2 — `CLAUDE.md`, "Repository mechanics" only.**
  - **The packages**, a new bullet: the database layer is `datastorekit` and the work queue is
    `raypoolkit`, each pinned to a tag in `requirements.txt`, never installed editable; ChamPBH
    supplies the registry and configuration (`config/datastore.py`, `config/sharding.py`); **a
    defect in either package is reported to and fixed in that package's repository, and ChamPBH moves
    to a new pin; it is never patched or worked around here** (README §5 rule 11). Name U3's
    `v0.2.3` as the precedent in a clause.
  - **The test commands with their counts**: the three, each followed by its count today (18, 114,
    97), and ComputeTargets' two halves (`-p 'test_[a-f]*.py'`, `-p 'test_[g-z]*.py'`) with one
    sentence on why (a Bash call is capped at ten minutes). Keep the bullet's sentences after the
    block as they are.
  - **Ray on this machine** (*added at 11's writing*, README §2.2): a run that needs a cluster here
    starts ChamPBH's isolated one with `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`
    (its own temp dir and ports, under the rehearsal lock shared with StochasticInstantons and
    SecondaryGWKit), passes `--ray-address 127.0.0.1:6380` to every driver, and stops it with the
    script; **no one runs `ray stop`**, which stops every Ray process on the machine. Two or three
    sentences; the script's usage text is the detail.
  - Nothing else in `CLAUDE.md` changes.
- **Z3 — the prose in code** of §1.1's list, and only that.
  - `tools/compare_stores.py`: the comment and the two reason strings name the package's file and
    lines at the tag (`datastorekit/SQL/ShardedPool.py`, `datastorekit/SQL/Datastore.py`, read in
    the installed package, which equals the tag), and the `timestamp` reason states the replicated
    case in as few words as it takes. Nothing else.
  - The five test files: the docstring or comment says what is true at 08's harness. Nothing else.
  - Each touched Python file is `black`-clean.
- **Z4 — the final measurements**, saved under `prompts/dk-and-rpk-migration/measure/out/11-*`:
  - both packages' scripts at your `HEAD` (they read a commit, not the working tree: say so; the
    orchestrator re-runs them at your commit), compared with §1.1's figures;
  - the three roots' counts, by module for Datastore, against §1.1;
  - `pip freeze` against 07's (101 lines), `pip check`, and the provenance line.
- **Z5 — verification §4, "The close"**, appended to `.documents/dk-and-rpk-migration-verification.md`:
  - a table of R0, R1 and R2: the tree (commit), the packages (none / `raypoolkit` /
    both, at their commits), the stores, each build's end (exit status) and each comparison's
    result, from §1–§3;
  - Z4's measurements;
  - **the two G3 records, drafted** for the packages' extraction boards (P16), each a fenced block
    ready to paste, **in that board's own form**:
    - **DatastoreKit's G3 cell** in the form of its G2 cell: `✅ 2026-10-13: holds. …`, mapping
      each of the checklist's §9 parts (and its `v0.3.0` addendum, "`v0.2.3` or later") to the
      ChamPBH commits and rehearsal that hold it, from log 10's record; then the cell's existing
      dated italic notes, **verbatim**;
    - **RayPoolKit's G3 cell** in the form of its G1 and G2 cells: `**holds** 2026-10-12, …`,
      mapping the checklist's §8 items 1–5 to `303324b`, `2f61943` and the scripts' figures, from
      log 04's record, then R1 and R2 as the runs under real Ray, then the checklist pointer the
      cell carries now.

    Neither is committed in its repository: the user makes those commits, or has a session there
    make them.
  - a closing list: each item of DatastoreKit's checklist §10 and RayPoolKit's §9 ("what goes
    stale"), and where it was done (04, 08 or 11) or why it is left (a dated record).
- **Z6 — the handover**, as `.documents/review-remediation-verification.md` **§4.12**, after §4.11,
  in its form: what this campaign did, what it supersedes by statement (§4.11's refresh route), and
  README §7's points, carried as the user's next steps:
  - every store built before `857ec73` is refused by the tree (D4): the science store, its
    bbn-tolerance copy, the user's pilot store. They stay on disk, readable by `tools/compare_stores.py`
    and `tools/bbn_from_store.py`, and by any commit before `857ec73`;
  - **the next science run is a full rebuild** into a fresh primary, and the bbn-tolerance refresh
    is subsumed by it; if `jordan-normalization` is planned next (`prompts/jordan-normalization/`,
    untracked), the rebuild is best made once, after it;
  - **what goes stale in each run script, line by line**, by its line numbers: the store path
    (the existing primary is refused), the `--drop` of the refresh script and its `cp`-based route
    (now `sharded_store copy`, and moot for a fresh build), the closing `--inventory` (read-only,
    before Ray, on the closed store; its `--shards` unused), `--prune-unvalidated` (already the
    default), and that no driver passes `--ray-address`. Say what each line would become; **do
    not edit, copy or run either script**;
  - a build on this machine takes the rehearsal lock and runs on ChamPBH's isolated cluster, with
    `--ray-address 127.0.0.1:6380` on every driver, never `auto` (README §6.6, §7);
  - **the check of the rebuild**: `tools/compare_stores.py` against `~/ChamPBH-stores-bt02/`'s
    store, with what it should show (the same histories, value by value; the bt02 copy's primary
    is ChamPBH's format, which the tool reads; `LambdaCDM`'s note is expected) and what may
    legitimately differ (anything `jordan-normalization` changes, if it lands first);
  - what the package adds for the user: `main.py --inventory` read-only, `python -m
    datastorekit.tools.shard_key_audit`, `python -m datastorekit.tools.sharded_store copy`, and
    `ShardedPool.closed_store_files` / `delete_store` for retiring a store;
  - the decisions README §6.4 leaves to the user.
- **Z7 — the records.**
  - **This board closes.** Status line **COMPLETE**, with the final suite counts; row 11 ✅ with
    "this commit" and its log; Z1–Z7 in §1.1; §2 gains no gate, but a line after the table that the
    two package records are drafted in verification §4 (P16); §3 keeps its four open issues,
    unchanged; a final `### 5.17 After prompt 11 (<date>)`, listing every issue this campaign leaves
    open with its owner (none assigned), and every decision left to the user (README §6.4, with
    the G3 commits and the merge into `main`, P17).
  - **`prompts/INDEX.md`**: this campaign's row **complete**, in the form of its neighbours (e.g.
    `bbn-tolerance`'s), with the date, the final suites, `§1.11; 4` from counting; the header's
    count ("0 live, 7 closed") and date.
  - **`.documents/OPEN_ISSUES.md`**: the header's description of this board (closed 2026-10-13, 12 of
    12 landed, four open), the count (27, by counting) and the date; §1.11's lead-in if it states
    the campaign's state. No row moves.

**Left for the user, not done here** (README §6.4): the merge of `dk-and-rpk-migration` into `main`
(P17); the two G3 commits in the package repositories (P16); the science-store rebuild (§7);
removing the rehearsal stores (§3); read-only plot pools (`[08-plot-scripts-open-read-write-pools]`);
renaming `Datastore` (P6); a required `--ray-address` (§6.6).

---

## 2. Verification

1. **The stale-path sweep.** Over `git ls-files` outside `prompts/`, skipping import statements
   and `import_module(` lines, a regex for the deleted layer modules in path and dotted form
   (`Datastore/object.py`, `Datastore/SQL/{ShardedPool,Datastore,ClientPool,SerialPoolBroker,ProfileAgent}.py`,
   `Datastore/SQL/ObjectFactories/base.py`), `ObjectFactories/base`, `RayTools`, `inventory_config`,
   `_drop_actions`, `_drop_order`, `_default_serial_batch_size` and `pool.inventory(`, with a
   lookbehind that keeps `datastorekit/…` and `datastorekit.…` out. The orchestrator's note gives its
   script; use it or your own, and quote it in the log. **Before any edit** it prints §1.1's 19
   lines (the non-vacuity). **After**: only the eight lines §1.1 says stay, and any new line of
   yours that names a deleted path *as deleted* (a dated note, the handover, verification §4),
   each classified in the log. A line that names one as current is a miss.
2. **Z3 changes prose only.** For each Python file Z3 touches: `ast.dump` of the module with
   docstrings removed is identical before and after, **except** `tools/compare_stores.py`, whose dump
   differs only in the two `GLOBAL_BOOKKEEPING` string values (show the differing nodes). Comments
   are not in the AST, so name each file's changed lines from `git diff -U0`.
3. **The documents.** `git diff --numstat` on `.documents/`: deletions only in
   `architecture-summary.md` and `OPEN_ISSUES.md`'s header, and none in either verification document
   (additive; invariant 6). Every path, function and line you cite in Z1 and Z6 exists at the tag or
   in the tree; give the list in the log.
4. **The index.** `.documents/OPEN_ISSUES.md`'s count equals its rows by counting
   (`grep -c '^- \`\['` or as the file counts; say which); §1.11's four rows agree with this board's
   §3. `prompts/INDEX.md`'s row agrees with this board.
5. **The suites.** The three roots, serially, in the foreground, **before** and **after** your edits:
   §1.1's counts both times. `black --check` on every Python file touched.

---

## 3. Acceptance

1. Z1–Z7 as §1.2 says.
2. §2.1's sweep prints the 19 lines before, and after only the lines §2.1 allows, each classified.
3. §2.2 holds for every touched Python file.
4. The index's count and date agree with its rows; this board, `prompts/INDEX.md` and
   `.documents/OPEN_ISSUES.md` agree with each other.
5. The three roots pass at §1.1's counts, before and after.
6. **No deliberate breakage beyond §2.1's non-vacuity run**: 11 adds no code and no test.
7. **One commit**, with the log `logs/11-close-the-migration.md` in README §5.1's form.

---

## 4. Stop conditions — stop and ask

- A gate of board §2 does not hold when you start, or a §1.1 figure differs at your `HEAD`.
- A line that §2.1 finds is code, not prose, or a Z3 line cannot be made true without changing
  what the code does.
- A sentence of a dated record (a verification document, an audit, a `> **Note, …**` block) would
  need rewriting rather than adding to.
- A document would need a statement about the package that neither the contract at the tag nor
  the installed package bears out.
- Any suite count moves.
- Anything would need an edit in DatastoreKit, RayPoolKit, SecondaryGWKit or StochasticInstantons,
  or in the user's run scripts.

---

## 5. What this prompt does not do

- **Files it changes:** `.documents/architecture-summary.md`; `CLAUDE.md` ("Repository mechanics");
  `tools/compare_stores.py` and the five test files of Z3, prose only;
  `.documents/dk-and-rpk-migration-verification.md` (§4 appended);
  `.documents/review-remediation-verification.md` (§4.12 appended); `.documents/OPEN_ISSUES.md`;
  `prompts/INDEX.md`; this board; the log; `measure/out/11-*`. Nothing else.
- It runs no build, opens no store (not even read-only), starts no Ray, installs nothing, and edits
  nothing outside this repository.
- It does not merge the branch, and makes no commit in a package repository.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
