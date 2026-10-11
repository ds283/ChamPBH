# Prompt 01 — rehearsal tooling and run provenance

**Campaign:** [`README.md`](README.md) · **Board items:** **T1**–**T3** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-11, with prompts 02–04, after the user reviewed the plan and ruled on P1–P17
(planning commit `97f50f8`, branch `dk-and-rpk-migration`).
**Gate:**
- you are on branch `dk-and-rpk-migration`, and `git status` shows no tracked change. The untracked
  entries are expected and are not yours: `pilot-2026.6.0*.db` (17 files), `pilot-main.log`,
  `pilot-plot.log`, `pilot-out/`, `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh` and
  `prompts/jordan-normalization/`. Do not stage, move or delete any of them;
- the venv holds neither `datastorekit` nor `raypoolkit` (`./venv/bin/pip show` finds neither).

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. The comparison tool is the acceptance instrument of three
rehearsals and of the user's science rebuild; its correctness is the point of the prompt.

**Read first:**

1. `CLAUDE.md`, in full: the campaign invariants and "Repository mechanics".
2. [`README.md`](README.md): §0.1, §0.2 ("The stores"), §2.2 (01's charter), **§4.1** (the protocol
   the scripts serve), **§4.2** (the comparison), §4.3, §5 and §6.2 (P3, P5).
3. SGK's `RunRegistry.package_provenance()`, `/Users/ds283/Documents/Code/SecondaryGWKit/RunRegistry/__init__.py:119-169`,
   and its tests (`RunRegistry/tests/`, search for `package_provenance`). **Read; do not edit, and
   do not import from SGK** (README §5 rule 12). The user means ChamPBH to import SGK's
   `RunRegistry` one day (P5), so your helper has the same fields with the same meaning.
4. Every factory under `Datastore/SQL/ObjectFactories/`: each `register()` (its columns and foreign
   keys) and each `store` (what it writes, and from where). §2.2 below needs the list of
   bookkeeping columns, and the polymorphic references.
5. `Datastore/SQL/ShardedPool.py:80-135` and `:285-345` (how a ChamPBH primary records its shards),
   and DatastoreKit's `datastorekit/shard_paths.py` and `docs/client-contract.md` §5 at `v0.2.3`
   (how the package's primary records them, and which primary tables are the layer's). Read
   DatastoreKit through `git -C /Users/ds283/Documents/Code/DatastoreKit show v0.2.3:<path>`; edit
   nothing there.
6. `tools/bbn_from_store.py` (its docstring, `find_model`, and its read-only `sqlite3` opens) and its
   test `ComputeTargets/tests/test_bbn_from_store.py`: the existing precedent for reading a store
   without the layer, and for an imitation store in a test.
7. `pilot-main.log` (untracked, read it as text): the banners, the queue-summary lines and the
   super-Planckian warnings that `stage_counts.py` must parse.

---

## 1. What is wanted

Three things, none of which runs a build:
- **T1, run provenance (P5).** Each driver says, in its own output, which `datastorekit` and
  `raypoolkit` it ran with, so that every rehearsal log, and every later production log, records
  it.
- **T2, the comparison of two stores by value (P3, README §4.2).** `tools/compare_stores.py`, a
  lasting tool, tested. It reads ChamPBH's store format and the package's, through `sqlite3`
  read-only, never through either layer, so that a store built before the DatastoreKit swap can be
  compared with one built after it.
- **T3, the rehearsal scripts (README §4.1)**, which 02, 06 and 10 run unchanged.

---

## 2. What to build

### 2.1 T1 — `utilities.installed_package_provenance`

- **`installed_package_provenance(names=("datastorekit", "raypoolkit")) -> dict`** in `utilities.py`.
  It is SGK's `package_provenance()` in behaviour: for each name, `None` if the distribution is not
  installed; otherwise exactly `{"version", "url", "commit", "requested_revision", "editable"}`,
  read with the standard library from `importlib.metadata` and the distribution's PEP 610
  `direct_url.json`. A field the install does not record is `None`; `editable` is `True` only when
  `dir_info.editable` is true. An absent, unreadable or malformed `direct_url.json` gives `None`
  for the three VCS fields and `False` for `editable`. It never raises and never imports a package
  it describes.
- **`format_package_provenance(provenance) -> str`**, one line, for example:
  `>> packages: datastorekit 0.2.3 (https://github.com/ds283/DatastoreKit@0de2b161…, requested v0.2.3); raypoolkit not installed`.
  An editable install says `editable` visibly. The exact wording is yours; record it in the log,
  since 02, 06, 07 and 10 quote it.
- **The drivers.** `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py` each print that line once,
  after argument parsing and before they open a pool. Nothing else in them changes. It is printed,
  never a refusal.
- **Tests**, in `Datastore/tests/test_package_provenance.py`, with stand-in distributions and no real
  install: not installed; a VCS install at a tag (commit and requested revision); an editable
  install (`editable: True`); `direct_url.json` absent, and present but not a JSON object. And the
  line's format for each. How you stand in a distribution is yours (a temporary `.dist-info`
  directory on `sys.path`, or a patched `importlib.metadata.distribution`); say which.

### 2.2 T2 — `tools/compare_stores.py`

README §4.2 is the specification. This section adds what the factories show.

**Interface.**

```bash
./venv/bin/python tools/compare_stores.py A.db B.db [--json OUT]     # compare two stores
./venv/bin/python tools/compare_stores.py --snapshot A.db [--out F]  # write A's schema snapshot
./venv/bin/python tools/compare_stores.py --schema A.db --against F  # compare A with a snapshot
```

`A.db` is a primary. Exit 0 when nothing compared differs, 1 when something does, 2 when a store
cannot be read. Human-readable output by default; `--json` also writes the full result.

**Reading a store.**
- Open every file `sqlite3.connect(f"file:{path}?mode=ro", uri=True)`, and nothing else. Never open
  a store through `Datastore`, `ShardedPool`, `datastorekit` or Ray.
- Find the shards by the glob `<stem>-shard*<suffix>` beside the primary, as
  `tools/bbn_from_store.py` does. Check their number against the rows of the primary's `shards`
  table, and stop (exit 2) if they disagree. **Never open a path read from the `shards` table**: a
  ChamPBH primary records absolute paths, which for a copied store name the original's files (README
  §0.2, "A hazard").
- The client tables are the tables of the shards that are not the layer's. The primary's tables
  (`shards`, `shard_key_config`, `shard_keys`, `replicated_tables`, `sharded_tables`, and
  `replication_in_flight` where it exists) are the layer's, and are not compared. Check whether a
  ChamPBH primary holds any client table; the log says what you found.

**Identities** (README §4.2).
- A row's identity is its columns except the bookkeeping ones, each value in canonical form: a
  float by `float.hex` of the stored value, an integer, string, boolean or `None` as it is.
- **Foreign keys** (`PRAGMA foreign_key_list` on each table) are replaced by the identity of the row
  they name, on the same shard, recursively. The `version` column resolves to the version row's
  label.
- **Polymorphic references** have no foreign key and must be declared. `ScalarModel` stores three:
  `cosmology_type`/`cosmology_serial`, `potential_type`/`potential_serial` and
  `coupling_type`/`coupling_serial` (`Datastore/SQL/ObjectFactories/ScalarModel.py:116-126`,
  `:542-547`). Find every such pair in every factory, derive each type value's table from the code
  that sets it (the classes' `type_id`), and declare the map in the tool, with a test that each
  declared table exists in a store built as §2.4's tests build one. A type value the map lacks is a
  named problem, not a crash.
- Memoise identities by `(shard, table, serial)`, as a digest, so that a value table of tens of
  thousands of rows resolves its parent once. A science store's value tables are large.

**Bookkeeping columns** are reported, never compared. They are `serial` and `timestamp` in every
table, and every column whose value depends on the run rather than on the physics. Reading the
factories at `52142d7`, the candidates are:
- `label` on `ScalarModel`, `AdiabaticHistory` and `BBNData` (a job name and a time:
  `main.py:257`, `:520`, `:783`);
- the timings `compute_time`, `mean_RHS_time`, `max_RHS_time`, `min_RHS_time` (`ScalarModel`),
  and `BBN_compute_time`, `NP_compute_time` (`BBNData`);
- `extra_data` (`ScalarModel`, JSON), if any field in it is a timing; it is built by
  `ComputeTargets/ScalarModel.py:1231-1262`.

Confirm each by reading where it is written, find any the list misses, and do **not** count as
bookkeeping a column that the physics determines (`compute_steps`, `RHS_evaluations`, `z_samples`,
the failure columns, every abundance and every value-table column). If `extra_data` mixes timings
with physics, compare it field by field with the timing fields set aside, rather than dropping it.
The list is a constant in the tool, each entry with a comment saying why, and it is tested.

**Replicated tables.** Within one store, each shard's copy of a replicated table is compared with
the others' (by identity), and a difference is reported as a named problem. Between stores, one
copy is compared: the lowest shard's.

**The output.**
- Per client table: the counts of rows matched, in A only and in B only; each unmatched row with its
  identity rendered by physical labels (a parent by its own leaves, not a digest).
- **Near misses:** an A-only row and a B-only row of the same table whose resolved references, with
  their non-float columns, are equal are paired, and each float column that differs is printed with
  both values and the relative difference. This is what makes a miss readable.
- The bookkeeping columns that differ, counted, and kept apart from the result.
- Problems: divergent replicated copies, unresolved references, unknown type values.

**The schema snapshot.** For each client table on shard 0: its columns (name, declared type, not
null, primary key), its indices (as the tuple of their columns and whether unique, not by name), and
its foreign keys, as sets, written as sorted JSON. `--schema A --against F` reports each difference.
A difference in a column's order is not a difference; the package prepends `serial`, `version`,
`timestamp`, `stepping` in that order (contract §2).

### 2.3 T3 — the rehearsal scripts

Under `.documents/dk-and-rpk-migration/rehearsal/`, each runnable from the repository root, each
with `--help` (or a usage line for the shell scripts), each taking the rehearsal root from an
argument or the environment variable `REHEARSAL_ROOT`, whose default is
`~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration` (P2):

- **`build.sh NN STORE BUILD ADDRESS [--prune-unvalidated]`** — one build of README §4.1: the
  command there, verbatim but for its parameters, with `STORE` in `reference`/`resumed` and `BUILD`
  the slug (`reference`, `resumed-a`, `resumed-b`). Output to `$REHEARSAL_ROOT/logs/NN-BUILD.log`;
  the driver's pid to `$REHEARSAL_ROOT/logs/NN-BUILD.pid` as soon as it starts; and, when it ends,
  `** driver exit status N` appended to the log. It runs in the foreground; the caller backgrounds
  it.
- **`interrupt_when.py LOG PIDFILE --banner TEXT --after SECONDS`** — waits until `LOG` holds a line
  containing `TEXT`, then `SECONDS` more, then sends `SIGTERM` to the pid in `PIDFILE`, and prints
  what it did and when. It reads the log file; it starts nothing. If the driver exits before the
  banner appears, it says so and exits non-zero.
- **`stage_counts.py LOG...`** — for each log, each stage banner (`** CALCULATE …`), its queue
  summary counts without the rates (`Queue summary: 5 lookup (avg …), 5 compute …` → lookup 5,
  compute 5, store 5, validation 5), and the "models skipped" lines. Tested on a fixture copied from
  the shape of `pilot-main.log`'s lines (not from the file itself, which is untracked).
- **`run_readers.sh NN STORE ADDRESS`** — README §4.1's L3 on `$REHEARSAL_ROOT/NN-STORE.db`, into
  `$REHEARSAL_ROOT/NN-readers/`:
  1. a copy of the store's files into `NN-readers/before/`, for comparison only, which no layer ever
     opens;
  2. `main.py --config exponential.yaml --database <store> --shards 16 --inventory --ray-address <ADDRESS>`;
  3. `plot_by_beta.py --config exponential.yaml --database <store> --shards 16 --phi-init-Mp 5 --output NN-readers/plot_by_beta --ray-address <ADDRESS>`;
  4. `plot_ScalarModel.py --config exponential.yaml --database <store> --shards 16 --output NN-readers/plot_ScalarModel --ray-address <ADDRESS>`;
  5. `tools/bbn_from_store.py <stem> --beta 1.15 --M-Mp 1e-3 --small-network --variant prod`, one
     PRyMordial solve, on the network production uses;
  6. `tools/compare_stores.py NN-readers/before/<primary> <store>`.

  Each step's output to its own file in `NN-readers/`, each exit status recorded, and the script
  exits non-zero if any step did. Check each driver's flags against `config/argument_parser.py` and
  the tool's against its parser; a flag that does not exist is a stop, not a guess.

No script starts or stops a Ray cluster: 02, 06 and 10 do that themselves (README §4.1, "The
cluster").

### 2.4 The tests of T2

In `Datastore/tests/test_compare_stores.py`. Each test builds its stores in a `tempfile` directory
with `sqlite3` alone: a primary with a `shards` table, two shards, a replicated parameter table, a
`version` table, a sharded parent table with a foreign key into the parameter table and a `version`
column, a value table with a foreign key into the parent, one polymorphic pair, and one bookkeeping
column. At least:
1. two equal stores: empty result, exit 0;
2. one float changed in its last bit (`math.nextafter`): named, with both values, exit 1;
3. a row in one store only: named, by its physical labels;
4. equal content under different serials and a different shard assignment: empty result;
5. a bookkeeping column that differs: reported apart, exit 0;
6. a primary whose `shards` table holds absolute paths, and one holding bare names: both read, and
   the absolute paths never opened (point them at files that do not exist);
7. a shard count that disagrees with the `shards` table: exit 2;
8. two shards whose copies of a replicated table differ: a named problem;
9. an unknown polymorphic type value: a named problem, not an exception;
10. the near-miss pairing: a value row whose float differs is paired with its counterpart, not
    listed as two unrelated rows;
11. the schema snapshot round-trips, and a dropped index is reported;
12. no file the tool reads changes: every file's checksum and modification time, before and after;
13. the bookkeeping list and the polymorphic map: every entry names a column or table that the
    factories declare (read from each factory's `register()`, which needs no store and no Ray).

---

## 3. Acceptance

1. **T1:** §2.1's tests pass. No driver is run. In today's venv,
   `format_package_provenance(installed_package_provenance())` prints both packages as not
   installed (shown in the log), and a test reads each driver's `ast` and finds exactly one call of
   it, before the driver's `ShardedPool(` construction.
2. **T2:** §2.4's tests pass. `--help` runs. `black` is clean.
3. **T3:** each script prints its usage; `stage_counts.py`'s test passes. **No script is run on a
   real build, and no store but a test's temporary one is opened.**
4. **The suites:** CosmologyModels 18 and ComputeTargets 114, unchanged; Datastore 35 plus exactly
   the tests you add, the log counting them by module.
5. **Deliberate breakage** (README §5 rule 8), each caught by a test, each recorded as a diff:
   - `float.hex` replaced by `repr` rounded to 12 digits in the canonical form (test 2 must fail);
   - the glob replaced by the paths in the `shards` table (test 6 must fail);
   - `timestamp` removed from the bookkeeping list (test 5 must fail);
   - the provenance helper's `editable` read from the wrong key (its editable test must fail);
   - one driver's provenance call moved after its `ShardedPool(` (§3.1's test must fail).
6. **The records**, in the same commit: this board's §1 row for 01, items T1–T3 and a
   `### 5.2 After prompt 01 (<date>)` entry; a §3 entry for anything opened, with
   `.documents/OPEN_ISSUES.md` in the same commit (`CLAUDE.md`).

---

## 4. Stop conditions — stop and ask

- A ChamPBH primary turns out to hold client tables, or a shard to hold a layer table other than
  those §2.2 names: the comparison's premise has moved.
- A column cannot be classed as bookkeeping or physics from the code alone.
- A polymorphic type value cannot be mapped to a table from the code.
- Any suite's count falls, or an existing test fails.
- Meeting §3 would need a change to a factory, to the layer, to `RayTools/`, or to any driver beyond
  the one provenance line.

---

## 5. What this prompt does not do

- **Files it changes:** `utilities.py`; one line, with its import, in each of `main.py`,
  `plot_by_beta.py` and `plot_ScalarModel.py`; the new `tools/compare_stores.py`,
  `Datastore/tests/test_package_provenance.py`, `Datastore/tests/test_compare_stores.py` and
  `.documents/dk-and-rpk-migration/rehearsal/` (with any test fixture under `Datastore/tests/`);
  the log and this board. Nothing else.
- It runs no build, starts no Ray, and opens no store but a test's temporary one: not the pilot
  store, and not the science store.
- It installs nothing, and changes nothing in DatastoreKit, RayPoolKit or SecondaryGWKit.

---

## 6. The log

`logs/01-rehearsal-tooling-and-run-provenance.md`, using README §5.1. It holds:
- the provenance line's exact format;
- the bookkeeping list and the polymorphic map, each entry with the `path:line` that justifies it;
- what a ChamPBH primary was found to hold (§2.2);
- the deliberate-breakage record, as diffs;
- the three suites beside the dispatch baselines, Datastore by module.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`. `black` the files you change, and only those.
