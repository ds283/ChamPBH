# Log 01 — rehearsal tooling and run provenance

**Subject:** Add run provenance and the tooling for the migration rehearsals
**Commit:** `d7791c5`, on branch `dk-and-rpk-migration`, on top of `0e64ef0`
**Date:** 2026-10-11 to 2026-10-12, by Claude Opus 5.5
**Result:** **COMPLETE.** T1–T3 shipped. Datastore 35 → 66 (+10 `test_package_provenance`, +21
`test_compare_stores`); CosmologyModels 18 and ComputeTargets 114 unchanged. All five deliberate
breakages caught. No §4 stop condition was met. One §3 issue opened, for a latent defect seen in
passing that this prompt did not fix (`[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]`).

## What shipped

### T1 — run provenance (`utilities.py`; the three drivers; `Datastore/tests/test_package_provenance.py`)

- `utilities.RECORDED_DISTRIBUTIONS = ("datastorekit", "raypoolkit")`.
- `utilities.installed_package_provenance(names=RECORDED_DISTRIBUTIONS) -> dict`. For each name,
  `None` if the distribution is not installed; otherwise exactly `{"version", "url", "commit",
  "requested_revision", "editable"}`, read through `importlib.metadata.distribution(name)` (called
  as a module attribute, `import importlib.metadata`) and the distribution's PEP 610
  `direct_url.json`. Its behaviour is that of SGK's `package_provenance()`
  (`SecondaryGWKit/RunRegistry/__init__.py:122-169`), which was read and not imported:
  - an absent `direct_url.json` (`read_text` gives `None`, so `json.loads` raises `TypeError`), one
    that is not JSON or not UTF-8 (`ValueError`), one that cannot be read (`OSError`), or one that
    is not a JSON object, gives `None` for `url`, `commit` and `requested_revision`, and `False`
    for `editable`;
  - a `vcs_info` or `dir_info` that is not an object is treated as absent;
  - `editable` is `dir_info.get("editable") is True`.
  It uses the standard library only (`importlib.metadata`, `json`, both new module-level imports in
  `utilities.py`), never imports a package it describes, and never raises on odd metadata.
- `utilities.format_package_provenance(provenance) -> str`, one line.
- **The provenance line's exact format** (quoted by 02, 03, 06, 07 and 10):

  ```text
  >> packages: ENTRY; ENTRY
  ```

  each `ENTRY`, in `RECORDED_DISTRIBUTIONS` order, one of:

  | Install | `ENTRY` |
  |---|---|
  | not installed | `NAME not installed` |
  | VCS at a tag | `NAME VERSION (URL@COMMIT, requested REVISION)`, the commit in full |
  | VCS, no requested revision | `NAME VERSION (URL@COMMIT)` |
  | editable | `NAME VERSION (editable, URL)` |
  | a local directory, not editable | `NAME VERSION (URL)` |
  | no usable `direct_url.json` | `NAME VERSION (no recorded origin)` |

  So at 07's pin it should read, for example,
  `>> packages: datastorekit 0.2.3 (https://github.com/ds283/DatastoreKit@0de2b161ba09362adc36231a7b5b12d99dec8bb3, requested v0.2.3); raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce6…, requested v0.2.0)`
  (the URL is whatever pip records in `direct_url.json`).
- **In today's venv** (`./venv/bin/python -c "import utilities as u;
  print(u.format_package_provenance(u.installed_package_provenance()))"`):

  ```text
  >> packages: datastorekit not installed; raypoolkit not installed
  ```

  and `installed_package_provenance()` is `{'datastorekit': None, 'raypoolkit': None}`.
- **The drivers.** `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py` each gain one import of
  the two names from `utilities` and one module-level statement,
  `print(format_package_provenance(installed_package_provenance()), flush=True)`, with a one-line
  comment above it, placed after the `if args.database is None: … sys.exit()` block and **before
  `ray.init(`** (the orchestrator's suggestion: a run that cannot reach its cluster still records
  what it ran with), and so before the `ShardedPool(` construction and before `main.py`'s
  `ProfileAgent`. Nothing else in the drivers changes (`git diff` shows the import, the comment and
  the print, and nothing else).

### T2 — `tools/compare_stores.py`

README §4.2 and prompt §2.2, as specified, with the choices listed under Deviations.

- **Interface:** `compare_stores.py A.db B.db [--json OUT] [--max-rows N]`;
  `--snapshot A.db [--out F]`; `--schema A.db --against F`. Exit 0 equal, 1 different (a row in one
  store only, a value that differs, or a named problem), 2 unreadable (a missing primary, a primary
  with no `shards` table, a shard count that disagrees, an SQLite error).
- **Reading:** every connection is `connect_ro(path)` =
  `sqlite3.connect("file:" + quote(path, safe="/") + "?mode=ro", uri=True)`; nothing imports
  `Datastore`, `datastorekit`, `ray` or SQLAlchemy. The shards are `sorted(glob(escape(<parent>/
  <stem>-shard) + "*" + escape(<suffix>)))`; their number must equal `select count(*) from
  shards`. The `filename` column of `shards` is never read.
- **Identities:** non-bookkeeping columns, canonical (`["f", float.hex]`, `["i", int]`,
  `["s", str]`, `["x", hex]`, `None`), each foreign key (`PRAGMA foreign_key_list`) and declared
  polymorphic reference replaced by `["ref", table, digest]` of the named row **on the same
  shard**, recursively; the `version` column so resolves to the version row's `label`. The
  digest is BLAKE2b-128 of the sorted `[column, value]` list with the table's name. Digests are
  memoised by `(shard, table, key column, key value)` for every table some reference names; a
  table that nothing references (a value table) is streamed and not memoised. Tables without a
  `serial` (the `*_tags`) are identified by their columns alone.
- **Classification:** a table the primary lists as replicated is compared by its lowest shard's
  copy, after every other shard's copy is checked equal to it (a difference, or a missing copy,
  is the named problem "replicated copies differ"); a table it lists as sharded is the union of
  every shard's rows; a table in neither list is classified by DatastoreKit's contract §5 rule
  (replicated if its foreign keys other than into `store_tag` all name replicated tables,
  otherwise sharded).
- **Output:** per table its class, matched / A only / B only; near misses with every differing
  float (`repr`, `float.hex`, relative difference); unmatched rows rendered by physical labels;
  the bookkeeping differences of matched rows, counted per column; notes; problems; a `RESULT:`
  line. `--json` writes the whole result.
- **Schema snapshot:** per client table on the lowest shard, `columns` = sorted
  `[name, declared type, not null, primary key]`, `indices` = sorted `[[columns…], unique]` (from
  `PRAGMA index_list`/`index_info`, autoindexes included, names dropped), `foreign_keys` = sorted
  `[[from…], table, [to…]]`; JSON with `format: "champbh-compare-stores-schema"`, `version: 1`.
  `--schema` compares by table, column name and set membership, so column order is not a
  difference.

**The bookkeeping list** (`GLOBAL_BOOKKEEPING`, `BOOKKEEPING_COLUMNS`), each entry with the line
that justifies it. Line numbers are at `0e64ef0` (the code is `52142d7`'s):

| Table | Column | Why it depends on the run |
|---|---|---|
| every table | `serial` | leased from the serial broker at insert (`Datastore/SQL/Datastore.py:309-314`, `:670-674`); resolved through, never compared |
| every table | `timestamp` | `datetime.now()` at insert (`Datastore/SQL/Datastore.py:679-680`) |
| `ScalarModel` | `label` | `f"{args.job_name}-ScalarModel-…-{datetime.now()…}"` (`main.py:257`), stored as `obj.label` (`Datastore/SQL/ObjectFactories/ScalarModel.py:541`) |
| `ScalarModel` | `compute_time` | `supervisor.integration_time` (`ComputeTargets/ScalarModel.py:1200`), a `perf_counter` interval (`Quadrature/supervisors/base.py:44-49`) |
| `ScalarModel` | `mean_RHS_time`, `max_RHS_time`, `min_RHS_time` | wall times of RHS evaluations (`ComputeTargets/ScalarModel.py:1203-1205`; `Quadrature/supervisors/base.py:62-85`) |
| `AdiabaticHistory` | `label` | `main.py:520`; stored at `Datastore/SQL/ObjectFactories/AdiabaticHistory.py:250` |
| `AdiabaticHistory` | `compute_time` | **not in the prompt's list**: `WallclockTimer().elapsed` (`ComputeTargets/AdiabaticHistory.py:273`, `:323`; `utilities.py:24-31`, `perf_counter`); stored by the factory's `store` (`AdiabaticHistory.py:252`) |
| `BBNData` | `label` | `main.py:783`; stored at `Datastore/SQL/ObjectFactories/BBNData.py:297` |
| `BBNData` | `BBN_compute_time`, `NP_compute_time` | `WallclockTimer().elapsed` (`ComputeTargets/BBNData.py:446`, `:489`, `:515-516`) |

Confirmed **physics**, compared: `compute_steps` (`result.nfev`, `ComputeTargets/ScalarModel.py:1201`)
and `RHS_evaluations` (a count, `Quadrature/supervisors/base.py:62-64`); `z_samples`; every
failure column; every abundance; `small_network`; `PRyM_version` (the constant
`PRYM_VERSION = "bf24c3d+ri02+sr01+bt02"`, `ComputeTargets/BBNData.py:48`); `validated`; every value
table column; the labels of `version`, `store_tag` (`SamplesPerLog10Z_N`, `main.py:133`) and
`IntegrationSolver`; `QCD_Cosmology.name`. **`extra_data`** (`build_extra_data`,
`ComputeTargets/ScalarModel.py:1231-1262`) holds `reflections` (a count), `cap_fraction`,
`cap_floor`, `cap_global_max_step`, `jacobian_factor_max`, `accepted_steps`,
`steps_rejected_by_exception` and the three RHS-*value* tuples (`supervisor.largest/smallest/
mean_RHS_values`, `Quadrature/supervisors/ScalarField.py:224-241`, values of the RHS, not times).
No field is a timing, so it is compared whole, as its stored JSON string.

**One masked fragment** (`MASKED_FRAGMENTS`): `BBNData.failure_reason` is physics, but the reason
of a wall-clock failure carries the seconds elapsed — `PRyMWallClockLimitError`'s message
`"wall-clock limit of %.6g s exceeded in stage '%s': %.6g s elapsed"` (`PRyM/PRyM_main.py:40-47`),
stored as `"PRyMordial: <Type>: <message>"` (`ComputeTargets/BBNData.py:344`). The identity
replaces the elapsed figure by `<elapsed>`; the limit and the stage stay compared, and the raw
column goes with the bookkeeping (a difference in it is reported there). See Deviations, D2.

**The polymorphic map** (`POLYMORPHIC_REFERENCES`): only `ScalarModel` has `*_type`/`*_serial`
pairs without foreign keys (`Datastore/SQL/ObjectFactories/ScalarModel.py:116-126`, written at
`:542-547` from each object's `type_id` and `store_id`). Every other `*_serial` column of every
factory has a foreign key, and no other `*_type` column exists (test 13 checks both). The table
is the class's registry name, since the layer dispatches a store by `type(obj).__name__`.

| Pair | Type value | Table | Justification |
|---|---|---|---|
| `cosmology_type`/`cosmology_serial` | 3 | `QCD_Cosmology` | the only stored cosmology class (its factory builds `QCD_Cosmology`, `Datastore/SQL/ObjectFactories/QCD_Cosmology.py:78`); it is a `LambdaCDM_GenericEOS` whose `type_id` is its EOS's (`CosmologyModels/GenericEOS/LambdaCDM_GenericEOS.py:87-89`); its EOS is always `Xav_EOS_spline` (`CosmologyModels/GenericEOS/QCD_Cosmology.py:41`), `type_id` = `XAV_IMPROVED_EOS_IDENTIFIER` = 3 (`Xav_EOS_spline.py:86-88`; `CosmologyModels/model_ids.py:19`) |
| | 0, 1, 2 | **no stored class** (`UNSTORED_TYPE_VALUES`) | 0 `LAMBDACDM_IDENTIFIER` (`model_ids.py:16`) is returned by no class; 1 and 2 are the `type_id`s of a `LambdaCDM_GenericEOS` on `SaikawaShirai_EOS_jax_autodiff` / `SaikawaShirai_EOS_spline` (`:17-18`; `SaikawaShirai_EOS_jax_autodiff.py:194-196`, `SaikawaShirai_EOS_spline.py:94-96`), a class no factory registers. A row with one is the named problem "unknown type value", with this reason |
| `potential_type`/`potential_serial` | 0 | `InversePowerPotential` | `CosmologyConcepts/Potentials/model_ids.py:16`; `InversePowerPotential.py:56-57` |
| | 1 | `ExponentialPotential` | `model_ids.py:17`; `ExponentialPotential.py:57-58` |
| | 2 | `ReflectingPotential` | `model_ids.py:18`; `ReflectingPotential.py:55-56` |
| | 3 | `StarobinskyPotential` | `model_ids.py:19`; `StarobinskyPotential.py:55-56` |
| | 4 | `ReclinerPotential` | `model_ids.py:20`; `ReclinerPotential.py:59-60` |
| `coupling_type`/`coupling_serial` | 0 | `ExponentialCoupling` | `CosmologyConcepts/ConformalCouplings/model_ids.py:16`; `ExponentialCoupling.py:41-42` |

A value in neither map is the named problem "unknown type value", and the row's reference becomes
`["unmapped", value]`; nothing raises.

**What a ChamPBH primary holds** (§2.2), found from the code; no store was opened:
`ShardedPool._create_engine` declares five tables on the primary's engine and
`_write_shard_data` creates exactly those — `shards`, `shard_key_config`, `shard_keys`,
`replicated_tables`, `sharded_tables` (`Datastore/SQL/ShardedPool.py:244-291`) — and nothing else
creates a table there; the actor builds every client table on each shard
(`Datastore/SQL/Datastore.py:282-401`). So a ChamPBH primary holds **no client table**, which is
what the orchestrator read from the pilot primary. DatastoreKit's primary adds
`replication_in_flight` (`v0.2.3:datastorekit/SQL/ShardedPool.py:898-962`; contract §5). The tool
does not assume this: a primary holding any other table, or a shard holding one of the layer's,
is a named problem.

**How the tool treats the tables the orchestrator flagged:**
- **`LambdaCDM`** is listed in `replicated_tables` (`config/sharding.py:34`) but no factory
  registers it (`Datastore/SQL/Datastore.py:94-122`), so no shard has it. A listed table that no
  shard holds is a **note** ("listed as replicated by the primary but no shard holds it; not
  compared"), not compared, and does not change the exit code. Since 08 removes it from the list,
  an R1–R2 comparison sees it listed in one primary only; that too is only a note.
- **`ExponentialCoupling`** is in the primary's `sharded_tables`, so it is sharded: the union of
  every shard's rows, each resolved on its own shard (its `beta_serial` into the replicated
  `beta_value`).
- **The `*_tags` tables** are in neither list; their foreign keys other than into `store_tag`
  name a sharded parent, so they are **sharded**. They have no `serial`, so their identity is
  their two resolved foreign keys alone (`timestamp` is bookkeeping) and they are never memoised.

### T3 — the rehearsal scripts (`.documents/dk-and-rpk-migration/rehearsal/`)

Each runs from the repository root, takes the rehearsal root from `REHEARSAL_ROOT` (default
`~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration`), prints its usage, and starts or stops no
Ray cluster.
- `build.sh NN STORE BUILD ADDRESS [--prune-unvalidated]` (mode 755): README §4.1's command with
  only `NN`, `STORE`, `BUILD` and `ADDRESS` substituted; output to `logs/NN-BUILD.log`, the
  driver's pid to `logs/NN-BUILD.pid` as soon as it starts, `** driver exit status N` appended,
  and the driver's status as its own. The driver is started with `&` and waited on, so the script
  is foreground and `$!` is the driver's pid. It validates its arguments, refuses to overwrite an
  existing log, and refuses to run outside the repository root.
- `interrupt_when.py LOG PIDFILE --banner TEXT --after SECONDS [--poll S]`: reads the two files,
  waits for the banner, then the delay, then sends `SIGTERM` to the pid in `PIDFILE` and prints
  each step with a UTC time. If the driver exits (pid gone, or the exit-status line in the log)
  before the banner or during the delay, it says so, sends nothing and exits 1.
- `stage_counts.py LOG... [--json OUT]`: per log, each `** CALCULATE …` banner with whether it
  completed, its queue-summary counts without rates (`lookup`, `compute`, `store`, `available`,
  `validation`), the pool's "no compute items were evaluated" line, the "models skipped" lines
  (parsed and verbatim), and build.sh's exit status. Its output carries no time.
- `run_readers.sh NN STORE ADDRESS` (mode 755): step 0 copies the store's files into
  `NN-readers/before/` (`cp -p`; nothing opens them but step 5's `compare_stores`); steps 1–5 run
  `main.py --inventory`, `plot_by_beta.py --phi-init-Mp 5 --output NN-readers/plot_by_beta`,
  `plot_ScalarModel.py --output NN-readers/plot_ScalarModel`, `tools/bbn_from_store.py <stem>
  --beta 1.15 --M-Mp 1e-3 --small-network --variant prod`, and `tools/compare_stores.py
  before/NN-STORE.db NN-STORE.db --json …`, each to its own log, each status to `status.txt`; exit 1
  if any step did not exit 0. It refuses to run if `NN-readers/` exists.
- **The flags were checked against their parsers:** `config/argument_parser.py` defines
  `--config` (`:52`, `is_config_file`), `--database` (`:57`), `--inventory` (`:64`),
  `--phi-init-Mp` (`:129`), `--prune-unvalidated` (`:257`), `--output` (`:287`), and `--shards`,
  `--job-name`, `--M-values-Mp`, `--beta-values`, `--ray-address`; `plot_by_beta.py` adds only
  `--no-baseline`. `tools/bbn_from_store.py`'s `main` defines the positional `stem`, `--beta`,
  `--M-Mp`, `--small-network` (a `BooleanOptionalAction`) and `--variant` (`prod` among its
  values). `exponential.yaml` is tracked at the repository root.

## Deviations from the prompt

- **D1 — `IMPLEMENTATION CHOICE`.** `AdiabaticHistory.compute_time` is bookkeeping, though the
  prompt's candidate list omits it: it is a `WallclockTimer` interval (table above). The prompt
  asked for any the list misses.
- **D2 — `IMPLEMENTATION CHOICE`, for review.** `BBNData.failure_reason` is compared with the
  seconds elapsed of a wall-clock failure masked (`MASKED_FRAGMENTS`). The prompt says the failure
  columns are physics, and they stay compared; but the message embeds a wall time, so two equal
  wall-clock failures would otherwise differ by a timing. This is the rule the prompt gives for
  `extra_data` ("compared field by field with the timing fields set aside, rather than dropping
  it"), applied to the one column where the code shows a timing inside a physics column; the
  column could be classed from the code, so it was not a §4 stop. Whether a wall-clock failure
  happens at all is run-dependent (README §4.3's candidate), and the tool still reports it (a
  failure in one store and a result in the other, or a different stage, is a difference).
- **D3 — `IMPLEMENTATION CHOICE`.** The bookkeeping differences of matched rows are counted per
  column for every bookkeeping column **except `serial`**, so that a value table's millions of
  rows need no per-row tuple held in memory. Serials are never compared, as specified; they are
  simply not counted.
- **D4 — `IMPLEMENTATION CHOICE`.** Near misses cascade through references. Prompt §2.2 pairs an
  A-only and a B-only row "whose resolved references, with their non-float columns, are equal".
  Taken literally, one ulp in a `ScalarModel` row unpairs every one of its `ScalarModelValue` and
  tag rows (their parent reference differs), and lists them all. The near key here keeps a
  reference to a row both stores hold as its digest and replaces a reference to any other row by
  that row's own near key, recursively. So each child pairs with the counterpart of its own `z`
  (whose `redshift` row both hold), its own differing floats are listed, and children that differ
  only through the parent are counted ("N paired row(s) … differing only through
  parent_serial") rather than listed; the JSON lists every pair with `through_references`.
  Test 10b pins it.
- **D5 — `IMPLEMENTATION CHOICE`.** `--max-rows N` (default 20) limits the rows listed per table
  and list in the human output; `--json` has all of them. A science-store comparison could
  otherwise print millions of lines.
- **D6 — `IMPLEMENTATION CHOICE`.** Every named problem (unknown type value, unresolved
  reference, replicated copies that differ, a client table on the primary, a layer table on a
  shard, rows in `replication_in_flight`, a table classed differently in the two stores, a
  composite foreign key) makes exit 1. A note (a listed table no shard holds, a table one store
  lacks) does not.
- **D7 — `IMPLEMENTATION CHOICE`.** A table in neither of the primary's lists is classified by
  DatastoreKit's contract §5 rule (above). Only the three `*_tags` tables are in that position,
  and they come out sharded.
- **D8 — `IMPLEMENTATION CHOICE`.** `main.py` already imported from `utilities`
  (`from utilities import grouper, energy_formatter`); that statement gains the two names (black
  wraps it over six lines) rather than a second import being added. The other two drivers gain a
  new `from utilities import …` line. Each driver also gains a one-line comment above the print.
- **D9 — `IMPLEMENTATION CHOICE`.** The `ast` test of the drivers also requires the print to
  precede `ray.init(` and to be a module-level statement, which is stricter than "before
  `ShardedPool(`".
- **D10 — `IMPLEMENTATION CHOICE`.** Tests beyond prompt §2.4's thirteen: 10b (D4); a listed
  table no shard holds; the classification of every imitation table; the masked fragment; `--help`;
  13b (each mapped potential and coupling class's `type_id` returns the mapped constant, read with
  `ast`); and 14, a store whose shards have exactly the tables the factories register (built
  with SQLAlchemy's `create_all` from `register()`, mirroring `Datastore._build_schema`, on
  temporary files, never through the layer), with the primary's lists from `config.sharding`.
  Test 14 is how "each declared table exists in a store built as §2.4's tests build one" is
  met: the imitation stores of tests 1–12 cannot hold ChamPBH's tables. Tests 13, 13b and 14 read
  the registry from `config.datastore.factories` if it exists (05 creates it) and otherwise from
  `Datastore.SQL.Datastore._factories`, so that 05 and 08 need not edit them.
- **D11 — `IMPLEMENTATION CHOICE`.** The `stage_counts.py` test is in
  `Datastore/tests/test_compare_stores.py` (one of the two named modules), with its fixtures
  inline as strings written for the test in the shape of `main.py`'s, `RayWorkPool`'s and
  build.sh's lines, not copied from `pilot-main.log`. No fixture file was added.
- **D12 — `IMPLEMENTATION CHOICE`.** The stand-in distributions of T1's tests are real
  `.dist-info` directories in a `tempfile` directory at the front of `sys.path`, found by the real
  `importlib.metadata`; nothing is patched (the prompt left the method open).
- **D13 — `IMPLEMENTATION CHOICE`.** `connect_ro` percent-quotes the path in the URI
  (`quote(path, safe="/")`), so a path holding `?`, `#` or `%` cannot change the URI's meaning.
  `tools/bbn_from_store.py` passes the path raw.

No `STRUCTURALLY REQUIRED` and no `UNINTENDED DRIFT` deviation.

## Verification performed

**Suites**, run from the repository root with `CLAUDE.md`'s commands, in the foreground, serially,
output to the scratch directory. ComputeTargets was run as two pattern groups (`test_[a-f]*.py`,
`test_[g-z]*.py`) to stay clear of the ten-minute cap.

| Root | Dispatch baseline (`ea3d192`) | Before (this session, `0e64ef0`) | After (`d7791c5`'s tree) |
|---|---|---|---|
| `CosmologyModels/tests` | 18 OK | 18 OK (123.3 s) | 18 OK (75.1 s) |
| `ComputeTargets/tests` | 114 OK | 61 + 53 = 114 OK (213.7 s + 141.7 s) | 61 + 53 = 114 OK (108.5 s + 137.2 s) |
| `Datastore/tests` | 35 OK | 35 OK (3.6 s) | **66 OK** (4.1 s) |

`Datastore/tests` by module, after: `test_bbn_failure_lookup` 6, `test_compare_stores` **21**
(new), `test_first_bounce_round_trip` 5, `test_fixed_T_values_round_trip` 5,
`test_package_provenance` **10** (new), `test_scalarmodel_failure_reason` 4,
`test_shard_key_assignment` 4, `test_version_keyed_lookups` 11 = 66 = 35 + 10 + 21. No existing
test was edited, and none failed.

The "After" column is the final run, on the tree as committed (2026-10-12, 00:28–00:34, load
average about 19); an earlier run during the work gave the same counts.

**T1, acceptance 1.** The ten provenance tests pass: not installed; a VCS install at a tag; an
editable install; a local directory not editable; `direct_url.json` absent; present but a JSON
list, a JSON string, not JSON, not UTF-8, or a directory; `vcs_info`/`dir_info` not objects; two
names in order with the described package's module raising on import (it is never imported);
the default names; and the drivers' `ast` test (exactly one call of each helper per driver, one
module-level print, before `ray.init(` and before the first `ShardedPool(`, both names imported
from `utilities`). The line in today's venv is shown above. No driver was run.

**T2, acceptance 2.** The 21 tests of `test_compare_stores.py` pass, among them §2.4's 1–13 (test 6
records every `sqlite3.connect` call and finds exactly the six `file:…?mode=ro` URIs of the two
primaries and their four globbed shards; test 12 compares every file's SHA-256, size and
`st_mtime_ns` before and after, and finds no new file). `compare_stores.py --help` exits 0;
`black --check` is clean on every Python file `d7791c5` adds or changes.

**T3, acceptance 3.** `build.sh --help` and `run_readers.sh --help` exit 0 under both
`/bin/bash` 3.2.57 and MacPorts bash, and `bash -n` passes for both; with no arguments each
prints its usage and exits 2. `interrupt_when.py --help` and `stage_counts.py --help` print their
usage. The two shell scripts' argument checks were exercised only on refusals that open nothing
(`build.sh 2 …` refuses the slug; `run_readers.sh` with `REHEARSAL_ROOT=/nonexistent-root` refuses
the missing store). **No script was run on a build, and no store but a test's temporary one was
opened.** `~/ScienceData/` was not created.

**Not run:** no build, no driver, no Ray, no `pip`; the pilot store, `~/ChamPBH-stores/` and
`~/ChamPBH-stores-bt02/` were not opened. `pilot-main.log` was read as text.

## The deliberate-breakage record

Each diff below replays with `git apply` from a clean tree at `d7791c5`. Each was checked in
`/bin/bash` with `git apply --check` (on the clean tree) and `git apply -R --check` (after
applying), run against the named test, and reversed with `git apply -R`; the tree was clean after
all five.

**B1 — `float.hex` replaced by `repr` rounded to 12 digits.** Test 2 fails: `0 != 1` (exit 0, the
one-ulp change is invisible).

```diff
--- a/tools/compare_stores.py
+++ b/tools/compare_stores.py
@@ -230,7 +230,7 @@
     if value is None:
         return None
     if isinstance(value, float):
-        return ["f", value.hex()]
+        return ["f", format(value, ".12g")]
     if isinstance(value, int):
         return ["i", int(value)]
     if isinstance(value, str):
```

**B2 — the glob replaced by the paths in the `shards` table.** Test 6 fails in all three subtests
(`absolute`, `missing`, `copied`): `2 != 0`, "unable to open database file" (a bare record resolves
against the working directory; an absolute one that does not exist cannot be opened).

```diff
--- a/tools/compare_stores.py
+++ b/tools/compare_stores.py
@@ -343,6 +343,10 @@
             self.recorded_shards = conn.execute(
                 'select count(*) from "shards"'
             ).fetchone()[0]
+            self.recorded_paths = [
+                row[0]
+                for row in conn.execute('select filename from "shards" order by serial')
+            ]
 
             self.replicated = []
             if "replicated_tables" in tables:
@@ -391,7 +395,7 @@
             + "*"
             + glob.escape(suffix)
         )
-        self.shard_files = [Path(p) for p in sorted(glob.glob(pattern))]
+        self.shard_files = [Path(p) for p in self.recorded_paths]
         if len(self.shard_files) != self.recorded_shards:
             raise StoreReadError(
                 f"{self.name}: the primary's shards table records {self.recorded_shards} "
```

**B3 — `timestamp` removed from the bookkeeping list.** Test 5 fails: `1 != 0` (the timestamps
are compared, and every row differs).

```diff
--- a/tools/compare_stores.py
+++ b/tools/compare_stores.py
@@ -107,7 +107,6 @@
 GLOBAL_BOOKKEEPING = {
     "serial": "the layer's key, leased from the serial broker at insert "
     "(Datastore/SQL/Datastore.py:309-314, :670-674); foreign keys are resolved through it",
-    "timestamp": "datetime.now() at insert (Datastore/SQL/Datastore.py:679-680)",
 }
 
 BOOKKEEPING_COLUMNS = {
```

**B4 — `editable` read from the wrong key.** `test_editable_install` fails: `'editable': False !=
'editable': True`.

```diff
--- a/utilities.py
+++ b/utilities.py
@@ -224,7 +224,7 @@
             "url": origin.get("url"),
             "commit": vcs_info.get("commit_id"),
             "requested_revision": vcs_info.get("requested_revision"),
-            "editable": dir_info.get("editable") is True,
+            "editable": vcs_info.get("editable") is True,
         }
     return packages
 
```

**B5 — `main.py`'s provenance print moved after its `ShardedPool(`** (to the end of the module,
still at module level). The drivers' `ast` test fails for `main.py`: `1369 not less than 1343`.

```diff
--- a/main.py
+++ b/main.py
@@ -88,9 +88,6 @@
     parser.print_help()
     sys.exit()
 
-# which datastorekit and raypoolkit this run uses (dk-and-rpk-migration prompt 01; README P5)
-print(format_package_provenance(installed_package_provenance()), flush=True)
-
 # connect to ray cluster on supplied address; defaults to 'auto' meaning a locally running cluster
 ray.init(address=args.ray_address)
 
@@ -1367,3 +1364,6 @@
 
     else:
         execute(pool, units)
+
+# which datastorekit and raypoolkit this run uses (dk-and-rpk-migration prompt 01; README P5)
+print(format_package_provenance(installed_package_provenance()), flush=True)
```

## Observations not acted on

1. **`--prune-unvalidated` is on by default.** `config/argument_parser.py:257-261` declares it a
   `BooleanOptionalAction` with `default=True`. So every open of a store prunes its unvalidated
   rows, `reference` and `resumed-a` included, and README §4.1's `[--prune-unvalidated]` on
   `resumed-b` repeats the default rather than turning pruning on. Nothing breaks; but 02's L2
   ("the prune at open removed only unvalidated rows, by its printed report") should read the
   report of every build, and only `--no-prune-unvalidated` would keep the rows. Not a defect;
   recorded for 02 (State handed to the next prompt), not opened as an issue.
2. **`sqla_ScalarModelValue_factory.build` checks the stored φ against the expected π**
   (`Datastore/SQL/ObjectFactories/ScalarModel.py:1063-1069`: `fabs(row_data.phi_Einstein_Mp -
   pi_Einstein_Mp)`, where `row_data.pi_Einstein_Mp` is meant; present since `d132774`). The path
   is latent: nothing calls `object_get("ScalarModelValue", …)` (`git grep`), since the values are
   written by `ScalarModel`'s `store` through the inserter. **Opened as §3
   `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]`**; not fixed (a factory is out
   of scope, and this prompt changes no factory).
3. **`--database` is `required=True`** (`config/argument_parser.py:56-58`), so each driver's
   `if args.database is None: parser.print_help(); sys.exit()` cannot fire; the provenance line
   sits after it regardless. Cosmetic.
4. **The comparison's speed.** On an imitation store of 4,000 value rows and 2,000 replicated
   parameter rows over two shards a comparison took 0.4 s, all in Python; a science store with
   millions of value rows will take minutes, and holds a digest per value row (16 bytes, plus a
   `Counter` entry) for each store in memory. Measured on a scratch imitation only.

## State handed to the next prompt

- **The provenance line** is `>> packages: datastorekit not installed; raypoolkit not installed`
  in today's venv; each driver prints it once, after argument parsing and before `ray.init(`, so
  in every rehearsal log it precedes Ray's connection lines. The format table
  above is what 03, 06, 07 and 10 quote.
- **`tools/compare_stores.py`** is ready for 02: `compare_stores.py REF.db RES.db --json OUT`
  for L2, and `--snapshot REF.db --out F` for the start tree's schema snapshot. Exit 0 means equal
  by value with no named problem; the `LambdaCDM` note is expected on every ChamPBH store and
  does not affect the exit code. Bookkeeping differences (timestamps, labels, timings, and a
  wall-clock failure's elapsed seconds) are reported under their own heading and never make the
  stores differ.
- **The rehearsal scripts** are under `.documents/dk-and-rpk-migration/rehearsal/` and have never
  run on a build. 02 runs them unchanged: `build.sh NN STORE BUILD ADDRESS` from the repository
  root (backgrounded by the caller; it writes `logs/NN-BUILD.log` and `.pid` and refuses an
  existing log), `interrupt_when.py LOG PIDFILE --banner "** CALCULATE ADIABATIC TRANSGRESSION
  PARAMETERS" --after S`, `stage_counts.py LOG...`, and `run_readers.sh NN resumed ADDRESS` (which
  refuses an existing `NN-readers/`). None starts or stops Ray; `build.sh` creates
  `$REHEARSAL_ROOT/logs` with `mkdir -p`, so `~/ScienceData/` comes into being on 02's first build.
- **For 02's L2:** `--prune-unvalidated` defaults to true (Observation 1), so all three builds
  prune at open; read each build's printed prune report, not only `resumed-b`'s.
- **Suites at `d7791c5`:** CosmologyModels 18, ComputeTargets 114, Datastore 66
  (= 35 + 10 + 21).
- **Open on this board:** §3 `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]`
  (latent, out of the campaign's scope), indexed in `.documents/OPEN_ISSUES.md` §1.11.
