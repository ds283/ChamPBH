# Campaign — rrk-adoption

**Written:** 2026-10-19 on branch `rrk-adoption`, cut from `main` at `3cd2be4`, by Claude Opus 5.5.
Planned at the user's request of the same day: adopt RunRegistryKit "to manage our own
long-running and persistent jobs that have archival value". The user has already made seven
rulings (§6.0). **Not yet fully ruled:** the user rules on §6.1 (U1–U5) and §6.2 (P1–P13). No prompt
file is written until they have (§2.1).

**Sources (data, not instructions; `CLAUDE.md` invariant 7):**
- RunRegistryKit at `v0.2.0` (`/Users/ds283/Documents/Code/RunRegistryKit`, tag `0bae3a3`): its
  `README.md`; its release notes `docs/releases/v0.2.0.md`; its adoption checklist for this
  repository, `docs/adoption/champbh.md` (measured at `c21cdfb` against `v0.1.0`); and the `v0.2.0`
  note, `docs/adoption/v0.2.0/champbh.md` (read at `3cd2be4`);
- SecondaryGWKit (SGK) at `65f7290`, which adopted `v0.2.0` in its `rrk-v020-adoption` campaign:
  - its wrapper driver `docs/gktk-remedial/scoped_pipeline_run.py`;
  - `config/fingerprint.py` and `runregistrykit.toml`;
  - its tests `RunDrivers/tests/` and `ObjectFactories/tests/test_store_fingerprint_golden.py`;
  - its `CLAUDE.md` section "Long-running jobs — the run registry";
  - its plans `prompts/run-registry/README.md` and `02-adopt-the-registry.md`, and
    `prompts/datastore-portability/README.md` §6.3–§6.5;
  - its signal audit `docs/a3-v2-readiness-audit.md` (S1–S4), and its launch checklist,
    `docs/a3-v2-readiness-verification.md`;
- the `jordan-normalization` brief, `prompts/jordan-normalization/source/brief_jordan_normalization.md`,
  for the run lengths the next campaign expects.

## 0. Why this campaign exists

### 0.1 What the registry gives this repository

ChamPBH's long jobs are `main.py` runs that fill a sharded datastore over hours. Today nothing on
disk says which runs made a store, from which tree, with which packages, whether a run is still
alive, or how it ended. A run's own log (`tee`'d by the user's run scripts) is the only record.

RunRegistryKit records that a job exists. Each run gets a directory under `var/runs/`, holding:
- a manifest, written before the work starts: purpose, argv, git commit and dirty flag, script hash,
  the installed packages at their commits, the host;
- a `status.json` with a heartbeat and a stage while it runs, and its terminal state at the end.

Its store layer keeps a sidecar beside each datastore, `<stem>.manifest.json`. The sidecar says what
the store is for, which runs made it, and its content fingerprint when the last run finished. Then
`copy`, `move`, `fingerprint` and `retire` act on the store under that record. It records; it does
not schedule, supervise, restart or lock. The failures it answers are SGK's, one working session's
worth (its `run-registry` README §0; RunRegistryKit's `README.md`, "The rules").

The next campaign, `jordan-normalization`, ends in the science rebuild. This campaign comes first so
that the rebuild is a registered run (§6.0, R7). The brief puts a history at 1.4–263 s today
(median 22 s), and 3–7 integrations per history once Λ and φ_ref are tuned (the brief's item R2).

### 0.2 What SGK does, which this campaign follows

SGK registers no run inside `main.py`. Every registered run goes through a **wrapper driver**, and
`main.py` runs byte-identically with or without it. SGK's `run-registry` plan put `main.py` out of
scope ("changes how jobs are launched and tracked, never what they compute", its §1). Its prompt 02
asked for "a thin wrapper or an opt-in flag" so that registering would not become a change to the
pipeline (its §3 item 1). The wrapper, `scoped_pipeline_run.py`:
1. **Registers before anything starts.** `register()` calls `runregistrykit.begin(campaign=…,
   prompt=…, slug=--register, purpose=…, script=__file__, results=<the database>, scope=…,
   heartbeat_means=…)` before Ray starts and before `main.py` is read. A `RuntimeError` from
   `begin` (a store an unfinished run names, a retired store) becomes a one-line refusal with no
   traceback. A `ConfigError` propagates.
2. **Beats from stdout.** It replaces `sys.stdout` and `sys.stderr` with pass-through proxies
   (`RegisteredStream`), which copy every write into the run directory's `stdout.log` and
   `stderr.log`. A stdout line refreshes the heartbeat, at most once per `BEAT_MIN_INTERVAL` = 30 s,
   and `StageTracker` reads the stage off RayWorkPool's own banners and progress lines. stderr is
   copied but never beats. The manifest's `heartbeat_means` says exactly what a fresh beat asserts:
   - **What it asserts.** RayWorkPool prints only after `ray.wait` returns completed work, so a
     fresh beat means work completed recently.
   - **What it does not assert.** A stage that prints nothing for longer than the staleness window
     (900 s) is reported stale while it works. Stale means "go and look".
3. **Records a signal.** `ray.init` installs its own `SIGTERM` handler (`sys.exit(signum)`, and
   absl's, which aborts the process 2.78–3.06 s later unless `signal.alarm(0)` cancels it; SGK's
   audit S1–S4). So the wrapper wraps `ray.init` and installs its own `SIGTERM`/`SIGINT` handler
   straight after it, which records `killed`, `128 + signum`, within about a second.
4. **Finishes after `main.py` unwinds** (`finish_after_exec`). Once the pool has closed, it records
   `done`, `failed` or `killed` with `fingerprint=True`, and exits with the code `main.py` would
   have.
5. **Names runs** `<campaign>-<prompt>-<slug>-<YYYYMMDDTHHMMSS>`. The campaign and prompt are those
   that ordered the run; the user's own launches used `--campaign handover --prompt <n>`.
6. **Leaves the store's sidecar to a person.** `store create --purpose …` is run on the store after
   its first close (its launch checklist; its records show it on each store). Until then a run's
   status says `fingerprint_sidecar: "not written … (there is none)"`, and from then on `written`.

   The timing follows from three things, though no SGK document argues for it as such:
   - `ShardedPool` knows nothing about sidecars, the user's ruling of 2026-09-24
     (`datastore-portability` §6.3);
   - `store create` requires a `--purpose` that only a person can give (`store-retirement` D3 cites
     it as the precedent);
   - the package refuses a sidecar for a primary that does not exist yet
     (`runregistrykit/stores.py:328-337`).

What SGK's wrapper also does, and ChamPBH's does not need:
- it bootstraps a private Ray (`address="local"`), where ChamPBH attaches to its isolated cluster;
- it substitutes `main.py`'s wavenumber-grid literals and filters its model list, so that it
  `exec()`s modified source;
- it refuses an existing store, where ChamPBH's blocks are restartable into one store
  (`full_run_2026.6.0.sh`'s header);
- it suffers the left-behind GCS server of a `SIGKILL`ed driver
  (`[03-a-sigkilled-drivers-gcs-server-deadlocks-in-shutdown]`). That belongs to a driver that owns
  its cluster; ChamPBH's driver owns none, which 04 confirms.

### 0.3 Measured at `3cd2be4` (2026-10-19), read-only

No store was opened and no Ray started.

**The package.**
- `v0.2.0` is tagged and pushed (`refs/tags/v0.2.0^{}` = `0bae3a3`). Its `runregistrykit/` and
  `pyproject.toml` are unchanged from the tag to its `HEAD` (`33a2cd2`).
- Its suite, in its own venv: **410 tests, OK (skipped=1)**, 146.5 s.
- `pip install --dry-run "runregistrykit[stores] @ …@v0.2.0" -r requirements.txt` into this venv
  installs **`runregistrykit` 0.2.0 and nothing else**.
- The declared range is Python ≥ 3.12 and `datastorekit` ≥ 0.4.0. This venv: Python 3.13.16,
  `datastorekit` 0.4.0 (`requirements.txt:17`), `ray` 2.53.0, `SQLAlchemy` 2.0.46. All inside;
  Ray lies between the tested ends.
- The six open RunRegistryKit issues (its `docs/OPEN_ISSUES.md` §1.3) bear on nothing ChamPBH does:
  one is Python 3.14 only, one is a docstring, three are malformed records, and one is a killed
  process that its parent has not reaped. Under the user's run scripts the driver's parent is
  `bash`, which reaps it.

**This repository.**
- No code imports a run registry. `CLAUDE.md:117` says "There is no run registry in this
  repository". `.gitignore` does not ignore `var/`, and there is no `var/`.
- `main.py` runs at module scope:
  - the provenance line at `:95`;
  - the `--inventory` branch, which reads the closed store and exits before Ray, at `:97-115`;
  - refusals that exit 2 at `:108`, `:125` and `:132`;
  - `ray.init(address=args.ray_address)` at `:133`;
  - `with ShardedPool(…) as pool: execute(pool, units)` at `:1232-1252`.

  It installs no signal handler, and calls `sys.exit` only in those branches.
- **The work queues' printing.** `RayWorkPool` prints `\n** {title}` when a queue starts
  (`raypoolkit/RayWorkPool.py:206`). Its progress lines (`:588`, `:592`) are printed only after a
  completed item has been processed, and no more often than `notify_min_time_interval`. `main.py`
  sets that to `MIN_NOTIFY_INTERVAL = 300` s (`:84`) on every queue. So a beat from stdout arrives
  at most about every 5 min while work completes; SGK's tracker vocabulary matches these lines.
- `--database` may come from a YAML config: the parser is `configargparse`
  (`config/argument_parser.py`). A wrapper that reads it must use the same parser.
- **The factory registry.** `config.datastore.factories` has 27 entries, 17 with an
  `inventory_spec`. `ObjectFactories/ScalarModel.py`'s spec has three polymorphic `Parent.types`
  keyed by `int` (the checklist's item 8), which `canonical_json` refuses unconverted.
- `ObjectFactories/tests/test_package_provenance.py` reads each driver's call of
  `utilities.installed_package_provenance()` by `ast` (`TestDriversPrintProvenance`).
- **The working tree.** `git status --porcelain` lists 22 untracked paths at the root: the pilot
  store and its 16 shards, `pilot-out/`, two logs and two run scripts. `begin()` records
  `git_dirty` from that command, untracked files included, so every run begun from this checkout
  today would be recorded dirty.
- **Where the stores are.** The user's run scripts write stores to `$HOME/ChamPBH-stores` and
  `$HOME/ChamPBH-stores-bt02` (`full_run_2026.6.0*.sh`, untracked). The package expands neither `~`
  nor an environment variable in its config (`runregistrykit/client.py:34-36`).

**The suites at `3cd2be4`:** see §0.4.

### 0.4 Baselines

Measured at `3cd2be4` with `CLAUDE.md`'s commands: CosmologyModels **18**, ComputeTargets
**176** (61 + 115), ObjectFactories **69**, tools **79**.

## 1. Scope

**In scope:**
- the pin, `runregistrykit.toml`, `var/` ignored, and the rules in `CLAUDE.md` (01);
- the fingerprinter, its format number, the guard against a spec changing without a bump, and a
  golden fingerprint (02);
- the wrapper driver, `tools/registered_run.py`, with tests that need no Ray (03);
- a rehearsal on ChamPBH's isolated cluster: a registered build, an interrupted one resumed, a
  hard-killed one ended, `store create` and the fingerprint (04);
- the documents, the handover and the drafts of RunRegistryKit's gate records (05).

**Out of scope:**
- **`main.py`, `plot_by_beta.py`, `plot_ScalarModel.py` and `utilities.py`.** They stay launchable
  without RunRegistryKit (§6.0, R2), and none of them imports it. Whether the provenance helper is
  replaced is U1; the proposal is not.
- **What the code computes.** No stored value changes.
- **Existing stores**: the pilot store at the root, `~/ChamPBH-stores*`, and the rehearsal stores
  under `~/ScienceData/ChamPBH-rehearsals/`. None is opened, adopted or given a sidecar (§6.0, R5).
  Deleting them is the user's.
- **The user's run scripts** (`full_run_2026.6.0*.sh`, untracked). 05's handover says what in them
  changes.
- **Registering the plot scripts or `--inventory`.** They are short and read-only.
- **The science rebuild**, which is `jordan-normalization`'s.
- **RunRegistryKit, DatastoreKit, RayPoolKit and SGK:** read, never edited. A defect in
  RunRegistryKit stops the prompt and is reported there (`CLAUDE.md`, "The packages").

## 2. Prompts

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-pin-config-and-rules.md` | The pin and the venv; `runregistrykit.toml` without `[stores]`; `var/` in `.gitignore`; `CLAUDE.md`'s section on long-running jobs, with the rules; a test of the config | Sonnet |
| 02 | `02-the-fingerprinter.md` | `config/fingerprint.py` (format 1, `SPEC_DIGESTS`, the fingerprinter); `[stores]` in the config; the format guard; a golden fingerprint of a temporary store | Sonnet |
| 03 | `03-the-wrapper.md` | `tools/registered_run.py`: register, proxy and beat, the signal handler after `ray.init`, run `main.py` unchanged, finish with the fingerprint, refusals. Tests with no Ray | Opus |
| 04 | `04-rehearse-registered-runs.md` | On the isolated cluster, under the lock: a registered build; a `SIGTERM`ed build resumed; a `SIGKILL`ed build ended with `end` and resumed; `store create`; the fingerprint at finish; the stores compared; `histories.csv` equal to R0's | Opus |
| 05 | `05-close.md` | `architecture-summary.md`; `CLAUDE.md` if 01–04 left anything; the handover for the run scripts; the drafts of RunRegistryKit's gate records; the board closed, the indexes | Sonnet |

### 2.1 When each prompt file is written

01–05 are written once the user has ruled on §6.1 and §6.2. 01 comes first. 02 and 03 each need
only 01, so they are independent of each other. 04 needs 02 and 03. 05 needs 04.

### 2.2 The charters

**01 — The pin, the config and the rules** (Sonnet).
- **What it changes.**
  1. `requirements.txt`: `runregistrykit[stores] @ git+https://github.com/ds283/RunRegistryKit@v0.2.0`,
     beside the existing `datastorekit` line. The venv is installed from it, not editable.
  2. `runregistrykit.toml` at the root: `runs_root = "var/runs"`, `stores_root =
     "var/datastores"`, `distributions = ["datastorekit", "raypoolkit"]`. There is no `[stores]`
     yet, since 02 writes the module it names.
  3. `.gitignore`: `var/`.
  4. **`CLAUDE.md`.** "Long-running jobs" becomes a section in SGK's form:
     - the package and where it records; the six rules, with `./venv/bin/python -m runregistrykit
       list` as the discovery command; the route after a hard kill (`end`, then resume);
     - the wrapper named as the way to launch a registered run, once 03 has landed (01 writes the
       sentence with a forward reference);
     - stores under `var/datastores/`, and **never `git clean -x` or `-X`**, which deletes ignored
       files;
     - that `main.py` stays launchable without the wrapper;
     - that production runs are still the user's, and that no prompt starts one but 04's
       rehearsal.
- **Acceptance.**
  1. `pip show runregistrykit` is 0.2.0, `direct_url.json` names the tag's commit, and it is not
     editable.
  2. `python -m runregistrykit list` on the empty runs root prints no run and exits 0.
  3. `git check-ignore var/runs/x var/datastores/x.db` matches both.
  4. A new test, `tools/tests/test_registry_config.py`, checks the config:
     - `find_client_root(client_root=<root>)` gives these roots and distributions;
     - the config has no key the package does not know;
     - `var/` is ignored, by `git check-ignore` on a path under it.
  5. The suites at §0.4's counts plus the new tests; `black` clean.
  6. Deliberate breakages: `var/` removed from `.gitignore`; a misspelt key in the config.

**02 — The fingerprinter** (Sonnet).
- **What it changes.**
  1. `config/fingerprint.py`, after SGK's:
     - `FINGERPRINT_FORMAT = 1`, with its history in the docstring;
     - `SPEC_DIGESTS = {1: [[class, sha256], …]}`, in registry order;
     - `fingerprinter = DatastoreKitFingerprinter(factories, FINGERPRINT_FORMAT)`.
  2. `runregistrykit.toml` gains `[stores] fingerprinter = "config.fingerprint:fingerprinter"`.
  3. `ObjectFactories/tests/test_fingerprint_format_guard.py`: the checklist's §6 recipe. Each
     spec is made plain, with every mapping key written as its own JSON so that `0` and `"0"` stay
     apart, then put through `canonical_json` and hashed. The test asserts the list equals
     `SPEC_DIGESTS[FINGERPRINT_FORMAT]`.
  4. `ObjectFactories/tests/test_store_fingerprint_golden.py`. A temporary store is built through
     `_TempStoreCase`'s harness, with at least one row of every class that declares a spec, and one
     history of each polymorphic type the store can hold. Its fingerprint, `taken` removed, equals
     a committed golden file under `ObjectFactories/tests/data/`. `load_fingerprinter` loads the
     config's fingerprinter.
- **Acceptance.**
  1. Both tests pass, with no Ray and no store outside a `tempfile` directory.
  2. `python -m runregistrykit store fingerprint <the temporary store>` prints the digest the
     golden file holds (run in the test or recorded in the log).
  3. The suites at §0.4's counts plus the new tests; `black` clean.
  4. **Deliberate breakages**, each shown failing:
     - a leaf added to one spec, with no bump;
     - the format bumped with no new pin;
     - a `Parent.types` key changed from `0` to `"0"`;
     - a row changed in the golden store.

**03 — The wrapper** (Opus).
- **What it changes.** A new `tools/registered_run.py`:

  ```
  ./venv/bin/python tools/registered_run.py --campaign C --prompt N --register SLUG \
      --purpose "one line" -- <main.py's arguments, verbatim>
  ```

  1. **It parses `main.py`'s arguments** with `config.argument_parser.create_argument_parser()`,
     so that a `--database` from a YAML config is seen. It refuses before `begin()`, exit 2, a run
     with no database (P3) and an `--inventory` run (U2).
  2. **It calls `runregistrykit.begin(…)`** before Ray starts:
     - `campaign`, `prompt`, `slug` and `purpose` from its flags;
     - `script` = `main.py` (U3);
     - `results` = the database;
     - `scope` from `main.py`'s arguments: the β, M and φ* sets, the shards and the job name;
     - `heartbeat_means`, ChamPBH's version of SGK's sentence, with the 300 s notify floor stated.

     A `RuntimeError` from `begin` is a one-line `!!` refusal, exit 2, and nothing is created. A
     `ConfigError` propagates.
  3. **Its streams** are SGK's `RegisteredStream` and `StageTracker`, carried over: stdout beats at
     most every 30 s and sets the stage; stderr is copied and never beats.
  4. **It wraps `ray.init`** so that its `SIGTERM`/`SIGINT` handler is installed straight after
     Ray's own, and records `killed`, `128 + signum`. A signal before Ray starts is handled by a
     handler installed at registration, as SGK's `terminal`.
  5. **It runs `main.py`** with `runpy.run_path(<root>/main.py, run_name="__main__")`, and
     `sys.argv = [main.py, *its arguments]`. Nothing in `main.py`'s text or arguments changes (P7).
  6. **It finishes after `main.py` returns or raises**, with the pool closed:
     - `alarm(0)` first (SGK's audit S2);
     - then `done` (exit 0), `failed` (any other exit or exception), or `killed` (a signal), each
       with `fingerprint=True`;
     - then it exits with `main.py`'s code. A `SystemExit` raised by `main.py` keeps its code.
  7. It prints the run id, the manifest's path and the `list` command at registration, and
     `** driver exit status N` at the end. That is `build.sh`'s marker, so
     `interrupt_when.py` works unchanged.
- **Acceptance.** Tests in `tools/tests/test_registered_run*.py`. None needs Ray: `ray.init` is
  stubbed, and the program run is a stand-in script that prints RayWorkPool's lines. Each test makes
  its own temporary runs root and client root.
  1. **What gets recorded:**
     - the manifest exists before the program's first line runs, with `script` = `main.py`'s path
       and hash, `results`, `scope`, `heartbeat_means` and `packages` (`runregistrykit`,
       `datastorekit`, `raypoolkit`);
     - beats are throttled to 30 s, with stages read off the stand-in's lines, and stderr does not
       beat;
     - exits 0, 1, 2 and an exception give `done` and `failed` with their exit codes, and the
       wrapper exits with the same code.
  2. **The handlers:**
     - a `SIGTERM` after the stubbed `ray.init` gives `killed`/143, in a subprocess;
     - a handler installed before `ray.init` is replaced as Ray replaces it, and the wrapper's is
       reinstated.
  3. **The fingerprint:**
     - a run on a temporary store with a sidecar records `fingerprint_sidecar: written`;
     - a run on one without a sidecar records "not written".
  4. **The refusals:**
     - a second `begin` on a store an unfinished run names is refused, exit 2, with no run directory
       created;
     - after `python -m runregistrykit end`, the same `begin` is accepted;
     - `--inventory` and a missing database are refused before `begin` (U2, P3).
  5. **`main.py` itself is unchanged:** `git diff 3cd2be4 -- main.py` is empty, and `main.py`
     imports nothing of `runregistrykit` (by `ast`).
  6. The suites at §0.4's counts plus the new tests; `black` clean.
  7. Deliberate breakages: the handler installed before `ray.init` only; `fingerprint=True` passed
     inside the program's lifetime; the exit code dropped.

**04 — Rehearse registered runs** (Opus). Under the shared lock, on ChamPBH's isolated cluster
(`ray_cluster.sh start "rrk-adoption 04"`), from this checkout. The grid is `build.sh`'s pilot grid:
`--M-values-Mp 1e-3 --beta-values 1.1 1.15 1.2 10 25`, `--shards 16`, about 3½ min a build. Every
build is launched through the wrapper, detached (`nohup`, rule 3), and checked once.
- **What it does.**
  1. **`reference`**, into `var/datastores/rrk-adoption-04-reference.db`.
     - `list` shows it `running`, then `finished`/`done`;
     - its status says `fingerprint_sidecar` "not written … (there is none)";
     - then `store create --purpose …` and `store fingerprint --write`, as SGK does;
     - then `plot_by_beta.py --phi-init-Mp 5` (unregistered) gives `histories.csv`.
  2. **`resumed-a`**, into `var/datastores/rrk-adoption-04-resumed.db`, interrupted with `SIGTERM`
     by `interrupt_when.py` during the scalar stage. It is recorded `killed`/143 within about a
     second, and gains its fingerprint. Then `store create` on the closed store.
  3. **`resumed-b`**, on the same store, `SIGKILL`ed during the adiabatic stage:
     - `list` shows it `stale`, `!!`;
     - a `resumed-c` launch is refused with nothing created, naming `end`;
     - `end <run> --reason …` records it `lost`.
  4. **`resumed-c`**, with `--prune-unvalidated`, runs to `done`. Its fingerprint is written to the
     sidecar.
  5. **The stores compared:** `tools/compare_stores.py` on the reference and the resumed store, and
     `store fingerprint` on each, which compare equal (or differ only where R0's protocol says
     they may).
  6. **Measured:** the fingerprint's time at finish; the heartbeat ages that `list` showed; the
     Ray processes after each kill (`ray_cluster.sh status`); and `git_dirty` as recorded.
- **Acceptance.**
  1. The four builds are recorded as above, every manifest written before its driver's first line.
  2. `histories.csv` from `reference` has R0's SHA-256,
     `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`, since nothing that
     computes has changed.
  3. The resumed store equals the reference by `compare_stores.py`.
  4. No ChamPBH Ray process outlives the cluster's `stop`.
  5. `.documents/rrk-adoption-verification.md` created, with the outputs under
     `.documents/rrk-adoption/out/` (small text only).
- **Out.** The stores stay under `var/datastores/` until the user removes them (§3).

**05 — Close** (Sonnet).
- `architecture-summary.md`: the wrapper, `config/fingerprint.py`, `runregistrykit.toml`, `var/`;
  and its stale line `:299` ("the RunRegistry component").
- `CLAUDE.md`: the wrapper's sentence made definite, and the suite counts.
- A dated addendum to `.documents/review-remediation-verification.md` (§4.15), the handover for the
  run scripts (§7).
- The drafts of the two gate records in RunRegistryKit (P12).
- The board closed; `prompts/INDEX.md` and `.documents/OPEN_ISSUES.md`. The final suites.

## 3. Stores

Only 04's: `var/datastores/rrk-adoption-04-reference.db` and `…-resumed.db`, 16 shards each, inside
the repository and ignored by git. 01, 02, 03 and 05 open no store but the tests' temporary ones. No
existing store is opened.

## 4. The interfaces between prompts

- **01 ships** the pin, the config without `[stores]`, `var/` ignored and the rules. 02 adds
  `[stores]`; 03 relies on the config's roots.
- **02 ships** `config.fingerprint:fingerprinter`, named in the config. 03's fingerprint tests and
  04's `store` commands load it.
- **03 ships** `tools/registered_run.py`, which 04 runs unchanged. If 04 finds a defect in it, 04
  stops; the fix is a lettered prompt (03b), not an edit in 04.
- **04 ships** the measurements and the verification document. 05's handover quotes them.

## 5. The rules this campaign runs under

`CLAUDE.md`'s invariants 1–7 and "Repository mechanics". From `dk-and-rpk-migration`:
- rule 8, deliberate breakages, recorded as diffs;
- rule 9, no stored science value changes;
- rule 10, no test needs Ray, and none opens a store outside a `tempfile` directory;
- rule 11, a package defect is reported upstream and never worked around here;
- rule 12, no change in a sibling repository;
- rule 15, no `ray stop`, and a cluster only through `ray_cluster.sh`;
- rule 16, the shared rehearsal lock: a held lock is a stop, never a wait.

And:
- **`main.py` is byte-identical at every commit of the campaign** (`git diff 3cd2be4 -- main.py`
  empty), and so are the plot scripts and `utilities.py`, unless U1 is ruled otherwise.
- **Existing tests are not modified.**
- **04's rehearsal is a one-off exception** to `CLAUDE.md`'s "no campaign prompt starts" a `main.py`
  run with a datastore, as `dk-and-rpk-migration`'s U2 and `read-only-plots`' U5 were.
- **No agent polls a run** (the registry's rule 4). 04 launches, checks once, and waits on the
  task's notification or a `Monitor` on the log's `** driver exit status` line.

The log template is `dk-and-rpk-migration` README §5.1's.

## 6. Decisions

### 6.0 Ruled by the user, 2026-10-19

- **R1 — the store layer.** Adopted, with the core.
- **R2 — a wrapper, not `main.py`.** "I want to be able to launch ChamPBH jobs without using
  `RunRegistryKit`, so changing `main.py` seems the wrong approach."
- **R3 — as SGK does:**
  - the run's naming;
  - the heartbeat's source (stdout, §0.2 item 2);
  - the sidecar's creation (by a person, after the first close, §0.2 item 6).
- **R4 — stores in `var/`**, so `var/datastores/` (the package's default `stores_root`).
- **R5 — the existing stores are dropped, not adopted.** `jordan-normalization` regenerates them.
- **R6 — the dirty flag.** It becomes meaningful once the old stores and the other untracked files
  at the root are cleaned up (§7).
- **R7 — order.** This campaign comes before `jordan-normalization`, so that `jordan-normalization`
  can use it.

### 6.1 For the user to rule (U1–U5)

| | Proposed | Alternatives |
|---|---|---|
| **U1** — `utilities.installed_package_provenance` | **Kept as it is.** The checklist offers replacing it by `runregistrykit.package_provenance` (the same body). But that makes `main.py` and both plot scripts import the package, which R2 means to avoid. The registry records the same fields in every manifest's `packages` anyway | Replace it, as the checklist's item 4: three drivers and `test_package_provenance.py` change, and `main.py` then needs the package installed |
| **U2** — `--inventory` through the wrapper | **Refused**, exit 2, before `begin()`. It is short, read-only and exits before Ray, so there is nothing to register | Passed through to `main.py` unregistered |
| **U3** — the `script` a manifest records | **`main.py`.** Its hash is the hash of what computed. The wrapper is covered by `git_head` and `git_dirty`. SGK records the wrapper because the wrapper `exec()`s modified `main.py` source, which ChamPBH's does not | The wrapper, as SGK. `main.py` is then covered by `git_head` and `git_dirty` only |
| **U4** — the run scripts | **A handover only** (05, §7). The scripts are the user's and untracked, and `jordan-normalization` writes its own | 05 commits a tracked template, e.g. `tools/full_run_template.sh`, launching each block through the wrapper |
| **U5** — the hard-kill route in 04 | **Rehearsed** (04 item 3): `SIGKILL`, `stale`, a refused resume, `end`, then a resume to `done`. It is the route the rebuild will need after a power loss | `SIGTERM` only; the route is then covered by 03's tests alone |

### 6.2 Proposed by the planner (P1–P13)

Accepted as proposed unless the user rules otherwise.
- **P1 — the wrapper's name and place**: `tools/registered_run.py`, run as `./venv/bin/python
  tools/registered_run.py`, beside the other tools. Its tests are in `tools/tests/`.
- **P2 — its flags.** `--campaign`, `--prompt`, `--register SLUG` and `--purpose` are all required,
  since the wrapper exists only to register; `main.py`'s arguments follow `--`. An unregistered run
  is `main.py` itself (R2). For the rebuild, for example: `--campaign jordan-normalization --prompt
  NN --register C1-main-M1e-3`, the slug matching `--job-name`.
- **P3 — `results`** is `main.py`'s `--database` as `main.py`'s parser resolves it. A run with none
  is refused, since `main.py` would print its help and exit.
- **P4 — the heartbeat**: SGK's `RegisteredStream` and `StageTracker`, with a 30 s throttle.
  RayWorkPool's 300 s floor means a beat arrives at most about every 5 min, inside the 900 s
  window. A queue whose items all take longer than 900 s (possible once `jordan-normalization`
  multiplies a history's cost) shows stale while it works. `heartbeat_means` says so, and
  `list --stale-after` is the reader's remedy, not a longer window in the package.
- **P5 — the signals**: SGK's design. Its handler records `killed` (`128 + signum`) as soon as it
  runs; `alarm(0)` before finishing; `ray.init` wrapped. 04 measures, on Ray 2.53 attached to an
  external cluster, that Ray installs its handler as SGK found.
- **P6 — the fingerprint is taken only after `main.py` has returned** and the `ShardedPool` `with`
  block has closed. A signal handler never passes `fingerprint=True`.
- **P7 — `runpy.run_path`**, not `exec` of `main.py`'s text, since nothing is substituted. The
  module then runs as `__main__` from its own file.
- **P8 — the fingerprint format starts at 1**, with `SPEC_DIGESTS` pinned from 02. Golden files are
  never overwritten; a format change adds one.
- **P9 — tests**: `tools/tests/test_registry_config.py` (01);
  `ObjectFactories/tests/test_fingerprint_format_guard.py` and `test_store_fingerprint_golden.py`
  (02); `tools/tests/test_registered_run*.py` (03). All follow `CLAUDE.md`'s test rules: no Ray, and
  stores only in `tempfile` directories.
- **P10 — the branch** is `rrk-adoption`, cut from `main` at `3cd2be4`, merged into `main` at the
  close.
- **P11 — models**: 01, 02 and 05 Sonnet; 03 and 04 Opus.
- **P12 — RunRegistryKit's gates.** Its `extraction` board's G3 (ChamPBH adopts a release) and its
  `interrupted-runs` board's G2 (ChamPBH adopts `v0.2.0`) are recorded in that repository. 05
  drafts both records, quoting the checklist's acceptance (its item 9), and the user commits them,
  as with `dk-and-rpk-migration`'s G3 records.
- **P13 — issues.** None is opened or assigned at planning. No open issue in
  `.documents/OPEN_ISSUES.md` concerns how runs are launched or tracked.

## 7. What this campaign hands to the user

- **The old stores and the other untracked files at the root**: `pilot-*.db`, `pilot-out/`,
  `pilot-*.log`, `full_run_2026.6.0*.sh`. Delete them, or move the scripts into the tree, so that a
  run's `git_dirty` means something (R5, R6). `~/ChamPBH-stores*` and
  `~/ScienceData/ChamPBH-rehearsals/` likewise, when the user wishes.
- **The run scripts' changes** (05's addendum):
  - `STORE_DIR` becomes `var/datastores`;
  - each block runs through the wrapper, detached;
  - `store create --purpose …` after the first block closes;
  - after a hard kill, `python -m runregistrykit end RUN_ID --reason …`, then the block again with
    `--prune-unvalidated`.
- **The production machine**: pull, reinstall the venv from `requirements.txt`, and run `list` at
  the start of a session there; records are per checkout.
- **04's two stores**, kept under `var/datastores/` until the user removes them.
- **The two gate records** in RunRegistryKit (P12).
