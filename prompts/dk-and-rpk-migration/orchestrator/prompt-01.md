# Orchestrator — prompt 01, rehearsal tooling and run provenance

Read [`../README.md`](../README.md) first: §0.2 ("The stores", "A hazard"), §2.2 (01's charter),
§4 (01 ships), §4.1, §4.2, §4.3, §5 and §6.2 (P3, P5). This is the campaign's first note, so it
states the conventions in full. They are SGK's `dk-and-rpk-migration` orchestrator conventions
(`SecondaryGWKit/prompts/dk-and-rpk-migration/orchestrator/prompt-01.md`), adjusted for ChamPBH:
no run registry, three test roots, and a prompt that adds a lasting tool rather than one field.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run an in-process check or a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`01-rehearsal-tooling-and-run-provenance.md`](../01-rehearsal-tooling-and-run-provenance.md)
**Board items:** T1–T3 · **Closes:** nothing · **Narrows:** nothing · **Opens:** only what the work
finds · **Model:** Opus, as the prompt recommends.

**Gate:**
- The prompts are written (`ea3d192`). The tree is clean apart from the user's untracked files.
- `HEAD` is the commit that adds this note, or a later one that touches no code.
  - "Code" here means anything outside `prompts/`, and in particular `utilities.py`, `main.py`,
    `plot_by_beta.py`, `plot_ScalarModel.py`, `tools/`, `Datastore/`, `RayTools/`, `config/` and
    `requirements.txt`.
  - If a later commit touches code, re-check every fact below that it could move, and hand the
    agent the corrections as an addendum.
- One prompt at a time in this checkout. The campaign runs in the main checkout (README §4.4,
  P17), and 02 runs 01's scripts.

## 0. What makes this prompt unusual

**It ships the acceptance instrument of the whole campaign.** `tools/compare_stores.py` decides
R0, R1 and R2, and later the user's science rebuild. A comparison that passes vacuously (two
stores "equal" because a table was skipped, a column dropped, or a float rounded) would let every
later rehearsal pass whatever the layer did. So the review spends most of its effort on T2, and
asks two questions of it:
- **does it see a difference that is there?** One ulp in one value row, one row missing, one
  type value it cannot map: each must be named, with exit 1 or a named problem;
- **does it ignore a difference that is not physics?** Serials, shard assignment, insertion
  order, `timestamp`, the job-name `label`, the timings: each must be reported apart, exit 0.

**The tests build imitation stores with `sqlite3`, so a tool can pass them and miss a real one.**
The review's probe (§3 check 6) runs the tool on a *copy* of the user's 2026-10-02 pilot store, in
the scratchpad, and perturbs the copy. The agent never opens the pilot store; the orchestrator
opens only its copy, through the tool, and the originals only to copy them.

**T1 stands in `importlib.metadata`, so a mock aimed at the wrong name passes vacuously.** If
`utilities.py` does `from importlib.metadata import distribution`, a patch of
`importlib.metadata.distribution` replaces nothing the function calls, and the "not installed"
test passes only because nothing is installed. The breakages catch part of this; the probe without
a mock (§3 check 3) catches the rest.

**What moves on purpose:**
- `utilities.py` gains `installed_package_provenance()`, `format_package_provenance()` and the
  standard-library imports they need.
- `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py` each gain one import and one print. No
  other statement in them changes.
- New: `tools/compare_stores.py`; `Datastore/tests/test_package_provenance.py`;
  `Datastore/tests/test_compare_stores.py`; `.documents/dk-and-rpk-migration/rehearsal/`
  (`build.sh`, `interrupt_when.py`, `stage_counts.py`, `run_readers.sh`, and any fixture).
- The log (`logs/` exists and is empty), the board.
- No issue is expected. If one opens, the index (`.documents/OPEN_ISSUES.md`, 22 open at the last
  count in `prompts/INDEX.md`) gains a section for this board in the same commit.

**The facts, checked by the orchestrator on 2026-10-11 at `ea3d192`** (the code is `52142d7`'s:
`git diff --stat 52142d7 ea3d192 -- . ':!prompts'` is empty). Pass them on.
- **The venv.** Python 3.13.16. `pip show datastorekit raypoolkit` finds neither. **No
  distribution in this venv has a `direct_url.json`**, so there is no real VCS or editable
  install to test against: the tests' stand-ins and the review's `sys.path` probe are the only
  evidence.
- **SGK's helper** is `package_provenance()` at
  `SecondaryGWKit/RunRegistry/__init__.py:122` (the prompt's `:119-169` brackets it, with its
  constant). It treats a `vcs_info` or `dir_info` that is not an object as absent, and sets
  `editable` by `dir_info.get("editable") is True`. Match the behaviour; do not import it.
- **`utilities.py`** (166 lines) imports `time`, `itertools.zip_longest`, `math.fabs`/`log`,
  `traceback.print_tb` and `Units.base.UnitsLike` at module scope. `importlib` and `json` are not
  among them. It is black-clean.
- **The drivers are module-level scripts.** Each parses its arguments at import time and opens the
  pool in a module-level `with ShardedPool(` block:

  | Driver | `parse_args()` | `if args.database is None: … sys.exit()` | `ray.init(` | `ShardedPool(` |
  |---|---|---|---|---|
  | `main.py` | `:79` | `:82-84` | `:87` | `:1338` |
  | `plot_by_beta.py` | `:90` | `:92-94` | `:97` | `:1007` |
  | `plot_ScalarModel.py` | `:70` | `:72-74` | `:77` | `:1577` |

  `main.py` also constructs a `ProfileAgent` (an actor with its own database) at `:96-101`, before
  the pool, when `--profile-db` is given. "Before they open a pool" is best met by printing the line
  **before `ray.init(`**, so that a run that cannot reach its cluster still records what it ran
  with. Where exactly is the agent's choice, logged; the acceptance test is the `ast` order against
  `ShardedPool(`. `main.py` already imports from `utilities` (`:74`); the other two do not.
  All three, and `tools/`, are black-clean at `ea3d192`.
- **Pre-images**, for the `ast` comparison (§3 check 4): `git rev-parse ea3d192:<path>` gives
  `utilities.py` `bb9b4a14…`, `main.py` `4bc7c9c5…`, `plot_by_beta.py` `0de115e3…`,
  `plot_ScalarModel.py` `b0c729fc…`.
- **The prompt's line references hold.** `ShardedPool.py:80-135` is the constructor's create/open
  branch (`self._primary_file = Path(db_name).resolve()` at `:88`, so the `shards` table records
  absolute paths); `:285-345` is `_write_shard_data`/`_read_shard_data`.
  `ObjectFactories/ScalarModel.py:116-126` and `:542-547` are the three polymorphic pairs;
  `main.py:257`, `:520`, `:783` build the three `label`s from `args.job_name` and
  `datetime.now()`; `ComputeTargets/ScalarModel.py:1231-1262` is `build_extra_data`.
- **What a ChamPBH primary holds** (the pilot primary, read `mode=ro` by the orchestrator): the
  tables `replicated_tables`, `shard_key_config`, `shard_keys`, `sharded_tables`, `shards`, and no
  client table. Its `shards` rows are absolute paths into this checkout.
- **Tables the agent must classify, not assume.** Read from the pilot store and
  `config/sharding.py`; the agent confirms each from the code:
  - `replicated_tables` names `LambdaCDM` (`config/sharding.py:34`), but **no shard has a
    `LambdaCDM` table** and `Datastore/SQL/Datastore.py:93-121` registers no `LambdaCDM` factory.
    A listed replicated table absent from every shard must not crash the tool; what it reports is
    the agent's choice, logged.
  - `ExponentialCoupling` is **sharded**, not replicated (`config/sharding.py`'s
    `sharded_tables`, and the primary's).
  - The three `*_tags` tables (`ScalarModel_tags`, `AdiabaticHistory_tags`, `BBNData_tags`)
    appear in **neither** list, exist on every shard, have **no `serial` column** (primary key
    `(model_serial, tag_serial)`, plus `timestamp`), and carry two foreign keys (`store_tag`, the
    parent). The identity memo keyed by `(shard, table, serial)` cannot assume every table has a
    `serial`.
- **The polymorphic references.** No `*_type`/`*_serial` pair in any factory but `ScalarModel`'s
  three. The type values come from three separate identifier spaces:
  `CosmologyModels/model_ids.py` (0–3), `CosmologyConcepts/Potentials/model_ids.py`,
  `CosmologyConcepts/ConformalCouplings/model_ids.py`. `LambdaCDM_GenericEOS.type_id` returns its
  equation of state's identifier (`CosmologyModels/GenericEOS/LambdaCDM_GenericEOS.py:87-89`), so
  several cosmology type values name one table (`QCD_Cosmology`), and `LAMBDACDM_IDENTIFIER = 0`
  is used by no class. The pilot's `ScalarModel` rows carry `(cosmology, potential, coupling)` type
  `(3, 1, 0)`. A value the code cannot map is a §4 stop for the agent only if the map cannot be
  *derived*; a value the code derives as "no stored class" is declared as such.
- **Factory `register()`** is an instance method; the instances are in
  `Datastore/SQL/Datastore.py:93-121` (`_factories`). Importing them imports `ray` but needs no
  cluster: `sqla_ScalarModelFactory().register()` returns a dict with `version`,
  `key_on_version`, `stepping`, `timestamp`, `validate_on_startup`, `columns`. Test 13 may read
  them this way.
- **`extra_data`.** `build_extra_data` stores `reflections`, `cap_fraction`, `cap_floor`,
  `cap_global_max_step`, `jacobian_factor_max`, `accepted_steps`, `steps_rejected_by_exception`
  and the three RHS-value tuples. The orchestrator saw no field named as a time; the agent confirms
  from where each is computed.
- **The pilot log's lines** (`pilot-main.log`, 33 lines): stage banners
  `** CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID`, `** CALCULATE ADIABATIC TRANSGRESSION
  PARAMETERS`, `** CALCULATE BBN DATA`; `   -- ALL WORK ITEMS COMPLETE in time 4m 53.5s`;
  `      Queue summary: 5 lookup (avg 61.32/hr), 5 compute (avg 61.32/hr), 5 store (avg
  61.32/hr), 5 validation (avg 61.32/hr)`; `-- AdiabaticHistory: 0 models skipped because their
  ScalarModel failed`; `-- BBNData: 0 models skipped because their ScalarModel failed; 0
  computations skipped because of a stored failure`; and three `!! warning:` super-Planckian
  lines.
- **The readers' flags exist.** `config/argument_parser.py` has `--config`, `--database`,
  `--shards`, `--inventory`, `--phi-init-Mp`, `--output`, `--prune-unvalidated`, `--job-name`,
  `--M-values-Mp`, `--beta-values`, `--ray-address`. `tools/bbn_from_store.py` takes a positional
  `STORE_STEM` and `--beta`, `--M-Mp`, `--small-network`, `--variant`; `prod` is a variant. The
  agent still checks each against the parser (prompt §2.3).
- **DatastoreKit `v0.2.3`** resolves to `0de2b161ba09362adc36231a7b5b12d99dec8bb3`;
  `datastorekit/shard_paths.py` and `docs/client-contract.md` §5 ("The layer's own tables") exist
  at the tag.
- **Ray.** No Ray process is running. The agent starts none.
- **Not touched, and staying:** every factory, `Datastore/SQL/`, `RayTools/`, `config/`,
  `requirements.txt`, every existing test, `tools/bbn_from_store.py`, the pilot store and logs,
  `~/ChamPBH-stores/` and `~/ChamPBH-stores-bt02/`, and the untracked `prompts/jordan-normalization/`.
- **Baselines at `ea3d192`** (2026-10-11, load average 8 → 20 → 44 falling): CosmologyModels 18
  OK (69 s), ComputeTargets 114 OK (403 s), Datastore 35 OK (5 s) =
  `test_bbn_failure_lookup` 6 + `test_first_bounce_round_trip` 5 +
  `test_fixed_T_values_round_trip` 5 + `test_scalarmodel_failure_reason` 4 +
  `test_shard_key_assignment` 4 + `test_version_keyed_lookups` 11.
- **Expected counts after 01:** CosmologyModels 18, ComputeTargets 114, unchanged; Datastore 35 +
  the new tests, by module.

**What the review exists to establish.**
- **(E1) The provenance helper.** `None` for a distribution not installed; otherwise exactly the
  five keys, each from its PEP 610 place. An absent, unreadable or non-object `direct_url.json`
  gives `None, None, None, False` and never raises. It imports nothing it describes, and uses the
  standard library only. It works against the real `importlib.metadata`, not only the mock.
- **(E2) The drivers.** Each prints the line exactly once, at module level, before `ShardedPool(`;
  nothing else in any driver changes.
- **(E3) The comparison reads only what it should.** Every `sqlite3.connect` is `mode=ro` on a
  globbed file; no path from a `shards` table is ever opened; no layer is imported; no file changes.
- **(E4) The comparison sees every physics difference and only those.** Canonical floats by
  `float.hex`; foreign keys resolved recursively on the same shard; `version` by label; the
  polymorphic map derived and complete; the bookkeeping list justified entry by entry; replicated
  copies cross-checked; near misses paired; exit codes 0/1/2 as specified. On a real store, not
  only on imitations.
- **(E5) The scripts.** Each prints its usage, takes `REHEARSAL_ROOT`, starts no Ray; `build.sh` is
  §4.1's command verbatim but for its parameters; `interrupt_when.py` signals only the pid it is
  given, and only after the banner and the delay.
- **(E6) The pins.** The five breakages each fail the named test, for the stated reason.
- **(E7) The records.** The board's §1 row, T1–T3 and `### 5.2 After prompt 01`; the counts.

**Conventions.** All of these bind the agent, and the brief says so.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Suites.**
  - Use `CLAUDE.md`'s commands, write the output to a file in the agent's scratch directory, and
    grep the verdict from it.
  - Run each suite **in the foreground**, serially, within the turn. A Bash call is capped at ten
    minutes; ComputeTargets took 403 s at dispatch. If it nears the cap under load, run it in two
    pattern groups and add the counts.
  - A failure is re-run alone before it is called real.
- **Breakage diffs** are recorded so that `git apply` replays them from a clean tree at the agent's
  commit. Check each with `git apply --check` and `-R --check`, and replay in `bash`, not zsh.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. Python's
  `tempfile` in tests is fine. No scratch `.py` file goes under `Datastore/`, `tools/` or
  `.documents/`, even for a moment.
- **The venv.** The agent installs nothing, uninstalls nothing and runs no `pip install`. 03 is
  the first prompt that changes the venv.
- **Stores.** The agent opens no store but a test's temporary one: not the pilot store (repository
  root), not `~/ChamPBH-stores/`, not `~/ChamPBH-stores-bt02/`, and it does not create
  `~/ScienceData/` (02 does).
- **Ray.** The agent starts no Ray, and runs no driver.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit, and check
  `git log` before measuring at `HEAD`.

## 1. Before you dispatch

1. **The tree.** Record the branch (`dk-and-rpk-migration`) and `HEAD`.
   - `git status --short` shows only the user's untracked entries (`pilot-2026.6.0*.db`, 17 files;
     `pilot-main.log`; `pilot-plot.log`; `pilot-out/`; `full_run_2026.6.0.sh`;
     `full_run_2026.6.0-refresh.sh`; `prompts/jordan-normalization/`), and `git diff --cached` is
     empty.
   - `git log --oneline ea3d192..HEAD` touches no code; otherwise see the gate.
2. **Ray.** `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
3. **The venv.** `./venv/bin/pip show datastorekit raypoolkit` finds neither (the prompt's gate).
4. **Baselines.** The three roots at `HEAD`, serially, in the foreground, with the load average.
   §0 gives the dispatch measurement. Give the agent the numbers.
5. **The stores' mtimes**, for check 9: `/usr/bin/stat -f '%m %z %N'` over
   `~/ChamPBH-stores/*.db`, `~/ChamPBH-stores-bt02/*.db`, `pilot-2026.6.0*.db` and
   `pilot-main.log`, saved in the scratchpad (52 files at dispatch). (The `stat` on `PATH` is GNU's;
   use `/usr/bin/stat`.)

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`, and does not read prompts 02–04.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/01-rehearsal-tooling-and-run-provenance.md`, in
  README §5.1's form, with the prompt's §6 additions.
- **The files it may add or change are the prompt's §5 list, and nothing else.** A test of
  `stage_counts.py` belongs in one of the two named test modules, or, if it goes in a third, that is
  a deviation it logs. It must not touch `orchestrator/`, `measure/`, `README.md`, any factory,
  `Datastore/SQL/`, `RayTools/`, `config/`, `tools/bbn_from_store.py` or any existing test.
- **It runs no build, opens no real store, installs nothing and starts no Ray.**
- **Stop and ask** on any of the prompt's §4 conditions. An existing test that fails is current
  behaviour moving, never a reason to edit it.
- **It replies with** the log's path; the provenance line's exact format, and its output in this
  venv; the bookkeeping list and the polymorphic map; the new tests by module, with counts; the
  three suites; every deviation with its tag; and the "State handed to the next prompt" section,
  verbatim.

## 3. The review — ten checks

1. **Scope.** `git diff HEAD~1 HEAD --stat` (at the agent's commit) touches only the prompt's §5
   list.
   - Outside `utilities.py`, the three drivers, `tools/compare_stores.py`, the two new test modules
     (and any fixture), `.documents/dk-and-rpk-migration/rehearsal/` and
     `prompts/dk-and-rpk-migration/{logs/,IMPLEMENTATION_STATE.md}`, the diff is empty — but for
     `.documents/OPEN_ISSUES.md` if an issue opened.
   - `git status` shows the same untracked set as §1 step 1, and nothing new.
2. **E1, by reading.** Read the two functions in full.
   - Only `PackageNotFoundError` gives `None`. Any failure to read or parse `direct_url.json`,
     including a value that is not an object, gives the four defaults; no other exception escapes.
   - `url` is the top-level `url`; `commit` and `requested_revision` are `vcs_info`'s `commit_id`
     and `requested_revision`; `editable` is true only when `dir_info.editable` is `True`.
   - The tests' patch replaces the name the module actually calls. Each of the five cases asserts
     every field, and the format of the line.
   - Then the odd cases, from the scratchpad with the agent's stand-in pattern: `vcs_info` or
     `dir_info` a list or a string; `dir_info: {"editable": false}`; `vcs_info` without
     `requested_revision`; `read_text` raising `OSError`. Each returns without raising.
3. **E1, the probe without a mock.** In the scratchpad, a directory holding
   `datastorekit-0.2.3.dist-info/` with a minimal `METADATA` (`Metadata-Version`, `Name`,
   `Version`) and a PEP 610 `direct_url.json` for `git+https://github.com/ds283/DatastoreKit` at
   `v0.2.3`, commit `0de2b161ba09362adc36231a7b5b12d99dec8bb3`. Put it first on `sys.path` in a
   fresh `./venv/bin/python -I` process with the repository on its path, and call both functions.
   - `datastorekit` gives every field, `editable: False`; `raypoolkit` gives `None`; the line says so.
   - Repeat with an editable `direct_url.json` (`{"url": "file:///…", "dir_info": {"editable":
     true}}`): the line says `editable` visibly. Then with the file removed.
4. **E2, by `ast` against the pre-images**, docstrings stripped. For each driver, the module body
   differs from `ea3d192`'s by exactly one import and one expression statement printing
   `format_package_provenance(installed_package_provenance())` (or the agent's equivalent), at
   module level, before the `With` whose context is `ShardedPool(`. Every other top-level statement
   dumps identically. `utilities.py`: every pre-existing function dumps identically.
5. **E3, the opens.** Run the tool on a test's stores (or on check 6's copies) under a wrapper from
   the scratchpad that replaces `sqlite3.connect` with a logger and then runs
   `tools/compare_stores.py` by `runpy.run_path(..., run_name="__main__")`. Every open is
   `file:…?mode=ro` with `uri=True`, on a file the glob finds. Then grep the tool for `import` and
   `ATTACH`: no `Datastore`, `datastorekit`, `ray`, `sqlalchemy`, and no `ATTACH`.
6. **E4, the real store.** Copy the pilot store's 17 files into a scratch directory twice, as
   `A/pilot.db` + shards and `B/pilot.db` + shards (renamed so the glob finds them). The copies'
   `shards` tables name the originals: that is the hazard the tool must ignore, and check 5's
   wrapper shows it does. Then, on the copies only:
   - `A` against `B`: exit 0, every client table compared with its row count, and the wall time;
   - in `B`, one `ScalarModelValue` float moved one ulp (`math.nextafter`): exit 1, the row named by
     physical labels, paired as a near miss with both values and the relative difference;
   - restored; in `B`, one `ScalarModel.label` and one `compute_time` changed: exit 0, both reported
     as bookkeeping;
   - restored; in `B`, one `ScalarModel.potential_type` set to 99: a named problem, no traceback;
   - restored; in `B`, one replicated row's float changed on one shard only: a named problem;
   - restored; in `B`, one `BBNData` row deleted: exit 1, an A-only row named by its labels;
   - `--snapshot A` then `--schema B --against` it: no difference; then one index dropped in `B`'s
     shard 0: reported.
   Delete the copies afterwards.
7. **E4, by reading.** The bookkeeping list: each entry with its `path:line`, each confirmed where it
   is written; no physics column in it (`compute_steps`, `RHS_evaluations`, `z_samples`, the
   failure columns, every abundance, every value-table column). The polymorphic map: each value
   with its `type_id` source, and the `LambdaCDM_GenericEOS` inheritance handled. The `*_tags`
   tables and `LambdaCDM` handled as the log says. The memo is a digest keyed by shard, table and
   row key.
8. **E5 and E6.**
   - Each script's `--help` or usage line. `build.sh`'s command against README §4.1, token by token.
   - `interrupt_when.py`, from the scratchpad: a `sleep 600` whose pid is in a pidfile, a log that
     gains the banner after a few seconds; it waits the delay, sends `SIGTERM` to that pid only,
     and says so. A second run where the process exits before the banner: it says so, exit
     non-zero.
   - In `bash`, replay each of the five breakage diffs with `git apply`, run the named test, see it
     fail for the stated reason and nothing else fail; revert with `git apply -R`; `git status`
     clean.
9. **The stores' mtimes** match §1 step 5. `~/ScienceData/` does not exist.
10. **E7 and the suites.**
    - The board has the §1 row (✅, "this commit", the log), T1–T3 and `### 5.2 After prompt 01`.
    - No §3 entry and the index untouched, or, if one opened, the index's row, section, count and
      date in the same commit.
    - The three roots at `HEAD`, serially: 18, 114, 35 + the new tests. `black --check` clean on
      every touched Python file.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 01* paragraph in §5.2: what each
  check found, the probe's outcome, and anything handed on to 02. Fix small residue in a follow-up
  commit of its own:
  - "this commit" → the SHA;
  - a list of the orchestrator notes on the board, with this one marked "used for 01".
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the provenance line, and its output in this venv and in the probe;
  - the comparison's behaviour on the pilot copy: the clean pass, its time, and each perturbation;
  - the bookkeeping list and the polymorphic map;
  - the tests added, by module, and the breakages;
  - the three roots' counts.
- **Then 02**, R0. It runs these scripts and this tool unchanged (README §4). Its note is written
  after this review.
