# Orchestrator — prompt 02, read-only plot pools

Read [`../README.md`](../README.md) first: §0.1, §0.3 in full, §2.2 (02's charter), §5, §6.1 (U3,
U4, U6) and §6.2 (P1–P4, P6, P7). Then the board's §5.2, which records what 01 handed on. The
conventions are those of [`prompt-01.md`](prompt-01.md) §0 ("Conventions"), restated here where 02
needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`02-read-only-plot-pools.md`](../02-read-only-plot-pools.md)
**Board items:** O1–O5 · **Gate:** "Read-only plots" · **Closes:**
`[08-plot-scripts-open-read-write-pools]` (on the `dk-and-rpk-migration` board) · **Opens:** only
what the work finds · **Model:** Opus, as the prompt recommends · **No network.**

**Gate:**
- 01 has landed and been reviewed (`04486f4`, review `000be0c`). `HEAD` is the commit that adds
  this note, or a later one that touches no code ("code" is anything outside `prompts/`; since
  `000be0c` nothing outside `prompts/` has changed).
- `ray_cluster.sh status`: lock free, no ChamPBH process. No Ray process on the machine
  (`pgrep -lf 'gcs_server|raylet|ray::'`).
- One prompt at a time in this checkout. 03 is not dispatched until 02 is reviewed.

## 0. What makes this prompt unusual

**The code it moves cannot be run here.** The two plot scripts parse `sys.argv` and call `ray.init`
at import, and the prompt (§5) runs neither. So the evidence that nothing a script computes has
changed is indirect: the payloads, compared key for key (§2.2 of the prompt); the scripts' module
bodies, compared by `ast` with the pre-images; and the new module, driven on stand-in stores. 03
runs the scripts under real Ray and compares `histories.csv` with R0's. Every claim the log makes
about the scripts must be one of those three kinds.

**Three kinds of miss, two exception classes.**
- A parameter row (rows 1–5, 8, 9 of README §0.3) and a potential (row 6) are replicated classes:
  a read-only miss raises `ReadOnlyMiss` in the actor (`datastorekit/SQL/Datastore.py:296-312` at
  `v0.3.0`, `_refuse_insert`).
- A coupling (row 7, `ExponentialCoupling`) is sharded: the same inserter raises `ReadOnlyWrite`,
  with `operation="an insert"` and `class_name="ExponentialCoupling"`. `ReadOnlyWrite` is also the
  package's backstop (SQLite's refusal of any write, re-raised with `operation` naming the call).
  Whether the coupling catch is narrowed to the inserter's refusal (by `class_name` and
  `operation`) is the agent's implementation choice, recorded with its reason; the catch is never
  wider than the one lookup.
- **The exception surfaces at `ray.get`, not at submission.** Under the stand-in, `.remote()` runs
  the actor call at once and stores the exception in the reference; `standin_get` raises it
  (`StoreFixtures/standin_pool.py:110-136`). Under Ray it reaches `ray.get` as
  `RayTaskError.as_instanceof_cause()`. So each catch wraps the `ray.get` of one reference. What
  can raise *at submission*, in the driver, is the pool's own routing: a coupling's
  `self._shard_keys[...]` (`KeyError` for a β with no shard key). That is not a read-only miss and
  propagates.

**`ray.get` of a list stops at the first error.** That is why P2 exists. Today the potentials are
one replicated vectorized call (`payload_data=[...]`, one actor call, one reference: one miss
refuses the lot), and the couplings are one vectorized *sharded* call, which the pool already
splits into one reference per β (`ShardedPool._get_impl_sharded_table`, `ShardedPool.py:3365-3389`
at the tag) but which the scripts read with one `ray.get` of the list. For the couplings, P2 is
therefore a change of how the references are read; for the potentials it is a change of the calls.

**One-at-a-time is the same lookup.** The actor's `object_get` (`Datastore.py:507-590` at the tag)
turns a scalar get into `payload_data=[kwargs]` and calls `factory.build` once per payload in both
cases. A sharded scalar get sends `object_get.remote(cls_name, **kwargs)`, exactly what the
vectorized path sends per item. So a per-combination payload equal key for key to the old item is
the same lookup. That is what the log's side-by-side table must show.

**Order.** Today `Potential_array` is in `itertools.product(M_table, Lambda_table)` order (M outer)
and `Coupling_array` in `beta_table`'s order, and `run_pipeline` iterates them. Skipping must keep
the survivors in that order; test 1's "stable order" is this order, asserted, not merely
repeatable.

**What stays where.**
- `config/model_list.py` is `config/` and not 02's to change. `plot_inputs.py` calls
  `build_model_list` and converts the `ReadOnlyMiss` it raises.
- Each script keeps its one `ShardedPool(` (`TestPoolsAreGivenTheConfig` requires exactly one per
  driver, with `factories=` and `serial_batch_sizes=` bound to `config.datastore`'s, and no
  `drop_tables` in the plot scripts), and the provenance print before it and before `ray.init`
  (`test_package_provenance.TestDriversPrintProvenance`).
- **01's check stays as it is.** `TestCheckPlacementByAst` requires the `_ray_problem = …`
  assignment to be followed *immediately* by its `if`, both before `ray.init`. The `--drop` refusal
  goes before the assignment, never between the two.
- The history lookups (rows 10–16, inside each `run_pipeline`: `plot_by_beta.py:678-1013`,
  `plot_ScalarModel.py:1502-1583`) and
  everything after the setup stay unchanged, `_do_not_populate` included.
- **A refusal at open** (a missing store, a journal, another `VERSION_LABEL`, U6) is the package's
  and reaches the user as the package raises it. Catching it to print it more neatly is not asked
  for; if the agent thinks it should be, that is an observation, not a change.

**What moves on purpose:**
- New: `plot_inputs.py`, `ComputeTargets/tests/test_plot_inputs.py`.
- `plot_by_beta.py`, `plot_ScalarModel.py`: the setup replaced by calls; `read_only=True` on the
  pool; the `--drop` refusal; the help or docstring line on `--prune-unvalidated` and `--drop`;
  `plot_by_beta.py`'s `job_name` `"plot_ScalarModel"` → `"plot_by_beta"`; imports that the move
  leaves unused (`itertools`, if nothing else uses it) may go, each named in the log.
- `ObjectFactories/tests/test_datastore_config.py`: `TestDriversPassShardKeyMapping.EXPECTED_COUNTS`
  gains `"plot_inputs.py": 0` (P6). No other text test is expected to change (below).
- `prompts/dk-and-rpk-migration/IMPLEMENTATION_STATE.md`: the issue from §3 to §4 with its
  **Resolved** line. The header's "Four issues stay open" is that board's count of the same move
  and may become "Three"; nothing else on that board.
- `.documents/OPEN_ISSUES.md`: the §1.12 row deleted and the section saying it emptied; §1.11's
  prose if it counts the assigned issue; the header's count 27 → 26, prose and date.
- This board and the log.

**The facts, checked by the orchestrator on 2026-10-13 at `000be0c`.** Pass them on.
- **The tree.** Branch `read-only-plots`, `HEAD` `000be0c` before this note; nothing staged; the
  untracked set is 01's (17 `pilot-2026.6.0*.db`, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`,
  `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh`, `prompts/jordan-normalization/`).
  `plot_inputs.py` and `test_plot_inputs.py` do not exist.
- **Ray.** `ray_cluster.sh status` exits 0: lock free, no ChamPBH process, ports free. `pgrep`
  finds nothing. `/tmp/ray/` holds 160 `session_*` directories.
- **The setup, at `000be0c`.** The prompt's line numbers were taken at `bacca64`; 01 added six
  lines above the pool. Now:
  - `plot_by_beta.py:1016-1030` the pool; `:1031-1037` tolerances; `:1039-1047` `T_init`;
    `:1049-1058` φ, π; `:1060-1104` the two helpers; `:1106-1114` `read_table` and the two
    `ray.get`s; `:1116` `build_model_list`; `:1118-1127` the baseline (stays); `:1129-1142`
    `T_stop` per model; `:1144-1156` the call of `run_pipeline` (defined at `:678`).
  - `plot_ScalarModel.py:1586-1600` the pool; `:1601-1686` the same setup, without the baseline;
    `:1688-1701` `T_stop`; `:1703-1714` the call of `run_pipeline` (defined at `:1502`).
  - The two copies are textually identical from `# build absolute and relative tolerances` to
    `model_list = build_model_list(pool, units)`.
  - Both scripts black-clean. Pre-images (`git rev-parse --short=8 000be0c:<path>`):
    `plot_by_beta.py` `3c339570`, `plot_ScalarModel.py` `819ba7af`,
    `test_datastore_config.py` `5856ec74`, the `dk-and-rpk-migration` board `87184ae4`,
    `OPEN_ISSUES.md` `03951f38`.
- **`main.py`'s side of each payload** (what the test store must be built with), at `000be0c`:
  `T_init` `:930-932`; φ, π `:934-944`; the β, M, Λ helpers `:952-970`; the potential helper
  `:972-1006`; the coupling helper `:1008-1016`; tolerances `:1021-1026` (by the string `"tolerance"`, the scripts by the class
  `tolerance`: the same `cls_name`); `build_model_list` `:1194`; `T_stop` `:1199-1207`.
- **The package at `v0.3.0`** (installed: `datastorekit 0.3.0`). `ReadOnlyMiss` and `ReadOnlyWrite`
  are at `datastorekit/replication.py:193` and `:240`, both `RuntimeError`s that pickle by
  `__reduce__`. The replicated read-only get draws one shard at random
  (`ShardedPool._get_impl_replicated_table_read_only`, `:774-795`). The worked example is
  **`datastorekit/tests/test_read_only_pool.py`** (the prompt's "Read first" 4 says
  `datastorekit/tests/`; there is no top-level `tests/`): it builds a store read-write, copies it,
  and reopens with `read_only=True`, `shards=3` and `read_table_config=`
  (`_ReadOnlyTestCase.open`, `:186-205`); `TestAtOpen.test_a_missing_primary_creates_nothing`
  (`:664-676`) is test 6's pattern.
- **Ray 2.53.0**, `ray/_private/worker.py`: `raise value.as_instanceof_cause()` for a
  `RayTaskError` at `:1015-1016` and `:2978-2979`.
- **The stand-in.** `StoreFixtures/standin_pool.py`: `StandinCluster.active()` patches `ray.get`
  (`:326`); `open_pool(primary, shards=3, version_label="standin", **kwargs)` (`:383-414`) passes
  `read_only=` and `read_table_config=` on and silences the constructor's output. Harness:
  `ObjectFactories/tests/test_version_keyed_lookups.py`, `_TempStoreCase`.
- **Text tests: what 02 should leave passing unchanged.**
  - `test_extraction.TestDriverReadsTheColumns.test_f_plot_by_beta_uses_the_new_functions` reads
    names called in `plot_by_beta.py`, `band_half_width`, and four `"_do_not_populate": True`
    literals (`:732`, `:870`, `:904`, `:931`, all in `run_pipeline`): none in the setup.
  - `test_kinematic_cap_loop`'s stepper label is in each script's `run_pipeline`
    (`plot_by_beta.py:857`, `plot_ScalarModel.py:1524`).
  - `test_initial_field_option.TestNoLiteralLeft` needs an `args.phi_init_Mp` attribute in each
    script. `plot_ScalarModel.py` reads it only at `:1623`, inside the setup that moves: passing
    `args.phi_init_Mp` to `plot_inputs` keeps it. Passing it any other way breaks that test.
  - `test_ray_address` (01's) reads each driver by `ast`.
  - So the only text test expected to change is `test_datastore_config`'s `EXPECTED_COUNTS`. Any
    other is a stop under the prompt's §4, unless it only follows moved text and the log says so.
- **The factories' `build`s write nothing on a hit.** `sqla.update` appears in
  `ObjectFactories/ScalarModel.py`, `AdiabaticHistory.py` and `BBNData.py` only in `validate`, never
  in `build`; the scripts pass `tags=[]`, so no tag is added.
- **The issue.** `prompts/dk-and-rpk-migration/IMPLEMENTATION_STATE.md:226-259`, §3, with its
  **Assigned (2026-10-13)** line; §4 starts at `:276` and holds one entry, whose **Resolved** line is
  the form. Index: `.documents/OPEN_ISSUES.md` §1.12 at `:211-219`; 27 rows (`grep -c '^- \`\['`).
- **Not touched, and staying:** `main.py`, `config/`, every factory, `StoreFixtures/`, `tools/`,
  `RayTools/`, `requirements.txt`, the rehearsal scripts, `CLAUDE.md`,
  `.documents/architecture-summary.md` (04's), the user's run scripts and pilot files, and
  `prompts/jordan-normalization/`.

**What the review exists to establish.**
- **(E1) The module.** `plot_inputs.py` parses nothing and starts no Ray at import; its functions
  take a pool, option values and `units`, not `args`; each lookup's payload equals the scripts'
  old one key for key.
- **(E2) The misses.** A parameter miss raises the module's exception naming the option (or the
  constant) and the value, and the scripts turn it into stderr and exit 2. A missing potential or
  coupling is skipped alone, with one warning each and a count, and the rest come back in today's
  order. Nothing else is caught.
- **(E3) The scripts.** The setup replaced by calls and nothing else moved; `read_only=True`, the
  other pool arguments as they were; `--drop` refused before 01's check; `job_name`; 01's check and
  `ray.init` exactly where `TestCheckPlacementByAst` wants them.
- **(E4) The tests and the pins.** O4's seven tests, on stores built by `main.py`'s lookups and
  reopened read-only; the four breakages each fail the named test.
- **(E5) The records.** The issue closed on its own board and in the index (26); the P2 timing;
  the log, the board, the counts.

**Conventions.** All of these bind the agent, and the brief says so.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Suites.** `CLAUDE.md`'s commands, ComputeTargets in its two halves, each in the foreground,
  serially, output to a file in the agent's scratch directory with the verdict grepped from it, and
  the load (`uptime`) beside each. A failure is re-run alone before it is called real.
  `test_prym_passenger.test_a_oscillating_case_completes` has a 60 s wall bound that fails at load
  ≳ 70 (board §5.2, check 10): re-run it alone and record the load before calling it anything.
- **Breakage diffs** are recorded so that `git apply` replays them from a clean tree at the agent's
  commit. Check each with `git apply --check` and `-R --check`, and replay in `bash`, not zsh.
- **Scratch.** The agent's own subdirectory of the session scratchpad, never `/tmp`. Python's
  `tempfile` in tests is fine. The P2 timing is a scratch script, not a committed test.
- **The venv.** Nothing installed or uninstalled.
- **Stores.** None opened but a test's temporary one. The pilot store in the repository root is
  the user's and is never opened.
- **Ray.** The agent starts no Ray and runs no plot script. `ray stop` is never run.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` shows only §0's untracked set; nothing
   staged; `git log --oneline 000be0c..HEAD` touches no code.
2. **Ray.** `ray_cluster.sh status` and `pgrep -lf 'gcs_server|raylet|ray::'`; the count of
   `/tmp/ray/session_*` (160 at writing), for check 9.
3. **Baselines.** The four roots at `HEAD`, serially, with the load. Give the agent the numbers.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`, and does not read prompts 03–04.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/02-read-only-plot-pools.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may add or change are the prompt's §5 list, and nothing else.**
- **It runs no plot script, starts no Ray, opens no store outside a `tempfile` directory and
  installs nothing.**
- **Stop and ask** on any of the prompt's §4 conditions. An existing test that fails is current
  behaviour moving, never a reason to edit it, except as the prompt's O5 allows.
- **It replies with** the log's path; the message as committed; `plot_inputs.py`'s public
  functions with their signatures and what each raises or skips; the exception class and the
  messages (one parameter miss, one potential, one coupling, verbatim from a test); the payload
  table's verdict; the P2 timings; the new tests by class, with counts; every text test touched,
  with its reason; the four suites; every deviation with its tag; and "State handed to the next
  prompt", verbatim.

## 3. The review — twelve checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` touches only the prompt's §5 list. `git status`
   shows §0's untracked set and nothing new (no stray store, journal or figure).
2. **E1, by reading.** `plot_inputs.py` in full. No `argparse`, no `sys.argv`, no `ray.init` at any
   level; no `args` parameter. A probe from the scratchpad: `import plot_inputs` in a fresh
   interpreter leaves `ray.is_initialized()` False.
3. **E1, the payloads.** For each moved lookup, the old item (pre-image line) and the new call side
   by side, as the log's table claims: the class name, every key, every value expression. For the
   potentials, the four types each with their keys (`n` only for Exponential and InversePower), and
   an unknown type still `ValueError`.
4. **E2, the misses by reading.** Each `except` wraps one `ray.get` of one reference. The parameter
   catch catches `ReadOnlyMiss` only; the coupling catch `ReadOnlyWrite` (narrowed or not, as the
   log says why); the potential catch `ReadOnlyMiss`. The skipped combinations keep today's order
   for the rest. The messages name the option and the value; `T_CMB` and the cosmology name the
   constant.
5. **E3, the scripts by `ast` against the pre-images.** Each script's module body differs from
   `000be0c`'s only in: the imports; the `--drop` refusal (before the `_ray_problem` assignment);
   the `ShardedPool` call's keywords (`read_only=True` added, `job_name` in `plot_by_beta.py`); and
   the `with` body's setup statements replaced by calls. Every top-level function (`run_pipeline`,
   `build_beta_plot`, …) dumps identically. Inside the `with`, `plot_by_beta.py`'s baseline and both
   scripts' `for model_data in model_list:` loops are unchanged but for how `T_stop` is obtained.
   `git diff -U0` of the scripts read in full.
6. **E3, `--drop` by hand.** From the repository root, with `RAY_ADDRESS=127.0.0.1:9` in the
   environment and `--database <scratch>/absent.db`, each script with `--drop bbn-data` and no
   `--ray-address`, then with `--ray-address 127.0.0.1:9`: exit 2, the `--drop` message, no
   traceback, nothing created. **Only after check 5 has shown the refusal precedes `ray.init`.**
   Each script with `--help`: the `--prune-unvalidated`/`--drop` line is there.
7. **E4, the tests.** `test_plot_inputs.py` read in full against O4's seven. The stores are built
   with `main.py`'s payloads (§0's line list), closed, and reopened `read_only=True`; test 5 hashes
   primary and every shard and looks for `-journal`, `-wal`, `-shm`; test 6's path and its parent
   do not exist afterwards; test 7's `ast` checks would fail on the pre-images (a probe: run the
   test's helper against `git show 000be0c:<script>`). The docstring says no PRyMordial solve.
8. **E4, the breakages.** Each of the log's four diffs replayed in `bash`: `git apply --check`,
   apply, run the named test, see it fail for the stated reason, `git apply -R`, `git status`
   clean.
9. **No Ray started.** After checks 6, 8 and 11, `pgrep` finds nothing, `/tmp/ray/session_*` still
   counts as §1 step 2, and `/tmp/ray-cpbh` is unchanged.
10. **E5, the records.** The `dk-and-rpk-migration` board: the entry moved whole to §4, with a
    **Resolved (date):** line naming 02, "this commit" and the three findings; the header count;
    nothing else. The index: §1.12's row gone and the section saying so, the count 26 by `grep -c`,
    the header's prose and date. This board: row 02 ✅, O1–O5 ✅, the gate ✅, §3.1, `### 5.3 After
    prompt 02 (<date>)`. The log in §5.1's form; the P2 timings; every deviation classified; the
    Ray lines of O2 cited.
11. **The suites** at the agent's commit, serially: 18, 61 + (70 + new), 63, 34. `black --check`
    on every touched Python file; the two scripts and `plot_inputs.py` byte-compile.
12. **Residue and leftovers.** `prompts/INDEX.md`'s row for this campaign (it still says "0
    landed" after 01; the orchestrator's to fix, not the agent's).

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 02* paragraph in §5.3. Fix
  small residue in a follow-up commit of its own:
  - "this commit" → the SHA (this board, the log, and the `dk-and-rpk-migration` board's
    **Resolved** line);
  - the orchestrator notes' list on the board, with this one marked "used for 02";
  - the status line: 2 of 4 landed; `prompts/INDEX.md`'s row.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:** the message as committed; what a plot run now does on a store whose
  options match, on a missing potential or coupling, and on a missing parameter; `--drop`; the
  tests added and the breakages; the P2 cost; the four roots' counts; the issue closed (index 26).
- **Then 03.** Its note is written after this review. 03 is the first prompt to run the scripts
  under real Ray and is the only evidence that `except ReadOnlyMiss` catches a `RayTaskError` there.
