# rrk-adoption campaign — implementation state

**Last updated:** 2026-10-20 · **Status: COMPLETE — 5 of 5 prompts landed (all reviewed); 1 issue open (§3).
Final suites: CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories 78, tools 84, RunDrivers 61, all OK.
R1–R7, U1–U5 and P2–P13 ruled by the user on 2026-10-19, as proposed; P1 amended (the wrapper in
`RunDrivers/`) (README §6).**
The plan is [`README.md`](README.md), written on 2026-10-19 on branch `rrk-adoption`, cut from
`main` at `3cd2be4`.

The campaign adopts RunRegistryKit `v0.2.0`, with its store layer:
- the pin, `runregistrykit.toml`, `var/` ignored, and the rules in `CLAUDE.md`;
- a fingerprinter at format 1, with a guard and a golden fingerprint;
- a wrapper driver, `RunDrivers/registered_run.py`, which registers a `main.py` run and leaves `main.py`
  unchanged and launchable without it;
- a rehearsal of registered, interrupted and hard-killed builds on the isolated cluster.

It comes before `jordan-normalization`, so that the science rebuild is a registered run.

**Campaign:** [`README.md`](README.md) ·
**Code:** `requirements.txt`, `runregistrykit.toml`, `.gitignore`, `config/fingerprint.py`, a new
`RunDrivers/registered_run.py` and its test root `RunDrivers/tests/`; `main.py` unchanged ·
**Verification:** `.documents/rrk-adoption-verification.md` (created by 04) ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.14

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [The pin, the config and the rules](01-pin-config-and-rules.md) | `runregistrykit[stores]@v0.2.0`; `runregistrykit.toml`; `var/` ignored; `CLAUDE.md`'s rules | Sonnet | ✍️ 2026-10-19 | ✅ 2026-10-19 | `b0f3488` | [log](logs/01-pin-config-and-rules.md) |
| 02 | [The fingerprinter](02-the-fingerprinter.md) | `config/fingerprint.py` at format 1; `[stores]`; the format guard; a golden fingerprint | Sonnet | ✍️ 2026-10-19 | ✅ 2026-10-19 | `c9eea37` | [log](logs/02-the-fingerprinter.md) |
| 03 | [The wrapper](03-the-wrapper.md) | `RunDrivers/registered_run.py`: register, beat from stdout, signals after `ray.init`, `main.py` by `runpy`, finish with the fingerprint | Opus | ✍️ 2026-10-19 | ✅ 2026-10-19 | `d5dd930` | [log](logs/03-the-wrapper.md) |
| 04 | [Rehearse registered runs](04-rehearse-registered-runs.md) | a registered build; `SIGTERM` and resume; `SIGKILL`, `end` and resume; `store create`; stores compared; `histories.csv` = R0's | Opus | ✍️ 2026-10-19 | ✅ 2026-10-20 | `d5d8c74` | [log](logs/04-rehearse-registered-runs.md) |
| 05 | [Close](05-close.md) | documents, handover, RunRegistryKit's gate drafts, indexes, board | Sonnet | ✍️ 2026-10-19 | ✅ 2026-10-20 | `c94c8d5` | [log](logs/05-close.md) |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).** 01–05 are written once the user has ruled on README §6.1 and §6.2.
*Written 2026-10-19 (§5.1).*

Orchestrator notes are written one prompt at a time, in [`orchestrator/`](orchestrator/), just
before each dispatch:
- [`orchestrator/prompt-01.md`](orchestrator/prompt-01.md) (`6dd54a0`), used for 01.
- [`orchestrator/prompt-02.md`](orchestrator/prompt-02.md) (`6824826`), used for 02.
- [`orchestrator/prompt-03.md`](orchestrator/prompt-03.md) (`06ae4bf`), used for 03.
- [`orchestrator/prompt-04.md`](orchestrator/prompt-04.md) (`07aa626`), used for 04.
- [`orchestrator/prompt-05.md`](orchestrator/prompt-05.md) (`e0a52da`), used for 05.

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **C1** | 01 | `runregistrykit[stores] @ …@v0.2.0` in `requirements.txt`; the venv installed from it, not editable | ✅ |
| **C2** | 01 | `runregistrykit.toml` (roots, distributions; no `[stores]`); `var/` in `.gitignore` | ✅ |
| **C3** | 01 | `CLAUDE.md`: the section "Long-running jobs — the run registry", with the rules and `git clean -x` forbidden | ✅ |
| **C4** | 01 | `tools/tests/test_registry_config.py`: the config, the ignore rule, the pin | ✅ |
| **F1** | 02 | `config/fingerprint.py`: `FINGERPRINT_FORMAT = 1`, `SPEC_DIGESTS`, `fingerprinter` | ✅ |
| **F2** | 02 | `[stores] fingerprinter` in `runregistrykit.toml` | ✅ |
| **F3** | 02 | `ObjectFactories/tests/test_fingerprint_format_guard.py`: the Q5 guard, keys kept by type | ✅ |
| **F4** | 02 | `ObjectFactories/tests/test_store_fingerprint_golden.py` and its golden file; the loader and the command line | ✅ |
| **W1** | 03 | the package `RunDrivers/` and its test root; `RunDrivers/registered_run.py` (P1 as amended) | ✅ |
| **W2** | 03 | the wrapper's flags and refusals before `begin()`, exit 2 (P2, P3, U2) | ✅ |
| **W3** | 03 | registration: `begin` with `script` = the program (U3), `scope`, `heartbeat_means`; the stream proxies | ✅ |
| **W4** | 03 | `ray.init` wrapped; the handler installed after it (P5) | ✅ |
| **W5** | 03 | `main.py` by `runpy.run_path` (P7); `finish_after_exec` with the fingerprint (P6); `** driver exit status N` | ✅ |
| **W6** | 03 | `RunDrivers/tests/test_registered_run*.py`, with no Ray; `CLAUDE.md`'s test command and `RunDrivers/` sentence | ✅ |
| **H1** | 04 | `reference`, `done`; `store create` and the fingerprint | ✅ |
| **H2** | 04 | `resumed-a`, `SIGTERM`ed, `killed`/143, with the signal's timing (P5) | ✅ |
| **H3** | 04 | `resumed-b`, `SIGKILL`ed: `stale`, the resume refused, `end`, `lost` (U5) | ✅ |
| **H4** | 04 | `resumed-c`, `done`, its fingerprint written to the sidecar | ✅ |
| **H5** | 04 | the stores compared; `histories.csv` equal to R0's | ✅ |
| **H6** | 04 | `.documents/rrk-adoption-verification.md` §1 | ✅ |
| **Z1** | 05 | `architecture-summary.md` | ✅ |
| **Z2** | 05 | `CLAUDE.md` | ✅ |
| **Z3** | 05 | the handover, `.documents/review-remediation-verification.md` §4.15 | ✅ |
| **Z4** | 05 | RunRegistryKit's G3 (`extraction`) and G2 (`interrupted-runs`) drafted, in the verification document §2 | ✅ |
| **Z5** | 05 | the board closed; `prompts/INDEX.md`; `.documents/OPEN_ISSUES.md` | ✅ |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Pin | the venv holds `runregistrykit` 0.2.0 from the tag, not editable; `list` runs on the empty runs root; `var/` is ignored | 01 | ✅ 2026-10-19 |
| Fingerprint | the config's fingerprinter loads; the format guard and the golden fingerprint pass, and each deliberate breakage fails | 02 | ✅ 2026-10-19 |
| Wrapper | with no Ray, a run through the wrapper is registered before its first line, beats, ends `done`/`failed`/`killed` with `main.py`'s exit code, takes its fingerprint after the program returns, and refuses a store an unfinished run names; `main.py` is unchanged | 03 | ✅ 2026-10-19 |
| R | on the isolated cluster, the four builds are recorded as README §2.2 (04) says, the resumed store equals the reference, and `histories.csv` has R0's SHA-256 | 04 | ✅ 2026-10-20 |
| RunRegistryKit G3 / G2 | recorded in RunRegistryKit, on its `extraction` and `interrupted-runs` boards, by the user from 05's drafts (P12) | 05 | drafted 2026-10-20 in `.documents/rrk-adoption-verification.md` §2; recording is the user's |

## 3. Active and unresolved issues

### `[04-the-pool-waits-for-every-in-flight-item-when-fewer-than-three]`

**Opened:** 2026-10-20 by the orchestrator's review of prompt 04, from the agent's observation
(log 04, "Observations not acted on"; verification document §1.11). **Not fixed here:** the code
is RayPoolKit's, which ChamPBH never patches (`CLAUDE.md`, "The packages"), and `main.py` is
byte-identical through this campaign (README §5).

- **What.** Each pass of `RayWorkPool`'s loop blocks in `ray.wait(…,
  num_returns=min(process_batch_size, len(inflight)))` (`raypoolkit/RayWorkPool.py:355-358` at
  `v0.2.0`). `main.py`'s three titled queues set `process_batch_size=3`. So whenever three or fewer
  items are in flight, the driver waits for **all** of them before it processes any. The next
  step of an item that is already done (its store or its validation) is then held behind the
  slowest item in flight. A validation is submitted only when the driver processes the store
  task's result (`:489-525`), and only a validation sets a row's `validated` flag.
- **Seen.** In `resumed-a` (04, H2), β = 10's `ScalarModel` row was written at 23:51:53, 85 s after
  the scalar banner, and was still `validated 0` when the `SIGTERM` came at 23:52:58. In flight were
  β = 10's store task and β = 25's compute, which took 195.7 s in `reference`. So the wait could
  return only when β = 25 finished. `resumed-b` opened the store with `--prune-unvalidated`, which
  pruned the row with an `INTEGRITY WARNING`, and `resumed-c` recomputed it
  (`out/04-compute-times.txt`, `out/04-resumed-b.log`). This is one observation, read against the
  code; the wait itself was not instrumented.
- **Impact.** No stored value changes: the resume is equal to the reference. What it costs:
  - completed work is thrown away after an interruption. In a long build, a history can sit
    stored but unvalidated for as long as the slowest history in flight, and a kill in that
    window costs its recomputation;
  - near the end of each queue the driver submits no new work while it waits;
  - progress lines and the heartbeat are later than the work. For `jordan-normalization`'s
    histories (up to minutes, 3–7 integrations each), this adds to the silences P4 already
    allows for.
- **Next step.** For the user to rule. Either report it to RayPoolKit as a defect (for example,
  `num_returns=1`, or a `timeout` on the wait), and ChamPBH moves to the new pin. Or a later
  campaign that may change `main.py` (`jordan-normalization`) sets its queues'
  `process_batch_size` with this in mind. Not assigned.

### 3.1 Assigned to this campaign

None.

## 4. Resolved issues

None.

## 5. Dispatch record

### 5.0 Planning (2026-10-19)

Planned at the user's request. The user asked whether RunRegistryKit could manage "our own
long-running and persistent jobs that have archival value", and the evaluation found it feasible.
- **The package.** `v0.2.0`'s suite passed (410 tests, one skipped), and its code is unchanged since
  the tag. A dry-run install into this venv adds only `runregistrykit`.
- **SGK's adoption is the pattern.** It uses a wrapper driver; beats from the pipeline's stdout;
  installs its signal handler after `ray.init`; finishes with the fingerprint after the pool
  closes; and leaves `store create` to a person after the first close (README §0.2).
- **R1–R7** were ruled by the user on 2026-10-19 (README §6.0):
  - the store layer;
  - a wrapper, so that `main.py` stays launchable without the package;
  - naming, heartbeat and sidecar as SGK does;
  - stores in `var/`;
  - the old stores dropped, not adopted;
  - the dirty flag cleared by cleaning up the root;
  - this campaign before `jordan-normalization`.
- **Measured** at `3cd2be4`, read-only (README §0.3); suites 18 / 176 (61 + 115) / 69 / 79.
- **No issue** opened or assigned (P13).

### 5.1 Ruled, and prompts 01–05 written (2026-10-19)

The user's rulings:
- **U1–U5 accepted as proposed:**
  - U1: `utilities.installed_package_provenance` kept, so that no driver imports the package;
  - U2: `--inventory` refused by the wrapper;
  - U3: the manifest's `script` is `main.py`;
  - U4: the run scripts get a handover only;
  - U5: the hard-kill route is rehearsed.
- **P1 amended**: "SecondaryGWKit placed the scripts in `RunDrivers/` and kept `tools/` for
  repo-level tooling. Let's do the same for consistency." The wrapper is
  `RunDrivers/registered_run.py`, with a new test root, `RunDrivers/tests/`. 03 adds the root to
  `CLAUDE.md`'s commands.
- **P2–P13 accepted as proposed.**

An earlier reply had been sent with the text "U1–U5 and P1–P13 accepted as proposed". The user had
not agreed to it, and said so. No ruling was recorded from it, and these are the user's own words.

Prompts 01–05 were written the same day, from the measurement of README §0.3 and a read of
SecondaryGWKit at `031bcb4`. Its `RunDrivers/scoped_pipeline_run.py` (moved there from
`docs/gktk-remedial/` by `031bcb4`) is the model for 03, and the prompts cite it at that commit. Three
choices made while writing them, each within its charter:
- **03 refuses an address that `ray_address_problem` refuses**, as well as no database and
  `--inventory` (README §2.2). Otherwise `main.py` would refuse it after registration, and a typing
  error would be recorded as a `failed` run.
- **03's refusals exit 2**, as ChamPBH's drivers do, where SecondaryGWKit's wrapper exits 1.
- **04 launches every build through `sh`**, which reaps the driver when it is killed. RunRegistryKit's
  `[04-a-killed-process-not-yet-reaped-answers-kill-0]` would otherwise make `end` refuse the
  `SIGKILL`ed run. Its launch logs and pid files are under `var/rrk-adoption-04/`, beside the stores
  (R4).

### 5.2 After prompt 01 (2026-10-19)

01 landed in `b0f3488` (log: [`logs/01-pin-config-and-rules.md`](logs/01-pin-config-and-rules.md)).
- **C1.** `runregistrykit[stores] @ …@v0.2.0` in `requirements.txt`; the venv installed from the
  file. `pip freeze` differs by the one line `runregistrykit @ …@0bae3a3c501a8264ee4be5bc24f06422af38720f`;
  `direct_url.json` names that commit and is not editable.
- **C2.** `runregistrykit.toml` (no `[stores]`); `var/` in `.gitignore`. `list` prints `no runs
  under …/var/runs`, exit 0, and creates no `var/`.
- **C3.** `CLAUDE.md`: the section "Long-running jobs — the run registry", with seven rules
  (SecondaryGWKit's numbering).
- **C4.** `tools/tests/test_registry_config.py`: 5 tests. Suites: CosmologyModels 18, ComputeTargets
  176 (61 + 115), ObjectFactories 69, tools 84 (79 + 5). Each of the three deliberate breakages fails
  at least one new test.
- **Gate "Pin"** holds. No issue opened.

**Orchestrator review of prompt 01 (2026-10-19).** Against `b0f3488`, by note 01's ten checks. All
pass.
- **Scope.** The prompt's §5 files and nothing else. The untracked set is unchanged (22 entries),
  and there is no `var/`.
- **The pin.** `requirements.txt` gains one line, at `:85`. The orchestrator's own `pip freeze`,
  taken before dispatch, differs from the one now by the single line
  `runregistrykit @ …@0bae3a3c501a8264ee4be5bc24f06422af38720f`. `direct_url.json` names that commit
  with `requested_revision` `v0.2.0` and has no `dir_info`. The package imports from the venv's
  `site-packages`.
- **The package, by probe.** `find_client_root(client_root=<root>)` gives the root as both record
  and found root, the two roots under `var/`, and `("datastorekit", "raypoolkit")`. `list` prints
  `no runs under …/var/runs` and exits 0, and creates no `var/`. `git check-ignore -v` names
  `.gitignore:7` for both paths.
- **`CLAUDE.md`.** The bullet is gone and the section is appended, 41 lines. Every C3 item is
  there. The seven rules follow SecondaryGWKit's order, and no SecondaryGWKit path, history or
  GCS-server paragraph came across. The wrapper sentence is true at this commit.
- **The test.** Five tests, as note 01 rules them: one `check-ignore` per path, asserted to exit 0
  exactly; the keys required ⊆ keys ⊆ known; the provenance dict compared whole.
- **The breakages.** The three diffs were replayed in `bash` and failed as the log says (B1 2
  failures; B2 1 failure and the package's `ConfigError`; B3 1 failure), then reverted cleanly. A
  probe added the `[stores] fingerprinter = "config.fingerprint:fingerprinter"` table that 02 adds,
  and the module passed. So 02 needs no edit to it.
- **Suites** at `b0f3488`, serially: 18, 61 + 115, 69, 84 (load 3–17). `black --check` clean.
- **Residue fixed in this follow-up:**
  - the SHA, on the board and in the log;
  - the log's subject line, which did not match the committed subject;
  - the notes list.

  Separately, the new module lacked the licence header that every other test module carries. It is
  added in a housekeeping commit of its own.
- No issue opened.

### 5.3 After prompt 02 (2026-10-19)

02 landed in `c9eea37` (log: [`logs/02-the-fingerprinter.md`](logs/02-the-fingerprinter.md)).
- **F1.** `config/fingerprint.py`: `FINGERPRINT_FORMAT = 1`; `SPEC_DIGESTS[1]` of **21 pairs** (15
  distinct digests), not the 17 the prompt names, which counted `inventory_spec` definitions
  (`STRUCTURALLY REQUIRED`; the count equals `test_inventory_specs`'s); the fingerprinter. The
  docstring says what importing it adds (RunRegistryKit's three modules and the standard library's
  six, nothing of Ray, SQLAlchemy or DatastoreKit), checked by `sys.modules`.
- **F2.** `runregistrykit.toml` gains `[stores] fingerprinter = "config.fingerprint:fingerprinter"`.
- **F3.** The format guard, 4 tests, with a stand-in `Parent` keyed by `0` and by `"0"`.
- **F4.** The golden test, 5 tests, on a store of its own builder (one row of each of the 21 classes
  and a `ScalarModel` of each of the five potential types), and the golden file
  `store_fingerprint_rrk-adoption-02.json`, digest `146a13c3cda4e4409d98fc8b97086f518c00f5301da8b21a1950455a5dbd990c`,
  SHA-256 `eb6a7a86fba2ab8e01568c1ed6c0e26e99dc350ecec4d9a30a3b464a0f340e4e`. `TestCommandLine` runs
  `store fingerprint` on that store: exit 0, the golden digest, `recorded: none recorded (the
  sidecar is absent)`.
- **Suites:** CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories 78 (69 + 9), tools 84.
  Each of the five deliberate breakages fails the named test.
- **Gate "Fingerprint"** holds. No issue opened.

**Orchestrator review of prompt 02 (2026-10-19).** Against `c9eea37`, by note 02's ten checks. All
pass.
- **Scope.** The prompt's §5 files and nothing else. The untracked set is unchanged (22 entries),
  there is no `var/`, and `main.py` is unchanged since `3cd2be4`.
- **The config.** `runregistrykit.toml` is 01's file with the `[stores]` table appended, one key.
- **The pin, by probe.** The recipe, written afresh in the scratchpad, gives 21 pairs equal to
  `SPEC_DIGESTS[1]`, with 15 distinct digests, and `SPEC_DIGESTS` has the one key `1`. Importing
  `config.fingerprint` after `config.datastore` adds 10 modules: note 02's nine and the module
  itself. None is of Ray, SQLAlchemy or DatastoreKit, as the docstring says.
- **The tests, by reading.**
  - The guard is the recipe, and it fails naming the class, with SecondaryGWKit's message.
  - The key test uses two stand-in `Parent`s and asserts the `TypeError`.
  - The golden builder is the module's own, on `_TempStoreCase`, and imports nothing from
    `tools.tests`. It writes every row note 02's ruling 3 asks for.
  - `TestGolden` builds the store twice.
  - `TestCommandLine` checks the exit, the digest, the format, the last line, and that nothing is
    written.
- **The agent's two choices beyond the note.** Both are sound and are accepted.
  - `TestCommandLine` asserts that `var/` is unchanged by the command, not that `var/` is absent. An
    absence test would fail once the user makes a first registered run.
  - The pin-count and every-class tests.
- **The golden file.** Its SHA-256 is `eb6a7a86…0e4e`, as logged, and its digest is
  `146a13c3…990c`. It re-serialises byte for byte as `json.dumps(…, indent=2, sort_keys=True) +
  "\n"`. `taken` is `null` and `problems` is `{}`. All 21 classes count at least 1, with
  `ScalarModel` at 5.
- **The breakages.** The five diffs were replayed in `bash` from the clean tree, and each failed
  exactly as the log says:
  - B1: 3 failures, 3 errors, `spec changed: tolerance`;
  - B2: 4 failures, with the "no pin" message;
  - B3: 6 failures, `spec changed: ScalarModel`, the golden store's `ScalarModel` unresolved;
  - B4: 2 failures;
  - B5: 1 failure and 1 error, the package's `ConfigError`, with `test_registry_config` passing.

  Each was reverted, and the tree is clean.
- **Suites** at `c9eea37`, serially: 18, 61 + 115, 78 (69 + 9), 84 (load 4–10). `black --check` is
  clean on the three new files.
- **Residue fixed in this follow-up:**
  - the SHA, on the board and in the log;
  - the status line, which said "02 awaiting review";
  - the notes list.

  Separately, the three new Python files have a licence header cut short: its last line, `#
  limitations under the License.`, is missing. The golden module also imports `ray` and `tempfile`
  and uses neither. Both are fixed in a housekeeping commit of their own, as with 01.
- No issue opened.

### 5.4 After prompt 03 (2026-10-19)

03 landed in `d5dd930` (log: [`logs/03-the-wrapper.md`](logs/03-the-wrapper.md)), under the
orchestrator's note for 03, whose eleven rulings governed.
- **W1.** The package `RunDrivers/` and its test root `RunDrivers/tests/`;
  `RunDrivers/registered_run.py`, which puts the root on `sys.path` and runs without `PYTHONPATH`.
- **W2.** The four flags required and non-empty; `argv` split at its first `--`; both parsers'
  `error` replaced on the instance, so that every refusal before `begin` is one `!! ` line, exit 2:
  a missing or empty flag, no `--`, anything `main.py`'s parser refuses (a missing `--database`
  among them, ruling 1), an empty database, `--inventory`, and an address `ray_address_problem`
  refuses (its own `!! ` removed).
- **W3.** `begin` with `script` = the program (`main.py` in production, U3), `results` the absolute
  database, `scope` from the parsed arguments as given, `heartbeat_means` (the 30 s throttle, the
  300 s floor, the workers' lines, the 900 s window), `argv` = the wrapper's path and arguments,
  `client_root` the repository; a `ConfigError` re-raised, any other `RuntimeError` a `Refusal`.
  The pre-Ray handler and the two proxies, each with a re-entrant lock around its bookkeeping.
- **W4.** `ray.init` wrapped: the real call with `main.py`'s arguments unchanged, then the handler.
- **W5.** `main.py` by `runpy.run_path`; SecondaryGWKit's `finish_after_exec`, the fingerprint only
  after the program; `run`, the script's entry, prints `** driver exit status N` last, on a line of
  its own (a guard added after a breakage replay found a signal can leave a line unfinished), and
  exits with `main.py`'s status.
- **W6.** 61 tests in six modules and the helper `stubbed_run.py`; none starts Ray. `CLAUDE.md`: the
  new root's command and count, and the `RunDrivers/` sentence.
- **Suites:** CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories 78, tools 84, and
  RunDrivers 61 (three runs in a row, all OK). Each of the six deliberate breakages fails a named
  test. `git diff 3cd2be4 -- main.py plot_by_beta.py plot_ScalarModel.py utilities.py` is empty.
- **§2.3, for 04:** the pilot's stages are each under 300 s, so it prints no progress line; its
  longest healthy silence is the scalar-history stage, about 2¼ minutes.
- **Gate "Wrapper"** holds. No issue opened.

**Orchestrator review of prompt 03 (2026-10-19).** Against `d5dd930`, by note 03's twelve checks.
All pass.
- **Scope.** §0's files and nothing else. The untracked set is unchanged (22 entries), there is no
  `var/`, and `git diff 3cd2be4 d5dd930 -- main.py plot_by_beta.py plot_ScalarModel.py
  utilities.py` is empty. `CLAUDE.md`'s diff is the three edits of note 03 §0, with the count 61.
- **The module, by reading.** `finish_after_exec` and the other functions carried over from
  SecondaryGWKit at `031bcb4` differ from it only in wording. `parse_arguments`, `describe_scope`,
  `run`, `stop_beating`, `mid_line` and the lock are each ruled in the note or named in the log.
  Nothing of the substitution, the model filter or the local Ray came across. `begin`'s arguments
  are those of rulings 4 and 6. The pilot's `scope` takes `Λ 0.001 eV` and the potential type from
  `exponential.yaml`, and its `--M-values-Mp 1e-3` overrides the file's 0.5. So the literal is
  right.
- **By probe**, through `stubbed_run`, on a stand-in of the orchestrator's own. The stand-in printed
  a banner, 50 progress lines and a `(pid=…)` worker line, then `sys.exit(3)`. Results:
  - exit 3 and `failed`/3;
  - the marker last in stdout and in `stdout.log`;
  - the stage is the last progress line's sentence, neither the worker line nor the marker;
  - `script` and `results` absolute, `argv[0]` the wrapper, `git_head` `unknown`, as ruling 4
    says.

  The real script on `-- --database x --inventory --ray-address 127.0.0.1:6380` exits 2 with one
  `!! ` line and the marker.
- **The tests, by reading.** Every in-process test goes through `stubbed_run.in_process`, which
  registers a restoring cleanup before the call and observes before it restores. Each of the two
  `Popen`s is reaped by a cleanup registered before its first read. Every case's `tearDown`
  asserts `ray.is_initialized()` false: `_GoldenStoreCase`'s for the fingerprint module. Test 7 is
  ruling 9's `ast` form, and test 8 expects (4, 2).
- **The breakages.** The six diffs, extracted from the log, apply cleanly at `d5dd930`. Each was
  replayed in `bash` and failed exactly as the log says: B1 3, B2 10, B3 4, B4 2, B5 2 and B6 2
  failures. Each was reverted, and the tree is clean.
- **Suites** at `d5dd930`, serially: 18, 61 + 115, 78, 84, and RunDrivers 61 twice (load 3–6).
  `black --check` is clean on the ten files. `--help` exits 0. `list` prints no runs. There is no
  `var/`. After the suites `pgrep` finds no Ray process and `ray_cluster.sh status` exits 0.
- **The agent's choices beyond the note**, accepted:
  - the marker always starts a line of its own (`mid_line`), after a replay showed a signal can
    leave `print`'s newline unwritten;
  - `KeyboardInterrupt` before registration gives 130;
  - `NOTIFY_FLOOR` is pinned to `main.py`'s value by `ast`;
  - the two-thread test.
- **Found by the review:** `HEARTBEAT_MEANS` says that a beat comes "at most once per 30 s". The
  proxy beats **at once** on any line that changes the stage (`changed or …`, SecondaryGWKit's
  design, which the prompt's test 2 pins for a banner). The probe's burst of 50 progress lines
  moved the stage on each. In production a stage changes only at a banner or a progress line,
  300 s or more apart, so nothing breaks. But the manifest's sentence should say so. It is fixed
  in a housekeeping commit of its own, which adds the clause and keeps the substring that
  `test_registered_run` asserts.
- **Residue fixed in this follow-up:**
  - the SHA, on the board and in the log;
  - the status line;
  - the notes list.
- No issue opened.

### 5.5 After prompt 04 (2026-10-20)

04 landed in `d5d8c74` (log: [`logs/04-rehearse-registered-runs.md`](logs/04-rehearse-registered-runs.md)),
under the orchestrator's note for 04, whose twelve rulings governed. The rehearsal ran on
2026-10-19, 23:34–00:07 BST, on ChamPBH's isolated cluster (`ray_cluster.sh start "rrk-adoption 04
(R)"`, exit 0, 5 s; `stop`, exit 0, 19 s), at `07aa626`. The record is
`.documents/rrk-adoption-verification.md` §1, with the outputs under `.documents/rrk-adoption/out/04-*`.
- **H1.** `reference`, `rrk-adoption-04-reference-20261019T234113`: `done`/0 in 5 min 40 s, 5 / 5 /
  5 / 5 in each stage. Its status has the fingerprint `2dd4b39b…932f` and "not written … (there is
  none)"; its manifest `script` `main.py` with `main.py`'s SHA-256 and `results_store_id` `null`.
  Then `store create`, `store fingerprint --write` (the same digest) and `store show`.
- **H2.** `resumed-a`, `rrk-adoption-04-resumed-a-20261019T234840`: `SIGTERM`ed 150 s after the
  scalar banner, `killed`/143, exit 143, with its fingerprint. Signal → `killed` record 0 s (±1 s);
  → the final record 9 s. No `** cancelled an alarm` line. The probe (ruling 8): `ray.init` attached
  to the external cluster installs Ray's `sigterm_handler` over `SIG_DFL`, and the wrapper's replaced
  it (P5). Then `store create` on the resumed store.
- **H3.** `resumed-b`, `rrk-adoption-04-resumed-b-20261019T235447`: `SIGKILL`ed 20 s after the
  banner; no driver marker; `list` `!!`, `stale`; no Ray worker, actor or process left; the next
  `resumed-c` refused, exit 2, one `!! ` line naming `end`, nothing created; `end` accepted,
  verified; `lost`.
- **H4.** `resumed-c`, `rrk-adoption-04-resumed-c-20261019T235647`: `done`/0 in 3 min 5 s,
  computing 2 of the 5 histories; `fingerprint_sidecar` `written`; status, sidecar and `store
  fingerprint` one digest. `resumed-b`'s and `resumed-c`'s `results_store_id` the resumed store's.
- **H5.** `compare_stores`: `RESULT: equal`, bookkeeping columns aside. Both stores' fingerprints
  `2dd4b39b61b9cc1b39bfcf5068ca2d00e84b52fee9da61958a11781931b0932f`. `histories.csv`
  `d7c62c37…c522`, R0's.
- **H6.** The verification document §1, created.
- **Suites**, before and after: CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories
  78, tools 84, RunDrivers 61. `pip freeze` unchanged. `main.py` unchanged since `3cd2be4`.
- **Deviations** (log): two `STRUCTURALLY REQUIRED` (the kill in the scalar stage, ruling 1; the
  heartbeat's age read from `status.json`, since `list`'s `AGE` is the run's age), two `UNINTENDED
  DRIFT` (one birth-time line re-read with `/usr/bin/stat`; `stop`'s exit status not echoed), the
  rest the note's rulings as `IMPLEMENTATION CHOICE`.
- **Observed, not opened:** in `resumed-a`, β = 10's stored row was still unvalidated 65 s after it
  was stored, so the resume pruned and recomputed it; the log offers a likely mechanism (the queues'
  `ray.wait` for `min(3, in flight)`), not measured. Left for the orchestrator.
- **Gate "R"** holds. No issue opened.

**Orchestrator review of prompt 04 (2026-10-20).** Against `d5d8c74`, by note 04's twelve checks.
All pass.
- **Scope.** 33 files: the verification document, 30 files under `.documents/rrk-adoption/out/04-*`
  (208 KB, the largest 36.5 KB), the log and the board. No code, and nothing under `var/` is
  committed. The untracked set is unchanged (22 entries). `git diff 3cd2be4 d5d8c74 -- main.py
  plot_by_beta.py plot_ScalarModel.py utilities.py` is empty. `pip freeze` equals the
  orchestrator's own, taken before dispatch.
- **The cluster.** `ray_cluster.sh status` exits 0: the lock free, no ChamPBH process, the ports
  free. No `ray::` process is on the machine.
- **The runs, from the registry.** `list` shows the four runs, all finished: `reference` `done`,
  `resumed-a` `killed`, `resumed-b` `lost`, `resumed-c` `done`. Each `manifest.json` and
  `status.json` under `var/runs/` is byte-identical to its committed copy. They agree with the
  document on these fields:
  - the state and exit code;
  - `script` `main.py` with `main.py`'s SHA-256 (`243a5026…7d59`);
  - `results`, and `results_store_id` as ruling 11 says;
  - `git_head` `07aa626` and `git_dirty` `true`;
  - the pilot's `scope`, and `heartbeat_means` with 03's housekeeping clause;
  - `resumed-b`'s `ended` record, `verified: true`, with the reason.

  `end` rewrote `resumed-b`'s `heartbeat` to its own time (23:56:39). The kill-time beat, 23:55:16,
  survives in `ended.evidence.heartbeat`, which is the one the document's §1.4 quotes.
- **The order and the signals.** In every launch log the `** registered_run:` lines come before
  `>> packages:`. `resumed-a` has the handler's line at 23:52:58 +0100, the same second as
  `interrupt_when`'s `sent SIGTERM` (22:52:58 UTC). Then come `SystemExit(1)`, the marker 143 and
  the launcher's 143, with no alarm line. `resumed-b` ends with the launcher's 137 and no marker.
  The refused launch is the `!! ` line naming `resumed-b` and `end`, then the marker 2.
- **The delays.** A fresh read-only query of the reference shards gives the five `compute_time`s as
  logged: 7.28, 7.33, 8.12, 50.91 and 195.74 s. Both delays follow ruling 6 from them: 150 s in the
  β = 10 / β = 25 gap, and 20 s ≤ 25.5 s.
- **The stores, re-measured.** `compare_stores`: `RESULT: equal`, exit 0. `store fingerprint` on
  each store gives `2dd4b39b…932f`, and each "matches" its sidecar's: the reference's by a person at
  23:47:55, the resumed store's by `resumed-c` at 23:59:44. `store show` names `reference` on the
  first store, and `resumed-c`, `resumed-b` and `resumed-a` on the second. `stage_counts.py` on the
  four logs equals the committed output. `histories.csv` is
  `d7c62c37…c522`, R0's.
- **The records.** The log is in §5.1's form, with fifteen deviations, each classified. The
  document's figures match the outputs and the registry's records. It claims no history of beats.
- **Suites** at `d5d8c74`, serially, with `var/` now present: 18, 61 + 115, 78, 84, 61 (load 5–8).
  `var/runs/`' listing is the same before and after.
- **Opened by this review:** `[04-the-pool-waits-for-every-in-flight-item-when-fewer-than-three]`
  (§3). The agent's observation of β = 10's unvalidated row is explained by RayPoolKit's wait,
  read at `v0.2.0`. Its record says "β = 10's validation" was in flight; by the code it was
  β = 10's store task, because a validation is submitted only after the store task's result is
  processed. The issue is the user's to rule, and not this campaign's to fix.
- **Residue fixed in this follow-up:**
  - the SHA, on the board and in the log;
  - the status line;
  - the notes list;
  - the index.

### 5.6 After prompt 05 (2026-10-20)

05 landed in `c94c8d5` (log: [`logs/05-close.md`](logs/05-close.md)), under the orchestrator's
note for 05, whose fourteen rulings governed. It added no code and no test.
- **Z1.** `.documents/architecture-summary.md`: the tree gains `runregistrykit.toml`, `RunDrivers/`,
  `config/fingerprint.py` and `var/`; a dated note at the end of §2; the last sentence of the §4.1
  paragraph made true of the tree.
- **Z2.** `CLAUDE.md`: the wrapper named with its usage line; the counts 78 and 84, with one date.
- **Z3.** `.documents/review-remediation-verification.md` §4.15, the handover for the run scripts,
  by line of each script as it is on disk today.
- **Z4.** `.documents/rrk-adoption-verification.md` §2: RunRegistryKit's `extraction` G3 and
  `interrupted-runs` G2, drafted; nothing written in that repository.
- **Z5.** This board closed; `prompts/INDEX.md`; `.documents/OPEN_ISSUES.md` (28 open; §1.14 states
  that the campaign closed).
- **Suites**, before and after: 18, 61 + 115, 78, 84, 61.
- No issue opened or closed. The one open issue is §3's.

**Left to the user:**
- **The merge** of `rrk-adoption` into `main` (P10). It is not merged.
- **The two gate records** in RunRegistryKit (Z4): the drafts are in the verification document §2.
  Bring each draft's "not merged into `main` when this was drafted" clause up to date when
  recording, and add the recording session's own "Read here" clause.
- **The run scripts** (Z3): `full_run_2026.6.0.sh` and `full_run_2026.6.0-refresh.sh`, edited by the
  user, not by the campaign. §4.15 says what changes, by line.
- **The files at the root**, the old stores and 04's stores and runs: the 22 untracked entries, so
  that `git_dirty` means something (R5, R6; §4.15 points 8 and 9); `~/ChamPBH-stores*` and
  `~/ScienceData/ChamPBH-rehearsals/` when the user wishes; and `var/datastores/`, `var/runs/` and
  `var/rrk-adoption-04/`, kept until the user removes them. **Never `git clean -x` or `-X`.**
- **Two stale docstrings**, not edited (existing tests are not modified, README §5, and
  `utilities.py` is byte-identical through the campaign), and not opened as issues (housekeeping,
  the user's call):
  - `ObjectFactories/tests/test_inventory_specs.py:27-29`, in the bullet at `:25-29`, says "The
    fingerprint of SecondaryGWKit's RunRegistryKit, which ChamPBH is expected to adopt, reads only
    what the specs declare …". It would read "The fingerprint of RunRegistryKit, which ChamPBH
    adopted in `rrk-adoption` (`config/fingerprint.py`), reads only what the specs declare …".
  - `utilities.py:190-192`, in `installed_package_provenance`'s docstring, says "SecondaryGWKit's
    `RunRegistry.package_provenance()`, which ChamPBH may one day import in place of this helper
    (README P5)". SecondaryGWKit no longer has `RunRegistry/`, and the function is
    `runregistrykit.package_provenance`. It would read "`runregistrykit.package_provenance`, which
    ChamPBH did not import in place of this helper, so that `main.py` and the plot scripts stay
    launchable without the registry (`rrk-adoption` U1)".
- **`jordan-normalization`** comes next (R7). Its rebuild is the first production run through the
  wrapper.

**Orchestrator review of prompt 05 (2026-10-20).** Against `c94c8d5`, by note 05's ten checks. All
pass.
- **Scope.** The eight files of note 05 §0 and nothing else. The untracked set is unchanged (22
  entries). `git diff 3cd2be4 c94c8d5 -- main.py plot_by_beta.py plot_ScalarModel.py utilities.py`
  is empty. `var/runs/`' listing, `list` (4 runs, 4 finished) and `pip freeze` equal the
  orchestrator's own, taken before dispatch. The run scripts' SHA-256s are note 05's. RunRegistryKit
  is at `33a2cd2` with no change.
- **Additive.** Re-extracted independently: `architecture-summary.md`'s `> **Note, …**` blocks go
  from 15 to 16, and `diff` deletes no line. `review-remediation-verification.md` gains §4.15 as one
  insertion after `:1659`, before the `---` above §5. The verification document at `e0a52da` is a
  byte prefix of the new one. The only deletions under `.documents/` are the ones rulings 9, 10 and
  13 allow: `config/`'s heading and five children (new prefixes), the two lines of §4.1's last
  sentence, and the header sentence and §1.14 intro of `OPEN_ISSUES.md`.
- **Z1 and Z2, by reading.** The tree's additions and the note are true of the tree: run
  directories hold `manifest.json`, `status.json`, `stdout.log` and `stderr.log`. The §4.1 sentence
  rests on a `git grep` re-run here. `CLAUDE.md`'s diff is ruling 11's three edits, and the wrapper's
  `--help` exits 0.
- **Z3, against the scripts.** Every cited line of both scripts was checked against `cat -n`, along
  with the `diff` of the two. The code citations hold: `slugify` (`__init__.py:129-132`), `git_dirty`
  from `git status --porcelain` (`:162`), `sidecar_path` (`stores.py:252-257`), `_primary_problems`
  (`:328-337`), the refusals of `create_sidecar` and the four `_running_runs_naming` callers, and the
  line-buffered copy (`registered_run.py:230`). 04's launch form matches the verification document
  §1.12. Where §4.15 reasons rather than measures, it says so: the script's `bash` reaping its
  driver, and a finished block re-registered as a short `done` run.
- **Z4, against RunRegistryKit and the records.** Every commit named exists in its repository
  (`b0f3488`, `c9eea37`, `d5dd930`, `d5d8c74`, `3cd2be4`; `aaab408`, `0bae3a3`). The venv's
  `direct_url.json` names `0bae3a3c…` with `requested_revision` `v0.2.0` and no `dir_info`. The
  refusal's quoted words are in `out/04-refusal.txt` and `04-resumed-c-refused.log`, and `end`'s in
  `out/04-checks.txt:110`. `TestBeginsRefusal` does `SIGKILL` a real child and asserts both quoted
  fragments. The §3 `git grep` finds nothing; `foreign` occurs only in the BBN-provenance tests. The
  item 9 points and the six §3 checks are each answered once, and neither draft claims a commit of
  the pin alone.
- **Z5, by counting.** `OPEN_ISSUES.md` has 28 `- \`[` rows, and its header says 28. `INDEX.md` says
  10 campaigns, 0 planned, 0 live, 10 closed, and its row agrees with this board.
- **The log** follows `dk-and-rpk-migration` README §5.1's sections. Its deviations are classified,
  and its commands are verbatim.
- **Suites** at `c94c8d5`, serially: 18, 61 + 115, 78, 84, 61, all OK (load 5–8).
- **Accepted, beyond the note:**
  - **Ruling 10 was wrong in one clause.** `ObjectFactories/tests/test_store_fingerprint_golden.py:64`
    imports `ENCODING_FORMAT` from `datastorekit.store_fingerprint`. The agent wrote the §4.1
    sentence true ("ChamPBH's own code imports the module nowhere; one test … imports its
    `ENCODING_FORMAT`") and recorded the deviation.
  - **Ruling 4's preference** is the guarded line. `create_sidecar` was found not to look for a run
    naming the store.
  - **The paragraph on `-u`** is read from the code and says so.
  - **The agent amended its own commit once** before handing back, to tighten a command in the G2
    draft. The commit was unreviewed and is the prompt's one commit, so the rollback boundary
    holds.
- **Residue fixed in this follow-up:**
  - the SHA, in the board, the log, `INDEX.md` and both drafts of the verification document §2;
  - the status line, which said "05 awaiting review";
  - the notes list.
- No issue opened or closed. `[04-the-pool-waits-for-every-in-flight-item-when-fewer-than-three]`
  stays open, for the user to rule.
