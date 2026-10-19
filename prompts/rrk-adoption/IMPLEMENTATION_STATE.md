# rrk-adoption campaign — implementation state

**Last updated:** 2026-10-19 · **Status: LIVE — 5 of 5 prompts written, 2 landed (01 and 02, both reviewed). R1–R7, U1–U5
and P2–P13 ruled by the user on 2026-10-19, as proposed; P1 amended (the wrapper in `RunDrivers/`)
(README §6).**
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
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) (no section: none opened)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [The pin, the config and the rules](01-pin-config-and-rules.md) | `runregistrykit[stores]@v0.2.0`; `runregistrykit.toml`; `var/` ignored; `CLAUDE.md`'s rules | Sonnet | ✍️ 2026-10-19 | ✅ 2026-10-19 | `b0f3488` | [log](logs/01-pin-config-and-rules.md) |
| 02 | [The fingerprinter](02-the-fingerprinter.md) | `config/fingerprint.py` at format 1; `[stores]`; the format guard; a golden fingerprint | Sonnet | ✍️ 2026-10-19 | ✅ 2026-10-19 | `c9eea37` | [log](logs/02-the-fingerprinter.md) |
| 03 | [The wrapper](03-the-wrapper.md) | `RunDrivers/registered_run.py`: register, beat from stdout, signals after `ray.init`, `main.py` by `runpy`, finish with the fingerprint | Opus | ✍️ 2026-10-19 | ⬜ | — | — |
| 04 | [Rehearse registered runs](04-rehearse-registered-runs.md) | a registered build; `SIGTERM` and resume; `SIGKILL`, `end` and resume; `store create`; stores compared; `histories.csv` = R0's | Opus | ✍️ 2026-10-19 | ⬜ | — | — |
| 05 | [Close](05-close.md) | documents, handover, RunRegistryKit's gate drafts, indexes, board | Sonnet | ✍️ 2026-10-19 | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).** 01–05 are written once the user has ruled on README §6.1 and §6.2.
*Written 2026-10-19 (§5.1).*

Orchestrator notes are written one prompt at a time, in [`orchestrator/`](orchestrator/), just
before each dispatch:
- [`orchestrator/prompt-01.md`](orchestrator/prompt-01.md) (`6dd54a0`), used for 01.
- [`orchestrator/prompt-02.md`](orchestrator/prompt-02.md) (`6824826`), used for 02.

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
| **W1** | 03 | the package `RunDrivers/` and its test root; `RunDrivers/registered_run.py` (P1 as amended) | ⬜ |
| **W2** | 03 | the wrapper's flags and refusals before `begin()`, exit 2 (P2, P3, U2) | ⬜ |
| **W3** | 03 | registration: `begin` with `script` = the program (U3), `scope`, `heartbeat_means`; the stream proxies | ⬜ |
| **W4** | 03 | `ray.init` wrapped; the handler installed after it (P5) | ⬜ |
| **W5** | 03 | `main.py` by `runpy.run_path` (P7); `finish_after_exec` with the fingerprint (P6); `** driver exit status N` | ⬜ |
| **W6** | 03 | `RunDrivers/tests/test_registered_run*.py`, with no Ray; `CLAUDE.md`'s test command and `RunDrivers/` sentence | ⬜ |
| **H1** | 04 | `reference`, `done`; `store create` and the fingerprint | ⬜ |
| **H2** | 04 | `resumed-a`, `SIGTERM`ed, `killed`/143, with the signal's timing (P5) | ⬜ |
| **H3** | 04 | `resumed-b`, `SIGKILL`ed: `stale`, the resume refused, `end`, `lost` (U5) | ⬜ |
| **H4** | 04 | `resumed-c`, `done`, its fingerprint written to the sidecar | ⬜ |
| **H5** | 04 | the stores compared; `histories.csv` equal to R0's | ⬜ |
| **H6** | 04 | `.documents/rrk-adoption-verification.md` §1 | ⬜ |
| **Z1** | 05 | `architecture-summary.md` | ⬜ |
| **Z2** | 05 | `CLAUDE.md` | ⬜ |
| **Z3** | 05 | the handover, `.documents/review-remediation-verification.md` §4.15 | ⬜ |
| **Z4** | 05 | RunRegistryKit's G3 (`extraction`) and G2 (`interrupted-runs`) drafted, in the verification document §2 | ⬜ |
| **Z5** | 05 | the board closed; `prompts/INDEX.md`; `.documents/OPEN_ISSUES.md` | ⬜ |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Pin | the venv holds `runregistrykit` 0.2.0 from the tag, not editable; `list` runs on the empty runs root; `var/` is ignored | 01 | ✅ 2026-10-19 |
| Fingerprint | the config's fingerprinter loads; the format guard and the golden fingerprint pass, and each deliberate breakage fails | 02 | ✅ 2026-10-19 |
| Wrapper | with no Ray, a run through the wrapper is registered before its first line, beats, ends `done`/`failed`/`killed` with `main.py`'s exit code, takes its fingerprint after the program returns, and refuses a store an unfinished run names; `main.py` is unchanged | 03 | ⬜ |
| R | on the isolated cluster, the four builds are recorded as README §2.2 (04) says, the resumed store equals the reference, and `histories.csv` has R0's SHA-256 | 04 | ⬜ |
| RunRegistryKit G3 / G2 | recorded in RunRegistryKit, on its `extraction` and `interrupted-runs` boards, by the user from 05's drafts (P12) | 05 | ⬜ |

## 3. Active and unresolved issues

None opened by this campaign.

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
