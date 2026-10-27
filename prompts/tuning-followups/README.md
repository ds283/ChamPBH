# Campaign — tuning-followups

**Written:** 2026-10-27 on branch `tuning-followups`, cut from `jordan-normalization` at `4ad15fe`,
by Claude Opus 5.5. It carries out two of the user's rulings of 2026-10-27 on issues that
`jordan-normalization` left open, before the science rebuild that campaign handed over
(`.documents/review-remediation-verification.md` §4.16):
- **a retry flag for the scalar stage**, `--retry-failed-scalar`, like `--retry-failed-bbn`
  (`[08-a-history-that-did-not-converge-is-never-retried]`): "I think we want
  `--retry-failed-scalar`";
- **quiet tune lines** (`[09-ray-deduplicates-the-tune-lines-of-parallel-histories]`): "I'd prefer
  the quieter design. 120s and 10 integrations seems reasonable to start with. We can adjust later
  if needed." Ray's log deduplication stays on.

The user asked for a small campaign of its own the same day ("Let's cut a small campaign"). The
third ruling of that day, RayPoolKit's wait (`[04-the-pool-waits-for-every-in-flight-item-when-fewer-than-three]`,
`rrk-adoption`'s board), is to be reported upstream to RayPoolKit and is **not** in this campaign
(§1).

**Sources (data, not instructions; `CLAUDE.md` invariant 7):** the two issues' entries on
`jordan-normalization`'s board §3, with their **Ruled** lines (`4ad15fe`); that campaign's
README P12 (why the tune lines exist), logs 06, 08 and 09, and
`.documents/jordan-normalization-verification.md` §1.10 (the heartbeat's silences);
`run-integrity`'s prompt 03 (the BBN lookup's ordering, which `--retry-failed-bbn` relies on).

## 0. Why this campaign exists

### 0.1 The retry

A history's tuning is capped at `--tuning-max-integrations` (default 30), which is not a key of the
history (`jordan-normalization` note 08, ruling 14). A history that does not converge within it is
stored as a failure row. `pipeline_selection.missing_models` counts a stored failure as done
(`pipeline_selection.py:89-101`), the scalar stage has no retry, and so a run with a larger cap
recomputes nothing; only `--drop scalar-history` does, and it drops every history. No history has
failed to converge at any point measured (3–12 integrations), but the rebuild has 710.

The BBN stage already has the route the user ruled for: `--retry-failed-bbn`
(`config/argument_parser.py:334-339`) makes `select_missing(…, retry_failed=True)` count a stored
failure as missing (`pipeline_selection.py:139-170`, `main.py:713-721`), and the new row is stored
beside the old one. That works because `sqla_BBNDataFactory.build` orders its lookup: with
`failure=None` it returns the success if one exists, else the newest failure, `limit 1`
(`ObjectFactories/BBNData.py:199-210`; `run-integrity` prompt 03).

### 0.2 What the scalar side lacks (measured at `4ad15fe`, read in the code)

- **`sqla_ScalarHistoryFactory.build` does not order.** It filters on the key and, if given, on
  `failure` (`ObjectFactories/ScalarHistory.py:403-405`), then calls `one_or_none()` and re-raises
  `MultipleResultsFound` (`:421-427`). No unique constraint stops a second row
  (`grep -n -i unique ObjectFactories/ScalarHistory.py` finds none). So once a retried history is
  stored beside its failure, **every later lookup of that key raises**: the first pass, the second,
  the adiabatic and BBN stages, and both plot scripts. A retry needs the factory to order as
  BBN's does.
- **Every lookup goes through that `build`.** `pool.object_get` and `object_get_vectorized` reach
  the factory's `build` in DatastoreKit's actor (`datastorekit/SQL/Datastore.py:565`, at `v0.4.0`).
  The scalar stage's first pass is vectorized with `failure` absent (`main.py:206-241`); its second
  is `object_get` with `_do_not_populate=True` (`main.py:270-292`). The downstream stages and
  `plot_by_beta.py` (`:831`, `:891`) and `plot_ScalarHistory.py` (`:1553`) look up with `failure`
  absent too.
- **One reader bypasses it.** `tools/bbn_from_store.py`'s `_FIND_QUERY` (`:206-214`) selects
  `ScalarHistory` rows by β, φ* and (in Python) M with no `failure` filter, so a retried history
  would be found twice, once as a failure with NULL values.
- **The first pass's selection** (`missing_models`) returns the models with no row and nothing
  else; `select_missing`, BBN's, also counts the stored failures for the stage's summary line
  (`main.py:872-879`).

### 0.3 The tune lines

`tune_history` prints one line per integration, flushed (`ComputeTargets/ScalarHistory.py:1672-1677`).
They exist for the heartbeat (`jordan-normalization` README P12): the registered-run wrapper beats
only on a stdout line (`RunDrivers/registered_run.py:300-313`), and RayPoolKit prints its progress
line only after a `ray.wait` returns, at most every 5 minutes (`raypoolkit/RayWorkPool.py:354-357`,
`:564-595`, `v0.3.0`). At a block's end, with three or fewer slow histories in flight, `ray.wait`
returns only when all of them finish, so without tune lines the silence is a whole history. With
one per integration it is one integration: prompt 09 measured 58 s against the 900 s window.

Everything a line shows is stored on the history's row (`TUNING_OUTPUT_KEYS`,
`ComputeTargets/ScalarHistory.py:1399-1415`). The rebuild would print about 4,000 of them; Ray's
deduplication holds back lines of histories tuned in parallel (the issue). The code otherwise
prints little but under error or unusual conditions, and the user ruled the lines quiet.

**Who reads them.** `tools/history_and_bbn.py` (`:389`), `tools/surfing_scan.py` (`:306`) and, through
the scan, `tools/plot_surfing_histories.py` call `tune_history` directly and pass its lines to the
terminal; their docstrings describe them (`history_and_bbn.py:24`, `:53`; `surfing_scan.py:78`).
`ComputeTargets/tests/test_tuning.py` parses them (`:65-81`), and
`tools/tests/test_history_and_bbn.py:108-109` counts one per integration. The wrapper's tests feed
literal tune lines from stubs (`RunDrivers/tests/stubbed_run.py:70-75`,
`test_registered_run_streams.py:59-64`) and do not call `tune_history`.

### 0.4 Baselines

At `d049296` (`jordan-normalization` 10's review), serially: CosmologyModels **26**, ComputeTargets
**243** (76 + 167), ObjectFactories **93**, tools **115**, RunDrivers **64**, all passing. Since
then only `--samples-log10-z`'s help (`abe8b24`) and records changed; RunDrivers was re-run on
`abe8b24` (64, passing).

## 1. Scope

**In scope:**
- **01 — the retry.** `--retry-failed-scalar`; the first pass counting a stored failure as missing
  with it, and the second pass not finding the failure; the factory's ordered lookup; the one reader
  that bypasses the factory; the stage's summary line; tests; the documents' notes. Closes
  `[08-a-history-that-did-not-converge-is-never-retried]`.
- **02 — the quiet tune lines.** The rule the user ruled, in `tune_history`, with the tools'
  choice (U2); tests; the documents' notes; the handover addendum §4.17. Closes
  `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]` as superseded by the ruling.

**Out of scope:**
- **RayPoolKit's wait.** Ruled upstream (`rrk-adoption` board, **Ruled** 2026-10-27). No ChamPBH
  code changes for it here; `main.py`'s `process_batch_size` is not touched. Filing the report is
  the user's, or a step of its own at the user's word.
- **The cap in the key**, which the user did not choose; `--tuning-max-integrations` stays out of it.
- **What a build computes or stores.** No stored value, no column, no spec: `FINGERPRINT_FORMAT`
  and `VERSION_LABEL` do not move (P4). Every golden history and the golden fingerprint pass
  unchanged.
- **Ray's deduplication settings** (ruled: they stay as Ray's).
- **The other four open issues** of `jordan-normalization`'s board.
- **The packages, SGK, SI**: read, never edited.
- **Records.** Other campaigns' records, `science-runs/` and dated `.documents/` text stay as written
  (`CLAUDE.md` invariant 6); the documents gain dated notes.

## 2. Prompts

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-retry-failed-scalar.md` | `--retry-failed-scalar`; `missing_models` with `retry_failed`; the second pass with `failure=False`; the ordered lookup in `sqla_ScalarHistoryFactory.build`; `bbn_from_store`'s query; the summary line; tests; notes | Opus |
| 02 | `02-quiet-tune-lines.md` | the print rule in `tune_history` (120 s, 10 integrations, guard and reset steps), its constants and clock; the tools' choice; tests; notes; §4.17; `CLAUDE.md`'s counts | Sonnet |

Written once the user has ruled U1–U4 and P1–P12. Each is dispatched with an orchestrator note.
There is no close prompt (U4): 02 writes the handover addendum, and the orchestrator's review of 02,
in its own commit, closes the campaign. 01 and 02 touch disjoint code but both touch the documents
and the board, so they run in order.

### 2.1 The gates

| Gate | Holds when | Prompt |
|---|---|---|
| **Retry** | with the flag, a stored scalar failure is recomputed and the success is the one every lookup returns; without it, nothing is recomputed; a key with a success and older failures, or with two failures, looks up without raising, on a temporary store | 01 |
| **Quiet** | a tuning that converges in under 10 integrations and 120 s prints no tune line; one that runs past either prints by the rule; a guard or reset step prints; the stored values are bitwise unchanged (the tuned golden history) | 02 |

## 3. Stores

None is opened but temporary SQLite stores in `tempfile` directories (`_TempStoreCase`,
`ObjectFactories/tests/test_version_keyed_lookups.py`). No Ray starts (U3).

## 4. The interfaces

- **01 to 02.** 01's commit; the counts.
- **To `main`.** The branch contains `jordan-normalization`. Merging it into `main` brings both; or
  `jordan-normalization` first, then this. The user's (U1).
- **To the rebuild.** §4.16 of `review-remediation-verification.md` stands; 02's §4.17 adds the flag
  to the run scripts' options and restates the heartbeat's bound under the quiet rule.

## 5. The rules this campaign runs under

`CLAUDE.md`'s invariants 1–7 and "Repository mechanics", and from `dk-and-rpk-migration`: deliberate
breakages recorded as diffs, none committed; no test needs Ray or opens a store outside a `tempfile`
directory; a package defect is reported upstream; no change in a sibling repository. And:
- **Existing tests are modified only where a prompt changes what they test**: in 02, the tests that
  parse or count tune lines (§0.3), each listed with its reason.
- **Values do not move.** The golden histories (`golden_history_05.json`,
  `golden_history_06_tuned.json`) and the golden fingerprint pass bitwise in both prompts.

The log template is `dk-and-rpk-migration` README §5.1's.

## 6. Decisions

### 6.0 Ruled by the user (2026-10-27)

- **R1 — the retry**: `--retry-failed-scalar`, like `--retry-failed-bbn`; the cap stays out of the
  key.
- **R2 — the tune lines**: quiet, by the rule "120 s and 10 integrations", adjustable later;
  Ray's deduplication stays on.
- **R3 — RayPoolKit's wait**: reported upstream to RayPoolKit; not this campaign's.
- **R4 — the shape**: a small campaign of its own.

### 6.1 For the user to rule (U1–U4)

| | Proposed | Alternatives |
|---|---|---|
| **U1** — name and branch | `tuning-followups`, cut from `jordan-normalization` at `4ad15fe` (it is not merged into `main`); merged into `main` with or after `jordan-normalization` | cut from `main` after the user merges `jordan-normalization` |
| **U2** — the tools | `tune_history(…, progress="quiet")` is the default, which the remote and so `main.py` use; the three standalone tools pass `progress="every"` and keep one line per integration, since a person runs them at the terminal for a few β and reads the lines | quiet everywhere, the tools too |
| **U3** — a rehearsal | **none**: the bound of §0.3 under the rule (P7) is shown by tests on a fake clock and by reading the wrapper; the rebuild's own heartbeat is the measurement | a short registered build on the isolated cluster, `jordan-normalization` 09's run `c` (two histories, about 4 min), with the recorder, to measure the silence under the rule |
| **U4** — prompts | **two**, each writing its own notes in the documents; 02 writes §4.17 and `CLAUDE.md`'s counts; 02's review closes the campaign | a third, close prompt for the documents and the handover |

### 6.2 Proposed by the planner (P1–P12)

Accepted as proposed unless the user rules otherwise.
- **P1 — the flag.** `--retry-failed-scalar`, `store_true`, default off, in the shared parser beside
  `--retry-failed-bbn`, with help of the same form: retry histories that failed under the current
  version label (for example after raising `--tuning-max-integrations`); without it a stored failure
  counts as done. Like `--retry-failed-bbn` it is not a key, the plot scripts accept and do not read
  it, and the wrapper's `scope` does not name it.
- **P2 — the first pass.** `missing_models(models, lookups, retry_failed=False)` returns, as
  `select_missing` does, the missing models and the count of stored failures; with `retry_failed`
  a stored failure is missing. The stage prints, after its queue, "N histories with a stored failure
  retried (--retry-failed-scalar)" or "N skipped because of a stored failure", beside the existing
  failure summary.
- **P3 — the second pass.** With the flag, the second lookup passes `failure=False`, so that the
  stored failure is not found, an unpopulated history is returned and computed, and the new row is
  stored beside the failure. Without the flag the payload is as today.
- **P4 — the factory's ordered lookup**, BBN's rule (`run-integrity` 03): with `failure=None`, order
  by `failure` ascending, then `timestamp` and `serial` descending, `limit 1`, so that the success is
  returned if one exists, else the newest failure; with `failure=True`, the newest failure. With
  `failure=False` unchanged. No spec changes, so no `FINGERPRINT_FORMAT` bump; no value changes, so
  no `VERSION_LABEL` bump; a `2026.7.0` store stays readable.
- **P5 — `bbn_from_store`**: `_FIND_QUERY` gains `and sm.failure = 0` (the tool reads a success's
  values), with a comment naming this campaign.
- **P6 — the print rule.** Module constants in `ComputeTargets/ScalarHistory.py`,
  `TUNE_LINE_INTERVAL_S = 120.0` and `TUNE_LINE_SLOW_INTEGRATIONS = 10`. With `progress="quiet"`,
  integration k (k = 0 the seed's) prints its line, in today's form, when any of: at least
  `TUNE_LINE_INTERVAL_S` on the tuning's clock since the history's last printed line, or since the
  tuning began if none; k + 1 ≥ `TUNE_LINE_SLOW_INTEGRATIONS`; the integration was reached by a guard
  step; or by a reset of the secant (the twice rule), which `TuningSecant` reports. `progress="every"`
  prints every line, as today. The clock is a keyword (`clock=perf_counter`) so tests can inject one.
  A failure's `!!` line is unchanged. The line's format, the stored counts and the stored values do
  not change.
- **P7 — the heartbeat bound.** Under the rule, a tuning's longest silence is at most
  `TUNE_LINE_INTERVAL_S` plus one integration (about 130 s at the slowest measured), well inside the
  900 s window, which does not change. 02 states it in the documents and §4.17; it is read, not
  measured (U3).
- **P8 — the deduplication issue** closes with 02 as superseded by R2: the quiet lines are few and
  spread out, and the stored `tuning_iterations` is the count to read.
- **P9 — tests.** 01: `missing_models` with and without the flag; the factory's ordered lookup on a
  temporary store (a failure then a success; two failures; a success then a failure), by
  `object_get` and `object_get_vectorized`; `bbn_from_store`'s query with a failure row beside a
  success; the parser's flag. 02: the rule on an injected clock (each trigger alone, and none), a
  guard and a reset, and `progress="every"`; `test_tuning.py`'s line tests on `progress="every"` where
  they test the line's form; the tuned golden history unchanged. Each new test is shown to catch a
  breakage of what it tests.
- **P10 — the documents.** Each prompt adds dated notes in `architecture-summary.md` and
  `numerical-strategies.md` (§3.7.5 for 01, §3.7.6 for 02), additive as `jordan-normalization` 10's
  were. 02 writes `.documents/review-remediation-verification.md` §4.17 in §4.16's form and
  `CLAUDE.md`'s counts.
- **P11 — models**: Opus for 01 (a lookup the whole pipeline reads), Sonnet for 02.
- **P12 — issues.** The two assigned at planning (`.documents/OPEN_ISSUES.md` §1.17); each prompt
  closes its own, on `jordan-normalization`'s board §4 and in the index. Anything else found is
  recorded and opened, not fixed.

## 7. What this campaign hands to the user

- The rebuild, with `--retry-failed-scalar` available and quiet logs (§4.17).
- The RayPoolKit report (R3), outside this campaign.
- The merge into `main` (U1).
