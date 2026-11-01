# Log 12 — close

**Subject:** Close the `inelastic-reflection` campaign: the documents, `CLAUDE.md`, the handover, the indexes
**Commit:** `7ad6744`, on branch `inelastic-reflection`, on top of `7cfced4`
**Date:** 2026-11-01, by Claude Sonnet 5.5
**Result:** **COMPLETE.** C1–C5 shipped; no stop condition was met; no code changed. Opened none, closed none.
- `numerical-strategies.md`: §6.10 and §6.11 (with three sub-subsections) and one one-line note, at §3.5.3;
  `architecture-summary.md`: six dated notes; `review-remediation-verification.md`: §4.18 (seven points);
  `CLAUDE.md`: descriptions only. **No existing line of the three documents is deleted or reworded** (`git diff`
  shows 0 deleted lines in each).
- Suites: 26 / 497 (122 + 375) / 129 / 123 / 68, log 11's exactly, all passing. `main.py --inventory` on the
  reference store: exit 0, 26 s, `problems: none`, all 18 files byte-identical before and after.
- The index stays at 27 open; the board and `prompts/INDEX.md` close the campaign.

Orchestrator note 12 (its Gate, §0 and ten rulings) governed where it differs from the prompt; each ruling is
classified in §2.

## 1. What shipped

- **C1, the documents.**
  - `numerical-strategies.md` §6.10, "the measurement of prompt 11": the drain across M, ΔN_eff against α,
    natural mode at 10⁻⁷ and 0.5, the parking ε, the margins at M = 10⁻¹² and 4×10⁻³¹, the BBN spline across
    injections, the M = 10⁻³ rehearsal and α = 0 against 2026.7.0, each with its verification section.
  - §6.11, "dissipation mode as it stands": §6.11.1 the eight steps (the drain, the gate, the unresolved rule,
    parking, the release, the wall search, the peak and the margins, the stored values) with `file:line` at
    `7cfced4`, the section of §6.1–§6.10 and the rulings; §6.11.2 the table of statements of §6.1–§6.9 that
    are superseded (14 rows), with where; §6.11.3 the two statements in code that are stale.
  - `numerical-strategies.md` §3.5.3: a one-line note that at α > 0 the floor rule drains.
  - `architecture-summary.md`: the §7.2 map (after 10's note), the §14 note (D3 and `PARKING_EPSILON`), a §5.2
    note (`alpha_value` in `replicated_tables`, the shard key still β), a §8.2 note (`alpha_value`), a note after
    the tree (`dark_radiation.py`, `alpha_value.py`) and a note after the `ScalarHistory` column table
    (`first_bounce_reflected` and the table's width).
- **C2, `CLAUDE.md`**: the five counts and the two halves; the time of `ComputeTargets/tests` (590 s, with the two
  halves' seconds and loads); the parenthesis (dated 2026-11-01, tests added per package, `tools/tests`'s sentence
  re-stated from this run); the plot-scripts bullet (`--alpha-values`, `alpha=<α>/`); the Redshift bullet (ρ_χ,
  ΔN_eff, the per-sample ratio, α as a key). No rule is added (D5).
- **C3, the handover** `review-remediation-verification.md` §4.18, in §4.16's and §4.17's form: the prompt-by-prompt
  list with commits, "this supersedes" (four items), and seven points: what changed for a build; the run scripts'
  changes as a table by change; the two modes; the α grid; the cost, measured and estimated apart; what stays
  open; the merge and the stores.
- **C4, the board closed:** row 12, the status line rewritten short, "(created by 07)" made "(created by 11)",
  the **Code:** line completed, §3's first paragraph, §5.14 and a closing paragraph. `prompts/INDEX.md`: the row
  **complete**, the header's date and counts. `.documents/OPEN_ISSUES.md`: the header's clause, §1.19 and §1.20.
- **C5, the final suites:** §4.2.
- **This log.**

## 2. Deviations from the prompt, and the rulings of note 12

1. **`STRUCTURALLY REQUIRED`: additive, in the documents' own form** (ruling 1; `CLAUDE.md` invariant 6). Every
   correction is a dated note at the statement, in the form
   `> **Note, 2026-11-01 (`inelastic-reflection`, prompt 12): …**` (a one-line note in `numerical-strategies.md`).
   The new subsections are §6.10 and §6.11 under the headings the ruling gives. No statement of §6.1–§6.9 is
   edited: §6.11.2 lists the superseded ones.
2. **`IMPLEMENTATION CHOICE`: what C1 writes** (ruling 2). §6.10's five items and the margins; §6.11 in the
   prompt's order with its table starting from the note's §0 list; the §7.2 map by piece with notes pointed to by
   their line at `7cfced4`; the §14 note with D3 and `PARKING_EPSILON`'s read by `file:line`. §5.2: 09's note
   says only "replicated as `beta_value` is" and does not mention the shard key, so a §5.2 note was added (it
   points to 09's note). Four further notes (tree, §8.2, `first_bounce_reflected`, `numerical-strategies.md`
   §3.5.3) come from the grep (§3), by ruling 1's test, "false of the present and no later dated note says so".
3. **`IMPLEMENTATION CHOICE`: the greps** (ruling 3): §3.1.
4. **`IMPLEMENTATION CHOICE`: the "made false" lines** (ruling 4): §3.2. The note's three code statements:
   two are listed "left: code, outside 12's files; for the orchestrator's housekeeping"; the third,
   `test_injection.py` test 4's docstring, was already corrected by `67cb5f8` and is true (§3.2).
5. **`STRUCTURALLY REQUIRED`: `CLAUDE.md`, descriptions only** (ruling 5; prompt §0). §0's list is what changed.
   The counts and times are this prompt's own run (ruling 9). ρ_χ went into the **Redshift** bullet, since the
   dark radiation is defined by the Einstein-frame e-fold N and that bullet describes the frames. The modal grep:
   §4.4. D3's sentence went to `architecture-summary.md` §14 as a description and is proposed in §6 below.
6. **`IMPLEMENTATION CHOICE`: §4.18** (ruling 6). Heading and opening as ruled; "this supersedes" has the four
   items (the three ruled, and §4.16 point 7's stores, whose inventory figures are of the day before); point 2 is a
   table by change with a column "a script that passes nothing new"; point 5 separates measured from estimated;
   point 6 gives each open item's next step from the board and names the two that bear on a rebuild; point 7 gives
   `git merge-base`. Every command it names ran with `--help`, exit 0 (§4.3).
7. **`NOTED`: the board closed** (ruling 7), as ruled. The **Code:** line was checked against
   `git diff --name-only 6f635e6 7cfced4 -- '*.py'` outside tests, the probes and `.documents/`: 22 files; the
   line named 10 paths, and now names all but `CosmologyConcepts/__init__.py` (a one-line export) and the
   `config/` files by name, which it keeps as `config/`.
8. **`NOTED`: the indexes** (ruling 8). `prompts/INDEX.md`'s old header read "15 campaigns: 1 planned, 1 live, 13
   closed" with 15 rows in the table (14 **complete**, this campaign's **live**); it now reads "15 campaigns: 0
   planned, 0 live, 15 closed", dated 2026-11-01. The row's old text stopped at "3 of 8 prompts written". No other
   campaign's row is edited. `.documents/OPEN_ISSUES.md`: the count stays 27.
9. **`IMPLEMENTATION CHOICE`: the suites** (ruling 9): once, before the edits, serially, from the repository root;
   ComputeTargets in two halves, the `test_[g-z]` half as a background call waited for by its notification, the
   rest in the foreground at 600 000 ms; `uptime` at each start and end. See also D13.
10. **`STRUCTURALLY REQUIRED`: the log** (ruling 10), in `dk-and-rpk-migration` README §5.1's form, with this
    section, §3, §4, §6 and §7.
11. **`STRUCTURALLY REQUIRED`: the reference store has 18 files, not 17.** The prompt's §2.3 says "17 files"; note
    12 ruling 6 counts the manifest. `shasum -a 256` of all 18 before and after is identical (§4.3).
12. **`STRUCTURALLY REQUIRED`: the prompt's "per-sample ρ_χ/ρ_R is a payload key and not a column" is corrected
    to the code.** The ratio is a payload key beside the samples (`"rho_chi_over_rho_rad"`, a tuple parallel to
    `"sample"`), not a field of `SampleValues`, and it is stored as a `ScalarHistoryValue` column
    (`ObjectFactories/ScalarHistory.py:1092`, `Float(64)`, not null; `ComputeTargets/ScalarHistory.py:5381`).
    `CLAUDE.md`'s Redshift bullet says that. A sentence that the code contradicts is not written.
13. **`UNINTENDED DRIFT`: two suites ran twice.** The first foreground pass printed only the last four lines of
    CosmologyModels, ComputeTargets `test_[a-f]` and ObjectFactories, which cut the counts off for the first two.
    I ran CosmologyModels and `test_[a-f]` again, with the counts captured (§4.2). ObjectFactories ran once, with
    its count. Both passes were serial and all passed; nothing else follows from it.
14. **`NOTED`: `_parking_cut` is not a function of the code.** Note 12 and the prompt's §14 wording read
    `PARKING_EPSILON` "in `_parking_cut`". The name occurs only in the docstring of
    `.documents/inelastic-reflection/measure/eps_point.py:14`. The code reads it in `parking_ratio_test`
    (`ComputeTargets/ScalarHistory.py:1138`, the read at `:1147`), which the step loop calls at `:2391`; the §14 note
    says so.

## 3. The greps (ruling 3) and the "made false" lines (ruling 4)

### 3.1 The greps

Commands: `git show 7cfced4:<file>` for each of `.documents/architecture-summary.md`,
`.documents/numerical-strategies.md` and `CLAUDE.md`, then `grep -n -i -e TERM` (`var/inelastic-reflection-12/greptab.py`
counts the hits and splits them into this campaign's own dated text, `architecture-summary.md` `:1056`–`:1319`
and `:1569`–`:1651` and `numerical-strategies.md` `:1119`–`:1552`, and the rest). All lines are those of
`7cfced4`, before the edits. Format: term: hits in `architecture-summary.md` / `numerical-strategies.md` /
`CLAUDE.md`, and a disposition for every hit. **"A dated record, left"** is a hit inside the campaign's own notes
(the campaign's sections are records; §6.11.2 lists what in them is superseded) or in another campaign's dated
note. The two paper notes and the `peakewsurf-*` reports were not read.

| Term | Hits (arch / NS / CLAUDE) | Dispositions of the hits outside the campaign's own text |
|---|---|---|
| `elastic` | 17 / 26 / 0 | 13 and 20 in the campaign's own text, a dated record, left. Elsewhere: arch `:845`, `:949`, `:1434` are `integrator-remediation`'s notes, a dated record, left; arch `:964` (`first_bounce_reflected` "an elastic reflection"), noted at the new note after that table; NS `:188`, `:217` true at α = 0 and noted at §3.5.3's note of this date; NS `:262` (§3.5.3's heading), `:274` noted at that note; NS `:391` (JN note, "the elastic reflection of §3.5.3 reads") true at α = 0, noted at §3.5.3's note; NS `:413` (the `first_bounce` row, `reflected`) noted at the arch note |
| `stopping rule` | 1 / 1 / 0 | all in the campaign's notes: arch 03's note says it was withdrawn; NS §6.3 says so and §6.11.2 lists it. A dated record, left |
| `w_rate` | 1 / 2 / 0 | the same: a dated record, left |
| `taper` | 0 / 1 / 0 | NS §6.3's text (the stopping rule withdrawn): a dated record, left |
| `0.25 eV` | 2 / 2 / 0 | arch 06's and 07's notes, and NS §6.5 and §6.6: 07's notes already say it was 0.25 meV (arch `:1215`, NS `:1397`); a dated record, left; listed in §6.11.2 |
| `0.27 eV` | 0 / 0 / 0 | none |
| `FINGERPRINT_FORMAT` | 6 / 1 / 0 | 1 in the campaign's 09 note (`:1260`, true: 6). Elsewhere: arch `:247`, `:254` the format-1 guard of `rrk-adoption`, a dated record, left; arch `:493` (JN 07's note, "is 5"), already noted at `:1260`; arch `:589` (JN note: "neither … moves"), a dated record, left; arch `:1438` (JN 01's note), a dated record, left; NS `:514` (JN §3.7 intro, a commit's `FINGERPRINT_FORMAT` 5), a dated record, left; §4.18 supersedes by statement |
| `format 5` | 0 / 0 / 0 | none |
| `2026.7.0` | 1 / 1 / 0 | arch `:494` (JN 07 note: "`VERSION_LABEL` is `2026.7.0`"), already noted at `:1260` (09's note, `2026.8.0`); NS `:513` (JN §3.7 intro), a dated record, left |
| `replicated_tables` | 5 / 0 / 0 | arch `:447` true (`ShardKeyType` is still `beta_value`); `:487`, `:761` JN 07's notes, a dated record, left; `:624` (how to add a table) true; `:727` the listing, noted at the §5.2 note of this date |
| `shard key` | 12 / 1 / 0 | arch `:88`, `:447`, `:722`, `:725`, `:753`, `:1400`, `:1892`, `:1902`, `:1936`, `:2023` and NS `:558` true (the shard key is still β; noted at the §5.2 note and the §8.2 note); `:509`, `:521` `dk-and-rpk-migration` notes, a dated record, left |
| `jordan-normalization-09-reference` | 0 / 0 / 0 | none (the store is named in §4.16, which §4.18 supersedes by statement) |
| `golden` | 14 / 9 / 0 | 6 and 5 in the campaign's text, a dated record, left. Elsewhere: arch `:255`, `:256`, `:411`, `:412` true (a golden fingerprint file; 09's note, `:1260`, adds the new one); `:493` already noted at `:1260`; `:1014`, `:1032`, `:1451` JN notes, a dated record, left (the golden histories are still bitwise); NS `:584`, `:605`, `:812`, `:834` JN §3.7, a dated record, left |
| `alpha` | 36 / 1 / 0 | all in the campaign's own text, a dated record, left |
| `α` | 55 / 33 / 0 | all but NS `:1932` (Li8(p,d)Li7 is α·exp(γ/T9): the rate, unrelated, true) in the campaign's own text, a dated record, left |
| `ρ_χ` | 4 / 22 / 0 | all in the campaign's own text, a dated record, left |
| `dark radiation` | 0 / 2 / 0 | NS §6.1 (in its own text), a dated record, left |
| `ΔN_eff` | 5 / 8 / 0 | all in the campaign's own text, a dated record, left |
| `parking` | 17 / 10 / 0 | all in the campaign's own text, a dated record, left |
| `--alpha` | 4 / 0 / 0 | all in arch 10's note, a dated record, left |

`CLAUDE.md` has no hit for any term at `7cfced4`; its edits (§1, C2) are the counts, times, the plot-scripts bullet
and the Redshift bullet. Further greps over the three files, run to decide the notes above: `tuning_tol\|tuning
tolerance\|key of every\|is a key\|keyed` (arch `:562`–`:563` and `:997`–`:1000`, JN 07's statements of the
`ScalarHistory` key without α, and NS `:626`–`:628`: already noted at arch `:1222` (09's note, "α is a key of
every history") and NS §6.9, so no note is added); `String(256)\|extra_data` (`extra_data` is now `Text`: already
noted at arch `:1248`); `reflections\|REFLECTIONS_KEY\|number_reflections` (NS `:302`–`:303`, `:374`: the count now
includes unresolved reflections, already noted at arch `:1141`).

### 3.2 The "made false" lines

The lines are in the logs' "Observations not acted on" and "State handed to the next prompt" (no log uses the
words "made false"); the pass covered logs 01–11 with 07b. `CLAUDE.md`'s counts are stale in each of 01–10's
lines "`CLAUDE.md`'s counts" and are done in `7ad6744` (C2).

| Log | The line | Fixed by | Why |
|---|---|---|---|
| 01 §6 | `.documents/integrator-audit-2026-09-30/harness.py:60` and `…/pipe_runs.py:146` call `ODEPolicy` with the old signature | left | dated records (invariant 6), and made false by `jordan-normalization` 04, not this campaign |
| 01 §6 | `AdiabaticHistory` does not see ρ_χ | 09 (`3984ec5`) | the code, P29 |
| 01–10 | `CLAUDE.md`'s suite counts are stale (each log) | `7ad6744` (C2) | the counts and times of §4.2 |
| 02 §7 | the loop's first wall `TurningPoint` and `first_bounce` can differ | left | true; it does not happen on any history measured |
| 03 §7 | `test_injection.py` test 4's docstring says every ratio measured 0.0 | `67cb5f8` ("Correct two docstrings of test_injection after the gate") | `ComputeTargets/tests/test_injection.py:396`–`:405` now reads "Under prompt 02's drains … Under prompt 03's gate one ratio here is 1.43e-14": time-scoped and true |
| 05 §7 | `REFLECTIONS_KEY`'s comment (`ComputeTargets/ScalarHistory.py:75`) says "elastic reflections (the floor-triggered reflection model …)"; the count now includes unresolved reflections and at α > 0 they drain | left: code, outside 12's files; for the orchestrator's housekeeping | a comment; listed in §6.11.3 |
| 05 §7 | `integrate_history`'s verbose line (`:4024`) prints "elastic reflection(s)" | left: code, outside 12's files; for the orchestrator's housekeeping | a printed string; listed in §6.11.3 |
| 05 §7 | README §8.0's drain at β = 2 and 3 (0.218, 0.261 of the ceiling) is superseded by 0.2902 and 0.4219 under P20 | noted in §6.11.2's closing paragraph; README left | the README is a record |
| 05 §7 | `classical_turning_point` refuses a quantized wall | 07b (`92f41f2`) | the code; `[05-the-wall-search-…]` closed |
| 06 §8 | `tracker_adiabatic` False raises today; 07 replaces the failure | 07 (`5397f8f`) | the code; NS §6.5 says so itself (§6.11.2) |
| 07 §7 | the M = 10⁻³, β = 1.2 release is given as 0.27 eV (arch's 06 note `:1182`, NS §6.5 `:1341`); it was 0.27 meV | 07's own notes (arch `:1215`, NS `:1397`) | the old text is left (invariant 6); listed in §6.11.2; no note added |
| 07b §7 | `assess_infall`'s docstring reads "ln(V − V_floor) = −inf where V rounds to V_floor or below" | none needed | true for its own expression, which has no width |
| 09 §7 | `bbn_from_store`'s `_FIND_QUERY` has no α; the wrapper's scope does not name α; `tools/history_and_bbn.py` takes no `--alpha` | 10 (`8c12afa`) | the code |
| 10 §7 | `registered_run.py --help` does not list `--alpha-values` | none needed | true: the wrapper passes `main.py`'s options after the bare `--` (`--help`, §4.3) |
| 11 §7 | "For `CLAUDE.md`" | `7ad6744` (C2) | counts, time, plot bullet; the `sys.modules` sentence is §14's description and §6's proposal |
| 11 §7 | "For the architecture summary" | `7ad6744`: §6.10, §7.2 map, §14 | |
| 11 §7 | "For the handover §4.18" | `7ad6744` | |
| the pass | arch's tree, §5.2, §8.2 and the `first_bounce_reflected` row, and NS §3.5.3, say what was true before α | `7ad6744`: dated notes | §3.1's greps |

## 4. Verification performed

### 4.1 The Gate

At 09:05 GMT, `HEAD` `7cfced4` (the dispatch commit), `git status --porcelain --untracked-files=all` empty, one
worktree; `git merge-base main inelastic-reflection` `6f635e6` = `main`; `ray_cluster.sh status` exit 0 (lock free,
no Ray process of `/tmp/ray-cpbh`, ports 6380, 6381, 8267, 52367 free); `runregistrykit list`: 14 runs, all
finished; the reference store, 16 shards and its manifest present (18 files); `.documents/OPEN_ISSUES.md` §1.20
reads "None is assigned now". The machine's load was 28.35 / 22.57 / 15.58 at 09:05, from its own daemons, with no
ChamPBH process running; the loads below are the suites' own.

### 4.2 The suites

Serially, from the repository root, before the edits, `CLAUDE.md`'s commands; ComputeTargets in two halves, the
`test_[g-z]` half as a background call; time `unittest`'s, load the one-minute figure of `uptime` at the start and
the end of each root.

| Root | Tests | Time | Load at start | Load at end |
|---|---|---|---|---|
| CosmologyModels | 26 OK | 68.9 s | 6.06 | 6.07 |
| ComputeTargets `test_[a-f]*` | 122 OK | 135.8 s | 6.07 | 24.97 |
| ComputeTargets `test_[g-z]*` | 375 OK | 454.6 s | 11.15 | 5.83 |
| ObjectFactories | 129 OK | 79.7 s | 6.63 | 6.24 |
| tools | 123 OK | 131.7 s | 24.97 | 9.70 |
| RunDrivers | 68 OK | 24.0 s | 9.70 | 11.86 |

Log 11's counts exactly, before and after: 26 / 497 (122 + 375) / 129 / 123 / 68. Nothing failed, so nothing was
re-run for a failure. The first, count-less pass of CosmologyModels and `test_[a-f]` (D13) ran at loads of 26.24 and
15.88 at its starts and ended `OK`. The suites ran with no Ray cluster.

### 4.3 The rest

- **The `--help`s**, from the repository root, each exit 0: `main.py` (158 lines; `--alpha-values` at `:58`),
  `plot_by_beta.py` (163), `plot_ScalarHistory.py` (162), `RunDrivers/registered_run.py` (18),
  `tools/history_and_bbn.py` (52; `--alpha`), `tools/bbn_from_store.py` (42; `--alpha`),
  `./venv/bin/python -m runregistrykit list --help` (exit 0, with `--stale-after SECONDS`), and `list` itself
  (exit 0; 14 runs, all finished). The
  options §4.18 names are in them: `--bbn-wall-clock-limit`, `--job-name`, `--ray-address`, `--shards`,
  `--database`, `--config`, `--alpha-values`, `--retry-failed-scalar`, `--prune-unvalidated`.
- **The inventory**: `PYTHONPATH=. ./venv/bin/python main.py --config exponential.yaml --database
  var/datastores/inelastic-reflection-11-reference.db --shards 16 --inventory`, no Ray, exit 0, 26 s (24.5 s of
  user time): "read-only; 16 shards", 75,050 records in 16 classes, `problems: none`, label 2026.8.0,
  `alpha_value (replicated): 2 records`, `ScalarHistory` 8 records (8 validated, 46,162 rows), `AdiabaticHistory` 8
  (46,162), `BBNData` 8 (8,909). `shasum -a 256 var/datastores/inelastic-reflection-11-reference*` (18 files: 16
  shards, the primary `3ca80192…8b40` and the manifest `4201c44d…9409`) before and after: `diff` exit 0; the
  digest of each listing is `2774a7fe9364af823b70b2c01bc386d59a5d8648209df3a0b6e977bbc87aeb77`. Output in
  `var/inelastic-reflection-12/inventory.log` and `sha-before.txt`, `sha-after.txt`.
- **No Ray was started, no store built, no tool or grid run**; nothing outside `var/inelastic-reflection-12/` was
  written there; the lock was not touched.
- **`git diff --numstat 7cfced4`** over the seven tracked files: `.documents/OPEN_ISSUES.md` 5/3,
  `.documents/architecture-summary.md` 100/0, `.documents/numerical-strategies.md` 165/0,
  `.documents/review-remediation-verification.md` 187/0, `CLAUDE.md` 21/14, `prompts/INDEX.md` 2/2,
  `prompts/inelastic-reflection/IMPLEMENTATION_STATE.md` 40/15; this log is the eighth file. No `.py` file changed,
  so `black --check` has nothing to find, and this run is C5's final run.
- **The board and the index agree**: 4 open on the board's §3, §1.19's four rows, the campaign's row in
  `prompts/INDEX.md` ("§1.19; 4 open, none assigned; 27 open in the whole index"), and the count 27.

### 4.4 The `CLAUDE.md` modal grep (ruling 5)

`git diff -U0 7cfced4 -- CLAUDE.md | grep '^+' | grep -v '^+++' | grep -n -i -E '\b(must|never|always|do not|don.t|should|shall)\b'`
printed one line:

```
12:+  one-minute load of 25.0 at its start; run it alone.) They **must not
```

It is an unchanged clause carried inside an edited line: "They **must not need a Ray cluster or a persistent
datastore**" is the original line's tail, and the line was re-flowed to follow the parenthesis. The same edit
carries "run it alone" (the original's) and, unedited above it, "run it in two halves". Every sentence `7ad6744`
adds to `CLAUDE.md` is a description.

## 5. The deliberate-breakage record

None: no code or test was changed, so there was nothing to break on purpose.

## 6. Observations not acted on

- **A sentence proposed for the user about D3.** Log 11 §7 raises one for `CLAUDE.md`, and the user has said some of
  its rules were never agreed, so none was added. As a description it could read: "The name
  `ComputeTargets.ScalarHistory` as an `import … as` target is the class that `ComputeTargets/__init__.py` exports,
  so a module constant of that file is set through the module object that
  `importlib.import_module("ComputeTargets.ScalarHistory")` or `sys.modules` gives." It is in
  `architecture-summary.md` §14 as a description; whether it belongs in `CLAUDE.md` is the user's.
- **Two statements in code are stale** and are left: the comment of `REFLECTIONS_KEY`
  (`ComputeTargets/ScalarHistory.py:75`) and `integrate_history`'s printed "elastic reflection(s)" (`:4024`). For the
  orchestrator's housekeeping (note 12 ruling 4); §6.11.3 says so.
- **The prompt's and the note's `_parking_cut`** is a name only in `eps_point.py`'s docstring (D14); the measurement
  script is a record and is not edited.
- **The note's count of "fourteen"** test modules that take the module by `importlib` is, by `grep -rl import_module`
  over the packages narrowed to files naming `ScalarHistory`, 17 files at `7cfced4`: twelve test modules and the
  helper `dissipation_histories.py` in `ComputeTargets/tests` (13), `tools/tests/test_surfing_scan.py` (14), and
  `tools/bbn_from_store.py`, `tools/history_and_bbn.py` and `tools/surfing_scan.py` (17). §14's note gives the 17.
- **`tools/tests`'s old sentence** ("three tuned histories, about 20-30 s each") was not re-measured test by test. I
  replaced it with what this run measured (131.7 s for the root, load 25.0 at its start) and "holds tuned
  histories", which five of its test files show (`tune_history` or "tuned" in each).
- **§4.18's `--stale-after` remark** ("reads it as running") is §4.16 point 3's, read and not measured; §4.18 says so.
- **§4.18 point 5's estimate for α > 0** (0.28 of the α = 0 scalar stage, from 43.2 s against 153.1 s on four
  histories at one M) is an order and not a figure; no rebuild-sized cost is given.
- **`prompts/INDEX.md`'s old header** counted wrongly (1 planned, 1 live, 13 closed, for 14 complete and one live);
  it now reads 0, 0, 15 and is right by the table's 15 rows.
- **The load** was 28.35 at the Gate and 26.24 at the first suite's start, from the machine's daemons; the
  `test_[a-f]` half's end load was 24.97 (the daemons again). Nothing failed.
- **Not read:** the two paper notes and the `peakewsurf-*` reports (ruling 3); `orchestrator/` (the brief).

## 7. State handed to the next prompt

The campaign is closed; there is no next prompt. For the user:
- **Tree:** `7ad6744`, on `inelastic-reflection`, on top of `7cfced4`; nothing pushed; `main` not merged (U1, the
  user's). `git merge-base main inelastic-reflection` is `6f635e6`, which is `main`, so the merge is a fast-forward;
  `main` is level with `github/main`; the branch has no upstream.
- **The science rebuild** is described in `.documents/review-remediation-verification.md` §4.18: what changed for a
  build (α a key, `--alpha-values`, format 6, 2026.8.0, a new store), the run scripts' changes by change, the two
  modes and where each holds, the α grid (the user's choice, with the facts), the cost (measured and estimated
  apart; no `--stale-after`), what stays open and the merge. `numerical-strategies.md` §6.10 is the measurement
  and §6.11 the reading guide.
- **Stores and runs:** `var/datastores/inelastic-reflection-11-reference.db` (16 shards, the manifest), unchanged by
  `--inventory` (18 files byte-identical); the resumed store and `jordan-normalization-09-reference` retired by the
  user; the runs of prompt 11 under `var/runs/` (14 finished, none running). Never `git clean` with `-x` or `-X`.
- **Open issues:** four on this board (`.documents/OPEN_ISSUES.md` §1.19), none assigned; 27 open in the index.
  Two bear on a rebuild: `[11-ray-deduplicates-the-dissipation-warnings-of-parallel-histories]` and
  `[11-the-ratio-steps-at-injections-and-parking-in-the-10-100-keV-window]`. For the user to rule:
  `[00-xav-sigma-is-non-monotone-at-25-35-keV]` (its own ruling, since it changes every history) and the two of
  prompt 11; `[09-the-bath-drifts-from-g-rho-T4-by-about-1e-3]` is left documented by the user.
- **For the user, not acted on:** the β = 0.5, M = 10⁻³, α = 0.1 history's ΔN_eff today of 8.95 (R17's unofficial
  regime); D3's sentence for `CLAUDE.md` (§6); the two stale statements in code (§6).
- **Counts:** CosmologyModels 26, ComputeTargets 497 (122 + 375), ObjectFactories 129, tools 123, RunDrivers 68.
