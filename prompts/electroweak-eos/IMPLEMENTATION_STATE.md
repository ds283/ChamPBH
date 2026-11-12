# electroweak-eos campaign — implementation state

**Last updated:** 2026-11-12 · **Status: LIVE — 2 of 4 landed (01a, 01); 02 next.** Planned 2026-11-12 at the user's
request. U1–U6 ruled by the user as proposed the same day, and U7 (the provenance of the rows) ruled out of
scope. P1–P12 stand, with P11 amended by U7 (README §6). The plan is
[`README.md`](README.md), on branch `electroweak-eos`, cut from `main` at `ec6bca8`.

The campaign replaces ChamPBH's w table, which above about 100 GeV is a free gas at vacuum masses, with
the table that follows Laine & Meyer through the electroweak crossover. That is the notebook's rows
together with the points of 2026-11-12, as recorded in the kicking-function audit's addendum (`ec6bca8`).
It then measures the electroweak-capture threshold and the change to a sample of the `2026.10.0` run's
histories, and hands a rebuild under `2026.11.0` to the user.

**Campaign:** [`README.md`](README.md) ·
**Code:** `CosmologyModels/GenericEOS/` (`Xav_EOS_data.csv`, `Xav_EOS_spline`), `tools/` (the build script),
`config/version.py` ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) (no issue assigned; P11 as
amended by U7)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01a | Resolved turning points | an interior pair of sign changes of π kept only if the solver resolves it (README §8); lands before 01's commit | Opus | ✍️ | ✅ | `634f141` | [`logs/01a-resolved-turning-points.md`](logs/01a-resolved-turning-points.md) |
| 01 | Install the table | the table by U2 (b); the build script and test; `VERSION_LABEL` 2026.11.0; re-pinned tests and golden histories; the docstring | Opus | ✍️ | ✅ | `5947b4f` | [`logs/01-install-the-table.md`](logs/01-install-the-table.md) |
| 02 | Measure the effect | the EW threshold; the sample of `2026.10.0` histories old against new; the bath drift | Opus | ⬜ | ⬜ | | |
| 03 | Documents and handover | the documents, issues, indexes and the handover; the close | Sonnet | ⬜ | ⬜ | | |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Prompts are written one at a time, each after the previous one lands and is reviewed. 01a was inserted on
2026-11-12 while 01 was held at its stop; its proposals U8–U11 (README §8.3) were
ruled as proposed by the user the same day: "U8 to U11 are fine as proposed; write 01a".

Orchestrator notes, one prompt at a time, in `orchestrator/`: [`prompt-01.md`](orchestrator/prompt-01.md), written
2026-11-12 with the prompt and dispatched the same day at `468266d`; 01's handoff at its stop,
[`prompt-01-handoff.md`](orchestrator/prompt-01-handoff.md); [`prompt-01a.md`](orchestrator/prompt-01a.md), written
2026-11-12 with prompt 01a, dispatched the same day at `0479587` and used for 01a.

**Planner's probe for 01 (2026-11-12, at `b3d5906`, not committed).** The U2 (b) table (170 rows) was built in a
scratch worktree and the suites run on it.
- 22 tests fail because of the table, and pass with the old one restored. CosmologyModels passes in full.
- Above 386.7 GeV the table has Σ = 0, so a history that starts at rest is not kicked until 387 GeV. On the
  golden β = 1.5 history a wall-side turning point at N = 3.94 (T_J ≈ 390 GeV) becomes `first_bounce`
  (it was at N = 18.64).
- The note makes this a stop for the user's ruling before anything that reads `first_bounce` is re-pinned
  (note §0 and ruling 3).

**01 at its stop (2026-11-12, nothing committed).** 01 was dispatched at `468266d` and stopped at its note's
ruling 3. The early wall/plateau pair is the interpolant read below the solver's tolerance, not the field
turning, and `first_bounce` does not move (README §8.1). It also stopped on a failure outside §0's list,
`test_dissipation_store` test_10, which pins `VERSION_LABEL`. **Ruled by the user, 2026-11-12:** "Handle it
in a separate classifier prompt; re-pin test_10 to 2026.11.0". 01's work so far (the table installed, the
build script and its test, the label, the reachability pins and test_10 re-pinned) stays uncommitted in the
main checkout until 01a lands.

**01a landed (2026-11-12, `634f141`, on branch `electroweak-eos-01a` in a worktree, U10).** An interior pair of sign
changes of π is kept only if its excursion exceeds the step's tolerance on π (README §8.3 U8); a dropped pair is counted in
`IntegrationResult.unresolved_turning_point_pairs`, which is not stored (U9). 01a stopped once: its scans to T_CMB found
two late near-stall pairs dropped on the old table. **Ruled by the user, 2026-11-12:** "For the stop issue, choose (a).
For the `dropped_drains` issue, add to the allowed differences." Gate Resolved holds as so ruled (log 01a §3). It opens
one issue (§3) and closes `[01-turning-points-read-below-the-solver-tolerance]` (§4). On 01's table, 4 of the 22 tests
pass with it and 18 still fail; the list is in log 01a §6, for 01's resume.

**Orchestrator review of prompt 01a (2026-11-12).** All six checks of note 01a §3 pass, with the gate as the user
ruled it at the stop.
1. *Scope.* `634f141`'s parent is `0479587`, the note's commit. Its 28 files are the prompt's §4 list. None is
   one of note 01a's ruling 2 or `CLAUDE.md`. Nothing is pushed.
2. *Old table, bitwise.* In the worktree, `test_golden_history`, `test_golden_reflecting_history`, `test_tuning`
   (e), `test_injection` and `test_resolved_turning_points` ran 42 tests, OK, in 65.2 s (20:18–20:20 GMT,
   one-minute load 3.98 to 5.98).
   - `compare_scans.py --mode old`, re-run on the agent's JSON, reproduces the committed `out/compare_old.txt`
     line for line. It compares 241 380 turning points and exits 0 under the ruled gate.
   - An independent comparison of the 36 histories' JSON, done by the orchestrator, finds 34 with turning points
     and `first_bounce` equal. In the other two (β = 10 and 20, M = 10⁻³ M_P), the turning points after are those
     before less one plateau/wall pair, in order. `first_bounce` is equal in both.
   - `unresolved_turning_point_pairs` is 1 in those two histories and 0 in the other 34.
3. *The rule.* `resolved_sign_changes` finds the changes by the old loop, verbatim. It then walks them:
   - the excursion is max |π| over the samples from change i's `N_after` to change i + 1's `N_before`
     inclusive, which are the samples strictly between the crossings;
   - a pair is dropped when the excursion is `<=` τ;
   - after a drop the walk goes on at i + 2, otherwise at i + 1.
   The end-to-end bracket is taken only when one change was found, none dropped and the nodes differ in sign, so
   a single change left by a drop is refined on its sub-interval, as the prompt says. τ comes from `params.atol`
   and `params.rtol`. `git grep unresolved_turning_point_pairs` finds the field only in the namedtuple, its
   comment, the docstring and the loop's return, besides tests and probes. It is no payload key.
4. *The new table.* The golden β = 1.5 history, re-run by the orchestrator on the new table's copy (SHA-256
   `690090c8…0119d`, read by redirect) with 01a's change:
   - 6 turning points, the first a wall at N = 18.643963 (`first_bounce`, unchanged);
   - none before N = 18.6, and `unresolved_turning_point_pairs` = 1;
   - `dropped_drains` 0, where 01's probe without the change had 1;
   - accepted steps (1219), nfev (9776) and N_final equal to 01's probe without the change.
   The worktree's CSV was still `28edd854…29a8` afterwards.
5. *Records.*
   - The log has §5.1's sections, and every deviation is classed. The user's words at the stop are quoted in
     the log and on the board.
   - Row 01a, the Resolved gate and §4 are on the board. `OPEN_ISSUES.md` has 01's row deleted, 01a's new row
     added, **30 open** (not the note's 29: 01a opened one) and the date 2026-11-12.
   - `numerical-strategies.md` §6.2 has the dated note.
   - One correction was made in review: §4 said the new-table pair was dropped "nowhere else", but
     `out/compare_new.txt` also drops a late near-stall pair at β = 20, M = 10⁻³ M_P, N = 126.278 (0.59 τ). The
     log's summary already said so. §4 now does too.
6. *The fast-forward.* `git diff --name-only 0479587 634f141` shares no path with the main checkout's uncommitted
   work, the user's or 01's. `git merge --ff-only` succeeded. Afterwards `git status` lists the same files as
   before. The only difference is that `.documents/electroweak-eos/` now shows as `.documents/electroweak-eos/table/`,
   because `turning-points/` beside it is tracked.
   The worktree was removed, after its `venv` symlink, and the branch `electroweak-eos-01a` was deleted.

**01 landed (2026-11-12, `5947b4f`), after two stops.** The first was note 01's ruling 3, the early pair, answered by
01a. The second was the Addendum's `test_injection` test_11: on the new table its history (β = 10, M = 0.5 M_P,
α = 0.1, parking cut off) holds no interior pair. That is the table's doing: its oscillation starts at 274 GeV rather
than 405 GeV, and near the old stall π stays above zero by hundreds of τ. 01a drops nothing there. 01a's own item-(11)
test failed for the same reason, outside the Addendum's 18. **Ruled by the user, 2026-11-12:** "Go with (D), keep β = 10
on the old table". Both item-(11) classes now build their cosmology on `prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv`
through `test_injection.on_the_old_w_table()`, with no assertion changed (*STRUCTURALLY REQUIRED*; log 01 §2.1).

**Orchestrator review of prompt 01 (2026-11-12).** All six checks of note 01 §3 pass, as the Addendum and the user's
two rulings at the stops amend them.
1. *Scope.* `5947b4f`'s parent is `85b52ac`, not the agent's. Its 38 files are the prompt's §4 list less `CLAUDE.md`
   (ruling 1), plus the re-pinned tests' modules and the two item-(11) modules added by the user's ruling. Nothing was
   staged afterwards. The user's ` M CLAUDE.md`, the two `science-runs/` READMEs and `relativistic/` are still modified
   and unstaged. Nothing is pushed.
2. *The table.* `tools/build_w_table.py --check` exits 0: 170 rows, old 14, notebook_low 126, points 29, notebook_top 1.
   `test_build_w_table` passes. The installed CSV is `690090c8…0119d`. The copied old table is `28edd854…29a8`, the
   SHA-256 of `ec6bca8`'s.
3. *The re-pins.* For each one, the log's old and new values match `git diff`. No tolerance is widened: reachability
   d1 keeps 5 × 10⁻⁷ and 10⁻³, d2 keeps 5 × 10⁻³, and the others keep theirs. All 18 of the Addendum's tests are
   accounted for. Two item-(11) classes changed no assertion. The patch `on_the_old_w_table()` redirects only a read
   whose file name is `Xav_EOS_data.csv`. The two surfing tests now read rows that 01 made again with
   `table/surfing_reference.py`. The committed surfing-temperature CSVs are unchanged as records, and on the old
   table that script reproduces them to 3 × 10⁻¹² and 2.1 × 10⁻⁸. `test_history_and_bbn` named its golden by hard
   path, and 01 repointed it; the log records this as D10.
4. *The goldens.* Run by the orchestrator, 23:03–23:04 GMT (one-minute load 9.16 at the start, 6.60 at the end):
   - `test_build_w_table`, `test_golden_history` and `test_golden_reflecting_history`: 20 tests, OK;
   - `test_tuning` (e): OK, 34.9 s;
   - `ObjectFactories` `test_store_fingerprint_golden` and `test_fingerprint_format_guard`: 9 tests, OK.
     `FINGERPRINT_FORMAT` is 7.
   - `compare_goldens.py`, re-run, matches the committed `out/compare_goldens.txt` line for line, apart from its
     header lines. Its last line is "FORM: SAME".
   - `git diff 85b52ac 5947b4f` on the `rr03` records is empty.
5. *The rulings.* Both of the user's rulings are quoted on this board and in log 01 §2.1, and carried out:
   - "Handle it in a separate classifier prompt; re-pin test_10 to 2026.11.0": 01a landed first, and test_10 reads
     2026.11.0;
   - "Go with (D), keep β = 10 on the old table": β = 10, M = 0.5 M_P, α = 0.1 are kept, and the cosmology is built
     on the old table.
6. *The records.*
   - The log has §5.1's sections, and its 24 class labels cover every deviation.
   - The verification document has §1 (1.1–1.8), and points to `electroweak-eos/turning-points/`.
   - `config/version.py`'s 2026.11.0 entry is dated 2026-11-12.
   - Row 01, its items and the Table gate are on the board.
   - No issue was opened, so `OPEN_ISSUES.md` is unchanged.
   - The orchestrator substituted `5947b4f` for "this commit" here and in the log.

### 1.1 Items

| Item | Prompt | State |
|---|---|---|
| The table: U2 (b), 170 rows, built by `tools/build_w_table.py` (`--check` exit 0), SHA-256 `690090c8…0119d` | 01 | ✅ |
| `tools/tests/test_build_w_table.py` (15 tests) | 01 | ✅ |
| `VERSION_LABEL` 2026.11.0 (covers 01a); `FINGERPRINT_FORMAT` 7 | 01 | ✅ |
| 13 tests re-pinned with reasons, tolerances kept; 2 item-(11) classes on the old table (log 01 §2.2, §2.1) | 01 | ✅ |
| Golden records `ee01`, `ee01_tuned`, `ee01_reflecting` beside `rr03`; FORM: SAME | 01 | ✅ |
| `Xav_EOS_spline` docstring; `XavEOS_test.py` dated note | 01 | ✅ |
| `.documents/electroweak-eos-verification.md` §1 | 01 | ✅ |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Resolved | on the old table, suites, goldens and 01's scan bitwise unchanged; on the new table, the early pair dropped wherever 01 saw it (README §8.2). *As ruled by the user 2026-11-12 at 01a's stop:* on the old table no pair dropped down to 100 GeV and everything else bitwise, the late near-stall pairs (two in 36 histories to T_CMB) recorded; on the new table `dropped_drains` too may differ at α = 0 | 01a | ✅ 2026-11-12, as ruled (log 01a §3) |
| Table | the build script regenerates the CSV byte for byte; the class evaluates the new table; every moved pin listed with a reason; the spline's coupling below the first changed row measured | 01 | ✅ 2026-11-12 (log 01 §3.1) |
| Threshold | the EW-capture threshold bracketed to 0.001 in β, beside 3.020 (today) and 3.00285 (notebook) | 02 | ⬜ |
| Effect | every sampled history's change in Y_p, D/H, ΔN_eff, capture temperature and turning points, set beside the figures' resolution | 02 | ⬜ |
| Close | documents, issues, indexes and handover written; suites at their counts | 03 | ⬜ |

## 3. Active and unresolved issues

- **`[01a-surfing-turning-points-keeps-crossings-below-the-solver-tolerance]`** (opened 2026-11-12, by prompt 01a,
  from its check of `surfing_turning_points`, prompt §2). `surfing_turning_points` samples g = 1 + βπ/M_P on each step
  as `observe_turning_points` samples π, and keeps every sign change of g, with no tolerance.
  - *Measured* (log 01a §5, `.documents/electroweak-eos/turning-points/out/surfing_survey.txt`): not in the early
    pair's circumstances, where g ≈ 1 (no crossing within 385–389 GeV in any scanned history). But in the captured
    histories of the scans (β = 10 and 20, M = 10⁻³ and 0.5 M_P, both tables, to T_CMB) it records 22 to 138 crossings,
    most of them with T_J equal to 10⁻⁹ or better, over tens of e-folds: on the old table at β = 20, 71 of 88 within
    10⁻⁹ of 846.17002981 GeV, some 7.5 × 10⁻⁶ apart in N. That is g held at zero on the surfing attractor with its sign
    set by the solver's error. The excursions of g between them were not measured against g's tolerance.
  - *Impact:* `tools/surfing_scan.py` counts them in `n_crossings`. Its capture, largest-maximum and second-dip
    columns read the first crossings, which are resolved. Nothing stored reads it.
  - *Next step:* none proposed; for the user. A rule like 01a's for g would need g's tolerance from π's.

### 3.1 Assigned to this campaign

None. `[00-kicking-function-table-has-no-provenance-in-the-repository]` was proposed for assignment and
left out of scope by the user's ruling on U7 (2026-11-12); it stays on the `review-remediation` board.

## 4. Resolved issues

- **`[01-turning-points-read-below-the-solver-tolerance]`** (opened 2026-11-12, from 01's stop).
  `observe_turning_points` samples π inside each accepted step and records every sign change of the
  interpolant as a turning point, however small. Where π is below the solver's absolute tolerance, the
  interpolant can change sign twice inside a step whose nodes agree in sign, and that is recorded as a
  wall and a plateau although the field never turns.
  - *Measured* (README §8.1): on the new table, 14 of 20 scanned histories started at rest have such a
    pair at T_J = 385–389 GeV, where Σ switches on (golden β = 1.5: π at the nodes −3.97 × 10⁻¹² and
    −1.82 × 10⁻⁹, interior +6.74 × 10⁻¹⁰, atol 10⁻⁸); none on the old table; gone at atol ≤ 10⁻¹².
  - *Impact:* `first_bounce` is unaffected. `turning_points`, the fixed-law drains, the one-loop bounce rows
    (two event rows at the head, every ordinal +2) and the first candidate's nonadiabaticity are.
  - *Next step:* 01a (README §8.2), under U8–U11 as ruled 2026-11-12.
  - **Assigned (2026-11-12):** to this campaign's prompt 01a, by the user's ruling "Handle it in a separate
    classifier prompt".
  - **Closed (2026-11-12) by prompt 01a, `634f141`.** `observe_turning_points` now keeps an interior pair only if
    its excursion exceeds τ = atol + rtol · max(|π_low|, |π_high|) (`resolved_sign_changes`), and counts a dropped
    pair in `IntegrationResult.unresolved_turning_point_pairs`. On the old table, over 01's 36 scanned histories to
    T_CMB, no pair is dropped down to 100 GeV and everything else is bitwise its baseline, but for two late
    near-stall pairs (β = 10 and 20 at M = 10⁻³ M_P, T_J ≈ 24 eV and 0.46 keV, excursions 0.175 τ and 0.95 τ), which
    the user ruled at 01a's stop to be the rule working as ruled; the suites and the three golden records pass
    unchanged. On 01's table the pair is dropped in every scanned history and every
    test integration where it was seen, and nowhere else but one late near-stall pair at β = 20, M = 10⁻³ M_P
    (N = 126.278, 0.59 τ; log 01a §3). *(The orchestrator's review added that exception: the text as committed said
    "nowhere else", which `out/compare_new.txt` does not bear out.)*

## 5. Baselines

The suites as `CLAUDE.md` records them (2026-11-09): CosmologyModels 26, ComputeTargets 665 (219 + 446),
ObjectFactories 163, tools 277, RunDrivers 76. Prompt 01 measures the counts again before it changes
anything.

After 01a (2026-11-12, in its worktree, log 01a §4): CosmologyModels 26, ComputeTargets 684 (219 + 465; 01a's
`test_resolved_turning_points` adds 19), ObjectFactories 163, tools 277, RunDrivers 76. Two of them fail in the worktree
only, before and after, for its path (`test_registry_config`, `test_registered_run`). `CLAUDE.md`'s counts wait for 01.

After 01 (2026-11-12, main checkout, log 01 §3.3): CosmologyModels 26, ComputeTargets 684 (219 + 465), ObjectFactories
163, tools 292 (+15, `test_build_w_table`), RunDrivers 76. `CLAUDE.md` still holds the user's uncommitted edits, so its
counts are not changed here (note 01, ruling 1). They are to be updated once the user has committed those edits.
