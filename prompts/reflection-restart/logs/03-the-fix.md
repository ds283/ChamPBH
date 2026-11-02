# Log 03 — the fix

**Subject:** Implement F3 and the guard at a restart with π > 0, and remove the probe
**Commit:** `b81eab8`, on branch `reflection-restart`, on top of `89a0760`
**Date:** 2026-11-02, by Claude Opus 5.5
**Result:** **COMPLETE, after one stop.** F3 and the guard are the production code, the probe switch is gone, the
labels moved (`VERSION_LABEL` 2026.9.0, `Radau+kinematic-cap-stepping1`, `FINGERPRINT_FORMAT` still 6), and the Fix gate
holds. The suites after the change failed in `test_release.py` (F3 past a window's bound; a re-pin the drains need);
the agent stopped, and the user ruled "Please allow both" (D16, D17).
- The three failures succeed (7183, 49 765 and 27 665 accepted steps, no retake). The replay of 01's 66 F3 tasks on the
  production code is equal to 01's F3 in every field compared, in all 66; the guard never fired; no step carries the
  ruled signature.
- The regenerated golden records equal the old ones in every field but `solver_label` and the added
  `restart_retakes` = 0; the reflecting golden (β 1.5, M 10⁻¹², α 0.05) is pinned.
- Suites: CosmologyModels 26, ComputeTargets 514 (122 + 392), ObjectFactories 130, tools 147, RunDrivers 68, all
  passing (before: 26, 508 (122 + 386), 130, 145, 68). ComputeTargets loses `test_probe_outward_fix.py`'s 10 and gains
  `test_restart_fix.py`'s 14 and `test_golden_reflecting_history.py`'s 2; tools gains 2.

The orchestrator's note for this prompt (its Gate, §0, eight rulings and the conventions, copied to the session
scratchpad as `rr03-orchestrator-section0.md`) governed where it differs from the prompt; each ruling is classified in §2.

## 1. What shipped

- **F3 and the guard** in `ComputeTargets/ScalarHistory.py` (prompt §1.1, §1.2):
  - module level: the comment above them (`:239`–`:256`), `OUTWARD_FLAG_SAMPLES` = 50 (`:258`),
    `_restart_first_step(y, global_max_step, h_floor, cap_fraction)` (`:261`; `None` where π ≤ 0) and
    `_outward_step_flagged(…)` (`:274`; P1's flag, 01's `_probe_outward_step_flagged` renamed, its body unchanged);
    `IntegrationResult.restart_retakes` (`:554`, in place of `probe_retakes`, default 0);
  - in `integrate_scalar_history` (`:1502`): `guard_first_step` and `restart_retakes` (`:1671`–`:1672`);
    `new_solver(N, y, restart=False, retake_step=None)` (`:1719`), which sets the first step (F3's, clamped to the
    solver's bound at `:1739`, D16) and arms or clears the guard; the three restarts call it with `restart=True`: the
    release in `resume_after_release` (`:2572`), the reflection (`:2658`), the drain (`:2842`); the guard after the
    `message` check (`:2700`–`:2728`: armed test `:2705`, the count `:2725`, the retake `:2727`), before `accepted_steps += 1` (`:2730`);
    the result (`:2878`);
  - the payload's `"restart_retakes"` (`:4305`) and `build_extra_data`'s key, only above 0 (`:4932`–`:4934`);
  - the probe removed: `_PROBE_OUTWARD_FIX`, `_PROBE_STIFFNESS_C`, `_PROBE_OUTWARD_FIX_VALUES`, `_PROBE_FLAG_SAMPLES`,
    the switch's reading, F1's and F2's cap terms, F4's discard and `probe_retake_cap`, `restart_first_step`,
    `probe_retakes`. `grep -n "probe\|PROBE"` finds only the Jacobian's "probe" (`:507`, `:694`, `:2684`).
- **The labels** (prompt §1.5): `SCALAR_HISTORY_STEPPER_LABEL = "Radau+kinematic-cap-stepping1"` (`:200`) with its
  comment; `config/version.py` `VERSION_LABEL = "2026.9.0"` with a dated entry; `main.py` registers and maps
  `IntegrationSolver(label="Radau+kinematic-cap", stepping=1)` beside stepping 0 (`:1020`, `:1034`, `:1045`);
  `plot_by_beta.py:886` and `plot_ScalarHistory.py:1557` select stepping 1.
- **The scanner** `tools/outward_step_scan.py` on the production code (prompt §1.3): `--variant`, `--stiffness-c` and
  the switch's code gone; a task naming a variant refused; each record's `restart_retakes`, each integration's, each
  row's `signature`, and `signature_kept` / `signature_discarded` in "scan". `classify_step`, `--from-store` and the
  batch unchanged.
- **Tests:** `ComputeTargets/tests/test_restart_fix.py` (14, new), `test_golden_reflecting_history.py` (2, new);
  `test_probe_outward_fix.py` (10) deleted; `tools/tests/test_outward_step_scan.py` 22 → 24.
- **The goldens** (prompt §1.6, ruling 3): `data/golden_history_rr03.json`, `golden_history_rr03_tuned.json`,
  `golden_history_rr03_reflecting.json` (new); `golden_history.py` (path, `record_of`'s `restart_retakes`, the
  provenance text), `golden_tuned_history.py` (path, provenance text), `golden_reflecting_history.py` (new).
- **The tests that name the label or pin the version** follow it: `test_kinematic_cap_loop.py`,
  `ObjectFactories/tests/test_dissipation_store.py`, `test_first_bounce_round_trip.py`,
  `test_fixed_T_values_round_trip.py`, `test_scalarhistory_failure_reason.py`, `test_tuned_history_round_trip.py`;
  `test_tuning.py`'s docstring names the new record. `test_release.py`'s `BETA_5_FAILED_CHECK_N` is re-pinned (D17).
- **The record:** `.documents/reflection-restart/fix/` (`compare_goldens.py`, `replay_tasks.py`, `replay_compare.py`) and
  `fix/out/` (`compare_goldens.txt`, `replay.txt`, `replay-breakage.txt`); `.documents/reflection-restart-verification.md`
  §2 (new); dated notes in `.documents/numerical-strategies.md` (§3.5.3) and `.documents/architecture-summary.md`
  (§7.2); the board; `CLAUDE.md`'s test counts; this log.

## 2. Deviations from the prompt, and the rulings of note 03

1. **`STRUCTURALLY REQUIRED`: the replay** (ruling 1). `fix/replay_tasks.py` writes 01's 66 F3 specs with `"variant"`
   removed and `" variant=F3"` taken out of each key. Each record is compared with 01's F3 line: the eight fields, the
   store comparison against **01's F3 comparison** (not bitwise against the store, which F3 moves), the golden hook's
   verdict on the new records, `restart_retakes` and the signature. 66 of 66 equal (§3).
2. **`STRUCTURALLY REQUIRED`: the plot scripts select stepping 1 only** (ruling 2), replacing the prompt's "either
   label". `config/version.py`'s 2026.9.0 entry says lookups are keyed on the version and never fall back, so a
   2026.8.0 store is read with its own tree. Found as the note says; not changed.
3. **`IMPLEMENTATION CHOICE` (ruled): the goldens** (ruling 3). The paths move in `golden_history.py` and
   `golden_tuned_history.py`; each module's own regeneration wrote the new file; no old file was opened for writing.
   `compare_goldens.py` allows exactly `solver_label`'s value and the added `restart_retakes` (0). The reflecting golden
   is `golden_reflecting_history.py` on `golden_tuned_history`'s builders, with its own path, command and test. *My
   choice within it:* the record adds a block `"reflecting"` (reflections, restart_retakes, accepted_steps, injections)
   beside `history` and `tuning`, compared exactly; the first three repeat the history's record, the injections do not.
4. **As the prompt says, made exact: the counts** (ruling 4). ComputeTargets falls by `test_probe_outward_fix.py`'s 10
   and gains 16 (`test_restart_fix.py` 14, `test_golden_reflecting_history.py` 2). tools does not fall:
   `test_observe_restores_the_module` is kept without the switch (Radau and `integrate_scalar_history` restored, SciPy's
   class untouched), the two predicate tests are on `_outward_step_flagged` (their class renamed
   `TestTheFlagAgreesWithTheGuard`), and two tests are added. Every other count is unchanged.
5. **`IMPLEMENTATION CHOICE` (ruled): the guard's bookkeeping** (ruling 5). `new_solver` is the one place that arms:
   `restart=True` with π > 0 (reflection, drain, release) or a `retake_step` sets `guard_first_step`; any other call (the
   start, a restart with π ≤ 0) clears it. The guard clears it on the first accepted step, whether it passes or is
   retaken. The retake is `new_solver(N, y_retake, retake_step=h/2)` from the loop's own frame with the local
   `y_retake`, so the scanner's frame reading still names it "retake". A discard leaves only the count and the RHS
   evaluations. The retake is not given F3's first step: h/2, as the prompt says.
6. **`IMPLEMENTATION CHOICE` (ruled), and one addition: the guard's tests** (ruling 6). The constants and the recorded
   state moved from `test_probe_outward_fix.py`. Without F3 the β = 1.5 window discards a step of h = `BAD_STEP_H`
   bitwise, retakes it at h/2 from the same state, and succeeds with φ > 0 on every kept step; with the predicate also
   replaced it fails with the history's m_eff² failure. "First step only" is shown on P1 at M = 0.5 to N = 21 (the
   flagged step after 20.8 kept, the predicate never called) **and**, added, on the β = 1.5 window run to
   ln T_J − 10⁻² under F3 (01's deviation 10: a later ratio-flagged step of the *restarted* instance is kept, and the
   predicate is called once). The P1 window has no restart, so it cannot see a guard that is armed but never cleared;
   B4 is caught only by the addition.
7. **As the prompt says: the payload's key** (ruling 7). No test pins the payload's key set. The tests that pin
   `build_extra_data`'s keys (`test_kinematic_cap_loop` test (g), `test_reflection_reporting`) pass unchanged: their
   stand-in payloads have no `restart_retakes`, which `build_extra_data` reads with `.get`.
8. **`STRUCTURALLY REQUIRED`: the files** (ruling 8). The commit holds prompt §4's list, `golden_tuned_history.py`,
   `golden_reflecting_history.py` and its test, and the tests that name the stepper label (§1). §4.6 shows the staged
   list.
9. **`STRUCTURALLY REQUIRED`: `test_dissipation_store.py` pinned `VERSION_LABEL` at 2026.8.0** (its test 10, beside
   `FINGERPRINT_FORMAT` 6). It now pins 2026.9.0, with a comment; its format assertions are untouched. The file is in
   ruling 8's set already for the stepper label; the version pin was not foreseen by the prompt or the note. Not a
   fingerprint test, and `FINGERPRINT_FORMAT` did not move, so not a stop.
10. **`IMPLEMENTATION CHOICE`: F3 at each restart kind in the tests.** The reflection on 01's window; the drain and the
    release on `test_release`'s history M = 0.5 M_P, β = 5, α = 0.1 (about 7 s here), which drains at its 105 candidates
    before the one that parks it (candidate 105), is released at N = 40.2545 with π > 0, and drains at 15 candidates
    after: 121 restarts (120 drains and the release), all with π > 0, each with F3's first step. No history run here has a restart with π ≤ 0, so that branch is tested on `_restart_first_step` alone.
11. **`IMPLEMENTATION CHOICE`: the scanner refuses a task that names a variant** (`"variant"` or `"stiffness_c"` other
    than `None`) with a `ValueError` naming `d4703df`, rather than silently running the production code; 01's None
    tasks, which carry `"variant": None`, still run.
12. **`IMPLEMENTATION CHOICE`: `main.py` keeps stepping 0 registered** beside stepping 1, as the older labels stay
    registered, so that the registration of a store's solvers does not change for an old one.
13. **`UNINTENDED DRIFT`, corrected: the scanner was edited before the baseline suites ended.** While the
    `ComputeTargets` `[g-z]` baseline ran I edited `tools/outward_step_scan.py` and its test (the suite does not import
    them). Before the `tools` baseline started I copied both to the scratchpad and restored them from `HEAD`
    (`git checkout`); the baseline `tools` run (14:45) was on `HEAD`'s files, and they were put back after it.
    `test_restart_fix.py` and the reflecting golden's files were written during the `[g-z]` and `[a-f]` baselines;
    discovery had run (386 = 01's count) or the pattern excludes them. No baseline count is affected.
14. **`UNINTENDED DRIFT`, corrected: `replay_compare.py`'s first two runs matched keys wrongly**: first by appending
    `" variant=F3"` (the goldens' keys hold it in the middle), then by a substring that also matched `variant=F3+F4`.
    Both reported missing or duplicate lines, never a field. The script now takes the token exactly; no record changed.
15. **`UNINTENDED DRIFT`, corrected: breakage B7's first form did not end** (§6): the bound at h_floor/1000 lies below
    SciPy's least step at N ≈ 36, so with the predicate forced true the retakes looped. Killed after about 9 min (it
    was a test's subprocess; the file was restored by the harness, SHA-256 checked) and re-run at h_floor/10.
16. **`STRUCTURALLY REQUIRED`, ruled by the user: F3 clamped to the solver's bound.** The suites after the change
    failed in `test_release.TestTheEnds.setUpClass` with SciPy's `ValueError: first_step exceeds bounds`: that history
    (M = 0.5 M_P, β = 5, α = 0.1) runs to `N_stop = N_rel + 0.01`, and the release's restart got F3's 0.1. SciPy refuses a
    first step past the solver's bound, so F3 as the probe defined it crashes at a restart closer to the bound than its
    step. The agent stopped; the user ruled, of this and D17, "Please allow both". `new_solver` now takes
    `first_step = min(first_step, N_bound − N)` when it is not `None`. **Where F3's value fits inside the bound this
    changes nothing**: every history with `N_stop = None` (`N_bound = N_failsafe = 1000`), so every replay task, the
    three failures and the goldens, all run before the clamp, stand without a re-run (the orchestrator's instruction).
    Test: `TestF3IsClampedToTheBound.test_a_restart_near_n_stop`; breakage B13 (no clamp) is caught.
17. **`STRUCTURALLY REQUIRED`, ruled by the user: `test_release.py`'s `BETA_5_FAILED_CHECK_N` re-pinned, 40.29089 →
    40.29068.** The same history drains at 120 restarts with π > 0 before and after its release, F3 sets each one's
    first step, and the history before the switch moves; the parked step grid moves with it by about 2.1×10⁻⁴, and
    the failing check after the release with it. Its comment says so, and that inelastic-reflection log 06's value
    stays the record of the old code. `test_release.py` joins ruling 8's files, at the user's ruling ("Please allow
    both").

## 3. The Fix gate

- **Item 1** (verification §2.1): β 1.5, 10⁻¹², α 0.05: success, 7183 steps, 32 reflections, `restart_retakes` 0;
  β 1.14, 10⁻¹⁵, α 0.01: success, 49 765, 284, 0; β 1.09, 10⁻¹⁵, α 0.03: success, 27 665, 153, 0. One integration each.
- **Item 2** (verification §2.2; `fix/out/replay.txt`): 66 tasks — 9 tuned, 54 store, 3 golden — **66 equal** in
  `outcome`, `samples_sha256`, `log_one_plus_z_sha256`, `n_samples`, `accepted_steps`, `RHS_evaluations`,
  `compute_steps`, `reflections`, the store comparison (54 of 54 equal to 01's F3) and the golden hook (3 of 3
  bitwise); `restart_retakes` 0 in all.
- **Item 3**: the ruled signature kept 0, discarded 0, over the 66 (the nine of the diagnosis among them) and the
  three runs of item 1.
- **Item 4** (verification §2.4; `fix/out/compare_goldens.txt`): "differences not allowed: 0"; the reflecting golden
  pinned (32 reflections, 7183 steps, `restart_retakes` 0).

## 4. Verification performed

### 4.1 The suites

Serially, from the repository root, ComputeTargets in its two halves, each start with `uptime` (`run_suites.sh` in the
session scratchpad's `rr03/`).

| suite | before (14:31–14:48) | s | load at start | after (15:12–15:28) | s | load at start | final, after D16–D17 (15:34–15:49) | s | load at start |
|---|---|---|---|---|---|---|---|---|---|
| CosmologyModels | 26 OK | 65 | 6.99 | 26 OK | 68 | 5.39 | — (imports no changed module) | | |
| ComputeTargets `test_[a-f]` | 122 OK | 133 | 4.62 | 122 OK | 135 | 8.24 | 122 OK | 133 | 2.57 |
| ComputeTargets `test_[g-z]` | 386 OK | 461 | 5.00 | 391 found, 2 failing (D16, D17) | 465 | 4.99 | 392 OK | 471 | 4.00 |
| ObjectFactories | 130 OK | 82 | 3.95 | 130 OK | 86 | 2.85 | 130 OK | 89 | 2.82 |
| tools | 145 OK | 162 | 4.79 | 147 OK | 168 | 7.71 | 147 OK | 159 | 23.04 |
| RunDrivers | 68 OK | 25 | 3.80 | 68 OK | 29 | 7.68 | 68 OK | 26 | 31.77 |

No count fell beyond ruling 4. ComputeTargets: −10 (`test_probe_outward_fix.py`: `test_None_reproduces_the_bad_step`,
`test_F1`, `test_F2`, `test_F3`, `test_F4`, `test_F3_and_F4`, `test_the_defaults`, `test_an_unknown_value_raises`,
`test_F3_without_an_outward_restart`, `test_F4_where_nothing_is_flagged`), +14 (`test_restart_fix.py`:
`test_the_formula_and_its_clamps`, `test_none_where_pi_is_not_positive`, `test_the_restarted_first_step`,
`test_every_restart`, `test_the_drains_and_the_release_are_restarts_with_pi_positive`, `test_a_restart_near_n_stop`,
`test_the_bad_step_is_discarded_and_retaken`, `test_without_the_guard_the_bad_step_is_kept`,
`test_a_retake_below_h_floor_fails_the_history`, `test_a_later_flagged_step_of_a_restarted_instance_is_kept`,
`test_no_restart_no_guard`, `test_extra_data_holds_the_count_only_above_0`, `test_the_payload_carries_the_count`,
`test_the_count_is_that_of_the_retakes`), +2 (`test_golden_reflecting_history.py`:
`test_it_succeeds_and_is_the_record_bitwise`, `test_the_record_reflects_and_the_guard_never_fired`). tools: +2
(`test_a_discarded_first_step_is_a_discarded_signature_step`, `test_a_task_that_names_a_probe_variant_is_refused`);
none deleted. The `[g-z]` "after" run is the one that stopped the prompt: 391 discovered, `TestTheEnds.setUpClass` an
error (its 4 tests not run, so "Ran 387") and one failure; the final run is after the user's ruling and the added test.
The final RunDrivers and tools runs began at a one-minute load of 23–32 from other work on the machine.

### 4.2 The goldens and the fingerprint

`test_golden_history`, `test_golden_reflecting_history`, `test_kinematic_cap_loop`, `test_reflection_reporting`,
`test_store_fingerprint_golden`, `test_fingerprint_format_guard` and the five ObjectFactories tests that name the label:
71 run in 69 s, one failure, `test_dissipation_store`'s version pin (D9), which then passed. The fingerprint tests passed
unchanged. `test_tuning` (e) passed in the suites.

### 4.3 The new tests

`ComputeTargets.tests.test_restart_fix`: 13 OK in 6.7 s (load about 6) before the stop; after D16 and D17,
`test_restart_fix` and `test_release` together 33 OK in 52 s (14 + 19). `tools.tests.test_outward_step_scan`: 24 OK in
27 s. `test_golden_reflecting_history`: 2 OK.

### 4.4 The replay

Launched detached 14:54:38 UTC (`nohup`, pid 28532 in `replay.jsonl.pid`; load 5.42 / 5.42 / 5.35), checked once
(alive, the log's `** scan start: 66 tasks`), waited on by a background command that ended with the pid; `** scan done`
15:00:43 (load 5.23 / 6.94 / 6.28). 66 records, no timeout, no error. `ScalarHistory.py` and the scanner had the same
SHA-256 before and after (`edf2ae7d…`, `f856437c…`). Nothing else ran past a turn.

### 4.5 `black`

`black` 26.1.0 on every Python file changed or added (23, the three `fix/` scripts included); `black --check` clean. `test_release.py` was black-clean before its one edit.

### 4.6 The staged files

`git diff --cached --name-status` before the commit, 36 files, staged by explicit path (ruling 8's list, with `test_release.py` at the user's ruling, D17):

    M	.documents/architecture-summary.md
    M	.documents/numerical-strategies.md
    M	.documents/reflection-restart-verification.md
    A	.documents/reflection-restart/fix/compare_goldens.py
    A	.documents/reflection-restart/fix/out/compare_goldens.txt
    A	.documents/reflection-restart/fix/out/replay-breakage.txt
    A	.documents/reflection-restart/fix/out/replay.txt
    A	.documents/reflection-restart/fix/replay_compare.py
    A	.documents/reflection-restart/fix/replay_tasks.py
    M	CLAUDE.md
    M	ComputeTargets/ScalarHistory.py
    A	ComputeTargets/tests/data/golden_history_rr03.json
    A	ComputeTargets/tests/data/golden_history_rr03_reflecting.json
    A	ComputeTargets/tests/data/golden_history_rr03_tuned.json
    M	ComputeTargets/tests/golden_history.py
    A	ComputeTargets/tests/golden_reflecting_history.py
    M	ComputeTargets/tests/golden_tuned_history.py
    A	ComputeTargets/tests/test_golden_reflecting_history.py
    M	ComputeTargets/tests/test_kinematic_cap_loop.py
    D	ComputeTargets/tests/test_probe_outward_fix.py
    M	ComputeTargets/tests/test_release.py
    A	ComputeTargets/tests/test_restart_fix.py
    M	ComputeTargets/tests/test_tuning.py
    M	ObjectFactories/tests/test_dissipation_store.py
    M	ObjectFactories/tests/test_first_bounce_round_trip.py
    M	ObjectFactories/tests/test_fixed_T_values_round_trip.py
    M	ObjectFactories/tests/test_scalarhistory_failure_reason.py
    M	ObjectFactories/tests/test_tuned_history_round_trip.py
    M	config/version.py
    M	main.py
    M	plot_ScalarHistory.py
    M	plot_by_beta.py
    M	prompts/reflection-restart/IMPLEMENTATION_STATE.md
    A	prompts/reflection-restart/logs/03-the-fix.md
    M	tools/outward_step_scan.py
    M	tools/tests/test_outward_step_scan.py

Nothing under `var/` is committed; no old golden record changed.

## 5. The measurement, in short

The production code is 01's F3 bit for bit on all 66 histories 01 ran, and the guard does not fire on any of them or
on the three failures: it is a guard, not a change of values, where F3 already removes the bad step. Every history
without a restart with π > 0 is bitwise what it was but for the stepper label. The replay took 6 min at six workers.

## 6. The deliberate-breakage record

Verification §2.5, with the commands in the scratchpad's `rr03/mutate.py`: B1–B13 (`ScalarHistory.py`; B13, the clamp
removed, after the user's ruling), T1–T4 (the
scanner), each caught by the test named there; R, the replay with F3's first step halved on one history, reported as
different in `samples_sha256`, `RHS_evaluations` and `compute_steps` (`fix/out/replay-breakage.txt`).

## 7. Observations not acted on

- **No history run by this prompt or by 01 restarts after a drain or a release** except `test_release`'s at
  M = 0.5 M_P (the unit test of D10). The 66 replay histories and the three failures restart only after reflections
  (13 700 instances). F3 and the guard at drains and releases are therefore exercised by the tests and not by a
  measurement over histories; histories at large M with α > 0 (the run's X block) have them. 04 recomputes the 42, all
  at M ≤ 10⁻¹², which do not either.
- **`tools/tests/test_history_and_bbn.py` reads the old tuned record by its path** (`GOLDEN`, `:44`), not through the
  module. It passes, since every value it reads is equal in the new record; it is outside ruling 8's files.
  `ObjectFactories/tests/test_tuned_history_round_trip.py`'s docstring also names `golden_history_06_tuned.json`
  (`:22`); its code reads the module's path.
- **The guard's termination relies on SciPy's least step being below `h_floor`** (10 ulp of N; below 10⁻¹¹ for N < 4096,
  far past `N_failsafe` = 1000). Were `h_floor` lowered below 10 ulp of the N reached, a retake would be clamped back up
  and could repeat forever (B7's first form showed it). Not reachable with today's `StepControl`.
- **The reflecting golden's tuning prints a margin warning** ("the spectrum's peak is not relativistic at a later
  bounce", worst ln(p/m) = −0.2183 at N = 27.279): a property of the history, as for 01's runs of it.
- **`prompts/INDEX.md`'s row** for this campaign still reads "1 of 3 landed"; the file is outside ruling 8's list, and
  is left for the orchestrator.
- **The 42 failures' normalization is NULL in the store** (they failed in the tuning's seed integration), so the
  scanner's `--from-store` specs, which run at a stored normalization, cannot run them as they are (§8).

## 8. State handed to the next prompt

- **The tree:** `VERSION_LABEL` 2026.9.0; stepper `Radau+kinematic-cap-stepping1`; `FINGERPRINT_FORMAT` 6;
  `IntegrationResult.restart_retakes`, payload `"restart_retakes"`, `extra_data` `restart_retakes` (only above 0).
- **The 42** are the `log_m_eff_squared` failures of `var/datastores/science-2026.8.0`, all in block C: 31 at
  M = 10⁻¹², 10 at 10⁻¹⁵, 1 at 10⁻¹³; α 0.01 (8), 0.02 (2), 0.03 (7), 0.05 (10), 0.08 (3), 0.1 (5), 0.15 (7). Their
  stored normalization and φ_ref are NULL, so `--from-store` cannot run them as stored. The route: build a task list
  from `select_from_store("var/datastores/science-2026.8.0", blocks=["C"])`, keep the specs whose stored
  `failure_reason` contains `log_m_eff_squared`, set `"mode": "tuned"` and drop `normalization`, `phi_ref_Mp` and
  `store` (keeping β, M, α, `phi_init_Mp`, `T_init_GeV`, `atol`, `rtol`), and run

      PYTHONPATH=. ./venv/bin/python tools/outward_step_scan.py batch \
          --tasks var/reflection-restart/04/tasks-42.json \
          --output var/reflection-restart/04/the-42.jsonl --jobs 6 --timeout 1800

  detached, as the replay was. Each record's `outcome`, `accepted_steps`, `restart_retakes` and
  `scan.signature_kept` / `signature_discarded` are the result; the store is read only for the inputs.
- **Throughput:** tuned at these M, one task per history, 5–25 s each at a load of about 7 (§3, item 1), several
  integrations if the tuning needs them; the slowest histories of 01 and of the replay were at α = 0.001, which the 42 do
  not include. Expected a few minutes at six workers.
- **Do not edit `ComputeTargets/ScalarHistory.py` or the scanner while a batch runs** (01's log §8).
- 01's `measure/` scripts and task lists run only at `d4703df`; the scanner refuses a task that names a variant.
