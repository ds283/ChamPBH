# Prompt 03 — the fix

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-02, after 01 landed (`d4703df`, reviewed `9d5ca45`) and the campaign was re-planned
at the user's word (`2883c13`, README §6.3). It is written from:
- README §6.3 as ruled: the signature (P1's flag on the first accepted step of a restarted Radau, either
  condition), 02 withdrawn, and U6 — "That's fine. We'll do F3 plus the guard.";
- README §6.2 P5–P9, standing;
- 01's record: `.documents/reflection-restart-verification.md` §1, the log `logs/01-measure-the-fixes.md`
  (§2 deviations 8–13, §7, §8), and the raw output under `var/reflection-restart/01/`.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- on branch `reflection-restart`, at or after the commit that adds this prompt, with no tracked change in
  `git status`; `d4703df` is an ancestor of `HEAD`;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- `var/reflection-restart/01/main.jsonl` and `var/reflection-restart/01/tasks-main.json` exist;
- `.documents/reflection-restart-verification.md` has §1 and no §2.

If any of these fails, stop and ask.

**Closes:** nothing (the assigned issue closes in 04, P10). **Opens:** only what the work finds.

**Recommended model:** **Opus**. A permanent change inside the step loop that must reproduce the probe's F3
bit for bit wherever the guard does not fire, the removal of the probe switch, and a label change that
ripples through the goldens, the driver and the plot scripts.

**Read first:**
1. [`README.md`](README.md): §0.1, §0.3 (F3, F4), §5, §6.2 P5–P10, §6.3 whole.
2. The verification document §1, whole; the log of 01, §2, §7 and §8.
3. `ComputeTargets/ScalarHistory.py` at `d4703df`:
   - the probe switch and its comment (`_PROBE_OUTWARD_FIX` and `_probe_outward_step_flagged`, :235–:285);
   - `IntegrationResult` and `probe_retakes` (:519–:545);
   - in `integrate_scalar_history` (:1490): the switch's reading (:1650–:1663), `new_solver` and
     `restart_first_step` (:1710–:1728), the release restart in `resume_after_release` (:2535–:2550), the
     reflection restart (:2636), the kinematic cap and its probe terms (:2639–:2663), F4's discard and
     retake (:2665–:2712), the drain restart (:2827) and the result (:2864);
   - `integrate_history` (:3987) where it builds the payload (`"accepted_steps"`, :4287), `tune_history`
     (:4594), and `build_extra_data` (:4891);
   - `SCALAR_HISTORY_STEPPER_LABEL` (:196) and its comment.
4. `ComputeTargets/tests/test_probe_outward_fix.py` and `tools/tests/test_outward_step_scan.py`: what 01
   tested, and the windows its tests use (01's log §2, deviations 9 and 10).
5. `tools/outward_step_scan.py`: how a variant is set (:500–:545), the start reasons read from frame locals
   (01's log §2, deviation 8), `classify_step`, and `--variant`.
6. `ComputeTargets/tests/golden_history.py` (its docstring: how a record is regenerated beside the old
   one), `test_golden_history.py`, and in `test_tuning.py` test (e) and `GOLDEN_TUNED_PATH`.
7. `config/version.py` (the history comment above `VERSION_LABEL`) and `config/fingerprint.py` (what moves
   `FINGERPRINT_FORMAT`).
8. Every use of the stepper label: `grep -rn "kinematic-cap" --exclude-dir=venv --exclude-dir=.claude .`
   outside `prompts/`, `science-runs/`, `var/` and dated documents — `main.py` (:1022–:1040),
   `plot_by_beta.py` (:886), `plot_ScalarHistory.py` (:1557), and the tests that name it.

---

## 0. Where this prompt sits

01 measured four fixes; the user ruled F3 plus a guard (U6, README §6.3). This prompt makes that the
production code and removes everything else of 01's probe. 04 then recomputes the 42 failures and closes
the campaign. The whole store is rebuilt later, by the user, after `quanta-diagnostics` 07, whose prompts
start on the tree this prompt leaves (its U1); so this prompt sets the labels the rebuild carries.

What 01 established, which this prompt relies on and re-checks (the Fix gate):
- under the probe's F3, no history of 01's batch (66, the three goldens among them) has a flagged first
  step of a restarted instance, so the guard does not fire on any of them;
- F3 leaves the goldens, and every history without a restart with π > 0, bitwise.

## 1. What is wanted

### 1.1 F3, permanently

At each restart with π > 0 — after a reflection, after a drain, after a release (P7) — the new Radau's
`first_step` is `min(global_max_step, max(h_floor, f·φ/π))` of the restarted state, exactly as the probe's
`restart_first_step` computes it. The history's start (π* = 0) is untouched, and a restart with π ≤ 0 gets
SciPy's own choice, as today.

Put the sizing in one module-level function (for example `_restart_first_step(y, global_max_step, h_floor,
cap_fraction)`), looked up at call time, so that a test can replace it to show what the guard does without
F3 (§1.4). It is not an option of any driver, and no other code reads it.

### 1.2 The guard

After each accepted Radau step, before the step is used for anything (the place F4 sits today: after the
`message` check, before `accepted_steps += 1`):
- **when it applies:** the step is the **first accepted step** of an instance started by a restart with
  π > 0 (reflection, drain or release, or a retake of such a step). On no other step: not the history's
  start, not a later step of the instance, not the parked phase (DOP853).
- **the test:** P1's flag, either condition, as `_probe_outward_step_flagged` defines it today: π_end/π_start
  < ½ with π > 0 at every sample of the step's interpolant, or the interpolant's least φ below
  (1 − f)·φ_start; 50 samples, ends included. Rename it to a production name and keep its definition.
- **when it fires:** the step is discarded — no change to `ts`, `interpolants`, turning points, the gate's
  samples, the ledger, the drain's arming, `accepted_steps` or the budget, as 01's ruling 3 has it — and
  Radau is rebuilt at the step's start (N, state) with `first_step` set to half the discarded h. The retaken
  instance is guarded in the same way. If half the discarded h is below `h_floor` the history fails with a
  `ComputationFailureError` naming the guard, N and h. The discarded step's RHS evaluations stay counted.
- **the count:** `IntegrationResult.restart_retakes` (replacing `probe_retakes`), carried by
  `integrate_history`'s payload as `"restart_retakes"` and stored by `build_extra_data` as the `extra_data`
  key `restart_retakes` **only when it is above 0**, as `steps_rejected_by_exception` is. A history the guard
  never touches therefore stores exactly what it stores today. The count is of the integration whose payload
  is stored (the one the tuning accepts).

This is a planner's choice, open to the orchestrator note: the count goes in `extra_data` and not in a
column, so that no table or inventory spec changes and `FINGERPRINT_FORMAT` stays at 6, leaving 7 to
`quanta-diagnostics` (its U3). It is a diagnostic of the integration, like `number_reflections` and
`steps_rejected_by_exception`, which also live there, and no figure reads it.

### 1.3 The probe removed

Delete `_PROBE_OUTWARD_FIX`, `_PROBE_STIFFNESS_C`, `_PROBE_OUTWARD_FIX_VALUES`, the F1, F2 and F4 code, the
switch's reading, `probe_retake_cap`, and `probe_retakes`. What remains of 01 in `ScalarHistory.py` is F3
and the guard, with comments that say what each is and cite this campaign; nothing says "probe".

`tools/outward_step_scan.py` stays, as a tool on the production code:
- `--variant`, `--stiffness-c` and the code that sets the switch go;
- each record gains `restart_retakes` (from the result) and the count of the ruled signature: first steps
  of restarted instances that carry P1's flag, kept and discarded apart;
- `classify_step` stays, and its test against the production predicate replaces the one against the probe's;
- `--from-store` and the batch stay as they are.

`.documents/reflection-restart/measure/` is 01's record and is not changed. Its scripts set the switch, so
they run only at `d4703df`; the verification document §2 says so.

### 1.4 Tests (P8)

In a new `ComputeTargets/tests/test_restart_fix.py`, replacing `test_probe_outward_fix.py`, which is
deleted with the switch:
- **F3:** a restart with π > 0 gets the first step of §1.1, at each of the three restart kinds, or at as many
  as can be reached in a test's runtime (say which and why, if not all three); a restart with π ≤ 0 and the
  history's start get SciPy's own choice.
- **the guard catches the bad step:** from the state of the diagnosis's failure β = 1.5, M = 10⁻¹², α = 0.05
  (01's window: the pre-reflection state, ending by temperature), with `_restart_first_step` replaced to
  return SciPy's own choice, the guard fires at least once, the history does not cross φ = 0, and
  `restart_retakes` counts the retakes. With the guard's predicate also replaced to return False, the bad
  step is kept and φ < 0 is reached: the breakage the test exists to catch.
- **the guard looks at the first step only:** a flagged step that is not the first of a restarted instance
  is not retaken (01 found such steps at every apex: the ratio condition; the agent chooses the window).
- **the count reaches the store:** `build_extra_data` writes `restart_retakes` when it is above 0 and not
  otherwise.
- every test's docstring states its runtime; each is shown to fail when the thing it guards is broken, in
  the log's breakage record.

### 1.5 The labels (P6)

- `VERSION_LABEL` → `2026.9.0`, with a dated entry in `config/version.py`'s history: what changed (F3 and the
  guard at restarts with π > 0, `extra_data` key `restart_retakes`), that every history that reflects may
  change value, and that a 2026.8.0 store is not to be extended at 2026.9.0.
- `SCALAR_HISTORY_STEPPER_LABEL` → `Radau+kinematic-cap-stepping1`, which is
  `IntegrationSolver(label="Radau+kinematic-cap", stepping=1)`. `main.py` registers and maps that solver.
  `plot_by_beta.py` and `plot_ScalarHistory.py` select histories of **either** label, so that a 2026.8.0
  store stays readable by them as far as it is today; whether such a store opens under 2026.9.0 code is
  recorded as found, not changed. Every test that names the label follows it.
- `FINGERPRINT_FORMAT` is expected to stay at 6: no inventory spec changes. `test_fingerprint_format_guard`
  and `test_store_fingerprint_golden` pass unchanged. If either needs a change, stop and ask
  (`quanta-diagnostics` U3 takes format 7).

### 1.6 The golden histories (P5)

The stepper label is a field of the golden record, so the goldens are regenerated, beside the old files
and never over them (`golden_history.py`'s rule):
- the two α = 0 histories as `data/golden_history_rr03.json`, and the tuned history as
  `data/golden_history_rr03_tuned.json`; `GOLDEN_PATH` and `GOLDEN_TUNED_PATH` point at them;
  `golden_history_05.json` and `golden_history_06_tuned.json` stay as the records of everything before this
  prompt;
- the record gains `restart_retakes`; it is 0 in all three;
- **every other field of the new records equals the old one**, `solver_label` excepted. Show it with a
  field-by-field comparison script in `.documents/reflection-restart/fix/`, its output in `out/`. A field
  that differs is a stop.
- **the reflecting golden** (P5): β = 1.5, M = 10⁻¹², α = 0.05, tuned, the diagnosis's first failure, in the
  tuned record's form, as `data/golden_history_rr03_reflecting.json` (P5 named it `golden_history_rr03.json`;
  the name moves because the regenerated α = 0 record takes it), with a test that passes it bitwise. It
  records its `restart_retakes` (expected 0) and its reflection count.

### 1.7 The Fix gate (README §2.1)

1. **The three failures succeed**: β = 1.5, M = 10⁻¹², α = 0.05; β = 1.14, M = 10⁻¹⁵, α = 0.01; β = 1.09,
   M = 10⁻¹⁵, α = 0.03; tuned, through the scanner. Each record's outcome, steps and `restart_retakes`.
2. **The fixed code reproduces 01's F3, bit for bit.** Re-run every task of
   `var/reflection-restart/01/tasks-main.json` that 01 ran under F3 (66 tasks: 63 histories and the three goldens), on
   the production code, as one detached batch (`--jobs 6`, `--timeout 1800`), written under
   `var/reflection-restart/03/`. For each, against 01's F3 line in `main.jsonl`: the outcome,
   `samples_sha256`, `log_one_plus_z_sha256`, `accepted_steps`, `RHS_evaluations`, the reflection count and
   (for the store's histories) the store comparison's verdict, all equal; `restart_retakes` 0. A difference
   is a stop, with the history and the first field that differs.
3. **No signature step** (the ruled signature) is kept in any of them, and in particular in the nine
   histories of the diagnosis.
4. **The goldens** as §1.6: the α = 0 records equal to the old in every field but `solver_label`, and the
   reflecting golden pinned.

## 2. What is not wanted

- **No other change to the step control**: the kinematic cap's terms, `cap_fraction`, `h_floor`,
  `global_max_step`, the floor rule, the parking cut, the gate's sampling, the drain rule. F1 and F2 are not
  kept in any form.
- **No store is written** but temporary ones in tests. The run's stores are read only, as 01's scanner reads
  them. No Ray.
- **No schema change**: no table, no column, no inventory spec. `FINGERPRINT_FORMAT` stays at 6.
- The 42 failures beyond the three, the run record (`science-runs/`), the diagnosis folder's pointer and the
  handover are 04's.
- No change to `.documents/reflection-restart/measure/` or `out/` of 01, or to any old golden file.
- No figure.

## 3. Verification

- The suites, before you change anything and after, per `CLAUDE.md` (ComputeTargets in two halves; tools
  alone): record both sets of counts. **A count may fall only by the tests deleted with the probe** (the
  module `test_probe_outward_fix.py`, and those of `test_outward_step_scan.py` that exercise `--variant`),
  each named in the log, with the tests that replace them. Any other fall is a stop.
- The golden histories and the fingerprint at the new paths, and the reflecting golden.
- The Fix gate, §1.7, every item.
- The breakage record: each new test, and the bitwise replay of §1.7 item 2, shown to fail when what it
  guards is broken (for the replay: run one history with F3's step halved and see it reported).
- `black` on the files you changed.
- Do not edit `ScalarHistory.py` or the scanner while §1.7's batch runs: each worker imports them when it
  starts (01's log §8).

## 4. The commit, the log and the board

One commit (invariant 1), with:
- `ComputeTargets/ScalarHistory.py`; `ComputeTargets/tests/test_restart_fix.py` (new) and
  `test_probe_outward_fix.py` (deleted); the tests that name the label;
- `tools/outward_step_scan.py` and its tests;
- `config/version.py`; `main.py`; `plot_by_beta.py`; `plot_ScalarHistory.py`;
- the three new golden files, `golden_history.py`, `test_golden_history.py`, `test_tuning.py` (paths only),
  and the reflecting golden's test;
- `.documents/reflection-restart/fix/` and its `out/` (text only); the verification document §2, added, not
  over §1 (invariant 6): the Fix gate item by item, the golden comparison, the replay's table, the
  breakage record, and the note that 01's `measure/` scripts run only at `d4703df`;
- `.documents/numerical-strategies.md`: a dated note on the restart's first step and the guard, beside the
  kinematic cap; `.documents/architecture-summary.md`: `IntegrationResult.restart_retakes`, the payload key
  and the `extra_data` key, as a dated note;
- the log `logs/03-the-fix.md` (template: `dk-and-rpk-migration` README §5.1), with every deviation
  classified, "Observations not acted on", and "State handed to the next prompt": the scanner's command for
  04's 42, its expected throughput, and anything 04 must know;
- the board: 03's row, its items, the Fix gate, §3/§4 for any issue opened, with `.documents/OPEN_ISSUES.md`
  in the same commit;
- `CLAUDE.md`'s test counts.

Stage by explicit path. The commit message follows `CLAUDE.md` invariant 5. Write "this commit" where the
SHA goes; the orchestrator substitutes it.
