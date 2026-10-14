# Orchestrator — prompt 02, surfing turning points and the driver

Read [`../README.md`](../README.md) first: §0.2, §0.3 (with the U5 addendum), §2.2 (02's charter),
§4 (the CSV), §5, §6.1 (U1–U3, U5) and §6.2 (P1–P3, P6, P7, P9). Then the board's §5.2, whose
"For the next notes" this note carries. The conventions are note 01's
([`prompt-01.md`](prompt-01.md), "Conventions"), restated here where 02 needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- run the agent's tool into the scratchpad;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`02-surfing-turning-points-and-driver.md`](../02-surfing-turning-points-and-driver.md)
**Board items:** T1–T5 · **Gate:** "Turning points" · **Closes:** nothing · **Opens:** only what the
work finds · **Model:** Opus, as the prompt recommends · **No network.**

**Gate:**
- 01 has landed (`2c98370`) and been reviewed (`b5d1c94`). `HEAD` is the commit that adds this
  note, or a later one that touches no code. Since `a8823a9`, only 01's commit has touched anything
  outside `prompts/`.
- One prompt at a time in this checkout. 03 is not dispatched until 02 is reviewed.

## 0. What makes this prompt unusual

**01 has landed, so T3's fallback does not apply.** `summarise` labels the capture with
`find_kicks`'s windows and reads its kick's roots from `reachability_roots`. It need not accept
`kicks=None`. If it does, the log says so. From board §5.2:
- **The table's ends** are reachable only through the private `cosmology._eos._T_min` and
  `_T_max`, in GeV. `find_kicks(cosmology, cosmology._eos._T_min * units.GeV,
  cosmology._eos._T_max * units.GeV)` is the call, made once per run, not once per β.
- **The keys differ.** A kick from `find_kicks` has `label`, `T_peak`, `Sigma_peak`,
  `T_window_hot`, `T_window_cold`, … A record from `reachability_roots` has `kick`, `beta`,
  `T_hot`, `T_cold`, `slope_hot`, `slope_cold`, `merged`, `multiple_crossings`. Temperatures in
  both are in the cosmology's units, not GeV.
- **A merged interval.** For a QCD capture with β > 2.154, the QCD record has `merged = "mu-pi"`
  and its `T_cold` is the merged interval's cold edge: 31.1 MeV at β = 2.2, and 25.7 MeV at 2.5.
  This is U5's ruling, and `T_cold_GeV` takes it as it stands.

**The cost is not what the prompt feared.** The orchestrator's probe (below) ran all 79 β of the
scan to T_stop = 1 MeV in **254 s in all**. The longest history takes 8.5 s (β = 5.0), and the
shortest 0.3 s. So:
- T5's projection will be a few minutes against P7's 60. The prompt's §4 stop on the budget is not
  expected to fire.
- T4's real history at β = 2 takes **0.8 s** to the default T_stop of 1 MeV. That is below its first
  bounce (0.7467 GeV), so it satisfies "a T_stop just below its first bounce" as it stands. A higher
  T_stop is the agent's choice, recorded in the log either way.
- 04's chunking is moot: one chunk fits easily under the cap. That is for 04's note, not for 02.

**Uncaptured histories still have a delivery.** At β = 1.10 (below the QCD threshold) and at
β = **3.00** there are no crossings before the first bounce. Both histories still bounce, at
143 MeV and at 1.68 GeV. At β = 3.00, just below the EW threshold (3.0118), the field reaches the
wall at 1.68 GeV, before the QCD kick at 0.18 GeV.
- **The ruling for 02.** Delivery is `first_bounce`'s T_J (U2), and it exists. So `T_deliver_GeV`,
  `N_deliver` and `deliver_reflected` are filled for an uncaptured row, and `captured=false` tells
  the reader the history was not captured.
- **Not a stop.** The prompt's §4 stop is "a captured first episode with no bounce above T_stop",
  and neither of these histories is captured. They are measurements. β = 3.00 is not one of T5's
  five, so the agent may never meet it. It is here so that 03's and 04's notes can carry it.

**The last crossing is often the wall's.** At 32 of the 77 captured β, the last `"max"` crossing
lies within 10⁻⁵ e-folds of the bounce (4e-7 to 5e-6), on a step of 10⁻⁸ to 10⁻⁶ e-folds:
- **Where:** β = 1.15–1.50, 2.60–2.80, 3.05, 3.15–3.25, 3.55–3.75, 4.15–4.45 and 4.90–5.00.
- **Why.** The field reaches the wall with β π/M_P < −1, the wall turns π round to 0, and g passes
  back through 0 on the way. It is a true maximum of T_J.
- **What follows.**
  - It is a crossing before the first bounce, so it counts, and `n_crossings` includes it.
  - At β = 1.15–1.50 it is the *only* `"max"`, so `T_max_GeV` equals `T_deliver_GeV` to about six
    digits. That is U1's definition applied as written, not a defect.
  - The walk must run up to `first_bounce(result).N` on the bounce's own step, not stop at that
    step's start.
  - At these crossings the residual |g| after `brentq` is up to 6e-10, because g is steep in the
    wall. Elsewhere it is ≤ 2.2e-16. T4's 1e-8 test is at β = 2, which has no wall crossing.

**Every count is even.** `n_crossings` is 0, 2, 4, 6, 8 or 10 on the whole grid:
- g = 1 at the start (π* = 0), and g = 1 at a turning-point bounce (π = 0), so the crossings pair
  up.
- At M = 10⁻³ no history reflects, so no bounce has π ≠ 0.
- An odd count on a T5 row means a crossing was lost or doubled. The agent may use the invariant
  as a check. It is not a requirement.

**Two crossings in one step: never, on the grid.** With 8 interior samples per step, every step
holds at most one crossing. The crossings away from the wall lie mostly on steps at the global cap
of 0.1 e-fold (the shortest is 1.7e-4, at β = 4.5), and consecutive crossings away from the wall
are 2.78 e-folds or more apart. The prompt's §4 stop for two
in one step is therefore not expected to fire. The agent still chooses its own interior count and
says why.

**What 04 and 03 need of the CSV**, which 02 fixes:
- **04's S3** compares two re-run rows with the scan's **as strings**, every column but `wall_s`.
  So every float is written by a deterministic formatter of the value alone, such as `repr`. Nothing
  may depend on the run or the clock. The probe's crossings at β = 2.0 were equal to nine printed
  digits across two separate processes.
- **03** reads the header, the blanks and the booleans. The log's "State handed to the next prompt"
  gives:
  - the header verbatim;
  - how a blank is written (an empty field);
  - how a boolean is written (`true`/`false`, or `True`/`False`);
  - the path of T5's scratch CSV, which 03's note will point at.

**What moves on purpose:**
- `ComputeTargets/ScalarModel.py`: `SurfingCrossing` and `surfing_turning_points`, after
  `first_bounce`. Nothing else.
- New: `tools/surfing_scan.py`, `ComputeTargets/tests/test_surfing_turning_points.py` (in the
  `test_[g-z]*.py` half) and `tools/tests/test_surfing_scan.py`.
- The log `logs/02-surfing-turning-points-and-driver.md`, and the board.
- No issue is expected. The index (`.documents/OPEN_ISSUES.md`) is untouched unless one opens.

**The facts, checked by the orchestrator on 2026-10-14 at `b5d1c94`.** Pass them on.
- **The tree.** Branch `surfing-temperature`, `HEAD` `b5d1c94` before this note; nothing staged. The
  untracked set is unchanged from note 01: 17 `pilot-2026.6.0*.db`, `pilot-main.log`,
  `pilot-plot.log`, `pilot-out/`, `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh`,
  `prompts/jordan-normalization/`.
- **Line numbers have drifted since the prompt was measured** (`8adb806`). In
  `ComputeTargets/ScalarModel.py` at `b5d1c94`:
  - `Reflection` is at `:123-130` and `IntegrationResult` at `:141-154`;
  - the temperature law is at `:441-443`;
  - `FirstBounce` and `first_bounce` are at `:777-856`;
  - `_T_Jordan_crossing_step` and `T_Jordan_crossing` are at `:873-922`;
  - `compute_scalar_model` starts at `:991` (the decorator), and its payload is at `:1198-1228`.

  In `tools/history_and_bbn.py`, the capture wrapper is at `:300-326`.
- **`ScalarModel.py`**: 1861 lines, black-clean. `brentq` and `bisect_left` are already
  imported. `ODEPolicyData` carries `d_logOmega_dphi`, read from `coupling.d_logOmega_dphi(phi)`,
  which for `ExponentialCoupling` is `beta / M_P` whatever φ is. T1's g must be read through the
  coupling, as the prompt says, not built from β.
- **`tools/history_and_bbn.py`.** The constants `PHI_INIT_MP`, `PI_INIT`, `T_INIT_GEV`,
  `LAMBDA_EV`, `SAMPLES_PER_LOG10_Z`, `LOG10_ONE_PLUS_Z_LOW` and `_HIGH` are module-level. The z grid
  is the private `_z_grid()`. Importing the module takes about 1.8 s and loads `ComputeTargets.BBNData`
  but not PRyMordial. Its root check tests for `PRyMrates/`. The new tool needs the root for
  `CosmologyModels/GenericEOS/Xav_EOS_data.csv`, which the EOS reads by a relative path. The check
  the tool makes is the agent's choice, and the log says which.
- **`config/version.py`**: `VERSION_LABEL = "2026.6.0"`.
- **The existing `tools/tests`** import their tool as `import tools.<module> as …`
  (`test_compare_stores.py:57`).
- **The orchestrator's probe**: scratch only, and a measurement, not a specification; the agent's
  own governs. It used `tools/history_and_bbn.py`'s setup (φ* = 5, M = 10⁻³, T_stop = 1 MeV), the
  wrapper idiom, and its own crossing finder: 8 interior samples per accepted step, `brentq` at
  xtol 1e-15, up to `first_bounce(result).N`. Capture labels came from `find_kicks`'s windows.

  | β | wall (s) | steps | n | kick | T_capture | T_max | T_2nd dip | T_deliver | T_hot | T_cold |
  |---|---|---|---|---|---|---|---|---|---|---|
  | 1.10 | 0.3 | 688 | 0 | — | — | — | — | 0.14343 | — | — |
  | 1.15 | 0.4 | 875 | 2 | QCD | 0.18858 | 0.20404 | — | 0.20404 | 0.21154 | 0.16076 |
  | 1.50 | 0.5 | 1231 | 2 | QCD | 0.32110 | 0.36139 | — | 0.36139 | 0.35698 | 0.13027 |
  | 2.00 | 0.8 | 2083 | 2 | QCD | 0.65707 | 0.74992 | — | 0.74669 | 0.74160 | 0.096529 |
  | 2.50 | 1.1 | 3214 | 2 | QCD | 1.1351 | 1.3803 | — | 1.3469 | 1.3503 | 0.025668 (merged) |
  | 3.00 | 1.6 | 4583 | 0 | — | — | — | — | 1.6800 | — | — |
  | 3.50 | 3.7 | 11059 | 4 | EW | 77.267 | 100.72 | 97.199 | 97.621 | 97.630 | 29.991 |
  | 5.00 | 8.5 | 24349 | 10 | EW | 136.87 | 187.85 | 173.35 | 175.99 | 175.99 | 18.431 |

  Temperatures are T_J in GeV. Every first bounce is a turning point (`reflected` false), and every
  one is above 1 MeV. The crossings at β = 2.0, in full:
  - `min`, N = 14.166305971, T_J = 0.6570651 GeV;
  - `max`, N = 18.947923558, T_J = 0.7499212 GeV;
  - the bounce at N = 20.352082230, T_J = 0.7466863 GeV.

  Over the grid, the captures are QCD for β = 1.15–2.95 and EW for 3.05–5.00. `mu-pi` and `ee` never
  capture. A second dip exists only where n ≥ 4: β = 2.60–2.80 and 3.15–5.00.
- **The suites at `b5d1c94`** are given in the dispatch (§1 step 2).

**What the review exists to establish.**
- **(E1) T1.**
  - The walk finds every crossing in order, with its direction, and none after the first bounce.
  - It reads g through the coupling, and it runs up to `N_bounce` on the bounce's step.
  - A zero at a step boundary is counted once, and the interior samples are real.
  - On the probe's histories it reproduces the probe's crossings to 1e-12 in N.
- **(E2) T3.**
  - `summarise` applies U1–U3 exactly: capture is the first `min`; largest is the largest T_J
    over the `max`es; the second dip is the first `min` after the first `max`; delivery is
    `first_bounce`'s.
  - The kick and its roots come from 01's functions, the merged edge included.
  - The blanks are blank, and an uncaptured row keeps its delivery.
- **(E3) T2.**
  - The columns are README §4's, in order.
  - The β grid is built by index.
  - The CSV is flushed per row.
  - Resume skips what it should and refuses what it should (exit 2).
  - The sidecar is complete, and the tool refuses a run made outside the root.
  - Floats are written deterministically.
  - The monkeypatch of `integrate_scalar_history` is undone in a `finally`.
- **(E4) Nothing in production moves.** In `ScalarModel.py`, only the namedtuple and the function
  are added. `compute_scalar_model`, the payload, `VERSION_LABEL`, `history_and_bbn.py` and
  `extract_common.py` are unchanged.
- **(E5) T5 and the projection.** The five rows agree with the probe's. The projection is
  interpolated, not guessed, and the longest history is named.
- **(E6) The pins.** Each of the five breakages fails a named new test.
- **(E7) The records.** The log, the board, the counts.

**Conventions.** All of these bind the agent, and the brief says so.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Suites.** `CLAUDE.md`'s commands, ComputeTargets in its two halves, each in the foreground,
  serially. Each run's output goes to a file in the agent's scratch directory, the verdict is grepped
  from it, and the load (`uptime`) is recorded beside it. A failure is re-run alone before it is
  called real.
- **Breakage diffs** are recorded so that `git apply` replays them from a clean tree at the agent's
  commit. Check each with `git apply --check` and `-R --check`, and replay in `bash`, not zsh.
- **Scratch.** The agent's own subdirectory of the session scratchpad, never `/tmp`. Python's
  `tempfile` in tests is fine. No scratch file goes under `ComputeTargets/`, `tools/` or
  `.documents/`. T5's CSV and sidecar are written there, and are not committed.
- **The venv.** Nothing installed or uninstalled.
- **Stores and Ray.** None opened, none started. 02 runs no PRyMordial, and at most the five
  histories of T5, the one of T4 and what the tests run.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` shows only §0's untracked set; nothing
   staged; `git log --oneline a8823a9..HEAD -- . ':!prompts'` lists only `2c98370`.
2. **Baselines.** The four roots at `HEAD`, serially, with the load. Give the agent the numbers.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`, and does not read prompts 03–05;
- its scratch directory.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/02-surfing-turning-points-and-driver.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may add or change are the prompt's §5 list, and nothing else.**
- **01 has landed.** Use `find_kicks` and `reachability_roots`. The fallback is not needed.
- **Uncaptured rows keep their delivery** (§0).
- **It starts no Ray, opens no store, runs no PRyMordial and installs nothing.**
- **Stop and ask** on any of the prompt's §4 conditions. An existing test that fails is current
  behaviour moving, never a reason to edit it.
- **It replies with:**
  - the log's path and the message as committed;
  - the interior-sample count and why;
  - T5's table and the projection;
  - the CSV header verbatim;
  - the new tests by class, with counts and wall time;
  - the four suites;
  - the five breakages and the tests each fails;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — eleven checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` touches only `ScalarModel.py`, the new tool, the
   two new test modules, the board and the log. `git status` shows §0's untracked set and nothing
   new. No CSV or sidecar is in the tree.
2. **E4, by `ast` against the pre-image** (`<agent>~1:ComputeTargets/ScalarModel.py`). The module
   body differs only by `SurfingCrossing` and `surfing_turning_points`, placed after `first_bounce`.
   Every other top-level statement dumps identically, `compute_scalar_model` included. `git diff
   <agent>~1 <agent> -- tools/history_and_bbn.py extract_common.py config/` is empty.
3. **E1, by reading.** `surfing_turning_points` in full:
   - the walk's order and its end at `first_bounce(result).N`, or at the history's end if there is
     no bounce;
   - the bounce's own step read only up to `N_bounce`;
   - a reflection treated as `first_bounce` treats it;
   - the interior samples, and how a sample that is exactly zero is handled;
   - a zero at a step boundary counted once;
   - the direction rule;
   - g read through `coupling.d_logOmega_dphi(φ)` at each N;
   - `brentq`'s bracket and xtol.
4. **E2/E3, by reading.** `tools/surfing_scan.py` in full:
   - `summarise` against U1–U3;
   - the kick by window containment, and the roots for that kick, merged edge included;
   - the blanks, and delivery on uncaptured rows;
   - the β grid by index;
   - the column order against README §4;
   - the float formatter;
   - the flush per row;
   - the resume tolerance (1e-9) and its refusals;
   - the sidecar's fields and the dirty-tree test (`--untracked-files=no`);
   - the root check;
   - the wrapper restored in `finally`;
   - a failure as a row;
   - where `compute_scalar_model`'s printing goes.
5. **E1/E5, a probe** from the scratchpad. Run the agent's tool from the root on
   β = 1.10, 1.15, 1.5, 2.0, 2.5, 3.0, 3.5 and 5.0 into a scratch CSV. Compare each row with §0's
   table:
   - temperatures and counts to the table's digits;
   - `capture_kick`, and `T_cold_GeV` at 2.5 equal to the merged edge;
   - delivery present and `captured` false at 1.10 and 3.0.

   On the same captured `IntegrationResult`s, call `surfing_turning_points` and the probe's own
   finder, and check that the N agree to 1e-12 with the same directions. Also run the finder with 0
   and with 32 interior samples, to see whether the count matters on real histories.
6. **E3, the tool's behaviour by probe.**
   - Re-run the same command: no new rows, and the sidecar extended.
   - Re-run with `--phi-init-Mp 4`: exit 2, file unchanged.
   - Run from a directory other than the root: refused.
   - Run two β into separate CSVs: equal as strings but for `wall_s`, which is 04's S3 in
     miniature.
   - `--beta-min 1.10 --beta-max 5.00 --beta-step 0.05` gives 79 β whose strings equal
     `round(1.10 + 0.05 k, 10)`. Read this from the grid function, and do not run the scan.
7. **E6.** In `bash`, replay each breakage diff with `git apply`, run the named test, see it fail for
   the stated reason, revert with `git apply -R`, and check that `git status` is clean. Then read the
   tests: does a test pin each of these?
   - two zeros in one step;
   - the stop at the bounce;
   - a reflection before any crossing;
   - the wall-type `max` just before the bounce (the stub's "rise to π > 0" makes one if π starts
     below −M_P/β);
   - the resume refusal;
   - the real history's |g| < 1e-8 and its delivery equality.
8. **E5, T5 in the log.** The five β with their wall time, steps, the four temperatures, the counts,
   the kick, and the bounce above T_stop. The projection over 79 β, and the longest history named.
   They should agree with §0's probe to its digits (wall times aside).
9. **E7, the records.**
   - **The board:** row 02 ✅ with "this commit" and the log; T1–T5 ✅; the gate "Turning points"
     ✅; `### 5.3 After prompt 02 (<date>)` with the projection.
   - **Issues:** §3 and the index untouched, or, if one opened, the index row, count and date in the
     same commit.
   - **The log:** in §5.1's form, every deviation classified, with §2's checks and their output.
10. **The suites** at the agent's commit, serially: 18, 61 + (106 + new), 69, 34 + new.
    `black --check` on every touched Python file.
11. **The interface for 03 and 04.** Read the log's "State handed to the next prompt": the header,
    the blank, the booleans, the sidecar's keys and the scratch CSV's path. Check them against
    03's F1–F3 and 04's S1–S3, which the orchestrator reads here and the agent did not. Anything
    that 03 or 04 would need and the CSV lacks is recorded for their notes. The uncaptured
    deliveries at β = 1.10 and 3.00, and the wall-type maxima, go to 03's note in any case.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 02* paragraph in §5.3. Fix small
  residue in a follow-up commit of its own:
  - "this commit" → the SHA (board and log);
  - this note in the notes table, marked "used for 02";
  - the status line: 2 of 5 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the turning-point method and the interior-sample count;
  - T5's table and the projection;
  - the tests added and the breakages;
  - the four suites' counts;
  - anything 03's or 04's note must carry. At the least: the uncaptured β = 3.00, the wall-type
    maxima at which `T_max` equals `T_deliver`, and the scan's real cost.
- **Then 03.** Its note is written after this review.
