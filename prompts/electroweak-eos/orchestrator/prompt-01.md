# Orchestrator — prompt 01, install the table

Read [`../README.md`](../README.md) first: §0, §2, §5, and §6 as ruled. The user's words are "U1 to U6 are
fine as proposed; U7 doesn't need to be in scope here." Then read the prompt. This note binds 01 only. The
conventions of earlier campaigns' notes bind here as well: a tolerance is a number with its reason, timing is
taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites, the build's `--check` and the scripts the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`01-install-the-table.md`](../01-install-the-table.md) · **Gate:** "Table" · **Closes:**
nothing · **Opens:** only what the work finds · **Model:** Opus, as the prompt recommends · **Network:**
none. Nothing is pushed.

**When:** written 2026-11-12 at the user's request ("please write prompt 01 and its orchestration note, but
don't dispatch it. I'll do that in a fresh context."). It is dispatched by the user from a fresh context,
which re-checks the Gate before dispatch (§1).

**Gate**, as measured by the planner at `b3d5906` on 2026-11-12 at 14:01 UTC, before this note and the
prompt were committed:
- `electroweak-eos` is checked out in the main checkout. `ec6bca8` is an ancestor of `HEAD`.
- `git status --porcelain` shows **the user's own uncommitted work**, which is not the campaign's and is not
  touched:
  - ` M CLAUDE.md`;
  - ` M science-runs/2026-11-08-science-2026.10.0/README.md` and ` M science-runs/README.md`;
  - `?? science-runs/2026-11-08-science-2026.10.0/relativistic/`.
  Nothing else is listed, apart from this prompt before its commit.
- `./venv/bin/python -m runregistrykit list`: 42 runs, all finished.
- `CosmologyModels/GenericEOS/Xav_EOS_data.csv` has SHA-256 `28edd854…29a8`, equal to `git show ec6bca8:`
  of the same path.
- `.documents/electroweak-eos-verification.md` does not exist.
- Load averages 9.24 / 12.47 / 15.28. The machine is shared, and loads near 20 were seen this afternoon.

## 0. What the orchestrator found

The planner built the U2 (b) table in a scratch worktree at `b3d5906` (detached, outside the repository) and
ran the five suites on it. It then re-ran every failing test there with the old table restored. Everything
below comes from those runs. The worktree has been removed. Its outputs are not committed.

- **The table is as the prompt says.** It has 170 rows from 10⁻⁵ to 386.7085 GeV: 14 of today's, 126 of the
  notebook's, 29 points and 1 notebook row. The two tables share the row at T₀ = 50.1 keV to every digit.
  The points' 100 GeV row is the notebook's bitwise. The spline's electroweak peak is Σ = 0.037757 at
  54.02 GeV.
- **The CSV must be written with `repr(float(x))`.** The planner's first attempt wrote `repr` of
  `numpy.float64`, which gives `np.float64(…)`. pandas then reads the column as strings, and
  `Xav_EOS_spline.__init__` fails on `.astype`. That one fault errored 3 + 47 tests. The prompt now says so.
- **Suite counts on the new table** were unchanged: CosmologyModels 26, ObjectFactories 163, RunDrivers 76,
  ComputeTargets 219 + 446, tools 277. **CosmologyModels passes in full.** `test_kicking_function`'s pins hold
  within their tolerances: the electroweak peak moves by 3.2 × 10⁻⁴ in Σ against 10⁻³, and by 1.5 % in T
  against 5 %. Nothing in `CosmologyModels/tests` is expected to be re-pinned. The pins that describe the
  table's top row (25 118.86 GeV) are another matter: the agent checks whether they still test anything,
  and says so.
- **22 tests fail because of the table** (each passes on the old table in the same worktree):

  | module | tests |
  |---|---|
  | `ComputeTargets/tests/test_golden_history` | `test_history_beta_1_5`, `test_history_beta_5_5` |
  | `…/test_golden_reflecting_history` | `test_it_succeeds_and_is_the_record_bitwise` |
  | `…/test_tuning` | `test_e_the_tuned_golden_history` |
  | `…/test_injection` | `test_1_no_injection_and_the_first_wall_turning_point_is_first_bounce`, `test_2_one_injection_per_wall_side_turning_point_and_none_after_a_plateau`, `test_2_the_turning_points_are_classified`, `test_11_a_plateau_and_wall_pair_inside_one_step_is_recorded` |
  | `…/test_reachability_roots` | `test_d1_four_kicks_and_their_peaks` (EW peak 0.037757 against 0.037436 ± 5 × 10⁻⁷), `test_d2_thresholds` (EW reachability threshold 2.99918 against 3.01 ± 0.005) |
  | `…/test_release` | `test_7_parks_at_6_is_released_near_0_27_meV_and_reaches_T_CMB` (parks at 8, not 6), `test_2_it_parks_at_105_and_is_released_once_inside_the_step_06_failed_on` |
  | `…/test_wall_rounding` | `test_3_the_drain_is_log_06s` |
  | `…/test_drain_law` | `test_4_the_values_under_the_measured_law` (fifth value 0.16185 against 0.16459) |
  | `ObjectFactories/tests/test_adiabatic_dark_radiation` | `test_8a_alpha_zero_is_the_dispatch_commits` |
  | `…/test_drain_store` | `test_the_extremes_and_the_row_count`: an **error**, `None * float` at :376, since the template bounce row's `delta_K` is `None` |
  | `…/test_tuned_history_round_trip` | `test_a_the_payload_is_the_tuned_golden_history` |
  | `tools/tests/test_history_and_bbn` | `test_a_the_tuning_line_is_the_tuned_goldens_record_bitwise`, `test_b_one_integration_at_the_given_normalization_and_phi_ref` |
  | `…/test_history_and_bbn_alpha` | `test_10_every_line_is_the_dispatch_commits` |
  | `…/test_surfing_scan` | `test_beta_1_5_tuned` |
  | `…/test_plot_surfing_histories` | `test_beta_3_is_not_captured_and_bounces_as_the_committed_scan` |

- **10 further failures were the worktree's, not the table's.** They are `test_registered_run`'s script
  path, `test_registry_config`'s `var/runs` root, and eight of `test_ray_cluster_lock`, which run
  `./venv/bin/python`. They fail on the old table in the same worktree. In the main checkout they pass, and
  they are not expected to fail for the agent.
- **A finding the prompt does not anticipate: a new first turning point near 387 GeV.** Above 386.7 GeV the
  new table has Σ = 0 exactly, so a history that starts at rest (π* = 0) at T* = 2 × 10⁴ GeV is no longer
  kicked from the start. On the old table Σ was about 10⁻⁶–4 × 10⁻³ up there.
  - On the golden β = 1.5 history (M = 10⁻³ M_P), the first wall-side turning point moves from N = 18.64 to
    **N = 3.94**: φ_E = 5 − 5 × 10⁻¹² M_P, ln T_J = −36.37 (T_J ≈ 390 GeV), nonadiabaticity 2.3 × 10⁶.
  - It becomes `first_bounce`. Under α > 0 it gets no injection, which breaks `test_injection`'s "one
    injection per wall-side turning point".
  - The drain store test's error is probably the same thing: the first bounce row now has no drain, so
    `delta_K` is `None`.
  - So the new table changes what the stored `first_bounce`, the bounce rows, and anything read from them
    (`T_deliver` and its figure) mean for every history that starts at rest above 387 GeV. That is
    production's start.
  - Whether this turning point is real, an artefact of the start, or the classifier misreading a field
    barely displaced from φ* is not known. Deciding how `first_bounce` should treat it would change code
    that computes, which the prompt forbids (§2). **It is a stop** (ruling 3).
- **β = 3 is near the new electroweak edge.** The reachability prediction falls from 3.0118 to 2.9992, and
  the surfing test that pinned β = 3 as uncaptured fails: β = 3's last-bounce measure is now 27.9, not
  ≤ 10⁻⁶. The tuned golden history is at β = 3 and M = 0.01 M_P. Re-pinning these is the prompt's §1.6
  work, but the log says whether β = 3 is now captured at the electroweak scale. The threshold itself
  is 02's.
- **The suites' cost**, at loads of 14–22 in the worktree: CosmologyModels 72 s, ObjectFactories 138 s,
  RunDrivers 28 s, `test_[a-f]` 155 s, `test_[g-z]` 550 s, tools 282 s. The `test_[g-z]` half is close to
  the ten-minute cap of a Bash call under load, so it runs in the background.

**Rulings.** Each is recorded in the log, with its class.
1. **The user's uncommitted work.** `CLAUDE.md`, the two `science-runs/` READMEs and
   `science-runs/2026-11-08-science-2026.10.0/relativistic/` are the user's. The agent never stages,
   edits, stashes or reverts them.
   - Because `CLAUDE.md` holds the user's edits, the agent does **not** update its test counts in this
     commit. It records the new counts in the log and on the board.
   - Under "State handed to the next prompt", it writes that `CLAUDE.md`'s counts are to be updated once the
     user has committed their edits.
   - If at dispatch the user has committed them and `git status` shows no change to `CLAUDE.md`, this
     ruling lapses and the prompt's §4 item applies.
   *STRUCTURALLY REQUIRED*.
2. **The expected failures.** The 22 tests of §0 are the ones the table is expected to move. For each, the
   prompt's §1.6 applies: show the failure is the table's, re-pin, and record old, new and reason. A test that
   fails and is not in §0's table is a stop, unless it is one of the worktree's ten and fails in the
   main checkout for a reason of its own, which the log records. A test of §0's table that passes for the
   agent is recorded as such. *IMPLEMENTATION CHOICE*, ruled here.
3. **The early turning point is a stop.**
   - First, establish what the turning point at N ≈ 3.94 on the golden β = 1.5 history is: φ, π and the
     classifier's inputs around it, on both tables, and whether every history started at rest above
     387 GeV now has one.
   - Then stop and report to the user through the orchestrator, **before** re-pinning any of
     `test_injection`'s four tests, `test_drain_store`'s extremes test, or any golden record whose
     `first_bounce` moved.
   - The user rules whether that is accepted as physics, or handled in a prompt of its own: a change to
     the classifier, to the start, or to the table's top.
   - The rest of the work (the build, its tests, the labels, Σ's change, and the re-pins that do not
     depend on `first_bounce`) may be done before the stop, and is not committed until the user rules.
   *STRUCTURALLY REQUIRED* (§0, the finding).
4. **The golden records.** `golden_history_ee01.json`, `golden_history_ee01_tuned.json` and
   `golden_history_ee01_reflecting.json`, as the prompt's §1.7 says. `test_tuning`'s test (e),
   `test_tuned_history_round_trip`, `test_dark_radiation` and `tools/tests/test_history_and_bbn` read the
   tuned record through `golden_tuned_history.GOLDEN_TUNED_PATH`. They follow the path, and are not
   re-pinned separately unless a pin of their own moves. *As the prompt says; made exact.*
5. **Tolerances.** A re-pin keeps the test's tolerance. Two stated exceptions:
   - `test_reachability_roots` d1 (5 × 10⁻⁷) pins the electroweak peak to the old table's digits. It is
     re-pinned to the new peak at the same tolerance.
   - d2's threshold gets a new centre with its tolerance unchanged.
   Neither tolerance is widened. *IMPLEMENTATION CHOICE*, ruled here.
6. **No file but the prompt's §4 list**, less `CLAUDE.md` under ruling 1, plus the 22 tests' modules where a
   pin moves, and the golden modules. *STRUCTURALLY REQUIRED*.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate, and the five suites on the old table: serially, ComputeTargets in its two halves, with the
     `test_[g-z]` half and `tools/tests` in the background, waiting for each notification.
  2. The copy of today's table; the build script and its tests; the table installed; `--check`.
  3. Σ's change (§1.8); the labels.
  4. The early turning point (ruling 3), established; **stop and ask**.
  5. After the ruling: the re-pins and the goldens; the suites again; the documents; the commit.
- **Scratch.** Probes and suite outputs go in the agent's own subdirectory of the session scratchpad. Nothing
  goes in `/tmp`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is written but temporary ones in
  tests.
- **Stop conditions are real:**
  - ruling 3;
  - a suite count that falls;
  - a failure outside §0's table (ruling 2);
  - a golden record whose keys or `solver_label` change;
  - `FINGERPRINT_FORMAT`, or a fingerprint test, needing a change;
  - any change the prompt's §2 forbids becoming necessary.
  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

The planner did 1–3 at `b3d5906`. The dispatching context repeats 1 and 2 on the commit that adds this note.
1. **The Gate above.** `git status` must show the user's work of ruling 1 and nothing else. If `CLAUDE.md` is
   no longer modified, ruling 1 lapses: say so to the agent.
2. **`runregistrykit list`**, and the load average.
3. **The probe of §0.** Its numbers are the planner's. The dispatching context need not repeat it.

## 2. Dispatch

Start one fresh-context subagent, on **Opus**, in the main checkout, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, after the user's ruling on ruling 3, with exactly the files of ruling 6. Its
  log is `logs/01-install-the-table.md`, in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Stop and ask** at ruling 3, and on any of the conventions' stop conditions.
- **At the stop it replies with:**
  - the early turning point on both tables (N, φ, π, T_J, classification, nonadiabaticity);
  - whether every history started at rest above 387 GeV has one, and on how many of the tests' histories
    it was seen;
  - what `first_bounce` and the bounce rows become, and what reads them downstream;
  - the work done so far, uncommitted.
- **At the end it replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the table: its rows by source, `--check`, and the SHA-256 of the installed CSV;
  - Σ's change by decade, the peaks and the integrals;
  - every re-pin, with old value, new value and reason;
  - the goldens: the comparison script's output for all three;
  - whether β = 3 is now captured at the electroweak scale;
  - the labels: `VERSION_LABEL`, and `FINGERPRINT_FORMAT` (expected 7);
  - the breakage record;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — six checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 6's files. Its parent is this note's commit, or a later
     commit that is not the agent's.
   - The user's files of ruling 1 are still modified and unstaged.
   - Nothing is pushed.
2. **The table.** Run `./venv/bin/python tools/build_w_table.py --check` and `tools/tests/test_build_w_table.py`.
   Recount the rows by source. The SHA-256 of the copied old table equals `28edd854…29a8`.
3. **The re-pins.** For each, the log's old and new values against `git diff`. No tolerance is widened
   (ruling 5). Every test of §0's table is accounted for.
4. **The goldens.** Run `test_golden_history`, `test_golden_reflecting_history`, `test_tuning` (e) and
   `test_store_fingerprint_golden` myself. Run the comparison script. Check that the `rr03` files are
   unchanged (`git diff --stat` on them is empty).
5. **The ruling on the early turning point** is carried out as the user gave it, and recorded in the log
   and on the board with their words quoted.
6. **The records.**
   - The log is in §5.1's form, with every deviation classed.
   - The verification document has §1.
   - `config/version.py`'s entry is dated.
   - On the board: row 01, its items and the Table gate. Any issue opened is on §3 and in
     `.documents/OPEN_ISSUES.md`.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 01 landed, the Table gate held, and this note marked "used for 01";
- an *Orchestrator review of prompt 01* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

**Report to the user:**
- the agent's commit and the review commit;
- the table and the Table gate;
- the early turning point as ruled;
- whether β = 3 moved;
- that 02 (the threshold and the sample) is written next.

## Addendum (2026-11-12, after 01a): for 01's resumption

Prompt 01a landed as `634f141` and `electroweak-eos` was fast-forwarded to it. 01's uncommitted work in the main
checkout is untouched and now sits on top of it (no shared path). The review is on the board. Where this addendum
differs from the note above, the addendum governs.

1. **Ruling 3 is answered by 01a.** The user ruled "Handle it in a separate classifier prompt". 01a is that
   prompt.
   - An interior pair of sign changes of π whose excursion is not greater than
     τ = atol + rtol · max(|π_low|, |π_high|) is dropped and counted in
     `IntegrationResult.unresolved_turning_point_pairs`, which is not stored.
   - On the new table the early wall/plateau pair at T_J ≈ 385–389 GeV is dropped in every history and every test
     integration that had it. `first_bounce` does not move. At α = 0, `dropped_drains` goes from 1 to 0 in those
     histories; the user ruled that an allowed difference.
   - Nothing about the early turning point is left for 01 to rule. The re-pins that ruling 3 held back are now
     made on this tree, under ruling 2.
2. **§0's table of 22 tests is replaced** by 01a's list of what still fails on the new table with 01a. The first
   failing assertion of each is in log 01a §6.
   - **Now pass, nothing to re-pin (4):**
     - `test_injection`: test_1 (`first_bounce`), test_2 (one injection per wall side), test_2 (the turning points
       are classified);
     - `test_drain_store`: extremes.
   - **Still fail (18):**
     - `test_golden_history`: β = 1.5 and β = 5.5;
     - `test_golden_reflecting_history`;
     - `test_tuning` (e);
     - `test_injection` test_11;
     - `test_reachability_roots` d1 and d2, already re-pinned by 01;
     - `test_release` test_7 and test_2;
     - `test_wall_rounding` test_3;
     - `test_drain_law` test_4;
     - `test_adiabatic_dark_radiation` test_8a;
     - `test_tuned_history_round_trip` test_a;
     - `test_history_and_bbn` test_a and test_b;
     - `test_history_and_bbn_alpha` test_10;
     - `test_surfing_scan` β = 1.5 tuned;
     - `test_plot_surfing_histories` β = 3.
   - **`test_injection` test_11 is a stop before any re-pin.** It guards the interior pair that led to interior
     sampling: β = 10, M = 0.5 M_P, α = 0.1, parking cut off. On the new table that history has no interior pair,
     even without 01a (log 01a §6). A re-pin of its count would leave the test guarding nothing. The agent reports
     what the history now does, and proposes a case that still holds a resolved interior pair. The orchestrator
     takes that proposal to the user. *STRUCTURALLY REQUIRED*.
   - A test of the 18 that passes for the agent is recorded as such. A test outside the 18 that fails is a stop,
     as ruling 2 says.
3. **The verification document** (`.documents/electroweak-eos-verification.md`, which 01 creates) gains a
   pointer to `.documents/electroweak-eos/turning-points/`, 01a's measurements, beside §1. 01 does not edit that
   folder.
4. **The suite counts 01 starts from** are 01a's: CosmologyModels 26, ComputeTargets 684 (219 + 465),
   ObjectFactories 163, tools 277, RunDrivers 76. 01's `test_build_w_table` makes tools 292. `CLAUDE.md`'s counts
   wait for 01 and then follow ruling 1: while `CLAUDE.md` holds the user's edits, 01 records the counts in its log
   and on the board, and leaves `CLAUDE.md` alone.
5. **01's handoff** ([`prompt-01-handoff.md`](prompt-01-handoff.md)) steps 2 to 12 stand, run on this tree. The
   golden records and the re-pins are made once, with 01a's change in place. At α > 0, dropping the pair moves
   trajectories, as U9 intends (log 01a §5), so no pin may be carried over from a run made before `634f141`.
6. **Housekeeping.** The comment on `TURNING_POINT_INTERIOR_SAMPLES` (`ComputeTargets/ScalarHistory.py`) ends
   without a full stop. That file is not 01's to touch, so the orchestrator fixed it in a standalone commit after
   the review. The commit changes nothing but that comment.
