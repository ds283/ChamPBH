# Orchestrator — prompt 03, the fix

Read [`../README.md`](../README.md) first: §0, §2, §5, §6.2 P5–P10, and §6.3 whole (the re-plan of 2026-11-02
and U6 as ruled: "That's fine. We'll do F3 plus the guard."). Then the prompt. This note binds 03 only. The
conventions of `inelastic-reflection`'s notes and of note 01 bind here as well: a tolerance is a number with
its reason, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites, the scanner, the replay and the comparison scripts the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`03-the-fix.md`](../03-the-fix.md) · **Gate:** "Fix" · **Closes:** nothing (the assigned
issue closes in 04) · **Opens:** only what the work finds · **Model:** Opus, as the prompt recommends ·
**Network:** none. Nothing is pushed.

**When:** at once, at the user's request of 2026-11-02 ("Please do", to writing this note and dispatching 03).

**Gate**, checked at `9518d41` on 2026-11-02 at 14:28 GMT, before this note was committed:
- `reflection-restart` is checked out in the main checkout at `9518d41`, the commit that adds prompt 03;
  `git status --porcelain` is empty; `d4703df` is an ancestor of `HEAD`. `git worktree list` also shows
  `.claude/worktrees/quanta-diagnostics` (branch `quanta-diagnostics`, `1e92150`), another session's,
  which touches only `prompts/quanta-diagnostics/`, `prompts/INDEX.md` and `.documents/OPEN_ISSUES.md` on
  its own branch. Nothing here touches it.
- `./venv/bin/python -m runregistrykit list`: 23 runs, all finished.
- `var/reflection-restart/01/main.jsonl` (71 MB) and `tasks-main.json` exist; the task list holds 66 tasks
  under F3.
- `.documents/reflection-restart-verification.md` has §1 and no §2.
- The suites were not re-run for this note; 01 left them at 26, 508 (122 + 386), 130, 145 and 68, all
  passing. The agent measures them again before changing anything (prompt §3).
- Load averages 2.92 / 3.66 / 4.51. The machine is shared.

## 0. What the orchestrator found

For this note the orchestrator read 01's task list and result lines, the golden modules
(`golden_history.py`, `golden_tuned_history.py`) and their tests, 01's probe tests and scanner tests, 01's
golden hook (`.documents/reflection-restart/measure/golden_post.py`), the plot scripts' lookups, and the
`ScalarHistory` factory's version keying. Here is what it found where the prompt is silent, loose or wrong.

- **The replay works without `--variant`.** Each of the 66 F3 tasks of `tasks-main.json` is a spec with a
  `"variant": "F3"` key, a `"key"` string, and its own fields: 9 tuned (the diagnosis's nine), 54 at a stored
  normalization with `"store"` and `"failure"` (the reflection store's 42 and block B's 12), and the three
  goldens, which name 01's hook in `"post"`. With the variant removed, every spec is a production task. The
  hook reads the golden paths through `golden_history.GOLDEN_PATH` and `golden_tuned_history.GOLDEN_TUNED_PATH`.
  After the paths move (prompt §1.6) it compares each golden task with the **new** records, which is what is
  wanted.
- **01's F3 lines are not bitwise against the store.** F3 moves every reflecting history, so for the 54 store
  tasks 01's line records a comparison that differs from the store (only the six α = 0 ones are bitwise).
  The prompt's "the store comparison's verdict, all equal" means equal to **01's F3 verdict**, not bitwise
  against the store.
- **The tuned golden's path is in `golden_tuned_history.py`**, not `test_tuning.py`, which the prompt's file
  list names. `test_dark_radiation.py` also reads it, through the module, and needs no change.
- **The reflecting golden needs a setup module.** `golden_tuned_history.py` fixes β = 3, M = 0.01 M_P, α = 0
  as module constants, but its builders take them as arguments (`make_model(units, beta, M_over_Mp)`,
  `run_tuning(..., **kwargs)`, `build_record`, `differences`). A history at β = 1.5, M = 10⁻¹², α = 0.05 is
  pinned by a module of its own that reuses them.
- **A 2026.8.0 store cannot be read by 2026.9.0 code in any case.** `ScalarHistory` lookups are keyed on the
  version label and may never fall back (`ObjectFactories/ScalarHistory.py:183`, `:420`,
  `require_version_serial`). Under `VERSION_LABEL` 2026.9.0 the plot scripts find no history in a 2026.8.0
  store, whatever the stepper label. The prompt's "select histories of **either** label" therefore buys
  nothing. A 2026.8.0 store is read with its own tree (`main`, or `9518d41`'s parent).
- **The scanner's tests.** Of `test_outward_step_scan.py`'s 22, one depends on the switch itself:
  `test_observe_restores_the_module` (:301–:311). The predicate tests (:136–:175) call
  `_probe_outward_step_flagged` and follow its rename. `test_probe_outward_fix.py` has 10 tests.
- **The window for the guard's test exists.** 01's window (`test_probe_outward_fix.py` docstring and
  `TestTheWindow`) starts at the reflection's pre-state of the β = 1.5 failure and reproduces the bad step
  bitwise under SciPy's own first step (`test_None_reproduces_the_bad_step`). Under F4 it discards that step
  (h = `BAD_STEP_H`) and the window succeeds (`test_F4`). F4 retook with `max_step` = h/2, where the guard
  retakes with `first_step` = h/2. The two give the same first attempt, since the cap loop clamps `h_abs` to
  `max_step`. 01's other window, the cap-loop probe P1 at M = 0.5, has no restart, and its first flagged step
  is after N = 20.8 (`test_F4_where_nothing_is_flagged`).
- **The replay's cost.** 01 ran 414 tasks in about 32 min at six workers. Its F3 tasks took 3–13 s each,
  except β = 1.2, α = 0.001, which took 200–320 s (01's log §8). The replay is expected to take about
  10 min at six workers.

**Rulings.** Each is recorded in the log, with its class.
1. **The replay.** The agent writes `var/reflection-restart/03/tasks-replay.json`: 01's 66 F3 task specs with
   `"variant"` removed and `"key"` rewritten without it. Everything else is unchanged, the goldens' `"post"`
   hook included. It runs them as one detached batch (`--jobs 6`, `--timeout 1800`) on the commit's tree.
   For each task it compares with 01's F3 line in `main.jsonl`: `outcome`, `samples_sha256`,
   `log_one_plus_z_sha256`, `n_samples`, `accepted_steps`, `RHS_evaluations`, `compute_steps` and
   `reflections`, all equal. For the 54 store tasks, the store comparison must equal 01's F3 comparison (the
   same columns differing, by the same amounts). For the three goldens, the hook must say `bitwise` against
   the new records. In every task, `restart_retakes` is 0, and no kept or discarded step carries the ruled
   signature. A difference is a stop, with the task and the first differing field.
   *STRUCTURALLY REQUIRED* (§0, first and second points).
2. **The plot scripts follow the label.** `plot_by_beta.py` and `plot_ScalarHistory.py` select
   `Radau+kinematic-cap-stepping1` only, not both labels. Instead, `config/version.py`'s 2026.9.0 entry says
   that lookups are version-keyed, so a 2026.8.0 store is read with its own tree. This replaces prompt §1.5's
   "either label". *STRUCTURALLY REQUIRED* (§0, fifth point).
3. **The goldens.**
   - `golden_history.GOLDEN_PATH` points at `golden_history_rr03.json`, and
     `golden_tuned_history.GOLDEN_TUNED_PATH` at `golden_history_rr03_tuned.json`. Each module's own
     regeneration writes the new file; the old files are not opened for writing.
   - `record_of` gains `restart_retakes`. The comparison script compares old with new and allows exactly
     two differences: `solver_label`'s value and the added `restart_retakes` key (0). Anything else is a
     stop.
   - The reflecting golden is set up in a new `ComputeTargets/tests/golden_reflecting_history.py`, built on
     `golden_tuned_history`'s builders with β = 1.5, M = 10⁻¹² M_P, α = 0.05. It has its own path, its own
     regeneration command and a test that passes it bitwise.
   *IMPLEMENTATION CHOICE*, ruled here (§0, third and fourth points).
4. **The test counts.**
   - `ComputeTargets/tests` may fall by exactly `test_probe_outward_fix.py`'s 10.
   - `tools/tests` may fall by at most one, `test_observe_restores_the_module`, and only if it cannot be kept
     in a form that tests the observer's restore without the switch. The predicate tests are kept, on the
     production predicate.
   - Every other count must not fall. The baselines are 26, 508 (122 + 386), 130, 145 and 68.
   *As the prompt says; made exact.*
5. **The guard's bookkeeping.**
   - Arming: each `new_solver` call made for a restart with π > 0 (reflection, drain, release) or for a retake
     marks the instance's first step as guarded. The first accepted step clears the mark, whether it passes
     or is retaken; a retake marks the new instance.
   - Not guarded: a restart with π ≤ 0, the history's start, and the parked phase (DOP853).
   - What a discard leaves: as 01's ruling 3, nothing but the count and the RHS evaluations.
   *IMPLEMENTATION CHOICE*, ruled here.
6. **The guard's tests use 01's windows.** The constants and the recorded state move from
   `test_probe_outward_fix.py` into `test_restart_fix.py`.
   - With `_restart_first_step` replaced to return SciPy's own choice, the β = 1.5 window must discard a step
     of h = `BAD_STEP_H` bitwise, and then succeed.
   - With the guard's predicate also replaced to return False, it must fail with the history's m_eff²
     failure, as 01's `test_None_reproduces_the_bad_step` does.
   - "First step only" uses the cap-loop window P1 at M = 0.5, run past its first flagged step after
     N = 20.8. A flagged step is kept there, and `restart_retakes` is 0.
   *IMPLEMENTATION CHOICE*, ruled here (§0, seventh point).
7. **The payload's key.** If a test pins the set of payload keys, it follows the new key `"restart_retakes"`,
   and the log names the test. `extra_data` gains the key only when it is above 0 (prompt §1.2), so the
   scanner's store comparison of shared `extra_data` keys is unaffected. *As the prompt says; restated.*
8. **No file but these changes:** prompt §4's list, plus `ComputeTargets/tests/golden_tuned_history.py`
   (the path), `ComputeTargets/tests/golden_reflecting_history.py` (new) and its test, and any test that
   names the stepper label or pins the payload's keys. The task list of ruling 1 and the replay's output stay
   in `var/`. *STRUCTURALLY REQUIRED*; followed.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate, and the five suites before anything else: serially, ComputeTargets in its two halves, the
     `test_[g-z]` half and `tools/tests` in the background, waiting for each notification.
  2. F3 and the guard; the probe removed; the tests of §1.4.
  3. The labels and the goldens (§1.5, §1.6), with the comparison script.
  4. The scanner on the production code, and the three failures (§1.7 item 1).
  5. The replay (ruling 1), detached. Nothing in `ScalarHistory.py` or the scanner is edited while it runs.
  6. The suites again; the documents; the records; the commit.
- **Long runs.** The replay is launched detached (`nohup`, pid recorded), checked once, and waited on by a
  background command that ends with it, not by polling.
- **Scratch.** The replay writes under `var/reflection-restart/03/`. The agent's probes and suite outputs go
  in its own subdirectory of the session scratchpad. Nothing goes in `/tmp`.
- **No Ray.** Nothing starts a cluster; no `ray` command is run. No store is written but temporary ones in
  tests.
- **Stop conditions are real:**
  - a suite count that falls beyond ruling 4;
  - a replay task that differs (ruling 1);
  - a golden field that differs beyond ruling 3;
  - `FINGERPRINT_FORMAT` or a fingerprint test needing a change;
  - the guard firing on any replay task;
  - any change the prompt's §2 forbids becoming necessary.
  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch, and another session works in the `quanta-diagnostics`
  worktree. The agent does not assume `HEAD` is its own, and never touches that worktree.

## 1. Before you dispatch

Done at `9518d41`, before this note was committed:
1. **The Gate above.**
2. **The reading of §0**: 01's task list and F3 lines counted (66 tasks, 66 lines), the golden modules, the
   probe tests, the scanner tests, the hook, and the version keying of lookups.

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout, in the background. Give it:
- the prompt, the README, the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 8. Its log is `logs/03-the-fix.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **It starts nothing that outlives its turn but the replay**, and the replay must have ended before it
  commits.
- **Stop and ask** on any of the conventions' stop conditions.
- **It replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`, and the tests deleted and added by name;
  - F3 and the guard: where each sits in the loop, by line, and how the guard is armed and cleared;
  - the goldens: the comparison script's output for both old records, and the reflecting golden's
    reflections, steps and `restart_retakes`;
  - the Fix gate: the three failures' records; the replay's table (66 tasks, each field equal or not); the
    signature count;
  - the labels: `VERSION_LABEL`, the stepper label in `main.py` and the plot scripts, and
    `FINGERPRINT_FORMAT` (expected 6);
  - the breakage record;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — seven checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 8's files. Its parent is this note's commit, or a later
     commit that is not the agent's. `git status --porcelain --untracked-files=all` is empty, and nothing is
     pushed.
   - `grep -in probe ComputeTargets/ScalarHistory.py` finds nothing of 01's switch.
   - `FINGERPRINT_FORMAT` is 6.
2. **Nothing left running.** The replay's pid does not answer `kill -0`, and `runregistrykit list` is
   unchanged.
3. **The goldens.** Run `test_golden_history`, `test_store_fingerprint_golden`, `test_tuning`, the
   reflecting golden's test and the comparison script myself.
4. **The replay.** Re-run three tasks myself on the commit's tree and compare with 01's F3 lines: the
   β = 1.5 failure (tuned), one store task at 4×10⁻³¹, and the tuned golden.
5. **The guard.** Read the code against prompt §1.2 and rulings 5–6. Run `test_restart_fix.py`. Then patch
   the guard's predicate to False in a scratch probe, and see the window reach the m_eff² failure.
6. **The labels.** `config/version.py`'s entry; `main.py`'s registration and mapping; the plot scripts'
   selection; `test_kinematic_cap_loop`'s label test.
7. **The records.**
   - The log is in §5.1's form, with every deviation classed and §0's rulings among them.
   - The verification document gains a §2 and leaves §1 alone; `out/` holds text only.
   - The numerical-strategies and architecture-summary notes are dated.
   - On the board: row 03, its items and the Fix gate. Any issue opened is on §3 and in
     `.documents/OPEN_ISSUES.md`.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 03 landed, the Fix gate held, and this note added to the list of orchestrator notes,
  marked "used for 03";
- an *Orchestrator review of prompt 03* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the
board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the Fix gate: the three failures, the replay, and the goldens;
- the labels as they now stand;
- that 04 (the 42 and the close) is written next, and that `quanta-diagnostics` can start once
  `reflection-restart` is merged into its branch (its U1).
