# Orchestrator — prompt 01, the model

**Used for 01** (dispatched 2026-11-14 at `75ac6fb`; landed `3aae9a5`; reviewed on the board, "Orchestrator review of
prompt 01").

Read [`../README.md`](../README.md) first: §0, §2, §5, and §6 as ruled. The user's words are, for the plan, "U1–U7 as
proposed", and for this prompt, "Yes, commit it and write prompt 01". Then read the prompt. This note binds 01 only.
The conventions of earlier campaigns' notes bind here as well: a tolerance is a number with its reason, timing is
taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites, the tools and the scripts the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`01-the-model.md`](../01-the-model.md) · **Gate:** "Model" · **Closes:** nothing · **Opens:** only
what the work finds · **Model:** Opus, as the prompt recommends · **Network:** none. Nothing is pushed.

**When:** written 2026-11-13 at the user's request. Whether it is dispatched from this context or a fresh one is the
user's to say; either way the dispatching context re-checks the Gate (§1).

**Gate**, as measured by the planner on 2026-11-13 at about 20:30 UTC, at `c540caa`, before this note and the
prompt were committed:
- The worktree `.claude/worktrees/power-law-wall` is on `power-law-wall`; `c540caa` is `HEAD`; the main checkout is on
  `main` at `a7e0978`, clean.
- `git status --porcelain` in the worktree: `?? venv` (the symlink the planner made, to
  `/Users/ds283/Documents/Code/ChamPBH/venv`; `.gitignore` does not name `venv`, and the main checkout's ignore rule
  matches the directory, not a symlink), and the README's P5 amendment and this prompt before their commit.
- `runregistrykit list`: nothing `running` or stale. The 2026.11.0 science run had not been launched; the user launched it at 20:42 (`science-2026.11.0-A-A-lab-20261113T204245`, `running` at 20:58), during the planner's suites (§0.1).
- `InversePowerModel.py` does not exist; `model_ids.py` has `EXPONENTIAL_MODEL = 1` only;
  `.documents/power-law-wall-verification.md` does not exist.
- Load averages 5.07 / 4.28 / 4.23 at 20:26, rising to about 12 under the probe.

## 0. What the orchestrator found

The planner ran a scratch power-law model through `tools/history_and_bbn.py` by substituting the model the tool
builds. The scratch class subclassed `ExponentialModel` for its coupling, floor, `renormalize` and seed, and replaced
the three potential hooks with the prompt's §1.1 forms; it inherited the exponential's `_drain_L`, so only natural
mode and the fixed law were run. Its files are in the planner's scratchpad and are not committed.

- **The hooks.** Against central differences at n = 1 and 2, M = 10⁻³ M_P, φ from 10⁻⁹⁵ to 5 M_P: ln V′ φ and
  (ln V)″ φ² agree to 10⁻⁸ relative or better, with no overflow. At φ = 10⁻⁹⁵ M_P, ln V − ln V₀ is 212 (n = 1) and 424
  (n = 2).
- **Laboratory M runs, and does not depend on n under the fixed law.** Tuned, fixed α = 0.1:

  | run | wall | tuning | reflections | first bounce | ΔN_eff today | Y_p | ln R at first impact |
  |---|---|---|---|---|---|---|---|
  | n = 1, β = 2 | 24 s | 1 integration, seed `tracking` | 49 | N = 20.352, 747 MeV | 0.0524407 | 0.2470913 | 530.6 |
  | n = 2, β = 2 | 38 s | 3 (plain, secant), seed `plain` | 49 | N = 20.352, 747 MeV | 0.0524407 | 0.2470878 | 320.7 |
  | n = 1, β = 1.2 | 15 s | 1, `tracking` | 9 | N = 17.668, 231 MeV | 0.0656475 | 0.2495310 | 511.1 |
  | exponential, β = 2 (2026.10.0 export) | — | — | 49 injections | — | 0.052440 | 0.247091 | 142.4 |
  | exponential, β = 1.2 (2026.10.0 export) | — | — | 7 injections | — | 0.065672 | 0.249487 | 138.2 |

  The exponential's rows are from the 2026.10.0 run's `histories.csv`, on the old w table, so they are not an exact
  comparison; the prompt's §1.5 runs the exponential on this tree. Every reflection at laboratory M is unresolved
  (`resolved_candidates=0`), and under the fixed law the drain does not read the wall's shape, so the history hardly
  moves with the wall. ln R at the first impact does move, by a factor of 2–4, and that is what the one-loop law reads
  (02's business).
- **The n = 1 seed.** The scratch class used the exponential's analytic seed, valid at n = 1 because V′ = −VM/φ² and
  −V₀M/φ² agree for M/φ ≪ 1; it converged in one integration. At n = 2 the exponential falls back to the plain seed
  and took three. The prompt's §1.2 seed at every n should bring n = 2 to one.
- **Natural mode at 10⁻³ M_P fails on both n**, at 5 s (n = 1) and 28 s (n = 2), with an uncaught
  `ValueError: math domain error` from ln(M/φ) at φ ≤ 0, raised inside the RHS during the seed's integration.
  - n = 2 first printed the tuning's warning: "the classical turning point lies below 1e-08 M_P (phi_t = 9.401e-27
    M_P at the floor reflection at N = 20.35209825), where dissipation mode would reflect without resolving it; this
    history may fail on Radau's float spacing or the step budget".
  - On the exponential at 10⁻³ M_P, φ_t is about M/L, near 10⁻⁵ M_P; the power wall puts it 22 decades deeper. Natural
    mode resolves the turn, so it must integrate there.
  - The exponential's hooks are finite at φ < 0 (e^{M/φ} → 0 at n = 1; even at n = 2), so its trial points there
    never raise. The prompt makes φ ≤ 0 a `ComputationFailureError` (§1.1) and asks for the measurement and a scratch
    probe of the even continuation (§1.6), for the user's ruling.
- **The suites in the worktree** (at `c540caa`, with the `venv` symlink): §0.1; two tests fail there, both the
  worktree's, not the code's (ruling 7).

### 0.1 The suites in the worktree

Run serially by the planner at `c540caa`, from the worktree root, with `./venv/bin/python` (the symlink) and
`PYTHONPATH=.`, 2026-11-13 20:29–20:58 local:

| suite | ran | result | time | load at start |
|---|---|---|---|---|
| CosmologyModels | 26 | OK | 81 s | 7.19 |
| ObjectFactories | 163 | OK | 164 s | 5.66 |
| RunDrivers | 76 | **1 failure** | 29 s | 5.49 |
| ComputeTargets `test_[a-f]` | 219 | OK | 177 s | 4.63 |
| ComputeTargets `test_[g-z]` | 465 | OK | 770 s | 6.60 |
| tools | 292 | **1 failure** | 477 s | 110.56 (the 2026.11.0 run had started) |

The counts are `CLAUDE.md`'s. Both failures are the worktree's, not the code's: the registry resolves the repository's
root to the main checkout, not to the worktree.
- `RunDrivers.tests.test_registered_run.TestRegisterInProduction.test_the_script_is_main_py_with_its_hash_and_this_repositorys_head`:
  `'.claude/worktrees/power-law-wall/main.py' != 'main.py'`.
- `tools.tests.test_registry_config.TestRegistryConfig.test_roots_and_distributions`: `runs_root` is the main
  checkout's `var/runs`, not the worktree's.

`electroweak-eos` 01's planner saw these two, and eight of `test_ray_cluster_lock`, in a scratch worktree without a
venv (its note §0); with the symlink the lock tests pass. **Ruling 7** below covers the two.

**Rulings.** Each is recorded in the log, with its class.
1. **The worktree and the main checkout.** The agent works only in `.claude/worktrees/power-law-wall`, from its root,
   with `./venv/bin/python` (the symlink) and `PYTHONPATH=.`. It never stages `venv`, never touches the main checkout,
   and never runs a command that names `/tmp/ray-cpbh`. If the 2026.11.0 science run is building when the agent runs,
   the suites and the tool runs share the machine with it: they run serially, the load is recorded beside each timing,
   and nothing is killed. *STRUCTURALLY REQUIRED* (README §5, U1).
2. **φ ≤ 0.** As the prompt's §1.1: a `ComputationFailureError` naming the model. The even continuation is measured in
   scratch only (§1.6) and not committed, whatever it shows. *IMPLEMENTATION CHOICE*, ruled here.
3. **The twelve runs' failures are results.** A history that fails with a stored reason is recorded and not retried.
   An uncaught exception that is not `ComputationFailureError` is a defect: if it comes from the new class, the agent
   fixes the class; if from anywhere else, it stops. *IMPLEMENTATION CHOICE*, ruled here.
4. **The exponential beside each run** is run on this tree by the same command without `--model`, so the comparison
   is of the wall alone. *IMPLEMENTATION CHOICE*, ruled here.
5. **`CLAUDE.md`'s counts** are updated on this branch in the commit, with the two new modules' tests. *As the prompt
   says.*
6. **No file but the prompt's §4 list.** *STRUCTURALLY REQUIRED*.
7. **The two worktree failures of §0.1** are expected on the base and after, and are not regressions. The counts are
   compared as ran and failed, before and after; any other test that fails, on the base or after, is a stop. They are
   recorded in the log, and no issue is opened for them (they are the worktree's, as `electroweak-eos` recorded).
   *IMPLEMENTATION CHOICE*, ruled here.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes it in its review.
- **Order.**
  1. The Gate, and the five suites on the base: serially, ComputeTargets in its two halves, the `test_[g-z]` half and
     `tools/tests` in the background, waiting for each notification.
  2. The class and its tests; `test_wall_model.py`'s additions; the seed.
  3. The tool route and its tests.
  4. The twelve runs and their exponential counterparts (§1.5); §1.6's measurement and scratch probe.
  5. The suites again; the documents; the commit.
- **Scratch.** Probes and suite outputs go in the agent's own subdirectory of the session scratchpad. Nothing goes in
  `/tmp`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is written but temporary ones in tests.
- **Stop conditions are real:**
  - a suite count that falls, or a test that passed on the base and fails after;
  - an exponential golden, or a pinned line of either tool under its defaults, that moves;
  - an integrator or store change becoming necessary;
  - an uncaught exception from outside the new class (ruling 3);
  - any change the prompt's §2 forbids becoming necessary.
  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

1. **The Gate above**, on the commit that adds this note.
2. **`runregistrykit list`**, and the load average. If the 2026.11.0 run is building, say so to the agent (ruling 1).
3. **The probe of §0.** Its numbers are the planner's. The dispatching context need not repeat it.

## 2. Dispatch

Start one fresh-context subagent, on **Opus**, in the worktree, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 6. Its log is `logs/01-the-model.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Stop and ask** on any of the conventions' stop conditions.
- **At the end it replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the hooks' tolerances and the closed-form checks, with their worst residuals;
  - the seed at n = 1 and 2: kind, and its agreement with the independent root;
  - the tools: what their defaults' tests show, and the lines under `--model inverse-power`;
  - the twelve runs and their exponential counterparts, as a table;
  - §1.6's findings, and the scratch probe's;
  - the breakage record;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — six checks

1. **Scope.** `git show --stat <agent>` lists only ruling 6's files; `venv` is not in it; its parent is this note's
   commit or a later one that is not the agent's; the main checkout is untouched (`git -C <main> status`). Nothing is
   pushed.
2. **The exponential.** Run `test_golden_history`, `test_golden_reflecting_history`, `test_tuning` (e),
   `test_store_fingerprint_golden`, `test_history_and_bbn` and `test_surfing_scan` myself. `git diff <base>` shows no
   change to `ExponentialModel.py`, `ComputeTargets/` outside `tests/`, `ObjectFactories/` or `config/`.
3. **The class.** Read it against §1.1: every hook in logs, φ ≤ 0 raising, no `_drain_L`, the coupling written in the
   class. Run `test_inverse_power_model` and `test_wall_model`.
4. **The seed.** At n = 2, β = 2, M = 4×10⁻³¹ M_P, `history_and_bbn --model inverse-power --n 2 --alpha 0.1` tunes in
   fewer integrations than the planner's three, or the log says why not.
5. **The runs and §1.6.** The table in the verification document matches the outputs under
   `.documents/power-law-wall/model-01/out/`; the natural-mode failures carry `ComputationFailureError`'s reason; the
   scratch probe of the even continuation is reported and not committed.
6. **The records.** The log in §5.1's form with every deviation classed; the verification document's §1; the board's
   row 01, its items and the Model gate; `CLAUDE.md`'s counts; any issue opened on §3 and in
   `.documents/OPEN_ISSUES.md`.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 01 landed, the Model gate held, and this note marked "used for 01";
- an *Orchestrator review of prompt 01* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

**Report to the user:**
- the agent's commit and the review commit;
- the Model gate;
- the twelve runs beside the exponential, and what laboratory M shows under the fixed law;
- natural mode at large M: §1.6's findings, with the question for the user's ruling;
- that 02 (the wall's measurements) is written next.
