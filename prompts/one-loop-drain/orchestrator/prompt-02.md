# Orchestrator — prompt 02, measure the law

Read [`../README.md`](../README.md) first: §5, and §8 whole, in particular §8.2 (the law), §8.5 (the Measure gate)
and §8.6 (U11 as ruled, U13–U14). Then the prompt. This note binds 02 only. The conventions of note 01 bind here as
well: a tolerance is a number with its reason, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites, the driver, the tool and the comparisons;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`02-measure-the-law.md`](../02-measure-the-law.md) · **Gate:** "Measure" · **Closes:** nothing ·
**Opens:** only what the work finds · **Hands the user:** U13 and U14, with the measurements · **Model:** Opus, as
the prompt recommends · **Network:** none. Nothing is pushed.

**When:** at once, at the user's request of 2026-11-03 ("Please write the orchestration note and dispatch it, and
afterwards carry out a review of the implementation agent's work").

**Gate**, checked at `56d477f` on 2026-11-03 at about 14:20 UTC, before this note was committed:
- `one-loop-drain` is checked out in the main checkout at `56d477f`, the commit that adds prompt 02.
  `git status --porcelain` is empty. `b99f6a4` and `d7cef0e` are ancestors of `HEAD`.
- `git diff b99f6a4 HEAD --` over the prompt's code paths is empty.
- `./venv/bin/python -m runregistrykit list`: 23 runs, all finished.
- The store is `var/datastores/science-2026.8.0.db` with its 16 shards. The orchestrator read all 25 serials of §1.1
  from the shards named, read-only. Each is there, validated and a success, with the M, β and α the table gives
  (M_eV 9.741×10⁻⁴ = 4×10⁻³¹ M_P, 2.435×10⁻⁶ = 10⁻³³, 2.435×10⁻³ = 10⁻³⁰ and 2.435×10¹⁵ = 10⁻¹²). Their stored
  `log_R_first_impact` agree with §0.3: at β = 2.0 it is 166.33 at 10⁻³³, 142.37 at 4×10⁻³¹ and 138.70 at 10⁻³⁰.
- `.documents/one-loop-drain-verification.md` has §1 (§1.1–§1.10) and no §2. `.documents/one-loop-drain/` holds
  `law/` only. `var/one-loop-drain/` holds `01/` only.
- The suites were not re-run for this note. The review of 01 left them at 26, 566 (163 + 403), 130, 156 and 68, all
  passing. Nothing under `tests/` has changed since `d7cef0e`. The agent measures them again first (prompt §3).
- Load averages 22.14 / 25.10 / 22.41 at 14:22. No other compute job was visible in `ps`. The machine has 10 cores
  and **16 GB of memory**, and it is shared.

## 0. What the orchestrator found

For this note the orchestrator read:
- `tools/history_and_bbn.py`: `main`, `drain_values` and its imports;
- `ComputeTargets/ScalarHistory.py`: `drain_regime`, `drain_alpha_effective`, the result and record types, every
  use of `alpha` in `integrate_scalar_history`, `integrate_history` and `tune_history`, and `dissipation_values`;
- `law/one_loop_run.py`, and the scanner's `_sha256`;
- the briefing note's `tab:beta` and its definitions of R and L;
- PRyMordial's file writes;
- the store's manifests.

Here is what it found where the prompt is silent, loose or wrong.

- **S2's premise holds in the code.**
  - In `integrate_scalar_history`, A is read only as the tests `alpha > 0.0`, in `drain_alpha_effective` (:2195,
    under the one-loop law) and as `DrainRecord.A` (:1948).
  - `integrate_history` and `tune_history` pass it through, and read it otherwise only in `alpha > 0.0` tests
    (:4647) and `check_drain_law`.
  - `_GAP_PRESCRIPTION` is read in `drain_alpha_effective` and also once at the integration's start, for
    `DrainRecord.gap_prescription` (:1939).

  So under S1 the record's `gap_prescription` differs, and under S2 its `A` does. Nothing else differs by
  construction (ruling 6).
- **"3–16 integrations a tuning" in 01's log (§7) is not a one-loop figure.** It comes from two *fixed-law*
  tunings of store failures (3460: 3, 4397: 16; verification §1.7). 01 ran no tuned one-loop history. The prompt's
  "about one integration" rests on the store (3466 of 3718 tuned in one) and on its own ladder probe (one). The
  estimate stands, but it is a projection. A second integration at A = 0.01 doubles a laboratory-M task to about
  30 min, and §1.4's projection is what catches that.
- **Memory is the binding resource at A = 0.01, not CPU.** `IntegrationResult.solution` is an `OdeSolution` that
  holds one interpolant per accepted step (:3182). 2197 at A = 0.01 took 1.97×10⁶ accepted steps. §1.3 asks the
  driver to keep **every** integration's `IntegrationResult`. Kept whole, a tuning of several integrations could
  hold several such solutions. Six workers would then be at risk on a 16 GB machine that is shared (rulings 3, 5).
- **A timed-out task can report nothing from inside.** §1.3 records "a timeout … with the integrations it
  finished", but the parent kills the child process. So the child must write each integration out as it
  finishes (ruling 4).
- **The tool's `main` is one function.** It calls `tune_history` (imported into the tool's namespace), wraps
  `SM.integrate_scalar_history` to keep the last result, sets and restores `_GAP_PRESCRIPTION` from
  `--gap-prescription`, builds the stand-in, and calls `compute_BBN_data._function`. It returns 0 or 1 and does not
  return the payload. The SHA-256 of the samples needs the payload, so "imports the tool's functions" needs a
  definite form (ruling 3).
- **The R·S(L) bound has a definite form in the payload.**
  - The accepted integration's `Injection` carries `N`, `log_rho_kin` (ln ρ_kin,E) and `classical_turning_point`.
    The last of these gives φ_t and ln R = ln(ω_t⁴/8π²E), which is the note's R = ρ_sat/ρ_kin.
  - `production_energy_ratio.tex` defines L ≡ M/φ_t and ρ_β/ρ_kin = R·S(L). It tabulates L²S as the prompt quotes:
    0.22, 0.24, 0.25, 0.25 and 0.26 at L = 40, 70, 90, 110 and 134.
  - M and φ_t are in the same units (Planck_units), so L is dimensionless.
  - **But** where nothing drains, K(N_final) = 0 and the bound's share of K is undefined. That is so in group C,
    and wherever S2 applies. Those are the histories for which the bound is the only number (ruling 7).
- **The store.** `var/datastores/` holds two sibling stores of the same run, `science-2026.8.0-large-M-dissipation`
  and `science-2026.8.0-reflections`. Each has its own shards. The prompt's references are the main store's
  (manifest purpose: "Figure 6 at laboratory M and its comparisons at larger M"). The glob
  `science-2026.8.0-shard*.db` matches its shards only.
- **PRyMordial writes no file by default.** Its `save_*` flags are False in `PRyM_init.py`, so parallel BBN stages
  do not collide.
- **The run's length is the user's ruling.** `CLAUDE.md` says that nothing a prompt runs should take longer than a
  few PRyMordial solves. That rule was never agreed by the user. The user ruled U11's bound of "about three hours"
  ("U11 - OK"). The registry's wrapper launches `main.py` only, and this batch writes no store. So it is detached
  and recorded as 01's and `reflection-restart` 04's batches were (ruling 2).

**Rulings.** Each is recorded in the log, with its class.
1. **The order of §1.4, made definite.**
   - Step 1 runs `zero` at A = 1 for all 30 histories. Then it runs `zero` at A = 0.1 for all 30, as the prompt
     says, although S2 would spare some of them. Where S2 holds and the A = 0.1 run exists, the line is `run`. It
     is compared with A = 1 under ruling 6, as a free check of S2, and a difference is a stop.
   - Then come the S2 and S1 decisions, and the `edge` runs at A = 1 and 0.1 that S1 does not cover.
   - Step 2 starts with **P's two alone** (`zero`, A = 0.01, two workers), and then makes the projection of
     ruling 5. Only then do the rest of A = 0.01's `zero` tasks run, in the order R, B, W, L. Group C at A = 0.01 is
     S2's, unless its A = 1 `zero` tuning met a one-loop candidate.
   - Steps 3 and 4 are as written.

   *IMPLEMENTATION CHOICE*, ruled here (§0, first and third points).
2. **Detached, not registered.** The batch is launched with `nohup`, its pid in `var/one-loop-drain/02/batch.pid`.
   Its stdout and stderr go to `batch.log`, and it writes `batch.end` when every child has ended. It is checked
   once that it started, and waited on by a background command that ends with it. It is not launched through
   `RunDrivers/registered_run.py`, which takes `main.py`'s arguments only. *STRUCTURALLY REQUIRED* (§0, last
   point; precedent: 01's batches).
3. **The driver runs the tool's own `main`.** Each task is a fresh `./venv/bin/python` subprocess, started from the
   repository root, not a fork. In it, the driver calls `tools.history_and_bbn.main(argv)` with the task's
   options (`BETA M --drain-law one_loop --alpha A --gap-prescription G`, the defaults otherwise). Its stdout goes
   to the task's own log under `var/one-loop-drain/02/tasks/`. Three captures are made around the call, each
   restored after it:
   - `ComputeTargets.ScalarHistory.integrate_scalar_history`, wrapped **before** `main` wraps it, so that it sees
     every integration. It **reduces each result at once** to the values §1.3 needs (N_final, `accepted_steps`,
     `reflections`, `nfev`, the candidates' regimes and the `drain` record's counts) and keeps no reference to the
     `IntegrationResult` or its `solution`;
   - `tools.history_and_bbn.tune_history`, to keep the payload;
   - `tools.history_and_bbn.compute_BBN_data`, replaced for the call by an object whose `_function` calls the
     real one and keeps its result.

   The tool's `main` then keeps one `IntegrationResult` (its own `captured[-1]`), as it does on its own. If a
   capture cannot be made this way, the agent composes the stages from the tool's imports instead and records an
   *IMPLEMENTATION CHOICE*. §1.3's tool check then carries the equivalence. *IMPLEMENTATION CHOICE*, ruled here
   (§0, third and fifth points).
4. **Each integration is streamed, so that a timeout keeps it.** The child appends one JSON line for each finished
   integration to `tasks/<task-id>.partial.jsonl`, flushed. On the 3600 s timeout the parent sends `SIGTERM`, and
   then `SIGKILL` after 30 s. It builds the task's line from the partial file, with outcome `timeout`, and waits for
   the child, so that nothing outlives the batch. *STRUCTURALLY REQUIRED* (§0, fourth point).
5. **Memory, workers and the projection.**
   - Each child records its peak RSS in its line (`resource.getrusage(RUSAGE_SELF).ru_maxrss`, which is in bytes on
     macOS).
   - At A = 1 and 0.1, six workers.
   - At A = 0.01 after P's two, the workers are min(6, ⌊8 GiB / the larger peak RSS of P's two⌋). Eight GiB is half
     the machine, which is shared.
   - **The projection** counts the 25 remaining `zero` tasks (W, R, B, L), plus an `edge` task for each of the 27
     histories of P, W, R, B and L. That is conservative, since S1 will spare few of them at A = 0.01 (§0.4). Each
     task is costed at the mean wall time of P's two, and the total is divided by the workers so found.
   - If the projection exceeds four hours, or the workers come to fewer than three, **stop and ask** with the
     numbers.

   *IMPLEMENTATION CHOICE*, ruled here (§0, third point).
6. **"Bitwise" for S1, S2 and the spot checks.** These are equal by `repr`:
   - per integration: N_final, `accepted_steps`, `reflections`, `nfev`, the sequence of regimes, and each
     Injection's N, `alpha_eff` and `delta_K`;
   - the tuning record (every key of `TUNING_OUTPUT_KEYS`);
   - the two SHA-256 digests;
   - Y_p, D/H, He3/H and Li7/H.

   Wall times differ and are not compared. So do the record's `gap_prescription` (S1) and `A` (S2), which differ by
   construction (§0, first point). Every other value of the line must also be equal. *As the prompt intends; made
   exact.*
7. **The R·S(L) bound.** For each classical candidate of the accepted integration:
   - L = M/φ_t, with M the model's M and φ_t from its `classical_turning_point`.
   - Where 40 ≤ L ≤ 134, ΔK_bound = exp(ln R + ln S(L) + `log_rho_kin` + 4N), with S = (L²S)/L² and L²S
     interpolated linearly in L between the table's rows.
   - Each history records three things:
     - the sum of ΔK_bound as a share of K(N_final), which is None where K(N_final) = 0;
     - the same sum as an r_χ today, Σ ΔK_bound · e^{−4N_final} / exp(`log_rhorad_Einstein` of the last sample),
       whose `raw_N` is N_final;
     - the count of classical candidates with L outside [40, 134].
   - The agent checks the r_χ form once on a drained history: K(N_final) · e^{−4N_final} / ρ_R,E(last sample)
     against its `rho_chi_ratio_today`. It states the relative difference, which is not a stop.

   *IMPLEMENTATION CHOICE*, ruled here (§0, sixth point).
8. **The fixed-law references** come from the main store only, `science-2026.8.0-shard*.db`, opened `mode=ro`. The
   two sibling stores are not read. *As the prompt intends; made exact* (§0, seventh point).
9. **The tool check (§1.3).** The CLI is run in a subprocess with the same argv as the driver's task. Its stdout is
   compared line by line with the driver task's captured stdout. Every line is byte-equal once the fields
   `wall=…` and `PRyM_time=…` are masked, in both the tune lines and the stage lines. A difference is a stop.
   *As the prompt intends; made exact.*
10. **No file but these:** `.documents/one-loop-drain/measure/` and its `out/` (text only);
    `.documents/one-loop-drain-verification.md` (§2 added); `logs/02-measure-the-law.md`; the board; and
    `.documents/OPEN_ISSUES.md` if §3 changes. Nothing under `var/` is committed. *STRUCTURALLY REQUIRED*;
    followed.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate, and the five suites before anything else. They run serially: ComputeTargets in its two halves, the
     `test_[g-z]` half and `tools/tests` in the background, waiting for each notification.
  2. The task list and `fixed_law.py`.
  3. §1.4's steps under ruling 1, each a detached batch.
  4. The spot checks and the tool checks.
  5. The analysis scripts.
  6. The suites again; the documents; the records; the commit.
- **Load.** Record the 1-, 5- and 15-minute loads at each batch's start and end, beside its wall time.
- **Scratch.** Outputs go under `var/one-loop-drain/02/`. Probes and suite outputs go in the agent's own
  subdirectory of the session scratchpad. Nothing goes in `/tmp`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is written. The 2026.8.0 store is
  opened `mode=ro` only.
- **Stop conditions are real:**
  - prompt §3's list;
  - ruling 5's projection or worker floor;
  - a difference under ruling 1's free S2 check;
  - a task whose failure is neither the wall search nor the inside-wall guard, but whose reason points at the
    drain law's own code (`drain_regime`, `drain_alpha_effective`) — report it, do not work round it.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `56d477f`, before this note was committed:
1. **The Gate above.**
2. **The reading of §0.**

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 10. Its log is `logs/02-measure-the-law.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **It starts nothing that outlives its turn** but the batches of §1.4, and every one of them must have ended before
  it commits.
- **Stop and ask** on any of the conventions' stop conditions.
- **It replies with:**
  - the log's path, the message as committed, and `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the batches: tasks run, derived by S1 and by S2, timed out and failed, with each batch's wall time, workers,
    loads and peak RSS;
  - ruling 5's projection, with its numbers;
  - the spot checks and the tool checks, each with its result;
  - the Measure gate, item by item (prompt §2.1–§2.6), with the headline numbers;
  - the handover for U13 and U14, verbatim;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — seven checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 10's files. Its parent is this note's commit, or a later commit
     that is not the agent's.
   - `git status --porcelain --untracked-files=all` is empty, and nothing is pushed.
   - `git diff <agent>^ <agent> --` over ComputeTargets, CosmologyConcepts, CosmologyModels, ObjectFactories, tools,
     config, RunDrivers, `main.py` and the plot scripts is empty.
   - `measure/out/` holds text only.
2. **Nothing left running.** No pid under `var/one-loop-drain/02/` answers `kill -0`, `batch.end` exists for every
   batch, and `runregistrykit list` is unchanged.
3. **The sample is complete.** `sample.jsonl` has one line for each of the 180 (history, A, prescription) tasks,
   each `run`, `S1`, `S2` or `timeout`, with no gaps and no duplicates. Re-derive every S1 and S2 decision from
   the lines myself, from each `zero` (and, for S2 under `edge`, `edge`) tuning's per-integration regime counts.
4. **A shortcut, re-run.** Run one task that S1 derived and one that S2 derived, other than the spot checks', with
   the driver in a process of my own. Compare each with the line it was derived from under ruling 6.
5. **The tool, re-run.** Run `tools/history_and_bbn.py` on one laboratory-M task at A = 1 (not 2197). Compare its
   `history`, `drain`, `dissipation` and `bbn` lines with the driver's line for it.
6. **The numbers.**
   - Recompute from `sample.jsonl`, in a scratch script, three headline values of verification §2:
     - the largest Δ(ΔN_eff today) between `edge` and `zero`;
     - 2197's ΔN_eff today at each A;
     - the count of histories that enter the gap at A = 0.01.
   - Recompute the R·S(L) bound for one history from its payload, by re-running it.
   - Check that one fixed-law reference row equals the store.
7. **The records.**
   - The log is in §5.1's form, with every deviation classed and §0's rulings among them.
   - The verification document's §1 is unchanged, and §2 follows prompt §2's six items. The U13/U14 handover
     states measurements and options, and makes no choice.
   - On the board: row 02, its items, the Measure gate, U13 and U14 marked "open for the user, with 02's
     measurements", and **Last updated**. `.documents/OPEN_ISSUES.md` matches §3, with its count and date.
   - `CLAUDE.md` is unchanged, and so are the suites' counts.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 02 landed, the Measure gate held, and this note added to the list of orchestrator notes, marked
  "used for 02";
- an *Orchestrator review of prompt 02* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the board's §3
and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the Measure gate: the sample, the regime shares and the gap's reach, the spread between the prescriptions, the
  comparison with the fixed law, and the cost;
- U13 and U14, put to the user with the measurements, for the user to rule in their own words;
- that 03 (the store) is written after those rulings.
