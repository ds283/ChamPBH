# Orchestrator — prompt 01, the drain law

Read [`../README.md`](../README.md) first: §5, §8 whole, and §8.8 in particular (the user's ruling "Add the
wall-search fix to `one-loop-drain` 01", and P21–P25). Then the prompt. This note binds 01 only. The conventions
of `reflection-restart`'s notes bind here as well: a tolerance is a number with its reason, timing is taken under
load, and the suites run serially.

**You do not write code.** You may:
- run the suites, the scanner, the scripts the log names and the comparisons;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`01-the-drain-law.md`](../01-the-drain-law.md) · **Gate:** "Law" · **Closes:**
`[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]` if §2.1 holds; closes or narrows
`[00-a-zero-drain-history-stops-at-the-inside-wall-guard]` · **Opens:** only what the work finds · **Model:**
Opus, as the prompt recommends · **Network:** none. Nothing is pushed.

**When:** at once, at the user's request of 2026-11-03 ("we need the orchestration note for prompt 01 in the
one-loop-drain campaign, and to dispatch it").

**Gate**, checked at `9616e04` on 2026-11-03 at 06:52 UTC, before this note was committed:
- `one-loop-drain` is checked out **in the main checkout** at `9616e04`, the commit that adds prompt 01.
  `git status --porcelain` is empty, and `1561b60` is an ancestor of `HEAD`. The worktree
  `.claude/worktrees/quanta-diagnostics` was removed the same morning at the user's request. It was clean,
  with no ignored files, and `git worktree list` now shows the main checkout only.
- `git diff reflection-restart HEAD --` over the prompt's code paths is empty.
- `./venv/bin/python -m runregistrykit list`: 23 runs, all finished.
- The store is `var/datastores/science-2026.8.0.db` with 16 shards `science-2026.8.0-shard00NN.db`. 2197 is in
  `shard0005`, 2188 in `shard0003`, 3016 and 3233 in `shard0002`, and 3873 in `shard0007`. Each is a success
  and validated.
- `.documents/one-loop-drain-verification.md` and `.documents/one-loop-drain/` do not exist.
- `reflection-restart` 04's inputs exist under `var/reflection-restart/04/`:
  - `tasks-42.json` and `the-42.jsonl` (42 lines, 41 successes; 4389 fails in `effective_minimum`);
  - `tasks-nine.json` and `the-nine.jsonl` (9 lines, 8 failures in `classical_turning_point`, and
    `shard0015#2386` a success);
  - `tasks-pair.json` and `pair-this.jsonl` (2197 and 2188 at the stored normalization, both failing in
    `classical_turning_point`).
- The suites were not re-run for this note. `reflection-restart` 04 left them at 26, 514 (122 + 392), 130, 147
  and 68, all passing, and only `test_release.py` changed under any `tests/` since. The agent measures them
  again before changing anything (prompt §4).
- Load averages 2.28 / 3.89 / 4.66. The machine is shared.

## 0. What the orchestrator found

For this note the orchestrator read the probe's patch, driver and JSON summaries, and `alpha_effective`,
`candidate`, `peak_estimate`, `history_margins` and the result types in `ComputeTargets/ScalarHistory.py`. It
also read the callers of `classical_turning_point` and `assess_infall`, `one_loop_turnaround`'s search, 04's task
lists and results, and `close/failing_call.py` and `close/out/nine_margins.txt`. Here is what it found where the
prompt is silent, loose or wrong.

- **The prompt's "Where it runs" is out of date.** It names the worktree, which no longer exists, and says that
  the main checkout is on `reflection-restart` and is not touched. The branch is now checked out in the main
  checkout. The agent works there, from the repository root, with `./venv/bin/python`, and commits there.
  `var/` is the checkout's own.
- **The store path is a prefix.** The prompt's `var/datastores/science-2026.8.0` names the store as
  `tools/outward_step_scan.py` and `failing_call.py` take it. The files are `science-2026.8.0.db` and its 16
  shards.
- **The probe's base is the merged tree's code.** The probe ran on `2d50b95`. The diff from `2d50b95` to
  `reflection-restart` over `ComputeTargets`, `CosmologyConcepts`, `CosmologyModels`, `tools` and `config` is
  empty, so the bitwise comparison with the probe (prompt §2.2.1) is meaningful on this tree.
- **The probe passed the stored α, not A.** `drain_probe.py` calls `integrate_history(..., alpha=alpha)` with the
  stored α, and the patch overrides α_eff from `PROBE_DRAIN_A`. Inside `integrate_scalar_history`, α is read only
  as the test `alpha > 0` and in `alpha_effective`, so passing A in its place moves nothing else. The probe's
  α_eff is `min(_PROBE_A * 2.7 / ta.ell, 0.99)`, and in the gap with `high` it is `_PROBE_A * 2.7 / 4.0`. Both are
  evaluated left to right and never multiplied by w_gate. Bitwise agreement needs the same order of operations
  (ruling 3).
- **`gate_weight` has one caller,** `alpha_effective` (:1857), which reads it as a module global. Patching
  `ScalarHistory.gate_weight` in a script's own process therefore reaches every candidate. The resolved ones then
  record w_gate = 1.0 where the probe recorded the gate's value. The comparison is on (N, α_eff, ΔK), so that is
  not a difference.
- **The probe's `cands` carry no regime and no ℓ.** Their fields are `N`, `N_turn`, `kind`, `H_before`,
  `pi_before`, `log_rho_kin`, `log_k_ex`, `log_R`, `log_omega_t_squared`, `phi_t`, `alpha_eff` and `delta_K`. The
  regime counts and ℓ's extremes are in `by_regime` only. The comparison with the probe uses what exists: each
  candidate's (N, α_eff, ΔK) bitwise, and the counts and the ℓ and α_eff extremes in `by_regime`.
- **P24 and `peak_estimate` disagree where ℓ ≤ 0.** `peak_estimate` returns `ESTIMATE_ONE_LOOP` whenever
  `one_loop_turnaround` returns a solution, whatever its ℓ. Under P24 the loop calls such a candidate `gap`. The
  prompt's "identical to what `peak_estimate` would compute" (§1.2) and "its regime agreeing with
  `peak_estimate`'s" (§2.2.5) cannot both hold there (ruling 4).
- **Where the reuse can travel.** `Injection` is a namedtuple. The payload's `"injections"` key carries it, and
  `ObjectFactories/ScalarHistory.py` reads that key. A field added to it would change an existing payload key
  under the fixed law. `IntegrationResult` gained `restart_retakes` with a default in `reflection-restart` 03,
  which is the precedent for a new field (ruling 5).
- **"The eleven calls" are nineteen inputs.** Prompt §0.3 counts 2 + 9 calls and then names eight more. The
  eight are the nine's failures on the fixed tree, with their own φ and ln E (`nine_margins.txt`, "this tree").
  So 19 inputs have been measured to raise:
  - 2197 and 2188, from the probe's logs (`failing_call.py`'s `PROBE`);
  - the nine's stored failing calls (their `failure_reason` in the store);
  - the eight fixed-tree failing calls (`the-nine.jsonl`, `nine_margins.txt`).

  A test may not open the store (`CLAUDE.md`), so the inputs are copied into the test module as literals
  (ruling 6).
- **`one_loop_turnaround` does not call `classical_turning_point`.** It runs its own `_search_log_field`
  (`AbstractModel.py` :672) and reads `_log_V_minus_floor` itself (:689). Under P21 neither changes, so the fix
  cannot move a one-loop classification or a `peak_estimate` reason string. The only callers of
  `classical_turning_point` are `RealizedModel.classical_turning_point` (:351) and `assess_infall` (:1171). The
  callers of `assess_infall` are `natural_floor_warning` (:2208) and the loop's assessment (:2588).
- **The batches of §2.1 already exist as task lists.** `tasks-42.json` is §2.1.2's replay. `tasks-pair.json`
  holds 2197 and 2188 at the stored normalization with α = 10⁻³ (§2.1.4), with the store's shard paths and
  serials. `tasks-nine.json` is §2.1.5's nine. All three are fixed-law specs that the scanner runs unchanged.
- **Line references.** `natural_floor_warning` is at :2190, not :2189. The others in the prompt's "Read first"
  match the tree.

**Rulings.** Each is recorded in the log, with its class.
1. **Where it runs.** In the main checkout `/Users/ds283/Documents/Code/ChamPBH`, on `one-loop-drain`, from the
   repository root, with `PYTHONPATH=.` and `./venv/bin/python`. This replaces the prompt's "Where it runs" and
   §3's "Nothing in the main checkout but …". The agent writes only the files of ruling 9 and under
   `var/one-loop-drain/01/`. It reads `var/reflection-restart/04/` and the store, both read-only. The Gate's
   first item is read as above. *STRUCTURALLY REQUIRED* (§0, first point).
2. **The batches.** These run through `tools/outward_step_scan.py batch` on this tree, detached:
   - §2.1.2 runs `var/reflection-restart/04/tasks-42.json` with `--jobs 6 --timeout 1800`, compared field by
     field with `the-42.jsonl`;
   - §2.1.4 runs `tasks-pair.json` with `--jobs 2 --timeout 1800`, beside the replay (eight workers in all);
   - §2.1.5 runs `tasks-nine.json` with `--jobs 6 --timeout 3600`, after the replay has ended. At most two
     one-loop runs of §2.2 may run beside it.

   The scanner is not changed. Outputs go to `var/one-loop-drain/01/the-42.jsonl`, `pair.jsonl` and
   `the-nine.jsonl`, each with its `.pid`, `.log` and `.end` as 04 kept them. *IMPLEMENTATION CHOICE*, ruled here
   (§0, tenth point).
3. **The law's arithmetic.**
   - At a one-loop candidate, α_eff = `min(ALPHA_CAP, A * C1 / ell) * w_gate`, with `A * C1 / ell` evaluated left
     to right.
   - In the gap under `edge`, α_eff = `min(ALPHA_CAP, A * C1 / ELL_EDGE) * w_gate`.
   - Under `zero`, and for classical and unassessed candidates, α_eff is `0.0` exactly, and the caller takes the
     α_eff = 0 branch it takes today.

   With w_gate = 1.0 these are the probe's values bit for bit. *IMPLEMENTATION CHOICE*, ruled here (§0, fourth
   point).
4. **ℓ ≤ 0.** `peak_estimate`'s result is not changed by the one-loop law. P18's reuse returns exactly what
   `peak_estimate` computes today, `ESTIMATE_ONE_LOOP` with the turnaround's values included where ℓ ≤ 0. The
   `"drain"` record's `regime` is the loop's own classification, `gap` there (P24). §2.2.5's agreement is
   checked for every candidate except those, and the log counts them. If any arise, each is listed with its N and
   ℓ, and that is not a stop. *STRUCTURALLY REQUIRED* (§0, seventh point).
5. **No existing type gains a field that an existing payload key carries.**
   - `Injection` and `PeakEstimate` are unchanged.
   - `IntegrationResult` may gain fields with defaults, as `restart_retakes` did: the `"drain"` record, and
     whatever P18's reuse needs to reach `history_margins`. Those fields stay out of every existing payload key.
   - `peak_estimate` may gain an optional argument for the reuse, whose default is today's behaviour.

   Under the fixed law, `history_margins` calls `peak_estimate` exactly as today. *STRUCTURALLY REQUIRED*
   (§0, eighth point).
6. **The failing calls.** §1.8's "eleven calls" and §2.1.3's are the 19 inputs of §0, ninth point. In the tests
   they are literals: (β, M/M_P, normalization, φ, ln E), with each one's source named in a comment. §2.1.3's
   script also reads the nine stored ones from the store, read-only, and shows that they equal the literals. For
   each input, the predicate holds, the call now returns None, and the pre-fix copy raises with the message
   recorded. *As the prompt intends; made exact* (§0, ninth point).
7. **The probe comparison (§2.2.1).**
   - The script builds each history as `drain_probe.py` does: the same `_z_grid()`, `T_INIT_GEV`, `PHI_INIT_MP`,
     `PI_INIT`, the cosmology, `ExponentialModel(0, M_value(0, M_eV·eV), 1, beta_value(0, β), units)` and
     `realize(normalization, phi_ref)`, from the store's row.
   - It passes `alpha=A, drain_law="one_loop"` and sets no environment variable.
   - It compares each candidate's (N, α_eff, ΔK) with the probe's `cands`, bitwise and in order. It compares the
     candidate and drain counts, `by_regime`'s counts and its ℓ and α_eff extremes, the parking N, r_χ today and
     ΔN_eff today with the probe's JSON, also bitwise.
   - A difference is a stop, with the first differing candidate.

   *IMPLEMENTATION CHOICE*, ruled here (§0, sixth point).
8. **The test counts.** The baselines are 26, 514 (122 + 392), 130, 147 and 68, measured again first. A count may
   rise only by this prompt's new tests, each named in the log, and no count may fall. An existing test changes
   only if it pins the payload's key set or `IntegrationResult`'s fields, and the log names each one. *As the
   prompt says; restated.*
9. **No file but these:** prompt §5's list; `.documents/one-loop-drain/law/` and its `out/`; and
   `.documents/OPEN_ISSUES.md` and `CLAUDE.md` as §5 says. Nothing under `var/` is committed. *STRUCTURALLY
   REQUIRED*; followed.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate (ruling 1), and the five suites before anything else: serially, ComputeTargets in its two halves,
     the `test_[g-z]` half and `tools/tests` in the background, waiting for each notification.
  2. The predicate, the rule and their tests (§1.6, ruling 6). The check of the 19 inputs (§2.1.3) runs
     before anything else is changed, so that the fix is measured alone.
  3. The law, the regime, the gap prescription and the `"drain"` key (§1.1–§1.4), with their tests.
  4. The tool route (§1.5).
  5. §2.1's batches (ruling 2), detached. Nothing in `ScalarHistory.py` or `AbstractModel.py` is edited while
     they run. If an edit is needed afterwards, the batches are run again on the final tree.
  6. §2.2's one-loop runs, each in its own process.
  7. 3873 (§1.7), within its bound.
  8. The suites again; the documents; the records; the commit.
- **Long runs.** Each batch or script is launched detached (`nohup`, pid recorded), checked once, and waited on by
  a background command that ends with it, not by polling. No more than eight workers run at once.
- **Scratch.** Outputs go under `var/one-loop-drain/01/`. Probes and suite outputs go in the agent's own
  subdirectory of the session scratchpad. Nothing goes in `/tmp`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is written except temporary ones in
  tests. The 2026.8.0 store is opened read-only only (`mode=ro`).
- **Stop conditions are real:**
  - a suite count that falls, or rises beyond ruling 8;
  - a golden that fails, or a replay task that differs (§2.1.1, §2.1.2);
  - a failing input at which the predicate does not hold, or the pre-fix copy does not raise (ruling 6);
  - a wall-search failure in §2.1.4 or §2.1.5;
  - the predicate's cost over 1% (§2.1.6);
  - any difference from the probe under ruling 7;
  - 3873's cause outside this prompt's files, or a fix that would move the fixed law (§1.7);
  - any change that the prompt's §3 forbids becoming necessary.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `9616e04`, before this note was committed:
1. **The Gate above.**
2. **The reading of §0.** The probe's patch, driver and `2197-A1.json`; `alpha_effective`, `candidate`,
   `peak_estimate`, `history_margins`, `Injection` and `IntegrationResult`; the callers of `classical_turning_point`,
   `assess_infall` and `gate_weight`; `one_loop_turnaround`'s search; 04's three task lists and their results;
   `failing_call.py` and `nine_margins.txt`.

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 9. Its log is `logs/01-the-drain-law.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **It starts nothing that outlives its turn** but the batches and scripts of §2, and every one of them must have
  ended before it commits.
- **Stop and ask** on any of the conventions' stop conditions.
- **It replies with:**
  - the log's path and the message as committed, and `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`, and the tests added by name;
  - the predicate: its form, its cache, the boundary values and the 19 inputs' table;
  - the law: where the regime and α_eff sit in `candidate`, by line, and how P18's reuse reaches
    `peak_estimate`;
  - the Law gate, item by item: §2.1.1–§2.1.6 and §2.2.1–§2.2.5, with each batch's table and each comparison's
    result;
  - 3873: the branch, the assessment, the cause, and what was done;
  - the breakage record;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — seven checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 9's files. Its parent is this note's commit, or a later commit
     that is not the agent's. `git status --porcelain --untracked-files=all` is empty, and nothing is pushed.
   - `git diff <agent>^ <agent> -- config main.py plot_by_beta.py plot_ScalarHistory.py ObjectFactories
     tools/outward_step_scan.py` is empty.
   - `_search_log_field`, `WALL_ROUNDING_QUANTA` and `WALL_SEARCH_HALVINGS` are unchanged.
   - `VERSION_LABEL` is 2026.9.0 and `FINGERPRINT_FORMAT` is 6.
2. **Nothing left running.** No pid under `var/one-loop-drain/01/` answers `kill -0`, and `runregistrykit list` is
   unchanged.
3. **The fixed law.** Run `test_golden_history`, `test_golden_reflecting_history`, `test_tuning` and
   `test_store_fingerprint_golden` myself. Re-run three of the 42 on the commit's tree and compare them with 04's
   lines, one of them 4389.
4. **The fix.** Read the predicate and the rule against P21. Run the new tests. In a scratch probe, call the
   pre-fix copy and the new method on two of the 19 inputs and on one input just across the boundary.
5. **The law.** Read `candidate` against ruling 3 and §1.2's table. Re-run 2197 at A = 1 with `gate_weight`
   patched, and compare it with the probe's `cands` myself.
6. **The tool route.** Run `history_and_bbn.py --help`, check that its fixed-law output is byte-identical to the
   pinned tests, and make one `--drain-law one_loop` run of a short history.
7. **The records.**
   - The log is in §5.1's form, with every deviation classed and §0's rulings among them.
   - The verification document is new, with §1 only, and `law/out/` holds text only.
   - The numerical-strategies and architecture-summary notes are dated.
   - On the board: row 01, its items and the Law gate. §3/§4 are updated for the two issues, and
     `.documents/OPEN_ISSUES.md` matches, with its count and date. `CLAUDE.md`'s counts are the measured ones.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 01 landed, the Law gate held, and this note added to the list of orchestrator notes, marked
  "used for 01";
- an *Orchestrator review of prompt 01* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the
board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the Law gate: the fixed law (goldens, replay, the failing inputs, 2197 and 2188, the nine), the one-loop law
  against the probe, and 3873;
- the issues closed or narrowed;
- that 02 (the measurement, then U13–U14) is written next.
