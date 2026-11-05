# Prompt 02d — measure the measured law

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-05, on branch `one-loop-drain`, after 02c landed (`18add05`, reviewed `2747741`; residue
`ff7aaeb`), at the user's request ("Can you write prompt 02d?"). It is the second prompt of README §8.10's re-plan. It
is written from:
- README §8.10 whole, including the three rulings the user made during 02c;
- prompt 02 and prompt 02b, with orchestrator notes 02 and 02b, whose method this prompt keeps where it does not say
  otherwise;
- 02b's log and `.documents/one-loop-drain-verification.md` §2b, in particular §2b.10 (the cost) and §2b.11 (the U14
  handover);
- 02c's log, its "State handed to the next prompt" in particular, and verification §2c;
- `.documents/one-loop-drain/measure/` and `measure-02b/`, read for this prompt (§0.2).

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Where it runs.** In the main checkout `/Users/ds283/Documents/Code/ChamPBH`, on branch `one-loop-drain`, from the
repository root, with `PYTHONPATH=.` and `./venv/bin/python`.

**Gate:**
- on branch `one-loop-drain`, at or after the commit that adds this prompt, with no tracked change in `git status`;
  `18add05`, `2747741` and `ff7aaeb` are ancestors of `HEAD`;
- `git diff 18add05 HEAD -- ComputeTargets CosmologyConcepts CosmologyModels ObjectFactories tools config RunDrivers
  main.py plot_by_beta.py plot_ScalarHistory.py` is empty, so the code is the code 02c's review passed;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- `var/one-loop-drain/02/sample.jsonl` (180 lines), `var/one-loop-drain/02/fixed_law.json` and
  `var/one-loop-drain/02b/sample.jsonl` (360 lines) exist;
- the 2026.8.0 store's 16 shards exist;
- `.documents/one-loop-drain-verification.md` has §2c and no §2d; `.documents/one-loop-drain/measure-02d/` and
  `var/one-loop-drain/02d/` do not exist.

If any of these fails, stop and ask.

**Closes:** nothing. **Narrows or extends:** `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`, with
what the re-tuned sample shows at A = 0.1 and 0.05 (§1.5). **Opens:** only what the work finds.

**Recommended model:** **Opus.** The work is 02b's on a changed law: a long detached batch, a new driver adapted from
02b's, and a report that replaces 02's and 02b's one-loop measurements and sets what 03 must store.

**Read first:**
1. [`README.md`](README.md) §8.10 whole; §8.5 and §8.6 for the gates and rulings it supersedes; §5.
2. The board's §1, §2 and §3, and its orchestrator reviews of 02b and 02c.
3. Prompt 02b and orchestrator note 02b whole; prompt 02 §1 and orchestrator note 02's rulings 2, 5, 6 and 9.
4. `logs/02b-measure-wide-A.md` and `logs/02c-the-measured-law.md`; verification §2b and §2c.
5. `.documents/one-loop-drain/measure/` and `measure-02b/`: every script, and the README that 02c added to each.
6. `ComputeTargets/ScalarHistory.py`: the law's block of constants, `DrainCandidate`, `DrainRecord`,
   `drain_fraction`, `drain_alpha_effective`, `drain_law_inputs`, and `candidate` in `integrate_scalar_history`.
7. `tools/history_and_bbn.py`: its `history` and `drain` lines under `--drain-law one_loop`.

---

## 0. Where this prompt sits

### 0.1 What it is for

02 and 02b measured the one-loop law as 01 built it: A·2.7/ℓ at a one-loop candidate, and a gap prescription. 02c
replaced both by the measured law of README §8.10. In that law:
- the one-loop fraction saturates (f ≤ 0.95, 0.9 below ℓ = 1);
- the gap drains by a fitted function of ln R and L;
- the classical regime drains A·R·S(L)·w_gate.

None of 02's or 02b's one-loop numbers holds under it. This prompt re-tunes 02b's thirty histories at U14's eight A
under the measured law, measures what 02b measured, and sets the result beside 02's and 02b's. **Its numbers replace
02's and 02b's as the campaign's measurement of the one-loop law.** 03 stores the law, and 08 hands it to the rebuild
of record.

**It changes no code.** Its scripts live in `.documents/one-loop-drain/measure-02d/`, and its outputs in
`var/one-loop-drain/02d/`. 02's and 02b's scripts, outputs and verification sections are not changed: they are the
record of the old law (invariant 6).

### 0.2 What 02, 02b and 02c left

- **The driver.** `measure/run_sample.py` runs the tool route one task per fresh process, and keeps every
  integration's `IntegrationResult.drain`, candidates and injections in `tasks/<id>.partial.jsonl`. It writes one JSON
  line per task, with a 3600 s timeout. 02b's `run_wide.py` imported it and overrode its globals.
- **Neither runs on this tree.** They pass `--gap-prescription` and read `DrainRecord.gap_prescription`, and 02b's also
  reads `DRAIN_C1` (02c's dated READMEs). 02b's import-and-override pattern was fragile: functions with paths bound as
  default arguments at import (orchestrator note 02b, §0) could not be redirected.
- **The tool route** is `tools/history_and_bbn.py BETA M --drain-law one_loop --alpha A`.
  - Its history line names `model=one-loop-measured-v1`.
  - Its `drain` line gives the counts by regime, the K shares by regime and parking, ℓ_min,
    `unresolvable_assessments` and `unresolved_declined`.
- **The record.** `DrainCandidate(regime, ell, alpha_eff, w_gate, log_E, log_H, L, log_R)` carries the law's inputs.
  So f at every candidate can be recomputed exactly, as `drain_fraction(regime, ell, L, log_R)`.
- **Classical candidates now drain.** Each one with α_eff > 0 takes `drain` and a restart, where under the old law it
  took the α_eff = 0 branch. The ladder at A = 0.2 met 83. In crossover, R-632 at A = 0.1 met 3269 without parking.
- **The gap clamp.** Where 1 + q ln L ≤ 0 (L ≤ 0.3045), the gap drains nothing, by the user's ruling. No candidate of
  02c's runs reached it. The B group at 10⁻³³ M_P is the likeliest place.
- **What 02c's maps suggest.** Crossover ran the measured law at 02b's old-law normalizations, not re-tuned:
  - every history that tuned at A = 0.05 under the old law still parks;
  - R-632 parks from A = 0.5 and not at 0.1;
  - the 5 × 10⁻²⁰ ladder parks from A = 0.2 and not at 0.1.

  Re-tuned, these may move.

### 0.3 No shortcuts

02's two shortcuts do not carry over.
- **S1** compared the two gap prescriptions, which no longer exist.
- **S2** held where A was read only at drains that were exactly zero. Under the measured law, every assessed candidate
  with f > 0 reads A: one-loop, gap above r₀, and every classical candidate whose R·S(L) does not underflow. In 02's
  group C nothing drained; here every classical candidate drains.

So every task is run.

### 0.4 The estimate

From 02b's costs (verification §2b.10), every tuning there took one integration. Per tuning:
- **P, W, R and B at A ≥ 0.5:** 12–30 s.
- **At A = 0.05:** 18–131 s, and 354 s for R-632 under `edge`.
- **The ladder at A = 0.05:** 711–942 s, with the classical tails near the step budget.

Under the measured law:
- **One-loop drains are smaller at small ℓ.** The plateau is 0.9, against 2.7/ℓ, so cascades may run longer before
  parking at small A.
- **The classical tail now drains,** with a restart at each candidate. This may shorten a tail, since the field parks
  sooner, or lengthen it, since restarts cost steps. Groups L and C are where it matters, and C is new: 02b never ran
  it. In 02, with nothing drained, a C tuning took 48–88 s for C-3016 and C-3233, and 1822–1832 s for C-3873, at both
  A = 1 and 0.1.

**The total.** C-3873 alone, at eight A and 02's cost, is about 4 h of CPU. Each of its tasks runs about half the
3600 s timeout, so classical drains could push it over. The rest is about 2–4 h. In all that is about 6–8 h of CPU, or
1.5–2.5 h at six workers. The batch's wall time is bounded below by C-3873's tasks.

The unknowns are C at every A, and L and R-632 at A ≤ 0.1. §1.4's projection replaces this estimate with a measured
one.

## 1. What is wanted

### 1.1 The sample

02's thirty histories (prompt 02 §1.1, groups P, W, R, B, L and C), at the same settings:
- re-tuned under the one-loop law;
- φ* = 5 M_P, T_init = 2 × 10⁴ GeV, and the tool's defaults otherwise (tolerance 10⁻⁵, at most 30 integrations, small
  network).

Each history is run at A ∈ {10, 5, 2, 1.5, 1, 0.5, 0.1, 0.05}, U14 as the user re-confirmed it (README §8.10): 240
tunings. A = 0.01 is not run.

### 1.2 The scripts

They go in `.documents/one-loop-drain/measure-02d/`, and the outputs in `var/one-loop-drain/02d/`.
- **Self-contained.** 02d's scripts do not import 02's or 02b's modules. Where a function of theirs serves, it is
  copied into 02d's with the changes this prompt needs. Each copy names its source file and function in a comment,
  and the log lists every copy and what changed.
- **02's driver properties hold unchanged:**
  - a fresh process per task;
  - the tool's stages, imported from `tools/history_and_bbn.py`, not copied;
  - every integration's `IntegrationResult` kept, by wrapping `integrate_scalar_history`;
  - streamed partial files;
  - SIGTERM, then SIGKILL after 30 s, on the 3600 s timeout;
  - peak RSS recorded;
  - nothing outliving the batch.
- **The task list** is written from 02's table and the store's check (02's `tasks.py`, copied), with the argv
  `BETA M --drain-law one_loop --alpha A` and task ids `<group>-<serial or M>-A<A>`. 02's table is the source of truth
  for M, β and the store's α.

### 1.3 Each line

Each line carries what 02b's line carried (prompt 02 §1.3 and 02b §1.3), with these changes:
- **Removed:** the gap prescription; S1 and S2; the R·S(L) bound. The classical drain is now the realized
  A·R·S(L)·w_gate, so a bound on it is no longer a separate quantity.
- **The record's model:** `DrainRecord.model`, checked to be `one-loop-measured-v1` on every one-loop integration.
- **The cap:** the one-loop, gap and classical candidates at which A·f ≥ α_cap, with f recomputed by
  `drain_fraction` from the candidate's record.
  - On one capped run, check the count against α_eff = α_cap·w_gate.
- **By regime,** from the accepted integration:
  - the count;
  - the count that drained (α_eff > 0);
  - the drained share of K(N_final);
  - the range of L and of ln R.
- **The law's edges:**
  - gap candidates with 1 + q ln L ≤ 0 (the clamp);
  - gap candidates with ln R ≤ r₀;
  - classical candidates with L > 134 (the continuation) and with L < 0.3 (S held);
  - classical candidates whose α_eff underflowed to 0.0.
- **The first impact,** as in 02b §1.3: the first candidate's regime, N and α_eff; its ΔK as a share of K(N_final);
  and the drains before parking.
- **The cost of the drains:** the number of drains, and the integration's accepted steps and RHS evaluations, so that
  the restarts' cost can be set against 02b's.

**The fixed-law references** are 02's `var/one-loop-drain/02/fixed_law.json`, read as it stands.

### 1.4 The order, and the budget

1. **The suites,** then the driver's checks:
   - **the tool check:** `tools/history_and_bbn.py` itself, on P-2197 at A = 10, with stdout compared to the driver's
     with `wall=` and `PRyM_time=` masked (as note 02's ruling 9);
   - **the determinism check:** P-2197 at A = 1 and L-M1e-16 at A = 1, each run twice in separate batches. The second
     run must be bitwise equal to the first, in note 02's ruling-6 sense (with the record's model in place of its gap
     prescription).

   A difference in either is a stop.
2. **The A = 1 pass,** every group, all thirty: the cost of C and L under the measured law at a known A.
3. **The passes at A = 10, 5, 2, 1.5 and 0.5,** every group.
4. **The projection.** Run P's two at A = 0.05, then project the rest of A = 0.1 and 0.05.
   - Cost each task at its own history's A = 0.5 wall time, multiplied by that history's ratio of A = 0.05 to A = 0.5
     wall time in 02b (02 for C, using A = 0.1 against A = 1). Take P's measured ratio where it is larger.
   - Divide the total by the workers below.
   - If it exceeds three hours, or the workers come to fewer than three, stop and ask with the numbers.
5. **The passes at A = 0.1 and 0.05.**

**Workers.** min(6, ⌊8 GiB / the largest peak RSS seen so far⌋), as note 02's ruling 5. At most eight processes of this
prompt run at once over everything; the machine is shared.

A task that reaches the 3600 s timeout is recorded with what it finished, and is not rerun.

### 1.5 The failures

- A failure at A = 0.1 or 0.05 of 02's two kinds (the step budget; "the effective minimum was not found") extends
  `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`. It is added to the board entry as a dated
  paragraph, without rewriting the entry's text. Its row in `.documents/OPEN_ISSUES.md` is changed to say what holds
  under the measured law.
- If no history fails at A = 0.05 or 0.1, the entry is **narrowed** to A = 0.01, which U14 leaves out. The log and the
  board say so, and the user decides whether it closes.
- A failure of another kind, at any A, opens an issue of its own.
- A failure whose reason points at the law's own code (`drain_fraction`, `drain_alpha_effective`, `drain_law_inputs`,
  the model's `_drain_L`), or at the wall search or the inside-wall guard, is a stop.

### 1.6 What is not wanted

- **No code change.** Nothing under `ComputeTargets/`, `CosmologyConcepts/`, `CosmologyModels/`, `ObjectFactories/`,
  `tools/`, `config/`, `RunDrivers/`, `main.py` or the plot scripts changes, and no test is added.
- No change to 02's or 02b's scripts, outputs or verification sections, or to 02c's.
- No store is written, no Ray, no cluster. The store is opened read-only (`mode=ro`), if at all.
- No ruling on the law or on the A values: the prompt measures and states; the user rules.
- No threshold on the results. Y_p and D/H are set beside the criteria of `bbn_constraints_explainer.tex` as context,
  never as a pass or a failure.
- No diagnosis of the failures beyond what §1.5 records.
- No merge, no push.

## 2. The Remeasure gate (README §8.10)

The gate holds when all of these are in the verification document's new §2d, each from a script of `measure-02d/`
with its output in `measure-02d/out/` (text only):

1. **The sample measured.** Every task's outcome, by group and A, with the failures and timeouts and their reasons.
   Also the tool check and the determinism check.
2. **The regime shares across the eight A.** Per history and A:
   - the K shares by regime and the parking leftover's;
   - the drains by regime;
   - ℓ_min;
   - where the cascade enters the gap and where it reaches R ≤ 1;
   - the law's edges (§1.3);
   - U9's count (w_gate < 1 at a drain) and P19's (unassessed).
3. **Parking and the first impact.** Per history and A:
   - the parking N, or none;
   - the first impact's regime and fraction;
   - the drains before parking;
   - how often the cap acts, by regime.
4. **The observables.** ΔN_eff at 1 MeV and today, r_χ today, Y_p and D/H, per history and A, laboratory M first
   (Fig 6, README §8.0).
5. **Against the old law.** Each (history, A) set beside 02's (A = 1, 0.1) or 02b's (the other six) `zero` line:
   - the tuning outcome;
   - the parking N;
   - the regime counts;
   - ΔN_eff at 1 MeV and today, Y_p and D/H.

   State where the two laws differ most, and where they agree.
6. **Against the fixed law,** as in 02 §2.4, at all eight A.
7. **The cost:**
   - wall time, integrations, accepted steps and RHS evaluations per tuning, by group and A, beside 02b's;
   - the classical drains' share of the steps where they are many;
   - the batches' wall times, the workers, and the load averages at each batch's start and end.
8. **The handover:**
   - for 03, the ranges a store must hold: candidates and drains per history (the bounce rows), the largest, and the
     range of L and ln R;
   - for the user, which (history, A) cannot be built on this code, and how the observables move with A under the
     measured law.

   Both are measurements and the options they bear on, not a choice.

## 3. Verification

- **The suites, before anything else,** per `CLAUDE.md`, serially (ComputeTargets in two halves, the `test_[g-z]`
  half and `tools/tests` in the background), with counts, times and `uptime`. They must stand at 26, 594 (162 + 432),
  130, 156 and 68, all passing. A count that differs is a stop. Run them again before the commit; the counts must not
  change.
- **The batches** are launched detached (`nohup`, pid recorded) and checked once to see that they started. Each is
  waited on by a background command that ends with it, not by polling. They are not registered: they are diagnostic
  tunings of minutes each, as 02's and 02b's were. All batches must have ended before the commit, and nothing outlives
  the prompt.
- **Load.** Record the 1-, 5- and 15-minute loads at each batch's start and end, beside its wall time.
- **Scratch.** Outputs go under `/Users/ds283/Documents/Code/ChamPBH/var/one-loop-drain/02d/`. Probes and suite
  outputs go in the agent's own subdirectory of the session scratchpad. Nothing goes in `/tmp`, and nothing is written
  under `var/one-loop-drain/02/`, `02b/` or `02c/`.
- **Reproducibility.** Every number in §2d comes from a script in `measure-02d/` with its output in `measure-02d/out/`,
  or from a line of `var/one-loop-drain/02d/sample.jsonl` (or of 02's or 02b's) that the document names.

**Stop conditions:**
- a suite count that changes, or a suite that fails;
- the tool check or the determinism check not bitwise;
- the projection over three hours, or fewer than three workers (§1.4);
- `DrainRecord.model` other than `one-loop-measured-v1` on a one-loop integration;
- a failure that §1.5 names as a stop;
- any change that §1.6 forbids becoming necessary.

## 4. The commit, the log and the board

One commit (invariant 1), with:
- `.documents/one-loop-drain/measure-02d/` (the scripts) and its `out/` (text only);
- `.documents/one-loop-drain-verification.md` §2d, added after §2c, which is not changed (invariant 6);
- the log `logs/02d-measure-the-measured-law.md` (template: `dk-and-rpk-migration` README §5.1), with:
  - every deviation classified;
  - every copied function and what was changed (§1.2);
  - "Observations not acted on";
  - "State handed to the next prompt": what 03 needs (§2's item 8), and any regime, record field or range that 02c's
    tests did not meet;
- the board: row 02d, its items, the Remeasure gate, §3 for anything extended, narrowed or opened, and **Last
  updated**;
- `.documents/OPEN_ISSUES.md` in the same commit, as §1.5 says.

Nothing under `var/` is committed. Format the Python files you add with `black`. Stage by explicit path, and show
`git diff --cached --name-status` before committing. The commit message follows `CLAUDE.md` invariant 5. Write "this
commit" where the SHA goes; the orchestrator substitutes it.
