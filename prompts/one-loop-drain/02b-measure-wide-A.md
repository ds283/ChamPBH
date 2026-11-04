# Prompt 02b — measure the law over a wider range of A

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-04, on branch `one-loop-drain`, after 02 landed (`3f3f0cf`, reviewed `2732535`) and the user
ruled U13 (`5ae8648`). It is inserted at the user's request, in their words: "Can we try `A \in { 0.05, 0.1, 0.5,
1.0, 1.5, 2.0, 5.0, 10.0 }`?", and then "Running a few histories where the condensate loses most or almost all of its
KE on the first impact might be worth doing, to get a measurement of what happens when the collision is mostly
inelastic. A=5 and A=10 sound like they probe slightly different configurations in this regime, so leave those.
Let's test first using a prompt 02b". It is written from:
- README §8 whole, and U13 as ruled (§8.6, P26);
- prompt 02, `02-measure-the-law.md`, and orchestrator note 02, `orchestrator/prompt-02.md`, whose rulings govern
  here where this file does not say otherwise;
- 02's log, `logs/02-measure-the-law.md`, and `.documents/one-loop-drain-verification.md` §2;
- 02's raw outputs under `var/one-loop-drain/02/`, read for this prompt (§0.3).

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Where it runs.** In the main checkout `/Users/ds283/Documents/Code/ChamPBH`, on branch `one-loop-drain`, from the
repository root, with `PYTHONPATH=.` and `./venv/bin/python`.

**Gate:**
- on branch `one-loop-drain`, at or after the commit that adds this prompt, with no tracked change in
  `git status`; `3f3f0cf`, `2732535` and `5ae8648` are ancestors of `HEAD`;
- `git diff b99f6a4 HEAD -- ComputeTargets CosmologyConcepts CosmologyModels ObjectFactories tools config RunDrivers
  main.py plot_by_beta.py plot_ScalarHistory.py` is empty, so the code is the code 02 measured;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- `var/one-loop-drain/02/sample.jsonl` has 180 lines, and `var/one-loop-drain/02/tasks/` holds the partial file of
  every task 02 ran;
- the 2026.8.0 store's 16 shards exist;
- `.documents/one-loop-drain-verification.md` has §2 and no §2b; `.documents/one-loop-drain/measure-02b/` and
  `var/one-loop-drain/02b/` do not exist.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds. It hands U14 (the A values of the rebuild of record) back
to the user with measurements at nine A (§2.6).

**Recommended model:** **Opus**. The work is 02's, extended: a long detached batch whose shortcuts must be argued,
and a report on which the user rules U14.

**Read first:**
1. [`README.md`](README.md) §8 whole; §5.
2. The board's §1, §2 and §3, and its orchestrator review of 02.
3. Prompt 02 whole, and orchestrator note 02 whole.
4. `logs/02-measure-the-law.md`, and `.documents/one-loop-drain-verification.md` §2.
5. `.documents/one-loop-drain/measure/`: every script, and its `out/`.
6. `ComputeTargets/ScalarHistory.py`: `DRAIN_*`, `drain_regime`, `drain_alpha_effective`, `DrainRecord`,
   `DrainCandidate`, and where the step loop reads `alpha`.

---

## 0. Where this prompt sits

### 0.1 What it is for

02 measured the one-loop law at A ∈ {0.01, 0.1, 1}. This prompt measures it at six more values, A ∈ {0.05, 0.5, 1.5,
2, 5, 10}, on the same thirty histories, under both gap prescriptions, so that the user can rule U14 from nine
values of A. It has two aims, in the user's words above:
- **a wider scan of A** for U14, from 0.05 to 10;
- **the mostly inelastic first impact.** At A = 5 and 10 the first impact drains a large share of the condensate's
  kinetic energy (§0.3). The user wants a measurement of what follows: how many bounces remain, when the field parks,
  and what ΔN_eff, Y_p and D/H become.

**It changes no code.** Its scripts live in `.documents/one-loop-drain/measure-02b/`. 02's scripts and outputs are
not changed: they are 02's record (invariant 6).

### 0.2 What 02 left

- The driver, `measure/run_sample.py`, runs the tool route one task per fresh process and writes one JSON line per
  task. Each integration's candidates and injections are kept in `tasks/<id>.partial.jsonl`. The scripts hard-code
  02's A values (`A_VALUES` in `common.py` and `tasks.py`) and its output directory (`OUT`).
- The shortcuts S1 and S2 (prompt 02 §1.2) held bitwise on every spot check. The definition of "bitwise" is
  orchestrator note 02's ruling 6.
- At A = 1 and 0.1, every history at laboratory M stays one-loop and parks. At A = 0.01, every tuning in the
  transition window (P and W, β 1.06–1.1875) fails but P-2188's under `edge`. R-632 (β = 1.0) fails at A = 0.1 and
  0.01. These failures are `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]` (board §3).
- Cost: 1 h 58 min for 110 runs at most six workers. In CPU time, the `zero` pass took 0.68 h at A = 1, 1.28 h at
  A = 0.1 and 2.98 h at A = 0.01. The peak RSS of a task was about 1.35 GiB.

### 0.3 The first impact, read from 02's partials

The fraction of the first impact's kinetic energy drained is the first injection's α_eff, in the accepted
integration's partial file. At A = 1, gap `zero` (the orchestrator's read of 02's partials, 2026-11-04; 02b's own
script recomputes it):

| histories | first regime | α_eff at A = 1 | so at A = 5 | so at A = 10 |
|---|---|---|---|---|
| P, W (laboratory M, β 1.06–1.1875) | one-loop | 0.084–0.118 | 0.42–0.59 | 0.84–0.99 (capped for 2188) |
| R-632 (β = 1.0) | one-loop | 0.303 | 0.99 (capped) | 0.99 (capped) |
| R-1193, R-865, R-611 | one-loop | 0.068–0.084 | 0.34–0.42 | 0.68–0.84 |
| B (10⁻³³, 10⁻³⁰) | one-loop | 0.060–0.086 | 0.30–0.43 | 0.60–0.86 |
| L 5×10⁻²⁰, 8×10⁻¹⁸ | one-loop | 0.30, 0.66 | 0.99 (capped) | 0.99 (capped) |
| L 10⁻¹⁶ | one-loop | 0.99 (capped) | 0.99 | 0.99 |
| L 5×10⁻¹⁶, 4×10⁻¹⁵; C | gap or classical | 0 | 0 (or `edge`'s at 5×10⁻¹⁶) | 0 (or `edge`'s) |

The columns for A = 5 and 10 scale the first column, capped at α_cap = 0.99. They are a projection, not a
measurement: the first impact's ℓ depends on A only through the re-tuned normalization, and nothing after the
first impact is projected. So A = 10 drains most of
the first impact's kinetic energy across laboratory M and block B, and A = 5 drains about half of it there.

### 0.4 The estimate

- **A ≥ 0.5.** The cascade parks sooner as A rises, so each `zero` pass should cost no more than A = 1's, about
  0.7 h of CPU. S2 removes group C at every A. The `edge` runs are needed only where the gap is reached: on 02's
  evidence, the ladder at most. In total, about 3–4 h of CPU.
- **A = 0.05.** It lies between A = 0.1 (1.28 h of CPU) and A = 0.01 (2.98 h). Its tunings in the transition window
  may fail as A = 0.01's did, and each such failure costs up to the step budget. Count about 2–3 h for `zero`, and as
  much again for `edge` if the gap is reached.
- **In all,** about 6–10 h of CPU, or about 1.5–2 h at five or six workers.

## 1. What is wanted

### 1.1 The sample

02's thirty histories (prompt 02 §1.1), at the same settings: re-tuned under the one-loop law, φ* = 5 M_P,
T_init = 2×10⁴ GeV, and the tool's defaults otherwise. Each history is run at A ∈ {10, 5, 2, 1.5, 0.5, 0.05}, under
gap `zero` and `edge`: 360 tunings in full, before the shortcuts.

A = 0.01, 0.1 and 1 are not re-run. 02's lines are used as they stand, once the reproduction check (§1.2) holds.

### 1.2 The reproduction check, and the shortcuts

**The reproduction check, first.** 02's lines are reused only if this tree reproduces them. Re-run 02's
`P-2197-A1-zero` and `R-865-A0.1-zero` with 02b's driver. Each must be bitwise equal to 02's line and partial file,
in note 02's ruling-6 sense. A difference is a stop.

**S1, per (history, A),** as in 02: if the `zero` tuning at that A met no gap candidate in any of its
integrations, the `edge` tuning is recorded as "equal to `zero` by S1", not run.

**S2, generalised to every A.** 02's S2 derived A = 0.1 and 0.01 from A = 1. The same argument holds at any A > 0.
In the step loop A appears only in:
- the tests `alpha > 0`;
- `drain_alpha_effective`, at a one-loop candidate, and at a gap candidate under `edge`;
- the record's `A` field.

So if 02's A = 1 tuning of a history met no one-loop candidate in any integration, and under `edge` no gap candidate
either, the tuning at any of 02b's A makes the same integrations bit for bit. It is recorded as "equal to A = 1 by
S2", with 02's line as its source, and is not run. Re-check this statement against the code before relying on it,
and record in the log where A is read. If the code reads A anywhere else in the step loop, stop and ask.

**Spot checks.** Each shortcut is run anyway on two cases, and must be bitwise equal to what it replaces (note 02,
ruling 6):
- S1: the first history of groups P, R and B, in prompt 02's table order, to which S1 applies at A = 10; and the
  first to which it applies at A = 0.05. If none applies at one of these A, use the next A in the order 5, 2, 0.5.
- S2: `C-3016` at A = 10, and `C-3233` at A = 0.05.

A spot check that differs is a stop.

### 1.3 The driver, the record, and the first impact

**The scripts.** They go in `.documents/one-loop-drain/measure-02b/`, and the outputs in `var/one-loop-drain/02b/`.
- Use 02's scripts by import wherever their functions serve unchanged.
- Where a 02 script hard-codes its A values or paths, 02b's script may copy it with only those changed. Record each
  copy in the log, with what was changed.
- 02's driver properties hold unchanged: a fresh process per task; the three captures; streamed partial files;
  SIGTERM, then SIGKILL after 30 s, on the 3600 s timeout; peak RSS recorded; nothing outliving the batch.

**Each line** carries everything 02's line carries (prompt 02 §1.3), plus **the first impact and the cap.** These
come from the accepted integration, and for 02's lines at A = 0.01, 0.1 and 1 from 02's partial files:
- the first candidate's regime and N;
- the fraction drained at the first impact: the first candidate's α_eff, 0 if it drained nothing;
- its ΔK as a share of K(N_final);
- the number of drains before parking (or to the end, if the field does not park);
- the number of one-loop drains at which the cap acted, that is A·c₁/ℓ ≥ α_cap. Where ℓ is not in the record
  (02's partials), count α_eff = α_cap·w_gate. State which way each count was made.

**The fixed-law references** are 02's `var/one-loop-drain/02/fixed_law.json`, read as it stands.

**The tool check.** Run `tools/history_and_bbn.py` itself on `P-2197` at A = 10, gap `zero`, as in 02 (note 02,
ruling 9): stdout compared with `wall=` and `PRyM_time=` masked. A difference is a stop.

### 1.4 The order, and the budget

1. The suites, then the reproduction check (§1.2).
2. The `zero` passes at A = 10, 5, 2, 1.5 and 0.5, every group, less S2's. Then S1's decision at each of those A,
   and the `edge` runs that S1 does not cover.
3. The `zero` pass at A = 0.05, P first (2197, 2188).
   - After P's two, project the rest of A = 0.05: `zero`, and `edge` where S1 will not spare it.
   - If the projection is over three hours at the workers below, stop and ask with the projection.
   - Then the rest of the `zero` pass at A = 0.05, and the `edge` runs S1 does not cover.
4. The spot checks, and the tool check.

**Workers.** At most min(6, ⌊8 GiB / the largest peak RSS seen so far⌋), as note 02's ruling 5. At most eight
processes of this prompt run at once over everything; the machine is shared.

A task that reaches the 3600 s timeout is recorded with what it finished, and is not rerun.

### 1.5 What is not wanted

- **No code change.** Nothing under `ComputeTargets/`, `CosmologyConcepts/`, `CosmologyModels/`, `ObjectFactories/`,
  `tools/`, `config/`, `RunDrivers/`, `main.py` or the plot scripts changes, and no test is added.
- No change to 02's scripts, outputs or verification §2.
- No store is written, no Ray, and no cluster. The store is opened read-only (`mode=ro`), if it is opened at all.
- No ruling on U14: the prompt measures and states; the user rules.
- No threshold on the results. Y_p and D/H are set beside the criteria of `bbn_constraints_explainer.tex` as context,
  never as a pass or a failure.
- No diagnosis of the failures. If A = 0.05 fails in the transition window, its failures are added to the
  measurements of `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`. A failure of another kind, at any
  A, opens an issue of its own.
- No merge, no push.

## 2. The Wide gate (README §8.5)

The gate holds when all of these are in the verification document's new §2b, each from a script of
`measure-02b/` with its output in `measure-02b/out/` (text only):

1. **The sample measured.** Every task's outcome, by group, A and prescription, with S1 and S2 marked, and the
   reproduction check. Failures and timeouts are listed with their reasons.
2. **The regime shares and the gap's reach, across the nine A** (02's three and 02b's six):
   - per history and A, the K shares by regime and the parking leftover's;
   - where the cascade enters the gap, and where it reaches R ≤ 1;
   - ℓ_min, the drains, and the R·S(L) bound's share of K;
   - U9's and P19's counts at 02b's A.
3. **The first impact and the cap** (§1.3), per history and A. For A = 5 and 10, set beside A = 1 and 2:
   - the fraction drained at the first impact;
   - the drains left before parking, and the parking N;
   - r_χ and ΔN_eff at 1 MeV and today, Y_p and D/H;
   - how often the cap acts.
4. **The spread between the prescriptions** at 02b's A, where both ran, as in 02 §2.3.
5. **Against the fixed law,** as in 02 §2.4, at all nine A.
6. **The cost:** wall time and integrations per tuning, by group and A; the batch's wall time; the workers; and the
   load averages at its start and end.
7. **The handover for U14,** as a measurement and the options it bears on, not a choice:
   - how each observable moves with A over the nine values, laboratory M first (Fig 6, README §8.0);
   - which histories change regime between A values;
   - which (history, A) cannot be built on this code.

## 3. Verification

- **The suites, before anything else,** per `CLAUDE.md` (ComputeTargets in two halves; tools alone), with counts,
  times and `uptime`. They must stand at 26, 566 (163 + 403), 130, 156 and 68, all passing. A count that differs is a
  stop. Run them again before the commit; the counts must not change.
- **The batches** are launched detached (`nohup`, pid recorded) and checked once to see that they started. Each is
  waited on by a background command that ends with it, not by polling. The batches must have ended before the commit,
  and nothing outlives the prompt.
- **Scratch.** Outputs go under `/Users/ds283/Documents/Code/ChamPBH/var/one-loop-drain/02b/`. Probes and suite
  outputs go in the agent's own subdirectory of the session scratchpad. Nothing goes in `/tmp`.
- **Reproducibility.** Every number in §2b comes from a script in `measure-02b/` with its output in
  `measure-02b/out/`, or from a line of `var/one-loop-drain/02b/sample.jsonl` (or of 02's) that the document names.

**Stop conditions:**
- a suite count that changes, or a suite that fails;
- the reproduction check, a spot check or the tool check not bitwise;
- A read in the step loop somewhere §1.2 does not list;
- the projection over three hours (§1.4);
- a failure whose reason is the wall search or the inside-wall guard, which 01 fixed;
- any change that §1.5 forbids becoming necessary.

## 4. The commit, the log and the board

One commit (invariant 1), with:
- `.documents/one-loop-drain/measure-02b/` (the scripts) and its `out/` (text only);
- `.documents/one-loop-drain-verification.md` §2b, added after §2, which is not changed (invariant 6);
- the log `logs/02b-measure-wide-A.md` (template: `dk-and-rpk-migration` README §5.1), with:
  - every deviation classified;
  - each copied 02 script and what was changed;
  - "Observations not acted on";
  - "State handed to the next prompt", that is what 03 needs: any regime, record field or range of A that 02 did not
    meet;
- the board:
  - row 02b, its items and the Wide gate;
  - U14 marked "open for the user, with 02's and 02b's measurements";
  - §3 for anything opened or extended, and **Last updated**;
- `.documents/OPEN_ISSUES.md` in the same commit if §3 changes.

Nothing under `var/` is committed. Format the Python files you add with `black`. Stage by explicit path, and show
`git diff --cached --name-status` before committing. The commit message follows `CLAUDE.md` invariant 5. Write
"this commit" where the SHA goes; the orchestrator substitutes it.
