# Prompt 02 — measure the law

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-03, on branch `one-loop-drain` after 01 landed (`b99f6a4`, reviewed `d7cef0e`). It is written
from:
- README §8 whole: §8.2 (the law), §8.4's row 02, §8.5's Measure gate, and U11 as ruled ("U11 - OK");
- 01's log, `logs/01-the-drain-law.md`, its "Observations not acted on" and "State handed to the next prompt";
- `.documents/one-loop-drain-verification.md` §1;
- the 2026.8.0 store, read for this prompt (§0.3), and two cost probes made for it (§0.4).

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Where it runs.** In the main checkout `/Users/ds283/Documents/Code/ChamPBH`, on branch `one-loop-drain`, from the
repository root, with `PYTHONPATH=.` and `./venv/bin/python`.

**Gate:**
- on branch `one-loop-drain`, at or after the commit that adds this prompt, with no tracked change in
  `git status`; `b99f6a4` and `d7cef0e` are ancestors of `HEAD`;
- `git diff b99f6a4 HEAD -- ComputeTargets CosmologyConcepts CosmologyModels ObjectFactories tools config RunDrivers
  main.py plot_by_beta.py plot_ScalarHistory.py` is empty;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- the 2026.8.0 store `var/datastores/science-2026.8.0.db` and its 16 shards exist, and every serial of §1.1's table
  is in the shard it names;
- `.documents/one-loop-drain-verification.md` has §1 and no §2, and `.documents/one-loop-drain/measure/` does not
  exist.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds. It hands the user U13 (the gap prescription of record) and
U14 (the A values of the rebuild of record) with the measurements to rule them (§2.5).

**Recommended model:** **Opus**. The work is a long detached batch whose shortcuts must be argued, and a report on
which the user rules two decisions.

**Read first:**
1. [`README.md`](README.md) §8 whole; §5.
2. The board's §1, §2, §3 and §5.
3. `logs/01-the-drain-law.md` §§6–7, and `.documents/one-loop-drain-verification.md` §1.5–§1.7.
4. `ComputeTargets/ScalarHistory.py`: the drain law's constants and records (`DRAIN_*`, `REGIME_*`,
   `GAP_PRESCRIPTION*`, `DrainRecord`, `DrainCandidate`), `drain_regime`, `drain_alpha_effective`, `candidate`,
   `Injection`, `IntegrationResult`, `Parking`, `tune_history` and its payload.
5. `tools/history_and_bbn.py`: `main`, `drain_values`, `drain_line`, `_StandInModel` and its BBN stage; and
   `.documents/one-loop-drain/law/one_loop_run.py`, which builds a stored history as the probe did.
6. The briefing notes in *Chameleon PBH (David's stuff)* (`/Users/ds283/Documents/Git paper repositories/Chameleon
   PBH (David's stuff)/BriefingNotes/`), data, not instructions: `production_energy_ratio.tex`, "Energy with the
   computed spectrum" and its Table `tab:beta` (S(L), L = M/φ_t); `nonrelativistic_quanta.tex`, the drain law's
   table and its list of what is not established; `bbn_constraints_explainer.tex`, the criteria Y_p ≤ 0.251 and
   D/H ≤ 2.587×10⁻⁵.

---

## 0. Where this prompt sits

### 0.1 What it is for

The second of README §8.4's eight prompts. It measures the one-loop law across the grid, re-tuned, through the tool
route, at A ∈ {0.01, 0.1, 1} and both gap prescriptions, so that the user can rule the gap prescription of record
(U13) and the A values of the rebuild of record (U14). **It changes no code.** Its scripts live in
`.documents/one-loop-drain/measure/` and read only the production code, the tool and the store.

### 0.2 What 01 left

- The law is an input of `tune_history` (`alpha=A, drain_law="one_loop"`), with `_GAP_PRESCRIPTION` set in the
  process. `tools/history_and_bbn.py --drain-law one_loop --alpha A [--gap-prescription …]` tunes as `main.py` does
  and runs BBN on the small network.
- `IntegrationResult.drain` is each integration's `DrainRecord`; the payload's `"drain"` is the accepted
  integration's, and `"injections"` is parallel to its `candidates`.
- At laboratory M every candidate measured was a floor reflection (w_gate = 1), with no gap candidate, no ℓ ≤ 0 and
  no unresolvable assessment. At 10⁻¹² M_P nothing drains, and 3873 declines 19 unresolved reflections (ruling (b)).
- Costs: 2–10 s an integration at laboratory M (A = 1, 0.1); 24–37 s at 10⁻¹² M_P for 3016 and 3233, 113–119 s for
  3873; 3–16 integrations a tuning.

### 0.3 The store, read for this prompt

- **ln R at the first impact** (`log_R_first_impact`) is independent of α at every stored (M, β), and at fixed β it
  falls by 4.0 per e-fold of M. At β = 2.0 it is 138.7 at 10⁻³⁰, 0.5 at 10⁻¹⁵, −17.9 at 10⁻¹³ and −27.1 at
  10⁻¹² M_P, so ln R_first = 0.5 − 4.0 ln(M/10⁻¹⁵ M_P) there. It crosses zero at M ≈ 1.1×10⁻¹⁵ M_P. The grid has no
  M between 10⁻³⁰ and 10⁻¹⁵, so the ladder of U11 is made of new M (§1.1).
- **The below-weight share** is `extra_data["margins"]["below_weight_share"]`. At 4×10⁻³¹ M_P its ten largest,
  2197 and 2188 aside, are all at α = 10⁻³, β 1.07–1.19, with shares 0.976–0.872.
- **The fixed-law references.** Every stored (M, β) of §1.1 has successes at several α: 13 at laboratory M
  where block A2's grid has the β (all of §1.1's but β = 1.0), and 7 elsewhere. The store's BBN rows are in `BBNData`, by `scalar_history_serial`, in the same shard.

### 0.4 The cost probes

Two probes made for this prompt on `d7cef0e`, in the planner's scratchpad, not committed:
- **2197 at A = 0.01, gap `zero`, untuned** (at its stored normalization, by `law/one_loop_run.py`, gate kept):
  906 s for one integration, against 2–9 s at A = 1 and 0.1.
  - 13,951 candidates and reflections, 1.97×10⁶ accepted steps, and no parking.
  - 183 one-loop drains, with ℓ down to 0.083 and α_eff 8×10⁻⁴–0.33, to N = 40.24. Then 508 gap candidates
    (N 40.24–41.15), then 13,260 classical ones to N = 46.0. None of these drains.
  - r_χ is 8.9×10⁻⁵ at 1 MeV and 8.3×10⁻⁴ today, since the drains go on after 1 MeV. ΔN_eff today is 0.0061.

  So at A = 0.01 the laboratory-M cascade reaches the gap and R ≤ 1, and S1 will not apply there.
- **β = 2.0, M = 10⁻¹⁶ M_P at A = 1, gap `zero`, tuned** (`tools/history_and_bbn.py`): one integration, 8.4 s, plus
  8.6 s of PRyMordial.
  - ln R_first = 9.758, against §0.3's 9.71.
  - 64 candidates: 1 one-loop, 9 gap and 54 classical. The one drain, at ℓ = 1.39, holds all of K but the parking
    leftover's 2×10⁻⁵.
  - It parks at N = 25.33, with r_χ today 0.0196, ΔN_eff today 0.146, Y_p 0.24881 and D/H 2.5061×10⁻⁵.

  So the ladder reaches the gap at A = 1, and needs its `edge` runs.
- **Tunings converge at the seed.** 3466 of the store's 3718 successes took one integration, and every stored
  history of §1.1 did. So a tuning costs about one integration.

**The estimate.** A = 1 and 0.1 cost minutes in all. A = 0.01 at laboratory M and in block B is about 22 histories,
times two prescriptions, at about 900 s each: about 11 hours of CPU, or about 2 hours at six workers. Group C is cut
by S2. The ladder at A = 0.01 is not measured. The total is about 2–3 hours at six workers, against U11's bound of
about three.

## 1. What is wanted

### 1.1 The sample (U11)

Thirty histories, as U11 lists them (U11 said "about forty"; its list comes to thirty). Each is re-tuned under the one-loop law, at φ* = 5 M_P, T_init = 2×10⁴ GeV,
and the tool's defaults otherwise (tolerance 10⁻⁵, at most 30 integrations, small network).

| group | M / M_P | β | from the store (shard#serial, its α) |
|---|---|---|---|
| **P** — the probe's two | 4×10⁻³¹ | 1.185, 1.06 | `0005#2197`, `0003#2188` (α 10⁻³) |
| **W** — the ten largest below-weight shares | 4×10⁻³¹ | 1.0775, 1.07, 1.0925, 1.175, 1.105, 1.1875, 1.125, 1.1225, 1.1025, 1.1725 | `0010#2187`, `0007#1631`, `0000#2179`, `0001#2078`, `0005#2192`, `0006#2209`, `0013#2183`, `0012#2172`, `0004#2178`, `0000#2185` (α 10⁻³) |
| **R** — the four β regions | 4×10⁻³¹ | 1.0, 1.15, 2.0, 10.0 | `0002#632`, `0007#1193`, `0002#865`, `0006#611` (α 0.01) |
| **B** — block B | 10⁻³³ | 1.2, 2.0, 10.0 | `0011#2819`, `0002#2496`, `0006#2772` (α 0.01) |
| **B** | 10⁻³⁰ | 1.2, 2.0, 10.0 | `0011#2821`, `0002#2498`, `0006#2774` (α 0.01) |
| **L** — the ladder | 5×10⁻²⁰, 8×10⁻¹⁸, 10⁻¹⁶, 5×10⁻¹⁶, 4×10⁻¹⁵ | 2.0 | none: ln R_first ≈ 40, 20, 10, 3, −5 by §0.3's fit |
| **C** — large M | 10⁻¹² | 4.0, 1.0, 1.11 | `0002#3016` (α 0.1), `0002#3233` (α 0.15), `0007#3873` (α 0.03) |

- The four β regions are: below the QCD surfing threshold (β ≤ 1.1074); the transition window (1.1074–1.2); between
  it and the electroweak threshold (1.2–3.02); and above it.
- The ladder brackets the crossing: two points well inside the one-loop regime, one at ln R_first ≈ 10, one inside
  the gap at first impact (0 < ln R < 6.7), and one below it. The measured ln R_first of each is recorded; a
  difference from the target is a measurement, not a stop.
- The store's α is that of the serial named. It identifies the fixed-law reference; it is not an input of the
  one-loop runs.

### 1.2 The runs, and the two shortcuts

Each history is run at A ∈ {1, 0.1, 0.01}, each at gap `zero` and `edge`: 180 tunings in full. Two shortcuts reduce
that, each exact by construction:

- **S1, the edge run, per (history, A).** `_GAP_PRESCRIPTION` is read only at a gap candidate
  (`drain_alpha_effective`). If the `zero` tuning at that A met no gap candidate in any of its integrations, the
  `edge` tuning makes the same integrations bit for bit. It is recorded as "equal to `zero` by S1", not run.
- **S2, the other A, per (history, prescription).** In the step loop A appears only in the tests `alpha > 0`, in
  `drain_alpha_effective` (at a one-loop candidate, and at a gap candidate under `edge`) and in the record's `A`
  field. If the A = 1 tuning met no one-loop candidate in any of its integrations, and under `edge` no gap
  candidate either, the tunings at A = 0.1 and 0.01 make the same integrations bit for bit, with only the record's
  `A` differing. They are recorded as "equal to A = 1 by S2", not run.
- **Together:** S2 is decided first, for `zero`; then S1 at each A, from that A's `zero` tuning, run or derived.

A shortcut needs **every integration's** candidates, not the accepted one's: the driver records each integration's
`IntegrationResult.drain` (§1.3).

**Spot checks.** Each shortcut is run anyway on two histories, and must be bitwise equal to what it replaces in the
tuning's integrations, the samples' SHA-256 and the BBN values:
- S1 on `0005#2197` at A = 1 and on `0002#865` at A = 0.1. If S1 does not apply to one of them, use the next
  history of groups P, R and B, in the table's order, to which it applies at that A. The ladder's 10⁻¹⁶ reaches
  the gap at A = 1 (§0.4), so S1 does not apply there;
- S2 on `0002#3016` at A = 0.01 and on `0002#3233` at A = 0.1.

A spot check that differs is a stop.

### 1.3 The driver and the record

`.documents/one-loop-drain/measure/run_sample.py`, which:
- reads a task list (`tasks.json`, written by `measure/tasks.py` from §1.1's table and the store, read-only);
- runs each task in a **process of its own** (the gap prescription is a module attribute), at most six at once;
- in that process, runs exactly the tool's stages: the tuning, then BBN through `compute_BBN_data._function` on the
  tool's stand-in, as `tools/history_and_bbn.py main` does. It imports the tool's functions and does not copy them;
- wraps `ScalarHistory.integrate_scalar_history` in that process, as the tool does, to keep **every** integration's
  `IntegrationResult` (its `drain`, `N_final` and `injections`), not only the last;
- writes one JSON line per task to `var/one-loop-drain/02/sample.jsonl`, with a per-task timeout of 3600 s. A
  timeout is recorded with the integrations it finished, and is not rerun.

**Each line holds:**
- the task (group, M, β, A, gap prescription, the store's serial if any) and how it was obtained: `run`, `S1` or
  `S2` (with the task it equals);
- the tuning: outcome and failure reason, integrations, wall time, and per integration its candidate counts by
  regime;
- from the accepted integration, by regime (one-loop, gap, classical, unassessed): the count and the share of
  K(N_final); the parking leftover's share; the drains; ℓ_min; the least ln R at a drain; `w_gate < 1` at a
  one-loop candidate (count; U9); `unresolvable_assessments` and `unresolved_declined`;
- **the gap's reach:** the first gap candidate's index, N and ln R, and ℓ of the one-loop candidate before it; the
  first classical candidate's index, N and ln R; whether the cascade enters the gap and whether it reaches
  R ≤ 1;
- **the R·S(L) bound** (README §8.2): for each classical candidate, L = M/φ_t from its `classical_turning_point`, and
  where 40 ≤ L ≤ 134 the bound R·S(L)·ρ_kin,E·e^{4N}, with L²S interpolated linearly in L between Table `tab:beta`'s
  rows (0.22 at 40, 0.24 at 70, 0.25 at 90 and 110, 0.26 at 134); the sum of those bounds as a share of K(N_final);
  and the number of classical candidates with L outside [40, 134], for which no bound is computed (the note's "The
  tail");
- the history's values: the first impact's ln R, `parking_N`, `release_N`, r_χ and ΔN_eff at 1 MeV and today,
  Y_p, D/H, the network and PRyMordial's time;
- the SHA-256 of `data["sample"]` and of the samples' `log_one_plus_z`, by the scanner's `_sha256`
  (`tools/outward_step_scan.py` :564, :683–:684), so that S1 and S2 are checked bitwise.

**The fixed-law references** are read from the store by `measure/fixed_law.py`, read-only: for each stored (M, β)
of §1.1, every success at that (M, β), with its α, ΔN_eff (1 MeV and today), r_χ today, parking N, the injections,
and its BBN row's Y_p and D/H. The ladder has no reference.

**The driver is checked against the tool.** For two tasks, `0005#2197` at A = 0.1 and the ladder's 10⁻¹⁶ at A = 1,
`tools/history_and_bbn.py` itself is run with the same options. Its `history`, `drain` and `bbn` lines must carry
the driver's values. A difference is a stop.

### 1.4 The order, and the budget

1. The `zero` passes at A = 1 and then 0.1, every group. Then S2's decision for each history, and S1's at A = 1 and
   0.1. Then the `edge` runs at A = 1 and 0.1 that S1 does not cover.
2. The `zero` pass at A = 0.01, P first (2197, 2188), then R, B, W and L. After P's two, project the remainder of
   A = 0.01 (`zero` and the `edge` runs, which S1 will not spare where the gap is reached) from their wall times.
   If the projection exceeds four hours at six workers, stop and ask with the projection.
3. The `edge` runs at A = 0.01 that S1 does not cover.
4. The spot checks, and the two tool checks.

U11 bounds the sample at about three hours (§0.4's estimate is 2–3). A task that reaches the 3600 s timeout is
recorded with what it finished and is not rerun.

### 1.5 What is not wanted

- **No code change.** Nothing under `ComputeTargets/`, `CosmologyConcepts/`, `CosmologyModels/`, `ObjectFactories/`,
  `tools/`, `config/`, `RunDrivers/`, `main.py` or the plot scripts changes, and no test is added. The suites must
  stand where 01 left them.
- No store is written, no Ray, and no cluster. The store is opened read-only (`mode=ro`).
- No ruling on U13 or U14: the prompt measures and states; the user rules.
- No threshold on the results. Y_p and D/H are set beside the criteria of `bbn_constraints_explainer.tex` as context,
  never as a pass or a failure.
- No merge, no push.

## 2. The Measure gate (README §8.5)

The gate holds when all of these are in the verification document's §2, each from a script of
`.documents/one-loop-drain/measure/` with its output in `measure/out/` (text only):

1. **The sample measured:** every task's outcome, by group, A and prescription, with S1 and S2 marked. Failures and
   timeouts are listed with their reasons.
2. **The regime shares and the gap's reach across M and β:**
   - per history and A, the K shares by regime and the parking leftover's;
   - where the cascade enters the gap (by group and ln R_first), and where it reaches R ≤ 1;
   - ℓ_min, the drains, and the R·S(L) bound's share of K, with the count outside S(L)'s range;
   - U9's question: how often w_gate < 1 at a one-loop candidate. P19's: how many candidates are unassessed.
3. **The spread between the prescriptions:** for each history and A run under both, Δ(Y_p), Δ(D/H), Δ(ΔN_eff at
   1 MeV and today) and Δ(r_χ today) between `edge` and `zero`, with the largest of each across the sample.
4. **Against the fixed law:** for each stored (M, β), the one-loop values at each A beside the fixed law's at each
   stored α (§1.3): ΔN_eff, r_χ, Y_p, D/H and parking N. The store's values are a measurement made on the 2026.8.0
   tree, not a bound.
5. **The cost:** wall time and integrations per tuning, by group and A; the batch's wall time; the workers; the
   load averages at its start and end.
6. **The handover for U13 and U14,** in the verification document, as a measurement and the options it bears on, not
   a choice:
   - for U13, where the prescriptions differ at all (which histories and A), by how much in each observable, and the
     share of K that is in the gap;
   - for U14, how each observable moves with A, at laboratory M first (Fig 6, README §8.0), and which histories
     change regime between A values.

## 3. Verification

- **The suites, before anything else**, per `CLAUDE.md` (ComputeTargets in two halves; tools alone), with counts,
  times and `uptime`. They must stand at 26, 566 (163 + 403), 130, 156 and 68, all passing; a count that differs is a
  stop. Run them again before the commit; the counts must not change.
- **The batch** is launched detached (`nohup`, pid recorded), checked once that it started, and waited on by a
  background command that ends with it, not by polling. At most eight workers run at once over everything; the
  machine is shared. The batch must have ended before the commit, and nothing outlives the prompt.
- **Scratch.** Outputs go under `/Users/ds283/Documents/Code/ChamPBH/var/one-loop-drain/02/`; probes and suite outputs
  in the agent's own subdirectory of the session scratchpad. Nothing goes in `/tmp`.
- **Reproducibility.** Every number of the verification document's §2 comes from a script in
  `.documents/one-loop-drain/measure/`, with its output in `measure/out/`, or from a line of
  `var/one-loop-drain/02/sample.jsonl` that the document names.

**Stop conditions:**
- a suite count that changes, or a suite that fails;
- a spot check of S1 or S2 that is not bitwise (§1.2);
- the driver disagreeing with the tool (§1.3);
- the projection over four hours (§1.4);
- a failure whose reason is the wall search or the inside-wall guard, which 01 fixed;
- any change the prompt's §1.5 forbids becoming necessary.

## 4. The commit, the log and the board

One commit (invariant 1), with:
- `.documents/one-loop-drain/measure/` (the scripts) and its `out/` (text only);
- `.documents/one-loop-drain-verification.md` §2, added after §1, which is not changed (invariant 6);
- the log `logs/02-measure-the-law.md` (template: `dk-and-rpk-migration` README §5.1), with every deviation
  classified, "Observations not acted on", and "State handed to the next prompt" (what 03 needs: the regimes that
  arise, the gap prescription's name in the record, any value the store will have to carry that 01's record lacks);
- the board: row 02, its items, the Measure gate, U13 and U14 marked "open for the user, with 02's measurements",
  §3 for anything opened, and **Last updated**; `.documents/OPEN_ISSUES.md` in the same commit if §3 changes.

Nothing under `var/` is committed. Format the Python files you add with `black`. Stage by explicit path, and show
`git diff --cached --name-status` before committing. The commit message follows `CLAUDE.md` invariant 5. Write
"this commit" where the SHA goes; the orchestrator substitutes it.
