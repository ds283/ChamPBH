# Orchestrator — prompt 02, measure the wall

**Used for 02** (dispatched 2026-11-14 at `35f6359`; landed `693784b`; reviewed on the board, "Orchestrator review of
prompt 02").

Read [`../README.md`](../README.md) first: §0, §2, §5, and §6 as ruled (R1–R5, U1–U7). For this prompt the user's words
are: "Please write prompt 02 and its orchestration note" (2026-11-14, after R4 and R5). Then read the prompt. This note
binds 02 only. The conventions of earlier notes bind here as well: a tolerance is a number with its reason, timing is
taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites and the scripts the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`02-measure-the-wall.md`](../02-measure-the-wall.md) · **Gate:** "Wall" · **Closes:** nothing ·
**Opens:** only what the work finds · **Model:** Opus · **Network:** none. Nothing is pushed.

**When:** written 2026-11-14 at the user's request. Amended the same day, before dispatch, at the user's request: "Add
k_ex/M_P at first impact to prompt 02's map" (prompt §1.4; §0's last bullet below; review check 3). Whether it is dispatched from this context or a fresh one is the
user's to say. Either way, the dispatching context re-checks the Gate (§1).

**Gate**, as measured by the planner on 2026-11-14 at 20:20 local, before this note and the prompt were committed:
- **The worktree.** `.claude/worktrees/power-law-wall` is on `power-law-wall` at `f710ff2` (R4–R5 recorded), on top of
  `161a5c0` (review of 01) and `3aae9a5` (01). `git status --porcelain` shows `?? venv`, plus this note and the prompt
  before their commit.
- **The main checkout** is on `main` at `9df9e13` and clean. The rebuild's session moved it there from `a19d71e`
  during 01.
- **The registry.** `runregistrykit list`: 62 runs, all finished, none `running` or stale, none of this campaign.
  The rehearsal lock was free at 11:12, and the rebuild's GCS was gone.
- **Prompt files.** `prompts/power-law-wall/source/` does not exist; the verification document has §1 only.
- **Load and disk.** Load averages 5.33 / 6.84 / 6.42; 130 GB free.
- **Crossover.** The directory was last changed at `18add05`, which is the commit the copies name.

## 0. What the planner found

A scratch probe, 2026-11-14 at about 20:15, from the worktree root:
`/private/tmp/claude-35086/-Users-ds283--claude-spares-spare-pVvgoJ/33879d88-4b17-46d9-b574-a61f8bdabf85/scratchpad/planner02/probe.py`
and `probe2.py` beside it, which are not committed. That directory belongs to the planner's session and may not
survive it. The numbers below are the record. `PowerWall(x, n)` was written in walls.py's
units (M = 1, ω_t = 1, U = cφ^{−n}, V₀ = c, φ_t = x^{−1/n}, c = φ_t^{n+2}/(n(n + 1)), E = cx) and run through
crossover's `born.chain`, `walls.Bounce` and `walls.beta_squared` unchanged. The load average was about 20 at the start
(another session's work).

- **The path formula holds exactly at 01's first impacts.** ln R = ln(V₀/M⁴) + 2 ln(n(n + 1)) − ln 8π² + (1 + 4/n) ln x.
  - With ln V₀ = −276.6307 (01's tuned normalization), laboratory M gives ln(V₀/M⁴) = 3.344.
  - 01's ln R at first impact, 530.575 (n = 1) and 320.687 (n = 2), then both give ln x = 106.04.
  - The exponential's own path formula at L = ln(1 + x) = 106.04 gives 142.36, against 01's 142.366.
  - So all three histories meet the wall with the same E, as their identical bounces imply, and the formula is right.
  - The curves at the prompt's three M:

  | M (M_P) | ln(V₀/M⁴) | n = 1: ln R | n = 2: ln R | x at R = 1 (n = 1, 2) | ln R at first impact (n = 1, 2) |
  |---|---|---|---|---|---|
  | 4×10⁻³¹ | 3.344 | 0.361 + 5 ln x | 2.559 + 3 ln x | 0.93, 0.43 | 530.6, 320.7 |
  | 10⁻³³ | 27.31 | 24.33 + 5 ln x | 26.52 + 3 ln x | 7.7×10⁻³, 1.4×10⁻⁴ | 554.5, 344.6 |
  | 10⁻¹⁵ | −138.48 | −141.46 + 5 ln x | −139.26 + 3 ln x | 2×10¹², 1.4×10²⁰ | 388.7, 178.9 |

  The last column assumes the same E at first impact at every M (delivery at β = 2), which 02's map will measure.
  - At laboratory M the late cascade (ℓ ≲ 2, the gap and the classical tail) sits at x ≈ 1–100, where the floor
    matters. This is like the exponential at laboratory M, whose gap runs from L ≈ 3.6 (x ≈ 36) to R = 1 at x ≈ 1.
  - At 10⁻¹⁵ M_P the whole cascade sits at x ≥ 10¹², on the pure power wall.
- **Scale invariance, and the units.** `born.chain` as written gave an ℓ that drifted with x at fixed R, n = 1:
  2.25 at x = 10¹⁰ and 1.88 at x = 10⁴⁰, both at ln R = 40. The cause is its absolute `xtol = 1e-15` on φ_ta, against
  φ_t = 10⁻⁴⁰. With `xtol` relative to φ_t (`probe2.py`), ℓ is the same at every large x:

  | n | ln R | ℓ at x = 10³ | 10¹⁰ | 10⁴⁰ | 10¹⁰⁰ |
  |---|---|---|---|---|---|
  | 1 | 20 | 0.4201 | 0.4437 | 0.4437 | 0.4437 |
  | 1 | 40 | 1.7027 | 2.2455 | 2.2455 | 2.2455 |
  | 1 | 100 | none | 7.3423 | 7.3442 | 7.3442 |
  | 1 | 300 | none | none | 24.110 | 24.110 |
  | 1 | 530 | none | none | 43.292 | 43.325 |
  | 2 | 20 | 1.3621 | 1.4798 | 1.4798 | 1.4798 |
  | 2 | 40 | 1.1113 | 4.1074 | 4.1074 | 4.1074 |
  | 2 | 100 | none | 9.4167 | 11.739 | 11.739 |
  | 2 | 300 | none | none | 36.882 | 36.882 |

  - "none" and the smaller values at small x are the floor: φ_ta lies where V − V₀ is no longer ≫ V₀, so λ, k_ex and
    ℓ fall. The floor matters once (φ_ta/φ_t)ⁿ approaches x.
  - On the laboratory curve ℓ at first impact is about 43 (n = 1) and about 39 (n = 2), against about 32 on the
    exponential. So the first bounces drain f ≈ 0.06 under the adopted law.
  - The chain ends (ℓ → 0) near ln R ≈ 18–20 at large x, n = 1. It ends lower at n = 2, since ℓ(ln R = 20) = 1.48.
  - walls.py's `ATOL = 1e-14` on φ has the same problem as the `xtol` at small φ_t. That is ruling 2.
- **The classical test field is scale-free and much less adiabatic than the exponential's.** 61 points on
  [10⁻², 6] ω_t, trapezoid in ln k; each run took 2 s:

  | n | x | S | \|β\|²_max | peak k/ω_t | peak k/k_ex | tail d ln\|β\|²/dk, per k_ex |
  |---|---|---|---|---|---|---|
  | 1 | 10³ | 1.0262×10⁻² | 7.68×10⁻² | 0.160 | 0.453 | −2.14 |
  | 1 | 10¹⁰ | 1.0267×10⁻² | 7.68×10⁻² | 0.160 | 0.452 | −1.97 |
  | 2 | 10³ | 5.2162×10⁻³ | 3.38×10⁻² | 0.198 | 0.485 | −2.49 |
  | 2 | 10¹⁰ | 5.2162×10⁻³ | 3.38×10⁻² | 0.198 | 0.485 | −2.49 |

  - The exponential has S(L) = 1.4×10⁻⁵ to 1.4×10⁻⁴ at L = 40–134 and |β|²_max ≈ 10⁻⁴. Its S at L ≈ 1 is
    8.7×10⁻³, close to the power wall's at n = 1.
  - x_p is about 0.45–0.49 against 0.62. The tail slope is about 2–2.5 per k_ex against κ ≈ 3.96 (`KAPPA`, L = 90).
    These come from a crude grid and fit, and κ was not taken by the module's two-point rule; the prompt's §1.8
    measures them properly.
  - k_ex/ω_t is (1/2)√(n/(n + 1)) on the pure power wall: 0.354 (n = 1) and 0.408 (n = 2), against 1/2 on the
    exponential at large L.
- **The Planck scale: the planner's estimates, not measurements.** k_ex = λ_t√E/2 grows with E, so the first impact
  gives the largest. k_ex < M_P (reduced) is the same as φ_t > n√E/(2M_P) on the power wall. In Planck units:
  - power wall: k_ex ≈ (n/2)(√E/M)(E/V₀)^{1/n}, so k_ex < M_P needs M > (n/2)E^{1/2+1/n}V₀^{−1/n};
  - exponential: k_ex ≈ (n/2)(√E/M)L^{1+1/n}, with L = ln(1 + E/V₀), so it needs M > (n/2)√E L^{1+1/n}.

  At the QCD kick (β = 2), E = e^{−170.6} M_P⁴ ≈ 0.3 GeV⁴ was taken from 01's history, with E/V₀ ≈ 10⁴⁶. At the
  electroweak kick (β ≈ 3, T ≈ 100 GeV, g* ≈ 107, E ≈ ρ_R/(6β² − 1)), the estimate is E ≈ 7×10⁷ GeV⁴ and
  E/V₀ ≈ 10⁵⁴; this one is not measured.

  | wall | M for k_ex = M_P, QCD | k_ex/M_P at 4×10⁻³¹, QCD | M for k_ex = M_P, EW |
  |---|---|---|---|
  | power, n = 1 | ≈ 5×10⁸ | ≈ 10³⁹ | ≈ 10²¹ |
  | power, n = 2 | ≈ 1×10⁻¹⁴ | ≈ 3×10¹⁶ | ≈ 2×10⁻⁶ |
  | exponential | ≈ 5×10⁻³⁴ | ≈ 1×10⁻³ | ≈ 1×10⁻²⁹ |

  - So on the estimate, the exponential at laboratory M is about 25 times above M_P at the electroweak kick.
  - The 10⁻¹⁵ M_P histories at n = 2 should sit near k_ex ≈ 10 M_P at the QCD kick.
  - These estimates carry O(1) uncertainty from g*, T_kick, β and A(φ). The full Planck mass is √(8π) ≈ 5 times the
    reduced one.
  - The map measures all of this at the twenty-five histories (prompt §1.4).
- **The suites.** Not re-run by the planner. At `3aae9a5` 01's agent measured, in this worktree:
  - CosmologyModels 26, ComputeTargets 705 (219 + 486), ObjectFactories 163, tools 302, RunDrivers 76;
  - only the two worktree failures of note 01 ruling 7 fail.

  Only text has changed since.

**Rulings.** Each is recorded in the log, with its class.
1. **The worktree and the main checkout.** As note 01's ruling 1: the agent works only in the worktree, from its root,
   with `./venv/bin/python` and `PYTHONPATH=.`. It never stages `venv`, never touches the main checkout, and never names
   `/tmp/ray-cpbh`. *STRUCTURALLY REQUIRED.*
2. **The units.** §0 shows that the crossover scripts' absolute tolerances fail on the power wall at large x. The
   agent works in units with φ_t of order 1, or makes every tolerance relative; which of the two is the agent's choice,
   stated. It validates the change of units as the prompt's §1.2 says. *IMPLEMENTATION CHOICE, ruled here.*
3. **Where stage 2 cannot reach** (the largest ℓ), `born.py` is used, with the overlap measured (§1.3). *IMPLEMENTATION
   CHOICE, ruled here.*
4. **Failures are results.** A mapped history that fails with a stored reason is recorded and not retried. An uncaught
   exception from a production module is a stop. A stage-2 point that fails is recorded with its reason. *IMPLEMENTATION
   CHOICE, ruled here.*
5. **The machine.** At most six workers for the scans, and three tuned histories at a time. The load is recorded
   beside every timing. If the scans' projected total exceeds about three hours of wall time, the agent stops and asks,
   with its estimate and a smaller plan. *IMPLEMENTATION CHOICE, ruled here.*
6. **No file but the prompt's §4 list.** *STRUCTURALLY REQUIRED.*
7. **The two worktree failures** of note 01's §0.1 are expected on the base and after. Any other failure is a stop.
   *IMPLEMENTATION CHOICE, ruled here.*
8. **A finding about production** is recorded and reported, not fixed. This covers a production wall search that
   disagrees with the agent's well-scaled chain (§1.3 item 5), or anything else. The agent may open a §3 issue for it,
   with `.documents/OPEN_ISSUES.md` in the same commit. *IMPLEMENTATION CHOICE, ruled here.*
9. **No recommendation is acted on.** The hand-back states options and evidence for U3–U5. The agent picks no law, L,
   S table or constant. *STRUCTURALLY REQUIRED* (README §5, "Measurements, not bounds").

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit", and the orchestrator substitutes it.
- **Order.**
  1. The Gate, and the five suites on the base: serially, ComputeTargets in its two halves, the `test_[g-z]` half and
     `tools/tests` in the background, waiting for each notification.
  2. The copies, `PowerWall`, the units, and the exponential's reproduction (§1.2).
  3. The map of the twenty-five histories (§1.4).
  4. The validation (§1.3).
  5. S (§1.5), then the stage-2 scan along the curves (§1.6), then the law's residuals and the trial refit (§1.7), then
     the spectrum (§1.8).
  6. §1.9, the suites again, the documents, and the commit.
- **Scratch.** Probes and suite outputs go in the agent's own subdirectory of the session scratchpad. Nothing goes in
  `/tmp`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is written.
- **Stop conditions are real:**
  - a suite count that falls, or a test that passed on the base and fails after;
  - a production change becoming necessary;
  - an uncaught exception from a production module (ruling 4);
  - a validation item of §1.3 that cannot be met within `source/wall/` (a stage 2 that does not conserve energy, or
    does not reach either limit);
  - the scans' projected time exceeding ruling 5's bound.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

1. **The Gate above**, on the commit that amends this note (the one after `0c49691`, which added it), or a later one.
2. **`runregistrykit list`**, and the load average.
3. **The probe of §0.** Its numbers are the planner's. The dispatching context need not repeat it.

## 2. Dispatch

Start one fresh-context subagent, on **Opus**, in the worktree, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad;
- the planner's probe scripts, copied beside it, as data (they are not to be committed).

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 6. Its log is `logs/02-measure-the-wall.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Stop and ask** on any of the conventions' stop conditions.
- **At the end it replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the units and their validation; the exponential's reproduction;
  - the twenty-five mapped histories (success, candidates, the ranges of x, ln R and ℓ reached, parking), and the path
    formula's residual;
  - the Planck-scale table of §1.4, with the planner's estimate beside each first impact;
  - the validation items 1–6, with their tolerances and worst residuals;
  - S_n(x);
  - the stage-2 points along each curve, as tables;
  - the residuals of §1.7 (i)–(v), by n and regime, beside the exponential's;
  - the spectrum: x_p, the low-k slope and κ, two ways, beside the exponential's recomputed values;
  - §1.9's lines;
  - the breakage record;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — six checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 6's files, and `venv` is not in it.
   - `git diff <base> <agent> -- ':!prompts/power-law-wall' ':!.documents'` is empty, which means no production file
     changed and nothing of crossover changed.
   - The commit's parent is this note's commit, or a later one that is not the agent's.
   - The main checkout is untouched. Nothing is pushed.
2. **The copies reproduce crossover.** Run the exponential reproduction points (§1.2) myself, and compare them with
   crossover's recorded outputs.
3. **The units and the chain.**
   - Re-run §1.3 item 5 at three mapped candidates myself: the agent's chain against
     `InversePowerModel.one_loop_turnaround`.
   - Re-run one change-of-units comparison (§1.2).
   - Compare S_n and ℓ at large x with the planner's tables in §0. They should agree to the probe's precision, or the
     log should say why not.
   - **The Planck scale.**
     - At three first impacts (one exponential at β = 3.05, one power wall at each n), recompute
       `log_k_ex − log(units.PlanckMass)` from the model at the stored φ_t and E, with a scratch script.
     - Check that the units are the reduced Planck mass's and the frame is stated.
     - Check that nothing is called a bound or excluded on it.
4. **The scan.** Re-run one stage-2 point from each curve and one validation point, and match them to the committed
   outputs. Check that every reported point states |ΔE/E| and whether the field came back out.
5. **The residuals.**
   - Recompute §1.7 (i) and (iv) for one n from the committed stage-2 outputs, with an independent scratch script that
     evaluates `drain_fraction`'s formula.
   - Check that the hand-back states options and does not choose one.
6. **The records.**
   - The log is in §5.1's form, with every deviation classed.
   - The verification document has §2.
   - `source/wall/README.md` exists.
   - The board has row 02, its items and the Wall gate.
   - Any issue opened is on §3 and in `.documents/OPEN_ISSUES.md`.
   - `CLAUDE.md` is unchanged, unless a count moved, which would itself be a finding.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 02 landed, the Wall gate held, and this note marked "used for 02";
- an *Orchestrator review of prompt 02* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

**Report to the user:**
- the agent's commit and the review commit;
- the Wall gate;
- **U3:** the residuals of (i)–(v), with the exponential's accuracy beside them, and the options as README §6.2 puts
  them: (a) the adopted law with an L the model defines, or (b) the law refitted as a new `drain_model` value, or
  (c) the alternative;
- **U4:** S_n(x) against the exponential's S(L_E), and whether a per-wall table is needed;
- **U5:** x_p and κ against 0.62 and `KAPPA`, with x_* and the associated piece carried over as ruled;
- **the Planck scale:** the §1.4 table against §0's estimates. This is for the user's question of the campaign's scope
  (which n and M the later prompts and the science run cover). It is not a ruling of U3–U5;
- that 03 is written only after the user rules U3–U5. If the rulings carry the law and spectrum over unchanged with
  `_drain_L` alone, 03 is dropped and `_drain_L` moves into 04's first commit (README §2).
