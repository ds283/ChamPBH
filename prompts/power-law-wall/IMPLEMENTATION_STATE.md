# power-law-wall campaign — implementation state

**Last updated:** 2026-11-15 · **Status: LIVE — 2 of 8 prompts landed and reviewed (01, the gate Model holds; 02, the gate Wall holds); R4–R5 ruled on 01's measurements (README §6.4); R6–R8 (U4 S = S_n; U3 the law f_n(ln R); U5 the three-region spectrum, the exponential's too) ruled on 02's (README §6.5); 03 written, held for U8–U9 (README §6.6); R1–R3 and U1–U7 ruled (README §6).**

The campaign adds the inverse-power-law model V = V₀[1 + (M/φ)ⁿ] beside the exponential, defines the one-loop drain
law on its wall from measurements, and takes it through the store, the drivers and a rehearsal, so that a science run
at n = 1 and 2 can follow, under the one-loop law with the fixed α beside it.

**Campaign:** [`README.md`](README.md) ·
**Code:** `CosmologyConcepts/Models/`, `ComputeTargets/quanta_spectrum.py`, `CosmologyConcepts/drain_model.py`,
`ObjectFactories/`, `config/`, `main.py`, the plot scripts, `tools/` ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) (no issue assigned)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | The model | `InversePowerModel`, the seed at any n, the tool route's `--model` and `--n`; natural mode at large M measured | Opus | ✍️ | ✅ | `3aae9a5` | [`logs/01-the-model.md`](logs/01-the-model.md) |
| 02 | Measure the wall | S, the one-loop fraction and the spectrum on the power wall (research, `source/wall/`); then U3–U5 | Opus | ✍️ | ✅ | `693784b` | [`logs/02-measure-the-wall.md`](logs/02-measure-the-wall.md) |
| 03 | The law on this wall | f_n(ln R) and S_n in `InversePowerModel` under `one-loop-measured-pl-v1`; the spectrum as tables on three walls (R6–R8) | Opus | ✍️ ⏸ U8, U9 | ⬜ | — | — |
| 04 | Measure a sample | about forty histories under both laws; the thresholds | Opus | ⬜ | ⬜ | — | — |
| 05 | The store | type id 2, its table, the registry, `FINGERPRINT_FORMAT` 8, the label (U6) | Opus | ⬜ | ⬜ | — | — |
| 06 | Drivers and exports | the model option, the plot scripts, the export layout (P11) | Sonnet | ⬜ | ⬜ | — | — |
| 07 | Rehearse | registered builds, exponential bitwise, power law resumed | Opus | ⬜ | ⬜ | — | — |
| 08 | Close | the documents, the handover, the issues, the indexes | Opus | ⬜ | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Prompts are written one at a time, each after the previous one lands and is reviewed. Orchestrator notes, one prompt at
a time, in `orchestrator/`: [`prompt-01.md`](orchestrator/prompt-01.md), written 2026-11-13 with prompt 01, used for
01 (dispatched 2026-11-14 at `75ac6fb`); [`prompt-02.md`](orchestrator/prompt-02.md), written 2026-11-14 with prompt 02, used for
02 (dispatched 2026-11-14 at `35f6359`); [`prompt-03.md`](orchestrator/prompt-03.md), written 2026-11-15 with prompt 03,
not yet used: it waits for U8 and U9 (README §6.6), and its §0 holds the f_n(ln R) fit the user ruled R7 on and the
spectrum probes R8 was ruled on. Note 02's §0 holds the planner's probe: the path formula, the power wall's scale invariance above V₀, and the
crossover scripts' absolute tolerances failing at large E/V₀.

**Planner's probe for 01 (2026-11-13, at `c540caa`, not committed).** A scratch power-law model through
`tools/history_and_bbn.py`: at laboratory M under the fixed law, n = 1 and 2 give the same history as each other and
as the exponential at β = 2, with ln R at the first impact 530.6 (n = 1) and 320.7 (n = 2) against the exponential's
142.4; natural mode at 10⁻³ M_P fails on both n at φ ≤ 0, with the n = 2 turning point at 9.4 × 10⁻²⁷ M_P (note §0).

**Orchestrator review of prompt 01 (2026-11-14).** All six checks of note 01 §3 pass, with the user's ruling at the
stop applied ("(a), add the unstored entry to compare_stores").
- **The gate at dispatch** (11:12 local): `HEAD` was `75ac6fb`, the worktree held `?? venv` only, and the registry
  had 62 runs, all finished.
  - One thing differed from the dispatch brief: the rehearsal lock was free and the rebuild's GCS (pid 34392) was no
    longer running. 01 needs neither, so this was reported and not acted on.
  - The main checkout moved during the prompt, from `a19d71e` to `9df9e13`, by the rebuild's own session. It is clean,
    and nothing of this campaign touched it.
1. *Scope.* `3aae9a5`'s parent is `75ac6fb`, the commit after the note's.
   - Its 34 files are the prompt's §4 list, plus `tools/compare_stores.py`, which the user's ruling added. That file
     changes by six lines: an `UNSTORED_TYPE_VALUES["ScalarHistory"]["model_type"]` entry for id 2, naming prompt 05.
   - `venv` is not in the commit. No branch on a remote contains it.
   - `git diff 75ac6fb 3aae9a5` is empty for `ExponentialModel.py`, `AbstractModel.py`, `ComputeTargets/*.py`,
     `ObjectFactories/`, `config/`, `main.py` and the plot scripts.
2. *The exponential.* I ran these at `3aae9a5`, starting 19:24 at a one-minute load of 25.7:
   - `test_golden_history`, `test_golden_reflecting_history`, `test_tuning`, `test_store_fingerprint_golden`,
     `test_fingerprint_format_guard` and `test_compare_stores`: 75 tests, OK, 194 s;
   - `test_history_and_bbn`, `test_history_and_bbn_alpha`, `test_history_and_bbn_drain_law`, `test_surfing_scan` and
     `test_model_option`: 47 tests, OK, 84 s.
   - None of the pinned tool tests is edited in the commit.
3. *The class.* Read against §1.1:
   - every hook is computed from ln u = n ln(M/φ), with ln(1 + u) overflow-safe and s = u/(1 + u);
   - (ln V)″ = [n(n + 1)s − n²s²]/φ² is right (re-derived), and is divided by φ twice;
   - φ ≤ 0, NaN and inf raise `ComputationFailureError` naming the model, the hook and φ;
   - the coupling is written in the class; there is no `_drain_L`, with a comment saying why;
   - `_log_V_floor` is the normalization and `reflects_at_origin` is True.
   - `test_inverse_power_model` and `test_wall_model` run 43 tests, OK, 3.7 s. The worst one-loop residual is 1.28e-13,
     as the log states.
   - The rejected-step handling the class relies on is the integrator's existing rule (`ScalarHistory.py:2028`, :3295).
4. *The seed.* `history_and_bbn.py 2.0 4e-31 --alpha 0.1 --model inverse-power --n 2`, re-run at 19:25 (load 27.3):
   - it tuned in 1 integration from the `tracking` seed, against the planner's 3 from `plain`;
   - every history, tuning, bounce, dissipation and BBN line is identical to the committed
     `out/ip-n2-M4e-31-beta2.0-fixed0.1.txt` once the timings are masked.
5. *The runs and §1.6.*
   - The verification document's §1.5 table matches `out/summary.md` and the eighteen outputs.
   - At laboratory M under the fixed law, both n match the exponential in reflections, injections, first bounce and
     ΔN_eff to the printed digits, with ln R at the first impact 2.1–3.7 times the exponential's.
   - The note expected the natural-mode failures to carry the φ ≤ 0 `ComputationFailureError`. They do not:
     - with the class's error, the φ < 0 trial states are rejected steps;
     - n = 2 succeeds at all three β (4, 28 and 59 reflections, against none on the exponential);
     - n = 1 fails at the integrator's guard G2 ("reflection requested inside the wall"), stored as the reason
       (`out/ip-n1-M1e-3-beta2.0-natural.txt`).
   - The even-continuation probe is reported in the verification document §1.6 and the log §3.6. It is not committed:
     no file of the commit is under the agent's scratch directory, and `probe16/` exists only there.
6. *The records.*
   - The log has §5.1's seven parts. It classifies rulings 1–7 and D1–D11, with no UNINTENDED DRIFT.
   - The verification document has §1, and the board has row 01, the Model gate and the baselines.
   - `CLAUDE.md`'s counts are 705 (219 + 486) and 302.
   - No issue was opened or closed, so §3, §4 and `.documents/OPEN_ISSUES.md` (32 open) are unchanged.
   - "This commit" is replaced by `3aae9a5` here and in the log (the verification document has none).
- **For the user's ruling** (not acted on):
  - (i) n = 1 natural mode at 10⁻³ M_P fails at G2;
  - (ii) whether φ ≤ 0 stays a failure, given the even-continuation probe.
  The agent's numbers are in the log §3.6 and §7.

**Orchestrator review of prompt 02 (2026-11-15).** All six checks of note 02 §3 pass. The re-runs below were made
serially by the orchestrator at `693784b`, from scratch scripts that are not committed.
- **The gate at dispatch** (2026-11-14, 23:29 local): `HEAD` `35f6359`, `?? venv` only; no `source/`; the
  verification document had §1 only; the registry had 62 runs, all finished; the rehearsal lock was free; the main
  checkout was at `9df9e13` and clean; load 7.12 / 10.08 / 7.20, 127 GB free. Everything was as the note recorded. The
  planner's probe scripts still existed and were given to the agent as copies, with the note's §0 proper (the preamble
  above it addresses the orchestrator).
1. *Scope.* `693784b`'s parent is `35f6359`.
   - Its 55 files are the prompt's §4 list. `git diff 35f6359 693784b` outside `prompts/power-law-wall` and
     `.documents` is empty, so no production file and no crossover file changed. `venv`, `CLAUDE.md` and
     `.documents/OPEN_ISSUES.md` are not in it.
   - The main checkout is still at `9df9e13` and clean, and no remote branch contains the commit.
   - The agent committed `3c51b6c` and amended it to `693784b` before its hand-back, to correct one sentence of log §7.
     No commit came between them, so the prompt is still one commit. The log does not mention the amend.
   - The copies differ from crossover's files only where a "power-law-wall 02" comment says so, and where `black`
     reflowed one call.
2. *The copies reproduce crossover.* `reproduce_exponential.py` re-run at 00:32 (load 2.62; 7 s) gives S(10) =
   1.3529e-03, S of WallN(5, 2) = 2.14335e-03, born.run 0.44982, and run_point(7.885, 23.833) 0.71709 / 0.5319 / 1.678
   with 311 modes and 53265 steps. Each equals the line in crossover's own outputs (`stage1a_S_of_L.txt:15`,
   `stage2_n2.txt:3`, `stage2_validation.txt:47` and `:12`), not only the values the script prints as "recorded".
3. *The units and the chain* (00:34, load 6.13 to 6.60; 15 s in all).
   - **The chain against production.** At three mapped candidates that the agent's item 5(b) did not pick (n = 2 lab,
     β 2.9, α 0.01, idx 30; n = 1, 1e-15, α 0.01, idx 70; n = 2, 1e-33, α 0.01, idx 45), `born.chain` on `PowerWall`
     equals `InversePowerModel.one_loop_turnaround` to 6.7e-16 in ℓ and 1.1e-14 in φ_ta/φ_t. ln R is equal to the map's
     to the digits printed.
   - **One change of units.** ℓ at n = 2, x = 10³, ln R = 40 is 1.11131358075508 in both units, a difference of 2e-15.
     At n = 1, x = 10², ln R = 20 the two are bitwise equal.
   - **Against note §0.** The ℓ table at x = 10³ to 10¹⁰⁰ is reproduced to every printed digit.
     - S_1 and S_2 agree with §0 at x = 10³ and with §0's |β|²_max. §0's 1.0267e-2 at x = 10¹⁰, n = 1 does not agree:
       there the agent has 1.02620e-2. I re-ran the probe's grid (61 points on [10⁻², 6]):
       - it gives 1.02667e-2 in M units at x = 10¹⁰;
       - it gives 1.02620e-2 in units of φ_t.
     - So the difference is the probe's own absolute-tolerance artefact, which §0 flagged for walls.py's `ATOL`. The
       log does not say this.
     - §0's peak k/ω_t (0.160, 0.198) and its tail slopes lie within that probe's coarse grid of the agent's
       0.165 / 0.207 and κ.
   - **The Planck scale.** From the model at the stored φ_t and E, at the first impacts of the exponential at β = 3.05
     and of IP n = 1 at lab M, β 2, and IP n = 2 at 1e-15, β 2:
     - `classical_turning_point`'s `log_k_ex − ln units.PlanckMass` is 2.424528, 90.049136 and 2.265948;
     - `one_loop_turnaround`'s is 0.440817, 1.222757 and −20.303603;
     - φ_t is the map's in all three.
     - Each equals the map's value to its printed digits.
     - The frame (Einstein) and ln A(φ_t) are stated (verification §2.2). The word "reduced" was not; this commit adds
       one sentence to §2.2 saying that M_P is the reduced Planck mass.
     - Nothing is called a bound, and no history is excluded on it.
4. *The scan* (00:35, load 4.44 to 6.12; 42 s serially). One stage-2 point from each of the six curves:
   - the points were lab n = 1 one-loop ℓ = 20; lab n = 2 gap; 1e-33 n = 1 one-loop; 1e-33 n = 2 gap (not back out);
     1e-15 n = 1 classical; 1e-15 n = 2 one-loop (not back out);
   - all reproduce frac, balance, v_out/v_in, back and |ΔE/E| as committed, except one frac that differs by 1e-6 from
     the four-decimal ln R it was re-run at;
   - the validation point V2 (n = 2, ℓ = 5, the Born limit) reproduces exactly;
   - every one of the 153 curve rows carries |ΔE/E| and whether the field came back out.
5. *The residuals.* An independent script (`drain_fraction`'s formula written out, its constants transcribed from
   `ScalarHistory.py:216–226`), run on the committed `stage2_scan.txt` at n = 1, reproduces `fit_law.txt`:
   - (i) L_E: one-loop ℓ ≥ 1, all points 0.429 / 0.128 / +0.073 (max / rms / mean); mapped 0.021; laboratory curve
     0.052; the plateau 0.409; the gap 0.806, and 0.124 on the laboratory curve;
   - (iv): the best constant L is 0.731 one-loop (max 0.254, against the file's 0.253 from its finer optimizer) and
     1.317 gap (0.445).
   - The hand-back (verification §2.10, log §7) states options and evidence and chooses no law, L, S table or constant.
6. *The records.*
   - The log has §5.1's seven parts. It classifies the nine rulings and D1–D13; D7 (18 processes for under a minute,
     then rerun serially) is its one UNINTENDED DRIFT.
   - The verification document has §2, and `source/wall/README.md` exists.
   - The board has row 02, the Wall gate and the baselines.
   - No issue was opened, so §3, §4 and `.documents/OPEN_ISSUES.md` (32 open) are unchanged.
   - `CLAUDE.md` is unchanged, and no count moved: 26, 705 (219 + 486), 163, 302, 76 before and after, with only the two
     known worktree failures.
   - "This commit" is replaced by `693784b` in the log and on the board.
- **For the user's ruling** (not acted on): U3, U4 and U5 on verification §2.10. Beside them, not a ruling, is the
  Planck-scale table of verification §2.2. Prompt 03 is not written until they are ruled.

## 2. The gates

As README §2.1: Model (01), Wall (02), Law (03), Sample (04), Store (05), Export (06), R (07), Close (08).

- **Model — holds** (prompt 01, 2026-11-14; log 01 §4, `.documents/power-law-wall-verification.md` §1): every hook
  equals its closed form and the centred difference of the hook below it from 1e-90 M to 10 M_P at n = 1, 2 (M = 4e-31
  and 1e-3 M_P) with no overflow; the turning point, the minimum, its derivative and the seed against closed forms or an
  independent root; the one-loop law refuses the model at entry; the exponential goldens bitwise; no suite count fell.
- **Wall — holds** (prompt 02, 2026-11-15; log 02 §4, `.documents/power-law-wall-verification.md` §2): S_n measured at
  n = 1, 2 (1.02620e-2, 5.21615e-3, constant in x); stage 2 at 153 points on the six mapped curves (laboratory M, 1e-33,
  1e-15 M_P), validated by the classical limit (2e-6), the Born limit (7.1e-4), energy (≤ 1.9e-8) and convergence
  (±1e-5 to ±6e-3), the chain equal to `one_loop_turnaround` (1.7e-14); x_p, the slope and κ measured with the
  exponential recomputed by the same code; the adopted law's residuals at L_E, n, L_M, the best constant L and a refit
  stated; nothing ruled by the agent. The options and evidence for U3–U5 are verification §2.10.

## 3. Active and unresolved issues

None.

### 3.1 Assigned to this campaign

None.

## 4. Resolved issues

None.

## 5. Baselines

The suites at `84a2494` (`CLAUDE.md`, counts of 2026-11-13): CosmologyModels 26, ComputeTargets 684 (219 + 465),
ObjectFactories 163, tools 292, RunDrivers 76. 01 re-measures them at its base.

- **01, 2026-11-14, in the worktree:** at the base `75ac6fb` 26, 684 (219 + 465), 163, 292, 76; after 01 26, 705
  (219 + 486), 163, 302, 76. In both, the two worktree failures of orchestrator note 01 ruling 7 (one in RunDrivers, one
  in tools) and no other.
- **02, 2026-11-14 to 15, in the worktree:** at the base `35f6359` 26, 705 (219 + 486), 163, 302, 76; after 02 the
  same. In both, the same two worktree failures and no other.

### History of the status line

- 2026-11-13: planned; R1–R3 ruled; U1–U7 ruled as proposed.
- 2026-11-13: prompt 01 and its orchestrator note written; not dispatched.
- 2026-11-14: prompt 01 landed (`3aae9a5`); the gate Model holds. Its stop on `test_compare_stores` (13) ruled by the
  user, "(a), add the unstored entry to compare_stores". No issue opened.
- 2026-11-14: prompt 01 reviewed by the orchestrator (this board, "Orchestrator review of prompt 01"); all six checks
  pass. The two §1.6 measurements go to the user; 02 is not written.
- 2026-11-14: R4 (φ ≤ 0 stays a failure) and R5 (n = 1 natural mode at large M fails at G2: noted, no issue, move on)
  ruled by the user (README §6.4).
- 2026-11-14: prompt 02 and its orchestrator note written; not dispatched.
- 2026-11-14: prompt 02 and its note amended before dispatch, at the user's request ("Add k_ex/M_P at first impact to
  prompt 02's map"): ln(k_ex/M_P) at every candidate, and five more mapped histories (§1.4).
- 2026-11-14: prompt 02 dispatched at `35f6359`.
- 2026-11-15: prompt 02 landed (`693784b`); the gate Wall holds. No issue opened. U3–U5 go to the user with the
  hand-back (verification §2.10).
- 2026-11-15: prompt 02 reviewed by the orchestrator (this board, "Orchestrator review of prompt 02"); all six checks
  pass. U3–U5 go to the user; 03 is not written.
- 2026-11-15: R6 (U4) ruled by the user: S is the constant S_n on this wall (README §6.5). U3 and U5 still wait; 03 is
  not written.
- 2026-11-15: R7 (U3) ruled by the user: the law on this wall is a fit f_n(ln R) per n (README §6.5). U5 still
  waits; 03 is not written.
- 2026-11-15: R8 (U5) ruled by the user: the spectrum's direct piece is a measured table around the peak, k² below
  it and an exponential above it, on every wall, the exponential's included (README §6.5). This amends README §5's
  rule "The exponential does not move", P2 and the gate Law for the spectrum alone; the standard analysis
  "Relativistic quanta" is repeated after this campaign (convention 8). U3–U5 are all ruled; 03 is not written.
- 2026-11-15: prompt 03 and its orchestrator note written, at the user's request ("Write prompt 03 and its orchestrator
  note"), with the planner's P15–P23 and the questions U8 (the spectrum's scale near the floor) and U9 (the associated
  piece on the new shape) in README §6.6. Not dispatched: it waits for U8 and U9.
