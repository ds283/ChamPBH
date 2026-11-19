# power-law-wall campaign — implementation state

**Last updated:** 2026-11-19 · **Status: LIVE — 12 of 13 prompts landed (07d landed 2026-11-19 on its restart (`b5f863a`, reviewed), the gate R2 holds, no issue opened or closed by it and one opened by its review (a reader refuses a store of an older label by a traceback, not exit 2): 07's grid rebuilt at 2026.14.0 in `power-law-wall-07d-r2-*`, thirteen registered runs, one interrupted and resumed; every history whose law reads no ℓ equal to 07's but P114's k_ex columns, the first attempt's five histories among them; the exponential's eleven one-loop histories under v2 measured, ΔN_eff today −0.4% to −6.7%; the readers on every store, U12's guard on 07's exp store; a stop at the suites before (a timing test across the machine's sleep) ruled (a) by the user; 07c2 landed 2026-11-19, the gate Range holds, one issue closed and none opened: the pulse rate |V‴/V″| as each model's hook in closed form, (n + 2)/φ on the power wall; the stop's histories build; the least margin of 07's grid's assessments 45.0 decades; the label 2026.14.0; 07c2 inserted and written 2026-11-19 on the user's ruling (a) of 07d's stop, on P125–P134, README §6.16; 07d dispatched 2026-11-18 and stopped in run `pl-a`, five power-wall histories at n = 1, β = 2.9, M = 4 × 10⁻³¹ M_P that 07 built failing at the tip, the pulse rate overflowing at φ_t = 2.3 × 10⁻¹⁰³ M_P; nothing committed by it; it restarts after 07c2 in fresh stores, `power-law-wall-07d-r2-*`; 07d written 2026-11-18 with its orchestrator note, on P111–P124, README §6.15; 07c landed 2026-11-18, the gate Consistent holds, one issue closed and one opened: one k_ex, ½E^{1/2}|V‴/V″|, at every use; the exponential's law `one-loop-exponential-v2` in `ExponentialModel`; the power wall's renamed `one-loop-inverse-power-v1`; the label 2026.13.0; 07b2 landed 2026-11-18, the gate Form holds, no issue opened: the min(rise, fall) form and its variants measured; U13 ruled 2026-11-18, the joint fit (fall shared, rise per n) on ln(V₀/M⁴) ≤ 10, README §6.13, P91–P98; 07b landed 2026-11-17, the gate Remeasure holds, one issue opened); re-planned 2026-11-17 on R13–R17 (one k_ex, ½E^{1/2}|V‴/V″|, on every potential and at every use; the exponential's one-loop law refitted under `one-loop-exponential-v2`; README §6.12, P73–P90; U11–U12 ruled as proposed, U13 ruled after 07b2), with 07b–07d inserted before 08 (01, the gate Model holds; 02, the gate Wall holds; 03, the gate Law holds; 04, the gate Sample holds; 05, the gate Store holds, after its ruling-4 stop was ruled (a) by the user; 06, the gate Export holds, one issue opened; 07, the gate R holds, after its §2.2 stop was ruled (a) by the user, one issue opened); 01–07d reviewed; 06 landed on R12 (the potentials treated symmetrically: every export under `<output>/<label>/<model>/` with a `model` column, `--models` a list) and P45–P58 (README §6.10); 07 landed on P59–P72 (README §6.11); R11 (the surfing thresholds at n = 1 recorded as measured) ruled after 04 (README §6.8); P33–P44 stand as proposed (README §6.9); P24–P32 ruled as proposed; D12 left as it is by the user; R4–R5 ruled on 01's measurements (README §6.4); R6–R8 (U4 S = S_n; U3 the law f_n(ln R); U5 the three-region spectrum, the exponential's too) ruled on 02's (README §6.5); R9–R10 (U8 table in x, U9 associated piece) ruled with 03 (README §6.6); R1–R3 and U1–U7 ruled (README §6).**

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
| 03 | The law on this wall | f_n(ln R) and S_n in `InversePowerModel` under `one-loop-measured-pl-v1`; the spectrum as tables on three walls (R6–R8) | Opus | ✍️ | ✅ | `d924275` | [`logs/03-the-law-on-this-wall.md`](logs/03-the-law-on-this-wall.md) |
| 04 | Measure a sample | U7's forty histories under both laws beside the exponential's; the thresholds of README §0.3, the surfing edges, R9's share, M_crit (P24–P32) | Opus | ✍️ | ✅ | `05f6920` | [`logs/04-measure-a-sample.md`](logs/04-measure-a-sample.md) |
| 05 | The store | type id 2, its table, the registry, `FINGERPRINT_FORMAT` 8, the label (U6); the key's drain model at entry and the joins by model type (P33–P44) | Opus | ✍️ | ✅ | `bdb6ae7` | [`logs/05-the-store.md`](logs/05-the-store.md) |
| 06 | Drivers and exports | the model option, the plot scripts, the export layout (P11); the one-loop rows under the model's own drain model, the quanta tools' wall by row, the scope; every potential under `<output>/<label>/<model>/` (R12, P45–P58) | Opus | ✍️ | ✅ | `fda2df2` | [`logs/06-drivers-and-exports.md`](logs/06-drivers-and-exports.md) |
| 07 | Rehearse | registered builds, exponential bitwise, power law resumed; U7's grid in five stores (the exponential at `a7e0978` and the tip, the power law straight and resumed, both potentials in one store), against 04, the readers on every store (P59–P72) | Opus | ✍️ | ✅ | `bbd99f4` | [`logs/07-rehearse.md`](logs/07-rehearse.md) |
| 07b | Re-measure k_ex | research in `source/kex/`: the definition against Erickcek et al.; every stage-2 point re-chained, new points where the exponential's regime moved; the candidate laws for `one-loop-exponential-v2` at n = 1, 2; the power wall's law unchanged; the tables and x_* in the new x; the paths; the inventory (R13–R17, P73–P90); then U13 | Opus | ✍️ | ✅ | `0223d50` | [`logs/07b-remeasure-k-ex.md`](logs/07b-remeasure-k-ex.md) |
| 07b2 | Measure the min(rise, fall) form | research in `source/kex/`: the exponential's law as min(rise in ln R, fall A/(ℓ − ℓ₀) in the new ℓ) and its variants at n = 1, 2, on three selections like for like beside v1 and 07b's (i)–(iv); errors, leave-one-set-out; new points predicted before they are fitted; the crossing, the cap, the edges; the paths; the reference implementation (P91–P98); then U13 | Opus | ✍️ | ✅ | `2f638e0` | [`logs/07b2-measure-the-min-form.md`](logs/07b2-measure-the-min-form.md) |
| 07c | k_ex in production | the V‴ hook, one k_ex at every use, the law as U13 rules (in `ExponentialModel`, P99), the drain-model names, the tables and x_*, the iteration cap, the label (U11–U12; P99–P110, U14) | Opus | ✍️ | ✅ | `c8461ab` | [`logs/07c-k-ex-in-production.md`](logs/07c-k-ex-in-production.md) |
| 07c2 | The pulse rate in closed form | |V‴/V″| as a model hook in closed form on both walls, in place of 07c's composition, which overflows below φ ≈ 3 × 10⁻¹⁰³ M_P; the stop reproduced, the range measured, the label 2026.14.0 (P125–P134, README §6.16) | Opus | ✍️ | ✅ | `bd8d648` | [`logs/07c2-pulse-rate-in-closed-form.md`](logs/07c2-pulse-rate-in-closed-form.md) |
| 07d | Rehearse again | 07's grid at the tip; histories whose law reads no ℓ equal to 07's but the k_ex columns; the exponential's one-loop histories measured against 07's (P111–P124, README §6.15; orchestrator note 07d). *First attempt stopped 2026-11-18 in run `pl-a`, ruled (a); restarted after 07c2 in `power-law-wall-07d-r2-*` (P133)* | Opus | ✍️ | ✅ | `b5f863a` | [`logs/07d-rehearse-again.md`](logs/07d-rehearse-again.md) |
| 08 | Close | the documents, the handover, the issues, the indexes | Opus | ⬜ | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Prompts are written one at a time, each after the previous one lands and is reviewed. Orchestrator notes, one prompt at
a time, in `orchestrator/`: [`prompt-01.md`](orchestrator/prompt-01.md), written 2026-11-13 with prompt 01, used for
01 (dispatched 2026-11-14 at `75ac6fb`); [`prompt-02.md`](orchestrator/prompt-02.md), written 2026-11-14 with prompt 02, used for
02 (dispatched 2026-11-14 at `35f6359`); [`prompt-03.md`](orchestrator/prompt-03.md), written 2026-11-15 with prompt 03,
used for 03 (dispatched 2026-11-15 at `7c60226`, after U8 and U9 were ruled as proposed, R9 and R10), and its §0 holds the f_n(ln R) fit the user ruled R7 on and the
spectrum probes R8 was ruled on; [`prompt-04.md`](orchestrator/prompt-04.md), written 2026-11-15 with prompt 04, not dispatched, and its §0 holds the planner's probe at `99cc79b` (surfing at 10⁻³ M_P, one-loop histories at 10⁻¹⁵ and 10⁻³³ M_P, the M_crit estimate); [`prompt-07.md`](orchestrator/prompt-07.md), written 2026-11-16 with prompt 07, not dispatched, and its §0 holds the planner's survey at `c7ba5ca` (the registry's roots in the main checkout, the data files equal at `a7e0978` and the tip, 04's costs and print formats). [`prompt-07d.md`](orchestrator/prompt-07d.md), written 2026-11-18 with prompt 07d, used for its first attempt (dispatched 2026-11-18 at `e6e50b0`, stopped in run `pl-a`, README §6.16) and amended 2026-11-19 for its restart (P133). [`prompt-07b.md`](orchestrator/prompt-07b.md), written 2026-11-17 with prompt 07b, used for 07b (dispatched 2026-11-17 at `e4ae58b`; landed `0223d50`), and its §0 holds the planner's reading of Erickcek et al. §V B, the size of the change on each wall, crossover's stage-2 rows re-chained at `c798d02` (20 move from gap to one-loop; v1's form misses by 0.19 in the new ℓ) and the inventory as far as the planner took it. Note 02's §0 holds the planner's probe: the path formula, the power wall's scale invariance above V₀, and the
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

**Orchestrator review of prompt 03 (2026-11-15).** All six checks of note 03 §3 pass. The re-runs below were made
serially by the orchestrator at `d924275`, from scratch scripts that are not committed, each timed, with the one-minute
load at its start.
- **The gate at dispatch** (09:46 local).
  - `HEAD` was `7c60226`, two text-only commits after the note's measured `434c747` (the prompt `1b9071a`, R9–R10
    `7c60226`). The worktree held `?? venv` only.
  - The code was as the note records: no `def _drain_L` in `InversePowerModel`, two drain models, `SPECTRUM_MODEL`
    `U4-U17-v1`, and the verification document with §1 and §2.
  - The registry had 62 runs, all finished. The rehearsal lock was free. The main checkout was at `9df9e13` and clean.
  - One thing differed from the note: the load was 10.84 / 7.63 / 6.15, against the planner's 2.44 / 4.52 / 5.57. Free
    disk was 125 GB, as the note recorded.
  - The planner's scratch directory still existed. Its six files were given to the agent as copies, with note §0 proper.
1. *Scope.* `d924275`'s parent is `7c60226`.
   - Its 33 files are the prompt's §4 list. `venv` is not in it, and neither is `.documents/OPEN_ISSUES.md`, since no
     issue was opened.
   - `git diff 7c60226 d924275` touches no driver, plot script, `config/`, `ObjectFactories/`, `extract_common.py`,
     `plot_inputs.py` or store tool, and nothing under `science-runs/` or crossover.
   - The main checkout is still at `9df9e13` and clean. No remote branch contains the commit.
   - **Six tests outside the three quanta modules are edited (log D12, classed STRUCTURALLY REQUIRED).** P22 names only
     the three. Each edit follows from a change the prompt orders:
     - `test_drain_store_records`: `DRAIN_MODELS` gains its third model;
     - `test_a_values_option`: the third model maps to `one-loop-measured-v1` under `model_of_law`;
     - `test_inverse_power_model` (g) and `test_model_option` (4): the one-loop refusal moves to n = 3;
     - `test_plot_quanta_history` and `test_plot_quanta_grid`: the footer's spectrum model is renamed.

     I read all six diffs. None pins an exponential history, golden or fingerprint. `test_a_values_option` failed on
     the agent's first run after the change and was re-pinned without a stop. Whether D12 stands is for the user.
2. *The exponential does not move.*
   - `git diff 7c60226 d924275 -- ComputeTargets/ScalarHistory.py` changes no constant, no S-table literal and nothing
     of `drain_fraction`'s body. It adds the dispatch, the check, the record's drain model, their comments, and the
     regimes imported from `drain_model.py` (the same objects).
   - `test_golden_history`, `test_golden_reflecting_history`, `test_tuning`, `test_store_fingerprint_golden` and
     `test_fingerprint_format_guard`: 54 tests, OK, 181 s, from a load of 3.27.
   - `test_history_and_bbn` and `test_history_and_bbn_drain_law`: 12 tests, OK, 44 s, from a load of 2.93.
   - None of these seven test modules is edited in the commit.
   - The exponential's tool route under the one-loop law, A = 1, β = 2, laboratory M, run from a `git archive` of
     `7c60226` (16 s, load 3.00) and at `d924275` (16 s, load 2.71). The two outputs are identical with timings masked,
     and both equal the committed `tool_route_exponential.txt`.
3. *The law.* A scratch script that does not import `fit_fn.py`, with the constants transcribed by hand (about 1 s, load
   1.94):
   - At every candidate of `ip-n1-M1e-33-b2.0-a0.1`, `ip-n2-M4e-31-b2.9-a0.01` and `ip-n1-M1e-15-b2.0-a0.01` (49, 55 and
     85, all one-loop), production's `drain_alpha_effective` at A = 1, w_gate = 1, through `InversePowerModel`, equals
     min(0.99, f) exactly.
   - From `stage2_scan.txt` with the committed constants, against note §0's:

     | n | points | worst | rms | mapped worst | not back out | §0 |
     |---|---|---|---|---|---|---|
     | 1 | 62 | 0.0541 | 0.0191 | 0.0042 | −0.0177 to +0.0114 | 0.054, 0.019, 0.004 |
     | 2 | 60 | 0.0557 | 0.0215 | 0.0030 | −0.0491 to +0.0262 | 0.056, 0.022, 0.003 |

     Across the switch f is continuous to 1.1e-16 (n = 1) and 0 (n = 2). f peaks at 0.9683 and 0.9659.
   - At n = 3 the one-loop law refuses `InversePowerModel`, realized or not, naming the class, `one_loop_drain_model` and
     n = 3. The fixed law accepts it. n = 1 and 2 are accepted under Planck and GeV units and name
     `one-loop-measured-pl-v1`. The exponential names `one-loop-measured-v1`; it has `_drain_L` and no fraction of its own.
4. *The spectrum.*
   - `spectrum_table.py table` re-run took 47 s, at a load of 1.99. On all three walls its literals equal the committed
     `_LN_BETA2_*` bit for bit (153, 156 and 156 nodes). The first, middle and last entries were matched by eye as well.
   - On `power-n1` I built my own shape from the committed literals: a not-a-knot spline in (ln x, ln s), x² below the
     table, the matched exponential above it, and R10's associated piece. I integrated it with scipy's `quad` at
     rtol 1e-13. Against the module at c_a = 1:
     - I agrees to 1.7e-13 relative, J to 4.2e-14 and G(1) to 1.1e-12;
     - x_p agrees to 3e-15 and κ_hi bit for bit.
   - At both edges s_dir is continuous to the printed 15 digits. At x_hi, d ln s_dir/dx is −2.1574513 on both sides, which
     is κ_hi.
   - On every wall C₁ equals 49 x_* s_dir(x_*) exactly, and s(x_*)/s_dir(x_*) = 50 at c_a = 1. s_assoc is continuous at
     x_p, and C scales with c_a.
   - An unknown wall is a ValueError naming the three.
   - The four readers set `SPECTRUM_WALL = qs.WALL_EXPONENTIAL` with a comment naming prompt 06. The rest of their diffs
     replace `SPECTRUM_MODEL` with `spectrum_model(SPECTRUM_WALL)`.
5. *The tool-route history.* n = 2, re-run at a load of 2.33 (16 s). Its output equals the committed
   `tool_route_ip_n2.txt` with timings masked: 48 one-loop injections, `model=one-loop-measured-pl-v1`, ΔN_eff at 1 MeV
   0.0329852.
6. *The records.*
   - The log has §5.1's seven parts. It classifies rulings 1–10 and D1–D16, with every moved test listed with its old and
     new value (§3.6, §2.2 D12). D1 (the base suite run disturbed by an early edit, then re-run on the clean base) is its
     one UNINTENDED DRIFT.
   - The verification document has §3. `law-03/` holds six files. `source/wall/README.md` is appended.
   - The board had row 03, the gate Law and the baselines.
   - `CLAUDE.md` now gives 732 and 513. Test discovery at `d924275` counts 26, 219 + 513, 163, 302 and 76, which match
     the suites' counts after. The full suites were not re-run by me.
   - `black --check` passes on the 22 Python files changed.
   - No issue was opened, so §3, §4 and `.documents/OPEN_ISSUES.md` (32 open) are unchanged.
   - "This commit" is replaced by `d924275` in the log and on the board.
- **For the user** (not acted on):
  - D12, above.
  - Three of the log's observations (§6):
    - note §0's energy table weighted the spectrum in ln x, not x. The share above x = 3 is 0.41%, 14.5% and 5.8%
      (exponential, n = 1, n = 2), against §0's 0.2%, 6.8% and 2.5%. The tables' tail share (≤ 7.4e-4) is measured
      directly and does not depend on it;
    - f_n peaks at 0.968 | 0.966, above the exponential law's branch caps (0.95, 0.9). Only `DRAIN_ALPHA_CAP` = 0.99
      applies on this wall;
    - `model_of_law` still names `one-loop-measured-v1` for the one-loop law, until 05 and 06.

**Orchestrator review of prompt 04 (2026-11-16).** All six checks of note 04 §3 pass. The re-runs below were made
serially by the orchestrator at `05f6920`, from scratch scripts that are not committed, each timed, with the one-minute
load at its start.
- **The gate at dispatch** (2026-11-15, 19:06 local).
  - `HEAD` was `7d7c578`, one text-only commit after the note's `44b4763` (the brief for this session). The worktree
    held `?? venv` only.
  - The code was as the note records: three drain models; `InversePowerModel` defines `_drain_fraction` and
    `one_loop_drain_model`; the verification document had §1–§3; `sample-04/` did not exist.
  - The registry had 62 runs, all finished. The rehearsal lock was free. The main checkout was at `9df9e13` and clean.
  - The load was 7.42 / 6.88 / 6.62, against the planner's 4.42 / 7.34 / 5.75. Free disk was 122 GB, as the note
    recorded.
  - The export copy's SHA-256 equalled note §0's. The planner's scratch directory still existed, and its sixteen files
    were given to the agent as copies, with note §0 proper and the run's README.
1. *Scope.* `05f6920`'s parent is `7d7c578`.
   - Its 92 files all lie under `prompts/power-law-wall/`, `.documents/power-law-wall/` and
     `.documents/power-law-wall-verification.md`. `venv`, `CLAUDE.md` and `.documents/OPEN_ISSUES.md` are not in it.
   - The verification document gains §4 (+332 lines) and loses nothing.
   - The main checkout is still at `9df9e13` and clean. No remote branch contains the commit.
2. *The sample is U7's.*
   - `runs/` holds forty files, twenty per n, named as the prompt names them. Each has its command, the `uptime` at its
     start and its exit status, wall time and `uptime` at its end. 37 exit 0; the three exiting 1 are n = 1, natural
     mode, 10⁻³ M_P at β 1.2, 2.0 and 2.9 (R5).
   - `tables.py tables`, run from a scratch mirror of `source/sample/`, `runs/` and the extract, rebuilds
     `summary.csv` (40 rows), `comparison.csv` and `thresholds.txt`. All three `cmp` equal to the committed files.
3. *Two histories re-run* through the runner, timings masked, `cand` lines included.
   - n = 2, 10⁻¹⁵ M_P, β 2, A 1 (17 s, load 8.64): identical to the committed file (44 lines). Its tool lines are also
     identical to the planner's `ol-n2-1e-15.txt`.
   - n = 1, laboratory M, β 2.9, A 0.05 (35 s, load 8.03), which no check of the agent's covered: identical (171 lines).
4. *The counterparts.*
   - The export copy's SHA-256 is P25's. The extract's header and its twenty rows are each found verbatim in the copy,
     CR stripped.
   - The exponential's 10⁻¹⁵ M_P tool route, re-run (18 s, load 8.71), is identical to the committed
     `tool-route/exp-M1e-15-b2.0-oneloop1.txt`. It equals the export row at the printed digits: normalization, 28
     reflections, 27 injections, the injected fraction, ΔN_eff at 1 MeV 5.78965e-8, Y_p, D/H, the 1 MeV ratio, and the
     classical (0.0088957) and parking (0.991104) shares.
5. *The thresholds and R9.* From the `cand` tables, by a scratch script, at n = 1 laboratory M β 1.2 A 0.05 (170
   candidates) and n = 2 10⁻¹⁵ M_P β 2 A 1 (23):
   - the largest φ_t (4.2005e-36, 3.30284e-29 M_P);
   - the smallest ln R (57.682118, 46.987453);
   - ρ_kin/ρ_R at the cut, the last candidate in both (9.83412e-7, 9.26948e-7);
   - the smallest ratio before it (1.00755e-6, 1.55772e-6);
   - the smallest ln(E/V₀) (11.464016, 62.08282), with 1/x there;
   - the share of K below E/V₀ = 100 and 10³ (0 and 0).

   All equal the runner's `sample` lines. m_eff/H at the cut is not in the `cand` table; it was read from the `sample
   parking` line (3.53e10, 2.19e10), not recomputed. The M_crit arithmetic of `thresholds.txt` §5 checks at n = 2
   10⁻¹⁵ M_P: 1e-15 · e^{46.987453/4} = 1.26e-10 M_P and 1e-15 · e^{178.8666/4} = 2.63e4 M_P. The exponential's first
   impact gives 1.15e-15 M_P.
6. *The surfing and the records.*
   - Re-run by `tools/surfing_scan.py`: n = 2 at β 3.003, the first EW capture (206 s, load 6.20), and n = 1 at β 1.25,
     the failed row below the onset bracket (4 s, load 3.19). Every column but `wall_s` equals the committed row. The
     n = 1 row fails at G2 again (W/(π²/2) = 1.0457 at N = 36.132).
   - The edges, read from the CSVs by a scratch script, are the log's. Q-exp and Q-n2 have their first QCD capture at
     1.117 after an uncaptured 1.116. E-n2 has its last QCD capture at 2.975, 26 uncaptured successes 2.976–3.002 (2.991
     failed) and its first EW capture at 3.003. C-n1 has 74 failures in 79 rows, QCD captures from 1.30 to 1.80 and no
     EW capture.
   - The log has §5.1's seven parts and classes rulings 1–10 and D1–D12. D1 (a zsh word-splitting refusal, re-run from
     bash) is its one UNINTENDED DRIFT.
   - Verification §4 has §4.1–§4.9. `source/sample/README.md` names each script and what it produced.
   - The board had row 04, the gate Sample and the baselines. No issue was opened, so §3, §4 and
     `.documents/OPEN_ISSUES.md` (32 open) are unchanged.
   - `CLAUDE.md` is unchanged. Test discovery at `05f6920` counts 26, 219 + 513, 163, 302 and 76, the base's counts. The
     full suites were not re-run by me.
   - `black --check` passes on the three Python files.
   - "This commit" is replaced by `05f6920` in the log and on the board.
- **For the user's ruling** (README §2 row 04, before 05): the surfing thresholds at n = 1, verification §4.9. At
  10⁻³ M_P, 74 of 79 coarse rows fail and no EW edge can be bracketed. The agent's options are:
  - (a) record it as measured, as R5 did;
  - (b) run the narrower Q-n1 grid, 1.250–1.300 (log D3's other reading);
  - (c) scan n = 1 at another M, outside U7;
  - (d) treat the n = 1 natural-mode G2 failures as a production issue for a later campaign.

  None is chosen. Beside them, not rulings, are the log's observations (§6): two failure kinds besides G2 in the scans
  (a tuning non-convergence at n = 1 β 3.20, and R4's φ ≤ 0 step underflow at n = 2 β 1.149 and 2.991); T_max blank and
  `n_crossings` one fewer on a reflected first bounce; and the one-loop law's cap and classical branch exercised by no
  history in the sample.

**Orchestrator review of prompt 05 (2026-11-16).** All six checks of note 05 §3 pass. The re-runs below were made
serially by the orchestrator at `bdb6ae7`, from scratch scripts that are not committed, each timed, with the one-minute
load at its start. The machine was heavily loaded by another process during the agent's after-suites (one-minute load
up to 147) and still at 15–28 when the review began.
- **The gate at dispatch** (2026-11-16, 01:44 local).
  - `HEAD` was `fb8bef2`, the note's own commit. The worktree held `?? venv` only.
  - The code was as the note records: `FINGERPRINT_FORMAT = 7`, `VERSION_LABEL = "2026.11.0"`, no
    `ObjectFactories/InversePowerModel.py`, and id 2 in `compare_stores`' `UNSTORED_TYPE_VALUES`.
  - The registry had 62 runs, all finished. The rehearsal lock was free. The main checkout was at `9df9e13` and clean.
  - The load was 8.46 / 5.56 / 5.39, against the planner's 3.24 / 5.45 / 6.71. Free disk was 117 GB.
  - The planner's three probe scripts still existed and were given to the agent as read-only copies, with note §0.
- **The stop.** The agent stopped at ruling 4: the entry check refused
  `test_drain_store_records.test_compute_passes_the_keys_law` (subtest `one_loop`), whose `mock.Mock()` model has a Mock
  for `one_loop_drain_model`. I read the test and confirmed it. The user ruled "(a) here please": the stand-in is given
  `one_loop_drain_model="one-loop-measured-v1"`, and the check is unchanged. The log records it as D1.
1. *Scope.* `bdb6ae7`'s parent is `fb8bef2`.
   - Its 29 files are prompt §5's list plus `test_drain_store_records.py` (D1, by the user's ruling). `venv` and
     `.documents/OPEN_ISSUES.md` are not in it.
   - `ObjectFactories/ExponentialModel.py` has no diff. In `test_store_fingerprint_golden` only `GOLDEN`, the class count,
     the count-0 exception for `InversePowerModel`, the command-line format and the docstring change; `build_store` does
     not.
   - In `ComputeTargets/ScalarHistory.py` the diff is `check_key_drain_model` and its call, with comments, at the head of
     `ScalarHistory.__init__`.
   - The main checkout is still at `9df9e13` and clean. The branch has no upstream; nothing is pushed.
2. *The exponential is unchanged.*
   - `test_golden_history`, `test_golden_reflecting_history`, `test_tuning`, `test_tuned_history_round_trip`,
     `test_drain_store` and `test_dissipation_store`: 86 tests OK in 268 s (load 14.7). The three goldens' modules and
     their `golden_*.py` data are unedited.
   - `tools/history_and_bbn.py 2.0 4e-31 --drain-law one_loop --alpha 1` (17 s, load 8.8), with `#` lines dropped and
     `wall=` and `PRyM_time=` masked, is identical to 04's `sample-04/tool-route/exp-M4e-31-b2.0-oneloop1.txt` (13
     lines each).
3. *The fingerprint.*
   - `spec_digests(config.datastore.factories)`, regenerated, equals `SPEC_DIGESTS[8]`: 17 pairs. Against format 7 only
     `ScalarHistory` moved; `InversePowerModel` is new, with `ExponentialModel`'s digest.
   - The diff of `config/fingerprint.py` removes no line but `FINGERPRINT_FORMAT = 7`.
   - `diff` of the two golden JSON files: exactly note §0's three differences (the format 7 → 8, the class
     `InversePowerModel` at count 0 with the empty digest and no tag sets, and the top digest).
4. *The round trip.*
   - `test_inverse_power_store`, `test_power_law_store_tools` and `test_bbn_from_store_model`: 24 tests OK in 46 s
     (load 6.6).
   - My own probe, on `_DissipationStoreCase` (unchanged since before 05) through the committed factories, crossed the
     serials: `ExponentialModel` n = 1, 2 took 1, 2 and `InversePowerModel` n = 2, 1 took 1, 2. With histories at n = 1
     under both models, each lookup found its own (by failure reason and model class), and `InversePowerModel` n = 2,
     which shares serial 1 with the exponential's n = 1, found nothing.
   - The check reads `alpha.drain_model` and `model.one_loop_drain_model` only; it holds no power-law expression.
   - The keys of prompt §1.7, constructed by me: refused, `InversePowerModel` n = 1 and n = 2 under
     `one-loop-measured-v1` and `ExponentialModel` under `one-loop-measured-pl-v1` (the check's `ValueError`), and
     `InversePowerModel` n = 3 under `one-loop-measured-pl-v1` (the model's own); accepted under `fixed`,
     `InversePowerModel` n = 1, `ExponentialModel` n = 1 and `InversePowerModel` n = 3.
5. *The store tools.*
   - `git grep model_serial` over the tree's Python: outside `.documents/`, `science-runs/`, `prompts/` and the tests,
     the raw joins of a model table are `tools/bbn_from_store.py` (both find queries, and the power law's copies) and
     `tools/outward_step_scan.py:878`. Both match `model_type`; `ObjectFactories/ScalarHistory.py`'s build already did.
   - `test_bbn_from_store`, `test_bbn_from_store_alpha`, `test_bbn_from_store_model`, `test_compare_stores` and
     `test_power_law_store_tools`: 45 tests OK in 42 s (load 7.9). `test_one_loop_tools` and `test_outward_step_scan`:
     38 tests OK in 62 s (load 6.2).
   - `bbn_from_store`'s header: at the defaults `model_label` returns "", so the line is the same string as at
     `fb8bef2`, read from both versions' source.
6. *The records.*
   - The log has §5.1's seven parts, classes rulings 1–10 and D1–D8, lists every changed test with its old and new value,
     and quotes the user's ruling. It has no UNINTENDED DRIFT.
   - The board had row 05, the gate Store and the baselines. No issue was opened, so §3, §4 and
     `.documents/OPEN_ISSUES.md` (32 open) are unchanged.
   - `CLAUDE.md` changes only in the counts and the note of their last change. Test discovery at `bdb6ae7` counts 26,
     226 + 513, 175, 307 and 76, as it says. The full suites were not re-run by me.
   - `config/version.py`'s paragraph says a 2026.11.0 store cannot be read by this tree and is read with the tree of
     its own commit.
   - `black --check` passes on the 25 changed Python files.
   - "This commit" is replaced by `bdb6ae7` in the log and on the board.
- **For 06**, from the log's §7: `main.py` and the plot scripts must look up the power law's one-loop `alpha_value` rows
  under the model's own `one_loop_drain_model`, or every power-law history is refused at construction; every read of
  the store must match `model_type`; `outward_step_scan` stays exponential-only, and re-runs every exponential row at
  n = 1 (an observation, harmless while the science stores hold n = 1 only).

**Orchestrator review of prompt 06 (2026-11-16).** All six checks of note 06 §3 pass. The re-runs below were made by
the orchestrator at `fda2df2`, from scratch scripts and copies of the agent's scripts in the session scratchpad, none
committed, each timed, with the one-minute load at its start (6–11 during the review). The quanta and RunDrivers tests
overlapped one 3 s run of `test_models_main`; nothing else ran beside them.
- **The gate at dispatch** (2026-11-16, 11:42 local).
  - `HEAD` was `b1433f4`, the note's own commit. The worktree held `?? venv` only.
  - The code was as the note records: `FINGERPRINT_FORMAT = 8`, `VERSION_LABEL = "2026.12.0"`, no `--models`, no
    `inverse_power.yaml` or `model_options.py`, and `SPECTRUM_WALL = qs.WALL_EXPONENTIAL` at `tools/quanta_measures.py:98`.
  - The registry had 62 runs, all finished. The rehearsal lock was free. The main checkout was at `9df9e13` and clean.
  - The load was 21.62 / 16.10 / 16.83, against the planner's 26.20 / 16.71 / 18.60. Free disk was 110 GB.
- **No stop.** The agent ran to its commit without asking.
- **D4, the unintended Ray start.** The agent's first base export passed a real `ScalarHistoryProxy`, whose `ray.put`
  started a private local Ray instance inside that process; the export failed and the process exited. It touched no
  cluster, lock or store, and not `/tmp/ray-cpbh`. Its session directory `/tmp/ray/session_2026-11-16_11-52-35_742188_42121`
  is still there, the only session under `/tmp/ray`, with `session_latest` pointing at it. No Ray process was running
  during the review, and none of the review's runs added a session. The export script now patches `ray.init` and
  `ray.put` to raise. The directory is left for the user.
1. *Scope.* `fda2df2`'s parent is `b1433f4`.
   - Its 62 files are prompt §5's list. `CosmologyConcepts/Models/__init__.py` is not among them (the new names are
     imported from `model_options`). `venv` and `var/` are not in it.
   - No diff in `ComputeTargets/ScalarHistory.py`, `config/fingerprint.py`, `config/version.py`, `ObjectFactories/`
     outside its tests, or any tool §3 of the prompt names.
   - In the model classes the diff is `spectrum_wall` and its imports alone. `exponential.yaml`'s diff is the line
     `models: [exponential]`. `pipeline_selection.py`'s diff is two docstrings.
   - `CLAUDE.md` changes only in the counts and the note of their last change. The verification document gains §5 and
     loses no line.
   - No base worktree of the agent's is left. The main checkout is still at `9df9e13` and clean. The branch has no
     upstream; nothing is pushed.
2. *The exponential.*
   - `export_tree.py` from a base worktree of my own at `b1433f4` (4.6 s, load 9.8) and from the tip (3.4 s) on the
     agent's store: both exit 0, and both trees' outputs equal the agent's by `diff -r`.
   - `compare_exports.py` on my two exports exits 0. My own `cmp`: the base's `<label>/` and the tip's
     `<label>/exponential/` hold the same 6 paths. The 5 files other than `histories.csv` are byte-equal. The tip's
     `histories.csv` has `model` at column 3, directly after `n`, holding `exponential` in all 4 rows. With it removed,
     the rows equal the base's, header included.
   - The store's SHA-256 (`store-shard0000.db` `2ec681d9…`, `store.db` `7aa84846…`) are the agent's, before and after
     every export of mine.
   - Serially: `test_golden_history` 3 OK (14 s, load 8.5), `test_golden_reflecting_history` 2 OK (6 s),
     `test_tuning` 40 OK (167 s), `test_driver_options` 14 OK (1 s, load 5.4), the five quanta tools' modules 89 OK
     (81 s, load 5.4), and `RunDrivers/tests` 79 with the one known worktree failure (`test_registered_run…this_repositorys_head`, note 01 §0.1) and no other (27 s, load 6.3).
   - `main.py`'s diff: for one potential the A rows are looked up A by A under its one drain model, after the fixed
     rows in the same `ray.get`; one model `object_get` per potential; the pipelines loop over `alpha_rows` as before,
     skipping another potential's A rows. The grid line is rebuilt from `MODEL_POTENTIALS["exponential"]`, which is the
     base's text, and the A line's model is `one-loop-measured-v1`. The agent's `main_order.py`, run by me from my base
     worktree against the tip, gives "lookups, pipelines and printed lines IDENTICAL" for all ten commands (five
     commands, each as given and with `--models exponential`). `test_models_main` passes, 4 OK (3 s, load 6.5).
3. *The option and the rows.* Parsed by me (with `--database`, which the parser requires):
   - `--models inverse-power --n-values 1 2 --A-values 1`: accepted, A rows under `one-loop-measured-pl-v1`.
   - `--models exponential inverse-power --A-values 1`: `one-loop-measured-v1` for the exponential,
     `one-loop-measured-pl-v1` for the power law.
   - `--models inverse-power --n-values 3 --A-values 1`, and the same with `--n-values 1 3`: exit 2, "--A-values needs
     one one-loop drain model for each potential of --models over the n of --n-values: the potential inverse-power has
     no one-loop drain law at n = 3: …".
   - `--models inverse-power --n-values 3 --alpha-values 0.1`: accepted.
   - No `--models`: `[exponential]`, the rest as at the base. `--config exponential.yaml` gives `[exponential]` with
     16 shards and M = 0.5 M_P. `--config inverse_power.yaml` gives `[inverse-power]` with the same keys, its A rows
     under `one-loop-measured-pl-v1`.
   - `git grep DRAIN_MODEL_ONE_LOOP` in production code matches the log's list (`drain_model.py`,
     `ExponentialModel.py:34, :100`, `ScalarHistory.py`'s import and comments, two docstrings). None is in a driver,
     the parser or `plot_inputs.py`. `model_of_law` has no production caller.
4. *The power law's exports.*
   - The tip's `<label>/inverse-power/` holds `histories.csv` (`model` after `n`, `inverse-power` in all 6 rows: n = 1
     and 2 at β = 2 under both laws, a fixed-law marker and a one-loop stand-in failure at β = 1.185) and 10 CSV
     files under `alpha=0.1/` and `one-loop-measured-pl-v1/A=1/`, by n.
   - `tools/quanta_measures.py` on a copy of that root (3.3 s): exit 0, "(R8-power-n1-v1, R8-power-n2-v1, assoc scale
     1): measured 2, skipped as fixed-law 3, failed histories 1". In `quanta_histories.csv`, n = 1 gives
     `R8-power-n1-v1` and n = 2 gives `R8-power-n2-v1`.
   - My own `plot_inputs` probe. On the agent's store, its shard opened read-only through the undecorated actor:
     `--models exponential` finds the two `ExponentialModel` rows, and `--models exponential inverse-power` at n = 1
     finds each potential's two rows in its own table. The A rows are under `one-loop-measured-v1`,
     `one-loop-measured-pl-v1`, or both, in that order. (`--models inverse-power --n-values 1 2` raised the actor's
     `ReadOnlyWrite` for (n = 2, β = 1.185), which the store does not hold. The plain actor raises at lookup and not at
     `ray.get`, where `stored_models` skips a miss. The store's `shard_keys` table is empty, so a `ShardedPool` cannot
     route a sharded lookup on it: a property of the harness store.) On a mixed store of my own, built with
     `test_models_plot_inputs._build_store` through the stand-in pool and reopened read-only: inverse-power at n = 1, 2
     finds its two rows, the missing (n = 1, β = 1) skipped. An A stored only under `one-loop-measured-v1` is refused
     for the power law, naming `--A-values 0.5 (one-loop-measured-pl-v1)`. On an exponential-only store,
     `--models inverse-power` is refused: "the store holds no InversePowerModel for --models inverse-power".
     `ray.is_initialized()` was False after each probe.
5. *The analyses' readers.* `git show main:` of `m-trend/m_trend.py` reads `bounces.csv` copies at
   `inputs/bounces/beta=<β>/M=<M>eV_n=1/`, the export's leaf below `<label>/<model>/<drain_model>/A=<A>/csv/`, and
   `changes/changes.py` has `KEY = ["M_Mp", "beta", "drain_model", "alpha"]`. Both agree with §2.3's table.
6. *The records.*
   - The log has §5.1's seven parts. It classes rulings 1–10 and D1–D14, with one UNINTENDED DRIFT (D4), and lists
     every changed test with its old and new value. It held §2.3's table only by reference to verification §5.8; the
     review copies the table into it, as prompt §5 asks.
   - The board has row 06, the gate Export, the baselines, §3's new issue and the history line.
     `.documents/OPEN_ISSUES.md` has the issue under the new §1.26, and the count is 33.
   - Test discovery at `fda2df2` counts 26, 783 (226 + 557), 175, 312 and 79, as `CLAUDE.md` says. The full suites were
     not re-run by me.
   - `black --check` passes on the 45 changed Python files.
   - "This commit" is replaced by `fda2df2` in the log and on the board.
- **For 07**, from the log's §7: `--n-values` is one list for every potential of a run, so a power-law build at
  n = 1, 2 beside an exponential at n = 1 is two runs (or both potentials at both n), and a plot run over a store
  holding the potentials at different n is one run per potential. 07's commands must say which. `plot_quanta_grid`
  refuses a power-law root with n = 1 and 2 at one (β, M) (§3), and 2026.11.0's `changes.py` key has no `n`; both
  bear on the science run, not on 07.

**Orchestrator review of prompt 07 (2026-11-16).** All seven checks of note 07 §3 pass, with the user's ruling (a) on
the agent's stop at §2.2. The re-runs below were made by the orchestrator at `bbd99f4`, from scratch scripts in the
session scratchpad (read-only `sqlite3` with `mode=ro`, none committed), one at a time, with the one-minute load 5–10
during the review.
- **The gate at dispatch** (2026-11-16, 18:40 local). `HEAD` was `945bf5d`, the note's own commit, and the worktree held
  `?? venv` only. The main checkout was at `9df9e13` and clean. The registry held 62 runs, all finished; the lock was free
  and ports 6380, 6381, 8267 and 52367 free. The load was 5.67 / 17.86 / 19.33 (falling); 113 GB free.
- **The stop.** At §2.2 `compare_stores.py` on `pl` and `pl-resumed` exited 1, its only differences the three R5 failure
  rows and their tags, whose `failure_reason` carries main.py's run label. I confirmed it from the agent's output before
  putting it to the user, who ruled "(a) please"; relayed in those words, and the agent resumed.
1. *Scope.* `bbd99f4`'s parent is `945bf5d`. Its 203 files are prompt §7's list: the verification document, the log,
   the board, `.documents/OPEN_ISSUES.md`, and `rehearse-07/` (12 scripts and 187 small text files under `out/`, no PDF or
   PNG). No code, test, config, `CLAUDE.md`, `.gitignore` or `PRyM/` file; nothing under `var/`. The worktree holds
   `?? venv` only, the main checkout is clean at `9df9e13`, `git worktree list` shows the two trees, the branch has no
   upstream. `$MAIN/var` gained only `datastores/power-law-wall-07-*` (five stores, 16 shards and a sidecar each) and
   `runs/power-law-wall-07-*` (16).
2. *The counts,* by discovery, mine: 26, 226 + 557, 175, 312, 79.
3. *The cluster, the registry and the trees.*
   - `ray_cluster.sh status` exit 0 and the lock's `status`: free, no process, the four ports free.
   - `runregistrykit list`: 78 runs, all finished; this prompt's 16 are 15 `done` and `pl-resumed-a` `killed`.
   - Every manifest's `git_head` and script hash, read by me: the three base runs `a7e0978` and `7b93fae4…`, which is my
     own SHA-256 of `git show a7e0978:main.py`; the thirteen tip runs `945bf5d` and `27056364…`, my SHA-256 of the tip's
     `main.py`. `git_dirty` is True in all sixteen, as the log says (the `venv` link; the untracked `rehearse-07/`).
   - The probes as committed: every process (driver, task, actor) of each tree imports that tree's modules from that
     tree's root as working directory, at 2026.11.0 / 7 for the base and 2026.12.0 / 8 for the tip, and reads
     `Xav_EOS_data.csv` `690090c8…119d` and `PRyMrates/` (84 files) `49b4cea2…9224` in both.
   - The sidecars: `fingerprint_format` 7 for `base`, 8 for the other four; `pl` and `pl-resumed` both `9d4fcf02…ff7a`.
4. *Bitwise, independently.* My script matches histories by (potential, M, β, n, drain model, α) across the sixteen
   shards and compares every shared column of `ScalarHistory`, `ScalarHistoryValue` (by redshift), `ScalarHistoryBounce`
   (by ordinal), `AdiabaticHistory`/`Value` and `BBNData`/`Value` by `float.hex`, serials, timestamps, labels and timing
   columns aside, and `failure_reason` under the ruled mask:
   - `base`/`exp`: 20 matched each way, 2 983 822 values, no difference;
   - `pl`/`pl-resumed`: 40 matched, 5 537 764 values, no difference; the three reasons equal only under the mask;
   - `mixed`/`exp` (the exponential): 20, 2 983 822, none; `mixed`/`pl` (the power law at n = 1), both ways: 20,
     2 547 734, none, three reasons equal under the mask.
   My counts sit a few hundred below the agent's (2 984 336, 5 538 777, 2 548 230) because I skip every `_serial`
   column and `extra_data`, which the agent resolves and compares; nothing compared by both differs.
5. *R, independently.*
   - `tools/compare_stores.py` on `pl` and `pl-resumed`, run by me: exit 1, `problems: none`, its output equal to the
     agent's (timing lines aside): `ScalarHistory` and `_tags` 3 and 3 on one side, the failure rows. `store fingerprint`
     on each: `9d4fcf02…ff7a`, format 8, `problems: none`. The stores' bytes were unchanged by both.
   - Against 04, five histories of my choosing, from `pl`: n = 1 at 10⁻³³ M_P (A = 1), n = 2 at laboratory M, β = 2.9,
     A = 0.05 (157 candidates), n = 2 natural mode at 10⁻³ M_P, β = 1.2, n = 1 natural mode at 10⁻³ M_P, β = 2 (a failure),
     and n = 1 at laboratory M, β = 1.2, α = 0.1. The tuning line's fifteen values bit for bit by `repr`, Y_p and D/H at
     ten digits, ΔN_eff at the printed six, and the first and last `cand` rows (index, N, kind, ln R, ℓ, regime,
     α_eff, w_gate, resolved) at their printed formats: 111 values, no difference. The failure's stored reason, label
     masked, is a prefix of 04's. A fixed-law history's printed candidates are not held (`bounce_rows` NULL), as
     `compare_04.py` lists them.
6. *The readers and the exports.*
   - `export-base` against `export-exp/…/exponential/`, by my own comparison, R5's `quanta_*` left out: 40 paths each;
     31 byte-identical, `histories.csv` identical once `model` (`exponential` throughout) is removed, and 8 `data.csv`
     differing only in `scalar_compute_time`, `NP_compute_time` and `BBN_compute_time`.
   - `export-mixed/…/exponential/` against `export-exp`: 63 paths, 55 identical, 8 timing-only. `export-mixed/…/inverse-power/`
     against `export-pl`: pl's 61 extra files all under `_n=2` paths; 50 identical, 7 `data.csv` timing-only;
     `quanta_histories.csv` equal to pl's n = 1 rows in order; `histories.csv` equal to them but for `failure_reasons`,
     which are equal under the ruled mask.
   - `tools/quanta_measures.py` re-run by me on a copy of `export-pl`'s root (6.9 s): exit 0, "measured 22, skipped as
     fixed-law 48, failed histories 26"; all 45 of its files byte-identical to the agent's; `spectrum_model` is
     `R8-power-n1-v1` on the 11 n = 1 rows and `R8-power-n2-v1` on the 11 n = 2 rows.
   - R4: the four logs (three refusals, one re-run) end in the named refusal, exit 2, and no output directory holds a
     file. R6's four re-solves equal the stored Y_p and D/H at ten digits. Every one of the 90 store files recorded in
     `shasums.txt` has its last recorded SHA-256 now.
   - One of `plot-pl`'s figures looked at (`M=0.00097413eV_n=1/Delta_N_eff.png`, laboratory M, n = 1): five rows
     ordered as expected, A = 10 the highest; the legend covers A = 10's point at β = 2.9, as the log notes.
   - **Found in the review, not in the log:** R4's first refusal (`--models inverse-power` on `exp`, with the A rows)
     prints the right refusal and exits 2, but its log also holds two Ray "Unhandled error" tracebacks
     (`ReadOnlyMiss` for A = 1 and 10 under `one-loop-measured-pl-v1`). `plot_inputs._get_parameter` reads the lookups one
     by one and raises at the first miss; the others' errors are never retrieved, and Ray prints them as the driver
     exits. Nothing is written, and the refusal is right. Opened as `[07-a-refused-plot-leaves-ray-errors-unretrieved]`
     (§3).
7. *The records.*
   - The log has §5.1's seven parts. It records rulings 1–9 of note 07 §0 with their classes, and D1–D15, three of them
     UNINTENDED DRIFT (D9 malformed R1 calls, D10 bugs in the agent's own scripts, D15 one throwaway file in `/tmp`). D1
     records the stop and the user's ruling in their words. Its §7 gives 08 the stores, the exports, the gate and the costs.
   - The verification document loses no line; §6 is appended, the commands verbatim.
   - The board has row 07, the gate R in §2, §3's new issue, the baselines and the history line;
     `.documents/OPEN_ISSUES.md` has the issue under §1.26, and the count was 34.
   - `black --check` passes on the ten committed Python files. The Ray token's contents appear nowhere in the commit (the
     matches for `ray-cpbh` are `ray_cluster.sh`'s own status output).
   - "This commit" is replaced by `bbd99f4` in the log and on the board.
- **For 08**, from the log's §7: the five stores stay in `$MAIN/var/datastores/power-law-wall-07-*`. The science run's
  handover takes the costs measured here (laboratory M about the exponential's; the n = 2 natural mode at 10⁻³ M_P about
  twice), `--n-values` one list per run, `plot_quanta_grid`'s refusal at two n (§3), and `compare_stores`' blindness to
  two builds that hold a failure (§3).

**Orchestrator review of prompt 07b (2026-11-17).** All six checks of note 07b §3 pass. The re-runs below were made by
the orchestrator at `0223d50`, serially, from scratch scripts in the session scratchpad (none committed), with the
one-minute load 3.5–5.0 during the review.
- **The gate at dispatch** (2026-11-17, 11:23 local). `HEAD` was `e4ae58b`, with `a64d8af` and `bbd99f4` ancestors; the
  worktree held `?? venv` only; `source/kex/` and `kex-07b/` did not exist; the verification document ended at §6;
  crossover was last changed at `18add05`; `AbstractModel.py:873` still took `lambda_ta = -self._d_logV_dphi(...)`. The
  registry held no run `running` or stale. The load was 24.51 / 14.90 / 13.00. The main checkout was not read: the
  session's brief forbids it, and 07b needs nothing from it.
- **The paper.** arXiv:1310.5149 fetched as PDF into the scratchpad (the one fetch), its text extracted with
  `pdftotext -layout` (2018 lines); §V B and Eqs. (62)–(72) are in it. Not committed.
- **No stop.** The agent ran without one.
1. *Scope.* `0223d50`'s parent is `e4ae58b`. Its 43 files are ruling 8's: `source/kex/` (22: README, nine scripts, five
   crossover copies at `18add05`, seven `source/wall/` copies at `c798d02`), `kex-07b/` (17 text files), the verification
   document, the log, the board and `.documents/OPEN_ISSUES.md`. Nothing outside `prompts/power-law-wall/`,
   `.documents/power-law-wall/`, the verification document and the index; no `venv`. Crossover's directory, `source/wall/`
   and `CLAUDE.md` are unchanged from `e4ae58b`, and no `__pycache__` is under `prompts/power-law-wall/source/` or
   crossover's directory, before or after my re-runs. The branch has no upstream; nothing is pushed.
2. *The re-chaining.* `rechain.py`, re-run by me with `python -B`: its output equals the committed `rechain.txt` in every
   line but the two `uptime` lines (507 lines; 371 rows, bitwise to the copied `born.chain` at 371 of 371, every recorded
   ℓ within its printed digits). The 20 n = 1 moves of note §0.3 are exactly crossover's n = 1 moves in the table
   (2197 ×4, grid L = 1.3 ×5, 2.0 ×4, 3.0 ×4, 4.0 and 6.0 at ln R = 10, L-M8e-18 #3). The old-form fit is check 4's.
   One figure the hand-back abbreviates: at n = 2 ℓ_new − ℓ_old reaches 3.71 in L ∈ [8, 10) (`rechain.txt` :483–:489),
   above the 1.46–1.95 the hand-back quotes at L = 4.
3. *One new point and one grid-reference point*, re-run by me serially through `stage2_kex.task_point`: the reference
   2197 A0.01 #183 (gap → one-loop) gives f 0.82626, balance 0.82626, v_out/v_in 0.4163, |ΔE/E| 3.6e-11, 289 modes,
   23 s at load 4.44 → 4.65; the new point "lab n=1 ℓ_new~0.55" gives f 0.33566, v_out/v_in 0.8143, |ΔE/E| 1.9e-10,
   261 modes, 14 s at 4.65 → 4.37. Both equal the committed rows at every printed digit.
4. *The fits.* `fits.py`, re-run: `fits.txt` equal to the committed file but for its two `uptime` lines, and
   `fits.json` (which it rewrites in place) byte-identical (`git status` clean after). It prints crossover's constants
   reproduced. Candidate (i) at n = 1 under fit_law's selection, recomputed by my own script from `fits.json`'s
   parameters, the per-point f and prompt §1.5's selection: one-loop max |res| 0.3530 (rms 0.0983, 43 points), plateau
   0.3495 (0.2249, 18), gap 0.0470 (0.0144, 21), as committed (0.353, 0.350, 0.047).
5. *The tables and x_*.* `spectrum_kex.py table` re-run into the scratchpad (49 s, the pool of four, load 3.6–5.0): the
   literals file is byte-identical (`cmp`) to the committed `spectrum_tables.txt`, all three walls. The power walls'
   k_ex/ω_t, 1.0606601717798214 and 0.816496580927726, equal (n + 2)/(2√(n(n + 1))) to 2.1e-16 and 1.4e-16 relative.
   x_* = 0.0029/0.5163952511757223 = 0.005615853347600149, and each X_LO = 10⁻³/(k_ex/ω_t), to the last digit; in k/ω_t
   on the power walls 0.0029/(k_ex/ω_t) = 0.002734 and 0.003552, as stated.
6. *The inventory and the records.*
   - A fresh `git grep` of the note's pattern outside `prompts/`, `.documents/`, `science-runs/`, `thirdparty/` and
     `PRyM/` hits 46 files; every one is in `inventory.md` (16 by file name alone) but `tools/tests/test_build_w_table.py`,
     whose only hit is "chec**k_ex**its" in a test name.
   - The log has §5.1's sections, 22 classed deviation entries (D1–D14 and rulings 1–8); verification §7.1–§7.10 exist;
     `kex-07b/` and `source/kex/` exist, the latter with a README.
   - The board has row 07b, its items, the gate Remeasure and the §3 issue
     `[07b-the-one-loop-fixed-point-needs-more-than-100-iterations-near-the-chain-end]`, which is in
     `.documents/OPEN_ISSUES.md` (35 → 36, 2026-11-17).
   - `CLAUDE.md` is unchanged. Counts by discovery, mine: 26, 226 + 557, 175, 312, 79, the base's.
   - "This commit" is replaced by `0223d50` in the log and on the board.
- **For the user.** (a) Ruling 4: the gap reference point #512 is off its record by −0.0032, inside the prompt's ±6×10⁻³
  and outside crossover's own ±2×10⁻³. The agent showed that both forms give the same fraction at the same k_max
  (0.15623), so the difference is the record's grid top, not the label (`stage2_reference_kmax.txt`; log §6 item 2); it
  did not stop, reading ruling 4's "stated precision" as the prompt's ±6×10⁻³. (b) D13: the spectrum runs were started
  beside the stage-2 validation, and the load reached 32.9; every stage-2 start was at a load of 12.2 or less, so ruling 3
  held for the stage-2 points. (c) Which points the exponential's law must describe (fit_law's selection or note §0.3's)
  moves every candidate's one-loop residual (0.35–0.62 against 0.07–0.19 at n = 1); that choice is part of U13.

**Orchestrator review of prompt 07b2 (2026-11-18).** 07b2 had no orchestrator note; its prompt's §5 stood in for
one. The checks below are those of note 07b §3, applied to 07b2. All pass. The re-runs were made by the orchestrator at
`2f638e0`, from 01:23 local, at one-minute loads of 7–8.
1. **Scope.** `2f638e0`'s parent is `dec7793`. Its 30 files are the prompt's §4 paths: new files in `source/kex/`
   (`minform*.py`, `fig/`, three PNG), `kex-07b2/` (13 files), the verification document (+224 lines, additions only, §8),
   the log, the board, and an added section of `source/kex/README.md` (no line removed). No 07b output, crossover file or
   `source/wall/` file changed; no `__pycache__` in either. Nothing outside `prompts/power-law-wall/` and `.documents/`;
   `CLAUDE.md` and `.documents/OPEN_ISSUES.md` unchanged. Nothing pushed.
2. **The fits.** `minform_reproduce.py`, `minform_fits.py base`, `minform_fits.py new`, `minform_prediction.py` and
   `minform_ref.py` re-run: each output equal to the committed file apart from its timing lines; `minform.json` byte-equal
   after the re-run. So 07b's (iii) and (iv) constants and README §6.13's (v) are reproduced as the log states.
3. **Two new stage-2 points** re-run serially through `minform_stage2.task` from a scratch driver: the first held-out
   point (n = 1, f 0.6229463793528066, 18 s at load 7.16) and the fourth n = 2 laboratory-curve point (f
   0.9274983867800551, 17 s at load 6.38). Every recorded field but the timing equal to the committed `PT` line.
4. **The prediction came first.** `prediction.json` says written 01:07:44 at `dec7793`; `stage2_points.txt` says the run
   started 01:07:48 and its first point 01:07:49. Its constants equal `minform.json`'s `without_new` S3 (v) at both n,
   bitwise, so the 36 points were predicted from the fit without them.
5. **One residual recomputed** by a scratch implementation of the form from `minform.json`'s `with_new` S3 (v) constants
   and points: max |res| 0.0739, rms 0.0208 (86 points, n = 1); 0.0639, 0.0213 (60, n = 2), as §8.3 gives; the scratch
   values equal the stored `f_fit` exactly at every point.
6. **Records.** The log is in §5.1's form (sections 1–7, every deviation classed, D1–D15); verification §8 and `kex-07b2/`
   exist; the board has row 07b2, the gate Form and the baselines. No issue opened. Corrected here: the status line's
   "9 of 13" (12 prompts in all), and "this commit" replaced by `2f638e0`.

**For the user (U13).** On S3, the histories' domain, with the new points, (v) misses by 0.074 at n = 1 and 0.064 at
n = 2, against v1's 0.098 and 0.125 in its own chain; on its falling branch by 0.015 at both. The L term in the fall is
zero within its error (C 0.021 ± 0.032, 0.008 ± 0.039), A and ℓ₀ agree between n = 1 and 2 within errors, and the
36 points it predicted before they ran it met to 0.071. Every option misses the corners above ln(V₀/M⁴) = 10 by 0.6–0.7.
The agent's observations to weigh: the measured f exceeds (v) near the peak where the field did not come back out (up to
+0.029), points the fits leave out; the rise, v1's gap form, carries the largest residuals, at small L; at n = 2 the rise
rests on one grid.

**Orchestrator review of prompt 07c (2026-11-18).** 07c had no orchestrator note; its prompt's §5, "What the
orchestrator will check on return", names the checks below. All pass. The re-runs were made by the orchestrator at
`c8461ab`, from 12:36 local, at one-minute loads of 4.6–5.6, with scratch probes not committed.
1. **Scope.** `c8461ab`'s parent is `c4258ef`. Its 50 files are of the kinds §4 names: 12 production files, 34 test files
   (one new, `test_k_ex.py`), `CLAUDE.md` (the two drain-model mentions, 2 lines, nothing else), the verification document
   (+187, −0: §9 only), the log, the board and `.documents/OPEN_ISSUES.md`. The three production files beyond the models,
   the drain models, `ScalarHistory`, the spectrum, the parser and the label (`CosmologyConcepts/alpha_value.py`,
   `tools/bbn_from_store.py`, `tools/history_and_bbn.py`) are inventory rows. No file under `source/`,
   `.documents/power-law-wall/` or a closed campaign's; `venv` not staged; no remote branch `power-law-wall`, so nothing
   pushed.
2. **The suites by discovery:** 26, 805 (226 + 579), 175, 312, 79, as the log states; the +22 all in the `test_[g-z]`
   half; no count fell. As a spot check `test_k_ex`, `test_measured_drain_law` and `test_quanta_spectrum` were run at the
   tip: 71 tests, OK, 14.8 s at load 5.64.
3. **§1.3's bitwise check**, by a scratch probe that imports `minform.fraction` from the record (`-B`) and calls
   `ExponentialModel.py`'s `one_loop_fraction`: the eight constants equal `minform.json`'s `with_new` `n12/S3/v-J`
   bitwise; at the 146 fitted points (n = 1: 64 one-loop, 22 gap; n = 2: 45, 15), 0 differences from the reference and 0
   from the stored `f_fit`; at 200 000 seeded random inputs, 0 differences, covering f = 0 below R₀ (79 056 inputs),
   1 + Q ln L ≤ 0 at one-loop and gap candidates (32 736 and 10 870), the rise active (25 795), the fall active (12 450),
   the gap (39 093), ℓ = ℓ₀ exactly, ln R = R₀ exactly and ln R down to 1e-300.
4. **History 2 re-run at the tip** (the exponential, n = 1, fixed α 0.1, reflecting, β 2, M 4e-31 M_P), through the
   agent's wrapper (read first: it calls `tools/history_and_bbn.py`'s `main` unchanged and dumps the payload), 17.7 s at
   load 4.56, against the agent's saved base output, by the orchestrator's own comparator over 73 834 leaves. Every sample,
   tuning, bounce, ledger and BBN value equal. What differs is the timings and the k_ex columns, as the log states: the
   generations' `log_k_ex` and `log_q` (largest change 0.2118), `birth_margin` (0.2146), `reflection_margin` (0.2136),
   `tracker_margin` (0.2118) and `log_m` (0.0028); the turning point's `ell_t` and `log_k_ex` (0.0683), `lambda_t`
   replaced by `pulse_rate_t`; the worst margins +0.180, +0.180, +0.178. Equal to the agent's tip output in everything but
   the timings. The wrapper keeps each sample as its `SampleValues` repr, which round-trips every float field but the two
   0-d g* arrays (printed to 8 digits); those are functions of `log_T_Jordan`, which is compared exactly.
5. **One spectrum kernel against quadrature:** G by the orchestrator's own piecewise adaptive quadrature in ln x (400
   pieces from 1e-40 to x_hi + 200/κ_hi, epsrel 1e-13) with its own I, on power-n1, where `G_Y_MIN` moved, at the new
   x_lo, at 4.9e-4, 5.0e-4 and 5.1e-4 either side of `G_Y_MIN`, and at 1e-3, 0.01, x_p, 1, 10, 49 and 51; and at four
   points on each other wall. Worst 3.9e-10, at y = 4.9e-4 on the small-y asymptote (the log's bound there 4.3e-10),
   below 7e-12 everywhere else; the gate's bound 1e-7. The three `_LN_BETA2_*` tuples equal `spectrum_tables.txt`'s text
   character for character; `SPECTRUM_MODELS` and `X_STAR` as P104.
6. **The `git grep` of §3** outside the records: 38 lines, each a retired name's definition or refusal (`drain_model.py`,
   `test_power_wall_law.py`), a dated comment (`config/version.py`, `AbstractModel.py`, `InversePowerModel.py`,
   `quanta_spectrum.py`, three test docstrings), the verbatim copy of the old method
   (`test_unresolvable_turning_points.py`), `test_measured_drain_law.py`'s list of the names v1 left, or the unknown name
   `one-loop-measured-v0` in two refusal tests.
7. **The re-pin list against `git diff` of the tests:** each of the 34 test files is named in the log; 18 change only
   names (with the renames undone and black's re-wrapping set aside, the removed and added text are equal); the numeric
   re-pins (`test_quanta_spectrum`, `test_drain_law`'s 2197, `test_wall_model`, `test_inverse_power_model`, the label in
   two ObjectFactories tests, `test_quanta_measures`) are as the log lists them, old value and new.

Corrected here: "this commit" replaced by `c8461ab` in this board's row and change log and in the log's header and §7;
the status line's "01–07b2 reviewed". `.documents/OPEN_ISSUES.md` needed no correction (36 open: 07b's closed, 07c's
opened, dated 2026-11-18).

**For the user.** The new issue, `[07c-the-third-hook-leaves-float-range-below-phi-1e-103-mp]`, is a limit this change
introduces: below φ ≈ 1e-103 M_P the turning points now raise where they returned before; by the agent's estimate it is
reached only on the power wall at n = 1 for M ≲ 1e-39 M_P, below 07's grid (1e-33). Lifting it needs scaled hooks, which
P102's interface does not have. Choices within P99 and P101 to look over: `drain_alpha_effective` no longer takes L, and
`model` and `log_E` are keyword-only and required (D7); `model_of_law` is removed, nothing in production calling it (D8).
One test tolerance was loosened: `test_quanta_measures`'s phase slip is now compared to 1e-12 of the larger of its two
phases, not of the slip, since with the one k_ex the stored history's smallest slip cancels to 1 part in 473 (D9). On
power-n1 the associated piece now carries 1.6e-2 of I at c_a = 1 (1e-3 or less before), so Relativistic quanta's
outputs on the power wall move with the tables (P89).

**Orchestrator review of prompt 07c2 (2026-11-19).** 07c2 had no orchestrator note; its prompt's §5, "What the
orchestrator will check on return", names the checks below. All pass. The re-runs were made by the orchestrator at
`bd8d648`, from 03:45 GMT, at one-minute loads of 2.1–3.2, with scratch probes not committed.
1. **Scope.** `bd8d648`'s parent is `7a202b7`. Its 14 files are §4's: the three model files and `config/version.py`,
   six test files (the two ObjectFactories ones for the label alone), the verification document (+176, −0: §10 only),
   the log, the board and `.documents/OPEN_ISSUES.md`. The production diff is the new hook on `AbstractModel` (raising,
   no default), its closed form on `ExponentialModel` and `InversePowerModel`, `_pulse_rate` calling it with its check
   kept, and comments. No `CLAUDE.md`, `.gitignore`, `PRyM/`, README or record change; nothing under `var/` tracked;
   `git status` `?? venv` only; nothing pushed.
2. **The suites by discovery:** 26, 809 (226 + 583), 175, 312, 79, as the log states; the +4 in the `test_[g-z]` half.
3. **The hook against the composition**, at 3000 seeded random points per model of the orchestrator's own (n = 1, 2;
   M from 1e-34 to 1e-2 M_P; the power wall's φ from 10^−101.5 to 10 M_P, the exponential's u from 1e-3 to 1e3), through
   the models' own hooks: worst 2.00 eps (power) and 3.00 eps (exponential), within the agent's 32 eps; the power wall's
   hook equal to (n + 2)/φ bit for bit at all 3000, and finite at the stop's φ_t, 1e-150 and 1e-300 M_P.
4. **History 2 re-run at the tip** (the power wall, n = 2, one-loop A = 1, β 2, 4e-31 M_P), through the agent's wrapper
   (read first: it runs `tools/history_and_bbn.py`'s `main` unchanged and dumps the payload), 17 s at load 2.1, against
   the agent's saved base output by the orchestrator's own comparator over 77 174 leaves: equal but for the timings and
   18 injections' `pulse_rate_t` (largest 2.75e-16 relative), as the log states; equal to the agent's tip output but for
   the timings.
5. **Histories 4–5 against 07's store.** History 5 (n = 1, β 2.9, 4e-31 M_P, A = 1) re-run at the tip, 19 s at load 2.8,
   against 07's `pl` serial 28 read by `sqlite3` `mode=ro`: ΔN_eff today 0.024529199130091498 and at 1 MeV
   0.02364613445286405, Y_p 0.24713074010549813, D/H 2.4657251493105763, normalization −276.63072859401797, one tuning
   iteration, parking N 33.375861349060195, 62 reflections and 123 bounce rows, each equal.
6. **The range, at three histories,** by the orchestrator's own reading of 07's `pl` samples (ln ρ_kin = 2 ln H +
   2 ln(−π) − ln 2, E = ρ_kin + V₀u as `assess_infall` forms it, φ_t = M(V₀/E)^{1/n}): serial 23 (n = 1, β 2.9, 4e-31)
   least φ_t 1.0748e-109 M_P at N 5.0424, 45.01 decades; serial 93 (n = 1, 1e-33) 3.5320e-104, 50.52; serial 95 (n = 2,
   1e-33) 5.9431e-69, 85.51; each equal to the agent's table to its digits. The end of the range, √(n(n + 1)/DBL_MAX),
   is 1.0548e-154 and 1.8269e-154 M_P, as the log states.
7. **The `git grep`** of `_d3_logV_dphi3` outside the records and tests: the hooks, the delegate, comments and
   `config/version.py`'s dated comment; no production caller.
8. **The re-pin list against `git diff` of the tests:** the removed assertions are the raises naming `d3_logV_dphi3` in
   `test_inverse_power_model` (d) and `test_k_ex`, the `NotImplementedError` of a model without the third hook, and the
   two labels; the added ones are the returns, `pulse_rate_t == (n + 2)/φ_t`, the refusals naming `_pulse_rate_hook` and
   2026.14.0, as the log lists them. The token grep over the commit's files finds nothing.

Corrected here: "this commit" replaced by `bd8d648` in this board's row and change log and in the log's header and §7;
the status line's "01–07c reviewed". The agent's D2 corrects the orchestrator's account of the stop: the failing
assessment was at step 47, N = 3.984, not at the integration's start, and each history's least φ_t falls near N ≈ 5.05,
at the peak of ρ_kin; README §6.16 and this board's §4 entry carry a dated note. `.documents/OPEN_ISSUES.md` needed no
correction (35 open: 07c's closed, dated 2026-11-19).

**For the user.** The suites 07d's restart must expect are 26, 809 (226 + 583), 175, 312, 79. By the agent's scaling
(log §6), the margin of 45 decades at 4e-31 M_P would reach zero on the power wall at n = 1 near M ≈ 4e-76 M_P at
β = 2.9, an extrapolation for a science grid's planning. Choices to look over: `_pulse_rate` refuses a negative rate
rather than taking its absolute value (D5), and `RealizedModel` has no delegate for the new hook (D8).

**Orchestrator review of prompt 07d (2026-11-19).** All seven checks of note 07d §3 pass, with the user's ruling (a) on
the agent's stop at the suites before. The re-runs below were made by the orchestrator at `b5f863a`, from 10:48 GMT, at
one-minute loads of 4–10, from scratch scripts in the session scratchpad (read-only `sqlite3` with `mode=ro`, none
committed), one at a time.
- **The gate at dispatch** (2026-11-19, 07:32–07:36 GMT). `HEAD` was `00fb718`, with `bd8d648` and `00fb718` its
  ancestors; `VERSION_LABEL` 2026.14.0; `?? venv` only; the main checkout at `9df9e13`. The registry held 83 runs, all
  finished; the lock and the four ports free; 07's four stores gave the note's digests by `store fingerprint` without
  `--write`; none of the restart's paths existed. The load was 24.7 at 07:32 and 4.6 at 07:36.
- **The stop.** The agent's `tools/tests` before ran across the machine's sleep (lid closed 07:57:16, full wake 09:04:57
  by `pmset -g log`), 312 tests, and failed `test_outward_step_scan … test_a_task_past_its_limit_is_a_timeout` (a 3 s
  timeout race, `'timeout' != 'done'`) beside the expected `test_roots_and_distributions`. I confirmed both failures and
  the count from its output, and read the test (`tools/tests/test_outward_step_scan.py:442`) before putting it to the
  user, who ruled: "This test is of no importance anyway, and has nothing to do with the science code. Please just do
  (a)." Relayed in those words, with the user's note on the sleep; the test passed in the agent's suites after and in
  mine.
1. *Scope.* `b5f863a`'s parent is `00fb718`. Its 194 files are prompt §7's as the dated block amends it: the
   verification document (+488, −0: §11 appended after line 2528), the log, the board (+33 −3), and `rehearse-07d/` (13
   scripts and 178 small text files under `out/`, 4.0 MB, no PDF or PNG). No code, test, config, `CLAUDE.md`,
   `.gitignore`, `PRyM/` or README change; `rehearse-07/` unchanged; nothing under `var/` tracked;
   `.documents/OPEN_ISSUES.md` untouched. The worktree holds `?? venv` only, `git worktree list` the two trees, the
   branch has no upstream. Against my listings at the Gate, `$MAIN/var` gained only `datastores/power-law-wall-07d-r2-*`
   (four stores, 16 shards and a sidecar each) and `runs/power-law-wall-07d-r2-*` (13). The 125 files of 07's stores
   and the first attempt's are byte-identical to my Gate `shasum`s, and 07's four give the Gate's digests again by my
   own `store fingerprint`.
2. *The counts,* by discovery, mine, from 10:51 GMT at loads of 5.9–9.7: 26, 226 + 583, 175, 312, 79, with the two
   worktree failures (`test_roots_and_distributions`, `test_the_script_is_main_py_with_its_hash_and_this_repositorys_head`)
   and no other.
3. *The cluster, the registry and the tree.*
   - `ray_cluster.sh status` exit 0 and the lock's `status`: free, no process, the four ports free; none of the 13 pids
     recorded under `$L` answers `kill -0`.
   - `runregistrykit list`: 96 runs, all finished; this prompt's 13 are 12 `done` and `r2-pl-resumed-a` `killed`.
   - Every manifest, read by me: `git_head` `00fb718`, `git_dirty` True (the untracked `rehearse-07d/`), script hash
     `2705636433a7…6fb87`, my own SHA-256 of `git show 00fb718:main.py`.
   - The probe as committed: the driver, a task and an actor import this worktree's modules from its root as working
     directory, at 2026.14.0 and format 8, and read `Xav_EOS_data.csv` `690090c8…119d` and `PRyMrates/` (84 files)
     `49b4cea2…9224`, 07's hashes.
   - The four sidecars format 8; `store fingerprint` by me: exp `17b288e7…c14c`, pl and pl-resumed `152f04f7…50be`, mixed
     `477755a5…b8b8`, each matching its recorded fingerprint, `problems: none`.
4. *No ℓ, no change, independently.* My script (sanity-tested first on 07's `pl` against 07's `pl-resumed`: 40 matched,
   no difference, three reasons equal under the mask) matches histories by (model table, M, n, β, α, drain model under
   the map, the three tolerances, φ, π, T, solver, cosmology), and compares by `float.hex` every column of
   `ScalarHistory` (and `extra_data` key by key), `ScalarHistoryBounce` (by ordinal), `ScalarHistoryValue`,
   `AdiabaticHistory`/`Value` and `BBNData`/`Value` (by redshift), serials, labels, timestamps and timing columns aside,
   `failure_reason` under 07's mask; on the power wall the drained-share sum, and any candidate newly `unassessed`.
   - pl and pl-resumed: 40 matched each way, 5 537 980 values each; exp (`fixed` only): 9, 1 330 655; mixed (the power
     wall and the exponential's `fixed`): 29, 3 878 488. No difference outside P114's list in any; 37 and 17 drained-
     share sums bitwise; no candidate newly unassessed; three reasons equal only under the mask in pl, pl-resumed and
     mixed.
   - P114's columns that moved, my counts and largest changes, equal to the agent's in every group: on the power wall's
     one-loop law the bounce `ell` and `birth_margin` 579 (n = 1, 1.2674) and 563 (n = 2, 0.8017), `log_k_ex_Mp`,
     `log_q_Mp`, `tracker_margin` 1.1922 and 0.7552, `log_m_Mp` 0.0751 and 0.0466, `reflection_margin` 568 and 552,
     `reflection_margin_N` 16 and 13; `ell_min` and the `all_worst_*` margins 11 of 11 at each n; the worst margins 34 of
     34 power-wall histories with values (up to 1.1323 / 1.1332 / 1.1022 at n = 1); on the exponential's fixed law the
     three worst margins 6 of 9, up to 0.1898. Nothing else of P114's list moved. My value counts sit a few thousand
     below the agent's (16 291 536 in all), as in 07's review: I skip the key tables, which the agent resolves.
   - The first attempt's five histories (power wall, n = 1, β 2.9, 4e-31 M_P; α 0.01, 0.1; A 0.05, 1, 10) succeed in
     07's and in 07d's `pl`, `pl-resumed` and `mixed`, with 299, 123 and 25 bounce rows under the one-loop law, each
     equal to 07's outside P114's columns.
5. *The tip's identities and the exponential under v2.* `tools/compare_stores.py` on 07d's `pl` and `pl-resumed`, run by
   me: exit 1, `problems: none`, its output equal to the agent's but for the timing lines, the only differences the
   three failure rows and their tags, whose reasons carry the run label (07's ruling (a)); `InversePowerModel` (16) and
   `ScalarHistoryBounce` (2262) matched. Both `store fingerprint`s `152f04f7…`. P115 re-read by me from 07's and 07d's
   `exp` at 4e-31 M_P: A = 0.05, β 2.9 (ΔN_eff today 0.0021126653 → 0.0019868061, Y_p 0.246970 → 0.246889, D/H 2.45971
   → 2.45843, the one-loop share 0.997964 → 0.996342, `ell_min` 18.277 → 18.871, 103 → 116 candidates); A = 1, β 2
   (0.0446240 → 0.04163249276504718, the tool route's value; 45 → 46); A = 10, β 1.2 (0.532988 → 0.505446; 6 → 7):
   each equal to the agent's §11.5 table to its digits.
6. *The readers and the exports.*
   - 07's `export-pl` against 07d's by my own `cmp` under the directory map: the same 120 files, 37 byte-identical. By
     my own CSV diff, the others differ only in the timing columns (`data.csv`, 15 fixed-law and 22 one-loop), P114's
     columns (`bounces.csv`; `histories.csv`'s `ell_min` and `all_worst_*`), the three label-carrying `failure_reasons`,
     and the quanta files (`spectrum_model` v1 → v2 and what the spectrum feeds, measured); every `fields.csv` identical.
   - `tools/quanta_measures.py --assoc-scale 1` re-run by me on a copy of 07d's `export-exp` in the scratchpad, its
     quanta files removed first: exit 0, measured 11, its 23 files byte-identical to the agent's, its summary equal but
     for the wall; every one of 60 982 rows `R8-exponential-v2`.
   - R4's five logs: the three plot refusals exit 2 with `!! plot:` lines naming the option and value, the two `main.py`
     refusals at parse naming the drain law (U14 for the exponential at n = 3); no `refuse-*` output exists, and no
     `power-law-wall-07d-r2-nonexistent*` file was created. R6's four `prod` lines equal the stored Y_p and D/H, which I
     read myself, to the ten digits printed. The 90 store files of the agent's `shasum` record (07d's four stores and
     07's `exp`) are unchanged today.
   - R8 (U12's guard on 07's `exp`): no `refuse-07-exp` output, the store's files unchanged; but the reader exits 1 on an
     uncaught `datastorekit.replication.ReadOnlyMiss` from the `ShardedPool` at `plot_by_beta.py:1118`, naming both labels
     and "Nothing was written", not 2 with a `!! plot:` line. The guard holds by R8's own terms (no export, store
     unchanged), as the agent counted it (D5); the form of the refusal is opened as
     `[07d-a-reader-refuses-an-older-label-by-a-traceback]` (§3), cosmetic.
   - `plot-exp`'s `M=0.00097413eV_n=1/Delta_N_eff.png`, looked at: both panels show α 0.01, 0.1 and A 0.05, 1, 10
     (`one-loop-exponential-v2`) over β 1.2, 2, 2.9, A = 10 falling 0.505 → 0.145 → 0.062 and A = 1 0.053 → 0.042 →
     0.032, as §11.5; the legend covers A = 10's point at β 2.9, as in 07.
7. *The records.*
   - The log has §5.1's seven parts. It records rulings 1–9 of the note with their classes and D1–D11, D1 the stop with
     the user's words; three UNINTENDED DRIFT, all of the agent's own scripts or records (the first attempt's
     `reflections` key, a `domain.py` overflow fixed before use, R1's empty start times, doubled file prefixes). Its §7
     gives 08 the stores, the exports, the gate, what moved and the costs.
   - The verification document loses no line; §11 is appended, its commands verbatim in §11.12.
   - The board has row 07d, the gate R2 in §2, the baselines and the history line; the token grep over the commit's
     files finds nothing; `black --check` passes on its 11 Python files.

Corrected here: "this commit" replaced by `b5f863a` in the board's row and change log and in the log's header; the
status line's "01–07c2 reviewed" and its issue count. Opened here: `[07d-a-reader-refuses-an-older-label-by-a-traceback]`
(§3), with `.documents/OPEN_ISSUES.md` (36 open). Note 07d's header is marked used for the restart.

**For the user.** 07d's four stores are the ones 08 reads (P124). 07's four and the first attempt's two
(`power-law-wall-07d-exp`, complete; `power-law-wall-07d-pl`, runs a and b, no sidecar) stay in `$MAIN/var/datastores`
for the user to retire. On the exponential, the history at 10⁻³³ M_P has 47 candidates with ln(V₀/M⁴) > 10 (up to 27.3),
outside v2's fitted domain; every other one-loop candidate of the grid is at or below 3.345. The least φ_t over the
grid's candidates sits 74.9 (n = 1) and 97.7 (n = 2) decades above where V″ leaves float range on the power wall.

## 2. The gates

As README §2.1: Model (01), Wall (02), Law (03), Sample (04), Store (05), Export (06), R (07), Remeasure (07b), Form (07b2), Consistent (07c), Range (07c2, added 2026-11-19), R2 (07d), Close (08). The last is
not yet met.

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

- **Law — holds** (prompt 03, 2026-11-15; log 03 §4, `.documents/power-law-wall-verification.md` §3): the fit f_n(ln R)
  reproduces note 03 §0's (worst 0.0541 | 0.0557, mapped 0.0042 | 0.0030; n = 1 | 2); `InversePowerModel`'s fraction
  equals `fit_fn.fraction_power` bit for bit at 122019 synthetic inputs per (n, units) under two `UnitsLike`, and
  production's α_eff at A = 1 equals min(0.99, f) bit for bit at all 1148 candidates of 02's power-law maps; the
  exponential's goldens, tool tests and fingerprint goldens pass unedited, and its tool route is identical to the base's;
  the power-law tool route runs under `one-loop-measured-pl-v1` at n = 1, 2; the spectrum is R8's three regions on every
  wall (tables to k/ω_t = 10⁻³, x_hi 4.0 | 6.5 | 5.5, the tail ≤ 7.4e-4 of the energy), every kernel within 5e-10 of its
  quadrature reference.

- **Sample — holds** (prompt 04, 2026-11-15; log 04 §4, `.documents/power-law-wall-verification.md` §4): U7's forty by
  the tool route, tuned, beside the exponential's 2026.11.0 counterparts; 37 succeed, the three failures R5's (n = 1,
  natural mode, 10⁻³ M_P, G2); the runner, the fourteen overlaps and the exponential's tool route against its export all
  identical with timings masked; every α > 0 candidate (1632) an unresolved one-loop reflection, every history parked at
  its last candidate, none released; R9's share 0 (ln(E/V₀) ≥ 11.46); the unresolved reflection, the parking cut and the
  wall's rounding never near their edges; the first impact one-loop at every M ≤ M_P; the surfing edges at 10⁻³ M_P:
  n = 2 the exponential's (QCD onset (1.116, 1.117], last QCD 2.975, EW from (3.002, 3.003]); n = 1 has no EW edge (74 of 79 coarse rows fail). The options on the surfing thresholds at n = 1
  are for the user (verification §4.9); none chosen.

- **Store — holds** (prompt 05, 2026-11-16; log 05 §4):
  - **The table.** `InversePowerModel` is registered after `ExponentialModel` (batch 50, sharded by β, no drop group),
    with the exponential's spec. `ScalarHistory`'s `model` parent maps type id 2 to it.
  - **The round trips.** A power-law history at laboratory M, β = 2 and n = 1 round-trips in a temporary store under
    the fixed law (α = 0.1) and the one-loop law (A = 1, `one-loop-measured-pl-v1`). Every column, sample and bounce
    row is equal, and the two models' histories are never confused, though their model rows share serial 1.
  - **The entry check.** The key's drain model is checked when a `ScalarHistory` is constructed. Its one refusal of an
    existing test was ruled (a) by the user.
  - **The store tools.** The inventory, `compare_stores`, `inventory_report`, `outward_step_scan` and `bbn_from_store`
    read a mixed store, and every raw join of a model table matches `model_type`.
  - **The fingerprint.** `FINGERPRINT_FORMAT` is 8, with 17 pairs and 14 distinct digests (only `ScalarHistory`'s
    moved). The golden at 8 differs from format 7's only in the format, the empty `InversePowerModel` class and the top
    digest. `VERSION_LABEL` is 2026.12.0.
  - **The exponential.** Its goldens pass unedited, and its tool route is identical to 04's file with timings masked.

- **Export — holds** (prompt 06, 2026-11-16; log 06 §4, `.documents/power-law-wall-verification.md` §5): on a store of
  both potentials built through 05's harness (6 tuned histories and 2 failures at laboratory M, β = 2 and 1.185), the
  base's exponential exports at `<label>/` and the tip's at `<label>/exponential/` are the same 6 paths and equal byte for
  byte, `histories.csv` once its `model` column (after `n`, `exponential` throughout) is removed; the store's SHA-256
  equal before and after each export; the tip's power-law root has `model` after `n`, its 5 histories and 1 marker row,
  218 cells and facts equal to the store; `quanta_measures` measures the power law with each row's wall
  (`R8-power-n1-v1`, `R8-power-n2-v1`), the exponential root's quanta files, PNGs and masked printed lines equal the
  base's, and a root without `model` is refused (exit 2); `main.py`'s lookups, pipelines and printed lines for an
  exponential-only run equal the base's in five commands.

- **R — holds** (prompt 07, 2026-11-16; log 07 §4, `.documents/power-law-wall-verification.md` §6), with the user's ruling
  (a) on the stop at §2.2 (the run label in `ScalarHistory.failure_reason` a run-dependent fragment):
  - **(i) the exponential bitwise:** the base store (`a7e0978`, format 7) and the tip's exp store, twenty histories each
    from the same command, matched once each way; 2 984 336 values bitwise equal; the fingerprints differ only in
    `version` and the three classes that name it as a parent, and `InversePowerModel` is empty at the tip;
  - **(ii) the power law resumed:** `pl` and `pl-resumed` (run a interrupted by `SIGTERM` in the A = 0.05 scalar stage,
    three unvalidated rows pruned and recomputed); the same fingerprint `9d4fcf02…` at format 8; `compare_stores` differs
    only in the three failure rows' reasons, equal under the ruled mask; 5 538 777 values equal; the exports and the quanta
    outputs equal but named timing columns and the masked reasons;
  - **(iii) against 04:** all forty of each power-law store, and exp's three plain-tool histories, equal to 04's tool route:
    the tuning bit for bit, every printed value at its digits, every candidate row, Y_p and D/H at ten digits (11 635 values
    per store); the 2026.11.0 counterparts equal to exp's export in all 61 shared columns (a measurement);
  - **(iv) two potentials in one store:** mixed's exponential histories equal exp's (2 984 336 values) and its power-law
    histories pl's at n = 1 (2 548 230); its exports equal exp's and pl's n = 1 files, timing columns named;
  - **(v) the readers:** five inventories `problems: none`; the exports of every store, the base's equal to exp's by 06's
    rule; the power law's figures drawn under Ray; three refusals, exit 2, nothing written; the quanta tools with each
    potential's spectrum, `plot_quanta_grid` refusing two n at one cell (§3, measured); four BBN re-solves equal to the
    stored values; no reader changed a store's `shasum`.

- **Remeasure — holds** (prompt 07b, 2026-11-17; log 07b §4, `.documents/power-law-wall-verification.md` §7), nothing
  ruled by the agent:
  - **the definition:** Eq. (64) with V_D ∝ (V″)² equals ½E^{1/2}|V‴/V″| to 4.0e-9 at twelve chain points, the factor
    (1 − 1/4ℓ)^{1/2} it leaves out measured to 3.4e-9; Eq. (71) to 4.4e-16 on the power wall; on the exponential new/old
    equals its closed form to 2.2e-16 and Eq. (66) is its leading term exactly; φ_t, ln ω_t² and ln R bitwise in both
    forms at nine candidates; three broken forms each caught by all three checks;
  - **the old chain reproduces every recorded ℓ:** bitwise equal to the copied born.chain at 371 rows, within the printed
    digits at all 199 recorded ℓ, and crossover's fit constants to their printed digits; the new chain within 1.9e-11 of
    the production hooks at 40 candidates;
  - **every stage-2 point re-chained** (371; 33 exponential and 52 power-wall rows move, all gap → one-loop) and fifteen new
    points (eleven at n = 1, four at n = 2), each with its energy (≤ 6.3e-10), three of them validated (classical 2.7e-6,
    Born 8.4e-4 off-curve at ℓ_new = 6, convergence 9.2e-4); the grid's reference within ±6e-3 at all three points;
  - **the candidate laws** (i)–(iv) stated at n = 1 and 2 beside v1's residuals; the power wall's law equal under both
    forms at 201 rows and 03's gate re-run identical;
  - **the three tables and x_*** in the new x (k_ex/ω_t 0.5163952511757223, 1.0606601717798214, 0.816496580927726;
    x_* = 0.005616), equal to the old in k/ω_t within the old spline's precision; R9's error on the power wall gone
    (≤ 3.4e-7);
  - **the inventory:** 278 rows, 20 beyond the planner's list.

- **Form — holds** (prompt 07b2, 2026-11-18; log 07b2 §4, `.documents/power-law-wall-verification.md` §8), nothing
  ruled by the agent:
  - **the form and its variants fitted** at n = 1 and 2 on S1–S3, (v), (v-L), (v-A), (v-J) and (vi), each from four
    starts, with standard errors and leave-one-set-out; on S3 with the new points (v) A 2.497 ± 0.074 | 2.528 ± 0.076,
    ℓ₀ 1.194 ± 0.063 | 1.183 ± 0.072, (v-L)'s C 0.021 ± 0.032 | 0.008 ± 0.039;
  - **like for like:** every law on each selection's identical points, beside v1 (old chain) and 07b's (i)–(iv) at their
    committed constants (S3: v1 0.098 | 0.125, (i) 0.350 | 0.355, (iii)/(iv) 0.070–0.107 | 0.071, (v) 0.074 | 0.064), and
    on each selection's dropped points; v1 read in the new chain is caught (0.743 | 0.638);
  - **07b's constants reproduced:** (iii) and (iv) at n = 1 (both selections) and n = 2, every constant at `fits.txt`'s
    digits; README §6.13's (v) to its printed digits;
  - **the new points:** 36, predicted in `prediction.json` (01:07:44) before the first ran (01:07:49), to max 0.071, rms
    0.024 (the fall points 0.025 | 0.023; A × 1.1 caught); energy ≤ 3.4e-9 at all; three validated (classical ≤ 1.1e-5,
    Born ≤ 8.4e-4 off the curve, convergence ≤ 7.0e-4);
  - **the paths re-mapped:** the nine exponential histories, K_X/K_v1 0.93–1.01 under (v), 07b's shares recomputed equal;
  - **the reference implementation** `minform.fraction` equal to all 3595 fitted values in `minform.json` bit for bit.

- **Consistent — holds** (prompt 07c, 2026-11-18; log 07c §4, `.documents/power-law-wall-verification.md` §9):
  - **one k_ex at every use of 07b's inventory:** every row changed, re-pinned, renamed or shown to hold; the `git grep`
    finds only the retired names' refusals, dated comments and the verbatim copy of the old method; the production chain
    within 2.6e-11 of 07b's `production_v3_*` at its 40 candidates, φ_t, ln ω_t² and ln R bitwise (R16);
  - **the V‴ hooks:** equal to their closed forms (202 points per check, worst 0.0026 of the tolerance) and to the
    centred difference of V″ (5.9e-10) on both models, from 1e-90 M to 10 M_P where (ln V)‴ is within float range;
  - **the law as ruled:** `one-loop-exponential-v2` equal to `minform.fraction` bit for bit at the 146 fitted points and
    28 synthetic inputs covering every branch;
  - **the spectrum:** the kernels within 2.2e-10 of quadrature on the new tables (270 evaluations); `G_Y_MIN` 5e-4 and
    `OMEGA_U_MAX` 9 on their docstrings' criteria;
  - **the tool route:** histories 1–4 (natural mode, the fixed law, the power wall at n = 1, 2 under the one-loop law)
    bitwise in every sample, tuning and BBN value, only the k_ex columns moving; history 5 measured;
  - **the cap:** all 333 stage-2 points with a chain converge within 400, the largest 195;
  - **the suites:** no count fell.

- **Range — holds** (prompt 07c2, 2026-11-19; log 07c2 §3, `.documents/power-law-wall-verification.md` §10):
  - **the pulse rate a model hook in closed form on both walls:** `_pulse_rate_hook`, (n + 2)/φ on `InversePowerModel`,
    [(n + 1)(n + 2) + 3n(n + 1)u + n²u²]/([(n + 1) + nu]φ) on `ExponentialModel`; `AbstractModel`'s raises naming the class;
  - **equal to the composition of each model's hooks where that is finite:** 4071 points from 1e-102 to 10 M_P at n = 1, 2,
    four M, worst 2.85 eps (tolerance 32 eps); **finite down to where V″ leaves float range:** (n + 2)/φ bit for bit down to
    1e-300 M_P (the end 1.05e-154 M_P at n = 1, 1.83e-154 at n = 2);
  - **the stop's inputs return a turning point:** φ_t, ln ω_t² and ln R bit for bit those of 2026.13.0's code path;
  - **the tool route:** histories 1, 2 and 6 (the fixed law, the power wall at n = 2 and 1 under the one-loop law) bitwise
    in every sample, tuning and BBN value, only `pulse_rate_t` moving (one ulp); the stop's histories 4 and 5 built, equal
    to 07's stored ΔN_eff, Y_p, D/H, reflections and bounce rows;
  - **the least margin of 07's grid's assessments:** 45.0 decades (least φ_t 1.07e-109 M_P at n = 1, β = 2.9,
    M = 4e-31 M_P, near N = 5.04);
  - **the suites:** no count fell.

- **R2 — holds** (prompt 07d, restarted 2026-11-19; log 07d §4, `.documents/power-law-wall-verification.md` §11):
  - **no ℓ, no change (P114):** 07's store against the restart's by `compare_histories.py --across-k-ex`: exp 9, pl 40,
    pl-resumed 40, mixed 29 histories matched each way, 16 291 536 values, every one outside P114's list equal by
    `float.hex`; the power wall's drained-share sums and `delta_K_Mp4` bitwise; no candidate 07 assessed unassessed; the
    k_ex columns that moved listed (the worst margins on every law, by up to 0.190 on the exponential's fixed law and
    1.13 on the power wall; on the power wall's one-loop law ℓ, `ell_min`, ln k_ex, ln q and the margins by up to 1.267
    at n = 1 and 0.802 at n = 2, ln m by 0.075); the first attempt's five histories built, equal but the k_ex columns;
  - **the exponential's one-loop histories, measured (P115):** eleven in exp and in mixed under v2 against 07's v1;
    ΔN_eff today falls in 10 by 0.36–6.70%;
  - **the tip's own identities (P116):** pl-resumed equal to pl (one fingerprint `152f04f7…`, 5 538 777 values; the run
    label in three failure reasons, as 07's ruling (a)); mixed equal to exp and to pl at n = 1; their exports;
  - **the exports against 07's (P118):** five roots, no difference outside the timing, masked-label, mapped and P114
    columns; the v2 files and the quanta files measured;
  - **the readers (P117):** four inventories `problems: none`; five refusals exit 2, writing nothing, U14's at parse;
    `R8-<wall>-v2` by potential and n; the BBN re-solves equal at ten digits; U12's guard on 07's exp store writes nothing
    (exit 1, a `ReadOnlyMiss` traceback); no store's `shasum` changed.

## 3. Active and unresolved issues

- **`[06-plot-quanta-grid-refuses-two-n-at-one-cell]`** (opened by prompt 06, 2026-11-16; log 06 §6). `tools/plot_quanta_grid.py`
  draws one figure per (drain model, A) of a root and refuses a figure with two rows in one (β, M) cell. On a power-law
  root the science run's grid will have n = 1 and n = 2 at every (β, M) under `one-loop-measured-pl-v1`, so every grid
  figure of that root would be refused (the exponential's science roots hold n = 1 only, so this never arose).
  - *Impact:* the standard analysis "Relativistic quanta"'s grid figures for the power law; `quanta_measures` and
    `plot_quanta_history` are not affected (each history has its own directory, by n).
  - *Next step:* group the grid by (drain model, A, n), or draw n as panels, in a prompt of its own (08's handover may
    assign it); the figure's paths and the exponential's figures then need a ruling on whether they move.

- **`[07-failure-reason-carries-the-run-label]`** (opened by prompt 07, 2026-11-16, from its stop at the gate R §2.2, ruled
  (a) by the user; log 07 §2 D1, `.documents/power-law-wall-verification.md` §6.5).
  - *What:* a stored `ScalarHistory.failure_reason` begins `integrate_scalar_history (<main.py's history label>): …`, the
    label holding the job name and the submission time, e.g. `power-law-wall-07-pl-b-ScalarHistory-InversePowerModel(M=0.001Mp,n=1,beta=1.2)-2026-11-16T19:26:30`
    in `pl` against `power-law-wall-07-pl-resumed-b-…-2026-11-16T19:37:37` in `pl-resumed`.
    - `tools/compare_stores.py` compares the column as content: its `MASKED_FRAGMENTS` masks a fragment of
      `BBNData.failure_reason` only. Two builds of the same failing history therefore never match: on `pl` and
      `pl-resumed` it reports `ScalarHistory` 37 matched, 3 and 3 on one side only, and `ScalarHistory_tags` the same, and
      exits 1 though every other value is equal and the fingerprints are equal.
    - Production cuts the reason to `DEFAULT_STRING_LENGTH` = 256 characters with the label inside it
      (`ComputeTargets/ScalarHistory.py`, ScalarHistory's construction; the column is `String(256)`). So how much of the
      reason survives depends on the job name's length. Measured on the three n = 1 natural-mode failures at 10⁻³ M_P:
      - the tool route's reason is 39 characters longer than `pl`'s stored one;
      - `pl-resumed`'s stored reason keeps 8 characters fewer than `pl`'s (157 of 165 masked characters), e.g. `…(W = 6.4355e`
        against `…(W = 6.4355e-05, pi^`;
      - `mixed` keeps 3 fewer than `pl` (162 of 165).
  - *Impact:* `compare_stores` cannot be the whole check of two builds that hold a failure (07 read its result through
    the ruled mask); a long job name loses the end of a failure's numbers from the store. No physics value is affected.
  - *Next step:* for a later prompt: a `MASKED_FRAGMENTS` entry for `ScalarHistory.failure_reason` in `compare_stores.py`,
    and a ruling on whether the stored reason should leave the label out, or be cut after it, so that its numbers survive
    whatever the job name.

- **`[07-a-refused-plot-leaves-ray-errors-unretrieved]`** (opened by the orchestrator's review of prompt 07, 2026-11-16;
  this board, "Orchestrator review of prompt 07", check 6).
  - *What:* when a plot script is refused for an option the store does not hold, and the option names several values
    (R4: `plot_by_beta.py --models inverse-power --A-values 0.05 1 10` on `exp`), the refusal names the first and the
    script exits 2, but its log ends with a Ray "Unhandled error" traceback for each other missed value (`ReadOnlyMiss`
    for A = 1 and 10). `plot_inputs._get_parameter` reads the lookups one by one and raises at the first miss; the
    remaining refs' errors are never retrieved, and Ray prints them as the driver exits.
  - *Impact:* cosmetic. Nothing is written and the refusal is right, but a refusal log reads like a crash, and only the
    first missing value is named.
  - *Next step:* for a later prompt: gather the lookups' outcomes before refusing (and name every missing value), or
    retrieve the remaining refs before raising.

- **`[07d-a-reader-refuses-an-older-label-by-a-traceback]`** (opened by the orchestrator's review of prompt 07d,
  2026-11-19, from the agent's observation, log 07d §6 and D5; this board, "Orchestrator review of prompt 07d", check 6).
  - *What:* the tip's `plot_by_beta.py --no-plots` on 07's 2026.12.0 `exp` store (R8, U12's guard) exits 1 with an
    uncaught `datastorekit.replication.ReadOnlyMiss` traceback from the `ShardedPool` opened at `plot_by_beta.py:1118`
    (a lookup of `version` matched no row; the message names the store's 2026.12.0 and the tip's 2026.14.0 and says
    nothing was written), not with a `!! plot:` line and exit 2 as its other refusals do. `plot_ScalarHistory.py` opens
    its pool the same way (`:1594`) and was not run on an older store.
  - *Impact:* cosmetic. The guard holds: nothing is written, the store's files are unchanged, and the message names both
    labels. But a refusal of a store of another label reads like a crash, and its exit status is not the plot scripts'
    refusal status.
  - *Next step:* for a later prompt: catch `ReadOnlyMiss` on the `version` lookup when the plot scripts open a store
    read-only, and refuse with a `!! plot:` line naming both labels and exit 2; a test on a temporary store of another
    label. The package raises the miss as it should, so the fix is ChamPBH's, not DatastoreKit's.

### 3.1 Assigned to this campaign

None.

## 4. Resolved issues

- **`[07c-the-third-hook-leaves-float-range-below-phi-1e-103-mp]`** (opened by prompt 07c, 2026-11-18; log 07c §6,
  `.documents/power-law-wall-verification.md` §9.8).
  - *What:* k_ex's pulse rate reads (ln V)‴, which near the wall is about n(n + 1)(n + 2)/φ³ and leaves float range below
    φ ≈ 1e-103 in the units' field (M_P = 1 in Planck units); (ln V)″ ~ 1/φ², all the turning points needed before 07c,
    reached 1e-154. There the hook raises `ComputationFailureError` naming itself, and so do `classical_turning_point` and
    `one_loop_turnaround`, where before 07c they returned. `test_inverse_power_model` (d)'s points at n = 1, ln E = −60,
    M = 4e-31 and 1e-16 M_P (φ_t = 1.3e-126 and 1.3e-112 M_P) now assert the raise.
  - *Impact:* none on any history of record, by estimate: on the power wall at n = 1, φ_t ≈ M V₀/E with E/V₀ ≲ 1e64 from
    T* = 2e4 GeV, so a first impact reaches the limit only for M ≲ 1e-39 M_P; 07's grid's least M is 1e-33 M_P (φ_t ≈
    1e-97 M_P, |(ln V)‴| about 1e17 below the largest float); at n = 2 and on the exponential (φ_t = M L^{−1/n}) nowhere
    near. A grid at smaller M on the power wall at n = 1 would see assessments fail.
  - *Next step:* a ruling on whether the pulse rate should be formed from scaled hooks (φ^k (ln V)^(k), or ln|V‴|), which
    the hook's interface as P102 states it does not allow; 07d reports the least φ_t of its grid.
  - **Narrowed 2026-11-19: the impact statement above is wrong; the issue reached 07's grid** (README §6.16). 07d's first
    attempt, run `pl-a`: the step-start assessment (`ComputeTargets/ScalarHistory.py:3104`) at the start of the
    integration, where E/V₀ ≈ e¹⁶⁶ ≈ 10⁷², puts φ_t at 2.31 × 10⁻¹⁰³ M_P at n = 1, β = 2.9, M = 4 × 10⁻³¹ M_P, and the
    pulse rate's composition overflows there (3(ln V)′(ln V)″ ≈ −2.4 × 10³⁰⁸, before (ln V)‴ itself). Five histories 07
    built (α 0.01, 0.1; A 0.05, 1, 10) fail at the tip; `pl-resumed` and `mixed` would fail alike. The estimate had taken
    first impacts only. *Impact:* 07d cannot pass its gate R2, and a science run at n = 1 would lose these histories.
    *Next step:* the user ruled route (a), "Try (a)": prompt 07c2 forms the rate in closed form ((n + 2)/φ on the power
    wall) and closes this issue if its measurement of the range allows (P131); 07d restarts after it.
  - **Closed by prompt 07c2, 2026-11-19** (log 07c2 §3, `.documents/power-law-wall-verification.md` §10), as P131 rules:
    the pulse rate is each model's `_pulse_rate_hook` in closed form ((n + 2)/φ on the power wall), with no third
    derivative, finite and exact wherever V″ is; the turning points' range again ends where V″/V = (ln V)″ + (ln V)′²
    leaves float range, at 1.05e-154 M_P (n = 1) and 1.83e-154 M_P (n = 2), as before 07c. The stop's inputs return a
    turning point and histories 4 and 5 of the tool route (07d's `pl-a` histories) build, equal to 07's stored values.
    Measured margin: the least φ_t of any assessment over 07's grid's 60 power-wall histories is 1.07e-109 M_P (n = 1,
    β = 2.9, M = 4e-31 M_P, near N = 5.04, at the peak of ρ_kin, not at the first assessment), **45.0 decades** above the
    end. `_d3_logV_dphi3` still leaves float range below 2.2e-103 M_P at n = 1, but no production code reads it.
  - *Corrected 2026-11-19 by 07c2's measurement* (log 07c2 D2, verification §10): the failing assessment was at
    step 47, N = 3.984, not at the start of the integration; each history's least φ_t falls near N ≈ 5.05, at the peak
    of ρ_kin.

- **`[07b-the-one-loop-fixed-point-needs-more-than-100-iterations-near-the-chain-end]`** (opened by prompt 07b, 2026-11-17;
  log 07b §6, `.documents/power-law-wall-verification.md` §7.3, §7.4).
  - *What:* near the fold where the one-loop chain stops having a solution, its fixed point (ℓ → m_ta → φ_ta → k_ex → ℓ)
    converges slowly. born.chain (and 07b's chain) allows 200 iterations; production's `one_loop_turnaround` allows
    `ONE_LOOP_MAX_ITERATIONS` = 100 and raises past it, which `drain_regime` reads as gap. Measured: in the new form 195
    iterations at 07b's laboratory point ℓ_new = 0.120 (L = 1.378) and 107 on L-M8e-18 at ℓ_new = 0.305; in the old form
    138 at 02's curve-1e-15-n1 row at ℓ = 0.100. The rest of the 371 re-chained rows and fifteen new points need ≤ 83.
  - *Impact:* a candidate just inside the chain's end is classified gap in production where the measurements call it
    one-loop; under the new form the fold sits where the exponential's late laboratory cascade is (ℓ_new ≈ 0.12–0.3).
    The fraction there is continuous across the fold only if the law is (candidate (iv) of §7.5 is; v1 and (i)–(iii) are
    not).
  - *Next step:* 07c's to rule with U13: raise the cap, accelerate the fixed point, or accept it as measured.
  - **Closed by prompt 07c, 2026-11-18** (log 07c §4, `.documents/power-law-wall-verification.md` §9.2), as P103 ruled:
    `ONE_LOOP_MAX_ITERATIONS` is 400, and production's `one_loop_turnaround` converges within it at every stage-2 point of
    07b and 07b2 whose new chain has a solution (333 of 422 rows), the largest count 195 (07b's laboratory point,
    ℓ = 0.1200), then 107 (L-M8e-18). `test_k_ex` (c) pins the slowest point.

## 5. Baselines

The suites at `84a2494` (`CLAUDE.md`, counts of 2026-11-13): CosmologyModels 26, ComputeTargets 684 (219 + 465),
ObjectFactories 163, tools 292, RunDrivers 76. 01 re-measures them at its base.

- **01, 2026-11-14, in the worktree:** at the base `75ac6fb` 26, 684 (219 + 465), 163, 292, 76; after 01 26, 705
  (219 + 486), 163, 302, 76. In both, the two worktree failures of orchestrator note 01 ruling 7 (one in RunDrivers, one
  in tools) and no other.
- **02, 2026-11-14 to 15, in the worktree:** at the base `35f6359` 26, 705 (219 + 486), 163, 302, 76; after 02 the
  same. In both, the same two worktree failures and no other.
- **03, 2026-11-15, in the worktree:** at the base `7c60226` 26, 705 (219 + 486), 163, 302, 76; after 03 26, 732
  (219 + 513), 163, 302, 76. In both, the same two worktree failures and no other.
- **04, 2026-11-15, in the worktree:** at the base `7d7c578` 26, 732 (219 + 513), 163, 302, 76, with the same two
  worktree failures and no other; after 04 the same counts by discovery (no file the suites import changed).
- **05, 2026-11-16, in the worktree:**
  - At the base `fb8bef2`: 26, 732 (219 + 513), 163, 302, 76.
  - After 05: 26, 739 (226 + 513), 175, 307, 76.
  - In both, the same two worktree failures and no other.
- **06, 2026-11-16, in the worktree:**
  - At the base `b1433f4`: 26, 739 (226 + 513), 175, 307, 76.
  - After 06: 26, 783 (226 + 557), 175, 312, 79.
  - In both, the same two worktree failures and no other.

- **07, 2026-11-16, in the worktree:**
  - At the base `945bf5d`: 26, 783 (226 + 557), 175, 312, 79, in full.
  - After 07: the same counts by discovery (P70; no file the suites import changed).
  - The same two worktree failures and no other.

- **07b, 2026-11-17, in the worktree:**
  - At the base `e4ae58b`: 26, 783 (226 + 557), 175, 312, 79, in full (68 s, 148 s, 553 s, 158 s, 344 s, 26 s at one-minute
    loads of 11.8, 6.7, 4.2, 8.4, 7.0 and 6.4 at their starts).
  - After 07b: the same counts by discovery (no file the suites import changed).
  - The same two worktree failures and no other.
- **07b2, 2026-11-18, in the worktree:**
  - At the base `dec7793`: 26, 783 (226 + 557), 175, 312, 79, in full (69 s, 152 s, 550 s, 158 s, 322 s, 27 s at one-minute
    loads of 13.6, 9.1, 6.2, 5.3, 23.1 and 19.4 at their starts).
  - After 07b2: the same counts by discovery (no file the suites import changed).
  - The same two worktree failures and no other.
- **07c, 2026-11-18, in the worktree:**
  - At the base `c4258ef`: 26, 783 (226 + 557), 175, 312, 79, in full (69 s, 154 s, 561 s, 160 s, 30 s, 318 s at one-minute
    loads of 65.5, 41.9, 37.1, 38.3, 28.9 and 52.1 at their starts; CosmologyModels, the two ComputeTargets halves,
    ObjectFactories, RunDrivers, tools).
  - After 07c: 26, 805 (226 + 579), 175, 312, 79, in full (67 s, 152 s, 556 s, 158 s, 26 s, 320 s at one-minute loads of 5.3, 5.3, 12.0, 4.6, 4.6 and 5.5 at their starts).
  - The same two worktree failures and no other.
- **07c2, 2026-11-19, in the worktree:**
  - At the base `7a202b7`: 26, 805 (226 + 579), 175, 312, 79, in full (67 s, 152 s, 541 s, 156 s, 25 s, 314 s at one-minute
    loads of 5.4, 4.5, 4.5, 4.1, 3.4 and 3.3 at their starts; CosmologyModels, the two ComputeTargets halves,
    ObjectFactories, RunDrivers, tools).
  - After 07c2: 26, 809 (226 + 583), 175, 312, 79, in full (66 s, 149 s, 534 s, 154 s, 25 s, 311 s at one-minute loads of 3.1,
    3.2, 3.9, 2.7, 3.3 and 4.0 at their starts); the +4 are `test_k_ex` (d).
  - The same two worktree failures and no other.
- **07d (restart), 2026-11-19, in the worktree:**
  - At `00fb718`: 26, 809 (226 + 583), 175, 312, 79, in full (63 s, 146 s, 532 s, 155 s, 434 s, 25 s at one-minute loads
    of 7.2, 6.4, 3.4, 3.0, 8.7 and 6.5 at their starts; CosmologyModels, the two ComputeTargets halves, ObjectFactories,
    tools, RunDrivers). `tools/tests` ran across the machine's sleep (wall 4367 s) and failed, beside the two worktree
    failures, `test_outward_step_scan` … `test_a_task_past_its_limit_is_a_timeout`; ruled (a) by the user (log 07d D1).
  - After 07d: the same counts by discovery (P123; 63 s, 152 s, 546 s, 155 s, 315 s, 24 s at one-minute loads of 17.8,
    13.5, 11.8, 5.3, 7.5 and 6.6 at their starts), with the two worktree failures and no other.

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
- 2026-11-15: R9 (U8 (a): one table per wall in x, the late cascade's error measured only) and R10 (U9 as proposed)
  ruled by the user (README §6.6). 03 needs no amendment and is not dispatched.
- 2026-11-15: prompt 03 dispatched at `7c60226`, and landed (`d924275`); the gate Law holds. No issue opened.
- 2026-11-15: prompt 03 reviewed by the orchestrator (this board, "Orchestrator review of prompt 03"); all six checks
  pass. D12 (six tests outside the quanta modules re-pinned) goes to the user beside the hand-back; 04 is not written.
- 2026-11-15: the user on D12, in their words: "No need to argue about D12. Leave it as it is." The six re-pinned tests
  stand (README §6.7).
- 2026-11-15: prompt 04 and its orchestrator note written, at the user's request ("Please write prompt 04 and its
  orchestrator note"), with the planner's P24–P32 in README §6.7. No question waits on the user; not dispatched.
- 2026-11-15: P24–P32 ruled by the user as proposed (README §6.7): "P24–P32 as proposed; don't yet dispatch prompt 04,
  though: I'll do that in a fresh session." The brief for that session is [`orchestrator/session-04.md`](orchestrator/session-04.md).
- 2026-11-15: prompt 04 dispatched at `7d7c578`, and landed (`05f6920`); the gate Sample holds. No issue opened. The
  surfing thresholds at n = 1 (no EW edge at 10⁻³ M_P; the onset only in (1.10, 1.30]) go to the user with options
  (verification §4.9).
- 2026-11-16: prompt 04 reviewed by the orchestrator (this board, "Orchestrator review of prompt 04"); all six checks
  pass. The surfing thresholds at n = 1 (verification §4.9) go to the user before 05; 05 is not written.
- 2026-11-16: R11 ruled by the user on the surfing thresholds at n = 1, in their words: "I'm not going to chase this
  now. Note it and move on." Option (a) of verification §4.9: recorded as measured, no further scan, no issue (README
  §6.8). The user asked for 05 and its orchestrator note.
- 2026-11-16: prompt 05 and its orchestrator note written, at the user's request ("Please write 05 and its
  orchestration note"), with the planner's P33–P44 in README §6.9, from a scratch probe of the fingerprint, the
  inventory and a format-7 store under the patched registry (note 05 §0). No question waits on the user; not
  dispatched.
- 2026-11-16: prompt 05 dispatched at `fb8bef2`.
  - It stopped at ruling 4: the new check refused `test_drain_store_records.test_compute_passes_the_keys_law`, whose
    `mock.Mock()` model names no one-loop drain model.
  - The user ruled, in their words, "(a) here please": the test's stand-in model is given
    `one_loop_drain_model="one-loop-measured-v1"`, and the check is unchanged.
  - 05 then landed (`bdb6ae7`), and the gate Store holds. No issue opened.
- 2026-11-16: prompt 05 reviewed by the orchestrator (this board, "Orchestrator review of prompt 05"); all six checks
  pass. 06 is written only when the user asks for it.
- 2026-11-16: prompt 06 and its orchestrator note written, at the user's request ("Please write 06 and its
  orchestration note"), with the planner's P45–P58 and U10 in README §6.10, from a survey of the drivers, the exports
  and the 2026.11.0 analyses' readers, and a scratch probe of the models' drain-model hooks (note 06 §0). U10 waits on
  the user; not dispatched.
- 2026-11-16: R12 ruled by the user on U10, in their words: "We don't want to start privileging one kind of
  potential over another. We want to treat all potentials symmetrically. Having a dedicated root is the wrong choice
  unless all potentials get it." and "Yes, re-plan 06 on the symmetric design". 06, its note and P45–P58 re-planned
  on it (README §6.10); P11, README §5 and the gate Export amended for the exports. Not dispatched.
- 2026-11-16: prompt 06 dispatched at `b1433f4`, and landed (`fda2df2`); the gate Export holds. One issue opened (§3,
  `[06-plot-quanta-grid-refuses-two-n-at-one-cell]`). One unintended Ray start, a private local instance in the first base
  export's process, which ended with it (log 06 D4, UNINTENDED DRIFT); its session directory is left under `/tmp/ray`.
- 2026-11-16: prompt 06 reviewed by the orchestrator (this board, "Orchestrator review of prompt 06"); all six checks
  pass. 07 is written only when the user asks for it.
- 2026-11-16: prompt 07 and its orchestrator note written, at the user's request ("Please write 07 and its orchestration
  note"), with the planner's P59–P72 in README §6.11 (the gate R made exact by P64–P68), from a survey of the registry,
  the base `a7e0978`, 04's outputs and the rehearsal tools (note 07 §0). No question waits on the user; not dispatched.
- 2026-11-16: prompt 07 dispatched at `945bf5d`. It stopped at the gate R §2.2: `compare_stores` on `pl` and `pl-resumed`
  differed only in three failure rows, whose stored reasons carry main.py's run label. The user ruled, in their words,
  "(a) please": the label a run-dependent fragment for 07, compared masked and to the shorter cut; `compare_stores.py`
  unchanged; one issue opened. 07 then landed (`bbd99f4`); the gate R holds. One issue opened (§3,
  `[07-failure-reason-carries-the-run-label]`).
- 2026-11-16: prompt 07 reviewed by the orchestrator (this board, "Orchestrator review of prompt 07"); all seven checks
  pass. One issue opened by the review (§3, `[07-a-refused-plot-leaves-ray-errors-unretrieved]`). 08 is written only when
  the user asks for it.
- 2026-11-16: the user passed on the writeup agent's ruling that k_ex = ½E^{1/2}|V‴/V″| (Erickcek et al., Eq. (64)); the
  planner checked it against the paper and the code (README §6.12).
- 2026-11-17: R13–R17 ruled by the user (the V‴ form on every potential, the exponential's law refitted under a new
  drain model, one k_ex at every use, φ_t unchanged, re-measure); re-planned with 07b–07d before 08 (P73–P90, U11–U13);
  prompt 07b and its orchestrator note written; not dispatched.
- 2026-11-17: U11 (the power wall's law renamed `one-loop-inverse-power-v1` in 07c) and U12 (`VERSION_LABEL` 2026.13.0
  in 07c) ruled by the user as proposed: "U11 and U12 as proposed; dispatch 07b in a fresh session". U13 after 07b.
- 2026-11-17: prompt 07b dispatched at `e4ae58b`, and landed (`0223d50`); the gate Remeasure holds. One issue opened
  (§3, `[07b-the-one-loop-fixed-point-needs-more-than-100-iterations-near-the-chain-end]`). The candidate laws for
  `one-loop-exponential-v2` go to the user (U13; verification §7.5, §7.10); 07c is not written.
- 2026-11-17: prompt 07b reviewed by the orchestrator (this board, "Orchestrator review of prompt 07b"); all six checks
  pass. No issue opened by the review. 07c is written only when the user has ruled U13 and asks for it.
- 2026-11-18: after 07b the user asked why ℓ moved on the exponential and whether 07b's fits had the wrong form; the
  orchestrator's scratch probe (README §6.13) found the wall's |V‴/V″| = λ(1 + 2(n+1)/(nu) + …), f a function of the new
  ℓ alone beyond its peak, and min(rise, fall) fitting about as v1 did. The user: "Measure the min(rise, fall) form
  properly before 07c". Prompt 07b2 inserted and written (P91–P98, the gate Form), with no orchestrator note; U13 after
  07b2.
- 2026-11-18: prompt 07b2 dispatched at `dec7793`, and landed (`2f638e0`); the gate Form holds. No issue opened. The
  min(rise, fall) form and its variants, with errors, a prediction of 36 new points made before they ran, the edges and the
  paths, go to the user (U13; verification §8.8); 07c is not written.
- 2026-11-18: prompt 07b2 reviewed by the orchestrator (this board, "Orchestrator review of prompt 07b2"); all six
  checks pass. No issue opened by the review. 07c is written only when the user has ruled U13 and asks for it.
- 2026-11-18: U13 ruled by the user (README §6.13): `one-loop-exponential-v2` is 07b2's min(rise, fall) form, its fall
  (A, ℓ₀) shared by n = 1 and 2 and its rise per n, the constants 07b2's joint fit on ln(V₀/M⁴) ≤ 10 with the new points.
  U11–U13 are now ruled; 07c is written when the user asks for it.
- 2026-11-18: the orchestrator's probe after 07b committed at the user's request (`source/kex/probe-07b/`), and 07b2's
  figures cited in verification §8.9.
- 2026-11-18: prompt 07c written at the user's word ("We are ready to write 07c, so please do so"), on P99–P110 (README
  §6.14), with no orchestrator note. U14 (the exponential's one-loop law at n other than 1 and 2) is the user's before
  07c is dispatched.
- 2026-11-18: U14 ruled by the user (README §6.14): "U14: yes, for now, refuse `n >= 3`. I am only expecting to do
  production runs for `n=1` and `n=2`." 07c's §1.3 made exact; 07c is dispatched at the user's word.
- 2026-11-18: prompt 07c dispatched at `c4258ef`, and landed (`c8461ab`); the gate Consistent holds. One issue closed
  (`[07b-the-one-loop-fixed-point-needs-more-than-100-iterations-near-the-chain-end]`, §4) and one opened
  (`[07c-the-third-hook-leaves-float-range-below-phi-1e-103-mp]`, §3). The exponential's one-loop history at laboratory M
  (β 2, A 1) moves under v2: ΔN_eff 0.04462 → 0.04163 at T_CMB (verification §9.7). 07d is written when the user asks.
- 2026-11-18: prompt 07c reviewed by the orchestrator (this board, "Orchestrator review of prompt 07c"); all seven checks
  pass. 07d is written only when the user asks for it.
- 2026-11-18: prompt 07d and its orchestrator note written, at the user's request ("Please write 07d (and an
  orchestration note, if needed) now"), with the planner's P111–P124 in README §6.15 (P87 made exact: four fresh stores
  at the tip against 07's, the k_ex columns named, the exponential's one-loop histories measured, U12's guard on a 07
  store), from a survey of the tip, 07's records and 07's stores, whose fingerprints still equal their sidecars (note 07d
  §0). No question waits on the user; not dispatched.
- 2026-11-18: prompt 07d dispatched at the user's word at `e6e50b0` (19:04, the Gate measured again by the orchestrator),
  and stopped in run `pl-a` at its §6 and P114: five power-wall histories at n = 1, β = 2.9, M = 4 × 10⁻³¹ M_P that 07
  built fail at the tip, the pulse rate overflowing at φ_t = 2.3 × 10⁻¹⁰³ M_P (§3,
  `[07c-the-third-hook-leaves-float-range-below-phi-1e-103-mp]`, narrowed). Before it: the suites at their counts, the
  probe, `exp` built and equal to 07's in its nine fixed-law and natural-mode histories but the k_ex columns. Nothing
  committed by the agent; the cluster stopped and the lock released at the orchestrator's word.
- 2026-11-19: the stop ruled by the user, "Try (a)". Prompt 07c2 (the pulse rate in closed form) inserted before 07d and
  written, on P125–P134 (README §6.16); 07d's prompt and note amended for its restart in `power-law-wall-07d-r2-*`
  (P133); the first attempt's stores and records left in place, read by nothing, for the user to retire (P134). Not
  dispatched.
- 2026-11-19: prompt 07c2 dispatched at `7a202b7`, and landed (`bd8d648`); the gate Range holds. One issue closed
  (`[07c-the-third-hook-leaves-float-range-below-phi-1e-103-mp]`, §4), none opened. The pulse rate is each model's hook in
  closed form; 07d's stop reproduced at the base and its histories built at the tip, equal to 07's; the least margin of
  07's grid's assessments 45.0 decades; `VERSION_LABEL` 2026.14.0. 07d restarts after 07c2's review (P133).
- 2026-11-19: prompt 07c2 reviewed by the orchestrator (this board, "Orchestrator review of prompt 07c2"); all eight
  checks of its §5 pass. 07d's restart is dispatched only at the user's word.
- 2026-11-19: prompt 07d's restart dispatched at `00fb718`; stopped at the suites before (a timing test of `tools/tests`
  failing across the machine's sleep), ruled (a) by the user ("This test is of no importance anyway, and has nothing to do
  with the science code. Please just do (a)."); then landed (`b5f863a`): the gate R2 holds. No issue opened or closed.
  Thirteen registered runs in `power-law-wall-07d-r2-{exp,pl,pl-resumed,mixed}`; every history whose law reads no ℓ equal
  to 07's but P114's k_ex columns, the first attempt's five among them; the exponential's one-loop histories under v2
  measured; the readers on every store and U12's guard on 07's exp store.
- 2026-11-19: prompt 07d reviewed by the orchestrator (this board, "Orchestrator review of prompt 07d"); all seven checks
  of note 07d §3 pass. One issue opened by the review (§3, `[07d-a-reader-refuses-an-older-label-by-a-traceback]`). 08 is
  written only when the user asks for it.
