# power-law-wall campaign — implementation state

**Last updated:** 2026-11-16 · **Status: LIVE — 6 of 8 prompts landed (01, the gate Model holds; 02, the gate Wall holds; 03, the gate Law holds; 04, the gate Sample holds; 05, the gate Store holds, after its ruling-4 stop was ruled (a) by the user; 06, the gate Export holds, one issue opened); 01–06 reviewed; 06 landed on R12 (the potentials treated symmetrically: every export under `<output>/<label>/<model>/` with a `model` column, `--models` a list) and P45–P58 (README §6.10); 07 written with P59–P72 (README §6.11), not dispatched; R11 (the surfing thresholds at n = 1 recorded as measured) ruled after 04 (README §6.8); P33–P44 stand as proposed (README §6.9); P24–P32 ruled as proposed; D12 left as it is by the user; R4–R5 ruled on 01's measurements (README §6.4); R6–R8 (U4 S = S_n; U3 the law f_n(ln R); U5 the three-region spectrum, the exponential's too) ruled on 02's (README §6.5); R9–R10 (U8 table in x, U9 associated piece) ruled with 03 (README §6.6); R1–R3 and U1–U7 ruled (README §6).**

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
| 07 | Rehearse | registered builds, exponential bitwise, power law resumed; U7's grid in five stores (the exponential at `a7e0978` and the tip, the power law straight and resumed, both potentials in one store), against 04, the readers on every store (P59–P72) | Opus | ✍️ | ⬜ | — | — |
| 08 | Close | the documents, the handover, the issues, the indexes | Opus | ⬜ | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Prompts are written one at a time, each after the previous one lands and is reviewed. Orchestrator notes, one prompt at
a time, in `orchestrator/`: [`prompt-01.md`](orchestrator/prompt-01.md), written 2026-11-13 with prompt 01, used for
01 (dispatched 2026-11-14 at `75ac6fb`); [`prompt-02.md`](orchestrator/prompt-02.md), written 2026-11-14 with prompt 02, used for
02 (dispatched 2026-11-14 at `35f6359`); [`prompt-03.md`](orchestrator/prompt-03.md), written 2026-11-15 with prompt 03,
used for 03 (dispatched 2026-11-15 at `7c60226`, after U8 and U9 were ruled as proposed, R9 and R10), and its §0 holds the f_n(ln R) fit the user ruled R7 on and the
spectrum probes R8 was ruled on; [`prompt-04.md`](orchestrator/prompt-04.md), written 2026-11-15 with prompt 04, not dispatched, and its §0 holds the planner's probe at `99cc79b` (surfing at 10⁻³ M_P, one-loop histories at 10⁻¹⁵ and 10⁻³³ M_P, the M_crit estimate); [`prompt-07.md`](orchestrator/prompt-07.md), written 2026-11-16 with prompt 07, not dispatched, and its §0 holds the planner's survey at `c7ba5ca` (the registry's roots in the main checkout, the data files equal at `a7e0978` and the tip, 04's costs and print formats). Note 02's §0 holds the planner's probe: the path formula, the power wall's scale invariance above V₀, and the
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

## 3. Active and unresolved issues

- **`[06-plot-quanta-grid-refuses-two-n-at-one-cell]`** (opened by prompt 06, 2026-11-16; log 06 §6). `tools/plot_quanta_grid.py`
  draws one figure per (drain model, A) of a root and refuses a figure with two rows in one (β, M) cell. On a power-law
  root the science run's grid will have n = 1 and n = 2 at every (β, M) under `one-loop-measured-pl-v1`, so every grid
  figure of that root would be refused (the exponential's science roots hold n = 1 only, so this never arose).
  - *Impact:* the standard analysis "Relativistic quanta"'s grid figures for the power law; `quanta_measures` and
    `plot_quanta_history` are not affected (each history has its own directory, by n).
  - *Next step:* group the grid by (drain model, A, n), or draw n as panels, in a prompt of its own (08's handover may
    assign it); the figure's paths and the exponential's figures then need a ruling on whether they move.

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
