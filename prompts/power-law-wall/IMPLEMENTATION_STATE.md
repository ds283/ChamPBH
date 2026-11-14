# power-law-wall campaign — implementation state

**Last updated:** 2026-11-14 · **Status: LIVE — 1 of 8 prompts landed and reviewed (01, the gate Model holds); R4–R5 ruled on its measurements (README §6.4); 02 written, not dispatched; R1–R3 and U1–U7 ruled (README §6).**

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
| 02 | Measure the wall | S, the one-loop fraction and the spectrum on the power wall (research, `source/wall/`); then U3–U5 | Opus | ✍️ | ⬜ | — | — |
| 03 | The law on this wall | `_drain_L`, the law and the spectrum as U3–U5 rule | Opus | ⬜ | ⬜ | — | — |
| 04 | Measure a sample | about forty histories under both laws; the thresholds | Opus | ⬜ | ⬜ | — | — |
| 05 | The store | type id 2, its table, the registry, `FINGERPRINT_FORMAT` 8, the label (U6) | Opus | ⬜ | ⬜ | — | — |
| 06 | Drivers and exports | the model option, the plot scripts, the export layout (P11) | Sonnet | ⬜ | ⬜ | — | — |
| 07 | Rehearse | registered builds, exponential bitwise, power law resumed | Opus | ⬜ | ⬜ | — | — |
| 08 | Close | the documents, the handover, the issues, the indexes | Opus | ⬜ | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Prompts are written one at a time, each after the previous one lands and is reviewed. Orchestrator notes, one prompt at
a time, in `orchestrator/`: [`prompt-01.md`](orchestrator/prompt-01.md), written 2026-11-13 with prompt 01, used for
01 (dispatched 2026-11-14 at `75ac6fb`); [`prompt-02.md`](orchestrator/prompt-02.md), written 2026-11-14 with prompt 02, not
dispatched. Its §0 holds the planner's probe: the path formula, the power wall's scale invariance above V₀, and the
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

## 2. The gates

As README §2.1: Model (01), Wall (02), Law (03), Sample (04), Store (05), Export (06), R (07), Close (08).

- **Model — holds** (prompt 01, 2026-11-14; log 01 §4, `.documents/power-law-wall-verification.md` §1): every hook
  equals its closed form and the centred difference of the hook below it from 1e-90 M to 10 M_P at n = 1, 2 (M = 4e-31
  and 1e-3 M_P) with no overflow; the turning point, the minimum, its derivative and the seed against closed forms or an
  independent root; the one-loop law refuses the model at entry; the exponential goldens bitwise; no suite count fell.

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
