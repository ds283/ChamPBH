# power-law-wall campaign — implementation state

**Last updated:** 2026-11-14 · **Status: LIVE — 1 of 8 prompts landed (01, the gate Model holds); R1–R3 and U1–U7 ruled (README §6).**

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
| 01 | The model | `InversePowerModel`, the seed at any n, the tool route's `--model` and `--n`; natural mode at large M measured | Opus | ✍️ | ✅ | this commit | [`logs/01-the-model.md`](logs/01-the-model.md) |
| 02 | Measure the wall | S, the one-loop fraction and the spectrum on the power wall (research, `source/wall/`); then U3–U5 | Opus | ⬜ | ⬜ | — | — |
| 03 | The law on this wall | `_drain_L`, the law and the spectrum as U3–U5 rule | Opus | ⬜ | ⬜ | — | — |
| 04 | Measure a sample | about forty histories under both laws; the thresholds | Opus | ⬜ | ⬜ | — | — |
| 05 | The store | type id 2, its table, the registry, `FINGERPRINT_FORMAT` 8, the label (U6) | Opus | ⬜ | ⬜ | — | — |
| 06 | Drivers and exports | the model option, the plot scripts, the export layout (P11) | Sonnet | ⬜ | ⬜ | — | — |
| 07 | Rehearse | registered builds, exponential bitwise, power law resumed | Opus | ⬜ | ⬜ | — | — |
| 08 | Close | the documents, the handover, the issues, the indexes | Opus | ⬜ | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Prompts are written one at a time, each after the previous one lands and is reviewed. Orchestrator notes, one prompt at
a time, in `orchestrator/`: [`prompt-01.md`](orchestrator/prompt-01.md), written 2026-11-13 with prompt 01, not
dispatched.

**Planner's probe for 01 (2026-11-13, at `c540caa`, not committed).** A scratch power-law model through
`tools/history_and_bbn.py`: at laboratory M under the fixed law, n = 1 and 2 give the same history as each other and
as the exponential at β = 2, with ln R at the first impact 530.6 (n = 1) and 320.7 (n = 2) against the exponential's
142.4; natural mode at 10⁻³ M_P fails on both n at φ ≤ 0, with the n = 2 turning point at 9.4 × 10⁻²⁷ M_P (note §0).

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
- 2026-11-14: prompt 01 landed (this commit); the gate Model holds. Its stop on `test_compare_stores` (13) ruled by the
  user, "(a), add the unstored entry to compare_stores". No issue opened.
