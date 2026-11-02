# reflection-restart — verification

**Campaign:** [`prompts/reflection-restart/`](../prompts/reflection-restart/README.md) · **Board:**
[`IMPLEMENTATION_STATE.md`](../prompts/reflection-restart/IMPLEMENTATION_STATE.md) · **Source:** the diagnosis
[`post-reflection-step-2026-11-02/`](post-reflection-step-2026-11-02/README.md).

This document holds the measurements of prompts 01, 02 and 04 (README §6.2 P9). It is additive (`CLAUDE.md`
invariant 6): a later run that supersedes a figure gets a new dated subsection, and the figures below stay as
they are, correct for the tree they were taken on.

## 1. Prompt 01 — the fixes measured (2026-11-02)

**Tree:** branch `reflection-restart` at `3b8a553` plus prompt 01's changes (the probe switch in
`ComputeTargets/ScalarHistory.py`, the scanner `tools/outward_step_scan.py`), committed as prompt 01's commit.
SciPy 1.17.0, Python 3.13.16. **Scripts:** [`reflection-restart/measure/`](reflection-restart/measure/), each with
a docstring. **Committed outputs:** [`reflection-restart/out/`](reflection-restart/out/). **Raw output** (not
committed): `var/reflection-restart/01/` — `signature.jsonl` (the Signature run, every first outward step's
row), `main.jsonl` (the batch, one JSON line per task, the flagged rows in full), with their `.log`, `.pid`, the
task lists and the launch and end times.

### 1.1 The scanner and its flag

`tools/outward_step_scan.py` runs one history as `tools/history_and_bbn.py` does, with no BBN: tuned, or untuned
at a given normalization and φ_ref, or (`--from-store`) at a stored history's normalization, φ_ref, α, φ*, T* and
tolerances, read from the store read-only. For every accepted Radau step that begins with π > 0, in every Radau
instance of every integration, it records N₀, h, h·ω (ω = √|J[1,0]| of the Jacobian Radau holds at the step's
start), π_end/π_start, whether π changes sign on the step's interpolant (50 points, ends included; "changes sign"
is a sample with π ≤ 0), the interpolant's least φ (in units of M and of φ_start), whether the step is the first
of its instance, what started the instance (start, reflection, drain, release, an F4 retake), the cap, the
solver's h_abs before the step and −π′. Per history: the outcome, the counts, the dark-radiation and parking
values, the distributions of π_end/π_start and h·ω, the largest h·ω, the flagged rows, and digests of the samples.

**How it observes.** A subclass of SciPy's `Radau` is put in place of the name `Radau` in
`ComputeTargets.ScalarHistory` for the duration of a history, and `integrate_scalar_history` is wrapped there to
count integrations; both are restored when the history ends, and SciPy's class is untouched. The subclass calls
SciPy's own `step` and only reads attributes, so an observed run is bitwise an unobserved one (§1.2: every
printed digit of the diagnosis; §1.4: 54 of 54 stored histories bitwise under None). What started an instance is
read from the frame of the loop that builds it. A hook in the production loop was not used: ruling 7 confines
`ScalarHistory.py`'s change to the switch.

**The flag (P1), as defined, not tuned (ruling 4).** A step beginning with π > 0 is flagged if π_end/π_start < ½
with no sign change of π on its interpolant (the *ratio* condition) or if its interpolant falls below (1 − f) φ_start
(the *phi* condition). Under the production code (None), over every history of the batch (63 successes and the
three failures; `out/measure-summary.txt`, "the flag under None"):

| | count |
|---|---|
| outward steps | 390 774 |
| with a sign change of π on the step | 16 995 |
| flagged by the phi condition | 8 |
| flagged by the ratio condition alone | 14 737 |

- **The phi condition** fires on the three bad steps of the diagnosis (the first step of an instance restarted
  after a reflection; −π′ < 0; h·ω 1064–1245; φ_min/φ_start = −20.5, −6.4, −6.5) and on five legitimate steps,
  the same step of β = 2.9, α = 0.08 at M = 4×10⁻³¹, 10⁻¹⁵, 10⁻¹², 10⁻³³ and 10⁻³⁰: a later step (not the first of
  its instance) with π_start = 6×10⁻⁷, −π′ = 0.099 > 0, which turns at its apex and falls back to φ_min/φ_start =
  0.89983, the f φ the acceleration cap allows.
- **The ratio condition** fires on 14 737 steps, every one with −π′ > 0 (the field decelerating towards an apex),
  none the first step of its instance: about one per outward flight (e.g. 25 at β = 1.5, M = 10⁻¹², α = 0.02,
  with 26 reflections). π_end/π_start of the steps with no sign change runs continuously through ½: the least
  unflagged is 0.500006, the greatest flagged 0.499988. By decade: [0, 10⁻⁴) 2, [10⁻⁴, 10⁻³) 19, [10⁻³, 10⁻²) 137,
  [10⁻², 0.1) 2000, [0.1, 0.5) 12 579, [0.5, 0.9) 53 021, [0.9, 0.99) 51 797, [0.99, 0.999) 32 453,
  [0.999, 0.9999) 21 998, [0.9999, 0.99999) 9993, [0.99999, 1) 6930, ≥ 1 182 850.
- **So the threshold is delicate for later steps**, where P1 assumed the separation the diagnosis found among
  first steps. Among the first outward steps of an instance the separation holds: over the nine histories of the
  diagnosis the good ones have π_end/π_start ≥ 0.999922 and the bad ones 6.6–8.9×10⁻⁵ (§1.2).
- **h·ω**, information only: of the outward steps, 36 276 have h·ω = 0 or below 10⁻¹² (ω is 0 to double precision away from the wall);
  221 have h·ω ≥ 10³, of which three are the bad steps. ω is read from the Jacobian the solver holds, which SciPy
  computes when the instance is built and recomputes only after a slow Newton iteration, so on a later step it is
  usually the restart's ω near the wall, not the local one; a later step's h·ω (up to 1.2×10⁴ among the ratio-only
  flags) does not measure its stiffness.
- **What distinguishes the bad steps** in everything measured here: the phi condition on the *first step of a
  restarted instance*. No other step of the batch has both, under any variant. This is not a ruled signature: it is
  issue `[01-p1-flag-fires-on-legitimate-steps]` (board §3).

**Tests** (`tools/tests/test_outward_step_scan.py`, 22, about 30 s): the flag on synthetic quantities (each
condition alone, neither, the strict thresholds, a sign change, an inward step); the flag against the probe's copy
in `ScalarHistory` on synthetic interpolants; the observer on a stub of the loop's shape (start reasons, counts,
rows, the patch restored and SciPy's class untouched); the batch on stub tasks (one line each, the resume, a task
that raises, numpy values in a record, a timeout, the pid file and `** scan done`); the store comparison on a
temporary store built by `_TempStoreCase` (a dissipation history, M = 0.5 M_P, β = 2, α = 0.1, written by the
factory): bitwise against its own payload, a changed sample, history column and `extra_data` key each reported, the
selection and the store mode re-integrating it bitwise, a path that cannot be opened read-only refused.

### 1.2 The Signature gate (ruling 1)

`measure/signature.py` against `post-reflection-step-2026-11-02/out/scan_first_steps.txt`, on the scanner's run of
the nine histories under None (`out/signature.txt`). **Every line the diagnosis printed is reproduced as text**:
each outcome line (the three failure reasons; RHS, accepted steps, reflections, samples and integrations of the
six successes), each count line, each good-step statistics line and the three flagged steps' N₀, h, h·ω,
π_end/π_start, φ_start/M, least φ/M, cap, first h_abs and −π′:

| history | first outward steps (diagnosis) | scanner, diagnosis's accounting | scanner's own | bad step: N₀, h, h·ω, π_end/π_start |
|---|---|---|---|---|
| β 1.5, 10⁻¹², α 0.05 | 30 | 30 | 30 | 35.91429105, 6.37×10⁻⁴, 1.24×10³, 6.58×10⁻⁵ |
| β 1.14, 10⁻¹⁵, α 0.01 | 270 | 270 | 270 | 37.67089767, 1.61×10⁻⁴, 1.07×10³, 8.79×10⁻⁵ |
| β 1.09, 10⁻¹⁵, α 0.03 | 151 | 151 | 151 | 37.41894954, 1.6×10⁻⁴, 1.06×10³, 8.9×10⁻⁵ |
| β 1.5, 10⁻¹², α 0.02 | 25 | 25 | 25 | — |
| β 1.5, 10⁻¹², α 0.15 | 32 | 32 | 32 | — |
| β 1.09, 10⁻¹², α 0.08 | 62 | 62 | 62 | — |
| β 1.2, 10⁻¹⁵, α 0.03 | 174 | 174 | 174 | — |
| β 1.5, 4×10⁻³¹, α 0.08 | 36 | 36 | 36 | — |
| β 1.2, 4×10⁻³¹, α 0.08 | 55 | 55 | 55 | — |
| **sum** | **835** | **835** | **835** | |

- **The diagnosis's README §4 and this campaign's README §2.1 and prompt §1.3 say 845; the diagnosis's own nine
  counts sum to 835.** 845 is an arithmetic slip; the gate is met on 835 (ruling 1). Those documents are not edited.
- **No accounting difference.** The diagnosis counted the step made by an instance's first call of `step()`; the
  scanner's own count is an instance's first accepted step. They differ only where a first call raises a
  trial-state exception, and none did. The orchestrator's concern that the diagnosis's patch counted parked-phase
  Radau instances does not arise: the parked phase integrates with DOP853 (`ScalarHistory.py`, `ParkedRHS` under
  `guarded(DOP853, …)`), and every SciPy Radau of a history is built by `new_solver`. Each history has one
  integration, and its instances are the start and one per reflection (31, 271, 152, 26, 33, 63, 175, 37, 56).

### 1.3 The probe switch

`ComputeTargets/ScalarHistory.py`: `_PROBE_OUTWARD_FIX` (None, "F1", "F2", "F3", "F4", "F3+F4"; anything else a
`ValueError` at the start of the integration) and `_PROBE_STIFFNESS_C` (1.0), read by `integrate_scalar_history` at
call time; `IntegrationResult.probe_retakes`. The comment says it exists for this measurement only, is no driver's
option and is removed by prompt 03.

- **None is bitwise today's code:** `ComputeTargets.tests.test_golden_history` (both golden histories and the
  record), `ComputeTargets.tests.test_tuning` (the tuned golden history, test (e)) and
  `ObjectFactories.tests.test_store_fingerprint_golden` pass unchanged; the two suite runs (log 01 §4) pass; and the
  batch reproduces 54 of 54 stored histories bitwise (§1.4).
- **The tests** (`ComputeTargets/tests/test_probe_outward_fix.py`, 10, about 1 s). The window starts at the state
  the diagnosis's first failure reflected from (β = 1.5, M = 10⁻¹², α = 0.05, N = 35.914291), recorded as float.hex
  by `measure/restart_state.py`, with the realized model and the ledger before the reflection's drain; the loop
  makes the reflection and the drain (the ledger's new entry and the restarted state are checked bitwise against
  the history's) and runs until ln T_J has fallen by 2×10⁻³. Under None the restarted Radau's first step is the
  history's, h = 6.3667356×10⁻⁴ bitwise, flagged, and the gate fails the window with the history's m_eff² failure.
  Under F1, F2 (c = 1, 0.3, 3), F3, F4 and F3+F4 the window succeeds, no kept step is flagged, π > 0 on every step
  up to N₀ + h, and φ and π at N₀ + h agree with the diagnosis's resolved re-integration (1.523361×10⁻⁶ M_P,
  2.351192×10⁻³) to 10⁻⁶; F4 discards the bad step and retakes it; F3's restarted first step is
  min(0.1, max(h_floor, f φ/π)). F3 is bitwise None on a window with no restart, F4 on a window with no flagged
  step; the default is None; other values raise.
- **F4 bitwise where nothing is flagged** is shown on the cap-loop probe P1 at M = 0.5 to N = 20.8 (104 outward
  steps, none flagged): ts, the final state and the RHS count equal None's. On a whole history it cannot be shown:
  every history measured holds ratio-only flags (§1.1).

### 1.4 The measurement

`measure/tasks.py main`: 414 tasks, run by `outward_step_scan.py batch --jobs 6 --timeout 1800` (ruling 5), detached.
Launched 11:04:28 UTC (load 8.34 / 12.52 / 11.02); it stopped at 11:07:07 after 73 records, on a record it could not
write as JSON (a 0-d numpy array in a comparison entry); the writer was fixed and the batch resumed at 11:09:18
(load 6.76 / 10.56 / 10.71) from its own output, re-running the three tasks that had been in flight; it ended at
11:38:29 (load at 11:39, 8.17 / 12.82 / 13.26; it reached about 20 in between). **No timeout and no error**: 411
successes and the three failures under None. The longest task took 510 s (F1, β = 1.2, M = 10⁻¹⁵, α = 0.001).
Per-history tables: `out/measure-summary.txt`; one line per task: `out/measure-tasks.tsv`.

**The variants over everything** (66 histories each; F2 at c = 0.3 and 3 on the nine only). "Bad kind" is a kept
step flagged by the phi condition that is the first step of a restarted instance; the store columns are items 4 and
5 against the 2026.8.0 stores (54 histories); the cost is accepted steps against None, per M (wall-clock in
`out/measure-summary.txt`, within about ±10 % of the step ratio but for F1, whose wall-clock ratio is 1.5–1.75).

| variant | the three failures | kept flags: bad kind / later phi / ratio-only | goldens (3) | store bitwise (54) | steps vs None at 4×10⁻³¹ / 10⁻¹⁵ / 10⁻¹² / 10⁻³³ / 10⁻³⁰ / 10⁻³ / 0.5 |
|---|---|---|---|---|---|
| None | fail (m_eff²) | 3 / 5 / 14 737 | bitwise | 54 | 1 |
| F1 | succeed | 0 / 0 / 13 316 | moved | 0 | 1.98 / 1.78 / 1.67 / 1.92 / 1.92 / 1.09 / 1.01 |
| F2, c = 1 | succeed | 0 / 5 / 14 735 | moved | 44 | 1.00 / 1.00 / 1.00 / 1.00 / 1.00 / 1.00 / 1.00 |
| F3 | succeed | 0 / 0 / 15 414 | bitwise | 6 (α = 0 only) | 1.03 / 1.00 / 1.01 / 1.02 / 1.02 / 1.00 / 1.00 |
| F4 | succeed | 0 / 0 / 0 (22 348 discarded) | moved | 0 | 1.00 / 0.95 / 1.02 / 1.00 / 1.00 / 1.01 / 1.01 |
| F3+F4 | succeed | 0 / 0 / 0 (23 346 discarded) | moved | 0 | 1.03 / 0.96 / 1.01 / 1.03 / 1.03 / 1.01 / 1.01 |

**The three failures** (accepted steps of the final integration; tuned, one integration each):

| variant | β 1.5, 10⁻¹², α 0.05 | β 1.09, 10⁻¹⁵, α 0.03 | β 1.14, 10⁻¹⁵, α 0.01 |
|---|---|---|---|
| None | m_eff² failure | m_eff² failure | m_eff² failure |
| F1 | 13 168 | 53 543 | 97 311 |
| F2, c = 1 / 0.3 / 3 | 7093 / 9805 / 7099 | 27 614 / 31 033 / 27 582 | 50 061 / 58 714 / 49 850 |
| F3 | 7183 | 27 665 | 49 765 |
| F4 (steps discarded) | 7123 (59) | 27 528 (256) | 48 564 (409) |
| F3+F4 (discarded) | 7236 (74) | 27 780 (220) | 49 755 (358) |

**The six clean histories.** Every variant succeeds; none keeps a step of the bad kind; under None they hold 330
ratio-only flags and no phi flag. Against None their per-history values (dissipation values, first bounce,
fixed-temperature values, the tuning's normalization and φ_ref) move by at most: F1 2.2×10⁻², F2 2.2×10⁻² (at c = 1,
0.3 and 3 alike; three of the six bitwise), F3 2.5×10⁻², F4 3.7×10⁻², F3+F4 1.3×10⁻². The largest is
`unassessed_share` (β = 1.09, M = 10⁻¹², α = 0.08, under every variant but F3+F4, whose largest is at β = 1.2,
M = 10⁻¹⁵: `unassessed_share`, then `injections` and `unresolved_candidates` at 5.7×10⁻³, one drain more before
parking); the next is `worst_tracker_margin` at ≤ 9×10⁻⁴. Steps against None: F1 1.86–1.95, F2 1.00–1.01, F3
1.01–1.02, F4 1.00–1.01.

**The golden histories** (α = 0, no reflection; compared by `measure/golden_post.py` with their records):
- **None and F3: bitwise**, all three.
- **F1, F2, F4, F3+F4: moved** — the sample digest, the step and RHS counts, the fixed-temperature values and (tuned)
  the first bounce. Largest change over the excerpts: F1 2.1×10⁻³ (β 1.5), 0.11 in `reflecting_term` (β 5.5), 3.3×10⁻²
  in `friction_term` (tuned); F2 5.9×10⁻⁶, 0.10, 3.1×10⁻²; F4 and F3+F4 2.5×10⁻³, 1.3×10⁻², 3.1×10⁻². F4 moves them
  because the ratio condition fires on 7, 346 and 2267 legitimate steps there, which it retakes.

**The reflection store's 42** (`science-2026.8.0-reflections`, stored normalization, compared column by column):
- **None: 42 of 42 bitwise**, every `ScalarHistoryValue` column, every computed `ScalarHistory` column and every
  shared `extra_data` key, the first evidence for 02 (P3).
- **F3: 6 of 42 bitwise**, the six at α = 0 (M = 10⁻³ and 0.5), which do not reflect. F2: 32 of 42. F1, F4, F3+F4: none.
- **The largest changes against the store** (items 4 and 5 together) of the values the figures read:

| variant | ΔN_eff (1 MeV) | ΔN_eff (today) | ρ_χ/ρ_R (70 keV) | parking N | parking ln T_J | ρ_NP/ρ_R (1 MeV) | ρ_NP/ρ_R (70 keV) | φ (1 MeV) | injections |
|---|---|---|---|---|---|---|---|---|---|
| F1 | 6.0×10⁻⁸ | 3.9×10⁻³ | 7.4×10⁻⁷ | 1.9×10⁻³ | 3.5×10⁻³ | 6.5×10⁻⁵ | 1.2×10⁻³ | 5.3×10⁻⁴ | 7.1×10⁻² |
| F2 | 0 | 4.6×10⁻³ | 0 | 1.9×10⁻³ | 3.5×10⁻³ | 9.3×10⁻⁵ | 3.7×10⁻⁴ | 2.5×10⁻⁵ | 7.1×10⁻² |
| F3 | 3.2×10⁻⁸ | 2.9×10⁻³ | 2.1×10⁻⁷ | 2.4×10⁻³ | 4.5×10⁻³ | 8.7×10⁻⁵ | 3.1×10⁻⁴ | 9.1×10⁻⁴ | 8.8×10⁻² |
| F4 | 1.0×10⁻⁷ | 8.3×10⁻³ | 3.8×10⁻⁷ | 2.4×10⁻³ | 4.5×10⁻³ | 3.2×10⁻⁵ | 4.2×10⁻⁴ | 1.8×10⁻⁴ | 8.8×10⁻² |
| F3+F4 | 8.3×10⁻⁸ | 1.1×10⁻² | 3.1×10⁻⁷ | 1.4×10⁻³ | 2.4×10⁻³ | 5.5×10⁻⁵ | 3.5×10⁻⁴ | 2.1×10⁻⁴ | 8.2×10⁻² |

  Relative changes; 0 is bitwise wherever the column was compared. The largest ΔN_eff (today) and the largest
  injections change are at β = 1.2, α = 0.001 (M = 10⁻¹⁵, 10⁻¹²) and β = 1.2, α = 0.08, M = 10⁻¹², whose parking
  moves by a bounce. Per sample (`out/measure-summary.txt`) φ and π change by up to 100 % at some sample of most
  reflecting histories under every fix, since the phase of the bounces near the wall moves; H_J by up to 3.1×10⁻²
  at one history (β = 1.2, α = 0.001, M = 10⁻¹²), ln T_J by ≤ 7.3×10⁻⁴.

**Block B** (12 histories at M = 10⁻³³ and 10⁻³⁰, β = 1.2, 2.0, 2.9, α = 0.01, 0.08, from `science-2026.8.0`):
None 12 of 12 bitwise; F2 12 of 12 bitwise; F1, F3, F4, F3+F4 none. Under None two of the five legitimate phi
flags of §1.1 are here (β 2.9, α 0.08). No variant keeps a step of the bad kind.

**Timeouts:** none.

### 1.5 Why Radau accepts the bad step (prompt §1.4, P4)

`measure/why_radau_accepts.py` (`out/why_radau_accepts-1.5-1e-12-0.05.txt`, `…-1.14-1e-15-0.01.txt`) takes the bad
step's start state, its right-hand side and the Jacobian the restarted Radau holds, and makes single Radau attempts
at h from 10⁻⁸ to 4×10⁻³ as SciPy 1.17.0's `_step_impl` makes one on an instance's first step (its own private
`solve_collocation_system`, `norm` and constants, reproduced rather than instrumented), against the step
re-integrated resolved (max_step = h/20000). The reproduction is exact: SciPy's path below lands on the accepted
step's h and endpoint bitwise and on its recorded `error_norm`.

**The account.** SciPy's ordinary error estimate does *not* under-read the bad step: err = (γ/h − J)⁻¹ (f + ZE)
gives a scaled norm of 1.09×10⁵ at the bad h (7.9×10⁴ at β = 1.14), within 3 % of the true error of its endpoint
(1.05×10⁵; 6.6×10⁴), and rejects every attempt from h·ω ≈ 0.2–0.3 up. The step is accepted by SciPy's *second*
estimate, which it makes only on an attempt after a rejection: err' = (γ/h − J)⁻¹ (f(y + err) + ZE) (Hairer and
Wanner's refinement for stiff problems; their RADAU5 also makes it on an integration's first step, SciPy only after a
rejection). At a restart with π > 0 and −π′ < 0 the cap is 0.1, so the new instance's first attempt is SciPy's
initial step, 3.18×10⁻³ (h·ω = 6223; 8.07×10⁻⁴, 5356 at β = 1.14). Newton converges there in two iterations, to a
collocation solution that damps the oscillation the restart's Jacobian sees (eigenvalues −0.51 ± 1.95×10⁶ i), and
the plain estimate rejects it. h is cut by SciPy's least factor, 0.2, to 6.37×10⁻⁴ (h·ω = 1245; 1.61×10⁻⁴, 1071);
Newton converges again, the plain estimate is again 1.09×10⁵, and because the step has been rejected once SciPy
recomputes it from f(y + err): 0.9375 (0.9922), and accepts. The refinement assumes f(y + err) − f(y) ≈ J err; at
the wall f is exponential in φ and err moves φ by about 10⁶ M, off the wall, so the refined estimate falls with h:
from 3.4 at h·ω = 654 to 1.26 at 1075, 0.94 at 1245, 0.04 at 6223 (β = 1.5). **It under-reads by 10⁵, and the
under-reading grows with h·ω** past the point where Newton converges. Newton converges from Z₀ = 0 with the
restart's Jacobian only for h·ω ≳ 10³ (h ≥ 5.5×10⁻⁴ at β = 1.5; h ≥ 1.6×10⁻⁴ at β = 1.14, with one failure above
it, at h·ω = 2222); below, SciPy halves the step until it reaches the small steps of a good restart. **Acceptance starts to be wrong at h·ω ≈ 1.1–1.2×10³**, where the
refined estimate crosses 1 within the range Newton converges, which is the h·ω of all three bad steps (1064–1245),
and only an attempt that follows a rejected one can be accepted there: the first step of an instance whose first
attempt was large, which is what an uncapped restart gives.

**Whether a grown step can be bad, so whether F3 alone suffices.** From the same state a Radau started at F3's
first step (10⁻¹¹), or at 10⁻⁶, 4×10⁻⁶, 4×10⁻⁵ or 1.6×10⁻⁴ (all cut by SciPy to 6–8×10⁻¹¹, since Newton fails
above), grows its step by up to ten a step, with no cap, to the apex of the outward flight or to N₀ + 0.02 (past the
apex the loop's inward cap would govern, and this bare Radau has none). The field leaves the wall exponentially fast, so ω, from a
Jacobian computed afresh at each step's start, falls faster than h grows: the largest h·ω over the outward flight
is 2.2×10⁻⁴ (β = 1.5) and 1.5×10⁻³ (β = 1.14), at its second or third step, and ω is 0 to double precision once h
reaches 10⁻⁵–10⁻⁴. A grown step therefore cannot reach the h·ω ≈ 10³ where acceptance goes wrong on this flight,
and the batch shows the same everywhere it ran: **under F3 alone no kept step of the batch carries the phi
condition, first or later, in 66 histories** (§1.4), against 3 and 5 under None. What F3 leaves exposed is a
restart whose own first attempts are small but whose field stays where ω is large while h grows; none was found.
Settled within ruling 6's bound; no issue opened for it.

### 1.6 The planner's question, answered

- **Which fixes remove the bad step:** all four, and F2 at c = 0.3, 1 and 3: the three failures succeed, and no
  kept step of the bad kind remains in any of the 66 histories under any of them.
- **Which leave which histories bitwise:**
  - **F3** leaves the golden histories and every history without a reflection bitwise (the three goldens, the six
    α = 0 histories at M = 10⁻³ and 0.5 of the reflection store); it moves every reflecting history, since it
    changes the first step of every restart with π > 0;
  - **F2 (c = 1)** leaves 44 of the 54 stored histories bitwise (all of block B, 32 of the reflection store) and
    three of the six clean histories, and moves the goldens; it acts wherever h ω > c, reflection or not;
  - **F1, F4 and F3+F4** leave nothing bitwise that measured here: F1 caps every outward step, and F4 acts on every
    legitimate step the flag catches (`[01-p1-flag-fires-on-legitimate-steps]`).
- **What each costs** (accepted steps against None): F1 1.7–2.0× wherever histories reflect (1.0–1.1× at M = 10⁻³
  and 0.5); F2 1.00×; F3 1.00–1.03×; F4 0.95–1.02×; F3+F4 0.96–1.03×. F2 at c = 0.3 costs up to 3.2× (one clean
  history at 10⁻¹²); at c = 3 it is F2's cost.
- **What each moves in the stored values** (the table of §1.4): every fix moves ΔN_eff today by 2.9–11×10⁻³ at
  worst, parking N and T by 1.4–4.5×10⁻³ and ρ_NP/ρ_R by up to 1.2×10⁻³ (F1, at 70 keV), and changes the count of
  drains before parking by up to 9 %; ΔN_eff at 1 MeV and ρ_χ/ρ_R at 70 keV by ≤ 10⁻⁷. Whether these matter is
  04's comparison with the figures' resolution.

No recommendation is binding (the user rules U6). If one is wanted, on what is measured here: **F3 alone** is the
smallest change that removes every bad step measured (no kept step of the bad kind in 66 histories; the goldens and
non-reflecting histories bitwise; cost ≤ 1.03×), and §1.5 explains why it should: the acceptance needs a rejected
large first attempt at h·ω ≳ 10³, which F3 removes, and a grown step does not reach that h·ω on an outward flight.
F4 as defined adds nothing to F3 here and moves the goldens through the flag's legitimate catches. F2 at c = 1 is the
cheapest in changed digits, but acts outside restarts and moves the goldens.

## 2. Prompt 03 — the fix (2026-11-02)

**Tree:** branch `reflection-restart` at `89a0760` plus prompt 03's changes, committed as prompt 03's commit
(`b81eab8`). SciPy 1.17.0, Python 3.13.16. **The fix** (README §6.3, the user's U6, "F3 plus the guard"): in
`integrate_scalar_history`, every Radau restarted with π > 0 (after a reflection, a drain or a release) is given
`first_step = _restart_first_step(y) = min(global_max_step, max(h_floor, f φ/π))`, at most the distance to the
solver's bound (§2.6), and its first accepted step is
tested by `_outward_step_flagged` (P1's flag, either condition); a flagged step is discarded and retaken from the
same (N, state) at `first_step` = h/2, and a retake below `h_floor` fails the history.
`IntegrationResult.restart_retakes` counts the retakes; the payload carries `"restart_retakes"` and `extra_data`
the key `restart_retakes` only when it is above 0. The probe switch of §1.3 is gone. `VERSION_LABEL` 2026.9.0,
stepper label `Radau+kinematic-cap-stepping1`, `FINGERPRINT_FORMAT` 6 (unchanged). **Scripts:**
[`reflection-restart/fix/`](reflection-restart/fix/), each with a docstring. **Committed outputs:**
[`reflection-restart/fix/out/`](reflection-restart/fix/out/). **Raw output** (not committed):
`var/reflection-restart/03/` — `tasks-replay.json`, `replay.jsonl` with its `.log`, `.pid`, `replay.launch` and
`replay.end`; `three-failures/` (the three `one` records); `breakage/` (the replay's breakage run).

**01's measurement scripts run only at `d4703df`.** `reflection-restart/measure/` (`tasks.py`, `golden_post.py`'s
callers, `signature.py`, `why_radau_accepts.py`, …) set the probe switch or name its variants, which this prompt
removed; the scanner now refuses a task that names a variant. They and `out/` stay as 01's record (invariant 6).
`golden_post.py`, which reads the golden paths through the modules, is reused by the replay below on the new
records.

### 2.1 The Fix gate, item 1: the three failures succeed

`tools/outward_step_scan.py one BETA M --alpha A` (tuned, the production code), at 14:53 UTC (load 7.15):

| history | outcome | integrations | accepted steps | RHS | reflections | restart_retakes | signature kept / discarded | wall |
|---|---|---|---|---|---|---|---|---|
| β 1.5, 10⁻¹², α 0.05 | success | 1 | 7183 | 52 403 | 32 | 0 | 0 / 0 | 5.5 s |
| β 1.14, 10⁻¹⁵, α 0.01 | success | 1 | 49 765 | 355 694 | 284 | 0 | 0 / 0 | 24.4 s |
| β 1.09, 10⁻¹⁵, α 0.03 | success | 1 | 27 665 | 198 179 | 153 | 0 | 0 / 0 | 14.2 s |

The accepted steps are 01's under the probe's F3 (§1.4, "the three failures"). Each history's last reflection parks it
(31, 283 and 152 restarts after a reflection; no drain or release).

### 2.2 The Fix gate, item 2: the fixed code reproduces 01's F3 bit for bit

`fix/replay_tasks.py` wrote `var/reflection-restart/03/tasks-replay.json`: 01's 66 task specs under F3
(`var/reflection-restart/01/tasks-main.json`) with `"variant"` removed and `" variant=F3"` removed from each key
(orchestrator note 03, ruling 1): the nine tuned histories of the diagnosis, the 54 at a stored normalization (the
reflection store's 42, block B's 12) and the three goldens with 01's `golden_post.py` hook. Run by
`outward_step_scan.py batch --jobs 6 --timeout 1800`, detached (`nohup`, pid 28532), launched 14:54:38 UTC (load
5.42 / 5.42 / 5.35), `** scan done` at 15:00:43 (load at 15:00, 5.23 / 6.94 / 6.28): 66 records, no timeout, no
error; the longest task 290.5 s (β 1.2, M 10⁻¹⁵, α 0.001), Σ wall 1287 s. `ScalarHistory.py` and the scanner were not
edited while it ran (their SHA-256 before the launch and after the end are equal).

`fix/replay_compare.py` (`fix/out/replay.txt`) compares each record with 01's F3 line for the same task: `outcome`,
`samples_sha256`, `log_one_plus_z_sha256`, `n_samples`, `accepted_steps`, `RHS_evaluations`, `compute_steps`,
`reflections`; for the 54 store tasks the store comparison as a whole (the same columns differing from the store, by
the same amounts, as under 01's F3; only the six at α = 0 are bitwise against the store, as in §1.4); for the three
goldens the hook's verdict against the regenerated records; `restart_retakes` of the record and of every integration;
and the ruled signature, kept or discarded.

| kind | tasks | equal in every field | restart_retakes | signature kept / discarded |
|---|---|---|---|---|
| tuned (the diagnosis's nine) | 9 | 9 | 0 | 0 / 0 |
| store (reflection store 42, block B 12) | 54 | 54 | 0 | 0 / 0 |
| golden (β 1.5, β 5.5 at M = 10⁻³; tuned β 3, M = 0.01) | 3 | 3 (hook: bitwise) | 0 | 0 / 0 |
| **all** | **66** | **66** | **0** | **0 / 0** |

**RESULT: EQUAL (66 of 66).** The guard never fired on any of them. Over the 66, the scanner saw 13 700 Radau
instances begun by a reflection and 70 starts (the tuned golden's five integrations among them); no instance was
begun by a drain, a release or a retake.

### 2.3 The Fix gate, item 3: no signature step

The ruled signature (P1's flag, either condition, on the first accepted step of an instance begun by a reflection, a
drain, a release or a retake) is on no step, kept or discarded, in any of the 66 (§2.2), the nine histories of the
diagnosis among them, nor in §2.1's three runs. Under 01's None the same count was 3 (§1.4).

### 2.4 The Fix gate, item 4: the golden histories

Regenerated beside the old records (`golden_history.py`'s rule), by each module's own command:
`data/golden_history_rr03.json` (β 1.5, 5.5 at M = 10⁻³; 11.3 s) and `data/golden_history_rr03_tuned.json` (β 3,
M = 0.01, tuned; 25.7 s); `golden_history_05.json` and `golden_history_06_tuned.json` stay as the records of
everything before this prompt. `fix/compare_goldens.py` (`fix/out/compare_goldens.txt`) compares old and new field by
field, every excerpt field included (12 + 21 + 25 samples, 1044 fields), and every tuning key: **equal but for the two
allowed differences** (ruling 3), `solver_label` (`…-stepping0` → `…-stepping1`) and the added `restart_retakes` = 0, in
all three. So F3 and the guard leave every history without a restart with π > 0 bitwise.

**The reflecting golden** (P5): `data/golden_history_rr03_reflecting.json`, β = 1.5, M = 10⁻¹² M_P, α = 0.05, tuned,
by `ComputeTargets/tests/golden_reflecting_history.py` on `golden_tuned_history`'s builders (4.1 s): one tuning
integration, 32 reflections, 32 injections, 7183 accepted steps, 52 403 RHS evaluations, 5166 samples,
`restart_retakes` 0, `solver_label` `Radau+kinematic-cap-stepping1`. `test_golden_reflecting_history.py` passes it
bitwise.

### 2.5 The breakage record

Each check edits one place, runs the named tests, expects a failure and restores the file (SHA-256 compared):

| # | breakage | test | result |
|---|---|---|---|
| B1 | `_restart_first_step` always `None` (F3 off) | `TestF3AtAReflection`, `TestF3AtADrainAndAtARelease`, `TestTheRestartFirstStep` | FAILED (4) |
| B2 | the drain's restart without F3 and the guard | `TestF3AtADrainAndAtARelease` | FAILED (2) |
| B2b | the release's restart without F3 and the guard | `TestF3AtADrainAndAtARelease` | FAILED (2) |
| B2c | the reflection's restart without F3 and the guard | `TestF3AtAReflection` | FAILED |
| B3 | the guard never applied | `test_the_bad_step_is_discarded_and_retaken` | FAILED |
| B4 | the guard's mark not cleared after the first step | `test_a_later_flagged_step_of_a_restarted_instance_is_kept` | FAILED |
| B5 | the guard armed at every new Radau, the start included | `test_no_restart_no_guard` | FAILED |
| B6 | the retake at the full discarded h | `test_the_bad_step_is_discarded_and_retaken` | FAILED |
| B7 | the failure bound at h_floor/10 | `test_a_retake_below_h_floor_fails_the_history` | FAILED |
| B8 | the retakes not counted | `test_the_count_is_that_of_the_retakes` | FAILED |
| B9 | `restart_retakes` stored at 0 too | `test_extra_data_holds_the_count_only_above_0` | FAILED |
| B10 | the payload without `"restart_retakes"` | `test_the_payload_carries_the_count` | FAILED |
| B11 | the guard's predicate without the phi condition | `TestTheFlagAgreesWithTheGuard` (scanner) | FAILED |
| B12 | F3's first step halved | `test_golden_reflecting_history` | FAILED |
| B13 | F3's first step not clamped to the solver's bound (§2.6) | `TestF3IsClampedToTheBound` | ERROR (SciPy's `ValueError`) |
| T1 | the scanner's signature not counted | `test_the_counts` | FAILED |
| T2 | a discarded signature step not counted | `test_a_discarded_first_step_is_a_discarded_signature_step` | FAILED |
| T3 | a task naming a variant not refused | `test_a_task_that_names_a_probe_variant_is_refused` | FAILED (2) |
| T4 | `observe` does not restore `Radau` | `test_observe_restores_the_module` | FAILED |
| R | the replay: β 1.5, 10⁻¹², α 0.05 run with B12's mutation, compared by `replay_compare.py` (`fix/out/replay-breakage.txt`) | — | DIFFERS: `samples_sha256`, `RHS_evaluations` 52 403 → 52 394, `compute_steps` |

B7's first form (the bound at h_floor/1000) did not end: with the predicate forced true, SciPy's least step at
N ≈ 36 (10 ulp, about 7×10⁻¹⁴) is above the bound, so each retake's step was clamped back up and halved again
forever. At the real bound it cannot happen: SciPy's least step is below `h_floor` = 10⁻¹¹ for every N < 4096, far past
`N_failsafe` = 1000, so each discarded step is at most half the one before and the bound is reached.

### 2.6 The clamp to the solver's bound, and `test_release`'s re-pin (the user's ruling)

The suites after the change found two things in `ComputeTargets/tests/test_release.py`, which the prompt and the
orchestrator's note had not foreseen. The agent stopped, and the user ruled: "Please allow both".

- **F3 past the bound.** `TestTheEnds` integrates M = 0.5 M_P, β = 5, α = 0.1 with `N_stop` 0.01 e-folds after the
  release. The release restarts with π > 0, F3's step there is 0.1 (f φ/π ≈ 0.66), and SciPy refuses a `first_step`
  past the solver's bound (`ValueError: first_step exceeds bounds`). `new_solver` now takes `min(first_step, N_bound − N)`.
  **Where F3's step fits inside the bound this changes nothing**: so in every history with `N_stop = None`
  (`N_bound = N_failsafe = 1000`), in particular every task of the replay (§2.2), the three failures (§2.1) and the
  three goldens and the reflecting golden (§2.4), which were run before the clamp and stand without a re-run. Tested by
  `TestF3IsClampedToTheBound` (that history with `N_stop = N_rel + 0.01`: it succeeds, ends at `N_stop`, and the
  release's restart has first step `N_stop − N_rel`, below F3's), which fails with SciPy's `ValueError` without the
  clamp (B13, §2.5).
- **The drains move `test_release`'s parked step grid.** The same history restarts with π > 0 at 120 drains and at the
  release, and F3 now sets each restart's first step. The history before the switch moves, and with it the parked
  phase's step grid, by about 2.1×10⁻⁴ e-folds: the check that fails after the release is at N = 40.29068, not 40.29089
  (`BETA_5_FAILED_CHECK_N`, re-pinned; inelastic-reflection log 06's value stays the record of the old code). Every
  other value the test pins holds: parking at candidate 105, the release's crossing within its tolerance of 40.25454.


## 3. Prompt 04 — the close (2026-11-02)

**Tree:** branch `reflection-restart` at `741dcce`, whose computing code is `b81eab8`'s (`git diff --stat b81eab8 741dcce`
over the computing directories shows only `ComputeTargets/tests/test_release.py`, the review's comments), and, for
§3.2, `89a0760` (`b81eab8^`) in a temporary detached worktree under the session scratchpad, run from its root with
`PYTHONPATH=.` and this checkout's venv, removed with `git worktree remove` before the commit (orchestrator note 04,
ruling 3). SciPy 1.17.0, Python 3.13.16. No Ray, no store written; the 2026.8.0 store opened read-only
(`sqlite mode=ro`). **Scripts:** [`reflection-restart/close/`](reflection-restart/close/), each with a docstring.
**Committed outputs:** [`reflection-restart/close/out/`](reflection-restart/close/out/), text only. **Raw output**
(not committed): `var/reflection-restart/04/` — the task lists `tasks-42.json`, `tasks-pair.json`, `tasks-nine.json`;
the batches' `the-42.jsonl`, `pair-89a0760.jsonl`, `pair-this.jsonl`, `the-nine.jsonl`, each with its `.log`, `.pid`,
`.nohup` and launch and end times; the in-process scripts' `f3-off.jsonl`, `candidates-89a0760.jsonl`,
`candidates-this.jsonl`, their `.out` and `.pid`, and every wall-search call in `candidates-<tree>-<serial>.npz`; the
drain probe's two logs and JSON, copied from `one-loop-drain` by `git show` (`probe-*-base-stored-alpha.*`).
`ScalarHistory.py`, `AbstractModel.py` and the scanner were not edited at any point (their SHA-256 before and after
the runs are equal: `the-42.sha-before`, `wave1.sha-this`, `wave1.sha-89a0760`).

**The suites, before anything else** (prompt §3; serially, ComputeTargets in two halves), at `741dcce`, 22:28–22:44
UTC: CosmologyModels 26 (68.1 s; load at start 3.80 / 4.76 / 4.48), ComputeTargets 122 + 392 = 514 (128.7 s, 4.87 /
4.84 / 4.53; 463.1 s, 3.42 / 4.35 / 4.38), ObjectFactories 130 (79.1 s, 4.27 / 4.20 / 4.25), tools 147 (157.5 s,
4.27 / 4.25 / 4.26), RunDrivers 68 (22.8 s, 3.06 / 4.08 / 4.21); all OK. This prompt changes no code or test file, so
they were not run again.

**The task lists** (`close/tasks.py`, `close/out/tasks.txt`): the store has 3846 histories over 16 shards; over all of
them 42 failures contain `log_m_eff_squared` and 9 begin `classical_turning_point` and say "not monotone", the counts
of the Gate. Block C holds 1302 histories and the 42 (by M: 31 at 10⁻¹², 10 at 10⁻¹⁵, 1 at 10⁻¹³; by α: 0.01 8, 0.02 2,
0.03 7, 0.05 10, 0.08 3, 0.1 5, 0.15 7), on 12 shards. Each task carries a key naming its shard and serial.

### 3.1 The 42, recomputed with the fix (prompt §1.1)

Built as log 03 §8 gives the route: mode `tuned`; `normalization`, `phi_ref_Mp`, `store` (and the spec's `failure`
flag) dropped; β, M, α, `phi_init_Mp`, `T_init_GeV`, `atol`, `rtol` kept. `outward_step_scan.py batch --jobs 6
--timeout 1800`, detached (`nohup`, pid 68745), launched 22:44:30 UTC (load 3.63 / 4.13 / 4.22), `** scan done` at
22:45:43 (load 11.10 / 6.07 / 4.93), alone (ruling 2). 42 records, no timeout, no error; Σ wall 390.6 s, the longest
30.9 s. `close/compare.py the-42` (`close/out/the-42.txt`):

| # | store | beta | M/M_P | alpha | outcome | integrations | accepted steps | restart_retakes | reflections | signature kept / discarded | wall (s) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | shard0000#3189 | 1.25 | 1e-12 | 0.15 | success | 1 | 6122 | 0 | 27 | 0 / 0 | 5.1 |
| 2 | shard0000#3441 | 1.25 | 1e-12 | 0.1 | success | 1 | 7774 | 0 | 36 | 0 / 0 | 5.9 |
| 3 | shard0000#3853 | 1.25 | 1e-15 | 0.03 | success | 1 | 29274 | 0 | 162 | 0 / 0 | 16.3 |
| 4 | shard0000#4206 | 2.25 | 1e-13 | 0.01 | success | 1 | 37652 | 0 | 205 | 0 / 0 | 21.2 |
| 5 | shard0000#4254 | 2.6 | 1e-12 | 0.01 | success | 1 | 21843 | 0 | 110 | 0 / 0 | 13.8 |
| 6 | shard0003#3084 | 1.14 | 1e-12 | 0.15 | success | 1 | 5182 | 0 | 22 | 0 / 0 | 4.7 |
| 7 | shard0003#3739 | 1.1 | 1e-15 | 0.03 | success | 1 | 26123 | 0 | 144 | 0 / 0 | 14.4 |
| 8 | shard0003#3818 | 1.14 | 1e-12 | 0.05 | success | 1 | 6737 | 0 | 30 | 0 / 0 | 5.2 |
| 9 | shard0003#3819 | 1.1 | 1e-12 | 0.05 | success | 1 | 5091 | 0 | 22 | 0 / 0 | 4.1 |
| 10 | shard0003#3906 | 1.14 | 1e-12 | 0.03 | success | 1 | 10035 | 0 | 48 | 0 / 0 | 6.8 |
| 11 | shard0003#4000 | 1.14 | 1e-15 | 0.01 | success | 1 | 49765 | 0 | 284 | 0 / 0 | 25.9 |
| 12 | shard0005#3332 | 1.3 | 1e-12 | 0.15 | success | 1 | 6847 | 0 | 31 | 0 / 0 | 5.2 |
| 13 | shard0005#3642 | 1.3 | 1e-12 | 0.05 | success | 1 | 7659 | 0 | 35 | 0 / 0 | 5.5 |
| 14 | shard0007#3240 | 1.15 | 1e-12 | 0.1 | success | 1 | 5141 | 0 | 22 | 0 / 0 | 4.2 |
| 15 | shard0007#3265 | 1.19 | 1e-12 | 0.15 | success | 1 | 5334 | 0 | 23 | 0 / 0 | 4.3 |
| 16 | shard0007#3782 | 1.11 | 1e-12 | 0.05 | success | 1 | 5659 | 0 | 25 | 0 / 0 | 4.4 |
| 17 | shard0007#3897 | 1.11 | 1e-15 | 0.03 | success | 1 | 27430 | 0 | 152 | 0 / 0 | 14.6 |
| 18 | shard0008#4296 | 1.8 | 1e-15 | 0.01 | success | 1 | 60931 | 0 | 353 | 0 / 0 | 30.9 |
| 19 | shard0009#3389 | 1.5 | 1e-12 | 0.1 | success | 1 | 7699 | 0 | 35 | 0 / 0 | 5.5 |
| 20 | shard0009#3405 | 1.5 | 1e-12 | 0.05 | success | 1 | 7183 | 0 | 32 | 0 / 0 | 5.3 |
| 21 | shard0009#3522 | 1.5 | 1e-12 | 0.08 | success | 1 | 8082 | 0 | 37 | 0 / 0 | 5.7 |
| 22 | shard0009#3912 | 1.55 | 1e-12 | 0.05 | success | 1 | 11265 | 0 | 54 | 0 / 0 | 7.4 |
| 23 | shard0009#4066 | 1.5 | 1e-12 | 0.03 | success | 1 | 10915 | 0 | 52 | 0 / 0 | 7.2 |
| 24 | shard0010#3111 | 1.35 | 1e-12 | 0.15 | success | 1 | 6676 | 0 | 30 | 0 / 0 | 4.9 |
| 25 | shard0010#3544 | 1.65 | 1e-12 | 0.05 | success | 1 | 9799 | 0 | 46 | 0 / 0 | 6.7 |
| 26 | shard0010#3606 | 1.65 | 1e-12 | 0.08 | success | 1 | 9390 | 0 | 44 | 0 / 0 | 6.5 |
| 27 | shard0011#3663 | 1.08 | 1e-15 | 0.05 | success | 1 | 20142 | 0 | 109 | 0 / 0 | 11.3 |
| 28 | shard0011#3664 | 1.2 | 1e-15 | 0.05 | success | 1 | 20716 | 0 | 112 | 0 / 0 | 11.4 |
| 29 | shard0011#3883 | 1.2 | 1e-15 | 0.02 | success | 1 | 34992 | 0 | 196 | 0 / 0 | 18.4 |
| 30 | shard0012#4382 | 1.85 | 1e-12 | 0.01 | success | 1 | 15786 | 0 | 78 | 0 / 0 | 9.7 |
| 31 | shard0013#4272 | 1.9 | 1e-12 | 0.01 | success | 1 | 14899 | 0 | 73 | 0 / 0 | 9.4 |
| 32 | shard0014#3381 | 1.6 | 1e-12 | 0.08 | success | 1 | 10116 | 0 | 48 | 0 / 0 | 6.9 |
| 33 | shard0014#3890 | 1.6 | 1e-12 | 0.03 | success | 1 | 11726 | 0 | 56 | 0 / 0 | 7.9 |
| 34 | shard0014#4039 | 2.05 | 1e-12 | 0.02 | success | 1 | 14376 | 0 | 70 | 0 / 0 | 9.1 |
| 35 | shard0015#2959 | 1.17 | 1e-12 | 0.15 | success | 1 | 5479 | 0 | 24 | 0 / 0 | 4.3 |
| 36 | shard0015#3260 | 1.09 | 1e-12 | 0.1 | success | 1 | 6928 | 0 | 32 | 0 / 0 | 5.0 |
| 37 | shard0015#3298 | 1.09 | 1e-12 | 0.15 | success | 1 | 4899 | 0 | 21 | 0 / 0 | 4.1 |
| 38 | shard0015#3304 | 1.13 | 1e-12 | 0.1 | success | 1 | 6078 | 0 | 27 | 0 / 0 | 4.7 |
| 39 | shard0015#3611 | 1.09 | 1e-15 | 0.03 | success | 1 | 27665 | 0 | 153 | 0 / 0 | 15.0 |
| 40 | shard0015#3630 | 1.09 | 1e-15 | 0.05 | success | 1 | 20157 | 0 | 109 | 0 / 0 | 11.4 |
| 41 | shard0015#4389 | 1.13 | 1e-12 | 0.01 | failure | 1 | — | 0 | 59 | 0 / 0 | 8.5 |
| 42 | shard0015#4390 | 1.09 | 1e-12 | 0.01 | success | 1 | 21405 | 0 | 112 | 0 / 0 | 11.9 |

"Accepted steps" is the final integration's; every history tuned in one integration. Each instance after the start
was begun by a reflection (3299 over the 42); none by a drain, a release or a retake.

- **41 succeed, 1 fails.** No `log_m_eff_squared` failure, and no signature step kept or discarded: the two stop
  conditions of prompt §1.1 do not hold. `restart_retakes` is 0 in all 42: the guard did not fire. The three
  reproduced failures (rows 20, 11 and 39) have 03's step counts (§2.1: 7183, 49 765, 27 665).
- **The failure** is 4389 (shard0015, β = 1.13, M = 10⁻¹², α = 0.01). Its seed integration completed (14 318 steps,
  59 reflections, to N = 45.724) and the tuning then failed: "the effective minimum was not found: effective_minimum
  (ExponentialModel(M=2.4353e+06GeV,n=1,beta=1.13)): V_eff' does not change sign on the bracket [8.20971e-05,
  0.0328388] (values 3.724e-121, 3.86555e-121) (tuning integration 0)". Its kind is among the run's ("effective
  minimum", run record "Failed histories"), but every one of those is at β ≤ 1.1074, below the surfing threshold,
  and the kind is not open as an issue: the user left β = 0.6 undiagnosed as non-surfing (`PLAN.md` §8). This one is
  above the threshold. Opened as `[04-a-recomputed-history-does-not-find-the-effective-minimum]` (board §3), not
  diagnosed. In the store 4389 failed in `log_m_eff_squared`, at φ_E = −3.84×10⁻¹².

### 3.2 2197 and 2188, before and after 03 (prompt §1.2 (a))

The two specs from `select_from_store` on the store's absolute path, in mode `store`, filtered on (shard0005, 2197)
and (shard0003, 2188), each carrying its shard and serial (ruling 1); both at normalization −276.63072859401797, α =
10⁻³, M = 4×10⁻³¹ M_P, φ_ref 8.645101631245453×10⁻¹⁶ and 9.140634547368375×10⁻¹⁶ M_P. Run by each tree's scanner
batch (`--jobs 2 --timeout 3600`), detached, launched together at 22:46:25 UTC (load 7.38 / 5.70 / 4.84) beside the
nine (§3.5): `pair-89a0760.jsonl` (pid 69140, run from the worktree, its cwd checked by `lsof`) ended 22:54:14,
`pair-this.jsonl` (pid 69141) at 22:48:34. `close/compare.py pair` (`close/out/pair.txt`):

| | 2197 (β 1.185) | 2188 (β 1.06) |
|---|---|---|
| **at `89a0760`** | success, 906 090 steps, 6 453 742 RHS, 6648 reflections, 4995 samples, 467.5 s | success, 795 508 steps, 5 621 781 RHS, 5741 reflections, 4927 samples, 406.4 s |
| store comparison | **bitwise** (every column; stored accepted_steps 906 090, compute_time 624.3 s) | **bitwise** (stored 795 508, 652.7 s) |
| signature (from the rows) | 0 kept, 0 discarded | 0 kept, 0 discarded |
| **on this tree** | failure in the first integration, 128.0 s, after 1554 reflections | failure in the first integration, 125.6 s, after 1531 reflections |
| the failure | `classical_turning_point`, φ = 2.950090845531173e-07, ln E = −306.7166549210319, bracket [−42.28625968598544, −42.34875968598544] | φ = 2.647733963294608e-07, ln E = −306.6698578858307, bracket [−42.269391484964466, −42.331891484964466] |
| against the probe | the whole printed message (1026 characters, probes included) **equal** to the probe's log line | **equal** |
| signature, `restart_retakes` | 0 kept, 0 discarded; 0 | 0 kept, 0 discarded; 0 |

So 01's bitwise reproduction holds for both at the run's smallest α, and on the fixed tree both fail exactly as the
drain probe found on `2d50b95`. **Against the probe's JSON** (ruling 6): `failure`, `failure_reason` (both cut at 256
characters, as the store's is; the whole message is the integration's printed line, which the scanner's `stdout_tail`
keeps), β and α are equal; the probe holds `M_eV` = 0.0009741294382105218 where the scanner holds M = 4×10⁻³¹ M_P,
the same model (`close/out/failing_call-this.txt`: the stored M and the tool's are bitwise equal). The wall times
(117.1 and 115.3 s in the probe, 128.0 and 125.6 s here) are not compared. Since `89a0760` succeeds, no older tree was
run.

### 3.3 Which of 03's changes moves the trajectory (prompt §1.2 (b))

`close/f3_off.py` replaces `ScalarHistory._restart_first_step` with a function returning `None` (SciPy's choice at
every restart), leaves the guard live, and calls the scanner's `scan_one` in its own process, one process per history
(ruling 4); launched 22:49:12 UTC (load 6.73 / 6.84 / 5.52), 439.7 s and 393.7 s. `close/compare.py f3-off`
(`close/out/f3-off.txt`), each against its `89a0760` record:

| | outcome | samples SHA-256 | ln(1 + z) SHA-256 | samples | accepted steps | RHS | reflections | retakes | signature discarded |
|---|---|---|---|---|---|---|---|---|---|
| 2197 | success = | `f8d5429b…` = | `245a3f2b…` = | 4995 = | 906 090 = | 6 453 742 = | 6648 = | 0 | 0 |
| 2188 | success = | `22416e3f…` = | `08f63493…` = | 4927 = | 795 508 = | 5 621 781 = | 5741 = | 0 | 0 |

**Bitwise equal, both.** With F3 off and the guard live, this tree gives `89a0760`'s history, and the guard discards
nothing (neither history has a signature step under SciPy's choice, as the impact statement said of laboratory M). **F3
alone is the change** that moves 2197 and 2188 onto the failure.

### 3.4 The failing call (prompt §1.2 (c))

**Alone.** `close/failing_call.py` calls `classical_turning_point(normalization, φ, ln E)` with the probe's three
values on a model built as the scanner builds it, on this tree (`close/out/failing_call-this.txt`) and at `89a0760`
(`close/out/failing_call-89a0760.txt`). Both raise, with a message equal to the probe's whole printed line, in
0.04–0.10 ms; the two outputs are identical but for the times. The wall search (`_log_V_minus_floor`,
`_search_log_field`, `classical_turning_point`, `WALL_ROUNDING_QUANTA` = 16, `WALL_SEARCH_HALVINGS` = 4) is unchanged
from `75208cd`, the store's tree, to `741dcce`: the only change to `AbstractModel.py` in that range is `745c8ee`'s, in
`effective_minimum`. So the call raises on its own inputs at any tree since the store was built.

**Where the root lies.** The target is ln V − ln V_floor = ln(1 + E/V_floor), in quanta of ulp(max(|ln V|, |ln V_floor|)) =
ulp(276.63) = 5.68434×10⁻¹⁴; `_log_V_minus_floor` returns −∞ at 16 quanta and below:

| history | ln E | root (quanta) |
|---|---|---|
| 2197 | −306.7166549210319 | **1.5107** |
| 2188 | −306.6698578858307 | **1.5830** |

Decoded probe by probe (ln V − ln V_floor = (M/φ) rounded, in quanta), 2197's search runs: −∞ at the start and the four
doublings, where V − V_floor is not resolved; 693 quanta at ln φ = −46.04, the first finite value, which brackets; then the halvings: −∞
(0), −∞ (13), 94, 34 (four halvings, H = 4, bracket [−43.04, −42.04]); the further halvings while the outer end is
−∞: 21, −∞ (16), 18, **17**, **17**. The last two, 2⁻⁵ e-folds apart (the midpoint of a bracket 2⁻⁴ wide), are the
same quantum, and the value 2.4206637264330197 repeated is ln(17 quanta × V_floor) − ln E: the least finite value the
search can see. 2188's is the same, ending 17, 17 at 2.3738666912318536.

**The nine, from their stored `failure_reason`** (φ, ln E and the normalization are complete there), each called alone on
this tree: all nine raise, each with a message of which the stored 256 characters are a prefix. Their roots:

| store | β | M/M_P | α | stored ln E | root (quanta) | the raise's bracket |
|---|---|---|---|---|---|---|
| shard0002#1592 | 1.1775 | 4×10⁻³¹ | 0.001 | −308.6129970063558 | 0.2268 | 2⁻⁴ |
| shard0003#2208 | 1.18 | 4×10⁻³¹ | 0.001 | −309.37538319488175 | 0.1058 | 2⁻⁵ |
| shard0004#2199 | 1.1825 | 4×10⁻³¹ | 0.001 | −308.6585130622487 | 0.2167 | 2⁻⁴ |
| shard0000#1001 | 0.75 | 4×10⁻³¹ | 0.03 | −304.3236804061682 | **16.5357** | 2⁻⁴ |
| shard0001#766 | 0.9 | 4×10⁻³¹ | 0.02 | −306.2985617947364 | 2.2948 | 2⁻⁵ |
| shard0015#403 | 0.5 | 4×10⁻³¹ | 0.1 | −308.8526551068955 | 0.1784 | 2⁻⁵ |
| shard0000#2735 | 0.75 | 10⁻³³ | 0.03 | −305.91588537339726 | 3.3646 | 2⁻⁴ |
| shard0015#2480 | 0.5 | 10⁻³³ | 0.1 | −308.24395716177537 | 0.3280 | 2⁻⁵ |
| shard0015#2386 | 0.5 | 10⁻³⁰ | 0.15 | −306.8183703090743 | 1.3646 | 2⁻⁵ |

Every root, 2197's and 2188's among them, lies within `WALL_ROUNDING_QUANTA` + 1 = 17 quanta (ruling 7's bound); the
largest is 1001's, 16.54, between 16 and 17. The stored M and the tool's M are bitwise equal for all nine.

**Why such a call always raises.** Every finite value of the residual ln(V − V_floor) − ln E is at least
ln(17 q V_floor) − ln E, which is positive when the root is below 17 quanta. So the first finite probe brackets, the
inner end stays finite and positive, the outer end stays −∞, and brentq is never reached. The halvings beyond H go on
while the outer end is −∞; they close in on the edge of the −∞ region, where the probes are finer than 07b's
derivation orders (2⁻ᴴ = 2⁻⁴ e-folds, with s = 1 and ρ = ½ for the exponential): two of them round to the same
quantum, 17, and the monotonicity check raises "not monotone". Were that check not met, the search would end on "the
bracket's outer end is still −∞". **§0.2's reading holds**: the raise is at a midpoint 2⁻⁵ e-folds from its ends (2⁻⁶
in five of the nine), and the root lies inside the −∞ region; the "not monotone" is how a root below ln V's resolution
shows itself, not float cancellation in a resolved difference.

**The margins along each history.** `close/candidates.py` wraps `AbstractModel.classical_turning_point` passively,
recording every call's φ, ln E, result and root margin, and runs each history at its stored normalization in its own
process, on each tree (ruling 5): launched 22:49:12 (this tree) and 22:56:46 UTC (`89a0760`, load 2.64 / 5.43 / 5.61),
ended by 23:04:45 (load 15.73 / 11.44 / 8.15). Each run reproduces its tree's (a) record — outcome, samples SHA-256
and accepted steps for the successes, the failure_reason for the failures — so the wrapper is passive
(`close/out/candidates.txt`). `close/infall_starts.py` (`close/out/infall_starts.txt`) reads the calls:

| | calls | least margin | where | least margin before the failure |
|---|---|---|---|---|
| 2197 at `89a0760` | 857 476 (all return a turning point) | **208.58** quanta, φ = 1.4800×10⁻⁷, ln E = −301.7889 | the first call of infall 1962 of 6648 | — (success) |
| 2197 on this tree | 231 891 | **1.5107** (the failing call) | the first call of infall 1554 | 2.83×10⁶ (infall 196's first call) |
| 2188 at `89a0760` | 753 395 | **1257.54**, φ = 1.3373×10⁻⁷, ln E = −299.9923 | the first call of infall 1935 of 5741 | — (success) |
| 2188 on this tree | 228 214 | **1.5830** (the failing call) | the first call of infall 1531 | 6.39×10⁸ |

The wall is assessed at every step start with π < 0 (`assess_infall`, from the step loop at α > 0), so the calls run in
infalls of 109–631 calls with φ falling, and a new infall starts where φ rises. **The least margin on both trees is
always at an infall's first call**: the first step start after the field has turned back on the plateau, where π has
just changed sign, so ρ_kin = H²π²/2 can be as small as the step grid places that start near the apex; V − V_floor there
is below ln V's resolution, so E = ρ_kin. The later calls of an infall carry its energy, and that is continuous:
- on this tree, before 2197's failure, infalls 1551–1553 start at ln E = −288.62 to −288.63 with second calls at
  −287.88 and last calls at −286.32 to −286.33; infall 1554's first call is at φ = 2.95×10⁻⁷ (the apex, 10 % beyond its
  neighbours' first-step φ) and ln E = −306.72. Earlier "outlier" starts of the same kind, ln E more than 3 below the
  local median of 21 starts, came in infalls 18, 196 and 197 (2197) and 184 and 185 (2188); each one's second and last
  calls lie on its neighbours' trend (infall 196: second −274.540 between −274.510 and −274.570, last −272.2484 between
  −272.2150 and −272.2817). The outlier is the start's place in the step grid, not the infall's energy;
- at `89a0760` the starts of infalls 1959–1963 (2197) all lie 11.6 e-folds below their second calls (ln E ≈ −301.78
  against −290.15), at 208–212 quanta, and then move to −297.96 at infall 1964; no start is an outlier.

So the history before 03 came within 208 quanta (2197) and 1258 quanta (2188) of the limit, at near-apex infall
starts, by the same mechanism; F3, by changing every first step after a reflection, moves the step grid of every
flight, and so where the first π < 0 step start falls against each apex. On this tree one start of 2197 and one of 2188
fall within 1.6 quanta. No candidate's energy contradicts the cascade before it.

### 3.5 The nine, on the fixed tree (prompt §1.2 (d))

Tuned as the 42 are, the three at α = 10⁻³ first (ruling 2): `batch --jobs 4 --timeout 3600`, detached (pid 69142),
launched 22:46:25 UTC, `** scan done` at 22:48:35; no timeout. `close/compare.py nine` (`close/out/the-nine.txt`) and
`close/nine_margins.py` (`close/out/nine_margins.txt`, each failure's call made alone, and its margin):

| store | β | M/M_P | α | outcome | wall | φ/M_P at the failure | ln E | root (quanta) | stored ln E, root |
|---|---|---|---|---|---|---|---|---|---|
| shard0002#1592 | 1.1775 | 4×10⁻³¹ | 0.001 | failure | 128.1 s | 2.92669×10⁻⁷ | −305.3632323353481 | 5.8473 | −308.613, 0.2268 |
| shard0003#2208 | 1.18 | 4×10⁻³¹ | 0.001 | failure | 128.6 s | 2.93456×10⁻⁷ | −306.87941516628615 | 1.2838 | −309.375, 0.1058 |
| shard0004#2199 | 1.1825 | 4×10⁻³¹ | 0.001 | failure | 129.1 s | 2.93892×10⁻⁷ | −305.2276525761867 | 6.6963 | −308.659, 0.2167 |
| shard0000#1001 | 0.75 | 4×10⁻³¹ | 0.03 | failure | 25.4 s | 8.56511×10⁻⁸ | −306.3563620503721 | 2.1659 | −304.324, 16.5357 |
| shard0001#766 | 0.9 | 4×10⁻³¹ | 0.02 | failure | 29.6 s | 1.79717×10⁻⁷ | −307.5556578621605 | 0.6528 | −306.299, 2.2948 |
| shard0015#403 | 0.5 | 4×10⁻³¹ | 0.1 | failure | 10.5 s | 6.90946×10⁻⁸ | −305.33592552406515 | 6.0091 | −308.853, 0.1784 |
| shard0000#2735 | 0.75 | 10⁻³³ | 0.03 | failure | 23.9 s | 8.56511×10⁻⁸ | −306.3567396216211 | 2.1651 | −305.916, 3.3646 |
| shard0015#2480 | 0.5 | 10⁻³³ | 0.1 | failure | 11.0 s | 6.90946×10⁻⁸ | −305.28832358387046 | 6.3021 | −308.244, 0.3280 |
| shard0015#2386 | 0.5 | 10⁻³⁰ | 0.15 | **success** | 26.4 s | — | — | — | −306.818, 1.3646 |

Every failure is `classical_turning_point` "not monotone", in the tuning's seed integration (one integration, at the
seed normalization −276.63072859401797), each call raising alone with the same whole message, at a new φ and ln E: F3
moved each history, and eight of the nine meet the same limit elsewhere, within 0.65–6.70 quanta. 2386 now succeeds,
tuned in three integrations (normalization −276.63101193186736; 15 860 steps, 91 reflections, no retake). No
signature step and no retake in any of the nine. The three at α = 10⁻³ took about 128 s; in the run they failed about
30 minutes into their batch, under the cluster's load (orchestrator note 04, §0).

### 3.6 The verdict (prompt §1.2)

**(ii): 03 moves a correct trajectory onto a limit that was already there.**
- The failing call raises on its own inputs at both trees, and the wall search is the same at every tree since the
  store's (§3.4).
- Its root lies within the wall search's rounding width: 1.51 and 1.58 quanta, against `WALL_ROUNDING_QUANTA` = 16
  (ruling 7's bound 17).
- The store's nine show the same limit before 03 (built at `75208cd`): roots 0.11–16.54 quanta, each call raising
  alone (§3.4); on the fixed tree eight of them meet it again, at 0.65–6.70 quanta (§3.5).
- Nothing of (i) is found: no signature step kept or discarded and no retake in either history on either tree (§3.2,
  §3.3); with F3 off the history is `89a0760`'s bit for bit (§3.3), so the restarts' first steps are F3's, as the code
  intends; and the failing candidate's energy is the kinetic energy at an infall's first step start, near an apex,
  while the infall energies run on smoothly (§3.4).

**The limit.** A root of V − V_floor = E below `WALL_ROUNDING_QUANTA` + 1 quanta of ln V's rounding lies in the region
that `_log_V_minus_floor` treats as −∞, and `classical_turning_point` cannot find it: it raises, "not monotone" or
"the outer end is still −∞". At laboratory M (ln V_floor = −276.63) that is E/V_floor below about 9.7×10⁻¹³. The step
loop asks for it at every step start with π < 0, so it is met wherever such a start falls close enough to the apex of a
plateau turn. Which histories meet it depends on the step grid, and F3 changes the step grid of every history that
reflects. Not fixed (prompt §2).

**Proposed destination, for the user to rule:** `one-loop-drain` 01, as the planner proposed. It lands the drain law in
the step loop on the merged tree, keeps the fixed α as a selectable law under which these histories run, and already
owns the related inside-wall stop of 3873; and the rebuild of record (its 08) runs at laboratory M, where the limit is
met. The fix is not this prompt's to choose; three forms the measurements point to:
- a model hook for ln(V − V_floor) in closed form, which for the exponential is normalization + ln expm1((M/φ)ⁿ), so
  that the difference is never formed from ln V and the root is found at any E;
- or, in the step loop, no assessment where E/V_floor is below the rounding width: such a start is far from the wall
  (φ/M ≈ 7×10²³ at 2197's and 2188's failures), where `unresolved_turnaround` decides nothing;
- or, in the search, a root inside the −∞ region reported (as the region's edge, or as "not resolved") rather than
  raised.

### 3.7 The Close gate (README §2.1; prompt §1.5)

1. **The 42 recomputed** and each outcome stated (§3.1): 41 succeed, 1 fails (4389, the effective minimum not found,
   opened as an issue); no `log_m_eff_squared` failure; no signature step kept or discarded; no retake. **Holds.**
2. **The issue closed**: `[run-2026.8.0-an-uncapped-outward-step-after-a-reflection-crosses-the-wall]` on the board's §4,
   with a Closed pointer on the `inelastic-reflection` board's entry, and its index row deleted. **Holds.**
3. **The handover written**: the run record's README ("Failed histories" and its status), `fig6b/PLAN.md` §3,
   `review-remediation-verification.md` §4.19, the diagnosis README's pointer, and this section. **Holds.**
4. **The fixed-α question answered**: verdict (ii), with the evidence of §3.2–§3.5, the issue narrowed and its
   destination proposed. **Holds.**
