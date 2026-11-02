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
