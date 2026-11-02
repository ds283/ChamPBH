# The m_eff² failures of the 2026.8.0 run — diagnosed 2026-11-02

A diagnosis made at the user's request on 2026-11-02, outside any campaign, on `main` at `7b35dc8`
(SciPy 1.17.0). It explains the 42 `log_m_eff_squared` failures of the 2026.8.0 science run
([`science-runs/2026-11-01-science-2026.8.0/README.md`](../../science-runs/2026-11-01-science-2026.8.0/README.md),
"Failed histories": 8 at β ≤ 1.1074 and 34 above it, all in block C), which that record leaves
undiagnosed. It opens
`[run-2026.8.0-an-uncapped-outward-step-after-a-reflection-crosses-the-wall]` on the
`inelastic-reflection` board (§3). The fix is left to a campaign; nothing here changes the code.

**In one paragraph.** After a floor reflection late in a dissipation-mode history, the outward step
can be uncapped. The kinematic cap limits only inward motion, and its acceleration term applies only
where the net force points inward. Where the reflection happens close enough to the wall that the
wall's push already beats the matter pull, the cap falls back to `global_max_step` (0.1). On such a
restart Radau sometimes takes a step 40–150× longer than any good one, spanning about 10³
radians of the local oscillation (h·ω), and accepts it. The accepted endpoint is wrong: the field
stops near the wall, where it should fly outward. The step's interpolant swings through φ < 0
between its endpoints, and at α > 0 the gate evaluates m_eff² on that interpolant
(`observe_turning_points`, ten samples per step), finds it negative there and fails the history.

## 1. The scripts

Run from the repository root. None needs Ray. The last two patch functions in their own process only
(SciPy's `Radau.step`, the supervisor's `notify_step_cap` and `notify_reflection`, and
`parking_ratio_test`) and run one history each through `tools/history_and_bbn.py`, as `main.py`
tunes it, in 3–25 s.

- [`failures.py`](failures.py) reads the main store, `var/datastores/science-2026.8.0`, read-only
  (sqlite `mode=ro` on each shard): [`out/failures.txt`](out/failures.txt).
- [`trace_step.py`](trace_step.py) `BETA M ALPHA` traces one failing history to its failure, then
  re-integrates the failing step from the same state on the same right-hand side, with the
  oscillation resolved (`max_step` = h/20000): [`out/trace-1.5-1e-12-0.05.txt`](out/trace-1.5-1e-12-0.05.txt),
  [`out/trace-1.14-1e-15-0.01.txt`](out/trace-1.14-1e-15-0.01.txt).
- [`scan_first_steps.py`](scan_first_steps.py) `BETA M ALPHA` lists every first step of a new Radau
  solver that starts outward (π > 0), the step after a reflection or a drain:
  [`out/scan_first_steps.txt`](out/scan_first_steps.txt), nine histories.

## 2. What the store holds

[`out/failures.txt`](out/failures.txt).

- **No samples.** A failed history is stored without samples (`ScalarHistory`'s failure branch clears
  `_values`, `ComputeTargets/ScalarHistory.py:5263`), so none of the 42 has a `ScalarHistoryValue` row.
  No sample in the whole store has φ_E < 0.
- **The reason is truncated** to 256 characters (`DEFAULT_STRING_LENGTH`), which cuts off the tuning
  integration's number. It keeps the state: φ_E, ln ρ_R,E, ln f_m, Σ and the model's normalization.
- **The failures are at one epoch for each M.** Matching each failure's ln ρ_R,E to a successful history
  at the same (M, β) gives T_J = 0.78–0.92 eV at M = 10⁻¹⁵ (10 failures), 3.5 eV at 10⁻¹³ (1) and
  10–28 eV at 10⁻¹² (31), whatever β (1.08–2.6) and α (0.01–0.15). φ_E/M at the failure is −0.5 to −4
  at 10⁻¹² and −30 to −800 at 10⁻¹⁵.
- The successful histories at the same (M, β) converged in their seed integration (`tuning_iterations`
  1). The reproduction (§3) fails in the seed too ("tuning integration 0").

## 3. One failure, step by step

β = 1.5, M = 10⁻¹² M_P, α = 0.05 ([`out/trace-1.5-1e-12-0.05.txt`](out/trace-1.5-1e-12-0.05.txt)). It
reproduces the stored failure to every printed digit (φ_E = −3.792756377×10⁻¹²) in 3.5 s.

1. **The infall.** The field falls toward the wall in capped steps of 1.0–1.8×10⁻¹¹ e-folds, each
   shrinking φ by about 10 %. The classical turning point is φ_t = 4.17×10⁻¹⁴ M_P = 0.042 M.
   `unresolved_turnaround` returns False down to φ = 0.237 M (it fires below 3φ_t = 0.125 M).
2. **The reflection.** At N = 35.914291, φ = 0.237 M, the floor rule fires (the cap would fall below
   h_floor = 10⁻¹¹). π goes from −0.0024974 to +0.0024342, drained by α (√0.95). The parking cut's ratio
   test fails, at ρ_kin/ρ_R = 1.074×10⁻⁶ against 10⁻⁶. It was 1.190 and 1.131×10⁻⁶ at the two
   reflections before, so the field would have parked within a few more bounces.
3. **The uncapped step.** A new Radau starts from the reflected state. Its inward acceleration −π′ is
   −0.0157: the wall pushes outward more strongly than the matter pulls inward. With π > 0 and −π′ < 0
   neither term of the kinematic cap applies, and the cap is `global_max_step` = 0.1. Radau's first
   h_abs is 3.2×10⁻³, and it takes a step of h = 6.37×10⁻⁴.
4. **The accepted endpoint is wrong.** Radau ends the step at φ = 6.2×10⁻¹² M_P (6.2 M) with
   π = 1.6×10⁻⁷. The same step re-integrated from the same state with the oscillation resolved ends at
   φ = 1.52×10⁻⁶ M_P with π = 0.00235: the field flies outward, π never changes sign, and φ never
   falls below its start. The other components agree to 2×10⁻⁶.
5. **The failure.** Radau's dense output over the step runs, in units of M, 0.237, 6.36, 8.93, 8.76,
   6.67, 3.47, −0.04, −3.04, −4.73, −4.29, −0.91, 6.21. The gate's sixth interior sample is at
   φ = −3.79 M, where m_eff² < 0, and `log_m_eff_squared` raises.

At M = 10⁻¹⁵, β = 1.14, α = 0.01 ([`out/trace-1.14-1e-15-0.01.txt`](out/trace-1.14-1e-15-0.01.txt)) the
failing step is the same in kind. It starts at φ = 162 M with π = 0.00177 and h = 1.6×10⁻⁴. Radau ends it
at 1659 M with π = 1.6×10⁻⁷; resolved, it ends at 2.6×10⁸ M with π = 0.00149. The interpolant reaches
−1030 M.

## 4. Good and bad first steps

[`out/scan_first_steps.txt`](out/scan_first_steps.txt): three failing histories (β = 1.5 at 10⁻¹²;
β = 1.14 and 1.09 at 10⁻¹⁵) and six that succeed (β = 1.5, α = 0.02 and 0.15, and β = 1.09, α = 0.08,
at 10⁻¹²; β = 1.2, α = 0.03 at 10⁻¹⁵; β = 1.5 and 1.2, α = 0.08 at 4×10⁻³¹). Together they have 845 first
outward steps.

> **Note (2026-11-02, `reflection-restart` 01).** The per-history counts in `out/scan_first_steps.txt`
> (30, 270, 151, 25, 32, 62, 174, 36, 55) sum to **835**, not 845. The 845 was a slip in this sentence.
> The counts themselves stand, and the scanner of `reflection-restart` 01 reproduces each of them line for
> line (`.documents/reflection-restart-verification.md` §1.2).

- **In each failing history exactly one is bad** (1 of 30, 1 of 151, 1 of 270), and it is the one that
  fails. Its h is 1.6–6.4×10⁻⁴, its h·ω 1060–1240, and π_end/π_start 7–9×10⁻⁵.
- **Every other step is good**, the six successful histories' included: h ≤ 4.1×10⁻⁶, h·ω ≤ 0.008, and
  π_end/π_start ≥ 0.99992. h·ω is 0 at 4×10⁻³¹, where the wall's force is zero at the reflection point to
  double precision.
- **An uncapped restart is necessary, not sufficient.** Each bad step starts with −π′ < 0 and cap 0.1.
  So do many good ones: 40 of 62 at β = 1.09, 10⁻¹² (cap median 0.1, first h_abs up to 1.8×10⁻³); 45 of
  174 at β = 1.2, 10⁻¹⁵; and 31 and 17 in the two failing histories at 10⁻¹⁵. On those Radau's error test cut the step to ≤ 4×10⁻⁶ before
  accepting one. Wherever the inward acceleration is positive, the acceleration cap held the first step
  to at most 4×10⁻⁶. **Why Radau's error test accepts the bad steps is not established.** A step spanning
  10³ radians of a stiff oscillation is where an L-stable method's error estimate, filtered through
  (I − hγJ)⁻¹, is known to under-read, but this was not tested.
- **Why the failures sit at one epoch** is suggested but not established. Late in a history, π at the
  wall is small and drained further at every reflection, and the parking cut is close (§3 step 2). A
  history that parks first never meets the bad step. One that is still reflecting can, and the race
  would be decided by M and the epoch rather than by α and β.

## 5. What this does and does not reach

- **The 42 failures** are this mechanism wherever it was reproduced: the three histories above. The
  other 39 share its signature (φ_E < 0 at the failure; the same epoch for each M) and are presumed to
  be the same. They were not run.
- **Successful histories.** None of the six scanned has a bad step. A bad step that does not take the
  interpolant below zero would pass silently at α > 0, stopping the field near the wall as a spurious
  parking. At α = 0 the gate does not run and any bad step would pass. The 2026.8.0 run has no α = 0
  history at small M, and its large-M histories do not reflect. A scan of every history at
  10⁻¹⁵–10⁻¹² (1302) would take hours and was not made.
- **Laboratory M.** The two histories scanned at 4×10⁻³¹ have no uncapped restart: −π′ > 0 at every
  one. Figure 6 rests on 4×10⁻³¹.

## 6. For the fix

Not ruled; each changes the stored digits of every history that reflects, so needs a campaign.

1. **Cap outward motion too**, h ≤ fφ/|π| for either sign of π. It is the existing rule made
   symmetric. Leaving the wall would take about ln(φ_end/φ_start)/ln(1 + f) steps per flight
   (about 160 at the failing reflection).
2. **Cap on the local stiffness**, h·√|∂π′/∂φ| ≤ c, from the Jacobian Radau already holds.
3. **Give the restarted solver a first step** (`first_step`) from the inward cap at the reflection, and
   let Radau grow it from there.

Any of them should be checked against the golden histories and against these three failures.
