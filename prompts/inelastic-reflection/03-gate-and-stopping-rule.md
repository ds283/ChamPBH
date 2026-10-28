# Prompt 03 — the gate and the stopping rule

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-28, after 02 landed (`fe2a944`) and was reviewed (`bc5314e`), from log 02 §8
("State handed to the next prompt"). The planner's probe (§0) answered the charter's first question
before dispatch, and the user ruled on its result the same day (§0.2). If an orchestrator note is
written for this prompt at dispatch, it governs where it differs from this file.
**Gate:**
- on branch `inelastic-reflection`, at or after the commit that adds this prompt, with no tracked
  change in `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. It sets the fraction every drain takes, at a stopping rule whose
bound must be proved, and the loop must stay bitwise at α = 0.

**Read first:**

1. [`README.md`](README.md):
   - §0.2 (the step loop);
   - §1;
   - §2.2 (03's charter, and 04's, which reads this record);
   - §2.3 (the gate "Taper");
   - §4 (02 → 03, 03 → 04);
   - §5;
   - §6.1: U4 as ruled (b′), U5, and the ruling of 𝒜₀;
   - §6.2: P4, P6, P11, P12 and P17.
2. [`source/scoping_2026-10-27.md`](source/scoping_2026-10-27.md) §5 ("Which turning points") and
   §6 (the stopping rule).
3. [`logs/02-injection-at-turning-points.md`](logs/02-injection-at-turning-points.md): §1, §2 and §8.
   The records, `alpha_effective()`, the drain point and the dropped-drain rule are 02's hand-over
   to this prompt.
4. `ComputeTargets/ScalarHistory.py`, at `bc5314e`:
   - `TURNING_POINT_INTERIOR_SAMPLES` (:86);
   - `TurningPoint`, `Injection` and `IntegrationResult` (:149–220);
   - `integrate_scalar_history` (:682–1150): `alpha_effective` (:829), `drain` (:839),
     `observe_turning_points` (:887), the floor branch and the drain point;
   - `integrate_history` (:1509) and its payload (:1792);
   - `_tuning_state` (:1967) and `tune_history` (:2056).
5. `ComputeTargets/AdiabaticHistory.py`, `AdiabaticComputePolicy.M2eff_over_H2` (:145). It is the
   *perturbation's* M²_eff, with the source response and the gravitational mass. It is **not** the
   m_eff of this prompt (§1.1); read it so as not to reuse it.
6. `ComputeTargets/tests/test_injection.py`, all of it, and the P1 harness of
   `test_kinematic_cap_loop.py` (:216–280).
7. `.documents/numerical-strategies.md` §6.2 and `.documents/architecture-summary.md` §7.2: the
   notes 02 added.

---

## 0. Measured at planning

### 0.1 The probe

The planner ran a read-only probe at `bc5314e`:
[`source/planning-probes-2026-10-28b/nonadiabaticity.py`](source/planning-probes-2026-10-28b/README.md).
It used 02's setup: M = 0.5 M_P, n = 1, untuned at the seed realization, φ* = 5 M_P, π* = 0,
T* = 2×10⁴ GeV, to T_CMB. It computed, post hoc on 33 points of every step's interpolant:
- m² = V″(φ) + (d ln A/dφ)² ρ_R,E (Σ + f_m);
- 𝒜 = H_E |d ln m²/dN| / 2m;
- 𝒜_k, the maximum of 𝒜 over the half-oscillation (N_{k−1}, N_k] that ends at turning point k.

At α = 0:

| β | wall-side | 𝒜_k at wall-side points: min / median / max | 𝒜_k ≥ 1 | 𝒜_k < 0.1 | Σ w_gate at 𝒜₀ = 1 |
|---|---|---|---|---|---|
| 1.2 | 9 | 2.2e-2 / 196 / 2.3e3 | 5 | 2 | 5.3 |
| 2 | 23 | 1.6 / 19 / 1.4e3 | 23 | 0 | 21.6 |
| 5 | 141 | 1.3e-3 / 0.94 / 8.5e2 | 64 | 23 | 69.1 |
| 25 | 353 | 8.6e-4 / 0.078 / 1.6e2 | 67 | 195 | 71.4 |

What it shows:
- **𝒜_k falls steadily through each history**, from the first, hard bounce (10²–10³) to the last
  (10⁻³–10⁻²). **There is no gap** between "wall bounces" and "small oscillations about the
  minimum". The late oscillations are not harmonic: V″ ∝ e^{M/φ} changes by e⁸ over an
  oscillation of 13% in φ, so they modulate the mass at the rate it oscillates.
- **𝒜 is large where m is small.** The maximum over a half-oscillation is set on its plateau side,
  so a wall-side point and the plateau turnaround after it get nearly the same value: 2.27e3 and
  2.26e3 at β = 1.2's first bounce. The half-oscillation in and the one out cross the same plateau.
  Restricting the samples to m ≥ H changes nothing at the wall points; m ≥ 10 H lowers the early
  ones by about 4. So 𝒜_k says nothing about the plateau turnaround itself. **m_eff/H there does**:
  it runs from 0.13 to 520, below 1 at 11 of 522 plateau points.
- **𝒜 ≫ 1 at M = 0.5 M_P does not contradict a "classical" bounce.** The produced energy is of
  order m⁴, which is tiny against ρ_kin at that M. That is why α there is a sensitivity parameter
  (U2).
- **The near-stall pair** that 02 found, at β = 5, α = 0.1 (N = 42.889 and 42.893), has
  𝒜_k = 1.6e-4 at its wall-side point (`nonadiabaticity_alpha0.1_beta5.out`), the smallest in that
  history. The gate takes it out.

### 0.2 The ruling it led to

README §2.1 makes a measurement that "does not separate wall bounces from small oscillations" a stop
for the user. The planner put that to the user on 2026-10-28, with three options: 𝒜₀ = 1, no gate,
or a wider probe first. **The user chose "𝒜₀ = 1"**: the physical threshold |ṁ|/m² = 1, below which
production is adiabatically suppressed. So 𝒜₀ is not this prompt's to choose. This prompt
**measures** 𝒜_k on its own implementation and checks it against a reference (§1.2), and does not
re-fit 𝒜₀.

### 0.3 A correction to the stopping rule's sum (P17)

U5 as ruled defines S_k as "the trailing-e-fold sum S of Δρ/ρ_R". If S counts the drains as made,
after the taper, the bound that U5 states (the omitted ΔN_eff ≲ 200ε) fails in the Zeno case U5 was
designed for.
- **The counterexample.** Take 1 000 candidates of Δρ/ρ_R = 10⁻⁵ in one e-fold, with ε = 10⁻⁴.
  - The first has S = 10⁻⁵, w_rate ≈ 0.01, and drains 10⁻⁷.
  - Each later one sees S ≈ 10⁻⁵ again, and drains almost nothing.
  - So 10⁻² per e-fold is left out, 100ε.
- **The fix (P17).** S counts each candidate at its gated, untapered size,
  c_j = α · w_gate,j · ρ_kin,j/ρ_R,E,j, whether or not the taper then reduced it.
  - In the example S passes ε after 10 candidates, and the rest drain in full.
  - In any window of one e-fold, the omitted part is then at most (π/2)ε (§1.4).
- **Why it is the planner's to make.** It is a clarification of "the planner's version" that the
  user ruled ("use your version"), made so that the version delivers its own stated bound. It is
  recorded as P17 and stands unless the user rules otherwise.

---

## 1. What is wanted

At α > 0, every drain point and every floor reflection becomes a **candidate**.
- Its effective fraction is α_eff = α · w_gate · w_rate.
- w_gate is a smooth weight on the bounce's non-adiabaticity 𝒜_k.
- w_rate is a smooth weight on the rate of injection over the trailing e-fold.
- Both are computed at α > 0 only. At α = 0 nothing in this prompt runs, and every value stays the
  old code's bitwise.

### 1.1 The effective mass and the non-adiabaticity

- **The mass** is the local mass of a chameleon quantum, the one 04 uses for m_ta:

  m_eff² = V″(φ) + [(ln A)″ + (ln A)′²] ρ_R,E (Σ + f_m),

  with:
  - V″ = V[(ln V)″ + (ln V)′²], from the realized model's `log_V`, `d_logV_dphi` and
    `d2_logV_dphi2`;
  - (ln A)′ and (ln A)″ from `d_logOmega_dphi` and `d2_logOmega_dphi2`;
  - Σ = 1 − 3w(T_J) from the cosmology, at T_J = exp(ln T_J) of the state.

  ρ_R,E(Σ + f_m) is the Einstein-frame trace that `ODERHS` couples to. The log derives this form
  from V_eff = V + A ρ̃_c with ρ̃_c conserved. It also says how this m_eff differs from
  `AdiabaticComputePolicy.M2eff_over_H2`, which carries the source's response and −(2 + Ḣ/H²).
  - m_eff² > 0 for this model: V is convex for φ > 0 and Σ ≥ 0. A sample with m_eff² ≤ 0 is a
    `ComputationFailureError` naming the state, and a stop for this prompt if it is ever seen.
- **The non-adiabaticity** at a point is 𝒜 = |dm_eff/dt| / m_eff² = H_E |d ln m_eff²/dN| / (2 m_eff).
  - **𝒜_k** is its maximum over the half-oscillation that ends at the candidate.
  - **A half-oscillation** runs from the previous boundary to the turning point (or floor
    reflection). A boundary is a turning point of either kind, a floor reflection, or N_start.
- **In the loop**, at α > 0 only, on every appended step:
  - Take the step's ten samples: the two ends from 02's scalar calls, and the 8 interior points.
    Read them as full states.
  - Compute ln m_eff² and H_E at each. d ln m_eff²/dN is `np.gradient` over the ten, within the
    step. Nothing differences across a step boundary.
  - Fold the step's 𝒜 into a running maximum. Samples at or before a turning point found on the step
    go into the half-oscillation that ends there; samples after it start the next.
  - At a floor reflection, the half-oscillation ends at the reflection's N, and the next starts
    there.
- **H_E at the samples.** By default, H_E at each sample comes from `HubblePolicy` on `ODEPolicy`'s
  data with the ledger, as the probe does. If that more than doubles a history's wall time at
  α = 0.1 (§3.4), you may take ln H_E linear in N between the step's two ends instead. That is two
  policy calls per step. You must:
  - state the error bound, from |d ln H/dN| ≤ 3 and the step length;
  - show that the test of §1.6 item 3 still passes;
  - classify the choice.
- **At every TurningPoint**, at α > 0, also record m_eff/H_E at the turning point itself, from
  its state.

### 1.2 The gate

- **w_gate = 𝒜_k² / (𝒜_k² + 𝒜₀²)**, with 𝒜₀ = 1, a module constant
  `NONADIABATICITY_SCALE = 1.0` in `ComputeTargets/ScalarHistory.py` (P6). The user ruled the value
  (§0.2).
- **It applies at every candidate:** the drain point after a wall-side turning point (its 𝒜_k is that
  turning point's), and a floor reflection (its 𝒜_k is the infall's, up to the reflection).
  - Under the floor rule the wall is thinner than a representable step, so 𝒜_k is a lower bound
    there.
  - Report the values the floor harness gives (§1.6 item 8).
- **Plateau turnarounds still never drain** (U4 (b′)). They get 𝒜_k and m_eff/H on their
  `TurningPoint`, as the diagnostic U4 (b′) asks for, and nothing else.

### 1.3 The stopping rule

At a candidate at N, with ρ_kin,E = ½H_E²π² (02's, with the ledger before the drain) and ρ_R,E from
the state:
- **c = α · w_gate · ρ_kin,E/ρ_R,E**, the candidate's gated, untapered size (P17).
- **S = c + Σ c_j** over the earlier candidates j with N − 1 < N_j ≤ N (`RATE_WINDOW_EFOLDS = 1.0`,
  in N_E). Every earlier candidate counts at its own c_j, whatever its taper did.
- **w_rate = S² / (S² + ε²)**, with ε = `RATE_EPSILON = 1e-4`. This is 1/(1 + (ε/S)²) written so
  that S = 0 gives 0 rather than a division by zero.
- **α_eff = α · w_gate · w_rate**, set in `alpha_effective()`, the one place 02 left for it. Its
  signature grows to what it needs.
- **If α_eff == 0.0** (only through underflow): there is no ledger entry, no change to π and no
  restart. The candidate is still recorded (§1.5).

"Re-armed by construction": a kick that re-energises the condensate makes c large, so S ≥ ε and
w_rate ≈ 1, however quiet the e-fold before it was.

### 1.4 The omitted fraction and its bound

- **The omitted part of candidate j** is o_j = c_j (1 − w_rate,j) = c_j ε²/(S_j² + ε²). It is the
  energy the taper left in the condensate. The gate's reduction α(1 − w_gate)ρ_kin/ρ_R is physics,
  not an error, and is not counted.
- **The bound.** For any interval I of one e-fold, Σ_{j ∈ I} o_j ≤ (π/2) ε.
  - Every earlier candidate of I is inside the trailing window of each later one, so S_j ≥ s_j, the
    running sum of the c over I up to j.
  - Then Σ o_j ≤ ∫₀^∞ ε²/(s² + ε²) ds = (π/2) ε.
  - Over a history whose candidates span ΔN e-folds, the total is at most (π/2) ε ⌈ΔN⌉.

  Check this in the log in a few lines, including the step from the sum to the integral (each o_j is
  at most the integral over [s_{j−1}, s_j]).
- **The bound is a test** (§1.6 item 5), and the measured total is reported against it (§2).

### 1.5 The records

- **`TurningPoint`** gains, appended last:
  - `nonadiabaticity` (𝒜_k over the half-oscillation that ends at it);
  - `m_eff_over_H` (at the turning point).

  Both are `None` at α = 0.
- **`Injection`** is now **one per candidate at α > 0**, α_eff = 0.0 included. It gains, appended
  last:
  - `nonadiabaticity` (𝒜_k), `w_gate`, `w_rate`;
  - `rate_sum` (S);
  - `candidate_over_rho_rad` (c);
  - `omitted_over_rho_rad` (o).

  These are P4's 𝒜_k, w_gate and w_rate, and the quantities the bound and 05's
  `omitted_fraction` need. The other P4 fields (λ, k_ex, m_ta, ln q) are 04's. Add no placeholders.
  - `alpha_eff` is α · w_gate · w_rate, computed in that order.
  - `delta_K` and `delta_rho_over_rho_rad` are those of α_eff, and 0.0 when α_eff == 0.0.
- **`IntegrationResult`** gains `dropped_drains`, appended last: the armed drains that never reached
  a drain point. Log 02 §8 says the subtraction stops being the count once a weight can be zero.
  The payload carries `"dropped_drains"` beside `"injections"`. Nothing on the way to the store reads
  any of it; check `store`, `build_extra_data`, `golden_history.record_of` and
  `TUNING_OUTPUT_KEYS` again.
- **The ledger** holds exactly the candidates with α_eff > 0.0.

### 1.6 The tests

Put the new tests in `ComputeTargets/tests/test_gate_and_taper.py`, new. Give each one's cost in
its docstring. Make the weights, the window sum and the omitted part module-level functions of
`ComputeTargets/ScalarHistory.py`, so that they can be tested on synthetic sequences without an
integration. The loop calls the same functions.

1. **α = 0 is the old code.** `test_golden_history.py`, the tuned golden test and
   `test_first_bounce.py` pass without edits. On the golden β = 1.5 history at α = 0:
   - every `TurningPoint` has `nonadiabaticity` and `m_eff_over_H` `None`;
   - the mass function is not called (patch it).
2. **The weights against hand values.**
   - w_gate at 𝒜 = 0, 10⁻³, 1, 10 and 10³.
   - w_rate at S = 0, ε/10, ε and 10ε.
   - The window at its edges: a candidate at exactly N − 1 is out, and one at N is in.
   - S on a synthetic sequence, computed by hand.
3. **𝒜_k against a reference.** On a real history at α = 0.1 (M = 0.5, β = 2 serves), the test
   recomputes 𝒜_k post hoc at every wall-side turning point, as the probe does: 33 points per step,
   with H_E at each. The loop's w_gate must agree with the reference's to 0.05 absolute at each.
   Since dw/d ln 𝒜 ≤ 1/2, that is a 10% tolerance on 𝒜 where the gate is sensitive. The test also
   reports the largest relative difference in 𝒜_k. Plateau points are checked the same way.
4. **The Zeno sequence.** 1 000 synthetic candidates of 10⁻⁵ in one e-fold:
   - the drains resume once S ≥ ε;
   - the omitted total is at most (π/2)ε;
   - the S of drains as made (the breakage B2) would omit nearly all of it.
5. **The bound on a long sequence.** A pseudo-random synthetic sequence (fixed seed) over 25 e-folds,
   with c spanning 10⁻⁸ to 10⁻², has its omitted total within any one-e-fold interval at most (π/2)ε,
   and its total at most (π/2)ε ⌈ΔN⌉.
6. **Re-arming.** A quiet e-fold of candidates at 10⁻⁷, then one at 10⁻³. The last has w_rate within
   10⁻² of 1.
7. **The record on a real history** (the same β = 2 history at α = 0.1):
   - α_eff = α · w_gate · w_rate exactly, as floats computed in that order;
   - o = c(1 − w_rate) to a stated rounding bound;
   - c, S and the window recomputed from the records;
   - the ledger's injections are exactly the records with α_eff > 0;
   - `dropped_drains` equals the count from the records.
8. **A floor-rule candidate.** The P1 harness at M = 10⁻¹⁰, with a ledger and α = 0.1. The record
   carries 𝒜_k, w_gate, w_rate and S; α_eff and the ledger agree; π_out = −√(1 − α_eff) π_in exactly.
   Report the w_gate the floor gets.
9. **The near-stall.** At β = 5, α = 0.1, M = 0.5, every wall-side turning point with
   𝒜_k < 10⁻² has w_gate < 10⁻⁴.
   - If that history still holds an interior pair under the gate, assert that the pair's wall-side
     point is among them.
   - If not, say so in the test's docstring and the log.

**The tests of `test_injection.py`** assert α_eff = α in items 2 and 5 and in the floor test. They are
modified, because 03 changes what they test (README §5):
- α → `i.alpha_eff` wherever a drain's fraction is checked;
- "one injection per wall-side turning point" stays, since every candidate is now a record.

List each edit in the log with its reason. Item 11 needs an interior plateau-and-wall pair at β = 5,
α = 0.1. If the gated trajectory no longer holds one, rebuild the test on a history that does.
Record the search, keep its B9 failing, and do not drop the test. If none can be found in a few
integrations, stop and ask.

**Deliberate breakages** (P11). Each must fail a named test. Record each as a diff in the log; never
commit one.
- **B1** w_gate = 1;
- **B2** S summed over the drains as made, α_eff ρ_kin/ρ_R, instead of c;
- **B3** the candidate left out of S;
- **B4** the window's lower edge included (N_j ≥ N − 1);
- **B5** 𝒜 without H_E (|d ln m/dN|/m);
- **B6** the running maximum not reset at a turning point;
- **B7** α_eff = α · w_gate (no w_rate);
- **B8** the 𝒜 computation run at α = 0 too. The golden histories must still pass, and item 1's
  patch must fail. Report B8's cost.

---

## 2. Measurement

On 02's setup (untuned, at the model's seed realization), for β ∈ {1.2, 2, 5, 25}, at α = 0.1. Run it
from your scratch space, never committed. Give per history:
- the wall-side and plateau turning points, and the candidates by kind;
- 𝒜_k at wall-side points (min, median, max), and the counts with w_gate < 0.01, between 0.01 and
  0.99, and > 0.99;
- the counts with w_rate < 0.5;
- m_eff/H at plateau turnarounds (min, median, max), and the count below 1;
- Σ c, Σ (α_eff ρ_kin/ρ_R) (injected), Σ o (omitted) against (π/2)ε⌈ΔN⌉;
- ρ_χ/ρ_R at T_J = 1 MeV, at 70 keV and at the end, against 02's (log 02 §5);
- dropped drains;
- accepted steps, RHS evaluations, restarts and wall time, against 02's;
- the 17-point check of 02 (`17>loop`), which must stay 0.

Then:
1. **The tuning.** `tune_history` at β = 2 and at β = 5, α = 0.1, from the model's seed: the
   iterations, the residuals, and whether each converges. Not converging is a measurement, not a
   stop.
2. **The residual's continuity** (the charter's measurement). For β = 2 and β = 5 at α = 0.1:
   - take the converged normalization n* and φ_ref of item 1, or the seed's if it did not converge;
   - integrate at 21 normalizations evenly spaced over an interval around n* on which residual_H
     spans about ±10⁻³, with φ_ref fixed;
   - compute residual_H by `_tuning_state`.
   - Do the same at α = 0, and at α = 0.1 with α_eff = α (02's rule) in a scratch-only patch that is
     never committed. Those are the baseline and the comparison.
   - Report, for each scan, the largest deviation from a least-squares cubic, in units of the
     tuning tolerance 10⁻⁵, and where on the scan the turning-point or candidate count changes.
   - Keep each scan inside the run-length rule (README §5): a scan of 21 integrations at a few
     seconds each.

## 3. Verification

1. **Before any edit:**
   - the five suites, run as `CLAUDE.md` says, with ComputeTargets in its two halves;
   - the two golden tests alone, with their times.

   Expect 26 / 305 (96 + 209) / 103 / 115 / 64 (board §5.2). `CLAUDE.md`'s ComputeTargets counts are
   stale until 08. A different count, or a failure, is a stop.
2. **After:** the five suites. ComputeTargets rises by the new tests and nothing else falls. Then
   `black --check` on the files you changed.
3. **The breakages** of §1.6, each with the tests it fails.
4. **The cost.**
   - At α = 0: the golden tests' wall time before and after, back to back, three times each, with
     the minima and the load. It should not move, since nothing runs; say whether it did.
   - At α = 0.1: the cost of the 𝒜 computation per accepted step, and the four histories' wall time
     against 02's.

## 4. Acceptance

1. §1.1–§1.6 as written, and the gate "Taper" (README §2.3):
   - each weight equals its hand value;
   - the tuning residual is continuous across a threshold on a real history: §2's scan at α = 0.1
     deviates from its cubic by less than a tenth of the tuning tolerance, and by no more than the
     α = 0 baseline's order;
   - the omitted fraction is within its bound.
2. **The measurement** of §2, as tables in the log.
3. **The log**, `logs/03-gate-and-stopping-rule.md`, in the template of `dk-and-rpk-migration`
   README §5.1. It holds:
   - the form of m_eff and how it differs from `M2eff_over_H2`;
   - the bound of §1.4, checked;
   - the suite counts before and after;
   - the breakages as diffs;
   - the measurement;
   - every deviation classified, and every edit to `test_injection.py` with its reason;
   - "State handed to the next prompt". This covers the final records, which candidates count as
     generations for 04 (those with α_eff > 0), the m_eff function 04 reuses, and the cost.
4. **The documents** (P12): dated notes in `.documents/numerical-strategies.md` (the gate, the
   rate and its bound) and `.documents/architecture-summary.md` (the records and the payload key).
5. **The board:**
   - row 03 and the gate Taper, in §1 and §2;
   - `### 5.3 After prompt 03 (<date>)` in §5;
   - §3 and §4 for anything opened, with `.documents/OPEN_ISSUES.md` in step.
6. **One commit**, in `CLAUDE.md`'s form, staged by explicit path only.

## 5. Stop conditions: stop and ask

- A golden history is not bitwise at α = 0, and the cause is not a mistake of yours you can see and
  undo.
- The loop's w_gate differs from the reference's by more than 0.05 at any wall-side point (§1.6
  item 3), with both computations done as written.
- A sample has m_eff² ≤ 0.
- The omitted total exceeds the bound on a real history.
- §2's scan at α = 0.1 deviates from its cubic by a tenth of the tuning tolerance or more, while the
  α = 0 baseline does not.
- 02's 17-point check (`17>loop`) is nonzero on any history of §2.
- No history in a few integrations holds an interior pair for item 11.
- An existing test fails for a reason this prompt does not name.
- A baseline count differs from §3.1 before you edit.

## 6. What this prompt does not do

- **No margin** (04): no λ, k_ex, ln q or m_ta field. The m_eff function is written so that 04 can
  call it.
- **No store, key or column** (05), and **no option** (06). `compute_scalar_history`, `main.py`, the
  plot scripts and the tools are unchanged.
- **`AdiabaticHistory` is unchanged** (05).
- **𝒜₀ is not re-fitted.** It is the user's (§0.2). If the measurement suggests another value,
  record it under "Observations not acted on".
- **Files it changes:**
  - `ComputeTargets/ScalarHistory.py`;
  - `ComputeTargets/tests/test_gate_and_taper.py` (new);
  - `ComputeTargets/tests/test_injection.py`, for the edits §1.6 lists;
  - the two `.documents/` files;
  - the log, the board and, if anything is opened, `.documents/OPEN_ISSUES.md`.

  Any other file is a deviation to classify.
