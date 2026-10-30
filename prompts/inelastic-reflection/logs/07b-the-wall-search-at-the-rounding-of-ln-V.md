# Log 07b — the wall search at the rounding of ln V

**Subject:** Treat a difference within ln V's rounding as no difference in the wall search
**Commit:** `92f41f2`, on branch `inelastic-reflection`, on top of `c2aac94`
**Date:** 2026-10-30, by Claude Opus 5.5
**Result:** **COMPLETE.** The gate "Band" holds. One issue closed, none opened.
- **The fix is the helper's width.** `_log_V_minus_floor` returns −∞ where
  ln V − ln V_floor ≤ K ulp(max(|ln V|, |ln V_floor|)), with K = `WALL_ROUNDING_QUANTA` = 16, as well as
  where V is at or below V_floor. Elsewhere it returns the shipped expression bitwise. Nothing else in
  the code changes: the search, its constants, the methods, `assess_infall`, G2 and every model are as
  they were. The only other code line is the `ulp` import.
- **K is derived (§1).** It is the smallest integer for which every comparison the search makes
  between two finite values is strict, for any input. The binding comparison is a halving midpoint
  against a finite end 1/16 e-fold away: (e^{1/16} − 1)(K + ½) > 1 gives K > 15.005. The probes' 64
  satisfies it with four times the width needed. K = 1 already fixes every failure observed, because
  each was the doubling's first comparison, but it leaves the halvings unguarded.
- **The gate "Band"** (README §8.4):
  - **α = 0 bitwise.** Natural mode at M = 0.5 M_P, β = 2, and in the band at M = 10⁻¹⁶ M_P, β = 2, is
    bitwise the dispatch commit's, warnings included. The golden tests pass unedited.
  - **Every history outside the band is bitwise 07's.** All 28 of prompt §2.4 match the dispatch commit
    `c2aac94` by `repr`: every record, the assessments, the samples, the final state, `N_final`,
    `nfev`, `ts`, the parking and release records, and the warnings. So do the 12 edge and M = 10⁻²⁷
    histories that also ran at the dispatch commit.
  - **Every history of the band reaches T_CMB.** That is all 30 of β = 1.2, 2, 3 × M = 10⁻¹³ to
    10⁻²⁷ M_P, of which 27 fail at the dispatch commit. Each parks at candidate 6, 48 or 58, at
    N = 28.364, 30.349 or 29.394, as log 06's histories do. Σ Δρ/ρ_R with the leftover equals the
    M = 10⁻³⁰ value within 1.6×10⁻⁸, 3.1×10⁻¹³ and 3.4×10⁻⁸ relative.
  - **K is derived**: §1.
- **For 08** (§5.3): the one-loop chain raises on every candidate with 0.05 ≤ log₁₀ 𝓡 ≤ 2.92 and
  solves every one with log₁₀ 𝓡 ≥ 3.00. The histories that cross the gap are M = 10⁻¹⁵ to 10⁻²⁰ M_P.
  At β = 2, M = 10⁻¹⁶, 59 % of the drained energy is in generations where the chain raises.
- **Counts:** 26 / 427 (96 + 331) / 103 / 115 / 64 before, 26 / **439** (96 + **343**) / 103 / 115 / 64
  after, all passing. The 12 new tests are in `ComputeTargets/tests/test_wall_rounding.py`.
- No store was opened, no Ray started and nothing written to `var/`. No golden JSON was regenerated.

## 1. The physics and the numerics

### 1.1 The rounding of ln V − ln V_floor (ruling 1, the rounding)

Write q = ulp(|ln V_floor|) for the quantum. At this campaign's normalizations (ln V₀ ≈ −276.63, in
the binade [256, 512)) q = 2⁻⁴⁴ = 5.684×10⁻¹⁴. Write u = (M/φ)ⁿ and D = ln V − ln V_floor.
- **The exponential.** ln V = normalization + (M/φ)ⁿ is one division (with a `pow` for n ≠ 1),
  rounded relatively within ε/2, then one sum, rounded to the nearest float. ln V_floor is the
  normalization itself, exactly. While ln V lies in the binade of ln V_floor (D < 20.6 here), the
  computed difference D̂ = ln V − ln V_floor is exact (Sterbenz). It is an integer number j of quanta,
  the nearest to the true u: |D̂ − u| ≤ q/2 + εu/2. At u ≲ 20q the second term is 2×10⁻¹⁵ q. So the
  rounding of the difference is **ρ = ½ quantum**.
- **A generic model** adds two things:
  - its ln V may be rounded by more than one operation, c half-quanta in all;
  - its ln V_floor need not be exact, c_f half-quanta more.

  Then ρ = (c + c_f)/2. D̂ is still an integer number of quanta wherever ln V and ln V_floor share a
  binade. Where they do not, it is finer, which only helps.
- **Ordering.** Two computed differences are strictly ordered, D̂₁ < D̂₂, if the true ones differ by
  more than 2ρ quanta: then D̂₂ ≥ D₂ − ρq > D₁ + ρq ≥ D̂₁. For the exponential that is "more than one
  quantum". The inequality must be strict: with ties to even, 1.5q and 2.5q both round to 2q.
- **From D̂ to the residual.** The helper's ln(V − V_floor) = ln V + log(−expm1(−D̂)) is monotone in
  D̂. At D̂ ≲ 10¹³ q it changes by far more than its own ulp per quantum. So the order of the D̂ is the
  order of the residuals the search compares. ln(V − V_floor) is noisy by about 1/(2j) at j quanta.
- **What a finite value tells.** With the width, a finite value has D̂ > Kq, so D̂ ≥ (K + 1)q. The
  true D is then at least (K + 1 − ρ)q, which is (K + ½)q on the exponential.

### 1.2 The comparisons, and K (ruling 1, the comparisons and the inequality)

What matters is the slope s of ln D = ln(ln V − ln V_floor) in ln φ. Two probes Δx apart in ln φ have
D₂/D₁ = e^{sΔx}. On the exponential D = (M/φ)ⁿ exactly, so s = n at every u. Where D is small, s is
also the slope of ln(V − V_floor), since V − V_floor ≈ V_floor D there (ruling 1's wording). H is
`WALL_SEARCH_HALVINGS` = 4. The search compares two finite values at three kinds of place.
1. **Consecutive doubling probes**, x_out and x_in = x_out − step, with step ≥ 1.
   - If r_out is finite, D₁ ≥ (K + 1 − ρ)q and D₂ ≥ e^{s} D₁. The comparison is strict if
     (e^{s} − 1)(K + 1 − ρ) > 2ρ.
   - If r_out = −∞, the check is r_in > −∞, or −∞ repeated. That always holds.
2. **The first H halvings with the outer end finite.**
   - At the h-th halving (h = 1 … H) the bracket is step/2^{h−1} wide, and the midpoint lies step/2^h
     from each end. The smallest separation is 2^{−H} e-folds (step = 1, h = H).
   - The smallest D compared is the outer end's, which is finite, so D₁ ≥ (K + 1 − ρ)q.
   - The comparison is strict if (e^{s 2^{−H}} − 1)(K + 1 − ρ) > 2ρ.
3. **The halvings at an outer end of −∞** (r_out = −∞, in the first H halvings or the further ones).
   - A midpoint that is −∞ is allowed.
   - A finite midpoint with r_mid < 0 lies below r_in ≥ 0 by sign.
   - The only comparison that can fail is a midpoint past the root (r_mid ≥ 0) against the inner end,
     both past the root.
     - Their separation is half the bracket. The bracket runs from an outer end with D̂ ≤ Kq (so true
       D ≤ (K + ρ)q) to an inner end past the root, so it is at least ln(D_t/((K + ρ)q))/s, with
       D_t = ln(1 + E/V_floor) the root's difference.
     - With D_mid ≥ D_t and t = D_t/((K + ρ)q), the comparison is strict if
       **(K + ρ) t (t^{1/2} − 1) > 2ρ**.

     This is a condition on the root, not on K.

**The inequality on K** comes from (1) and (2). (2) binds, since its separation is the smaller:

    (e^{s 2^{−H}} − 1)(K + 1 − ρ) > 2ρ.

On the exponential, with s = n = 1, ρ = ½ and H = 4: e^{1/16} − 1 = 0.0644945, so K + ½ > 15.5052 and
**K > 15.0052, so K = 16**, the smallest integer that satisfies it. (1) alone gives
K > 1/(e − 1) − ½ = 0.082, so K ≥ 1.
- **The probes' 64 satisfies the inequality**: (e^{1/16} − 1)(64.5) = 4.16 > 1. It is wider than needed
  by a factor of 4, and it enlarges (3)'s unresolvable region by the same factor (§1.3).
- **K = 1 fixes every failure observed**, because each one is at place (1). At the dispatch commit the
  field sits at u₀ ∈ [0.50, 0.55)q (D̂ = 1q), and the first probe, one e-fold in, sits at
  e·u₀ ∈ [1.36, 1.50)q, which also rounds to 1q. This holds on all 31 failing histories of §5.1 and
  §5.2 (D̂ = 1, 1).
  - The shipped helper is in effect K = 0: a finite value needs only D̂ ≥ q, so D ≥ q/2, and
    (e − 1)·½ = 0.86 < 1.
  - At K = 1 the outer end of that comparison is −∞.
  - No band history makes a comparison (2) near the width: its roots lie at D_t = 71–133, 35 e-folds in
    (§4.2).
- **K = 15 satisfies (2) except in a sliver.** (e^{1/16} − 1)·15.5 = 0.99966, so an outer end at
  u ∈ [15.5, 15.5005)q and a midpoint 1/16 e-fold in both round to 16q.
- **No factor stands between the inequality and the constant.** The integer step leaves
  (e^{1/16} − 1)(16.5) = 1.064, a 6 % headroom. That covers what the inequality leaves out: the
  relative roundings of M/φ, of φ = e^x and of the midpoint x_mid, each about 10⁻¹⁵ of what it rounds.
  The constant's comment says so.
- **A generic model.** K = 16 covers a model with ρ ≤ ½ and s ≥ 1. A model whose difference is rounded
  by more, or whose wall is shallower in ln φ, needs a larger K from the same inequality. The comment
  states both assumptions.

### 1.3 Condition (3): the unresolvable region, and a scan (ruling 1, the region)

At K = 16 and ρ = ½, (3) holds for t > 1.11198. That is D_t > 18.35q, or, in the form of ruling 5's
margin, m_D = ln(D_t/(Kq)) > **0.1369**.
- **The failure below it** is "the searched quantity is not monotone along the search (a midpoint of the
  bracket [...] is not between its ends)". In the scan it was never "the bracket's outer end is still
  -inf": the bracket's two ends meet at the width long before the 68 halvings run out.
- **The scan** (`agent07b/derive/scan.py`, scratch). It calls `classical_turning_point` on the exponential
  at M = 10⁻¹⁶ M_P, normalization −276.6307286951731, with the helper patched to width K. The grid is
  241 fields u₀ = 0.3–60q by 1 281 roots D_t = 0.4–80q, log-spaced, 308 721 calls per K:

  | K | raises | largest D_t with a raise | smallest D_t with a success | fields inside their turning point that raise |
  |---|---|---|---|---|
  | 0 (shipped) | 54 512 (4 236 "moved away", 50 276 "midpoint") | 80q (the band's fields, at every root) | 4.01q | 1 782 |
  | 1 | 62 842 ("midpoint") | 14.96q | 4.01q | 10 087 |
  | 15 | 159 668 | 15.99q | 16.05q | 73 095 |
  | **16** | 164 892 | **16.94q** | **17.01q** | 75 768 |
  | 64 | 296 430 | 64.78q | 65.04q | 138 092 |

  At K ≥ 15 the edge is sharp: every root above it is found from every field, and every root below
  it raises. At K = 0 and 1 the failures are ragged between 4q and 15q, depending on where the field
  starts: those are comparisons (2) failing by rounding. The measured edge at K = 16 (16.94–17.01q)
  lies inside (3)'s sufficient bound (18.35q).
- **How K enlarges the region.** The shipped helper declared −∞ only at D̂ = 0, D ≤ q/2. But its search
  failed by rounding for roots up to 15q, depending on the field, and for every root from a field at
  [0.50, 0.55)q. At K = 16 the search cannot return a turning point for D_t ≲ 17q, that is
  ln E − ln V_floor ≲ ln(17q) = −27.7, or E ≲ 10⁻¹² V_floor. Above that it always can. With the
  probes' 64 the region would reach 65q.
- **A field inside its turning point but within the width** now raises rather than returning `None`
  when the root is also within the width (75 768 cases in the scan). r₀ = −∞ < 0, so the search runs
  and meets (3). The field and its turning point are then both where V − V_floor is lost in the
  rounding.
- **Ruling 5's margin.** m = ln E − ln V_floor − ln(Kq) equals m_D to first order where E ≪ V_floor,
  and exceeds it otherwise, since log1p(y) < y. So §5 reports both, and tests m_D against 0.1369 (D3).

### 1.4 What −∞ now means, where the helper is used

The helper has two callers (`git grep`), both in `AbstractModel.py`.
- **`classical_turning_point`'s residual** (`:570`). −∞ is "V rounds to V_floor, or lies within K quanta
  of it". The search already allowed it: −∞ repeated is monotone, and the halvings go on while the
  bracket's outer end is −∞. A field whose own value is −∞ is treated as outside its turning point.
  So the search runs where it used to return `None` (§1.3), and only for E ≲ 10⁻¹² V_floor.
- **`one_loop_turnaround`'s `outside_classical`** (`:681`) is
  `_log_V_minus_floor(ln V(φ_ta), ln V_floor) < ln E`. Where V(φ_ta) lies within K quanta of V_floor,
  this is −∞ < ln E, which is True. A turnaround where V rounds to V_floor is outside the classical
  one, as before for V = V_floor exactly. The flag is reported, not enforced. On the measured
  histories every one-loop solution has it True (§5.3).
- **Not the helper's:**
  - `ScalarHistory.assess_infall` forms ln(V − V_floor) with its own expression, so ln E stays bitwise.
    It passes ln E to `classical_turning_point`, so natural mode's warning (`natural_floor_warning`)
    goes through the new helper. Its text did not change (§5.5);
  - G2, and `minimum_on_trace`, which never reads V_floor.

### 1.5 The helper and the docstrings (ruling 3)

- **The helper.** One width, `WALL_ROUNDING_QUANTA * ulp(max(|ln V|, |ln V_floor|))`, and one chained
  comparison, `log_V - log_V_floor <= width < float("inf")`.
  - The comparison is ≤, so that a difference of exactly K quanta is −∞ (prompt §1.1).
  - `width < inf` keeps ln V = +∞ out of the rule (D1). There the width is ulp(∞) = ∞, and ∞ ≤ ∞
    would make it −∞. Shipped, it is +∞, which `_evaluate` refuses as non-finite. It still is.
  - NaN and ln V ≤ ln V_floor are −∞ by the first test, as shipped.
- **The docstrings.** Two changed, each where it describes −∞:
  - the helper's own: "-inf where V is below V_floor, or V rounds to V_floor, or lies within K quanta
    of it", with the width and the +∞ case;
  - `_evaluate`'s phrase "where V rounds to V_floor", which now reads "where V rounds to V_floor, or
    lies within K quanta of it".

  No line changes in `_search_log_field`, `classical_turning_point` or `one_loop_turnaround`. Their
  docstrings do not say what −∞ means. `git diff c2aac94 -- CosmologyConcepts/Models/AbstractModel.py`
  touches only the import, the constant with its comment, the helper and `_evaluate`'s docstring.

## 2. What shipped

### `CosmologyConcepts/Models/AbstractModel.py`

```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -48,7 +48,7 @@ None assumes a form of the potential.
 import sys
 from abc import ABC, abstractmethod
 from collections import namedtuple
-from math import exp, expm1, isfinite, isnan, log, pi
+from math import exp, expm1, isfinite, isnan, log, pi, ulp
 from typing import Callable, List, Optional, Tuple
 
 from scipy.optimize import brentq
@@ -103,6 +103,13 @@ WALL_ROOT_XTOL = 1.0e-14
 # outer end's value is -inf, which brentq's interpolation cannot use
 WALL_SEARCH_MAX_DOUBLINGS = 64
 WALL_SEARCH_HALVINGS = 4
+# the width of ln V's rounding (inelastic-reflection prompt 07b; log 07b §1): _log_V_minus_floor is
+# -inf where ln V - ln V_floor <= K ulp(max(|ln V|, |ln V_floor|)). Two finite probes 2^-H e-folds
+# apart (H = WALL_SEARCH_HALVINGS, a halving midpoint against an end, the closest the search
+# compares) are ordered if (e^{s 2^-H} - 1)(K + 1 - rho) > 2 rho, for a model whose ln V - ln V_floor
+# is rounded within rho quanta and has |d ln(ln V - ln V_floor)/d ln phi| >= s. The exponential
+# (s = n = 1, rho = 1/2) needs K > 15.005; K = 16 is the next integer, with no margin beyond its 6 %
+WALL_ROUNDING_QUANTA = 16
 # the one-loop fixed point: from ell_0 = 32 (eq:chain-turn) until |delta ell| <= 1e-12 ell
 ONE_LOOP_ELL_START = 32.0
 ONE_LOOP_ELL_RTOL = 1.0e-12
@@ -128,9 +135,17 @@ def _failure(message: str):
 
 
 def _log_V_minus_floor(log_V: float, log_V_floor: float) -> float:
-    """ln(V - V_floor) from ln V and ln V_floor, -inf where V rounds to V_floor or below."""
+    """
+    ln(V - V_floor) from ln V and ln V_floor; -inf where V is below V_floor, or V rounds to V_floor,
+    or lies within K quanta of it: ln V - ln V_floor <= K ulp(max(|ln V|, |ln V_floor|)), K =
+    WALL_ROUNDING_QUANTA (inelastic-reflection prompt 07b). A difference that small is not resolved
+    by ln V's rounding. ln V = +inf has an infinite width and is not within it: it returns +inf.
+    """
     if not log_V > log_V_floor:
         return _NEG_INF
+    width = WALL_ROUNDING_QUANTA * ulp(max(abs(log_V), abs(log_V_floor)))
+    if log_V - log_V_floor <= width < float("inf"):
+        return _NEG_INF
     return log_V + log(-expm1(log_V_floor - log_V))
 
 
@@ -139,7 +154,8 @@ def _evaluate(residual: Callable[[float], float], x: float, context: str) -> flo
     residual(phi) at phi = e^x. A phi that is not a positive normal float, a hook that raises, or a
     value that is NaN or +inf ends the search as a ComputationFailureError naming the probe, with the
     hook's own exception chained (orchestrator note 04, ruling 3). -inf is allowed: it is
-    ln(V - V_floor) where V rounds to V_floor, or the minimum's residual where V' >= 0.
+    ln(V - V_floor) where V rounds to V_floor, or lies within K quanta of it, or the minimum's
+    residual where V' >= 0.
     """
     try:
         phi = exp(x)
```

### The tests (`ComputeTargets/tests/test_wall_rounding.py`, new; 12 tests, about 6.5 s at a load of 8–14)

No store, no Ray, no PRyMordial solve. Item 4 of prompt §1.4 is the suites' (§4.1).

| Test | Item | Asserts | Tolerance |
|---|---|---|---|
| `test_1_the_width_is_the_derived_one` | 1 | K computed in the test from the inequality (s = 1, ρ = ½, H read from the module) is 16; `WALL_ROUNDING_QUANTA` equals it; the inequality holds at K and fails at K − 1 | exact |
| `test_1_the_difference_is_j_quanta` | 1 | `nextafter` j times from ln V_floor = −276.6307286951731 gives a difference of exactly j·2⁻⁴⁴, for j = 0 … 2K + 2 | exact |
| `test_1_j_from_0_to_K_returns_minus_inf` | 1 | j = 0 … K: −∞ | exact |
| `test_1_j_equal_to_K_plus_1_returns_the_shipped_expression_bitwise` | 1 | j = K + 1, K + 2, 2K: the shipped expression, bitwise, and finite | bitwise |
| `test_1_V_below_its_floor_returns_minus_inf` | 1 | ln V one float below ln V_floor, 1 below, −∞: −∞ | exact |
| `test_1_a_difference_of_order_one_returns_the_shipped_expression_bitwise` | 1 | D = 0.5, 1, 3, 100: the shipped expression | bitwise |
| `test_1_an_infinite_log_V_keeps_the_shipped_value` | 1 (D1) | ln V = +∞ returns +∞, as shipped | exact |
| `test_2_the_twelve_inputs_return_the_closed_form` | 2 | at each of `band_shipped.out`'s twelve (β, M, normalization, φ, ln E), copied as printed (`repr`, so the floats are the probe's): the shipped helper, patched in, raises "not monotone"; the method returns ln φ_t = ln M − ln log1p(e^{ln E − norm}) | ruling 9's sum, 6.6×10⁻¹⁴ to 1.1×10⁻¹³ (§4.2) |
| `test_2_the_comparison_that_binds_the_width` | 2, ruling 4 | the field at u₀ = 16.6q, the root at D_t = 17.5q. The probes in quanta are 17, 45, 27, 21, 19, 18, all finite. The fourth midpoint is 1/16 e-fold from the field and lies strictly between its ends. Then φ_t equals the closed form | x to 10⁻¹⁴; φ_t to ruling 9's sum, 0.029 here (the quantization, half a quantum in 17.5) |
| `test_3_it_reaches_T_CMB` | 3 | M = 10⁻¹⁶ M_P, β = 2, α = 0.1: no failure; ln T_J(N_final) = ln T_CMB | 9 places |
| `test_3_it_parks_at_candidate_48_a_floor_reflection` | 3 | parking at candidate 48, a floor candidate, N = 30.349, no release | N to 3 places |
| `test_3_the_drain_is_log_06s` | 3, ruling 6 | Σ Δρ/ρ_R over the candidates plus the leftover against 0.012616907 | 0.012616907 × 2.5×10⁻⁸ + 5×10⁻¹⁰ = 8.2×10⁻¹⁰ |

**Item 2's tolerance** (ruling 9, `_log_phi_t_tolerance`). It has three named terms, with x_t = ln φ_t,
u_t = log1p(E/V₀) and ln V_t = norm + u_t:
- **brentq**: WALL_ROOT_XTOL + WALL_ROOT_RTOL |x_t|. This is 4.1–6.5×10⁻¹⁴.
- **The residual's rounding over its slope**:
  - ln V's rounding, (ulp(ln V_t)/2 + εu_t/2), reaches ln(V − V_floor) multiplied by 1/(−expm1(−u_t));
  - the log, the sum and the subtraction of ln E add ε|ln E|;
  - the slope is u_t/(−expm1(−u_t)).

  The term is 4.6–9.5×10⁻¹⁶. In the synthetic case it is the quantization, half a quantum in 17.5
  quanta: 0.029.
- **The closed form's rounding**:
  - the exponent's ε(|ln E| + |norm|)/2, carried into u_t by f = z/((1 + z) log1p z), z = E/V₀;
  - ε each for exp, log1p and log;
  - ε(2|x_t| + |ln u_t| + |ln M|) for ln M − ln u_t, the method's e^x and the test's log.

  This is 2.5–4.4×10⁻¹⁴.

The measured errors are 0 or 7.1×10⁻¹⁵ at the twelve inputs, and 1.1×10⁻¹⁴ in the synthetic case.

**Ruling 4's case does not catch B2, and no input can (§6).** Its probes lie above 16q, so K = 1 takes
the same path and returns the same φ_t. Whether some input makes K = 1 raise at the binding comparison
while K = 16 returns a turning point was checked by argument and by the scan:
- At K = 1, a comparison (2) fails only when two finite probes 2^{−h} e-folds apart round alike. That
  needs D ≲ 1/(e^{1/16} − 1) + ½ ≈ 16q for the smaller.
- The bracket that holds them then ends below about 16.5q. So the root lies below about 16.5q.
- K = 16 cannot resolve such a root: its edge is at 17.0q.

The scan confirms it. No root above 14.96q fails at K = 1, and none below 16.94q succeeds at K = 16.
So B2 is caught by item 1 alone, through `test_1_the_width_is_the_derived_one` and the j = 0 … K test.
That is why the first of those computes K from the inequality instead of reading the module's
constant (D4).

### The documents (P12)

- `.documents/architecture-summary.md`: a dated note after 04's on the model methods. It covers the
  width and why, the band where the shipped search failed, that −∞ is "V rounds to V_floor, or lies
  within K quanta of it", what does not change, and the region the width gives up.
- `.documents/numerical-strategies.md`: a new §6.7 after 07's §6.6. It covers the quantization, K's
  derivation, the unresolvable region and the scan, the band before and after, and what does not
  change, with the cost.

## 3. Deviations from the prompt

**The rulings of orchestrator note 07b (1–11):**

| # | Ruling, as applied | Class |
|---|---|---|
| R1 | K's derivation covers every comparison between two finite values. The rounding is §1.1: ρ = ½, the exponential's difference exact by Sterbenz and an integer number of quanta; ordered if more than 2ρ quanta apart; a generic model's c and c_f. The comparisons (1), (2) and (3), each with its separation and the slope s = n of ln D (§1.2). The inequality from (2), K > 15.005; why 64 satisfies it and why K = 1 fixes what was observed. (3)'s condition on the root, (K + ρ)t(√t − 1) > 2ρ, i.e. m_D > 0.1369; the failure below it, "a midpoint … is not between its ends"; and how K enlarges the region, measured by the scan (§1.3) | STRUCTURALLY REQUIRED |
| R2 | K = 16, the smallest integer of the inequality at n = 1 and H = 4. No margin factor; the 6 % the integer leaves is named in the comment and §1.2. The comment states the inequality and its two assumptions (ρ, s) in six lines (D5). K ≠ 64, so every measurement of §5 is at K = 16 and the probes' K = 64 results are references only | IMPLEMENTATION CHOICE (ruled) |
| R3 | The helper's docstring states the new meaning; `_evaluate`'s phrase changed to the same words and nothing else. No code line changes in the four functions; the other three docstrings do not describe what −∞ means and are unchanged (§1.5) | IMPLEMENTATION CHOICE (ruled) |
| R4 | `test_2_the_comparison_that_binds_the_width`: the binding comparison is made, with all values finite and 17–19 quanta from the floor, and the closed form is returned. No input makes B2 fail it while K = 16 resolves it; why is in §2, with the scan. B2 is caught by item 1 alone (§6) | IMPLEMENTATION CHOICE (ruled) |
| R5 | A spy in `agent07b/meas/hist.py` (scratch) counts the helper's calls, the reclassified ones (the shipped value finite, now −∞), and the margins of every `classical_turning_point` call that returns a turning point, on all 30 band histories, the 13 edges and the 28 of §2.4 (§5). The smallest m_D is 25.18 (M = 10⁻³ M_P, β = 0.5), far above 0.1369. No stop | IMPLEMENTATION CHOICE (ruled) |
| R6 | Item 3's tolerance is 0.012616907 × 2.5×10⁻⁸ + 5×10⁻¹⁰. The measured difference is −9.8×10⁻¹² (0.012616906990246; the full-precision M = 10⁻³⁰ value is 0.012616906990249208, a difference of −2.6×10⁻¹³ relative) | IMPLEMENTATION CHOICE (ruled) |
| R7 | Edges at quarter decades at the dispatch commit, β = 2, every one reported, no bisection (§5.2). The failures are contiguous: 10⁻¹³·⁰⁰ and 10⁻²⁵·⁰⁰ to 10⁻²⁶·⁰⁰; for each running history, whether a step start fell in [0.5, 0.552)q (none did) | IMPLEMENTATION CHOICE (ruled) |
| R8 | One worktree of `c2aac94` in scratch, removed afterwards. Every history compared by `repr` on 27 keys: the 13 fields of `IntegrationResult` other than `solution` (warnings, parking and release among them), `ts`, and 13 of the payload's (the samples, z grid, first bounce, fixed-T values, reflection count, accepted and rejected steps, injections, turning points, parking, release, warnings and ledger). §2.1's final-tree histories used the same harness | IMPLEMENTATION CHOICE (ruled) |
| R9 | Item 2's inputs are the literals of `band_shipped.out`, all twelve, as printed by `repr` (17 significant digits or fewer, each the probe's float). `ExponentialModel(M·M_P, 1, β)`; the method called with the printed normalization. The tolerance is the sum of three named terms, each with its numbers in the docstring (§2) | IMPLEMENTATION CHOICE (ruled) |
| R10 | Only prompt §6's files changed. `ComputeTargets/` but the new module, `ExponentialModel.py`, the golden modules and JSON, every existing test module and the `integrator-remediation` board are byte-identical. In `AbstractModel.py` the diff is the constant with its comment, the helper, ruling 3's docstring line, and the `ulp` import (D2) | as ruled, with D2 |
| R11 | The helper's cost is the minimum of 7 × 10⁵ calls, shipped against final, in the band and far from it. Three histories, plus M = 10⁻³⁰, timed in three rounds with the dispatch commit and the final tree back to back. The rest are single rounds. Suites serial, in halves, to scratch, with `uptime`. §2's histories four at a time under `timeout 900` (none reached it). No failure needed a rerun | as ruled |

**My own deviations:**

| # | Deviation | Class |
|---|---|---|
| D1 | `width < float("inf")` in the helper's comparison: ln V = +∞ keeps the shipped value +∞. The prompt's inequality, read literally, makes it −∞ (ulp(∞) = ∞). Tested by `test_1_an_infinite_log_V_keeps_the_shipped_value` | STRUCTURALLY REQUIRED |
| D2 | `ulp` added to the module's `math` import, a line ruling 10 does not list | STRUCTURALLY REQUIRED |
| D3 | Ruling 5's margin m = ln E − ln V_floor − ln(Kq) is reported, and so is m_D = ln(D_t/(Kq)), D_t = log1p(E/V_floor). Condition (3) is on D_t, and m ≥ m_D, with m much larger where E ≫ V_floor (78 against 32 on the band). The stop was tested on m_D, the stricter | STRUCTURALLY REQUIRED |
| D4 | Item 1 computes K from the inequality in the test, rather than reading `WALL_ROUNDING_QUANTA`. With K read from the module, B2 passed every test, because item 1 followed the breakage. A new test, `test_1_the_width_is_the_derived_one`, asserts the constant equals the derived K | IMPLEMENTATION CHOICE |
| D5 | The constant's comment is six lines, not ruling 2's "one or two", to state the inequality, its two assumptions and the margin | IMPLEMENTATION CHOICE |
| D6 | Item 2 also asserts that the shipped helper, patched in, raises "not monotone" on each of the twelve inputs, so that the test shows the input is one the shipped search refused | IMPLEMENTATION CHOICE |
| D7 | §5.1 runs the 30 band histories at both trees, and §5.2's 13 edge histories at the final tree too. The 12 that ran at both are compared bitwise as well as §2.4's 28 | IMPLEMENTATION CHOICE |
| D8 | The scan of §1.3, beyond ruling 1's request for a condition and a name: it measures the edge at K = 0, 1, 15, 16 and 64 | IMPLEMENTATION CHOICE |
| D9 | On the board, a "Measured (2026-10-30, prompt 07b)" line on `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]`, with §5.3's result. It neither narrows nor closes the issue, so its index row is unchanged | IMPLEMENTATION CHOICE |

## 4. Verification performed

### 4.1 The suites

Run serially with `CLAUDE.md`'s commands, ComputeTargets in two halves, output in my scratch directory.
"Before" is at `c2aac94`, before any edit. "After" is on the final tree.

| Suite | Before (count, unittest's time, load before → after) | After |
|---|---|---|
| CosmologyModels | 26 OK, 65 s, 8.0 → 5.9 | 26 OK, 66 s, 6.8 → 5.2 |
| ComputeTargets [a-f] | 96 OK, 121 s, 5.9 → 7.7 | 96 OK, 163 s, 5.2 → 37.1 |
| ComputeTargets [g-z] | 331 OK, 398 s, 7.7 → 16.1 | **343** OK, 612 s, 37.1 → 6.5 |
| ObjectFactories | 103 OK, 16 s, 16.1 → 17.3 | 103 OK, 18 s, 6.5 → 6.2 |
| tools | 115 OK, 124 s, 17.3 → 11.4 | 115 OK, 153 s, 6.2 → 10.2 |
| RunDrivers | 64 OK, 23 s, 11.4 → 10.3 | 64 OK, 27 s, 10.2 → 10.4 |

The "after" ComputeTargets halves took 4 728 s and 8 592 s of wall time for 163 s and 612 s of test
time. The machine went into maintenance sleep, on battery, while they ran (`pmset -g log`), and the
load reached 166 between the halves from other work. Each suite ran once and passed, so nothing was
re-run.

- The baselines are prompt §3.1's: 26 / 427 (96 + 331) / 103 / 115 / 64.
- ComputeTargets rose by 12, the new module, which sorts into [g-z]. Nothing fell.
- **The tests of prompt §1.4 item 4 passed unedited.** These are the golden tests (`test_golden_history`,
  the tuned golden test), `test_wall_model` (its bump test `test_5_a_bump_between_the_field_and_the_wall_is_refused`
  among them), `test_first_bounce`, `test_surfing_turning_points`, `test_kinematic_cap_loop`,
  `test_unresolved_turnarounds`, `test_injection`, `test_gate_and_taper`, `test_parking` and
  `test_release`. No existing test module changed.
- `black --check` on the two changed Python files: unchanged.

### 4.2 Item 2 at every input

| β | M (M_P) | u₀/q at the field | u_t = D_t | log₁₀ 𝓡 | \|Δ ln φ_t\| | tolerance (brentq + residual + closed form) |
|---|---|---|---|---|---|---|
| 1.2 | 10⁻¹⁶ | 0.5474 | 102.2 | 2.42 | 0 | 7.7e-14 (4.7e-14 + 6.3e-16 + 3.0e-14) |
| 1.2 | 10⁻²⁰ | 0.5221 | 102.1 | 18.41 | 0 | 9.1e-14 |
| 2 | 10⁻¹³ | 0.5033 | 133.4 | 4.91 | 0 | 6.6e-14 |
| 2 | 10⁻¹⁴ | 0.5490 | 108.6 | −2.56 | 7.1e-15 | 7.0e-14 |
| 2 | 10⁻¹⁵ | 0.5017 | 106.3 | 0.37 | 0 | 7.3e-14 |
| 2 | 10⁻¹⁶ | 0.5095 | 106.1 | 4.25 | 7.1e-15 | 7.7e-14 |
| 2 | 10⁻¹⁸ | 0.5254 | 106.0 | 12.24 | 0 | 8.4e-14 |
| 2 | 10⁻²⁰ | 0.5417 | 106.0 | 20.24 | 0 | 9.1e-14 |
| 2 | 10⁻²² | 0.5028 | 106.0 | 28.24 | 7.1e-15 | 9.8e-14 |
| 2 | 10⁻²⁵ | 0.5434 | 71.47 | 23.86 | 0 | 1.1e-13 |
| 3 | 10⁻¹⁶ | 0.5135 | 108.3 | 5.31 | 7.1e-15 | 7.7e-14 |
| 3 | 10⁻²⁰ | 0.5444 | 108.3 | 21.29 | 0 | 9.1e-14 |
| ruling 4 | 10⁻¹⁶ | 16.6 | 17.5q | −117.45 | 1.1e-14 | 2.9e-2 |

Every field lies in [0.50, 0.552)q, the window of §1.2.

## 5. The measurement (prompt §2)

Every history uses test_injection's setup: n = 1, φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV, to T_CMB, default
tolerances, the seed realization, untuned, through `integrate_history` with its sampling. α = 0.1
unless stated. The scripts are in my scratch directory, `agent07b/meas/`:
- `hist.py`: one history, pickled for `repr` comparison, with the spy of ruling 5 and the gap of §5.3;
- `runjobs.sh`: four at a time under `timeout 900`;
- `an_disp.py`, `an_final.py`, `tables.py`: the tables;
- `timing.sh` and `cost_helper.py`.

The dispatch commit's histories ran from a worktree of `c2aac94` in scratch, removed afterwards (§5.4).
Load 4–28 throughout.

### 5.1 The band (prompt §2.1)

At the dispatch commit each failure is "not monotone (a probe moved away from the target)" at a step
start with π < 0. The residual is equal at the field and one e-fold in, and D̂ = 1, 1 quantum in every
case. At the final tree every history reaches T_CMB, with no release. "Rel." is
(Σ + leftover)/(the same at M = 10⁻³⁰, β alike) − 1. Wall times are single rounds with the spy on
(the spy adds under 10 %, §5.6).

| β | M (M_P) | dispatch: N, φ (M_P), u₀/q, residual (twice) | final: parks at, N | N_final | Σ + leftover | rel. | wall (s) |
|---|---|---|---|---|---|---|---|
| 1.2 | 10⁻¹³ | 13.52959, 3.49245, 0.5037, −148.643399 | #6 floor, 28.36410 | 46.07564 | 0.015092605259 | 6.7e-9 | 1.7 |
| 1.2 | 10⁻¹⁴ | 17.28773, 0.326352, 0.5391, −134.194850 | #6, 28.36406 | 46.07564 | 0.015092605166 | 5.6e-10 | 1.7 |
| 1.2 | 10⁻¹⁵ | 17.62954, 0.0327706, 0.5368, −132.798587 | #6, 28.36417 | 46.07564 | 0.015092605406 | 1.6e-8 | 1.7 |
| 1.2 | 10⁻¹⁶ | 17.66417, 0.00321397, 0.5474, −132.657279 | #6, 28.36406 | 46.07564 | 0.015092605169 | 7.4e-10 | 1.7 |
| 1.2 | 10⁻¹⁷ | 17.66756, 3.28664e-4, 0.5353, −132.643478 | #6, 28.36409 | 46.07564 | 0.015092605227 | 4.6e-9 | 1.7 |
| 1.2 | 10⁻¹⁸ | 17.66790, 3.25396e-5, 0.5406, −132.642062 | #6, 28.36408 | 46.07564 | 0.015092605206 | 3.2e-9 | 1.7 |
| 1.2 | 10⁻²⁰ | 17.66794, 3.36967e-7, 0.5221, −132.641908 | #6, 28.36408 | 46.07564 | 0.015092605215 | 3.8e-9 | 1.7 |
| 1.2 | 10⁻²² | 17.66794, 3.23508e-9, 0.5438, −132.641906 | #6, 28.36406 | 46.07564 | 0.015092605158 | 2.4e-11 | 1.7 |
| 1.2 | 10⁻²⁵ | 28.36406, 3.45077e-12, 0.5098, −77.908515 | #6, 28.36406 | 46.07564 | 0.015092605158 | 0 | 1.7 |
| 1.2 | 10⁻²⁷ | runs (parks #6, 46.07564) | #6, 28.36406 | 46.07564 | 0.015092605159 | 8.4e-11 | 1.7 |
| 2 | 10⁻¹³ | 13.47136, 3.49521, 0.5033, −163.896851 | #48 floor, 30.34933 | 50.07564 | 0.012616906990 | 2.5e-14 | 6.3 |
| 2 | 10⁻¹⁴ | 19.70835, 0.320433, 0.5490, −139.118678 | #48, 30.34933 | 50.07564 | 0.012616906990 | 1.8e-13 | 6.3 |
| 2 | 10⁻¹⁵ | 20.28164, 0.0350625, 0.5017, −136.824002 | #48, 30.34933 | 50.07564 | 0.012616906990 | −1.4e-13 | 6.3 |
| 2 | 10⁻¹⁶ | 20.34516, 0.00345284, 0.5095, −136.569920 | #48, 30.34933 | 50.07564 | 0.012616906990 | −2.6e-13 | 6.3 |
| 2 | 10⁻¹⁷ | 20.35142, 3.40016e-4, 0.5174, −136.544900 | #48, 30.34933 | 50.07564 | 0.012616906990 | −1.2e-13 | 6.4 |
| 2 | 10⁻¹⁸ | 20.35203, 3.34835e-5, 0.5254, −136.542437 | #48, 30.34933 | 50.07564 | 0.012616906990 | −3.1e-13 | 6.4 |
| 2 | 10⁻²⁰ | 20.35210, 3.24729e-7, 0.5417, −136.542170 | #48, 30.34933 | 50.07564 | 0.012616906990 | −3.8e-14 | 6.5 |
| 2 | 10⁻²² | 20.35210, 3.49900e-9, 0.5028, −136.542168 | #48, 30.34933 | 50.07564 | 0.012616906990 | −5.6e-14 | 6.4 |
| 2 | 10⁻²⁵ | 27.75954, 3.23771e-12, 0.5434, −101.965910 | #48, 30.34933 | 50.07564 | 0.012616906990 | −2.9e-13 | 6.4 |
| 2 | 10⁻²⁷ | runs (parks #48, 50.07564) | #48, 30.34933 | 50.07564 | 0.012616906990 | −1.5e-13 | 6.4 |
| 3 | 10⁻¹³ | 10.60261, 3.50079, 0.5025, −194.550381 | #58 floor, 29.39426 | 55.07564 | 0.007962110088 | 2.8e-8 | 7.5 |
| 3 | 10⁻¹⁴ | 22.55071, 0.349869, 0.5028, −144.140399 | #58, 29.39426 | 55.07564 | 0.007962110062 | 2.5e-8 | 7.5 |
| 3 | 10⁻¹⁵ | 24.37660, 0.0349178, 0.5038, −139.199460 | #58, 29.39426 | 55.07564 | 0.007962110051 | 2.3e-8 | 7.5 |
| 3 | 10⁻¹⁶ | 24.48724, 0.00342624, 0.5135, −138.842107 | #58, 29.39426 | 55.07564 | 0.007962109596 | −3.4e-8 | 7.6 |
| 3 | 10⁻¹⁷ | 24.49785, 3.38034e-4, 0.5204, −138.807397 | #58, 29.39426 | 55.07564 | 0.007962110074 | 2.6e-8 | 7.6 |
| 3 | 10⁻¹⁸ | 24.49889, 3.33001e-5, 0.5283, −138.803975 | #58, 29.39426 | 55.07564 | 0.007962110085 | 2.8e-8 | 7.6 |
| 3 | 10⁻²⁰ | 24.49900, 3.23122e-7, 0.5444, −138.803605 | #58, 29.39426 | 55.07564 | 0.007962110103 | 3.0e-8 | 7.6 |
| 3 | 10⁻²² | 24.49900, 3.47936e-9, 0.5056, −138.803601 | #58, 29.39426 | 55.07564 | 0.007962110082 | 2.7e-8 | 7.6 |
| 3 | 10⁻²⁵ | 27.97056, 3.49029e-12, 0.5040, −121.367473 | #58, 29.39426 | 55.07564 | 0.007962110067 | 2.5e-8 | 7.6 |
| 3 | 10⁻²⁷ | runs (parks #58, 55.07564) | #58, 29.39426 | 55.07564 | 0.007962109864 | −1.5e-13 | 7.5 |

- **27 of 30 fail at the dispatch commit**, among them M = 10⁻¹⁷, which the probes did not run. All
  three at M = 10⁻²⁷ run there, and are bitwise at the final tree (§5.4).
- Most fail at the first infall: at N = 10.6–13.5 at M = 10⁻¹³, and below that at 17.3–17.7
  (β = 1.2), 19.7–20.35 (β = 2) and 22.6–24.5 (β = 3). At 10⁻²⁵ the failure moves to the bottom of the
  cascade (N = 27.8–28.4), where the field reaches M/φ ≈ 0.5q later.
- **The candidates and the parking are log 06's** at every M: candidates 7, 49 and 59, parked at the
  last, all floor reflections.
- **Σ + leftover against M = 10⁻³⁰** differs by at most 1.6×10⁻⁸, 3.1×10⁻¹³ and 3.4×10⁻⁸ at
  β = 1.2, 2 and 3. That is within log 06's M-dependence (5×10⁻⁸, 2×10⁻⁸ and 1.3×10⁻⁷). The accepted
  steps differ from M = 10⁻³⁰'s by up to 7 at β = 1.2 and 3 at β = 3, and not at all at β = 2.
- **Ruling 5 on the band.** The width reclassifies 465–1 546 helper calls per history at
  M = 10⁻¹³–10⁻¹⁶, and up to 8 284 at 10⁻²² (β = 3), out of 34 510–223 743. Every reclassified call at
  β = 2, M = 10⁻¹⁶ has D̂ = 1–16 quanta. Every `classical_turning_point` call returns a turning point.
  The smallest margins are m = 75.14, 78.46 and 102.78, and m_D = 31.585, 31.652 and 32.044 (β = 1.2,
  2, 3), at every M. Condition (3) needs 0.1369.

### 5.2 The band's edges at β = 2, at the dispatch commit (prompt §2.2, ruling 7)

| M (M_P) | dispatch | at the failure: N, φ (M_P), u₀/q; or, running, the step starts with π < 0 near the window |
|---|---|---|
| 10⁻¹²·⁵⁰ | runs | 9 220 starts; none in [0.5, 0.552)q; the nearest u₀ = 1.1126q (D̂ = 1, then 3 one e-fold in) |
| 10⁻¹²·⁷⁵ | runs | 9 220 starts; none in the window; the nearest u₀ = 0.6257q (D̂ = 1, then 2) |
| 10⁻¹³·⁰⁰ | **fails** | N = 13.47136, φ = 3.49521, u₀ = 0.5033q |
| 10⁻²⁵·⁰⁰ | **fails** | 27.75954, 3.23771×10⁻¹², 0.5434q |
| 10⁻²⁵·²⁵ | **fails** | 29.41123, 1.86722×10⁻¹², 0.5298q |
| 10⁻²⁵·⁵⁰ | **fails** | 29.80923, 1.01100×10⁻¹², 0.5503q |
| 10⁻²⁵·⁷⁵ | **fails** | 30.06557, 5.87541×10⁻¹³, 0.5325q |
| 10⁻²⁶·⁰⁰ | **fails** | 30.23215, 3.36666×10⁻¹³, 0.5225q |
| 10⁻²⁶·²⁵ | runs | 9 220 starts; none in the window; the nearest u₀ = 0.4464q (D̂ = 0, then 1) |
| 10⁻²⁶·⁵⁰ to 10⁻²⁸·⁰⁰ (7) | runs | 9 220 starts each; none with u₀ in [0.3, 2]q |

- **The upper edge** lies between 10⁻¹²·⁷⁵ and 10⁻¹³·⁰⁰. The prediction, M ≈ 0.552q × 5 M_P =
  1.57×10⁻¹³ = 10⁻¹²·⁸⁰, lies between them: a field starting at φ* first meets the window there.
- **The lower edge** lies between 10⁻²⁶·⁰⁰ and 10⁻²⁶·²⁵. The failures are contiguous on the
  quarter-decade grid, from 10⁻¹³·⁰⁰ through the band (§5.1) to 10⁻²⁶·⁰⁰. Below 10⁻²⁶·⁵⁰, no step start
  comes within [0.3, 2]q: the cascade parks (N = 30.349) before the infall reaches M/φ ≈ 0.5q. The
  lower edge is where that happens, not where a window is missed by chance.
- At every running edge the final tree is bitwise the dispatch commit's (§5.4).

### 5.3 The gap, for 08 (prompt §2.3, R19)

At the final tree, over each history's candidates with an assessment, `one_loop_turnaround`
(`phi_assessed`, `log_E_assessed`) is called where 𝓡 > 1. The share is ΔK_j/K(N_final), summed. In every
history the candidates with an assessment carry 0.9998–0.9999 of K; the rest is the leftover.

| β | M (M_P) | assessed | log₁₀ 𝓡 | 𝓡 > 1 (share of K) | the chain raises (share), at log₁₀ 𝓡 | `None` | solves (share) |
|---|---|---|---|---|---|---|---|
| 1.2 | 10⁻¹³ | 7 | −36.00 to −9.59 | 0 | 0 | 0 | 0 |
| 1.2 | 10⁻¹⁴ | 7 | −32.00 to −5.59 | 0 | 0 | 0 | 0 |
| 1.2 | 10⁻¹⁵ | 7 | −28.00 to −1.59 | 0 | 0 | 0 | 0 |
| 1.2 | 10⁻¹⁶ | 7 | −24.00 to 2.41 | 1 (0.8777) | 1 (0.8777) at 2.41 | 0 | 0 |
| 1.2 | 10⁻¹⁷ | 7 | −20.00 to 6.41 | 2 (0.9552) | 1 (0.0775) at 1.07 | 0 | 1 (0.8777) |
| 1.2 | 10⁻¹⁸ | 7 | −16.00 to 10.41 | 2 (0.9552) | 0 | 0 | 2 (0.9552) |
| 1.2 | 10⁻²⁰ | 7 | −8.00 to 18.41 | 5 (0.9957) | 2 (0.0232) at 0.66–1.72 | 0 | 3 (0.9724) |
| 1.2 | 10⁻²² | 7 | −0.00 to 26.41 | 6 (0.9999) | 0 | 0 | 6 (0.9999) |
| 1.2 | 10⁻²⁵ | 7 | 12.00 to 38.41 | 7 (0.9999) | 0 | 0 | 7 (0.9999) |
| 1.2 | 10⁻²⁷ | 7 | 20.00 to 46.41 | 7 (0.9999) | 0 | 0 | 7 (0.9999) |
| 2 | 10⁻¹³ | 49 | −31.66 to −7.76 | 0 | 0 | 0 | 0 |
| 2 | 10⁻¹⁴ | 49 | −27.66 to −3.76 | 0 | 0 | 0 | 0 |
| 2 | 10⁻¹⁵ | 49 | −23.66 to 0.24 | 1 (0.2812) | 1 (0.2812) at 0.24 | 0 | 0 |
| 2 | 10⁻¹⁶ | 49 | −19.66 to 4.24 | 4 (0.8689) | 3 (0.5877) at 0.32–2.22 | 0 | 1 (0.2812) |
| 2 | 10⁻¹⁷ | 49 | −15.66 to 8.24 | 7 (0.9625) | 3 (0.0936) at 0.46–2.83 | 0 | 4 (0.8689) |
| 2 | 10⁻¹⁸ | 49 | −11.66 to 12.24 | 9 (0.9769) | 1 (0.0033) at 1.94 | 0 | 8 (0.9736) |
| 2 | 10⁻²⁰ | 49 | −3.66 to 20.24 | 21 (0.9950) | 10 (0.0139) at 0.05–2.45 | 0 | 11 (0.9811) |
| 2 | 10⁻²² | 49 | 4.34 to 28.24 | 49 (0.9999) | 0 | 0 | 49 (0.9999) |
| 2 | 10⁻²⁵ | 49 | 16.34 to 40.24 | 49 (0.9999) | 0 | 0 | 49 (0.9999) |
| 2 | 10⁻²⁷ | 49 | 24.34 to 48.24 | 49 (0.9999) | 0 | 0 | 49 (0.9999) |
| 3 | 10⁻¹³ | 59 | −20.18 to −6.71 | 0 | 0 | 0 | 0 |
| 3 | 10⁻¹⁴ | 59 | −16.18 to −2.71 | 0 | 0 | 0 | 0 |
| 3 | 10⁻¹⁵ | 59 | −12.18 to 1.29 | 1 (0.1478) | 1 (0.1478) at 1.29 | 0 | 0 |
| 3 | 10⁻¹⁶ | 59 | −8.18 to 5.29 | 9 (0.7832) | 7 (0.5194) at 0.19–2.62 | 0 | 2 (0.2638) |
| 3 | 10⁻¹⁷ | 59 | −4.18 to 9.29 | 23 (0.9845) | 10 (0.0662) at 0.11–2.78 | 0 | 13 (0.9183) |
| 3 | 10⁻¹⁸ | 59 | −0.18 to 13.29 | 56 (0.9998) | 27 (0.0068) at 0.07–2.92 | 0 | 29 (0.9930) |
| 3 | 10⁻²⁰ | 59 | 7.82 to 21.29 | 59 (0.9998) | 0 | 0 | 59 (0.9998) |
| 3 | 10⁻²² | 59 | 15.82 to 29.29 | 59 (0.9998) | 0 | 0 | 59 (0.9998) |
| 3 | 10⁻²⁵ | 59 | 27.82 to 41.29 | 59 (0.9998) | 0 | 0 | 59 (0.9998) |
| 3 | 10⁻²⁷ | 59 | 35.82 to 49.29 | 59 (0.9998) | 0 | 0 | 59 (0.9998) |

- **Every raise is in the gap, and every candidate in the gap raises.** All raises are at
  log₁₀ 𝓡 = 0.05–2.92, and all solves at ≥ 3.00, on every history. Every raise is "no one-loop
  turnaround: ell = ln(k_ex/m_ta) fell to …", the saddle-node of log 04 §5.2. No call returned `None`.
  Every solution has `outside_classical` True, with ℓ = 0.34–26.7.
- **Which histories cross the gap.** These are β = 1.2 at M = 10⁻¹⁶, 10⁻¹⁷ and 10⁻²⁰, β = 2 at 10⁻¹⁵
  to 10⁻²⁰, and β = 3 at 10⁻¹⁵ to 10⁻¹⁸.
  - Where the first generations are in the gap, they carry most of K: 0.88 at β = 1.2, M = 10⁻¹⁶; 0.59
    at β = 2, 10⁻¹⁶; 0.52 at β = 3, 10⁻¹⁶.
  - Further down only late, small generations remain in it, about 10⁻²–10⁻³ of K.
- Every candidate has 𝓡 < 1, and so is classical, at M ≥ 10⁻¹⁵ for β = 1.2 and M ≥ 10⁻¹⁴ for β = 2
  and 3. At M ≤ 10⁻²² (β = 1.2, 2) and M ≤ 10⁻²⁰ (β = 3) none is in the gap. One candidate at β = 1.2,
  M = 10⁻²², has log₁₀ 𝓡 = −0.0038, just below 1.

### 5.4 What does not change (prompt §2.4, ruling 8)

The dispatch commit `c2aac94` and the final tree, compared by `repr` on 27 keys (ruling 8): the 13
`IntegrationResult` fields but `solution`, `ts`, and 13 of the payload's (the samples, z grid, first
bounce, fixed-T values, records, steps, warnings and the ledger). "Reclass." is ruling 5's count; m and m_D are the
smallest margins over the calls that returned a turning point.

| History | against `c2aac94` | helper calls | reclass. | m, m_D |
|---|---|---|---|---|
| β = 1.2, M = 10⁻⁷ / 10⁻⁸ / 10⁻⁹ / 10⁻¹⁰ | **bitwise**, all four | 24 130 / 26 876 / 29 765 / 31 734 | 0 | 75.13–75.14, 31.585 |
| β = 1.2, M = 10⁻¹² / 10⁻³⁰ | **bitwise** | 33 848 / 39 708 | **459 / 393** | 75.14, 31.585 |
| β = 2, M = 10⁻⁷ / 10⁻⁸ / 10⁻⁹ / 10⁻¹⁰ | **bitwise**, all four | 89 389 / 107 691 / 126 698 / 144 298 | 0 | 76.41–81.38, 31.611–31.708 |
| β = 2, M = 10⁻¹² / 10⁻³⁰ | **bitwise** | 162 273 / 193 016 | **437 / 2 305** | 78.46, 31.652 |
| β = 3, M = 10⁻⁷ / 10⁻⁸ / 10⁻⁹ / 10⁻¹⁰ | **bitwise**, all four | 106 830 / 129 110 / 152 189 / 172 352 | 0 | 97.63–105.03, 31.973–32.074 |
| β = 3, M = 10⁻¹² / 10⁻³⁰ | **bitwise** | 190 726 / 225 865 | **367 / 2 760** | 102.78, 32.044 |
| M = 0.5, β = 1.2 / 2 / 5 / 10 / 25 | **bitwise**, all five (β = 5, 10, 25 park and release) | 12 015 / 22 719 / 42 940 / 42 848 / 52 783 | 0 | 57.48–86.07, 31.119–31.792 |
| M = 10⁻³, β = 0.5 / 1.2 / 2 / 3 | **bitwise**, all four (β = 1.2 releases) | 24 923 / 18 434 / 50 495 / 61 868 | 0 | 25.22–100.69, **25.180**–32.016 |
| natural mode, M = 0.5, β = 2 | **bitwise** | 0 | 0 | — |
| also, at both trees: β = 1.2, 2, 3 at M = 10⁻²⁷; β = 2 at 10⁻¹²·⁵⁰, 10⁻¹²·⁷⁵ and 10⁻²⁶·²⁵ to 10⁻²⁸·⁰⁰ | **bitwise**, all 12 | 39 166–223 743 | 490–4 070 | 75.14–102.78, 31.585–32.044 |

- **Every history is bitwise**, warnings included: the 28 of prompt §2.4, 12 more, and §5.5's one.
- **The reclassified calls are where the field comes within 16 quanta of the floor**: at M = 10⁻¹²
  (u at φ* = 5 M_P is 3.5q) and 10⁻³⁰, and at the edges. Each such search starts from −∞ or meets it,
  goes on to the same bracket and the same root, and leaves every value bitwise. Ruling 5's expected
  case.
- **No margin approaches condition (3).** The smallest m_D is 25.18, at M = 10⁻³, β = 0.5 (m = 25.22).
- The worktree `agent07b/wt-dispatch` was removed with `git worktree remove`. `git worktree list` shows
  the main checkout alone.

### 5.5 Natural mode in the band (prompt §2.5)

M = 10⁻¹⁶ M_P, β = 2, α = 0. It reaches T_CMB at N_final = 50.07564 after 472 218 steps, and is
**bitwise** the dispatch commit's on the 27 keys, warnings included. The warnings, the same at both
trees, are one line:

> natural mode: the classical turning point lies below 1e-08 M_P (phi_t = 9.43e-19 M_P at the floor
> reflection at N = 20.35210038), where dissipation mode would reflect without resolving it; this
> history may fail on Radau's float spacing or the step budget

**The text did not change.** Natural mode calls the wall search once, at its first floor reflection
(`natural_floor_warning`). There the field is at the wall, not in the band: 18 helper calls, none
reclassified, margin m_D = 32.39. The dispatch commit gave no "could not be assessed" warning.

### 5.6 The cost (prompt §2.6, ruling 11)

**The helper**, the minimum of 7 rounds of 10⁵ calls (load 4.1):

| Input | shipped | final |
|---|---|---|
| band, D̂ = 1 quantum (final: −∞) | 94.5 ns | 172.1 ns |
| band, D̂ = 17 quanta (finite) | 94.4 ns | 183.0 ns |
| far, D = 1.0 | 98.3 ns | 187.9 ns |
| far, D = 100 | 93.5 ns | 183.1 ns |

The width adds about 85 ns per call: an `ulp`, a `max`, two `abs` and a chained comparison. A β = 2
history makes 160 000–193 000 calls, which costs about 15 ms more, 0.2 % of its 6.3 s.

**Histories**, the minimum of three rounds, dispatch and final back to back, no spy:

| History | dispatch commit | final tree |
|---|---|---|
| β = 2, M = 10⁻¹⁶ | 0.2 s (fails at N = 20.345) | **5.9 s** (5.9, 6.6, 6.2) |
| β = 1.2, M = 10⁻²⁰ | 0.3 s (fails) | 1.6 s (1.7, 1.7, 1.6) |
| β = 3, M = 10⁻¹⁶ | 0.3 s (fails) | 7.7 s (15.4, 7.8, 7.7) |
| β = 2, M = 10⁻³⁰ | 6.2 s (6.7, 6.6, 6.2) | **6.2 s** (6.9, 6.2, 6.5) |

The load was 5.3 at the start and 28.4 at the end ( 8:47  up 25 days,  7:32, 1 user, load averages: 6.62 12.02 11.82). A band history costs what the M = 10⁻³⁰
one does: 5.9 against 6.2 s at β = 2. The width costs nothing measurable at M = 10⁻³⁰ (6.2 s at both
trees), against the 15 ms its 193 000 helper calls predict. The spy of ruling 5 adds under 10 %.

## 6. The deliberate-breakage record (P11)

- **The replay.** `make.py` wrote B2–B4 against the final `AbstractModel.py` (difflib), and B1 by hand
  so that its hunk shows the two lines removed. `replay.sh`, run in bash, does the following for each
  one:
  - checks it with `git apply --check`, applies it, and checks it with `git apply -R --check`;
  - runs `test_wall_rounding` and `test_wall_model` (34 tests);
  - reverts it, and compares the file's md5 with the final one.

  All four were restored. Both scripts are in `agent07b/breakages/`; none is committed.
- **The final replay** was against the final test module, after D4. Before D4, B2 failed nothing (D4).
- **`test_wall_model` passed under every breakage**, the bump test included.

**B1 — the shipped helper restored** (items 1, 2, 3). Fails 16:
- item 1's `j_from_0_to_K` (−306.44 != −inf at j = 2);
- item 2's twelve subtests, each an ERROR: the method raises "not monotone" on the input it should
  solve;
- item 3's three tests. The history fails at N = 20.345 with "not monotone".

Item 2's synthetic case passes, since its probes are all above the width.
```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -142,9 +142,6 @@
     by ln V's rounding. ln V = +inf has an infinite width and is not within it: it returns +inf.
     """
     if not log_V > log_V_floor:
         return _NEG_INF
-    width = WALL_ROUNDING_QUANTA * ulp(max(abs(log_V), abs(log_V_floor)))
-    if log_V - log_V_floor <= width < float("inf"):
-        return _NEG_INF
     return log_V + log(-expm1(log_V_floor - log_V))
 
```

**B2 — a width of one quantum, K = 1** (item 1). Fails 2:
- `test_1_the_width_is_the_derived_one` (1 != 16);
- `test_1_j_from_0_to_K_returns_minus_inf` (−306.44 != −inf at j = 2).

Items 2 and 3 pass, as do ruling 4's case and the twelve inputs: every failure of the band is the
doubling's first comparison, which K = 1 already fixes (§1.2). No input can make the binding comparison
fail at K = 1 while K = 16 resolves it (§2). So B2 is caught by item 1 alone.
```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -109,7 +109,7 @@
 # compares) are ordered if (e^{s 2^-H} - 1)(K + 1 - rho) > 2 rho, for a model whose ln V - ln V_floor
 # is rounded within rho quanta and has |d ln(ln V - ln V_floor)/d ln phi| >= s. The exponential
 # (s = n = 1, rho = 1/2) needs K > 15.005; K = 16 is the next integer, with no margin beyond its 6 %
-WALL_ROUNDING_QUANTA = 16
+WALL_ROUNDING_QUANTA = 1
 # the one-loop fixed point: from ell_0 = 32 (eq:chain-turn) until |delta ell| <= 1e-12 ell
 ONE_LOOP_ELL_START = 32.0
 ONE_LOOP_ELL_RTOL = 1.0e-12
```

**B3 — < in place of ≤** (item 1). Fails 1: `test_1_j_from_0_to_K_returns_minus_inf`
(−304.36 != −inf at j = 16).
```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -144,7 +144,7 @@
     if not log_V > log_V_floor:
         return _NEG_INF
     width = WALL_ROUNDING_QUANTA * ulp(max(abs(log_V), abs(log_V_floor)))
-    if log_V - log_V_floor <= width < float("inf"):
+    if log_V - log_V_floor < width < float("inf"):
         return _NEG_INF
     return log_V + log(-expm1(log_V_floor - log_V))
 
```

**B4 — the width in units of ulp(ln V − ln V_floor)** (items 1, 2). Fails the same 16 as B1, item 3
included. For a normal D > 0, ulp(D) ≤ 2⁻⁵² D, so D ≤ 16 ulp(D) never holds, and the helper is the
shipped one.
```diff
--- a/CosmologyConcepts/Models/AbstractModel.py
+++ b/CosmologyConcepts/Models/AbstractModel.py
@@ -143,7 +143,7 @@
     """
     if not log_V > log_V_floor:
         return _NEG_INF
-    width = WALL_ROUNDING_QUANTA * ulp(max(abs(log_V), abs(log_V_floor)))
+    width = WALL_ROUNDING_QUANTA * ulp(log_V - log_V_floor)
     if log_V - log_V_floor <= width < float("inf"):
         return _NEG_INF
     return log_V + log(-expm1(log_V_floor - log_V))
```

## 7. Observations not acted on

- **The one-loop gap is where the band's energy is.** At β = 2, M = 10⁻¹⁶ M_P, generations carrying
  59 % of K have 1 < 𝓡 < 10^2.92, where the one-loop chain raises. That is 88 % at β = 1.2 and 52 % at
  β = 3. R19 already rules the estimate there (classical, flagged), and 08 closes
  `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]`. Recorded for 08's prompt, not opened.
- **A field inside its turning point within the width now raises instead of returning `None`**, for
  roots within about 17 quanta of the floor (E ≲ 10⁻¹² V_floor; §1.3). No history measured came near
  it: the smallest m_D is 25. A caller that met it would fail the history. Not opened: it is the
  unresolvable region this prompt defines. Noted for 08, whose per-generation calls use the same
  method.
- **"Not monotone" does not prove a non-monotone model.** Below the width's edge the search raises
  "a midpoint … is not between its ends" for a monotone potential (§1.3). The text is the search's,
  which this prompt does not change (§6). A clearer text would name the rounding.
- **Item 2's tests raise rather than fail under B1 and B4.** They are ERRORs, because the method raises
  inside the test. The breakage is caught either way.
- **`assess_infall`'s docstring** still reads "ln(V - V_floor) = -inf where V rounds to V_floor or
  below". That is right for its own expression, which has no width; it is not the helper.

## 8. State handed to the next prompt

- **The helper** (`CosmologyConcepts/Models/AbstractModel.py`). `_log_V_minus_floor(log_V, log_V_floor)`
  is −∞ where V ≤ V_floor, or ln V − ln V_floor ≤ 16 ulp(max(|ln V|, |ln V_floor|))
  (`WALL_ROUNDING_QUANTA` = 16). Elsewhere it is the shipped expression, and +∞ stays +∞. The methods,
  the search and its constants are unchanged.
- **What it gives up.** A root with E ≲ 10⁻¹² V_floor (D_t ≲ 17 quanta) raises "not monotone (a
  midpoint … not between its ends)". A field inside such a root, within the width, raises too.
  `outside_classical` is True where V(φ_ta) is within the width.
- **The band** (dissipation mode, α = 0.1, test_injection's setup) now runs everywhere measured:
  β = 1.2, 2, 3 × M = 10⁻¹³ to 10⁻²⁷ M_P, parking at #6, #48 and #58 at N = 28.364, 30.349 and 29.394,
  with Σ + leftover M-independent to 3×10⁻⁸.
- **The band's edges at the dispatch commit**, β = 2: from 10⁻¹³·⁰⁰ (10⁻¹²·⁷⁵ runs; predicted
  10⁻¹²·⁸⁰) to 10⁻²⁶·⁰⁰ (10⁻²⁶·²⁵ runs), contiguous on the quarter-decade grid. At β = 1.2 and 3, every
  decade from 10⁻¹³ to 10⁻²⁵ failed, and 10⁻²⁷ ran.
- **The gap table for 08** (§5.3, R19):
  - the one-loop chain raises on every candidate with 0.05 ≤ log₁₀ 𝓡 ≤ 2.92 ("ell fell to …") and
    solves on every one with log₁₀ 𝓡 ≥ 3.00, `outside_classical` True, ℓ = 0.34–26.7. No `None`.
  - **These histories cross the gap**: β = 1.2 at M = 10⁻¹⁶, 10⁻¹⁷, 10⁻²⁰; β = 2 at 10⁻¹⁵ to 10⁻²⁰;
    β = 3 at 10⁻¹⁵ to 10⁻¹⁸.
  - The share of K in the gap is 0.88, 0.59 and 0.52 at M = 10⁻¹⁶ (β = 1.2, 2, 3), 0.28 and 0.15 at
    10⁻¹⁵ (β = 2, 3), and 10⁻²–10⁻³ further down.
  - Every candidate is classical (𝓡 < 1) at M ≥ 10⁻¹⁵ (β = 1.2) and 10⁻¹⁴ (β = 2, 3). None is in the
    gap at M ≤ 10⁻²² (β = 1.2, 2) and 10⁻²⁰ (β = 3).
- **The cost**:
  - the helper is 172–188 ns per call against the shipped 94–98 ns, about 15 ms on a β = 2 history;
  - a band history takes 1.6–7.7 s at the final tree, as at M = 10⁻³⁰ (5.9 against 6.2 s at β = 2).
- **Tests**: `ComputeTargets/tests/test_wall_rounding.py` (12).
- **Counts after this prompt:** CosmologyModels 26, ComputeTargets 439 (96 + 343), ObjectFactories 103,
  tools 115, RunDrivers 64. `CLAUDE.md`'s counts are still those of `tuning-followups` 02 (2026-10-27);
  this campaign's close (12) updates them.
- **Issues:** closed `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`; opened none.
