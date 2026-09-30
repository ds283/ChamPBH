# Prompt 03 — The adiabatic mass: the source-response term

**Campaign:** [`README.md`](README.md) · **Board item:** **P3** ·
**Board:** `IMPLEMENTATION_STATE.md`. Update your row and P3.
**Closes:** `[00-adiabaticity-diagnostic-omits-the-source-response-term]` on the
`review-remediation` board (review H5, audit §5). **Recommended model:** **Opus**. The code change
is a new EOS method, one term in the mass, and a non-singular route to Q's numerator. The work is the derivation, and a reference
independent of it that decides between the two closed forms on offer.

**Read first:**

1. [`README.md`](README.md) §0, **§2 (c)–(h) in full** (§2 (e) and (h) amended 2026-09-30), §5,
   §6.3.
2. `.documents/audit-2026-09-29/README.md` §5, and the `review-remediation` board's entry for the
   issue above. Both are evidence. **README §2 (c) says the audit's formula is missing a factor**,
   and that is evidence too.
3. [`planning-probes/h5_bracket_probe.py`](planning-probes/h5_bracket_probe.py). Run it (≈ 60 s)
   and read it. Your reference replaces it; it is not a template to copy. Also run
   [`planning-probes/q_sign_change_probe.py`](planning-probes/q_sign_change_probe.py) (a few
   seconds), the measurement behind A3.
4. `ComputeTargets/AdiabaticHistory.py`, the whole file.
5. **Read only; you do not change it:** `ComputeTargets/ScalarModel.py`:
   - `:195–275`, `ODEPolicy.__call__`, which defines Σ, f_m, E and R and gives the kicking term;
   - `:325–365`, `ODERHS`, which gives d ln ρ_R,E/dN, d ln f_m/dN and d ln T_J/dN.

   The three response relations of README §2 (c) are read off these lines. Check them yourself.
6. `CosmologyModels/GenericEOS/`:
   - `GenericEOS.py`, the base class and its `w`;
   - `SaikawaShirai_EOS_spline.py:100–235`, its derivatives and its `w` with the 2 MeV freeze;
   - `Xav_EOS_spline.py`, the production `w`, a spline in ln T;
   - `SaikawaShirai_EOS_jax_autodiff.py:156–245`;
   - `LambdaCDM_GenericEOS.py:100–120`, the forwarding.
7. `CosmologyConcepts/ConformalCouplings/`, the interface: `log_Omega`, `d_logOmega_dphi`,
   `d2_logOmega_dphi2`.
8. `ComputeTargets/Policies/PotentialDerivativePolicy.py:399`, `Hdot_over_H2_plus_3`.
9. `CosmologyModels/tests/eos_reference.py`, the existing helpers.
10. `.documents/numerical-strategies.md` §5 and `.documents/numerical-methods-for-paper.md` §5.

---

## 1. First, the derivation — in the log, before any code

Derive the φ-derivative of the source term in V_eff′ at fixed a_E and fixed comoving entropy,
from the three response relations. **Re-derive the relations yourself from `ScalarModel.py`; do not
take them from the README.** State:

- the result in physical variables and in the code's (E, R, Σ, f_m, x, Σ_T);
- the limits f_m → ∞ and Σ = Σ_T = f_m = 0;
- where it and audit §5's form differ, and why.

If your derivation gives a third form, keep it. The reference (§3) decides, and §6 below says what
happens then.

---

## 2. The changes

**A1 — `dw_dlogT` on every EOS class** (README §2 (d)).

- **The interface.** `dw_dlogT(T: TemperatureLike) -> float` is d w/d ln T and dimensionless. It
  goes on `GenericEOSBase` (non-abstract), and `LambdaCDM_GenericEOS` forwards it.
- **Each class is consistent with its own `w`:**
  - `Xav_EOS_spline`: `self._spline.derivative()` at ln T, and 0 exactly where `w` returns 1/3
    without the spline. Build the derivative spline once, in `__init__`.
  - `SaikawaShirai_EOS_spline`: 0 at and below `_EOS_T_LO`, where `w` freezes its argument. Above
    it, d/d ln T of 4g_s/(3g_ρ) − 1 from `dG_s_dlogT` and `dG_rho_dlogT`.
  - `GenericEOSBase`: the same formula, without the freeze.
  - the jax class: autodiff of its own `w`, with the same freeze.
- **No finite difference of `w` anywhere in production code.**

**A2 — the term.** In `AdiabaticComputePolicy.M2eff_over_H2`, add the (ln Ω)′² term of README
§2 (c), as your derivation and the reference confirm it.

- **The signature.** The method gains `T_Jordan`. `compute_adiabatic_values` passes
  `exp(value.log_T_Jordan)`, never anything derived from `z` (README §2 (f)).
- **The ingredients:**
  - Σ_T = −3 `cosmology.dw_dlogT(T_J)`;
  - x = `dG_s_dlogT / G_s / 3` at T_J;
  - Σ is the stored `value.Sigma`, as the existing terms use.
- **Overflow.** Guard (Σ² − Σ_T/(1 + x) + f_m)/(1 + f_m) for large f_m as `R` is guarded.
- **Consider factoring the conformal part into a pure function** of
  (E, Σ, f_m, (ln Ω)′, (ln Ω)″, Σ_T, x). It makes the reference comparison clean. The choice is
  yours; record it.
- **The existing (ln Ω)″ R term, the self mass and the gravitational mass do not change.**

**A3 — Q's numerator in its smooth form** (README §2 (e); amended 2026-09-30).

- **The problem.** `compute_adiabatic_values` gets A·C by splining log|H² M²_eff/H²| against N and
  multiplying its derivative by A. That route is singular where M²_eff changes sign, although A·C
  is not, and the new term makes such crossings more likely.
- **The change.** Replace it with A·C = m (1 + Ḣ/H²) + ½ dm/dN, where m = M²_eff/H² and Ḣ/H² is
  `Hdot_over_H2_plus_3` − 3 at each sample.
  - First re-derive the identity yourself and put it in the log.
  - Take dm/dN from a representation that is accurate both through m = 0 and across a bounce's
    dynamic range. A spline of asinh m, with dm/dN = √(1 + m²) d asinh m/dN, is one (README §2 (e)
    table); an analytic dm/dN is another. Record the pick as an IMPLEMENTATION CHOICE, with its
    alternatives.
- **What does not change.**
  - A, B, the |B|^{3/2} and the modulus in Q.
  - `Q_labels` and what is stored.
  - No history is failed, floored or clipped because M²_eff crosses or touches zero; that is
    README §4's stop.
  - `log` of |M²_eff| no longer appears on the path to Q. If you keep `log_abs_M2eff_grid` for
    anything else, it must tolerate zero.

**A4 — the version comment.** `VERSION_LABEL` is already `"2026.3.0"` (prompt 02). **Do not bump
it.** Add a dated sentence to `main.py`'s comment: from 2026.3.0 `AdiabaticHistory`'s M²_eff
includes the source response. If the label is not `"2026.3.0"` when you start, stop.

**A5 — documents, additively** (CLAUDE.md rule 6).

- **`.documents/numerical-strategies.md` §5.2.** A dated subsection giving the four pieces of
  M²_eff as they now are, Σ_T's source, and the smooth form of Q's numerator. It corrects §5.3's
  account of the log-spline, and states the three assumptions of README §2 (h).
- **`.documents/numerical-methods-for-paper.md` §5.** A dated addendum beneath the H5 bullet. It
  gives:
  - the term as implemented;
  - that the note's quoted form (from the audit) is missing the 1/(1 + x) on the log-derivative,
    or whatever your reference established;
  - the bracket's range from your test;
  - that the matter piece is the standard β² ρ_m/M_P²;
  - that the paper's claim that the modulus lets sign changes pass "without loss" was not true of
    the old code, and what the code does now;
  - the three assumptions of README §2 (h), and the fixed-k_p/H question as the board states it.

  The authors rewrite the paper's sentence from it. Do not edit the paper.

---

## 3. Tests

### 3.1 `CosmologyModels/tests/test_eos_w_derivative.py` — no solve, a few seconds

On ≥ 1000 log-spaced points over [12 keV, 20 TeV], `dw_dlogT` against a central difference of `w`
in ln T (half-step 1e-4). Each must be **≤ 1e-6 absolute**:

- `Xav_EOS_spline`, everywhere (probe: 1.0e-7 at 150 MeV);
- `SaikawaShirai_EOS_spline` and the base-class formula, above 2 MeV, and exactly 0 at and below
  2 MeV for the spline class;
- the jax class, `skipUnless` jax imports.

Avoid putting a grid point within one half-step of a clamp. Say how in the docstring.

### 3.2 `ComputeTargets/tests/test_adiabatic_mass.py` — no Ray cluster, no datastore, no solve

- **(a) The reference.**
  - **Build it** from the defining relations only: T_J(φ ± h) from T_J Ω g_s(T_J)^{1/3} = const,
    root-found with `G_s`. **Never `dG_s_dlogT` or `dw_dlogT`.**
  - ρ_R,E(φ ± h) from d ln ρ_R,E = Σ d ln Ω, integrated across the step accurately enough.
  - ρ_m,E ∝ Ω.
  - Central difference of the force (ln Ω)′ (Σ ρ_R,E + ρ_m,E) in φ.
  - **Show in the test that it converges** at the expected order.
- **(b) The exponential coupling.**
  - Through `M2eff_over_H2` itself (or through your pure function, plus a check that
    `M2eff_over_H2` calls it with the cosmology's Σ_T and x at the T_J it is given), for f_m ∈ {0,
    1, 100}, on ≥ 1000 points over [12 keV, 20 TeV].
  - Target: **≤ 1e-6 absolute** in the bracket norm of README §6.3, at a step giving
    Δ ln Ω = 1e-4 (probe: 9.9e-8).
  - Subtract the self and gravitational parts, or isolate the conformal part, so that the
    potential does not enter the comparison.
- **(c) A coupling with (ln Ω)″ ≠ 0**, a stand-in, for example ln Ω = βφ/M_P + γ(φ/M_P)². Same
  comparison and tolerance, in a norm you state that includes the (ln Ω)″ term.
- **(d) The matter limit.** Exponential coupling, f_m = 1e6: the new term equals
  β² ρ_m,E/(M_P² H²) to 1e-6 relative.
- **(e) Q's numerator.** Use the two synthetic histories of README §2 (e) at ΔN = ln 10/250, with
  H² ∝ e^{−4N}.
  - **Accuracy.** A·C against its analytic value is **≤ 1e-4 of max |A·C| on both**. The probe
    measures 1.1e-5 and 9.3e-6 for the asinh spline; the old log route gives 1.8 on the crossing
    history.
  - **An exact zero.** Put one sample exactly at m = 0. Q there is finite and equals
    ½ (dm/dN)/(k_p/H)³ to 1e-4 relative, and nothing raises.
  - Reach the loop through `compute_adiabatic_values._function` with stand-ins, or through a
    factored pure helper; say which.
  - **This must fail on `HEAD~1`**, on the crossing history's accuracy or on the exact zero.
- **(f) The audit's form**, computed in the test, is compared against the reference, and the
  largest difference is reported with `CHAMPBH_TEST_REPORT=1`. **It is not asserted either way.**
  It is a measurement for the log.

**The breakage check.** The orchestrator will run (b) against `HEAD~1`'s `AdiabaticHistory.py`.
It must fail because the term is missing, not only because the signature changed. Make that
possible: for example, keep the new argument optional in the test's call path, or record in the log
the reference against the old formula's conformal part (max |bracket| ≈ 0.41 by the probe).

---

## 4. What this prompt does not do

- **No change to `ScalarModel.py`**, the ODE, the kicking term or `PotentialDerivativePolicy`. The
  background evolution is untouched; only the diagnostic changes.
- **No use of Σ_g** (README §2 (d)).
- No change to Q's definition, to `Q_labels`, or to the `AdiabaticHistory` schema. A3 changes
  only how Q's numerator is computed.
- No change to the choice of fixed k_p/H scales (README §2 (h); a board issue for the authors).
- No change to `Xav_EOS_data.csv` or the Saikawa–Shirai coefficients.
- No second version bump.

## 5. Acceptance

1. README §6.3, every row, with measured values in the log.
2. `git diff --stat HEAD~1 HEAD` does not contain `ComputeTargets/ScalarModel.py`.
3. Both suites pass and both counts rise. `black --check` is clean on the changed files.
4. The board and the index:
   - P3 done;
   - the issue closed: its row deleted from `.documents/OPEN_ISSUES.md`, and a dated **Resolved**
     line added to its `review-remediation` board entry (README §5 rule 4);
   - count and date corrected.

## 6. Stop conditions — stop and ask the user

- **The reference agrees with neither README §2 (c)'s closed form nor your own derivation.**
  Report all three on the probe's Table 1 temperatures.
- **The reference agrees with your derivation, and that differs from README §2 (c).** This touches
  a §2 fact. Report both, and stop before implementing either.
- The fix appears to need a change to `ScalarModel.py`, or Σ_g in place of the ODE's Σ.
- A class's `dw_dlogT` cannot be made consistent with its `w` to 1e-6.
- The smooth form of A3 does not reproduce the log route to 1e-4 relative on a history with no
  sign change (the spike case). Then one of the two is wrong; report both.
- A3 appears to need a change to Q's definition, to `Q_labels`, or to what is stored.
- `VERSION_LABEL` is not `"2026.3.0"` when you start.

## 7. The log and the board

`logs/03-adiabatic-source-response.md`, in the README §5.1 template. Beyond the template:

- the derivation (§1);
- the reference's convergence table;
- Table 1 of the probe, re-measured through your code and your reference;
- the audit-form measurement (test (f));
- the A3 identity, the representation chosen for dm/dN, and test (e)'s numbers;
- the bracket's minimum and maximum over the grid, with their T_J.

Board: P3 done; the header's consequence line says **no `AdiabaticHistory` row made before 2026.3.0
is comparable**.
