# Corrections to the paper's numerical section (for the authors)

**Written:** 2026-10-01, by prompt 03 of the `integrator-remediation` campaign.
**Paper:** `Paper1.tex` (outside this repository, at
`/Users/ds283/Documents/Git paper repositories/Chamlelon PBHs/Paper1.tex`), subsection
"Numerical implementation", `\label{NumericalSection}`, lines 2868–3030; the paragraphs
"Stiffness" (2932), "Resolving the reflections" (2978) and "Reflecting boundary condition"
(3014). Line numbers are those of the file read on 2026-10-01 and will move.
**Not edited.** Nothing in the paper has been changed; this is a list for the authors to act on.

**Code.** "Then" is tree `b1f64d8` (`VERSION_LABEL 2026.4.0`), the tree the audit measured.
"Now" is the tree after prompt 02 of the campaign (`614c41a`, `VERSION_LABEL 2026.5.0`). Every
earlier-store history is invalid; a result in the paper that was computed with the old scheme
should be recomputed or labelled as such. "Audit" is
[`integrator-audit-2026-09-30/README.md`](integrator-audit-2026-09-30/README.md), whose scripts
are named; "log 01/02" are `prompts/integrator-remediation/logs/`. The mechanism, parameters and
exception table are in [`numerical-strategies.md` §3.5](numerical-strategies.md).

**How to read the table.** Each row is one sentence or clause of the paper that disagrees with the
code. "Then" is a statement about `b1f64d8`. Rows 1, 2, 5, 6 and 7 were already wrong about the
code before this campaign: the paper described something the code never did. Rows 3, 8 and 9
described `b1f64d8` accurately and are wrong now because the campaign replaced the mechanism.
Row 4 is the replacement text. Suggested sentences are drafts.

## 1. Sentences that disagree with the code

| # | Paper sentence (approx. line) | Then (`b1f64d8`) | Now (`614c41a`) | Measurement | Suggested replacement |
|---|---|---|---|---|---|
| 1 | "Outside both regions the maximum step is of order $10^{-2}$ e-folds, and the solution advances freely through the slow cosmological drift." (2994–2996) | `default_max_step` is `inf` (`ExponentialPotential.py:69–71`, `DEFAULT_MAX_STEP_SIZE = inf`): no limit outside the regions, so the step there was set by the error estimator alone | A global cap of $0.1$ e-folds, and below it the kinematic cap of row 4, which is far larger than $10^{-2}$ except near the wall. No regions | Audit §1; §9.1; log 01 (cap in `StepControl`: `global_max_step = 0.1`). A fixed $10^{-2}$ outside protects the first bounce at $M = 0.5$ and nothing smaller: audit §3.1 table (`p_total2.py`) | "Away from the wall the step is limited only by the error estimator and by a ceiling of $0.1$ e-folds." |
| 2 | "Inside the outer region it is reduced to $10^{-5}$ e-folds, and inside the inner region to $10^{-6}$ e-folds" (2997–2998) | The caps were $3\times10^{-3}M$ and $10^{-4}M$ e-folds (outer boundary $1.5M$, inner $0.05M$, caps boundary/500). These equal $10^{-5}$ and $10^{-6}$ only for $M \approx 3\times10^{-3}$ and $10^{-2}$ respectively; at $M = 0.5$ they were $1.5\times10^{-3}$ and $5\times10^{-5}$ | There are no regions and no fixed caps. The maximum step is set before every step from the field's velocity and inward acceleration (row 4) | Audit §1 (code map); §3.2 (all 299 939 steps of the parked window were the cap $5\times10^{-5}$: 2 099 582 RHS against 18 880); `p2_parked.py regions`, `p2_caps.py` | Delete; replace with the sentence of row 4. |
| 3 | "Two nested regions in field space are defined around the repulsive arm of the potential. The integration is halted when $\phi$ crosses either boundary, the maximum permitted step is reset, and the integration is restarted from the crossing point." (2989–2993) and "The crossings themselves are located by bracketed root-finding on the interpolated solution, so the switching points are determined to an accuracy comparable to the tolerance of the integration." (3002–3005) | As described, with six terminal events and `SolutionFragment`s (restarting Radau at each crossing); each rebound that grazed the outer boundary crossed it twice: 85 fragments, 5 188 284 RHS on the grazing window, and the full $\beta = 1.2$, $M = 0.01$ history died of the 100-fragment failsafe at $N = 37.165$ | Gone: no regions, events, fragments or restarts. One Radau instance is stepped under the cap; it is rebuilt only at an elastic reflection. The one crossing that is still located by bracketed root-finding on the interpolant is the end point, $\ln T_J = \ln T_{\rm CMB}$ | Audit §3.3 (`p3_grazing.py regions`); §3.4 (no measurable effect of the restarts on the solution); log 01 (P3: 38 547 RHS, 0 restarts, 51 bounces, $\phi(37.5) = 5.8078\times10^{-4}$, both schemes) | Delete the three sentences. See row 4. |
| 4 | (no sentence; replaces rows 1–3.) The paragraph's premise, "Local error control alone is not a reliable guide here, because the field can traverse the entire steep region within a single trial step", is correct and measured (audit §3.1) and should stay | — | The maximum step is $h \le f\phi/|\phi'|$ if $\phi' < 0$ and $h \le \sqrt{2 f \phi/|\phi''|}$ if $\phi'' < 0$ (the inward acceleration), with $f = 0.1$, so that the inward displacement in one step is at most a fraction $f$ of the field's distance to the origin whatever $M$ is; both terms are needed. The cost of an approach is about $10\ln(\phi_{\rm start}/\phi_{\rm wall})$ steps, independent of $M$ | Audit §9.1 (derivation; the velocity-only hole at $\beta = 3$, $M = 0.01$, $N = 34.60$, `p_full.py … kin`); §3.6 (1 945, 2 120, 2 275 RHS for the first reflection at $M = 0.5, 0.01, 0.001$, `p1_smallM.py`, `p_total2.py`); log 01 (nine full histories complete in 24 193 – 327 046 RHS) | "We therefore limit the step so that, in one step, the field cannot move towards the origin by more than a fraction $f = 0.1$ of its current distance from it. The bound uses the field's velocity, and its acceleration, because a field at an outer turning point has no velocity yet can still fall through the wall in a single step. This cap is independent of $M$ and is imposed before every step on top of the error control; away from the wall it is not active." |
| 5 | "If the integration fails, it is retried from the beginning with a backward differentiation formula method, then with a method that switches automatically between stiff and non-stiff formulations, and finally with an explicit Dormand–Prince $8(5,3)$ scheme as a last resort." (2950–2956) | Never wired: `solver_list = ["Radau","BDF","LSODA","DOP853"]` was walked, but `solve_ivp` was called with the literal `method="Radau"` (since `f67bc3a`, 13 Jan 2026), so a failing history was integrated four times identically before it was recorded as a failure | One stepper, Radau; `solver_list` and the cascade are deleted. A history that fails is recorded as a failure (one attempt) and the survey continues | Audit §1 (`git log -S'method="Radau"'`), §4; log 02 (`test_g_no_fallback`). Whether an explicit method is viable was not measured, but the system's stiffness was: $|\partial\phi''/\partial\phi| = 1.3\times10^5$ at $M = 0.01$ and $1.1\times10^6$ at $M = 0.001$ in the matter-era well, so an explicit method would be stability-limited to a few $10^{-3}$ e-folds throughout (audit §4) | "If the integration fails for a given parameter point, the point is recorded as a failure and excluded from the survey." (The commented lines 2957–2961 say much the same; they are right if "all four" is dropped.) |
| 6 | "For very steep potentials, $M/\Mp \lesssim 10^{-3}$, these tolerances are relaxed to $10^{-5}$ and $10^{-6}$ respectively." (2968–2970) | Not in the production path. Only `ReclinerPotential` has such an override (`default_abs_tol`/`default_rel_tol`); the production potential, `ExponentialPotential`, returns $10^{-8}$ for both, and nothing reads the overrides: `main.py` passes `--abs-tol`/`--rel-tol`, both defaulting to $10^{-8}$. Unless the command line said otherwise, no stored history used relaxed tolerances (not settled from the repository: the stored `tolerance` rows would say) | Unchanged: $10^{-8}$ and $10^{-8}$ for every $M$. Inside the cap the tolerance does not set the cost, and loosening it changes the trajectory (two bounces instead of one by $N = 21$ at $M = 0.001$) | Audit §1, §6; §3.6 table (`p1_smallM.py`: shipped regions at $10^{-8}$ and at $10^{-5}/10^{-6}$ cost 26 422 and 25 391 RHS) | "Absolute and relative tolerances are both set to $10^{-8}$ for every parameter point." Delete the following sentences up to "…more useful than an accurate solution that cannot be computed." |
| 7 | "In this regime the reflection off the repulsive arm is so sharp that a tolerance of $10^{-8}$ drives the step size below the point at which the integration can make progress" (2972–2974) | Not reproduced. The $M = 0.001$ reflection completes at $10^{-8}$ under the shipped scheme and under the cap (26 422 and 2 275 RHS) | What does limit progress is different: steps below about $10^{-13}$ e-folds are not representable at $N \sim 20$–$55$, and resolving the wall needs $h \approx 2\times10^{-3}M$, so a reflection can be resolved only for $M \gtrsim 10^{-8}$. Below that the code reflects the field elastically at a representable step (row 8). This depends on $M$, not on the tolerance | Audit §3.6 (`p1_smallM.py`), §3.7 (`p_smallM_scan.py`), §6, §9.3 (full $M = 0.001$ histories complete at $10^{-8}$); log 01 | Delete with row 6, or: "The wall cannot be resolved by any step representable in double precision for $M \lesssim 10^{-8}\Mp$; there the reflection is treated as instantaneous (below)." |
| 8 | "For the steepest potentials the strategy above is not always sufficient, and the field can reach a point at which the potential gradient can no longer be evaluated reliably. We define such a point for each potential and treat it as a hard boundary. If the field reaches it, as a fallback measure, the integration is restarted from that point with an elastic reflection (i.e., the sign of $\phi'$ is reversed)." (3015–3025) | The point was `hard_reflection_point`, $\phi = 0$ for the exponential potential, detected as a downward crossing by an event. It was not a recovery: for $M \gtrsim 10^{-13}$ the root lies inside the wall, so on the $\phi = 0^+$ side Radau failed at once and on the $0^-$ side the field ran on at $\phi < 0$ to $T_{\rm CMB}$ with no wall, which would have been stored as a success. It was right only for $M \lesssim 10^{-13}$, and the shipped scheme failed outright for $10^{-13} \lesssim M \lesssim 10^{-8}$ (it died at $M = 10^{-10}$) | It is a deliberate model, with a stated trigger. Before each step, if $\phi' < 0$ and the cap $f\phi/|\phi'|$ would fall below a floor of $10^{-11}$ e-folds, the sign of $\phi'$ is reversed at that point and the solver is restarted. Two checks guard it: the potential must declare that it reflects at the origin (only the exponential does), and the wall part of the potential energy at the moment of reflection must not exceed the kinetic energy; otherwise the history fails. It is not a failure fallback, and $\phi \le 0$ is never reflected: it is a failure. Where both exist the reflected and the resolved answers agree to seven digits | Audit §3.5, §3.7 (`p_smallM_scan.py regions|kin|kinref`); §9.1; log 01 (reflection at $\phi = 4.70\times10^{-11}$, $\phi(21) = 1.184428\times10^{-1}$ for every $M$ from $10^{-6}$ to $4.1\times10^{-28}$; largest wall/kinetic ratio at any legitimate reflection $1.76\times10^{-20}$, against $\approx 23$ for a step-over state; the guard needs ratio $\le 1$) | "When the wall is thinner than the smallest representable step (for the exponential potential, $M \lesssim 10^{-8}\Mp$), the bounce cannot be resolved, and we replace it by an instantaneous elastic reflection ($\phi' \to -\phi'$) applied when the step the cap would allow falls below $10^{-11}$ e-folds. Because the wall force conserves $\tfrac12\phi'^2 + V/(3H^2\Mp^2)$, the bounce is elastic, and the error is the change in the background during the flight neglected, under $2\times10^{-10}$ e-folds, which is below the integration tolerance. Where the bounce can be resolved the two treatments agree to seven digits. The number of reflections is recorded with each history." The final two sentences of the paragraph (the reflection "discards any energy loss"; "acceptable in view of §EPsection") stay, with "as a fallback measure" deleted |
| 9 | The sentence on 3006–3007, "The complete solution is assembled from the individual fragments produced between crossings." | As described | The solution is one piecewise interpolant over every accepted step (a `scipy` `OdeSolution`) evaluated on the output grid | Log 01 (sampling) | Delete, or: "The solution is the piecewise interpolant through every accepted step, and is evaluated on the output grid." |

Row 1 should be read together with row 4: the "$10^{-2}$" outside figure is not what the code did
and not what it does now.

## 2. Statements the audit and the campaign leave correct

These need no change. They are listed so that the authors know they were checked.

- "The equation of motion is stiff … An explicit integrator is forced to take steps set by the
  fastest timescale" (2923–2944): supported by the Jacobian entries of audit §4 (row 5). That an
  explicit integrator "in practice fails to complete the integration" was not tried.
- "implicit Runge–Kutta method of Radau IIA type, of fifth order and L-stable" (2945–2949): the
  one stepper (`scipy.integrate.Radau`, SciPy 1.17.0). It is now used through a hand-written step
  loop rather than through `solve_ivp`, which makes no difference to the method.
- "Absolute and relative tolerances are both set to $10^{-8}$ by default" (2963–2964): true, and
  now true for every $M$ (row 6).
- "Local error control alone is not a reliable guide here, because the field can traverse the
  entire steep region within a single trial step" (2983–2986): confirmed by direct measurement
  (audit §3.1: four accepted steps from $\phi = 0.17$ to $\phi = 1.7\times10^{-16}$ with $\pi$
  unchanged, at $M = 0.5$ and $0.01$, past a wall at $\phi \approx 9\times10^{-5}$ to $4.6\times10^{-3}$).
- The commented paragraph on fragment counts (3008–3012, "after one hundred fragments is treated
  as pathological") is moot: there are no fragments. A failure is recorded, with the reason
  printed and not stored.

## 3. Statements the paper may want to carry

### 3.1 The temperature rises during the surfing overshoot (audit §8 F1)

From full histories at $M = 0.5$ under the current loop (`ln T_J` at every accepted step; audit
§8 F1):

| $\beta$ | min $\beta\phi'$ | where | window over which $T_J$ rises | rise |
|---|---|---|---|---|
| 1.2 | $-1.069$ | $N = 15.98$, $T_J = 216$ MeV | $N = 14.58 \to 17.66$ (3.1 e-folds) | $205.4 \to 231.1$ MeV ($+12\,\%$) |
| 2.0 | $-1.054$ | $N = 15.57$, $T_J = 690$ MeV | $N = 14.17 \to 18.97$ (4.8 e-folds) | $657.1 \to 749.9$ MeV ($+13\,\%$) |

The excursion of $\beta\phi'$ below $-1$ is 5–7 %, at the level of the right-hand side. The
consequences, confirmed by the current loop's first-bounce figures (log 01, nine histories: first
bounce at $N = 20.343$, $T_J = 746.6$ MeV for $\beta = 2$, $M = 0.5$):

- $T_J(N)$ is not monotonic above 100 MeV. Anything that assumes it can be inverted there must
  allow for this; the BBN window is unaffected.
- The first-bounce temperature is reached after that rise. It is the temperature at the end of
  the overshoot, not the plateau temperature: for $\beta = 2$ the temperature is 657 MeV at its
  minimum and 747 MeV at the first bounce.

### 3.2 Physical $M$ with $\beta \ge 1.2$ needs a parked-tracking model the code does not have (audit §3.7, §9.4)

This is not a correction but a statement the paper may need in its numerical section or its
limitations. For $M \lesssim 10^{-10}\Mp$ (the physical case is $M = 1$ eV $= 4.1\times10^{-28}\Mp$)
with $\beta \ge 1.2$, the field in the matter era is pressed onto the wall by the kick and its
bounce amplitude decays under Hubble friction. The bounce rate then grows by a factor of 2–2.5 per
e-fold from $N \approx 37$ ($\beta \le 2$) or $41$ ($\beta = 3$); at $\beta = 1.2$ the steps per
e-fold for $N = 36 \to 41$ are 4 587, 10 161, 20 462, 36 688 and 62 135. The integration to
$T_{\rm CMB}$ would take $10^7$–$10^8$ steps. No scheme that follows the bounces one at a time
reaches $T_{\rm CMB}$ (audit §3.7; `p_full.py … kin reflect`).

What the code now does is fail cleanly: after $2\times10^6$ accepted steps the history raises and
is recorded as a failure row (log 02). What it does not do, and cannot until the authors decide it,
is replace the bouncing field by its tracking solution once the amplitude is far below any scale
of interest: $\phi = \phi_{\rm wall}(\rho(N))$, $\pi = d\phi_{\rm wall}/dN$, with the energy of the
bounces discarded. The decisions are the switch criterion (an amplitude, or a bounce period
against the sample spacing) and the parked field's contribution to $\rho_\phi$, $p_\phi$ and the
adiabatic diagnostic. The cases that did complete are $\beta = 0.9$ at $M = 4.1\times10^{-28}$ (140 reflections at $\phi$ between $1.7\times10^{-12}$ and $8\times10^{-11}$) and histories at
$M \gtrsim 10^{-6}$, where the bounces become resolved oscillations in the well. Open issue
`[00-settling-at-physical-M-needs-a-parked-tracking-model]`.

### 3.3 The stored samples alias the rebounds (audit §7)

Not a correction to a sentence. The $z$ grid has $\Delta N \approx 0.0092$; turning points are
0.080 e-folds apart on the parked window (8.7 samples per half-period) and 0.022 on the grazing
window (2.4). If the paper claims the BBN and adiabatic stages resolve the rebounds, this is the
measurement against it. Open issue `[00-stored-samples-alias-the-rebounds]`.

## 4. Open dependencies of these suggestions

- Row 8's "(for the exponential potential, …)" is conditional on guard G1: the other four
  potentials do not declare that they reflect at the origin, and a history under one of them
  that reached the floor would fail rather than reflect (open issue
  `[00-declare-reflects-at-origin-for-the-other-potentials]`).
- Rows 1–9 describe the integration of $\phi$ and the cosmology. They do not change the field
  equation, the right-hand side on physical states, the equation of state, the BBN interface or
  the adiabatic stage (campaign README §0.4).
- The audit's remark in §1 that the paper's inner two caps "hold only for $M \approx 3\times10^{-3}$
  and $M = 10^{-2}$ respectively" is the origin of row 2's arithmetic; the audit did not attempt
  to say which $M$ the authors had in mind.

## 5. Added 2026-10-02 (`science-readiness`, prompt 08): the BBN-interface sentences

**Written:** 2026-10-02, by prompt 08 of the `science-readiness` campaign. **Not edited:** nothing
in `Paper1.tex` has been changed. The file was read on 2026-10-02 at the path in this document's
header (last modified 2026-10-01 12:04). Line numbers are those of that read and will move.

**What this section covers.** The paragraph "Interface to nucleosynthesis" (`\para`, 3127–3155 in
`NumericalSection`) and the sentence in "Nucleosynthesis" that points back to it (3668–3677). §1
above covers only the integrator paragraphs ("Stiffness", "Resolving the reflections", "Reflecting
boundary condition"). The "Thermodynamics" and "Adiabaticity" paragraphs are not touched by this
campaign.

**Code.** "Then" is tree `eba4473` (`review-remediation` prompt 04, 2026-09-29): ratio splines,
`NP_thermo_flag` on, the pressure and density-derivative callbacks, the spline floor at 0.1 eV.
That is also the tree whose behaviour the paper's asinh sentences already did not describe. "Now"
is `541c048`, the tree after prompt 07 (`VERSION_LABEL 2026.6.0`, `PRYM_VERSION
bf24c3d+ri02+sr01`). "Log NN" is `prompts/science-readiness/logs/NN-….md`. The mechanism is in
[`numerical-strategies.md` §7.6](numerical-strategies.md) and, for measurements, in
[`numerical-methods-for-paper.md` §4.1](numerical-methods-for-paper.md). Suggested sentences are
drafts, as in §1.

### 5.1 Sentences that disagree with the code

| # | Paper sentence (approx. line) | Then (`eba4473`) | Now (`541c048`) | Measurement | Suggested replacement |
|---|---|---|---|---|---|
| 10 | "This requires the additional energy density and pressure, relative to the Standard Model radiation and matter budget, as functions of the Jordan-frame temperature." (3132–3134) and "we pass the additional energy density and pressure, relative to the Standard Model energy budget, as functions of $T_J$, to the PRyMordial code" (3672–3674) | ρ_NP and p_NP were both built; p_NP from Ḣ_J through the Einstein-to-Jordan conversion (with its Ω″π² term), and passed with `NP_thermo_flag` on | **Only the density is built and passed.** There is no pressure, no Ḣ_J reconstruction and no `NP_thermo_flag`. ρ_NP = 3 M_P² H_J² − ρ_R,J (1 + f_m) enters PRyMordial's expansion rate and nothing else; the plasma temperature equation is the Standard-Model one | Log 01: `NP_hubble_flag` read once, in `Hubble`; thermodynamic `solve_ivp` has 2 components (3 before); `rho_NP` is called only from `Hubble` (`{'Hubble': N}` against `{'Hubble': 2029, 'dTgdt': 903, 'N_eff': 1}`); `pressure_NP`, `drho_NP_dT`, `jordan_Hdot_over_H2` and `Tstart_NP` are absent from the tree; ρ_NP ≡ 0 is plain PRyMordial exactly (`==` on four abundances). The earlier route did not finish in 900 s at β = 2, M = 0.5 and 10⁻³ (campaign README §0.3, §6.1), where this one takes about 10 s | "This requires the additional energy density, relative to the Standard Model radiation and matter budget, as a function of the Jordan-frame temperature. We supply it to the nucleosynthesis code through the expansion rate only; the plasma's temperature evolution is left as in the Standard Model." (and delete "and pressure" at 3672) |
| 11 | "These are signed quantities that span many decades across the relevant temperature window. A logarithmic representation cannot accommodate the sign changes, and a linear one loses all precision where the contribution is small. We therefore interpolate the inverse hyperbolic sine of each quantity against $\log T_J$, and invert the transformation on evaluation. This behaves logarithmically where the argument is large and linearly where it is small, and remains smooth and single-valued through zero." (3135–3146) | **Already wrong at `eba4473`, and before this campaign:** the asinh splines of ρ_NP and P_NP were replaced in `review-remediation` prompt 04 by cubic splines of the ratios r = ρ_NP/ρ_R,J and s = p_NP/ρ_R,J against ln(T/MeV), multiplied back by ρ_SM(T) | One cubic spline of r = ρ_NP/ρ_R,J against ln(T/MeV) on the stored samples with T_J in [0.2 keV, 100 MeV], multiplied by ρ_SM(T) = (π²/30) g_ρ(T) T⁴ (no transform, no sort: a non-monotonic T_J is refused). r is signed and of order 10⁻²–10⁻¹, not a quantity spanning many decades: ρ_NP spans decades only through the factor ρ_SM ∝ T⁴ | Numerical-methods note §4 [bbn]: constant ratio, spurious ρ_NP/ρ_SM **5.2e-17** (asinh 7.9e-10); an oscillating ratio **9.75e-9** (asinh 3.3e-8). Log 05 (ratio windows, four histories, 0.3 keV–100 keV): −0.0737 ≤ r ≤ 0.11, so |r| ≤ 0.11 in every window printed | "We interpolate the ratio $r = \rho_{\rm NP}/\rho_{R,J}$, which is signed and of order $10^{-2}$ to $10^{-1}$ in the relevant window, with a cubic spline in $\ln T_J$, and multiply it by the Standard Model radiation density $\rho_{\rm SM}(T) = (\pi^2/30)\,g_\rho(T)\,T^4$ evaluated at the same temperature." |
| 12 | "The temperature derivative of the density is obtained by differentiating the same interpolant and applying the chain rule, rather than by differencing, so that the density and its derivative supplied to the nucleosynthesis solver are consistent with each other." (3147–3151) | From `eba4473`: `dρ_NP/dT = r′ ρ_SM/T + r ρ_SM′`, the analytic derivative of the ratio spline (the chain rule of the paper's sentence was for the asinh form, since gone) | **No derivative is supplied.** PRyMordial is given ρ_NP(T) only, and reads it only in `Hubble`. The `drho_NP_dT` callback does not exist | Log 01 (`build_rho_NP_callback` returns one callable; `jordan_Hdot_over_H2`, `NPCallbacks`, `drho_NP_dT` removed). The derivative's old consumer was the `dρ_NP/dT` term of the plasma equation, gone with `NP_thermo_flag` | Delete the sentence. If a statement about consistency is wanted: "the density is the only quantity supplied, so there is no derivative to keep consistent with it." |
| 13 | "We require that the scalar solution extends at least one decade in temperature below the nucleosynthesis window, so that the interpolants are never extrapolated during the abundance calculation." (3152–3155) | `compute_BBN_data` refused a history that did not reach 0.1 × 0.1 eV = 0.01 eV. That is a decade below a spline floor of 0.1 eV, which is **not** the nucleosynthesis window: PRyMordial queries the callback only between 0.3628 keV and 10 MeV | The floor is 0.2 keV and the pre-check is T_stop ≤ 0.1 × 0.2 keV = **20 eV**. The sentence is true if "the nucleosynthesis window" is read as the interpolation range (0.2 keV to 100 MeV); it is about 1.3 decades (log₁₀ 362.8/20 = 1.26) below the lowest temperature PRyMordial queries. A request outside the range raises `ComputationFailureError` and is stored as a failure with its reason; nothing is extrapolated | Log 01 (`prym_callback_domain.py` on prompt 01's tree: lowest query 0.3628 keV, highest 10 MeV, no negative T; 1 944 calls); log 06 (`test_bbn_spline_floor (a)`: 10 eV passes, 100 eV fails; `(b)`: lowest positive query 0.3628 keV; abundances at β = 2, M = 0.5 and 10⁻³ unchanged to every printed digit by the narrowing) | "We require that the scalar solution extends at least one decade in temperature below the lowest temperature of the interpolation range, $0.2\,\mathrm{keV}$ (that is, to $20\,\mathrm{eV}$ or below), so that the interpolant is never extrapolated. The nucleosynthesis code queries only temperatures between $0.36\,\mathrm{keV}$ and $10\,\mathrm{MeV}$." |

Rows 10–12 describe code that the paper's text never matched in the form given (rows 11 and 12 by
`eba4473`) or that this campaign replaced (row 10, and the removal of the derivative in row 12).
Row 13 changed numerically but not in kind.

**Searched for and not found.** The paper has no sentence about the fictitious new-physics
temperature $T_{\rm NP}$ or a temperature-dependent new-physics fluid, about `NP_thermo_flag`, or
about a wall-clock limit (searched `Paper1.tex` for `T_{\rm NP}`, `thermo_flag`, `fictitious`,
"new-physics fluid", "wall clock", "timeout"; the 13 lines matching "fluid" are all about the
cosmological, radiation or a perfect fluid; "wall clock" and "timeout" match nothing). So no row corrects them: the removal of $T_{\rm NP}$ is
internal to the code and §5.4 below states what the text may want to say instead.

### 5.2 Statements the campaign leaves correct, or newly makes true

- "A scalar that still carries an appreciable share of the energy budget at this epoch changes the
  expansion rate at fixed plasma temperature, and therefore also changes the neutron-to-proton
  ratio at freeze-out" (3662–3665): this is now exactly what the code does. The scalar enters the
  nuclear network through the Jordan-frame H alone, and the plasma temperature obeys the
  Standard-Model law (log 01, `test_bbn_solver_failures (f)`). Before this campaign it held only
  to the order of a cancellation (≈ r · 1.2×10⁻³).
- "we generally choose initial conditions with $\phi$ in the vicinity of $5\,M_{\rm P}$" (2898–2899):
  the default of the new `--phi-init-Mp` is 5.0 (log 04), and a different value is a run option
  rather than an edit to three drivers. The authors' `\ds` notes on the 5 against 10 $M_{\rm P}$
  discrepancy (2066–2070, 2751, 2903) are about the paper, not the code; either value can be run.
  Couplings with $\Omega(\phi_*) T_* > M_{\rm P}$ are warned about, and computed (log 04: at
  $\phi_* = 5$, $T_* = 2\times10^4\,\mathrm{GeV}$, 93 of the 125 values of the exponential grid,
  from $\beta = 6.526$).

### 5.3 Statements to check against the new measurements

No sentence of `NumericalSection` was found that claims the stored samples resolve the rebounds.
If the authors write one, these are the measurements (log 05, Verification; the orchestrator's
half-period counts per sample cell, ΔN = 0.0092):

- **For the BBN stage: it holds in PRyMordial's window.** The median number of half-periods of π
  per cell is 0 from 100 MeV down to about 100 eV, on β = 1.6 and 2 at M = 10⁻⁵ and β = 2 at
  M = 0.5. The jumps of ρ_NP/ρ_R,J below 3 keV at small M are resolved bounces, not phase noise.
  The text should not say the BBN input is smoothed or averaged: it is not.
- **Against the adiabatic stage: it fails below about 100 eV.** At M = 10⁻⁵ the median is 1
  half-period per cell at 10–100 eV, 5–7 at 1–10 eV and 16–17 at 0.1–1 eV. That is
  `[post-adiabatic-Q-reads-aliased-late-samples]`, open. §3.3 above is **narrowed**, not closed:
  `[00-stored-samples-alias-the-rebounds]` has no BBN half; its adiabatic half stays.
- **Not measured.** Whether the cubic spline through the resolved jumps rings between samples
  (`[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]`), so a PRyMordial failure on this input
  cannot yet be attributed to the true H rather than to the interpolation.

### 5.4 What the text may want to state that it does not

Not corrections; additions the authors may want in the numerical section or its limitations:

- **A bound on each nucleosynthesis solve.** A PRyMordial solve is limited to 600 s of wall time by
  default (an unloaded full-network solve takes about 10 s), and a solve that exceeds it, that
  fails, or whose output is outside the checks (finite, $0 < Y_p < 0.5$, positive D/H, ³He/H and
  ⁷Li/H) is recorded as a failure with its reason and excluded, not stored as a result (log 01).
- **PRyMordial's sensitivity.** A few-ulp change in ρ_NP moves D/H by about $10^{-4}$ relative
  (log 01, Deviation 3: 1.06e-4, Yp 2.8e-6; the review-remediation issue
  `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`). D/H differences below that
  level between two runs are not resolved.
- **The first bounce** is stored from the dense output, because it cannot be recovered from the
  samples at small M: the sample-based estimate returned 742.79 MeV for 746.69 MeV at β = 2,
  M = 10⁻³, and 0.39 MeV for 420.76 MeV at β = 1.6, M = 10⁻⁵ (log 03). Any figure of
  "delivery temperature against $\beta$" should be made from the stored value
  (`numerical-strategies.md` §3.6).
