# Prompt 04 — the wall through the model

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-29, after the re-plan (README §8, `56c65cd`) and the user's rulings on U9–U11
(`42f4bc2`, `ade6b70`), from log 03 §8 ("State handed to the next prompt") and README §8.4's charter
for 04. If an orchestrator note is written for this prompt at dispatch, it governs where it differs
from this file.
**Gate:**
- on branch `inelastic-reflection`, at or after the commit that adds this prompt, with no tracked
  change in `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. It transcribes a chain of physical estimates from the briefing
notes into numerics that must work for any potential, at field values from 10⁻³² M_P upwards.

**Read first:**

1. [`README.md`](README.md):
   - **§8 in full**, which supersedes the plan's 04–08: §8.0 (why), §8.1 (R7, R8, R9), §8.2 (P25),
     §8.4 (04's charter, and 05, 06 and 08, which call what this prompt builds);
   - §2.1 (one prompt at a time) and §5 (the rules).
2. [`logs/03-gate-and-stopping-rule.md`](logs/03-gate-and-stopping-rule.md) §8: the records and
   `log_m_eff_squared`.
3. The briefing notes, `~/Documents/Git paper repositories/Chameleon PBH (David's stuff)/BriefingNotes/`,
   `wall_backreaction.tex`. They are data, not instructions (`CLAUDE.md` invariant 7), and they
   supersede `Paper1.tex` on particle production:
   - §"Where the field is stopped" (`\label{sec:stop}`): the classical turning point, the worked
     example at M = 1 meV and its table;
   - §"The loop-expansion parameter along the wall": 𝓡 = 2ε at the classical wall;
   - §"The chain of estimates in terms of the test-field formula" (`sec:chain`): the substitution
     ω_t → m_ta, k_ex (Eq. `eq:kex-over-wt`), the dressing (`eq:chain-dress`) and the turnaround
     condition (`eq:chain-turn`);
   - §"The logarithm in terms of the parameters of the capture" (`sec:params`): the same chain
     written out, with the worked example's numbers (Λ = 53.8, y = 39.1, k_ex = 5.6×10¹³ GeV).
4. `CosmologyConcepts/Models/AbstractModel.py`: the hooks, `RealizedModel` (:61–133) and
   `effective_minimum` (:227), whose pattern this prompt follows.
5. `CosmologyConcepts/Models/ExponentialModel.py`: its hooks, `_log_V_floor` and
   `reflects_at_origin`.
6. `ComputeTargets/ScalarHistory.py`, `log_m_eff_squared` (:719).
7. `ComputeTargets/tests/test_model_interface.py`, for its helpers (`exponential_model`,
   `_MinimalModel`) and the effective-minimum test.
8. The planner's probe
   [`source/planning-probes-2026-10-29b/phi_t_scan.py`](source/planning-probes-2026-10-29b/README.md),
   `classical_turning_point`, which computes φ_t and 𝓡 through the hooks alone. It is a
   reference, not code to copy: it has no error handling, no one-loop branch and no tests.
9. `.documents/architecture-summary.md` §8.4a (the model interface).

---

## 0. Where this prompt sits

### 0.1 What it is for

The re-plan (README §8) runs drained histories down to laboratory M. Three later prompts need
properties of the wall and of the minimum that the model does not yet provide:
- **05** decides, on every infalling step, whether the turnaround ahead is resolved. It needs the
  classical turning point φ_t and the test-field ratio 𝓡 there (R8).
- **06** parks the field on the minimum of the effective potential. It needs that minimum on the
  full trace, radiation included, and its response to the trace.
- **08** estimates where the produced spectrum peaks (R7, P25). It needs the classical estimate
  where 𝓡 < 1 and the one-loop estimate where 𝓡 > 1.

The user ruled that none of this may assume a closed form of the potential (R9): "They depend on
the potential, which is pluggable. They won't apply for a different potential." So each is a method
of the model with a **generic default built on the hooks**, as `effective_minimum` is. This prompt
writes them and tests them. **It does not touch the integrator.**

### 0.2 Departures from README §8.4's charter, and why

- **dφ_min/dN is 06's, not 04's.** The charter asks for dφ_min/dN "by the implicit function theorem
  along the background's derivatives". The derivative along the parked trajectory needs
  d ln T/dN of the trace, and that depends on π through the coupling terms of the right-hand side.
  That is a property of the integrator's equations, which 06 owns. This prompt provides the
  model's half, ∂φ_min/∂ ln T at fixed trace (§1.4), and 06 composes it with the right-hand side.
- **λ is the notes' λ.** P25 says "the wall's local steepness". The notes use λ = −d ln V/dφ
  (λ = M/φ² = y²/M for the exponential, n = 1), and so does every number in their tables. The margin
  probe of 2026-10-28 (`planning-probes-2026-10-28c/margin.py`) used −d ln V″/dφ, which differs by
  about 4/y. This prompt uses the notes' definition.
- **The one-loop turnaround is §`sec:chain`'s**, m_ta⁴ = 32π²E/ℓ (`eq:chain-turn`). It is not
  §`sec:stop`'s Born condition V″ = (8π²E/c)^{1/2}, nor its Erickcek κ condition, which gives the
  y = 40 of the §`sec:stop` table. The chain's y is 39.1 on the same example. README §8.4's gate
  "Wall" says "y ≈ 40"; it means the chain's value.

---

## 1. What is wanted

### 1.1 Where the methods live

Methods on `AbstractModel` with generic defaults, taking the normalization as the hooks do. Each is
mirrored on `RealizedModel` by a method that supplies the normalization, as the existing methods
delegate. A model may override any of them with a closed form. **`ExponentialModel` overrides
none in this prompt**: the generic path must be the one tested and used. If 05 finds the cost too
high, an override is its decision to propose.

Three results, each a `namedtuple` defined beside `Seed`. Every quantity that can overflow or
underflow is carried as a logarithm. The names below are a proposal; choose and record yours.
- `ClassicalTurningPoint`: φ_t, ln V″(φ_t) = ln ω_t², ln 𝓡, λ_t, ln k_ex, and ℓ_t = ln(k_ex/ω_t).
- `OneLoopTurnaround`: φ_ta, ln m_ta, λ_ta, ln k_ex, ℓ = ln(k_ex/m_ta), the iteration count, and
  whether φ_ta lies outside φ_t.
- The minimum is a float, and its derivative another; no tuple is needed.

All take **E = ρ_kin, the condensate's kinetic energy density in the Einstein frame at the
infall**, as ln E. The integrator has it as H_E²π²/2 (prompt 02's `log_rho_kin`). The incoming speed
is |φ̇_in| = (2E)^{1/2}. The wall is crossed in a time far shorter than a Hubble time, so expansion
during the crossing is neglected, as the notes neglect it.

**Errors.** A failure inside one of these is a failure of the history that called it, so raise
`ComputationFailureError` (`ComputeTargets.exceptions`; `ExponentialModel` already imports it), naming
the model, the inputs and the reason. The cases are:
- a model that is not `reflects_at_origin`, or has no `log_V_floor`;
- E ≤ 0, or a non-finite input;
- a bracket not found within a fixed number of doublings;
- a non-monotone crossing (§1.5);
- a one-loop fixed point that does not converge.

**The one exception: the field is already inside its turning point.** If V(φ) − V_floor ≥ E at the
field itself, the classical turning point is behind it. Return `None`; that is not a failure. The
probe met this case, and 05 needs to know it.

### 1.2 The classical turning point

- **φ_t** is the root of V(φ_t) − V_floor = E, searched **inwards from the field's φ in ln φ**. Start
  with a step of one e-fold and double it until the sign changes, then use `brentq` on the last
  bracket. Form ln(V − V_floor) as ln V + ln(−expm1(ln V_floor − ln V)), and treat V = V_floor as
  −∞. The probe found φ_t to 10⁻¹² against the exponential closed form this way.
- **The wall frequency:** ln ω_t² = ln V″(φ_t), with V″ = V[(ln V)″ + (ln V)′²] from the hooks.
  A non-positive V″ at φ_t is an error: a reflecting wall is convex there.
- **The test-field ratio:** 𝓡 = ω_t⁴/8π²E (`sec:chain`, first paragraph), so
  ln 𝓡 = 2 ln ω_t² − ln 8π² − ln E. 𝓡 > 1 says the field cannot reach the classical wall (R7, R8).
- **The classical peak:** λ_t = −d ln V/dφ at φ_t, and k_ex = λ_t|φ̇_in|/2√2 = λ_t E^{1/2}/2
  (`eq:kex-over-wt` with y = L). ℓ_t = ln(k_ex/ω_t) is about ln ½ for the exponential: the
  classical regime's ℓ is negative by construction (README §8.1 R13).

### 1.3 The one-loop turnaround

Transcribe `sec:chain` and `sec:params` for a generic potential, with the derivation in the log.
- **The condition** (`eq:chain-turn`): the field stops where the dressing ρ_dress = m⁴ ln(k_ex/m)/32π²,
  with m² = V″, reaches E. So m_ta⁴ = 32π²E/ℓ with ℓ = ln(k_ex/m_ta). There is **no renormalization
  scale**: the user, "The turnaround comes from the state-dependent 'dynamic' contribution, and for
  that the logarithm is `ln(k_ex / m_ta)`" (R7).
- **The position:** φ_ta is the root of V″(φ_ta) = m_ta², searched inwards from the field's φ as in
  §1.2. On the approach V″ rises monotonically (`sec:chain`, before `eq:peak-is-ta`), so the first
  crossing is the turning point. Search in ln V″ = ln V + ln((ln V)″ + (ln V)′²).
- **The peak:** λ_ta = −d ln V/dφ at φ_ta, and k_ex = λ_ta E^{1/2}/2 (`eq:kex-over-wt`, whose λ is
  taken at φ_ta).
- **The fixed point.** ℓ enters m_ta, which fixes φ_ta, which fixes k_ex and so ℓ. Iterate from
  ℓ₀ = 32 until |Δℓ| ≤ 10⁻¹² ℓ, within a fixed iteration limit. The notes say the dependence is weak:
  "any ℓ between 10 and 100" moves m_ta by less than 1.3. So expect a few iterations, and record how
  many it took.
- **Where it has no solution.** The chain needs ℓ > 0. Near M_crit the one-loop point approaches
  the classical wall, k_ex approaches ω_t/2 and ℓ falls. If the iteration drives ℓ to zero or below,
  or does not converge, there is no one-loop turnaround. Raise, with ℓ's last values in the message.
  08 uses the classical estimate wherever 𝓡 < 1, so the one-loop branch is called only where
  𝓡 > 1. §2 maps how far below 𝓡 = 1 the chain still has a solution.
- **`outside_classical`** is φ_ta > φ_t: the field is stopped before the classical wall. The notes
  give this for 𝓡 > 1 up to the logarithm. Report it; do not enforce it.

### 1.4 The minimum on a given trace

- **φ_min(T)** is the root of V′(x) + (ln A)′(x) T = 0, with T the Einstein-frame trace
  ρ_R,E(Σ + f_m) (the coupling `ODERHS` uses, and `log_m_eff_squared`'s ρ_R,E(Σ + f_m)), taken as
  ln T.
  - **This is the parked field's condition.** A field at its minimum has ρ̃_c = T/A(φ_min), and
    V′ + (ln A)′ A ρ̃_c = 0 at x = φ_min is this equation. It is `effective_minimum`'s equation with
    the radiation term included and φ₀ = φ_min.
  - **Solve it in logs:** ln|V′| = ln V + ln|(ln V)′| against ln((ln A)′ T), searching in ln x from
    a positive starting point the caller must give (06 will pass the field, or the last φ_min),
    outward or inward until the sign changes. There is no default: φ_ref can be 0, and no other
    field value is generic. For a reflecting wall with (ln A)′ > 0 the root is unique: V′ < 0 rises
    to 0 as x grows. Raise if (ln A)′ ≤ 0 at the root.
- **∂φ_min/∂ ln T at fixed T**, by implicit differentiation of the same equation:
  −(ln A)′ T/(V″ + (ln A)″ T) at the root. 06 composes it with d ln T/dN from the right-hand side
  (§0.2).

  Note the denominator: **at fixed T it has no (ln A)′² term**. `log_m_eff_squared`'s mass, the
  fluctuation's mass at fixed ρ̃_c, has one. Both are right for what they describe. Say so in the
  docstring, so that 06 does not mix them.

### 1.5 Numerics, for any potential

- **No closed form** of any potential, anywhere (R9). The tests compare with closed forms they
  compute themselves.
- **Logarithms throughout.** At M = 10⁻³⁰ M_P, φ_t ≈ 10⁻³² M_P, E can be 10⁻⁸⁰ M_P⁴ or less, and V″
  spans hundreds of orders of magnitude.
- **The brackets** double in ln φ from the field inwards, at most 64 doublings, then `brentq` with a
  relative tolerance near 10⁻¹³ in ln φ. Record the tolerance you choose.
- **Monotonicity.** A quantity that crosses its target more than once between the field and the
  root found would make the outermost crossing the physical one. Check the sign at the bracket's
  outer end and at the bracket's midpoints as you narrow it. If a second crossing appears, raise.
  Do not search for it exhaustively.

### 1.6 The tests

A new module, `ComputeTargets/tests/test_wall_model.py`, where `test_model_interface.py` keeps the
model tests. No integration, no store, no Ray, and no file outside the tree is read: the notes'
numbers are written into the test with their source.
1. **The classical turning point against the exponential closed form**, n = 1, at M = 10⁻³⁰, 10⁻¹⁶,
   10⁻¹⁰ and 10⁻⁷ M_P and three E each:
   - φ_t equals M/ln(1 + E/V₀) to 10⁻¹² relative;
   - ln 𝓡 equals the closed form with ω_t² = (E + V₀)(y⁴ + 2y³)/M² to 10⁻¹⁰ absolute;
   - k_ex equals (y²/M)E^{1/2}/2 to 10⁻¹².
2. **The notes' worked example** (`sec:params`): M = 1 meV, V₀ = 10⁻⁴⁸ GeV⁴, E = (c_β T_s)⁴ with
   c_β = 1.18 and T_s = 0.23 GeV.
   - **Classical:** y_t = M/φ_t equals ln(1 + E/V₀) ≈ 105.3; the notes print 105.
   - **One-loop:** the method's fixed point against the test's own evaluation of `sec:params`'
     equations at the same E, which are m_ta⁴ = 32π²E/ℓ, y = Λ − 4 ln y − ln(1 + 2/y),
     Λ = ln(m_ta²M²/V₀), k_ex = y²E^{1/2}/2M and ℓ = ln(k_ex/m_ta), iterated to convergence. They
     must agree to 10⁻⁸.
   - **The printed numbers:** y = 39.1 ± 0.05, Λ = 53.8 ± 0.05 and k_ex = 5.6×10¹³ GeV ± 3 %.
     `eq:chain-turn` prints m_ta² ≈ 0.25 GeV² at the rounder ℓ = 32. Report the chain's value at
     its own ℓ, about 0.23 GeV², and do not assert the notes' rounding.
3. **No closed form is used**, on two potentials the exponential n = 1 forms do not describe: the
   exponential with n = 2, and a test-only power-law wall V = V₀(1 + (M/φ)^p), p = 2, with
   `reflects_at_origin` and `log_V_floor = ln V₀`. On each, every defining equation holds at the
   returned values to rounding:
   - V(φ_t) − V_floor = E;
   - V″(φ_ta) = m_ta², m_ta⁴ ℓ = 32π²E and k_ex = λ_ta E^{1/2}/2;
   - V′(φ_min) + (ln A)′ T = 0.

   State each rounding bound and why it is rounding, as 01–03 did.
4. **The minimum:**
   - its residual, as item 3;
   - at T = ρ_m,E today with Σ = 0, it equals `effective_minimum(norm, phi_ref, φ_min, T)`
     to `effective_minimum`'s tolerance, which shows the two equations agree there;
   - ∂φ_min/∂ ln T against a centred finite difference in ln T, to 10⁻⁶ relative;
   - at M = 10⁻³⁰ and at M = 0.5 M_P.
5. **The cases of §1.1:**
   - a model that does not reflect, raising;
   - E ≤ 0, raising;
   - a field already inside its turning point, returning `None`;
   - a one-loop call where the chain has no solution, raising, at an E and M you find from §2.
6. **`RealizedModel` delegates** each method to its model with the normalization, and adds
   nothing.

**Each test is shown to catch a breakage** (P11): a wrong sign in V″; λ from V″ instead of V; the
exponential closed form substituted for the generic root, which item 3 must catch on n = 2; a
missing (ln A)″ term; a wrong factor in 𝓡; the one-loop iteration stopped after one step. Record
each breakage as a diff and the tests it fails; never commit one.

---

## 2. Measurement

Read-only, on the exponential model, n = 1, recorded as tables in the log.
1. **The worked example's chain**, step by step: y, Λ, m_ta, k_ex, ℓ and the iteration count. Give
   them beside the notes' printed values.
2. **The crossover at 𝓡 = 1.** At the worked example's E, and at one E of a later bounce (E smaller
   by 10⁻⁴), scan M through M_crit, from 10⁻¹³ to 10⁻¹⁹ M_P, at enough points to find:
   - where 𝓡 = 1;
   - where the one-loop chain stops having a solution;
   - ln k_ex and ln m_ta of both estimates on either side;
   - the size of the jump in ln k_ex when 08 switches from the classical to the one-loop estimate at
     𝓡 = 1.

   At 𝓡 = 1 the two conditions differ by a factor (4/ℓ)^{1/4} in the mass, so a jump is expected.
   Measure it; do not smooth it.
3. **The cost** of each method per call, in µs, at M = 10⁻³⁰ and 10⁻¹⁰ M_P. 05 calls the classical
   one on every infalling step at α > 0.

---

## 3. Verification

1. **Before any edit:** the five suites, run as `CLAUDE.md` says, with ComputeTargets in its two
   halves. Expect 26 / 317 (96 + 221) / 103 / 115 / 64 (log 03 §8; no code has changed since
   `0afa199`). A different count, or a failure, is a stop.
2. **After:** the five suites. ComputeTargets rises by the new tests and nothing else changes.
   Nothing in the integrator changed, so the golden tests must pass unchanged; say so. Then
   `black --check` on the files you changed.
3. **The breakages** of §1.6, each with the tests it fails.

## 4. Acceptance

1. §1 as written, and the gate **"Wall"** (README §8.4, as §0.2 reads it):
   - φ_t and 𝓡 equal the exponential closed form;
   - the notes' chain is reproduced: classical y ≈ 105, one-loop y = 39.1;
   - the n = 2 model and the power-law wall satisfy every defining equation;
   - the minimum and its derivative are checked.
2. **The measurement** of §2, as tables in the log.
3. **The log**, `logs/04-the-wall-through-the-model.md`, in the template of `dk-and-rpk-migration`
   README §5.1. It holds:
   - the derivation of each method from the notes, with the equations by label;
   - the choices of §1.1 (names) and §1.5 (tolerances);
   - the suite counts before and after;
   - the breakages as diffs;
   - every deviation classified;
   - "State handed to the next prompt": the method names and signatures, what each returns, when
     each raises or returns `None`, the costs, and the 𝓡 = 1 crossover for 08.
4. **The documents** (P12): a dated note in `.documents/architecture-summary.md` §8.4a (the new
   methods, generic, overridable).
5. **The board:**
   - row 04 and the gate Wall, in §1 and §2;
   - `### 5.5 After prompt 04 (<date>)` in §5;
   - §3 and §4 for anything opened, with `.documents/OPEN_ISSUES.md` in step.
6. **One commit**, in `CLAUDE.md`'s form, staged by explicit path only.

## 5. Stop conditions: stop and ask

- The test's evaluation of the notes' chain does not reproduce the notes' printed y = 39.1,
  Λ = 53.8 or k_ex = 5.6×10¹³ GeV within the tolerances of §1.6 item 2. Either the transcription or
  the notes is wrong, and which is not yours to decide.
- The one-loop fixed point has no solution, or converges to ℓ ≤ 0, at the worked example.
- A defining equation of §1.6 item 3 fails by more than rounding on a model the method returned a
  value for.
- `effective_minimum` and the new minimum disagree where they should agree (§1.6 item 4).
- An existing test fails, or a baseline count differs from §3 item 1 before you edit.
- The work seems to need a change to `ComputeTargets/ScalarHistory.py` or any integrator file.

## 6. What this prompt does not do

- **No change to the integrator**, the policies, the records or the payload. 05 calls these
  methods; nothing calls them after this prompt but the tests.
- **No closed-form override** on `ExponentialModel` (§1.1).
- **No dφ_min/dN along a trajectory** (§0.2), no parking, and no margin.
- **No store, key or column.**
- **Files it changes:**
  - `CosmologyConcepts/Models/AbstractModel.py`;
  - `ComputeTargets/tests/test_wall_model.py` (new);
  - `.documents/architecture-summary.md`;
  - the log, the board and, if anything is opened, `.documents/OPEN_ISSUES.md`.

  Any other file is a deviation to classify.
