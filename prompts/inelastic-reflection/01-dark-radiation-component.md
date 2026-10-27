# Prompt 01 — the dark-radiation component

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-27, after the user's rulings (README §6.0 R1–R5; §6.1 U1–U8, with U4 as (b′);
§6.2 P1–P16 stand). If an orchestrator note is written for this prompt at dispatch, it governs
where it differs from this file.
**Gate:**
- on branch `inelastic-reflection`, at or after `327f78b`, with no tracked change in `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. It changes the expressions every history is integrated with, and
they must stay bitwise at K = 0.

**Read first:**

1. [`README.md`](README.md): §0.2, §0.4, §1, §2.2 (01's charter), §2.3 (the gate "DR"), §4
   (01 → 02), §5, §6.2 P1, P2, P11 and P12.
2. [`source/scoping_2026-10-27.md`](source/scoping_2026-10-27.md) §2 and §3.3: why ρ_χ is carried
   as dark radiation, and why it must enter the tuning.
3. `ComputeTargets/Policies/PotentialDerivativePolicy.py`, all of it.
4. `ComputeTargets/ScalarHistory.py`: `ODEPolicyData`, `HubblePolicyData`, `SampleValues`
   (:150–210), `ODEPolicy` and `HubblePolicy` (:230–398), `ODERHS` (:401), `_reflection_guards`
   (:513), `fixed_T_value_at` (:1060), `integrate_history` (:1137–1385), `_tuning_state` (:1557)
   and `tune_history` (:1631).
5. `ComputeTargets/tests/golden_history.py`, `golden_tuned_history.py`, `test_golden_history.py`
   and `test_tuning.py`: what is pinned and how.
6. `.documents/numerical-strategies.md` around :950 and :1085: how the policy expressions are
   described today.

---

## 1. What is wanted

A component ρ_χ of dark radiation that every policy expression sees. Nothing in this prompt injects
any. The only way to give a history a nonzero ρ_χ is a test harness: an initial ratio.

### 1.1 The ledger

`DarkRadiationLedger`, in a new module `ComputeTargets/dark_radiation.py`.
- **What it holds:** the injections, as pairs (N_j, ΔK_j) in increasing N_j. N is the integrator's
  own variable, N_E = ln a_E counted from N_start (README P1).
- **What it returns:**
  - ρ_χ,E(N) = K(N) e^{−4N}, with K(N) = Σ_{N_j ≤ N} ΔK_j;
  - the ratio r_χ(N) = ρ_χ,E(N)/ρ_R,E, given ln ρ_R,E, computed in logs as
    exp(ln K − 4N − ln ρ_R,E);
  - whether K(N) is zero, so that callers can take the old code path.
- **Its interface:**
  - `add(N, delta_K)`: appends an injection. An N below the last one is a `ValueError`. 02 calls it.
  - `rho_chi(N)` and `ratio(N, log_rhorad_Einstein)`, as above.
  - The constructor takes one optional argument, an initial injection at N_start. This is the test
    harness (§1.4).
- **Its tests** in `ComputeTargets/tests/test_dark_radiation.py`:
  - K is piecewise constant;
  - the rule N_j ≤ N at an injection's own N;
  - a^{−4} scaling between injections;
  - an empty ledger is zero everywhere;
  - `add` out of order raises.

**N_j ≤ N is deliberate.** After an injection the new Radau instance evaluates the RHS at N_j itself,
with the new K. A dense-output interpolant that ends at N_j holds the state before the injection.
Evaluated at N_j, it would therefore be paired with the new K. That edge belongs to 02. Say so in
"State handed to the next prompt".

### 1.2 The expressions

Write ρ_χ ≡ ρ_χ,E, r_χ = ρ_χ/ρ_R,E and Y = ρ_χ/3H²M_P². The constraint becomes
3M_P²H²G = V + ρ_R(1 + f_m) + ρ_χ, with G = 1 − π²/6M_P². Ḣ = −(ρ + p)/2M_P², with
ρ + p = H²π² + ρ_R(1 + f_m) + p_R + (4/3)ρ_χ and p_R = ρ_R(1 − Σ)/3.

**Derive each expression below from those two equations in the log.** Do not take the planner's
forms on trust. They are the planner's, written for this prompt from the code at `6f635e6`, and
are what the tests should confirm.

**A disagreement is a stop** (README §5, "The physics is the scoping's").

- **V/3H²M_P²** (`_evaluate_V_over_3H2Mp2_using_log_V`): G·T with
  T = 1/(1 + (ρ_R/V)(1 + f_m + r_χ)). In the branch ln(ρ_R/V) > 2 the form is
  T = (V/ρ_R)/(V/ρ_R + 1 + f_m + r_χ).
- **E** (`ODEPolicy.__call__`, :329): E = G − V/3H²M_P² − Y, which equals ρ_R(1 + f_m)/3H²M_P².
  - Use Y = r_χ (G − V/3H²M_P²)/(1 + f_m + r_χ), or the equivalent form E = (G − V/3H²M_P²)(1 + f_m)/(1 + f_m + r_χ).
  - Say which you use and why, given the cancellation in G − V/3H²M_P² when V dominates.
  - E stays the matter-and-bath factor of the kick, −3M_P² E (d ln A/dφ) R. The quanta are
    traceless, and ρ_χ must not reach the kick.
- **Ḣ/H² + 3** (`Hdot_over_H2_plus_3`): G·A₁ + C·A₂ − Y·R/2. A₁, A₂, C and R are today's. In a
  radiation-only limit (Σ = 0, f_m = 0, so R = 0) the new term vanishes, and Ḣ/H² = −2 whatever
  ρ_χ is. With matter (R = 1) it reproduces −(3/2)(ρ_m + (4/3)ρ_χ)/(ρ_m + ρ_χ).
- **`HubblePolicy`'s V = 0 branch** (:386): 3H²M_P² = ρ_R(1 + f_m + r_χ)/G.
- **`_tuning_state`'s V_target** (:1578): the bracket of Eq. 7 subtracts ρ_χ,J = r_χ ρ_R,J today:
  V_target = A₀⁴ (3M_P²H₀²G/(A₀²b′²) − ρ_R,J(1 + f_m + r_χ)). This is the same as subtracting
  ρ_χ,E outside it, because r_χ is frame-invariant (README P1).

**Unchanged:**
- **`effective_minimum`** reads ρ_m,E only, and the quanta do not shift the minimum.
- **d ln ρ_R/dN, d ln f_m/dN and d ln T_J/dN** do not involve H.
- **`_reflection_guards`' W** is built from V/3H²M_P², so it sees ρ_χ through the denominator and
  needs no change.
- **`fixed_T_value_at`'s ρ_NP** = 3M_P²H_J² − ρ_R,J(1 + f_m) includes ρ_χ through H_J. This is
  intended: it is the ratio BBN sees (README §0.2). Record it in the log.

### 1.3 How the policies receive it

- **`ODEPolicy`** takes an optional `dark_radiation: Optional[DarkRadiationLedger] = None`.
  - In `__call__(N, state)` it evaluates r_χ at N and at the state's ln ρ_R,E.
  - It passes r_χ to the `PotentialDerivativePolicy` methods as a keyword `r_chi: float = 0.0`.
- **`ODEPolicyData`** gains a field `rho_chi_over_rho_rad`, the r_χ it used, and `HubblePolicy`
  reads it.
  - Check every construction and unpacking of `ODEPolicyData`.
  - The existing callers (`AdiabaticHistory`, the tests that build `ODEPolicy` directly, the
    scripts under `.documents/`) keep working with no change, because the default is "none".
  - `AdiabaticHistory` is 05's, and is not changed here.
- **The exactness rule (README §5, "α = 0 is the old code").**
  - When the ledger is absent, or K(N) = 0, every expression takes **exactly the old operations**.
  - Prefer a branch on "no dark radiation" to adding a 0.0. Even where adding 0.0 would be exact,
    a branch makes the claim visible and skips the exp and log.
  - Both golden histories are the arbiter.

### 1.4 The harness and the payload

- **`integrate_history`** and **`tune_history`** take `rho_chi_ratio_init: float = 0.0`. This is
  r_χ at T_init, a test harness, documented as such in their docstrings.
  - `integrate_history` builds the ledger. It is empty when the ratio is 0.0. Otherwise it holds
    one injection at N_start with ΔK = r₀ ρ_R,E,init e^{4 N_start}.
  - `tune_history` passes the ratio to each iterate. Each iterate gets a fresh ledger, because
    the ledger is built inside `integrate_history`.
  - `compute_scalar_history` (the remote that `main.py` runs) does **not** take it. Nothing a
    driver can launch has a nonzero ρ_χ in this prompt. A stored history with a ρ_χ that is not
    in its key would be wrong.
- **The payload** gains `"dark_radiation"`, the ledger. `_tuning_state` reads r_χ today from it,
  at the last sample's N and ln ρ_R,E.
  - Check, by reading, that nothing iterates over the payload's keys on the way to the store.
    That means `ScalarHistory.compute` and `store`, `build_extra_data`, and the round-trip tests
    in `ObjectFactories/tests`. A payload key that reaches the store would change it, and the
    store is 05's. If anything does iterate over the keys, stop and ask.
  - The payload must still pickle, because Ray returns it.

### 1.5 The tests

In `ComputeTargets/tests/test_dark_radiation.py`, beside §1.1's. Give each one's cost in its
docstring.
1. **The golden histories, unchanged and bitwise.** `test_golden_history.py` and the tuned golden
   test in `test_tuning.py` pass without edits.
2. **The constraint, to rounding.** At constructed states, `ODEPolicy` and `HubblePolicy` satisfy
   3M_P²H_E²G = V + ρ_R(1 + f_m) + ρ_χ. Cover:
   - both branches of the log-safe T;
   - the V = 0 branch of `HubblePolicy` (a state with ln(ρ_R/V) > 745, so that V/3H²M_P²
     underflows);
   - r_χ from 10⁻⁶ to 10.
   At the same states, E·3H²M_P² = ρ_R(1 + f_m), to rounding.
3. **Ḣ, from the definitions.** At the same states, Ḣ/H² computed in the test from ρ + p and the
   constraint's H, written out in the test and not by the policy's algebra, equals
   `Hdot_over_H2_plus_3` − 3, to rounding. Include R = 0, where the term vanishes, and a state with
   f_m ≳ 1, where it does not.
4. **Continuity on a real history.** Integrate with `integrate_history` and r₀ chosen so that
   Y·R/2 is well above the solver's tolerance somewhere on the window. Say where that is and how
   large the term gets.
   - **The H check.** On the dense output, d ln H_E/dN by a centred finite difference of
     `HubblePolicy`'s H equals the policy's Ḣ/H² to a tolerance you justify from rtol and the
     difference step.
   - **The constraint check.** On the samples, 3M_P²H_E²G − ρ_R(1 + f_m) − V = ρ_χ,E(N), to rounding
     relative to the total.
   - Use the golden histories' setup if it serves. A shorter T_stop is fine.
5. **The tuning with an initial ρ_χ.** `tune_history` at the tuned golden point with a nonzero r₀
   converges. Its residuals are inside the tolerance, and H_J(T_CMB) = H₀ and φ_ref = φ_min hold.
   - Choose r₀ so that leaving ρ_χ out of V_target makes the tuning fail. Say how far it falls
     short and why.
   - A unit test of `_tuning_state` as well: on a constructed last sample and ledger, putting
     V_target back into the constraint gives H_J = H₀ to rounding.

**Deliberate breakages** (P11). Each must fail a named test. Record each as a diff in the log; never
commit one.
- the −Y·R/2 term dropped;
- ρ_χ left in E;
- r_χ left out of the V = 0 branch;
- r_χ left out of V_target;
- the ledger's rule changed to N_j < N.

For the V_target breakage, the unit test is enough; say whether the tuned test also fails, and
after how many integrations.

---

## 2. Verification

1. **Before any edit:** the five suites, run as `CLAUDE.md` says, with ComputeTargets in its two
   halves. Expect the baselines of README §0.4, 26 / 269 (77 + 192) / 103 / 115 / 64, which were
   not re-run at planning. A different count, or a failure, is a stop.
2. **After:** the five suites. ComputeTargets rises by the new tests and nothing else falls. Then
   `black --check` on the files you changed.
3. **The deliberate breakages** of §1.5, each with the test it fails.
4. **The cost at K = 0:** the golden histories' wall time before and after, from the tests' own
   output or a timing in your scratch space. The branch should cost nothing measurable. Say what
   you measured.

## 3. Acceptance

1. §1.1–§1.5 as written, and the gate "DR" (README §2.3):
   - both golden histories bitwise at K = 0;
   - with an initial ρ_χ, the constraint and continuity agree to tolerance;
   - the radiation limit holds to rounding (§1.5 items 2–3);
   - a tuned history meets both conditions.
2. **The log**, `logs/01-dark-radiation-component.md`, in the template of `dk-and-rpk-migration`
   README §5.1. It holds:
   - the derivations of §1.2;
   - the suite counts before and after;
   - the breakages as diffs;
   - every deviation classified;
   - "State handed to the next prompt". This covers the ledger's API, how the policies receive
     it, the N_j ≤ N edge, and the payload key.
3. **The documents** (P12). A dated note in `.documents/numerical-strategies.md` where the policy
   expressions are described, and one in `.documents/architecture-summary.md` for the ledger and
   the new module.
4. **The board:**
   - row 01, and the gate DR, in §1 and §2;
   - `### 5.1 After prompt 01 (<date>)` in §5;
   - §3 and §4 for anything opened, with `.documents/OPEN_ISSUES.md` in step.
5. **One commit**, in `CLAUDE.md`'s form, staged by explicit path only.

## 4. Stop conditions: stop and ask

- A golden history is not bitwise at K = 0 and the cause is not a mistake of yours you can see and
  undo.
- Your derivation of an expression in §1.2 disagrees with the planner's.
- Something on the way to the store iterates over the payload's keys (§1.4).
- The tuning with an initial ρ_χ does not converge at an r₀ small enough that the history stays
  physical (G > 0, E ≥ 0 throughout).
- A baseline count differs from README §0.4 before you edit.

## 5. What this prompt does not do

- **It injects nothing.** There are no turning points, drains, restarts or α (02).
- **It stores nothing.** There is no new column, key or fingerprint format, and `AdiabaticHistory`
  is unchanged (05).
- **No option is added.** `main.py`, the plot scripts and the tools are unchanged.
- **Files it changes:**
  - `ComputeTargets/dark_radiation.py` (new);
  - `ComputeTargets/Policies/PotentialDerivativePolicy.py`;
  - `ComputeTargets/ScalarHistory.py`;
  - `ComputeTargets/tests/test_dark_radiation.py` (new);
  - the two `.documents/` files;
  - the log, the board and, if anything is opened, `.documents/OPEN_ISSUES.md`.

  Any other file is a deviation to classify.
