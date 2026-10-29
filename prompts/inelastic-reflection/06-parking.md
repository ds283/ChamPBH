# Prompt 06 — parking

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-29, after 05 landed (`9c2483b`), from log 05 §8 ("State handed to the next
prompt"), README §8.4's charter for 06 and the orchestrator's probes of the same day
([`source/planning-probes-2026-10-29c/`](source/planning-probes-2026-10-29c/README.md)), at the user's
word ("Please can you write prompt 06?"). If an orchestrator note is written for this prompt at
dispatch, it governs where it differs from this file.
**Gate:**
- on branch `inelastic-reflection`, at or after the commit that adds this prompt and after the
  orchestrator's review of 05 is committed, with no tracked change in `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** nothing. `[00-settling-at-physical-M-needs-a-parked-tracking-model]` is closed by the
campaign once 07 has added the release (README §8.6). **Opens:** only what the work finds.
**Recommended model:** **Opus**. It adds a second phase to the integration that every history runs
through, must leave α = 0 bitwise, and must leave every α > 0 history bitwise 05's up to the point
where it parks.

**Read first:**

1. [`README.md`](README.md):
   - **§8** in full, in particular R6 (the two modes), R14 (the cut), R15 (the parked field), R16
     (unparking, which is 07's), P18 (the constants), P19 (the leftover), P21 (the guard), P22
     (natural mode is the old code), P24 (one park, one release); §8.4's charters for 06 and 07;
     §8.5 ("06 → 07");
   - §2.1 (one prompt at a time) and §5 (the rules).
2. [`logs/05-unresolved-turnarounds.md`](logs/05-unresolved-turnarounds.md) §8, §5.5 (how the small-M
   histories fail without parking, and where ρ_kin/ρ_R crosses 10⁻⁶) and §7.
3. [`logs/04-the-wall-through-the-model.md`](logs/04-the-wall-through-the-model.md) §8:
   `minimum_on_trace`, `d_minimum_d_log_trace` (its denominator has no (ln A)′² term, and must not
   be given one), what they raise, and their cost.
4. `ComputeTargets/ScalarHistory.py`:
   - the records `Reflection`, `Injection` and `IntegrationResult` (:181–296);
   - `ODEPolicy` (:408), `HubblePolicy` (:579), `ODERHS` (:626), `log_m_eff_squared` (:794);
   - `integrate_scalar_history` (:970), in particular `candidate` (:1285), `drain` (:1361), the
     floor branch (:1657–1699), the termination (:1762–1789), the drain point (:1811–1832) and the
     result (:1843);
   - the readers of the dense output: `first_bounce` (:1878), `surfing_turning_points` (:1968),
     `_T_Jordan_crossing_step` (:2092), `fixed_T_value_at` (:2143), and `integrate_history`'s
     sampling (:2226);
   - `_tuning_state` (:2696) and `tune_history` (:2785).
5. `ComputeTargets/dark_radiation.py`: `DarkRadiationLedger.add` (two entries at one N are allowed).
6. The orchestrator's probes
   [`source/planning-probes-2026-10-29c/`](source/planning-probes-2026-10-29c/README.md), and the
   planner's `cut_rhoR.out` and `parked_tracker.out` in
   [`source/planning-probes-2026-10-29b/`](source/planning-probes-2026-10-29b/README.md). They are
   references, not code to copy.
7. The two tests whose setup this prompt changes (§1.7 item 10):
   `ComputeTargets/tests/test_injection.py` (`TestTwoTurningPointsOnOneStep`, :502) and
   `ComputeTargets/tests/test_gate_and_taper.py` (`TestTheNearStallIsGated`, :488).

---

## 0. Where this prompt sits

### 0.1 What it is for

In dissipation mode the drain makes every cascade of bounces a geometric sequence. Without a switch,
every small-M history fails at the bottom of its first full cascade. Log 05 §5.5 found them failing
0.25–5.6 e-folds after ρ_kin/ρ_R falls below 10⁻⁶ at every β and M measured, by guard G2, by 04's wall
search, or on a dense-output sample with φ < 0. The user ruled (R14) that once the condensate's kinetic
energy is small relative to the bath, ρ_kin/ρ_R < ε, the field is **parked**: the leftover kinetic
energy goes to the ledger (P19), and the field then tracks the minimum of its effective potential with
zero lag (R15). That tracker is integrated as a reduced system to T_CMB, and is read through the
existing dense-output interface.

This prompt does not release the field: 07 does (R16, P24). In 06 a parked tracker that stops being
adiabatic (m_eff/H < 10) is a failure of the history, with its reason.

### 0.2 What the orchestrator's probes found (`9c2483b`, α = 0.1, ε = 10⁻⁶)

**At the cut, on 05's histories** (`probe_cut.out`). The cut falls at the same candidate and N as log
05 §5.5 found, at every M. "φ/φ_min" is the field at the cut over the tracker's. "ln A range" is the
range over the oscillation since the previous candidate. "Δ ln T_J" is the reduced integration less the
oscillating history over the window that 05's history survives.

| β | M (M_P) | cut | φ/φ_min | m/H at φ_min | Δ ln A at the switch | ln A range | Δ ln T_J, max \| end |
|---|---|---|---|---|---|---|---|
| 1.2 | 10⁻³⁰ | #6 floor, N = 28.3641, 11.6 keV | 4.1×10¹³ | 3.5×10¹¹ | −1.9×10⁻¹³ | 7.5×10⁻³ | 1.1×10⁻³ \| 1.1×10⁻³ |
| 1.2 | 10⁻⁷ | #6 unresolved | 1.71 | 1.5×10³ | −2.9×10⁻⁹ | 7.5×10⁻³ | 1.1×10⁻³ \| 1.1×10⁻³ |
| 2 | 10⁻³⁰ | #48 floor, N = 30.3493, 83.6 keV | 8.4×10¹⁶ | 4.8×10¹⁴ | −4.4×10⁻¹³ | 2.2×10⁻⁵ | 1.7×10⁻⁵ \| 6.6×10⁻⁸ |
| 2 | 10⁻¹² | #48 floor | 7.3 | 1.7×10⁷ | −3.8×10⁻¹³ | 2.2×10⁻⁵ | 1.7×10⁻⁵ \| 6.6×10⁻⁸ |
| 2 | 10⁻⁷ | #48 unresolved | 2.34 | 7.2×10⁴ | −6.1×10⁻⁹ | 2.2×10⁻⁵ | 1.7×10⁻⁵ \| 8.2×10⁻⁸ |
| 3 | 10⁻³⁰ | #58 floor, N = 29.3943, 23.3 MeV | 3.9×10¹⁸ | 1.1×10¹⁶ | −7.0×10⁻¹³ | 2.1×10⁻⁵ | 1.7×10⁻⁵ \| 1.5×10⁻⁷ |
| 3 | 10⁻⁷ | #58 unresolved | 2.51 | 1.4×10⁵ | −6.8×10⁻⁹ | 2.1×10⁻⁵ | 1.7×10⁻⁵ \| 1.6×10⁻⁷ |

What follows from it:
- **The field never reaches φ_min.** Every reflection is made outside the minimum: the unresolved rule
  at 1.7–2.5 φ_min, the floor rule at up to 10¹⁸ φ_min. So the switch moves φ far, in ratio. It moves
  ln A by at most 7×10⁻⁹, against the 2×10⁻⁵–7×10⁻³ that ln A spans over the oscillation it replaces.
  That comparison, not φ/φ_min, is the measure of the switch (§1.3, §2.4).
- **The tracker is very adiabatic at the cut** at small M: m/H ≥ 1.5×10³.
- **At β = 1.2 the oscillation that is cut is slow and wide.** The trace is small there (Σ ≈ 0,
  f_m ≈ 7×10⁻⁵), so at ρ_kin/ρ_R = 5×10⁻⁷ the field still swings out to 6×10⁻³ M_P, and the parked
  ln T_J differs from the oscillating one by 1.1×10⁻³ an e-fold later. At β = 2 and 3 the difference
  is 1.7×10⁻⁵ at most, and below 2×10⁻⁷ by the window's end. ε is to be tuned (R14); §2.5 measures this
  at three ε, as an input to 11.
- **π_min = dφ_min/dN is linear algebra, not a derivative to difference.** The right-hand side's
  d ln ρ_R,E/dN, d ln f_m/dN and d ln T_J/dN are each linear in π, through the factor 1 + (ln A)′π. So
  d ln T/dN = a + bπ, with T = ρ_R,E(Σ + f_m) and dΣ/dN from the cosmology's `dw_dlogT`, and
  π = s(a + bπ), with s = `d_minimum_d_log_trace`. Solved for π, this agrees with a finite difference of
  φ_min along the reduced solution to that difference's own error (10⁻⁷–10⁻⁵).
- **The reduced system is cheap.** DOP853 at max step 0.1 took 3–12 steps per window, about 0.01 s.
  `minimum_on_trace` costs 50–80 µs from a far start and 20–30 µs from the last root.
- **H_E across the switch** changes by −2.5×10⁻⁷ to −4.7×10⁻⁷ if the leftover is left out of the
  ledger: about half the leftover fraction, as it should be. With the leftover in the ledger (P19), H_E
  should be continuous to the change in V and the tracker's own kinetic energy, both far smaller.

**At large M the cut fires too** (`probe_large_M.out`). On the full M = 0.5 M_P histories:
- at β = 1.2 and 2 a single candidate falls below 10⁻⁶ (#6 at 2.2 keV, #27 at 1.2 keV), and there
  m/H = 1.9 and 3.1, so the cut does not fire. **The histories stay bitwise 05's.**
- at β = 5, 10 and 25 it fires at a resolved drain point: #105 at 81 MeV (m/H = 123), #99 at 358 MeV
  (m/H = 267) and #83 at 10 GeV (m/H = 78).
- at β = 5 the tracker then falls below m/H = 10 near N = 40.26, T_J = 9.65 MeV, on the oscillating
  history's trace. **In 06 such a history fails there**, and 07 releases it. No α > 0 history is
  stored before 09.

At M = 10⁻³ it fires at a drain point too (β = 1.2, 2 and 3: #6 at 11.5 keV, #48 at 82 keV and #58 at
23 MeV, m/H = 20–1 500). The cut is R14's as ruled: these firings are its consequence, not a departure.
At a resolved drain point the gate, not w_gate = 1, drained the cascade before the cut, so P19's reason
("the rest of the cascade would have drained it") holds there only in its bound: the leftover is at
most ε ρ_R,E. The cut ends oscillations there that would have gone on.

**The user has ruled that this stands** (README §8.1 R17): "At large M, the dissipative mode is
'unofficial'. It is not clear it corresponds to anything physical. I think we stick with the first
option (the same parking condition applies at large M, even if it does terminate oscillations that
could have kept going), and simply document the behaviour." So the cut is the same at every M, and the
behaviour is documented (§4 item 4).

### 0.3 Departures from README §8.4's charter, and why

- **Two existing tests change their setup.** The charter expects none. `test_injection`'s item (11)
  (β = 10) needs the near-stall at 36 MeV, after the cut at 358 MeV. `test_gate_and_taper`'s item (9)
  (β = 5) needs the full oscillating history, which in 06 fails at 9.65 MeV. Both test 02's and 03's
  machinery on an oscillating history, not parking. Each runs with the cut disabled: ε patched to 0.0,
  so that ρ_kin/ρ_R < ε never holds. That is the only change (§1.7 item 10).
- **The cut's adiabaticity condition is the tracker's m_eff/H_E ≥ 10** at the candidate, the same
  threshold as the release (P18). Here it is a condition, and inside the parked phase a failure.
- **The leftover is the drained state's kinetic energy.** The candidate drains as it always has, and
  is recorded as its `Injection`, because 08 needs it as a generation. The cut then puts what is left
  into the ledger as a second entry at the same N. Together the two entries hold all of ρ_kin.
- **ln A is the measure of the switch, not φ** (§0.2).

---

## 1. What is wanted

### 1.1 The constants (P18)

Module constants in `ComputeTargets/ScalarHistory.py`, beside 05's, each with a comment citing its
ruling. None is a key:
- the parking ε = 10⁻⁶ (R14);
- the adiabaticity threshold m_eff/H_E = 10: the cut's condition here, and 07's release (R16);
- the guard on |π|, 10⁻⁹ M_P, stored in units of M_P (P21).

The names are yours. The loop reads each at call time, so that `unittest.mock.patch` of any of them
changes it (§1.7 items 6, 9 and 10 rely on this).

### 1.2 The cut (R14, P19)

At α > 0, on a model with a wall (05's `assess_wall`), at every candidate **after the history's first**
(its first impact), a drain point or a reflection of either kind:
1. **The candidate as now:** its record, and its drain at its α_eff.
2. **The test:** ρ_kin,E/ρ_R,E < ε, strictly. ρ_kin,E is the candidate's `log_rho_kin`, from before its
   drain (π_in at a reflection), and ρ_R,E is the state's. If it fails, nothing more is computed.
3. **The tracker:** φ_min = `model.minimum_on_trace(φ, ln T)` at the drained state, T = ρ_R,E(Σ + f_m)
   with Σ = 1 − 3w(T_J), and π_min from §1.4. Then m_eff/H_E ≥ 10, with m_eff from `log_m_eff_squared`
   at (φ_min, ln ρ_R,E, ln f_m, Σ) and H_E from `HubblePolicy` at (φ_min, π_min) with the ledger as it
   stands. Neither goes through `fun`. If it fails, the history goes on as before.
4. **The switch**, if both hold:
   - the drained state's ρ_kin,E (H_E²π²/2, H_E from the drained state and the ledger after the drain)
     goes into the ledger as ΔK = ρ_kin,E e^{4N} at the same N (P19);
   - the parked phase (§1.3) starts at N from (φ_min, π_min) and the drained state's ln ρ_R,E, ln f_m
     and ln T_J, unchanged;
   - the parking record (§1.5).

Steps 3 and 4 are module-level functions, called through the module's names at call time (as 03's and
05's are), so that they can be tested on synthetic values and patched. A `ComputationFailureError`
from the model's methods is a failure of the history, with its reason.

**The guard (P21).** At a reflection of either kind where the cut did not fire, |π_in| < 10⁻⁹ M_P is a
failure of the history. The reason names |π_in|, ρ_kin/ρ_R and, if step 3 ran, m/H.

**Natural mode** (α = 0) runs none of this: no test, no guard, no parking (P22).

### 1.3 The parked phase (R15)

- **The reduced system** is (ln ρ_R,E, ln f_m, ln T_J). Its derivatives are `ODERHS`'s components 2–4,
  evaluated through `fun` (so `nfev` and the supervisor count them) at the full state
  (φ_min, π_min, ln ρ_R,E, ln f_m, ln T_J), with φ_min and π_min solved at every evaluation from that
  state's trace. The ledger is the history's, unchanged after the switch.
- **The solver** is yours, among SciPy's. The system has no fast mode; the probe used DOP853. Use
  `params.rtol` and `params.atol`, and `params.global_max_step` as the largest step, so that the parked
  dense output is as finely stepped as the oscillating one at its widest. Say in the log why you chose
  it, with its cost.
- **The end** is as for the oscillating phase: the first accepted step with ln T_J < log_T_stop, at the
  root on that step's interpolant; or `N_stop`; or `N_failsafe`, a failure. The step budget counts the
  steps of both phases. `accepted_steps` is their total, and the parking record holds the parked
  phase's own.
- **Adiabaticity.** At the end of every accepted parked step, m_eff/H_E < 10 on the tracker is a
  failure of the history, with N, T_J and m/H in its reason (07 replaces this with the release).
- **The dense output.** Each accepted parked step appends one segment to `ts` and `interpolants`, so
  that the history's `OdeSolution` (`alt_segment=True`, so the parked state at the switch's N) covers
  both phases. A parked segment is a callable that returns the full five-component state, like a
  Radau segment: shape (5,) for a scalar N, and (5, n) for an array. For each N it solves φ_min and
  π_min from its step's interpolant of the reduced state. `first_bounce`, `surfing_turning_points`,
  the fixed-temperature readers and `integrate_history`'s sampling then read the parked phase without
  change.
- **Determinism.** A parked segment's value at N depends on N alone, never on the order of calls:
  `minimum_on_trace`'s start is fixed per segment (for example the φ_min at its left node), never the
  last root found. The same applies inside the reduced right-hand side within a step. A scalar call and
  the same N in an array call return the same floats.
- **Nothing else runs** in the parked phase: no turning points, no candidates, no 03 samples, no
  assessment.

### 1.4 The tracker's velocity

π_min = s(a + bπ_min) solved for π_min, with s = `d_minimum_d_log_trace(φ_min, ln T)` and
d ln T/dN = a + bπ from the right-hand side's expressions for d ln ρ_R,E/dN, d ln f_m/dN and
d ln T_J/dN, with dΣ/dN = −3 (dw/d ln T_J)(d ln T_J/dN) (§0.2). Derive it in the log. A non-finite
result, or 1 − sb = 0, is a `ComputationFailureError`. Write it as a module-level function of the
model, the cosmology, φ_min and the reduced state. Do not form π_min by differencing φ_min.

### 1.5 The records

Append new fields last, with comments, as 05 did.
- **`IntegrationResult`** gains the parking record, `None` if the history did not park. It holds:
  - N and ln T_J at the switch;
  - the candidate's index in `injections`;
  - φ and π before the switch, φ_min and π_min;
  - ρ_kin/ρ_R,E at the test;
  - the leftover's ΔK and its Δρ/ρ_R,E;
  - m_eff/H_E at the switch;
  - H_E before the candidate and at the parked state;
  - Δ ln A at the switch;
  - the parked phase's accepted steps and its smallest m_eff/H_E.

  The names are yours. 07 adds the release; 09 stores the N and T_J of parking.
- **The payload** of `integrate_history` gains the record under a new key, `"parking"`, which is not
  stored.
- **Unchanged:** `ScalarHistory.store`, `build_extra_data`, `golden_history.record_of`,
  `TUNING_OUTPUT_KEYS`, `"reflections"` and the existing records' fields.

### 1.6 The tuning

`tune_history` is not changed. Whether it converges at small M is measured (§2.6).

### 1.7 The tests

A new module, `ComputeTargets/tests/test_parking.py`. No store and no Ray. Each test that integrates
gives its cost and the load in its docstring. Share integrations between items where you can.
1. **α = 0 is the old code.**
   - The golden tests, `test_first_bounce.py`, `test_surfing_turning_points.py`,
     `test_kinematic_cap_loop.py` and `test_unresolved_turnarounds.py` pass unedited.
   - A spy on the model's `minimum_on_trace` (on the class) records zero calls on a golden history at
     α = 0, and on the P1 harness at α = 0.
2. **The cut's decision, on synthetic values:**
   - ε at its edge: equal does not fire, the next float below does;
   - m/H = 10 fires, the next float below does not;
   - the history's first candidate never fires.
3. **The tracker's velocity.**
   - On synthetic states, π_min against a finite difference of φ_min along a constructed trace, to a
     tolerance you derive.
   - At the same states, d ln T/dN formed from `ODERHS`'s components 2–4 at (φ_min, π_min) equals
     a + bπ_min (self-consistency).
   - On a parked history, π on the samples against a finite difference of φ on the samples.
4. **An M = 10⁻³⁰ M_P history at α = 0.1 reaches T_CMB** (β = 2; it fails at 9c2483b):
   - it parks at the candidate and N that log 05 §5.5 gives;
   - up to the switch it is bitwise the same history with the cut disabled (ε patched to 0.0): the
     records, and the dense output on the steps before N;
   - the parking record's fields, by construction.
5. **The switch.**
   - H_E at the parked state against H_E before the candidate, continuous to a bound you derive from
     the change in V and the tracker's kinetic energy, and far below the leftover fraction.
   - The ledger: K after the switch equals K before the candidate plus the candidate's ΔK plus the
     leftover's. K at the history's end equals that.
   - The dense output at the switch's N returns the parked state.
6. **The parked samples.**
   - φ on every parked sample equals `minimum_on_trace` at the sample's own trace, to the root's
     tolerance (`WALL_ROOT_XTOL` in ln φ).
   - π equals §1.4's π_min there.
   - A parked segment returns bitwise the same floats in any call order, and for scalar and array
     calls.
   - `integrate_history` succeeds, with `first_bounce` before the switch, and T_J = 1 MeV and 70 keV
     located wherever they fall.
7. **Large M.**
   - M = 0.5 M_P, β = 2, α = 0.1: no parking, and the history bitwise 05's (the cut disabled; share
     with `test_unresolved_turnarounds`' setup where you can).
   - M = 0.5 M_P, β = 5, α = 0.1: parks at a drain point, then fails on the tracker's loss of
     adiabaticity, with its reason (§0.2 put it near N = 40.26, T_J = 9.65 MeV, on the oscillating
     history's trace).
8. **The window.** `N_stop` inside the parked phase ends the integration there, successfully.
9. **The guard (P21)** fails a history with its reason. If no history reaches |π_in| < 10⁻⁹ M_P before
   another failure, as 05's M = 10⁻³⁰ histories do not (log 05 §5.5), raise the threshold by a patch,
   and say why.
10. **The two setup changes** (§0.3). `TestTwoTurningPointsOnOneStep` in `test_injection.py` and
    `TestTheNearStallIsGated` in `test_gate_and_taper.py` integrate with ε patched to 0.0, and nothing
    else in them changes. These are the only edits to existing tests that this prompt anticipates. Each
    is listed in the log with its reason. Any other is a deviation to classify, or a stop (§5).

**Each test is shown to catch a breakage** (P11). Record each breakage as a diff, with the tests it
fails; never commit one.
- B1: the cut tested at the history's first candidate (item 2).
- B2: ≤ in place of < for ε (item 2).
- B3: the leftover not added to the ledger (item 5).
- B4: π_min = 0 in the parked state (items 3, 6).
- B5: d_minimum_d_log_trace's denominator given log_m_eff_squared's (ln A)′² term (item 3).
- B6: π_min left out of the right-hand side's evaluation, but kept on the samples (item 3's
  self-consistency).
- B7: `minimum_on_trace` started from the last root found (item 6).
- B8: the adiabaticity condition dropped from the cut (item 7).
- B9: the guard removed (item 9).
- B10: the cut tested at α = 0 (item 1).
- B11: the parked phase's adiabaticity check removed (item 7).

---

## 2. Measurement

Recorded as tables in the log. Every history uses `test_injection`'s setup (n = 1, φ* = 5 M_P, π* = 0,
T* = 2×10⁴ GeV, to T_CMB, default tolerances, the model's seed realization, untuned) unless stated,
with a step budget of 2×10⁵ (patch `SM.StepControl` in the scratch script, as 05 did). No store, no
Ray.
1. **The cost.**
   - Per cut test, in µs: the ratio alone, and the tracker (φ_min, π_min, m/H) when it runs.
   - Per parked right-hand-side evaluation, and per parked step.
   - Per history: at M = 0.5 M_P, β = 2, α = 0.1, against the dispatch commit, run back to back in a
     temporary worktree of it that is removed afterwards. Minima of three, with the load.
2. **M = 0.5 M_P, α = 0.1, β ∈ {1.2, 2, 5, 25}**, against the dispatch commit (the same worktree):
   - every pre-06 field of every record, the samples and the final state bitwise 05's up to the switch,
     and over the whole history where it does not park;
   - where it parks: the candidate, N, T_J and m/H; where the tracker falls below m/H = 10 (for 07).
3. **M = 10⁻³ M_P, α = 0.1, β ∈ {0.5, 1.2, 2, 3}:** the outcome; where it parks (candidate, kind, N, T_J,
   m/H); where the tracker falls below m/H = 10, if it does (for 07; the planner's `parked_tracker.out`
   put it at 31 keV at β = 0.5 and near today at β = 1.2).
4. **Down to laboratory M.** Log 05's grid: β ∈ {1.2, 2, 3} × M ∈ {10⁻⁷, 10⁻⁸, 10⁻⁹, 10⁻¹⁰, 10⁻¹², 10⁻³⁰}
   M_P, α = 0.1, to T_CMB. For each history:
   - the outcome;
   - the cut's candidate, kind, N and T_J, and ρ_kin/ρ_R there;
   - Σ Δρ/ρ_R over the candidates, the leftover's Δρ/ρ_R, and the two together, against the ceiling
     1/(6β² − 1);
   - Δ ln A at the switch against the range of ln A over the oscillation it replaces;
   - H_E across the switch;
   - m/H at the switch, and its smallest value over the parked phase;
   - the steps before and after the switch, and the wall time;
   - that none of log 05 §5.5's bottom-of-cascade failures occurs.
5. **The parked against the oscillating history.** At β ∈ {1.2, 2, 3} and M ∈ {10⁻⁷, 10⁻³⁰}, over the
   window after the switch that 05's history survives (log 05 §5.5): ln T_J, ln ρ_R,E, H_E and H_J,
   largest difference and difference at the window's end. Then at β = 1.2, M = 10⁻⁷, the same at
   ε ∈ {10⁻⁴, 10⁻⁶, 10⁻⁸}, with where each parks. This is the probe's §0.2 measurement made on the
   implementation, and an input to 11's ε (U10).
6. **The tuning.** `tune_history` at α = 0.1, β = 2, at M = 10⁻³⁰ and 10⁻⁷ M_P, and at β = 1.2 and 3 at
   M = 10⁻³⁰, with `main.py`'s defaults otherwise. For each: the integrations, the residuals per
   integration, whether it converged, where each integration parked, the warnings printed, and the
   wall time. A tuning that does not converge is a finding, not a stop (README §8.1: "just something we
   learn by testing").
7. **Natural mode** at M = 0.5 M_P, β = 2: bitwise the dispatch commit's.

---

## 3. Verification

1. **Before any edit:** the five suites, run as `CLAUDE.md` says, with ComputeTargets in its two
   halves. Expect 26 / 374 (96 + 278) / 103 / 115 / 64 (log 05 §8). A different count, or a failure,
   is a stop.
2. **After:** the five suites. ComputeTargets rises by the new tests only, and the golden tests pass
   unchanged; say so. Then `black --check` on the files you changed.
3. **The breakages** of §1.7, each with the tests it fails.

## 4. Acceptance

1. §1 as written, and the gate **"Park"** (README §8.4):
   - α = 0 bitwise;
   - an M = 10⁻³⁰ drained history reaches T_CMB;
   - the parked samples lie on φ_min;
   - H_E continuous at the switch;
   - the leftover in the ledger;
   - the guard.
2. **The measurement** of §2, as tables in the log, with α > 0 at M = 0.5 bitwise 05's up to the switch.
3. **The log**, `logs/06-parking.md`, in the template of `dk-and-rpk-migration` README §5.1. It holds:
   - the derivation of π_min (§1.4);
   - the order at a candidate, with the constants;
   - the solver chosen for the parked phase, and why;
   - the suite counts before and after;
   - the breakages as diffs;
   - every edit to an existing test, with its reason;
   - every deviation classified;
   - "State handed to the next prompt", for 07: the parking record and what each field holds; the
     parked segment's interface and how a release would read its state; where the adiabaticity check
     is made and on what; the histories that lose adiabaticity, with where; the constants' names; the
     cost.
4. **The documents** (P12):
   - a dated note in `.documents/architecture-summary.md` beside 05's (the parked phase, the parking
     record and the payload key);
   - a dated subsection 6.5 in `.documents/numerical-strategies.md`, after 05's 6.4 (the cut, the
     tracker's velocity, the reduced system and its dense output);
   - in both, the behaviour at large M (R17): the same cut applies at every M; at large M it fires at
     resolved drain points, where the gate drained little, and ends oscillations that would have gone
     on, putting at most ε ρ_R,E into the ledger; dissipation mode at large M is not claimed to be
     physical, and whether its answer means anything is for the user of the result to judge. Give the
     M = 0.5 and 10⁻³ histories of §2.2 and §2.3 that park, with where.
5. **The board:**
   - row 06 and the gate Park, in §1 and §2;
   - `### 5.7 After prompt 06 (<date>)` in §5;
   - §3 and §4 for anything opened. Narrow
     `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]` with what §2.4 finds (the cut
     comes first, or does not), with `.documents/OPEN_ISSUES.md` in step.
6. **One commit**, in `CLAUDE.md`'s form, staged by explicit path only.

## 5. Stop conditions: stop and ask

- A baseline count differs from §3 item 1, or an existing test fails before you edit.
- Either golden history, `test_first_bounce`, `test_surfing_turning_points`, `test_kinematic_cap_loop`
  or `test_unresolved_turnarounds` changes.
- α > 0 at M = 0.5 M_P is not bitwise 05's before the switch, or parks at β = 1.2 or 2.
- A history of §2.4 does not reach T_CMB, or does not park. The probes and log 05 put every one of them
  past the cut with an adiabatic tracker.
- The switch moves ln A by more than ln A spans over the oscillation it replaces, or H_E by more than
  the leftover fraction.
- `minimum_on_trace` or `d_minimum_d_log_trace` raises on a history of §2.2–§2.5. That would be a defect
  in 04's methods, whose fix is not this prompt's.
- An existing test needs an edit other than those of §1.7 item 10.
- The work seems to need a change to 04's methods, to the policies or the right-hand side, to the ledger,
  or to the store.

## 6. What this prompt does not do

- **No release**: 07's (R16, P24). A parked tracker that stops being adiabatic fails the history.
- **No peak, no margin**: 08's. The records carry what 08 needs.
- **No store, key or column**, no change to `compute_scalar_history`'s signature, and no change to
  `AdiabaticHistory` (09 computes Q on parked samples).
- **No change to the model methods**, to `ExponentialModel`, to the policies, to `ODERHS` or to the
  ledger.
- **No change to the tuning, the tools or the drivers.**
- **Files it changes:**
  - `ComputeTargets/ScalarHistory.py`;
  - `ComputeTargets/tests/test_parking.py` (new);
  - `ComputeTargets/tests/test_injection.py` and `ComputeTargets/tests/test_gate_and_taper.py`, the
    setup of one class each only (§1.7 item 10);
  - `.documents/architecture-summary.md` and `.documents/numerical-strategies.md`;
  - the log, the board and `.documents/OPEN_ISSUES.md`.

  Any other file is a deviation to classify.
