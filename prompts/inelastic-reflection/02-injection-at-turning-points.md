# Prompt 02 — injection at turning points

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-28, after 01 landed (`85661b5`) and was reviewed (`b30f7bd`), from log 01 §7
("State handed to the next prompt"). If an orchestrator note is written for this prompt at
dispatch, it governs where it differs from this file.
**Gate:**
- on branch `inelastic-reflection`, at or after `b30f7bd`, with no tracked change in `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. It changes the step loop every history is integrated with, and
the loop must stay bitwise at α = 0.

**Read first:**

1. [`README.md`](README.md):
   - §0.2 (the step loop);
   - §1;
   - §2.2 (02's charter, and 03's, which builds on this record);
   - §2.3 (the gate "Inject");
   - §4 (01 → 02, 02 → 03);
   - §5;
   - §6.1 U4 as ruled (b′);
   - §6.2 P1, P3, P4, P5, P11 and P12.
2. [`logs/01-dark-radiation-component.md`](logs/01-dark-radiation-component.md): §2 and §7. The
   ledger's API and the N_j ≤ N edge are 01's hand-over to this prompt.
3. `ComputeTargets/dark_radiation.py`, all of it.
4. `ComputeTargets/ScalarHistory.py`, at `b30f7bd`:
   - `IntegrationResult` and `Reflection` (:124–155);
   - `ODEPolicy` (:236–404) and `HubblePolicy` (:407);
   - `_reflection_guards` (:566);
   - `integrate_scalar_history` (:615–829), the step loop;
   - `first_bounce` (:848) and `surfing_turning_points` (:938);
   - `integrate_history` (:1190–1460);
   - `tune_history` (:1719).
5. `ComputeTargets/tests/test_kinematic_cap_loop.py`: the P1 harness and the floor reflection at
   M = 1e-10 (:216–280). Also `test_first_bounce.py`, `test_surfing_turning_points.py` and
   `test_dark_radiation.py`.
6. `.documents/numerical-strategies.md` §6.1 and `.documents/architecture-summary.md` §7.2, the
   notes 01 added.

---

## 0. Measured at planning

The planner ran a read-only probe at `b30f7bd`:
[`source/planning-probes-2026-10-28/turning_points.py`](source/planning-probes-2026-10-28/README.md).
Its setup was:
- M = 0.5 M_P, n = 1;
- φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV;
- `integrate_history` to T_CMB at the model's seed realization, untuned;
- atol = rtol = 1e-8, with main.py's sampling axis;
- α = 0, at a load of about 5.

Turning points were counted as sign changes of π across accepted steps, with `first_bounce`'s
strict convention. Each step's interpolant was also sampled at 17 points, to find steps holding
two or more sign changes.

| β | wall time | accepted steps | floor reflections | wall-side (π − → +) | plateau (π + → −) | steps with ≥ 2 changes | first wall-side, T_J | last wall-side, T_J |
|---|---|---|---|---|---|---|---|---|
| 1.2 | 0.8 s | 2 177 | 0 | 9 | 8 | 0 | 231 MeV | 12 eV |
| 2 | 1.4 s | 3 725 | 0 | 23 | 22 | 0 | 747 MeV | 62 keV |
| 5 | 4.1 s | 11 887 | 0 | 141 | 140 | 0 | 176 GeV | 169 keV |
| 25 | 8.0 s | 22 344 | 0 | 353 | 352 | 0 | 1.06 TeV | 6.5 keV |

So at α = 0.1 a history drains 9 to 353 times, with each drain restarting Radau. The turning points
reach into the BBN window.

**`OdeSolution` at a breakpoint** (SciPy 1.17.0 in the venv). `OdeSolution(ts, interpolants)`
evaluates a t equal to a node with the *earlier* segment: `side="left"`, then `segment = ind − 1`.
With `alt_segment=True` it takes the *later* one. That is the N_j ≤ N edge of log 01 §7: at a drain
point the earlier segment holds the state before the drain, while the ledger already holds the new
K. §1.4 settles it.

---

## 1. What is wanted

At α > 0, each wall-side turning point of φ drains a fraction α of the condensate's kinetic energy
into the dark-radiation ledger. The drain happens at the kinetic-energy maximum that follows the
turning point. Each floor-rule reflection drains the same fraction in its own restart. Nothing
is gated or tapered yet: in this prompt α_eff = α at every drain, and 03 adds the weights.

### 1.1 Turning points

After every accepted step, the loop checks π at the step's two ends, with `first_bounce`'s strict
convention.
- **A wall-side turning point**: π(t_old) < 0 < π(t). The field was turned back by the potential.
- **A plateau turnaround**: π(t_old) > 0 > π(t). The field was turned back by the matter coupling.

Each is located on the step's interpolant by `brentq` on π, with `xtol = 1e-15`, exactly as
`first_bounce` does. The first wall-side turning point is then bitwise `first_bounce`'s whenever no
reflection precedes it.
- A floor-rule reflection happens at a step boundary, so it is never seen as a sign change inside a
  step (`first_bounce`'s docstring). It is recorded as a floor event (§1.3), not as a turning point.
- The loop records turning points at **every** α, α = 0 included. Detection reads the interpolant
  and does not touch the solver. 03 needs the plateau turnarounds as a diagnostic at every α.
- Turning points on the step that ends the history are recorded up to N_final.

The record is `TurningPoint(N, phi_Einstein, log_T_Jordan, kind)`, with `kind` either `"wall"` or
`"plateau"`. `IntegrationResult` gains `turning_points`, the list in order.

### 1.2 The drain point and the drain

A wall-side turning point **arms** a drain.
- **The drain point** is the end of the first accepted step, counting from the turning point's own
  step, at whose end π > 0 and π′ ≤ 0. π′ is `solver.f[1]` at the step's end. This is the end of
  the first step after the maximum of |π| (P3). The maximum may lie inside the turning point's own
  step.
- **The drain**, at the drain point (N, y), with α_eff = α:
  1. ρ_kin,E = ½ H_E² π², with H_E taken from `HubblePolicy` on `ODEPolicy`'s data at (N, y),
     with the ledger as it stands before this drain.
  2. ΔK = α_eff ρ_kin,E e^{4N}. Compute it without overflow; log ΔK's terms if you need to.
  3. `ledger.add(N, ΔK)`.
  4. π → √(1 − α_eff) π.
  5. `new_solver(N, y_drained)`.

  The ledger is added to **before** the new Radau instance is built: its constructor evaluates the
  RHS at (N, y_drained), and that call must see the new K.
- **H_E is continuous.** The constraint 3M_P²H²G = V + ρ_R(1 + f_m) + ρ_χ, with G = 1 − π²/6M_P²,
  is met at the same H_E by (π, ρ_χ) and by (√(1 − α)π, ρ_χ + αρ_kin). This holds exactly in
  exact arithmetic: check it in the log, in two lines. H_J jumps, because π does (P14).
- **No drain**:
  - on the step that ends the history (the termination root, or N_stop);
  - when α_eff == 0.0. At α = 0 there is no ledger entry, no restart and no change to y.
- **A pending drain is dropped** if a plateau turnaround comes before the drain point, or the
  history ends first. Count the dropped drains. The planner expects none: between two turning
  points |π| passes its maximum.

### 1.3 Floor-rule reflections

Under the floor rule the reflection is the drain point (P3). In the one restart:
- π_out = −√(1 − α_eff) π_in;
- ΔK = α_eff ½ H_E² π_in² e^{4N}, with H_E from the state before the reflection and the ledger
  before the drain.

The guards G1 and G2 are checked on π_in, as today. At α_eff == 0 the restart is today's exactly,
with `y_reflected[1] = -pi_` and no ledger entry. Every floor event is still a `Reflection`, so
`result.reflections`, its count, the payload's `"reflections"` and `first_bounce` keep their
meaning (README §2.2). At α > 0 it is also an `Injection` of kind `"floor"`.

### 1.4 The records, the dense output and the result

- **`Injection`**, one per drain, in order:
  - `N`: the drain point;
  - `kind`: `"turning_point"` or `"floor"`;
  - `N_turn`: the turning point's N, or N for a floor event;
  - `phi_turn`: φ at the turning point, or at the reflection;
  - `pi_before` and `pi_after` at the drain;
  - `log_rho_kin`: ln ρ_kin,E before the drain;
  - `alpha_eff`;
  - `delta_K`;
  - `delta_rho_over_rho_rad`: α_eff ρ_kin,E/ρ_R,E;
  - `H_Einstein_before` and `H_Einstein_after`: H_E from the state and ledger before the drain,
    and from those after it.

  These are P4's fields that 02 can fill. λ, k_ex, m_ta, 𝒜_k, w_gate, w_rate and ln q are 03's and
  04's: those prompts extend the record, and 02 does not add placeholders for them.
- **`IntegrationResult`** gains `turning_points` and `injections`, appended last. Check every
  construction (one, `:820`) and every reader.
- **The dense output is right-continuous at a restart.** Build the result's solution as
  `OdeSolution(ts, interpolants, alt_segment=True)`. At a drain point N_j it then returns the
  post-drain state, which the ledger's rule N_j ≤ N pairs with the post-drain K. The same then holds
  at a floor reflection, where it returns the reflected state.
  - **Why this, and not the ledger's rule changed:** the RHS of the new Radau instance at N_j needs
    the new K, and so does every later query. With the dense output right-continuous, every caller
    sees one consistent pairing: `ODEPolicy`, the sampling loop, `fixed_T_value_at` (which reads
    `interpolants[k]` itself, on the step the crossing is in) and `_tuning_state`.
  - **The only node a caller queries on purpose is N_final** (ts[-1]), and that is unaffected. So
    the golden histories should stay bitwise. If they do not, **stop and ask**; do not fall back to
    another scheme on your own.
- **The parameter.** `integrate_scalar_history`, `integrate_history` and `tune_history` take
  `alpha: float = 0.0`.
  - A value outside 0 ≤ α < 1 is a `ValueError` at entry: before the seed in `tune_history`, before
    any integration in the others.
  - `integrate_scalar_history` drains into `policy.dark_radiation`. At α > 0 a policy with no ledger
    is a `ValueError`.
  - `tune_history` passes α to each integration, and each builds its own ledger.
  - `compute_scalar_history` does **not** take α (README §2.2, 05/06): a stored history with an α
    that is not in its key would be wrong.
- **The payload** of a successful integration gains `"injections"` and `"turning_points"`, the two
  lists. Nothing on the way to the store reads them (01 checked `store` and `build_extra_data`;
  check again). The payload must still pickle.

### 1.5 The tests

Put them in `ComputeTargets/tests/test_injection.py`, new. Give each one's cost in its docstring.

1. **α = 0 is the old code.** `test_golden_history.py`, the tuned golden test,
   `test_first_bounce.py`, `test_surfing_turning_points.py` and `test_kinematic_cap_loop.py` all
   pass without edits. Add one test: on the golden β = 1.5 history at α = 0, the ledger is empty,
   `injections` is empty, and the first wall-side `TurningPoint` equals `first_bounce` field by field
   (N bitwise).
2. **The classification.** On a real history at α = 0.1 (M = 0.5, β = 2 serves: 1.4 s):
   - each wall-side turning point has π(t_old) < 0 < π(t), and each plateau one the reverse;
   - the kinds alternate;
   - each wall-side turning point is followed by exactly one injection of kind `"turning_point"`,
     before the next turning point;
   - no injection follows a plateau turnaround.
3. **The drain point.** For each injection of kind `"turning_point"`:
   - π > 0 and π′ ≤ 0 at the step end it was made at;
   - at the end of the step before, π′ > 0, unless that step is the turning point's own.
4. **H_E continuous.** For every injection, `H_Einstein_after` equals `H_Einstein_before` to a
   rounding bound you state and justify (ruling 8 of note 01 stands: a number, not
   `assertAlmostEqual`'s default).
5. **The ledger is the sum of the drains.** `ledger.injections` equals `[(i.N, i.delta_K)]` exactly.
   Each ΔK = α ½H_E²π_before² e^{4N} to rounding, and π_after = √(1 − α) π_before exactly, as floats
   computed the same way.
6. **The dense output at N_j.** For each drain, `result.solution(N_j)` is the post-drain state, π
   included, and H_E rebuilt there equals `H_Einstein_after`. The test says that SciPy's default
   would return the pre-drain state.
7. **The new Radau instance sees the new K.** Every RHS evaluation at N ≥ N_j after a drain sees
   K(N_j) with the drain in it. Design the check (wrapping the RHS is one way) so that the
   breakage below fails it.
8. **A floor-rule drain.** Use `test_kinematic_cap_loop.py`'s P1 harness at M = 1e-10 (one floor
   reflection, about 0.2 s), with a ledger and α = 0.1. Check:
   - π_out = −√(0.9) π_in exactly;
   - ΔK and the ledger;
   - H_E before and after, as item 4;
   - `len(result.reflections) == 1`, and one injection of kind `"floor"`.

   At α = 0 the same run is today's, with no injection.
9. **The parameter.** α < 0 and α ≥ 1 raise; α > 0 with no ledger raises. `tune_history` raises
   before the seed (patch `integrate_history` and assert it is not called, as `test_tuning.py`
   (m) does).
10. **Pickling.** The payload at α = 0.1 pickles, and the copy has the same records.

**Deliberate breakages** (P11). Each must fail a named test. Record each as a diff in the log;
never commit one.
- **B1** drain π without adding ΔK to the ledger;
- **B2** ΔK computed from π after the drain;
- **B3** plateau turnarounds drain too;
- **B4** drain at the end of the turning point's own step, whatever π′;
- **B5** the floor reflection without √(1 − α);
- **B6** `ledger.add` after `new_solver`;
- **B7** `OdeSolution` without `alt_segment=True`;
- **B8** at α = 0, restart at each would-be drain point with y unchanged. The golden histories must
  fail on it, and if they do not, say so: it would mean a restart is free at α = 0, which is worth
  knowing.

---

## 2. Measurement

On the planner's setup of §0 (untuned, at the model's seed realization), for β ∈ {1.2, 2, 5, 25},
at α = 0 and α = 0.1. Run it from your scratch space, never committed. Give per history:
- wall-side and plateau turning points;
- injections, by kind;
- dropped drains;
- floor reflections;
- accepted steps and RHS evaluations;
- restarts;
- wall time;
- Σ Δρ/ρ_R;
- ρ_χ/ρ_R at T_J = 1 MeV, at 70 keV and at the end;
- the number of steps whose interpolant, sampled at 17 points, has two or more sign changes of π.

Then one tuning, `tune_history` at β = 2, α = 0.1, from the model's seed. Report its iterations,
its residuals, and whether it converges. **Not converging is a measurement, not a stop.** The
tuning residual may be discontinuous in the normalization when the number of drains changes, and
that is 03's to measure and taper.

Your α = 0 counts must equal the planner's table in §0. A difference is a stop.

## 3. Verification

1. **Before any edit:**
   - the five suites, run as `CLAUDE.md` says, with ComputeTargets in its two halves;
   - the two golden tests alone, with their times.

   Expect 26 / 288 (96 + 192) / 103 / 115 / 64 (board §5.1). `CLAUDE.md`'s ComputeTargets counts are
   stale (269, 77) until 08. A different count from 288, or a failure, is a stop.
2. **After:** the five suites. ComputeTargets rises by the new tests and nothing else falls. Then
   `black --check` on the files you changed.
3. **The breakages** of §1.5, each with the tests it fails.
4. **The cost at α = 0:** the golden tests' wall time before and after, back to back, three times
   each, with the minima and the load. The turning-point detection runs at α = 0, so a cost is
   possible. Report it, and the number of `brentq` calls it adds to the golden histories.

## 4. Acceptance

1. §1.1–§1.5 as written, and the gate "Inject" (README §2.3):
   - both golden histories bitwise at α = 0;
   - at α > 0 H_E continuous across every injection to rounding, and the ledger equal to the sum of
     the drains;
   - a floor-rule drain exact.
2. **The measurement** of §2, as tables in the log.
3. **The log**, `logs/02-injection-at-turning-points.md`, in the template of
   `dk-and-rpk-migration` README §5.1. It holds:
   - the H_E continuity argument of §1.2;
   - the suite counts before and after;
   - the breakages as diffs;
   - the measurement;
   - every deviation classified;
   - "State handed to the next prompt". This covers the `TurningPoint` and `Injection` records,
     where α_eff is set (the one place 03 multiplies in w_gate and w_rate), the dropped-drain rule,
     and the counts 03 will gate.
4. **The documents** (P12): dated notes in `.documents/numerical-strategies.md` (the step loop: the
   drain, the restart, the dense output's side) and in `.documents/architecture-summary.md` (the
   records and the payload keys).
5. **The board:**
   - row 02 and the gate Inject, in §1 and §2;
   - `### 5.2 After prompt 02 (<date>)` in §5;
   - §3 and §4 for anything opened, with `.documents/OPEN_ISSUES.md` in step.
6. **One commit**, in `CLAUDE.md`'s form, staged by explicit path only.

## 5. Stop conditions: stop and ask

- A golden history is not bitwise at α = 0, `alt_segment=True` included, and the cause is not a
  mistake of yours you can see and undo.
- H_E is not continuous across a drain to rounding.
- The measurement finds a step with two or more sign changes of π, at α = 0 or 0.1. The loop would
  then miss turning points, and 03's gate rests on them.
- An α = 0 count in §2 differs from §0's table.
- An existing test fails for a reason this prompt does not name.
- A baseline count differs from §3.1 before you edit.

## 6. What this prompt does not do

- **No gate, no taper, no stopping rule** (03). α_eff = α at every wall-side turning point and every
  floor reflection.
- **No margin** (04), **no store, no key, no column** (05), **no option** (06).
  `compute_scalar_history`, `main.py`, the plot scripts and the tools are unchanged.
- **`AdiabaticHistory` is unchanged** (05).
- **Files it changes:**
  - `ComputeTargets/ScalarHistory.py`;
  - `ComputeTargets/tests/test_injection.py` (new);
  - the two `.documents/` files;
  - the log, the board and, if anything is opened, `.documents/OPEN_ISSUES.md`.

  `ComputeTargets/dark_radiation.py` changes only if the ledger needs something 01 did not give it,
  as a deviation with its reason. Any other file is a deviation to classify.
