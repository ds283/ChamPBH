# Prompt 06b — Store the fixed-temperature values on the `ScalarModel` row

**Campaign:** [`README.md`](README.md) · **Board item:** **V** · **Board:**
`IMPLEMENTATION_STATE.md`. Update your row and V.
**Closes:** `[00-fixed-T-values-are-not-stored]` on this board.
**Recommended model:** **Opus.** The function is short. The judgement is in where the history
crosses a temperature, across a reflection restart and at the ends of the solution, and in proving
that the stored values are the ones BBN sees.

Planned on 2026-10-02, after the user's ruling on prompt 07's first implementation (board
Decisions; README §0.2, the amendment for U6). It runs after 06 and before 07.

**Read first:**

1. [`README.md`](README.md) §0.2 (the U6 amendment, P5), §0.5, §2 (g), §2 (n), §5, §6.4, §6.7b.
2. `logs/03-first-bounce.md`, "State handed to the next prompt": how the first bounce is
   computed, stored and read back. Copy that pattern.
3. `ComputeTargets/ScalarModel.py`:
   - `IntegrationResult`, `Reflection`, and how `integrate_scalar_history` builds `ts`,
     `interpolants` and `reflections`;
   - `first_bounce`, the model for the new function;
   - `compute_scalar_model`'s sampling loop. It shows how `ODEPolicy`, `HubblePolicy` and the
     coupling turn a state into `H_J`, `ρ_R,J` and `f_m`;
   - `ScalarModel.__init__`, `store()` and the `first_bounce` property.
4. `ComputeTargets/BBNData.py`, `compute_BBN_data`: the loop that builds `density_NP` and
   `density_NP_ratio` from a `ScalarModelValue`. The new ratio must equal it.
5. `Datastore/SQL/ObjectFactories/ScalarModel.py`: the four `first_bounce_*` columns, their
   units on write and read, and what `_do_not_populate` skips.
6. `tools/history_and_bbn.py`, and `Datastore/tests/test_first_bounce_round_trip.py`.

---

## 1. The changes

All of this follows README §2 (n).

- **In `ComputeTargets/ScalarModel.py`:**
  - Two module constants, `FIXED_T_JORDAN_HIGH_MEV = 1.0` and `FIXED_T_JORDAN_LOW_MEV = 0.07`.
  - `FixedTValues`, a namedtuple with the fields `phi_Einstein_1MeV`, `density_NP_ratio_1MeV`,
    `phi_Einstein_70keV` and `density_NP_ratio_70keV`. Each field is a float, or `None` if the
    history does not reach that temperature.
  - `T_Jordan_crossing(result, log_T)`, which returns the `N` of the history's **first**
    crossing of `ln T_J = log_T`, or `None`.
    - Walk the accepted steps in order, using their own interpolants.
    - Find the first step on which `y[4] − log_T` changes sign or reaches zero.
    - Take the root on that step's interpolant with `brentq` (`xtol=1e-15`).
  - `fixed_T_values(result, policy, coupling, units)`, which returns a `FixedTValues`. At each
    crossing `N`, read the state from the interpolant. Then:
    - φ is `y[0]`;
    - `ρ_NP/ρ_R,J` comes from `policy` and `HubblePolicy`, exactly as the sampling loop builds
      `H_J`, `log_rhorad_Jordan` and `log_fm`, and then exactly as `compute_BBN_data` combines
      them: `(3 M_P² H_J² − ρ_R,J (1 + f_m)) / ρ_R,J`.
- **`compute_scalar_model`** calls `fixed_T_values` after the loop, next to `first_bounce`, and
  returns it as `"fixed_T_values"`.
- **`ScalarModel`** stores the values through `store()` and `__init__`. A `fixed_T_values`
  property returns the tuple. It raises on a failure row, as `first_bounce` does, and it works on
  an object read with `_do_not_populate`.
- **The factory:** four nullable `Float(64)` columns with the same names as the namedtuple's
  fields. φ is stored in units of `M_P`, as `first_bounce_phi_Einstein` is. A column is NULL when
  the history does not reach that temperature, and all four are NULL on a failure row.
- **The driver** prints one `fixed_T` line per history. For each temperature it gives:
  - φ in `M_P` and the ratio;
  - the number of sign changes of `ln T_J − ln T*` across the accepted steps' end points;
  - the same two values interpolated linearly in `ln T_J` between the two stored samples either
    side. This is the stand-in.

## 2. Tests

- **(a) Agreement with the stored samples.** Use one full history, β = 2 at `M = 0.5` (about
  2 s; say so in the docstring). Choose three of its stored samples with `T_J` between 0.07 and
  1 MeV. Call `T_Jordan_crossing` at each sample's own `log_T_Jordan`, and evaluate the state and
  ratio there with the function behind `fixed_T_values`.
  - The crossing gives the sample's `raw_N` to `1e-10`.
  - φ equals the sample's `phi_Einstein` to `1e-9` relative.
  - The ratio equals `compute_BBN_data`'s expression applied to that sample's fields, to `1e-8`
    relative. Write the expression into the test from `BBNData.py`, citing the line. Do not
    import it.
- **(b) Not reached.** A target above the history's first `T_J`, or below its last, gives
  `None`. The P1 window of `test_kinematic_cap_loop.py` ends near `T_J ≈ 0.4 GeV`, above both
  temperatures, so all four fields of `fixed_T_values` on it are `None`.
- **(c) The first crossing, across a reflection.** Use P1 at `M = 1e-10`, which has one floor
  reflection. A target `T_J` on the step that ends at the reflection is found on that step, and
  the result is continuous in `N` across the reflection: `ln T_J` matches on both sides to
  `1e-12`.
- **(d) The round trip** (`Datastore/tests/`). Use a temporary SQLite store.
  - A row with all four values reads them back, floats to the last bit.
  - A row that does not reach 70 keV reads back `None` for those two fields.
  - A failure row raises on `fixed_T_values`.
  - **A read with `_do_not_populate=True` returns the four values.** This is the point of the
    prompt.
- **On `HEAD~1`** nothing stores these values. The stand-in measurement is the driver's
  sample-interpolated value beside the dense-output value, on the three histories of
  acceptance 2, recorded in the log. It is a measurement, not a bound.

## 3. What this prompt does not do

- No change to the step loop, the sampling, `IntegrationResult`, the reflection, the policies or
  `extra_data`.
- No change to `BBNData.py`. BBN keeps computing its own per-sample ratio; test (a) ties the two
  together.
- No change to `plot_by_beta.py` or `extract_common.py`. That is prompt 07.
- The temperatures are not a run option, and they are not part of the lookup key.
- No `VERSION_LABEL` bump (README §0.2 P9; the user confirmed on 2026-10-02 that no 2026.6.0
  store exists).

## 4. Acceptance

1. README §6.7b, every row.
2. **The driver** on β = 2 at `M = 0.5` and `10⁻³`, and β = 1.6 at `10⁻⁵`, unloaded, one at a
   time:
   - every history crosses each temperature exactly once (one sign change);
   - the four values are recorded beside the stand-in.
3. RHS, accepted steps, wall bounces and the first bounce's `N` on those three histories are
   identical to the orchestrator's baseline: no trajectory moved.
4. All three suites pass and rise by your methods. `black --check` clean.
5. The board and the index: V done, its issue closed.

## 5. Stop conditions — stop and ask the user

- A roster history crosses 1 MeV or 70 keV more than once. Which crossing to store is then the
  user's ruling, not yours.
- Test (a)'s ratio does not agree with `compute_BBN_data`'s expression to `1e-8`, or φ does not
  agree with the sample's.
- A reflection restart leaves a step's interpolant unusable for locating a crossing.
- Acceptance 3 fails.
- Making the property work under `_do_not_populate` needs a change outside the files this prompt
  allows.

## 6. The log and the board

Write `logs/06b-fixed-T-values.md` in the README §5.1 template. In "State handed to the next
prompt", give:
- the signatures of the two functions;
- the namedtuple, the four column names and their units;
- the property;
- the driver's `fixed_T` output on the three histories.
