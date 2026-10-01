# Prompt 05 — Bounce averages on the dense output

**Campaign:** [`README.md`](README.md) · **Board item:** **A** · **Board:**
`IMPLEMENTATION_STATE.md`. Update your row and A.
**Narrows:** `[00-stored-samples-alias-the-rebounds]`, assigned from the `integrator-remediation`
board. Its BBN half closes here; its adiabatic half stays open there as
`[post-adiabatic-Q-reads-aliased-late-samples]`. Add a dated **Narrowed** line under the entry on
that board, and correct the index row's hook. Also closes `[00-bbn-input-is-aliased-at-small-M]`
on this board.
**Recommended model:** **Opus.** Quadrature is easy. The judgement is in walking two sorted
sequences, steps and cells, without an off-by-one at the edges, at a cost that stays near the
estimate, while leaving every point value and every trajectory bit-identical.

**Read first:**

1. [`README.md`](README.md) §0.2 (U3, P7), §0.5, §2 (d), (i), §5, §6.1, §6.6.
2. `logs/04-initial-field-option.md`, "State handed to the next prompt", and the driver's output
   in logs 01 and 03.
3. `ComputeTargets/ScalarModel.py`: `compute_scalar_model`'s sampling loop (`z_grid_cut`,
   `N_forward`, `policy`, `hubble`, `SampleValues`), `HubblePolicy`, `ScalarModelValue`, and
   `ScalarModel.store()`.
4. `Datastore/SQL/ObjectFactories/ScalarModel.py`: the value table and how samples are written
   and read.
5. `ComputeTargets/BBNData.py`: where `density_NP` is computed from `value.H_Jordan`.
6. `.documents/review-remediation-verification.md` §4.9 point 3 (the sampling, by window), and the
   source's §2 (the β = 1.6, `M = 10⁻⁵` failure).

---

## 1. The changes

- **The cell means (README §2 (i)).** In `compute_scalar_model`, the sampling builds the samples
  in increasing `N`. Reverse the order you walk them in if you need to, but store them in the
  order they are stored now.
  - Compute `H_Jordan_sq_cell_mean` and `phi_Einstein_cell_mean` for every sample by three-point
    Gauss–Legendre on each piece of an accepted step inside the sample's cell. Evaluate the
    step's own interpolant, never `OdeSolution.__call__` across a step boundary.
  - The cell boundaries are the midpoints in `N` between neighbouring samples, clipped to the
    history's first and last `N` (P7).
  - Both are new `SampleValues` and `ScalarModelValue` fields and two new non-null `Float(64)`
    columns, written and read by the factory.
  - A non-finite node value raises `ComputationFailureError` (a failure row, with prompt 02's
    reason).
- **BBN reads the average.** `compute_BBN_data` computes
  `ρ_NP = 3 M_P² H_Jordan_sq_cell_mean − ρ_R,J (1 + f_m)`, using the sample's point `ρ_R,J` and
  `f_m`. `BBNDataValue.density_NP` and `density_NP_ratio` are then the averaged values. Nothing
  else in BBN changes.
- **The driver** prints the `ratio` windows from the averaged ratio. It also prints, beside them,
  the point ratio for comparison.
- **`AdiabaticHistory`** is not touched.

## 2. Tests

- **(a) The quadrature, exactly.** A synthetic `OdeSolution` of a few steps whose state makes
  `H_J²` and `φ` known in closed form, with a cell boundary inside a step and a step boundary
  inside a cell. Drive the averaging function directly: factor it as a pure function taking
  `(ts, interpolants, sample_N, evaluate)`. The cell means match the analytic integrals to
  `1e-10` relative. A polynomial of degree ≤ 5 per piece is exact.
- **(b) The partition.** On the same input, the cells cover `[N_first, N_last]` with no gap and no
  overlap. The sum of cell length × mean equals the integral over the whole range.
- **(c) A resolved window.** On the P2 window (β = 2, `M = 0.5`, parked), with a z grid of your
  choosing inside the window, the cell mean of `φ` differs from the point value by less than
  `1e-3` relative wherever there is no bounce in the cell. About two seconds; the docstring says
  so.
- **(d) The point values are untouched.** On the P1 window, every stored point field is
  bit-identical to the result without the averaging. Build both from the same `IntegrationResult`.

## 3. What this prompt does not do

No change to the step loop, the z grid, the point sample fields, the adiabatic stage, or the
spline floor (prompt 06). No smoothing in `BBNData` (README §4). No time-weighting: the average is
in `N` (P7). The `N`-against-time comparison is a measurement, not a change.

## 4. Acceptance

1. README §6.6, every row, from the driver on the three §6.1 histories and β = 2 at `M = 10⁻⁵`.
2. **The breakage witness.** The `ratio` windows below 3 keV, from the driver on prompt 04's tree
   and on yours, at β = 1.6, `M = 10⁻⁵` and β = 2, `M = 10⁻³`. The rms step falls as README §6.6
   requires. Quote both sets. (The source's PRyMordial failure at β = 1.6, `M = 10⁻⁵` did not
   reproduce on `6aaa706`, so it is not the witness; README §6.1.)
3. **The trajectory.** RHS, accepted steps and the first bounce on all four histories are
   identical to log 04's.
4. **The cost.** The history's wall time at β = 2, `M = 10⁻⁵` is at most 1.5× that on prompt 04's
   tree, with both run unloaded, one after the other. Quote both, and the number of policy
   evaluations the averaging made.
5. **`N` against time.** For β = 2, `M = 10⁻⁵`, in the samples with `T_J` in [0.3, 3) keV, the
   mean over samples of `|⟨H²⟩_N − ⟨H²⟩_t| / ⟨H²⟩_N` (scratch, from the same interpolants, with
   `dt = dN/H_J`). Record it as a measurement.
6. All three suites pass and rise. `black --check` clean. The board and the index: A done, both
   issues handled as the header says.

## 5. Stop conditions — stop and ask the user

- Acceptance 2: the rms step at β = 1.6, `M = 10⁻⁵` does not fall by 10×. Either the cells do
  not span the bounces, or the noise is not aliasing.
- β = 1.6, `M = 10⁻⁵` fails in PRyMordial with averaged input.
- Acceptance 3 or (d): something changed a trajectory or a point value.
- Acceptance 4: the cost is above 1.5×, and the evaluation count is not the reason.
- Acceptance 5 is above `1e-3`. The `N`-average would then not be a good proxy for the
  time-average at that level, which is a decision for the user.

## 6. The log and the board

`logs/05-bounce-averages.md`, in the README §5.1 template. In "State handed to the next prompt",
give the two field names, the driver's output on the four histories (point and averaged
`ratio` windows), and the BBN abundances.
