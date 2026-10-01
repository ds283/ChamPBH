# Prompt 04 — The initial field as a run option, with a super-Planckian warning

**Campaign:** [`README.md`](README.md) · **Board item:** **P** · **Board:**
`IMPLEMENTATION_STATE.md`. Update your row and P.
**Closes:** `[00-initial-field-value-is-hard-coded-and-unchecked]`, assigned from the
`review-remediation` board (README §5 rule 4).
**Recommended model:** **Sonnet.** One option read in three places, and one pure function.

**Read first:**

1. [`README.md`](README.md) §0.2 (P6), §2 (h), §5, §6.5.
2. `logs/03-first-bounce.md`, "State handed to the next prompt".
3. `prompts/review-remediation/IMPLEMENTATION_STATE.md`, the issue's entry (review H8).
4. `config/argument_parser.py`; `main.py:800–860` and the coupling grid it builds;
   `plot_by_beta.py:820–840`; `plot_ScalarModel.py:1660–1680`; `pipeline_selection.py` and its
   test `ComputeTargets/tests/test_pipeline_selection.py`.
5. `CosmologyConcepts/ConformalCouplings/`: `log_Omega` on each coupling.
6. `exponential.yaml`, `recliner.yaml`, `starobinsky.yaml`: which β ranges they run.

---

## 1. The changes

- **`--phi-init-Mp`** (float, default `5.0`, help text naming M_P) in the shared parser.
- **`main.py`, `plot_by_beta.py`, `plot_ScalarModel.py`** build `phi_init` from
  `args.phi_init_Mp * units.PlanckMass`. No `5.0 * units.PlanckMass` literal remains.
- **`super_planckian_couplings(couplings, phi_init, T_init, units) -> list`** in
  `pipeline_selection.py`. It returns the couplings with
  `coupling.log_Omega(φ*) + ln T* > ln M_P`.
- **`main.py`** prints, before step 1, `"!! warning: β=…, φ*=… M_P: Ω(φ*) T* = … M_P (super-Planckian start)"`
  for each such coupling, and the count. **It changes nothing else**: the coupling array, the
  batches and what is computed and stored are exactly as without the check (P6, as ruled by the
  user: warn, never skip or refuse).
- **The driver** gains `--phi-init-Mp`.

## 2. Tests

- **(a) The check.** The README §6.5 rows, with `ExponentialCoupling` and `Planck_units`. The
  threshold arithmetic is in the docstring.
- **(b) No literal left.** `ast`-parse the three drivers: no `BinOp` of the constant `5.0` with
  `units.PlanckMass` remains, and each reads `args.phi_init_Mp`.
- **(c) The parser.** `create_argument_parser().parse_args([])` gives `phi_init_Mp == 5.0`, and
  `["--phi-init-Mp", "2"]` gives 2.0.
- **(d) Nothing is dropped.** `main.py`'s warning step, factored as a function, takes a coupling
  list with super-Planckian entries and returns it unchanged (same objects, same order), having
  printed one warning per entry.
- **On `HEAD~1`:** (b) fails, finding three literals. The check is new. The stand-in is the
  arithmetic for `exponential.yaml`'s grid at φ\* = 5, recorded in the log: which β it warns about.

## 3. What this prompt does not do

No store tag (`phi_Einstein_init` is already in the lookup key). No `π*` option. No change to a
YAML file. No coupling is removed, skipped or refused. The log records which β the check warns
about for each YAML file.

## 4. Acceptance

README §6.5, every row. The driver with `--phi-init-Mp 5` reproduces log 03's β = 2, `M = 0.5`
history exactly. The driver with `--phi-init-Mp 2` completes, and its figures are recorded. All
three suites pass and rise. `black --check` clean. The board and the index: P done, the assigned
issue closed.

## 5. Stop conditions — stop and ask the user

- A coupling class has no `log_Omega`, or uses a convention in which `Ω(φ*) T*` is not the
  Einstein-frame temperature scale.
- Printing the warning would need any change to the coupling array, the batches or the stored
  rows.

## 6. The log and the board

`logs/04-initial-field-option.md`, in the README §5.1 template.
