# Prompt 01 — the temperature-law harness, and the guard that would have caught R1

**Campaign:** [`README.md`](README.md) · **Board item:** **R1** (the guard) ·
**Board:** `IMPLEMENTATION_STATE.md` — exists (created at planning); update your row in §1 and the
R1 row in §2.
**Closes:** nothing. **Opens:** anything out of scope that you find (§5), without fixing it.
**Recommended model:** **Opus**. No production code. The substance is making the reference
*independent* of the thing being measured, and characterising the defect honestly so that the
next prompt's fix announces itself.

**Read first:**

1. [`README.md`](README.md) §0, §2 (a)–(c), §5, §6.1.
2. `.documents/audit-2026-09-29/README.md` §1, and the three scripts beside it —
   `tlaw_check.py` and `eos_consistency.py` are the measurements you are turning into tests.
   Run them first, from the repository root, and confirm you reproduce the §6.1 "now" column.
3. `CosmologyModels/GenericEOS/SaikawaShirai_EOS_spline.py` in full (`_log_T_grid` at `:55`,
   `dG_rho_dlogT` `:114–130`, `dG_s_dlogT` `:155–171`, `w` `:176–200`), and
   `SaikawaShirai_EOS_jax_autodiff.py:200–230` (`dG_s_dlogT` returns `T_in_GeV * grad`).
4. `CosmologyModels/GenericEOS/Xav_EOS_spline.py` (the class in production, `QCD_Cosmology.py:46`).
5. `ComputeTargets/ScalarModel.py:340–370`, the RHS — the consumer at `:359–360`. **You do not
   change this file.**
6. `CLAUDE.md`, "Repository mechanics": tests in `<package>/tests/`, from the root, no Ray, no
   datastore; the CSV is read by a relative path.

**It changes no production code.** Nothing outside `CosmologyModels/tests/`, this campaign's
`logs/` and board, and `.documents/OPEN_ISSUES.md` appears in the diff.

---

## 1. The defect, stated so the tests can see it

The EOS spline classes build their g_ρ and g_s splines on a grid uniform in **log10 T**
(`SaikawaShirai_EOS_spline.py:55`) and return `spline.derivative()` unchanged from
`dG_s_dlogT` / `dG_rho_dlogT`. That is dg/d log10 T = ln 10 · dg/d ln T. The RHS at
`ScalarModel.py:359–360` writes the temperature law for dg_s/d ln T, and the jax class returns
dg_s/d ln T. Commit `5962833` made the change; before it, the RHS used `(T/G_s) dG_s/dT`.

Three independent witnesses, none of which uses the shipped derivative:

1. **Exact entropy conservation.** T_J a_J g_s^{1/3} = const, so the e-fold count from T* to T
   at fixed field is N = ln(T*/T) + ⅓ ln[g_s(T*)/g_s(T)]. Integrating the code's own law
   d ln T/dN = −1/(1 + dG_s_dlogT/G_s/3) from T* = 2×10⁴ GeV must reproduce it. Today it
   overshoots by 1.42 e-folds at T_CMB.
2. **A central difference of `G_s` in ln T** against `dG_s_dlogT`. Today the ratio is 2.303.
3. **The ρ_R witness.** d ln ρ_R/dN = Σ − 4 with Σ = 1 − 3w(T) from the production class,
   integrated alongside the law from 5 MeV to 10 keV, against (π²/30) g_ρ(T) T⁴. Today 0.182
   (0.0041 if started from 2×10⁴ GeV).

---

## 2. What to build

`CosmologyModels/tests/__init__.py` (empty) and two modules.

### 2.1 `CosmologyModels/tests/eos_reference.py` — the measurement infrastructure

Pure functions, no `unittest` in it, so that prompts 02, 05 and 06 and the campaign scripts can
import it:

- `exact_efolds(eos, T0_GeV, T1_GeV) -> float` — witness 1's closed form, from `eos.G_s` only.
- `integrate_temperature_law(eos, T0_GeV, T1_GeV, kappa=1.0, with_rho=False, rtol=1e-10,
  atol=1e-12, max_step=0.05) -> result` — integrates the law exactly as `ODERHS.__call__`
  writes it, with `kappa` multiplying `dG_s_dlogT` so the same function can score the shipped
  convention (`kappa=1`) and the corrected one (`kappa=1/ln 10`) **before prompt 02 lands**; with
  `with_rho=True` it carries ln ρ_R with d ln ρ_R/dN = Σ − 4. Returns the e-folds reached and,
  if asked, the integrated ρ_R. Terminate on an event at ln T₁, as `tlaw_check.py` does.
- `thermodynamic_rho_R(eos, T_GeV) -> float` — (π²/30) g_ρ(T) T⁴ using `constants.RadiationConstant`.
- `derivative_convention_ratio(eos, T_GeV, h=1e-4) -> float` — `eos.dG_s_dlogT(T)` divided by
  the central difference of `eos.G_s` in ln T with half-step `h`.
- `production_eos(units=None)` — `Xav_EOS_spline(GeV_units())`, built once per test class.

Take the geometry and the numbers from `tlaw_check.py`; keep the docstrings saying which
witness each function is and why it does not call the shipped derivative.

### 2.2 `CosmologyModels/tests/test_temperature_law.py` — the tests

Every threshold is a **named module constant with a comment saying which prompt tightens it**.
This prompt characterises; prompt 02 flips the constants. Cases:

1. **The guard, characterised.** For each T₁ in {1 GeV, 100 MeV, 5 MeV, 1 MeV, 100 keV, 70 keV,
   10 keV, T_CMB}: `integrate_temperature_law(eos, 2e4, T1, kappa=1.0)` minus
   `exact_efolds` equals the audit's offset (+0.180, +0.779, +0.987, +0.994, +1.336, +1.403,
   +1.422, +1.422) to **± 2e-3**. Constant `TEMPERATURE_LAW_EFOLD_OFFSET_TOLERANCE`; the
   comment says prompt 02 replaces the expected offsets by zero and the tolerance by 1e-5.
2. **The guard, corrected convention.** The same with `kappa = 1/ln 10` agrees with
   `exact_efolds` to **1e-5** at every T₁. This must pass *today*; it proves the fix is a
   factor and nothing else, before anyone edits production code.
3. **The derivative convention, characterised.** `derivative_convention_ratio` over 60 points
   log-spaced in [20 keV, 5 TeV] (inside the clamps, away from the table's own joins by at least
   a factor 1.5 in T) equals **ln 10 to 1e-3 relative**. Comment: prompt 02 sets the expected
   ratio to 1 and the tolerance to 1e-6.
4. **The two implementations disagree by ln 10.** `skipUnless` jax imports:
   `SaikawaShirai_EOS_spline.dG_s_dlogT / SaikawaShirai_EOS_jax_autodiff.dG_s_dlogT` at the
   same 60 points is ln 10 to 1e-3. Comment: prompt 02 sets it to 1 and 1e-6. (jax 0.9.0 is in
   the venv; the skip is for other environments.)
5. **The ρ_R witness, characterised.** `integrate_temperature_law(eos, 5e-3, 1e-5, kappa=1.0,
   with_rho=True)` gives ρ_R/thermodynamic = **0.182 ± 2e-3**; with `kappa = 1/ln 10`,
   **1.005 ± 5e-3**. From 2×10⁴ GeV the same pair is 0.0041 and 1.0026 (README §2 (c)); assert
   either range, but say which. Comment: prompt 02 removes the `kappa=1` half; prompt 05 tightens the other.
6. **Clamps.** `dG_s_dlogT` and `dG_rho_dlogT` are exactly 0 below 10⁻⁵ GeV and above 10¹⁶ GeV,
   and `G_s`, `G_rho` take `LOW_T_G_S_STAR`, `LOW_T_GSTAR`, `HIGH_T_GSTAR` there. These hold before
   and after and are not the point; they pin the plateaus the paper describes.

Each numerical assertion prints the measured value on failure (use `msg=`). Total runtime should
be well under a minute; `integrate_temperature_law` with `max_step=0.05` over 40 e-folds is a
few seconds per call, so build the eight-point tables once in `setUpClass`.

---

## 3. What this prompt does not do

- It does not touch `SaikawaShirai_EOS_spline.py`, `ScalarModel.py`, `main.py` or the version
  label. Characterise, do not fix (README §5 rule 5).
- It does not delete or edit the audit scripts; they stay as the record of the measurement on
  `f5896bb`. If your test and a script disagree, say so in the log and say which is right.
- It does not pin the kicking-function peaks. That is prompt 05.

---

## 4. Acceptance

1. `CosmologyModels/tests/` exists, is discoverable from the root, and needs no Ray cluster and
   no datastore. Suite count: **0 → the number of cases above (6, or 5 if jax is skipped —
   record which)**.
2. Cases 1, 3, 4, 5 pass **as characterisations of the defect**, each with its constant and
   comment; case 2 passes and is the proof that ÷ln 10 is the whole fix; case 6 passes.
3. `black --check` clean on the files added; the board's §1 and §2 rows updated; the log
   written; `.documents/OPEN_ISSUES.md` updated only if you opened an issue.
4. The log's verification section reproduces README §6.1's "now" column from the tests
   themselves, with the printed values.

---

## 5. Stop conditions — stop and ask the user

- `integrate_temperature_law` with `kappa = 1/ln 10` does **not** agree with `exact_efolds` to
  1e-5. That would mean the defect is not a pure factor, and prompt 02 as written is wrong.
- The ρ_R witness with the corrected convention is outside 1.005 ± 5e-3 (from 5 MeV). That would mean the
  table and the g's are inconsistent at a level the campaign has not budgeted for.
- The jax class also returns a log10 derivative. README §2 (b) would then be wrong.
- You find you need to edit a production file to make a test importable.

---

## 6. The log and the board

`logs/01-temperature-law-harness.md`, template README §5.1. Beyond it: the exact geometry of the
60-point derivative grid and why it avoids the table joins; the wall-clock of the suite; and the
measured offsets to four decimals, since prompt 02 will quote them as "before".

Board: your §1 row (landed, commit, log), R1's status in §2 ("guard landed; fix pending").
`.documents/OPEN_ISSUES.md`: only if §3 changed.
